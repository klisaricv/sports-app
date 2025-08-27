# services/data_repo.py  (MYSQL ONLY)
from __future__ import annotations

from datetime import datetime, date, timedelta, timezone
from typing import Dict, List, Tuple, Optional, Set
import json
import time

from mysql_database import get_mysql_connection

# ---------- HTTP klijent (API-Football) ----------
try:
    from .api_client import rate_limited_request, SESSION  # ako već postoji u projektu
except Exception:
    import requests
    API_KEY = '505703178f0eb0be24c37646ea9d06d9'
    HEADERS = {'x-apisports-key': API_KEY}
    SESSION = requests.Session()
    SESSION.headers.update(HEADERS)

    def rate_limited_request(url, params=None, max_retries=5, timeout=20):
        retries = 0
        while retries <= max_retries:
            try:
                resp = SESSION.get(url, params=params, timeout=timeout)
            except requests.RequestException:
                time.sleep(2 ** retries)
                retries += 1
                continue
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 429:
                retry_after = int(resp.headers.get('Retry-After', '2'))
                time.sleep(retry_after + 0.8)
                retries += 1
            else:
                time.sleep(2 ** retries)
                retries += 1
        return None

BASE_URL = "https://v3.football.api-sports.io"
CACHE_TTL_HOURS = 48
EXTRAS_TTL_HOURS = 48
BASE_SEASON_FALLBACK = datetime.utcnow().year

# ---------- MySQL helper-i (INSERT/UPSERT) ----------
def insert_team_matches(team_id: int, matches: list):
    if not matches:
        return
    conn = get_mysql_connection()
    cur = conn.cursor()
    for m in matches:
        fid = ((m.get("fixture") or {}).get("id"))
        if not fid:
            continue
        cur.execute("""
            INSERT INTO team_matches (team_id, fixture_id, data)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE data=VALUES(data)
        """, (team_id, fid, json.dumps(m, ensure_ascii=False)))
    conn.commit()
    conn.close()

def insert_h2h_matches(a: int, b: int, matches: list):
    if not matches:
        return
    x, y = sorted([a, b])
    conn = get_mysql_connection()
    cur = conn.cursor()
    for m in matches:
        fid = ((m.get("fixture") or {}).get("id"))
        if not fid:
            continue
        cur.execute("""
            INSERT INTO h2h_matches (team1_id, team2_id, fixture_id, data)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE data=VALUES(data)
        """, (x, y, fid, json.dumps(m, ensure_ascii=False)))
    conn.commit()
    conn.close()

def try_read_fixture_statistics(fixture_id: int):
    conn = get_mysql_connection()
    cur = conn.cursor()
    cur.execute("SELECT data FROM match_statistics WHERE fixture_id=%s", (fixture_id,))
    row = cur.fetchone()
    conn.close()
    if row and row[0]:
        try:
            return json.loads(row[0]) if isinstance(row[0], str) else row[0]
        except Exception:
            return None
    return None

# ---------- DataRepo ----------
class DataRepo:
    # ---- helpers ----
    def _day_bounds(self, d: date) -> Tuple[datetime, datetime]:
        s = datetime(d.year, d.month, d.day, 0, 0, 0)  # naive (server time)
        e = s + timedelta(days=1)
        return s, e

    def _db_has_fixtures_for_day(self, d: date) -> bool:
        s, e = self._day_bounds(d)
        conn = get_mysql_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM fixtures WHERE `date` >= %s AND `date` < %s LIMIT 1", (s, e))
        row = cur.fetchone()
        conn.close()
        return row is not None

    def _read_fixtures_for_day(self, d: date) -> List[dict]:
        s, e = self._day_bounds(d)
        conn = get_mysql_connection()
        cur = conn.cursor()
        cur.execute("SELECT fixture_json FROM fixtures WHERE `date` >= %s AND `date` < %s", (s, e))
        rows = cur.fetchall()
        conn.close()
        out = []
        for (j,) in rows:
            try:
                out.append(json.loads(j) if isinstance(j, str) else j)
            except Exception:
                continue
        return out

    def _store_fixtures(self, fixtures: List[dict]) -> None:
        if not fixtures:
            return
        conn = get_mysql_connection()
        cur = conn.cursor()
        for fx in fixtures:
            try:
                fid = fx["fixture"]["id"]
                fdt = fx["fixture"]["date"]           # ISO string (MySQL DATETIME akceptira 'YYYY-MM-DD HH:MM:SS')
                lid = fx["league"]["id"]
                hid = fx["teams"]["home"]["id"]
                aid = fx["teams"]["away"]["id"]
                fxj = json.dumps(fx, ensure_ascii=False)
                cur.execute("""
                    INSERT INTO fixtures
                        (id, `date`, league_id, team_home_id, team_away_id, stats_json, fixture_json)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                        `date`=VALUES(`date`),
                        league_id=VALUES(league_id),
                        team_home_id=VALUES(team_home_id),
                        team_away_id=VALUES(team_away_id),
                        stats_json=VALUES(stats_json),
                        fixture_json=VALUES(fixture_json)
                """, (fid, fdt, lid, hid, aid, None, fxj))
            except Exception:
                continue
        conn.commit()
        conn.close()

    # ---- PUBLIC ----
    def ensure_day(self, d: date, last_n: int = 15, h2h_n: int = 10, prewarm_stats: bool = False) -> dict:
        seeded = False
        if not self._db_has_fixtures_for_day(d):
            resp = rate_limited_request(f"{BASE_URL}/fixtures", params={"date": d.isoformat(), "timezone": "UTC"})
            fixtures_raw = (resp or {}).get("response") or []
            self._store_fixtures(fixtures_raw)
            seeded = True

        fixtures = self._read_fixtures_for_day(d)

        if prewarm_stats:
            for f in fixtures:
                league_id = ((f.get("league") or {}).get("id"))
                season    = ((f.get("league") or {}).get("season"))
                if league_id and season:
                    for tid in [
                        ((f.get("teams") or {}).get("home") or {}).get("id"),
                        ((f.get("teams") or {}).get("away") or {}).get("id"),
                    ]:
                        if tid:
                            try:
                                _ = self.get_team_statistics(int(tid), int(league_id), int(season), no_api=False)
                            except Exception:
                                pass

        team_ids: Set[int] = {((f.get("teams") or {}).get("home") or {}).get("id") for f in fixtures} | \
                             {((f.get("teams") or {}).get("away") or {}).get("id") for f in fixtures}
        team_ids = {t for t in team_ids if t is not None}

        pairs: Set[Tuple[int,int]] = set()
        for f in fixtures:
            h = ((f.get("teams") or {}).get("home") or {}).get("id")
            a = ((f.get("teams") or {}).get("away") or {}).get("id")
            if h is None or a is None:
                continue
            x, y = sorted([h, a])
            pairs.add((x, y))

        all_team_matches = {}
        for tid in team_ids:
            all_team_matches[tid] = self.get_team_history(tid, last_n=last_n, no_api=False)

        for (a, b) in pairs:
            _ = self.get_h2h(a, b, last_n=h2h_n, no_api=False)

        stats_warmed = 0
        if prewarm_stats:
            fids = set()
            for matches in (all_team_matches or {}).values():
                for m in matches or []:
                    fid = ((m.get("fixture") or {}).get("id"))
                    if fid:
                        fids.add(fid)
            for fid in list(fids):
                try:
                    if self.get_fixture_stats(fid, no_api=False) is not None:
                        stats_warmed += 1
                except Exception:
                    pass

        odds_warmed = 0
        if prewarm_stats:
            for f in fixtures:
                fid = ((f.get("fixture") or {}).get("id"))
                if not fid:
                    continue
                try:
                    if self.get_odds_1h(fid, no_api=False):
                        odds_warmed += 1
                except Exception:
                    pass
                try:
                    if self.get_odds_ft(fid, no_api=False):
                        odds_warmed += 1
                except Exception:
                    pass

        return {
            "day": d.isoformat(),
            "seeded": seeded,
            "fixtures_in_db": len(fixtures),
            "teams": len(team_ids),
            "pairs": len(pairs),
            "stats_warmed": stats_warmed,
            "odds_warmed": odds_warmed,
            "history_missing_before": 0,
            "h2h_missing_before": 0,
            "stats_missing_before": 0,
        }

    def get_team_history(self, team_id: int, last_n: int = 15, no_api: bool = False) -> List[dict]:
        now = datetime.utcnow()
        conn = get_mysql_connection()
        cur = conn.cursor()
        cur.execute("SELECT data, updated_at FROM team_history_cache WHERE team_id=%s AND last_n=%s", (team_id, last_n))
        row = cur.fetchone()
        conn.close()

        if row:
            try:
                updated_at = row[1] if isinstance(row[1], datetime) else datetime.fromisoformat(str(row[1]))
            except Exception:
                updated_at = now - timedelta(hours=CACHE_TTL_HOURS + 1)
            if (now - updated_at) <= timedelta(hours=CACHE_TTL_HOURS) or no_api:
                try:
                    j = row[0]
                    return (json.loads(j) if isinstance(j, str) else j) or []
                except Exception:
                    return []

        if no_api:
            return []

        resp = rate_limited_request(f"{BASE_URL}/fixtures", params={'team': team_id, 'last': last_n, 'timezone': 'UTC'})
        data = resp.get('response', []) if resp else []

        try:
            insert_team_matches(team_id, data)
        except Exception:
            pass

        conn = get_mysql_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO team_history_cache(team_id,last_n,data)
            VALUES(%s,%s,%s)
            ON DUPLICATE KEY UPDATE data=VALUES(data)
        """, (team_id, last_n, json.dumps(data, ensure_ascii=False)))
        conn.commit()
        conn.close()
        return data

    def get_h2h(self, team_a: int, team_b: int, last_n: int = 10, no_api: bool = False) -> List[dict]:
        a, b = sorted([team_a, team_b])
        now = datetime.utcnow()
        conn = get_mysql_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT data, updated_at FROM h2h_cache
            WHERE team1_id=%s AND team2_id=%s AND last_n=%s
        """, (a, b, last_n))
        row = cur.fetchone()
        conn.close()

        if row:
            try:
                updated_at = row[1] if isinstance(row[1], datetime) else datetime.fromisoformat(str(row[1]))
            except Exception:
                updated_at = now - timedelta(hours=CACHE_TTL_HOURS + 1)
            if (now - updated_at) <= timedelta(hours=CACHE_TTL_HOURS) or no_api:
                try:
                    j = row[0]
                    return (json.loads(j) if isinstance(j, str) else j) or []
                except Exception:
                    return []

        if no_api:
            return []

        h2h_key = f"{a}-{b}"
        resp = rate_limited_request(f"{BASE_URL}/fixtures/headtohead", params={'h2h': h2h_key, 'last': last_n})
        data = resp.get('response', []) if resp else []

        try:
            insert_h2h_matches(a, b, data)
        except Exception:
            pass

        conn = get_mysql_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO h2h_cache(team1_id,team2_id,last_n,data)
            VALUES(%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE data=VALUES(data)
        """, (a, b, last_n, json.dumps(data, ensure_ascii=False)))
        conn.commit()
        conn.close()
        return data

    def get_fixture_stats(self, fixture_id: int, no_api: bool = False) -> Optional[list]:
        existing = try_read_fixture_statistics(fixture_id)
        if existing is not None or no_api:
            return existing

        response = rate_limited_request(f"{BASE_URL}/fixtures/statistics", params={"fixture": fixture_id})
        stats = (response or {}).get('response') or None
        if stats is not None:
            conn = get_mysql_connection()
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO match_statistics(fixture_id, data)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE data=VALUES(data)
            """, (fixture_id, json.dumps(stats, ensure_ascii=False)))
            conn.commit()
            conn.close()
        return stats

    def get_fixture_full(self, fixture_id: int, no_api: bool = False) -> Optional[dict]:
        conn = get_mysql_connection()
        cur = conn.cursor()
        cur.execute("SELECT fixture_json FROM fixtures WHERE id=%s", (fixture_id,))
        row = cur.fetchone()
        conn.close()
        if row and row[0]:
            try:
                return json.loads(row[0]) if isinstance(row[0], str) else row[0]
            except Exception:
                pass

        if no_api:
            return None

        resp = rate_limited_request(f"{BASE_URL}/fixtures", params={"id": fixture_id})
        arr = (resp or {}).get("response") or []
        fx = arr[0] if arr else None
        if fx:
            self._store_fixtures([fx])
        return fx

    def get_venue(self, venue_id: Optional[int], no_api: bool = False) -> Optional[dict]:
        if not venue_id:
            return None
        now = datetime.utcnow()
        conn = get_mysql_connection()
        cur = conn.cursor()
        cur.execute("SELECT data, updated_at FROM venues_cache WHERE venue_id=%s", (venue_id,))
        row = cur.fetchone()
        conn.close()
        if row:
            try:
                updated_at = row[1] if isinstance(row[1], datetime) else datetime.fromisoformat(str(row[1]))
            except Exception:
                updated_at = now - timedelta(hours=EXTRAS_TTL_HOURS + 1)
            if (now - updated_at) <= timedelta(hours=EXTRAS_TTL_HOURS) or no_api:
                try:
                    j = row[0]
                    return json.loads(j) if isinstance(j, str) else j
                except Exception:
                    return None

        if no_api:
            return None

        resp = rate_limited_request(f"{BASE_URL}/venues", params={"id": venue_id})
        arr = (resp or {}).get("response") or []
        ven = arr[0] if arr else None

        conn = get_mysql_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO venues_cache(venue_id, data)
            VALUES(%s,%s)
            ON DUPLICATE KEY UPDATE data=VALUES(data)
        """, (venue_id, json.dumps(ven, ensure_ascii=False)))
        conn.commit()
        conn.close()
        return ven

    def get_lineups(self, fixture_id: int, no_api: bool = False) -> Optional[list]:
        now = datetime.utcnow()
        conn = get_mysql_connection()
        cur = conn.cursor()
        cur.execute("SELECT data, updated_at FROM lineups_cache WHERE fixture_id=%s", (fixture_id,))
        row = cur.fetchone()
        conn.close()
        if row:
            try:
                updated_at = row[1] if isinstance(row[1], datetime) else datetime.fromisoformat(str(row[1]))
            except Exception:
                updated_at = now - timedelta(hours=EXTRAS_TTL_HOURS + 1)
            if (now - updated_at) <= timedelta(hours=EXTRAS_TTL_HOURS) or no_api:
                try:
                    j = row[0]
                    return json.loads(j) if isinstance(j, str) else j
                except Exception:
                    return None

        if no_api:
            return None

        resp = rate_limited_request(f"{BASE_URL}/fixtures/lineups", params={"fixture": fixture_id})
        arr = (resp or {}).get("response") or []

        conn = get_mysql_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO lineups_cache(fixture_id, data)
            VALUES(%s,%s)
            ON DUPLICATE KEY UPDATE data=VALUES(data)
        """, (fixture_id, json.dumps(arr, ensure_ascii=False)))
        conn.commit()
        conn.close()
        return arr

    def get_injuries(self, fixture_id: int, no_api: bool = False) -> Optional[list]:
        now = datetime.utcnow()
        conn = get_mysql_connection()
        cur = conn.cursor()
        cur.execute("SELECT data, updated_at FROM injuries_cache WHERE fixture_id=%s", (fixture_id,))
        row = cur.fetchone()
        conn.close()
        if row:
            try:
                updated_at = row[1] if isinstance(row[1], datetime) else datetime.fromisoformat(str(row[1]))
            except Exception:
                updated_at = now - timedelta(hours=EXTRAS_TTL_HOURS + 1)
            if (now - updated_at) <= timedelta(hours=EXTRAS_TTL_HOURS) or no_api:
                try:
                    j = row[0]
                    return json.loads(j) if isinstance(j, str) else j
                except Exception:
                    return None

        if no_api:
            return None

        resp = rate_limited_request(f"{BASE_URL}/injuries", params={"fixture": fixture_id})
        arr = (resp or {}).get("response") or []

        conn = get_mysql_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO injuries_cache(fixture_id, data)
            VALUES(%s,%s)
            ON DUPLICATE KEY UPDATE data=VALUES(data)
        """, (fixture_id, json.dumps(arr, ensure_ascii=False)))
        conn.commit()
        conn.close()
        return arr

    def get_team_statistics(self, team_id: int, league_id: int, season: int, no_api: bool = False) -> dict | None:
        now = datetime.utcnow()
        conn = get_mysql_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT data, updated_at FROM team_stats_cache
            WHERE team_id=%s AND league_id=%s AND season=%s
        """, (team_id, league_id, season))
        row = cur.fetchone()
        conn.close()
        if row:
            try:
                updated_at = row[1] if isinstance(row[1], datetime) else datetime.fromisoformat(str(row[1]))
            except Exception:
                updated_at = now - timedelta(hours=24 + 1)
            if (now - updated_at) <= timedelta(hours=24) or no_api:
                try:
                    j = row[0]
                    return (json.loads(j) if isinstance(j, str) else j) or {}
                except Exception:
                    pass

        if no_api:
            return None

        resp = rate_limited_request(f"{BASE_URL}/teams/statistics",
                                    params={"team": team_id, "league": league_id, "season": season})
        data = (resp or {}).get("response") or {}

        conn = get_mysql_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO team_stats_cache(team_id, league_id, season, data)
            VALUES (%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE data=VALUES(data)
        """, (team_id, league_id, season, json.dumps(data, ensure_ascii=False)))
        conn.commit()
        conn.close()
        return data

    def get_referee_fixtures(self, ref_name: str, season: Optional[int] = None, last_n: int = 200, no_api: bool = False) -> List[dict]:
        if not ref_name:
            return []
        now = datetime.utcnow()
        year = season or BASE_SEASON_FALLBACK

        conn = get_mysql_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT data, updated_at FROM referee_cache
            WHERE ref_name=%s AND season=%s AND last_n=%s
        """, (ref_name, year, last_n))
        row = cur.fetchone()
        conn.close()
        if row:
            try:
                updated_at = row[1] if isinstance(row[1], datetime) else datetime.fromisoformat(str(row[1]))
            except Exception:
                updated_at = now - timedelta(hours=EXTRAS_TTL_HOURS + 1)
            if (now - updated_at) <= timedelta(hours=EXTRAS_TTL_HOURS) or no_api:
                try:
                    j = row[0]
                    return (json.loads(j) if isinstance(j, str) else j) or []
                except Exception:
                    return []

        if no_api:
            return []

        params = {"season": year, "last": last_n, "timezone": "UTC", "referee": ref_name}
        resp = rate_limited_request(f"{BASE_URL}/fixtures", params=params)
        arr = (resp or {}).get("response") or []

        conn = get_mysql_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO referee_cache(ref_name, season, last_n, data)
            VALUES(%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE data=VALUES(data)
        """, (ref_name, year, last_n, json.dumps(arr, ensure_ascii=False)))
        conn.commit()
        conn.close()
        return arr

    def get_odds_1h(self, fixture_id: int, no_api: bool = False) -> Optional[dict]:
        now = datetime.utcnow()
        conn = get_mysql_connection()
        cur = conn.cursor()
        cur.execute("SELECT data, updated_at FROM odds_cache WHERE fixture_id=%s AND market=%s", (fixture_id, "ALL_1H"))
        row = cur.fetchone()
        conn.close()
        if row:
            try:
                updated_at = row[1] if isinstance(row[1], datetime) else datetime.fromisoformat(str(row[1]))
            except Exception:
                updated_at = now - timedelta(hours=EXTRAS_TTL_HOURS + 1)
            if (now - updated_at) <= timedelta(hours=EXTRAS_TTL_HOURS) or no_api:
                try:
                    j = row[0]
                    return (json.loads(j) if isinstance(j, str) else j) or {}
                except Exception:
                    if no_api:
                        return None

        if no_api:
            return None

        resp = rate_limited_request(f"{BASE_URL}/odds", params={"fixture": fixture_id})
        arr = (resp or {}).get("response") or []

        def normalize_markets(rows):
            out = {"OU_1H": {}, "BTTS_1H": {}}
            for r in rows:
                for bk in (r.get("bookmakers") or []):
                    for mkt in (bk.get("bets") or []):
                        name = (mkt.get("name") or "").lower()
                        if "over/under" in name and ("1st half" in name or "1h" in name):
                            for v in (mkt.get("values") or []):
                                val = (v.get("value") or "").lower().replace(" ", "")
                                odd = v.get("odd")
                                if not odd:
                                    continue
                                if val in ("over0.5","o0.5","over0,5"):
                                    out["OU_1H"]["over_0_5"] = float(odd)
                                elif val in ("under0.5","u0.5","under0,5"):
                                    out["OU_1H"]["under_0_5"] = float(odd)
                                elif val in ("over1.5","o1.5","over1,5"):
                                    out["OU_1H"]["over_1_5"] = float(odd)
                                elif val in ("under1.5","u1.5","under1,5"):
                                    out["OU_1H"]["under_1_5"] = float(odd)
                        if ("both teams to score" in name or "btts" in name) and ("1st half" in name or "1h" in name):
                            for v in (mkt.get("values") or []):
                                label = (v.get("value") or "").lower()
                                odd = v.get("odd")
                                if not odd:
                                    continue
                                if label in ("yes","da"):
                                    out["BTTS_1H"]["yes"] = float(odd)
                                elif label in ("no","ne"):
                                    out["BTTS_1H"]["no"] = float(odd)
            return out

        markets = normalize_markets(arr)
        payload = {"markets": markets, "raw": arr, "updated_at": now.isoformat()}

        conn = get_mysql_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO odds_cache(fixture_id, market, data)
            VALUES(%s,%s,%s)
            ON DUPLICATE KEY UPDATE data=VALUES(data)
        """, (fixture_id, "ALL_1H", json.dumps(payload, ensure_ascii=False)))
        conn.commit()
        conn.close()
        return payload

    def get_odds_ft(self, fixture_id: int, no_api: bool = False) -> Optional[dict]:
        now = datetime.utcnow()
        conn = get_mysql_connection()
        cur = conn.cursor()
        cur.execute("SELECT data, updated_at FROM odds_cache WHERE fixture_id=%s AND market=%s", (fixture_id, "ALL_FT"))
        row = cur.fetchone()
        conn.close()
        if row:
            try:
                updated_at = row[1] if isinstance(row[1], datetime) else datetime.fromisoformat(str(row[1]))
            except Exception:
                updated_at = now - timedelta(hours=EXTRAS_TTL_HOURS + 1)
            if (now - updated_at) <= timedelta(hours=EXTRAS_TTL_HOURS) or no_api:
                try:
                    j = row[0]
                    return (json.loads(j) if isinstance(j, str) else j) or {}
                except Exception:
                    if no_api:
                        return None

        if no_api:
            return None

        resp = rate_limited_request(f"{BASE_URL}/odds", params={"fixture": fixture_id})
        arr = (resp or {}).get("response") or []

        def normalize_ft(rows):
            out = {"OU_FT": {}, "BTTS_FT": {}}
            for r in rows:
                for bk in (r.get("bookmakers") or []):
                    for mkt in (bk.get("bets") or []):
                        name = (mkt.get("name") or "").lower()
                        if "over/under" in name and "1st half" not in name and "1h" not in name:
                            for v in (mkt.get("values") or []):
                                val = (v.get("value") or "").lower().replace(" ", "")
                                odd = v.get("odd")
                                if not odd:
                                    continue
                                if val in ("over1.5","o1.5","over1,5"):
                                    out["OU_FT"]["over_1_5"] = float(odd)
                                elif val in ("under1.5","u1.5","under1,5"):
                                    out["OU_FT"]["under_1_5"] = float(odd)
                        if ("both teams to score" in name or "btts" in name) and "1st half" not in name and "1h" not in name:
                            for v in (mkt.get("values") or []):
                                label = (v.get("value") or "").lower()
                                odd = v.get("odd")
                                if not odd:
                                    continue
                                if label in ("yes","da"):
                                    out["BTTS_FT"]["yes"] = float(odd)
                                elif label in ("no","ne"):
                                    out["BTTS_FT"]["no"] = float(odd)
            return out

        markets = normalize_ft(arr)
        payload = {"markets": markets, "raw": arr, "updated_at": now.isoformat()}

        conn = get_mysql_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO odds_cache(fixture_id, market, data)
            VALUES(%s,%s,%s)
            ON DUPLICATE KEY UPDATE data=VALUES(data)
        """, (fixture_id, "ALL_FT", json.dumps(payload, ensure_ascii=False)))
        conn.commit()
        conn.close()
        return payload

    def save_artifacts_for_fixture(
        self,
        fixture: dict,
        league_baselines: Optional[dict],
        team_strengths: Optional[dict],
        team_profiles: Optional[dict],
        micro_db: Optional[dict],
        extras: Optional[dict],
    ) -> None:
        try:
            conn = get_mysql_connection()
            cur = conn.cursor()

            if league_baselines:
                cur.execute("""
                    INSERT INTO league_baselines_store (id, data)
                    VALUES (1, %s)
                    ON DUPLICATE KEY UPDATE data=VALUES(data)
                """, (json.dumps(league_baselines, ensure_ascii=False),))

            for tid, obj in (team_strengths or {}).items():
                cur.execute("""
                    INSERT INTO team_strengths_store (team_id, data)
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE data=VALUES(data)
                """, (int(tid), json.dumps(obj, ensure_ascii=False)))

            for tid, obj in (team_profiles or {}).items():
                cur.execute("""
                    INSERT INTO team_profiles_store (team_id, data)
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE data=VALUES(data)
                """, (int(tid), json.dumps(obj, ensure_ascii=False)))

            for tid, sides in (micro_db or {}).items():
                for side in ("home", "away"):
                    if side in (sides or {}) and sides[side] is not None:
                        cur.execute("""
                            INSERT INTO team_micro_form_store (team_id, side, data)
                            VALUES (%s, %s, %s)
                            ON DUPLICATE KEY UPDATE data=VALUES(data)
                        """, (int(tid), side, json.dumps(sides[side], ensure_ascii=False)))

            fid = ((fixture or {}).get("fixture") or {}).get("id")
            if fid and extras is not None:
                cur.execute("""
                    INSERT INTO fixture_extras_store (fixture_id, data)
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE data=VALUES(data)
                """, (int(fid), json.dumps(extras, ensure_ascii=False)))

            conn.commit()
            conn.close()
        except Exception as e:
            print(f"[save_artifacts_for_fixture] warn: {e}")

# ---- PURGE 72h (možeš pozvati iz management skripte) ----
def purge_old_data():
    conn = get_mysql_connection()
    cur = conn.cursor()
    queries = [
        ("DELETE FROM fixtures              WHERE `date`     < NOW() - INTERVAL 72 HOUR", ()),
        ("DELETE FROM match_statistics      WHERE updated_at < NOW() - INTERVAL 72 HOUR", ()),
        ("DELETE FROM team_history_cache    WHERE updated_at < NOW() - INTERVAL 72 HOUR", ()),
        ("DELETE FROM h2h_cache             WHERE updated_at < NOW() - INTERVAL 72 HOUR", ()),
        ("DELETE FROM team_matches          WHERE updated_at < NOW() - INTERVAL 72 HOUR", ()),
        ("DELETE FROM h2h_matches           WHERE updated_at < NOW() - INTERVAL 72 HOUR", ()),
        ("DELETE FROM league_baselines_store WHERE updated_at < NOW() - INTERVAL 72 HOUR", ()),
        ("DELETE FROM team_strengths_store   WHERE updated_at < NOW() - INTERVAL 72 HOUR", ()),
        ("DELETE FROM team_profiles_store    WHERE updated_at < NOW() - INTERVAL 72 HOUR", ()),
        ("DELETE FROM team_micro_form_store  WHERE updated_at < NOW() - INTERVAL 72 HOUR", ()),
        ("DELETE FROM fixture_extras_store   WHERE updated_at < NOW() - INTERVAL 72 HOUR", ()),
        ("DELETE FROM venues_cache          WHERE updated_at < NOW() - INTERVAL 72 HOUR", ()),
        ("DELETE FROM lineups_cache         WHERE updated_at < NOW() - INTERVAL 72 HOUR", ()),
        ("DELETE FROM injuries_cache        WHERE updated_at < NOW() - INTERVAL 72 HOUR", ()),
        ("DELETE FROM referee_cache         WHERE updated_at < NOW() - INTERVAL 72 HOUR", ()),
        ("DELETE FROM odds_cache            WHERE updated_at < NOW() - INTERVAL 72 HOUR", ()),
        ("DELETE FROM team_stats_cache      WHERE updated_at < NOW() - INTERVAL 72 HOUR", ()),
    ]
    for q, p in queries:
        cur.execute(q, p)
    conn.commit()
    conn.close()