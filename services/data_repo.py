# services/data_repo.py
from __future__ import annotations

from datetime import datetime, date, timedelta, timezone
from typing import Dict, List, Tuple, Optional, Set
import json
import time
from functools import lru_cache

from database import (
    get_db_connection,
    DB_WRITE_LOCK,
    insert_team_matches,
    insert_h2h_matches,
    try_read_fixture_statistics,
    create_all_tables,
)

# Ako već imaš .api_client, koristi njega (isti backoff/retry kao u app-u)
try:
    from .api_client import rate_limited_request, SESSION  # noqa: F401
except Exception:
    # Fallback: minimalistički GET (bez retries) – koristi se samo ako .api_client ne postoji
    import requests
    API_KEY = '505703178f0eb0be24c37646ea9d06d9'
    HEADERS = {'x-apisports-key': API_KEY}
    SESSION = requests.Session()
    SESSION.headers.update(HEADERS)

    def rate_limited_request(url, params=None, max_retries=5, timeout=20):
        retries = 0
        while retries <= max_retries:
            try:
                response = SESSION.get(url, params=params, timeout=timeout)
            except requests.RequestException:
                time.sleep(2 ** retries)
                retries += 1
                continue
            if response.status_code == 200:
                return response.json()
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', '2'))
                time.sleep(retry_after + 0.8)
                retries += 1
            else:
                time.sleep(2 ** retries)
                retries += 1
        return None

from zoneinfo import ZoneInfo
USER_TZ = ZoneInfo("Europe/Sarajevo")

BASE_URL = "https://v3.football.api-sports.io"
CACHE_TTL_HOURS = 48
EXTRAS_TTL_HOURS = 48
BASE_SEASON_FALLBACK = datetime.utcnow().year  # za referee sezonu (ako nije prosleđeno)

class DataRepo:
    """
    Repo sloj: čita iz lokalne baze; ako fali i no_api=False → povuče sa API-ja, upiše u DB, pa vrati.
    Kompatibilno sa pozivima u app.py (ensure_day, get_team_history, get_h2h, get_fixture_stats, ...).
    """

    # ----------------- internal helpers -----------------
    def _day_bounds_utc(self, d: date) -> Tuple[datetime, datetime]:
        start = datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=timezone.utc)
        end   = start + timedelta(days=1) - timedelta(microseconds=1)
        return start, end

    def _db_has_fixtures_for_day(self, d: date) -> bool:
        s, e = self._day_bounds_utc(d)
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT 1 FROM fixtures WHERE date >= ? AND date <= ? LIMIT 1",
            (s.isoformat(), e.isoformat()),
        )
        row = cur.fetchone()
        conn.close()
        return row is not None

    def _read_fixtures_for_day(self, d: date) -> List[dict]:
        s, e = self._day_bounds_utc(d)
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT fixture_json FROM fixtures WHERE date >= ? AND date <= ?", (s.isoformat(), e.isoformat()))
        rows = cur.fetchall()
        conn.close()
        out = []
        for (j,) in rows:
            try:
                out.append(json.loads(j))
            except Exception:
                continue
        return out

    def _store_fixtures(self, fixtures: List[dict]) -> None:
        if not fixtures:
            return
        with DB_WRITE_LOCK:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("BEGIN IMMEDIATE;")
            for fixture in fixtures:
                try:
                    fid = fixture["fixture"]["id"]
                    fdt = fixture["fixture"]["date"]
                    lid = fixture["league"]["id"]
                    hid = fixture["teams"]["home"]["id"]
                    aid = fixture["teams"]["away"]["id"]

                    # NE pišemo nove statistike ovdje; samo čitamo ako već postoje
                    stats_json = None  # izbegni dupli izvor istine; čitaj isključivo iz match_statistics
                    fixture_json = json.dumps(fixture, ensure_ascii=False, default=str)

                    cur.execute("""
                        INSERT OR REPLACE INTO fixtures
                        (id, date, league_id, team_home_id, team_away_id, stats_json, fixture_json)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (fid, fdt, lid, hid, aid, stats_json, fixture_json))
                except Exception:
                    continue
            conn.commit()
            conn.close()

    # ----------------- PUBLIC: ensure_day -----------------
    def ensure_day(self, d: date, last_n: int = 15, h2h_n: int = 10, prewarm_stats: bool = False) -> dict:
        """
        Idempotentno:
        - ako nema fixtures za dan → povuci i upiši,
        - osiguraj history & H2H keševe,
        - opciono pre-warm statistike i 1H kvote za istorijske/mečeve dana.
        """
        create_all_tables()

        seeded = False
        if not self._db_has_fixtures_for_day(d):
            resp = rate_limited_request(f"{BASE_URL}/fixtures", params={"date": d.isoformat(), "timezone": "UTC"})
            fixtures_raw = (resp or {}).get("response") or []
            self._store_fixtures(fixtures_raw)
            seeded = True

        fixtures = self._read_fixtures_for_day(d)

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

        # Ensure history/H2H caches
        all_team_matches = {}
        for tid in team_ids:
            all_team_matches[tid] = self.get_team_history(tid, last_n=last_n, no_api=False)

        for (a, b) in pairs:
            _ = self.get_h2h(a, b, last_n=h2h_n, no_api=False)

        # Optional: prewarm stats for historical matches (not just today's fixtures)
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

        # NEW: pre-warm 1H odds (market prior)
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

        return {
            "seeded": seeded,
            "fixtures": len(fixtures),
            "teams": len(team_ids),
            "pairs": len(pairs),
            "stats_warmed": stats_warmed,
            "odds_warmed": odds_warmed,  # <-- novo polje u povratku
        }

    # ----------------- PUBLIC: history / h2h -----------------
    def get_team_history(self, team_id: int, last_n: int = 15, no_api: bool = False) -> List[dict]:
        conn = get_db_connection()
        cur = conn.cursor()
        with DB_WRITE_LOCK:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS team_history_cache (
                    team_id INTEGER,
                    last_n INTEGER,
                    data TEXT,
                    updated_at TEXT,
                    PRIMARY KEY (team_id, last_n)
                )
            """)
            conn.commit()

        now = datetime.utcnow()
        if True:
            cur.execute("SELECT data, updated_at FROM team_history_cache WHERE team_id=? AND last_n=?", (team_id, last_n))
            row = cur.fetchone()
            if row:
                try:
                    updated_at = datetime.fromisoformat(row["updated_at"])
                except Exception:
                    updated_at = now - timedelta(hours=CACHE_TTL_HOURS+1)
                if (now - updated_at) <= timedelta(hours=CACHE_TTL_HOURS) or no_api:
                    conn.close()
                    try:
                        return (json.loads(row["data"]) or [])[:last_n]
                    except Exception:
                        return []

            # superset fallback (najveći last_n)
            cur.execute("""
                SELECT data, updated_at, last_n FROM team_history_cache
                WHERE team_id=? ORDER BY last_n DESC LIMIT 1
            """, (team_id,))
            row2 = cur.fetchone()
            if row2:
                try:
                    updated_at2 = datetime.fromisoformat(row2["updated_at"])
                except Exception:
                    updated_at2 = now - timedelta(hours=CACHE_TTL_HOURS+1)
                have_n = row2["last_n"] or 0
                if have_n >= last_n and ((now - updated_at2) <= timedelta(hours=CACHE_TTL_HOURS) or no_api):
                    conn.close()
                    try:
                        return (json.loads(row2["data"]) or [])[:last_n]
                    except Exception:
                        return []

        if no_api:
            conn.close()
            return []

        resp = rate_limited_request(f"{BASE_URL}/fixtures", params={'team': team_id, 'last': last_n, 'timezone': 'UTC'})
        data = resp.get('response', []) if resp else []

        with DB_WRITE_LOCK:
            cur.execute("""
                INSERT OR REPLACE INTO team_history_cache(team_id,last_n,data,updated_at)
                VALUES(?,?,?,?)
            """, (team_id, last_n, json.dumps(data, ensure_ascii=False), now.isoformat()))
            conn.commit()
        conn.close()
        return data

    def get_h2h(self, team_a: int, team_b: int, last_n: int = 10, no_api: bool = False) -> List[dict]:
        a, b = sorted([team_a, team_b])
        conn = get_db_connection()
        cur = conn.cursor()
        with DB_WRITE_LOCK:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS h2h_cache (
                    team1_id INTEGER,
                    team2_id INTEGER,
                    last_n INTEGER,
                    data TEXT,
                    updated_at TEXT,
                    PRIMARY KEY (team1_id, team2_id, last_n)
                )
            """)
            conn.commit()

        now = datetime.utcnow()
        cur.execute("SELECT data, updated_at FROM h2h_cache WHERE team1_id=? AND team2_id=? AND last_n=?", (a,b,last_n))
        row = cur.fetchone()
        if row:
            try:
                updated_at = datetime.fromisoformat(row["updated_at"])
            except Exception:
                updated_at = now - timedelta(hours=CACHE_TTL_HOURS+1)
            if (now - updated_at) <= timedelta(hours=CACHE_TTL_HOURS) or no_api:
                conn.close()
                try:
                    return json.loads(row["data"]) or []
                except Exception:
                    return []

        if no_api:
            conn.close()
            return []

        h2h_key = f"{a}-{b}"
        resp = rate_limited_request(f"{BASE_URL}/fixtures/headtohead", params={'h2h': h2h_key, 'last': last_n})
        data = resp.get('response', []) if resp else []

        with DB_WRITE_LOCK:
            cur.execute("""
                INSERT OR REPLACE INTO h2h_cache(team1_id,team2_id,last_n,data,updated_at)
                VALUES(?,?,?,?,?)
            """, (a,b,last_n,json.dumps(data, ensure_ascii=False), now.isoformat()))
            conn.commit()
        conn.close()
        return data

    # ----------------- PUBLIC: stats -----------------
    def get_fixture_stats(self, fixture_id: int, no_api: bool = False) -> Optional[list]:
        """
        Vrati match statistics iz lokalne baze; ako fale i no_api=False → povuci sa API-ja i upiši.
        """
        existing = try_read_fixture_statistics(fixture_id)
        if existing is not None or no_api:
            return existing

        response = rate_limited_request(f"{BASE_URL}/fixtures/statistics", params={"fixture": fixture_id})
        stats = (response or {}).get('response') or None

        if stats is not None:
            with DB_WRITE_LOCK:
                conn = get_db_connection()
                cur = conn.cursor()
                cur.execute("BEGIN IMMEDIATE;")
                cur.execute("""
                    INSERT OR REPLACE INTO match_statistics(fixture_id, data, updated_at)
                    VALUES (?, ?, datetime('now'))
                """, (fixture_id, json.dumps(stats, ensure_ascii=False, default=str)))
                conn.commit()
                conn.close()
        return stats

    # ----------------- PUBLIC: fixture / venue / lineups / injuries -----------------
    def get_fixture_full(self, fixture_id: int, no_api: bool = False) -> Optional[dict]:
        """
        Vraća kompletan fixture JSON iz tabele fixtures; ako ga nema i no_api=False → povuci po id-u.
        """
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT fixture_json FROM fixtures WHERE id=?", (fixture_id,))
        row = cur.fetchone()
        conn.close()
        if row and row[0]:
            try:
                return json.loads(row[0])
            except Exception:
                pass

        if no_api:
            return None

        # Fetch by id i upiši
        resp = rate_limited_request(f"{BASE_URL}/fixtures", params={"id": fixture_id})
        arr = (resp or {}).get("response") or []
        fx = arr[0] if arr else None
        if fx:
            self._store_fixtures([fx])
        return fx

    def _cache_table(self, name: str) -> None:
        """
        Kreira pomoćne keš tabele ako ne postoje (venues_cache, lineups_cache, injuries_cache, referee_cache).
        """
        with DB_WRITE_LOCK:
            conn = get_db_connection()
            cur = conn.cursor()
            if name == "venues_cache":
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS venues_cache (
                        venue_id INTEGER PRIMARY KEY,
                        data TEXT,
                        updated_at TEXT
                    )
                """)
            elif name == "lineups_cache":
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS lineups_cache (
                        fixture_id INTEGER PRIMARY KEY,
                        data TEXT,
                        updated_at TEXT
                    )
                """)
            elif name == "injuries_cache":
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS injuries_cache (
                        fixture_id INTEGER PRIMARY KEY,
                        data TEXT,
                        updated_at TEXT
                    )
                """)
            elif name == "referee_cache":
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS referee_cache (
                        ref_name TEXT,
                        season INTEGER,
                        last_n INTEGER,
                        data TEXT,
                        updated_at TEXT,
                        PRIMARY KEY (ref_name, season, last_n)
                    )
                """)
            elif name == "odds_cache":
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS odds_cache (
                        fixture_id INTEGER,
                        market TEXT,
                        data TEXT,
                        updated_at TEXT,
                        PRIMARY KEY (fixture_id, market)
                    )
                """)

            conn.commit()
            conn.close()

    def get_venue(self, venue_id: Optional[int], no_api: bool = False) -> Optional[dict]:
        if not venue_id:
            return None
        self._cache_table("venues_cache")

        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT data, updated_at FROM venues_cache WHERE venue_id=?", (venue_id,))
        row = cur.fetchone()
        conn.close()

        now = datetime.utcnow()
        if row:
            try:
                updated_at = datetime.fromisoformat(row["updated_at"])
            except Exception:
                updated_at = now - timedelta(hours=EXTRAS_TTL_HOURS+1)
            if (now - updated_at) <= timedelta(hours=EXTRAS_TTL_HOURS) or no_api:
                try:
                    return json.loads(row["data"])
                except Exception:
                    return None

        if no_api:
            return None

        resp = rate_limited_request(f"{BASE_URL}/venues", params={"id": venue_id})
        arr = (resp or {}).get("response") or []
        ven = arr[0] if arr else None

        with DB_WRITE_LOCK:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("""
                INSERT OR REPLACE INTO venues_cache(venue_id, data, updated_at)
                VALUES(?,?,?)
            """, (venue_id, json.dumps(ven, ensure_ascii=False), now.isoformat()))
            conn.commit()
            conn.close()
        return ven

    def get_lineups(self, fixture_id: int, no_api: bool = False) -> Optional[list]:
        self._cache_table("lineups_cache")

        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT data, updated_at FROM lineups_cache WHERE fixture_id=?", (fixture_id,))
        row = cur.fetchone()
        conn.close()

        now = datetime.utcnow()
        if row:
            try:
                updated_at = datetime.fromisoformat(row["updated_at"])
            except Exception:
                updated_at = now - timedelta(hours=EXTRAS_TTL_HOURS+1)
            if (now - updated_at) <= timedelta(hours=EXTRAS_TTL_HOURS) or no_api:
                try:
                    return json.loads(row["data"])
                except Exception:
                    return None

        if no_api:
            return None

        resp = rate_limited_request(f"{BASE_URL}/fixtures/lineups", params={"fixture": fixture_id})
        arr = (resp or {}).get("response") or []

        with DB_WRITE_LOCK:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("""
                INSERT OR REPLACE INTO lineups_cache(fixture_id, data, updated_at)
                VALUES(?,?,?)
            """, (fixture_id, json.dumps(arr, ensure_ascii=False), now.isoformat()))
            conn.commit()
            conn.close()
        return arr

    def get_injuries(self, fixture_id: int, no_api: bool = False) -> Optional[list]:
        self._cache_table("injuries_cache")

        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT data, updated_at FROM injuries_cache WHERE fixture_id=?", (fixture_id,))
        row = cur.fetchone()
        conn.close()

        now = datetime.utcnow()
        if row:
            try:
                updated_at = datetime.fromisoformat(row["updated_at"])
            except Exception:
                updated_at = now - timedelta(hours=EXTRAS_TTL_HOURS+1)
            if (now - updated_at) <= timedelta(hours=EXTRAS_TTL_HOURS) or no_api:
                try:
                    return json.loads(row["data"])
                except Exception:
                    return None

        if no_api:
            return None

        resp = rate_limited_request(f"{BASE_URL}/injuries", params={"fixture": fixture_id})
        arr = (resp or {}).get("response") or []

        with DB_WRITE_LOCK:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("""
                INSERT OR REPLACE INTO injuries_cache(fixture_id, data, updated_at)
                VALUES(?,?,?)
            """, (fixture_id, json.dumps(arr, ensure_ascii=False), now.isoformat()))
            conn.commit()
            conn.close()
        return arr

    # ----------------- PUBLIC: referee fixtures (best-effort) -----------------
    def get_referee_fixtures(self, ref_name: str, season: Optional[int] = None, last_n: int = 200, no_api: bool = False) -> List[dict]:
        """
        Best-effort: API-Football može (zavisno od plana) podržati filter 'referee' na /fixtures.
        Ako API ne vrati rezultat ili endpoint ne postoji → vrati [] (neutralni REF profile).
        Keširamo po (ref_name, season, last_n).
        """
        if not ref_name:
            return []

        self._cache_table("referee_cache")
        year = season or BASE_SEASON_FALLBACK

        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT data, updated_at FROM referee_cache WHERE ref_name=? AND season=? AND last_n=?", (ref_name, year, last_n))
        row = cur.fetchone()
        conn.close()

        now = datetime.utcnow()
        if row:
            try:
                updated_at = datetime.fromisoformat(row["updated_at"])
            except Exception:
                updated_at = now - timedelta(hours=EXTRAS_TTL_HOURS+1)
            if (now - updated_at) <= timedelta(hours=EXTRAS_TTL_HOURS) or no_api:
                try:
                    return json.loads(row["data"]) or []
                except Exception:
                    return []

        if no_api:
            return []

        # Pokušaj direktno preko API-ja (ako ne podržava 'referee', vratiće prazno → neutralno)
        params = {"season": year, "last": last_n, "timezone": "UTC", "referee": ref_name}
        resp = rate_limited_request(f"{BASE_URL}/fixtures", params=params)
        arr = (resp or {}).get("response") or []

        with DB_WRITE_LOCK:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("""
                INSERT OR REPLACE INTO referee_cache(ref_name, season, last_n, data, updated_at)
                VALUES(?,?,?,?,?)
            """, (ref_name, year, last_n, json.dumps(arr, ensure_ascii=False), now.isoformat()))
            conn.commit()
            conn.close()
        return arr
    # ----------------- PUBLIC: odds (market-implied) -----------------
    def get_odds_1h(self, fixture_id: int, no_api: bool = False) -> Optional[dict]:
        """
        Vraća keširane kvote za 1H tržišta (OU i BTTS 1H) ili ih povlači sa API-ja i kešira.
        Struktura povratne vrednosti:
        {
        "markets": {
            "OU_1H": { "over_0_5": 1.72, "under_0_5": 2.15, "over_1_5": 2.90, "under_1_5": 1.38 },
            "BTTS_1H": { "yes": 3.80, "no": 1.25 }
        },
        "raw": [...],             # sirov API odgovor (po želji)
        "updated_at": "..."
        }
        """
        self._cache_table("odds_cache")

        # 1) Probaj iz baze
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT data, updated_at FROM odds_cache WHERE fixture_id=? AND market=?", (fixture_id, "ALL_1H"))
        row = cur.fetchone()
        conn.close()

        now = datetime.utcnow()
        if row:
            try:
                updated_at = datetime.fromisoformat(row["updated_at"])
            except Exception:
                updated_at = now - timedelta(hours=EXTRAS_TTL_HOURS+1)
            if (now - updated_at) <= timedelta(hours=EXTRAS_TTL_HOURS) or no_api:
                try:
                    cached = json.loads(row["data"]) or {}
                    return cached
                except Exception:
                    if no_api:
                        return None

        if no_api:
            return None

        # 2) Povuci sa API-ja (API-Football: /odds?fixture=<id>)
        resp = rate_limited_request(f"{BASE_URL}/odds", params={"fixture": fixture_id})
        arr = (resp or {}).get("response") or []

        # 3) Parsiranje – imena marketa variraju po bukmejkeru; tražimo 1H OU i BTTS
        def normalize_markets(rows):
            out = {"OU_1H": {}, "BTTS_1H": {}}
            for r in rows:
                for bk in (r.get("bookmakers") or []):
                    for mkt in (bk.get("bets") or []):
                        name = (mkt.get("name") or "").lower()
                        # over/under 1st half
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
                        # btts 1st half
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

        with DB_WRITE_LOCK:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("""
                INSERT OR REPLACE INTO odds_cache(fixture_id, market, data, updated_at)
                VALUES(?,?,?,?)
            """, (fixture_id, "ALL_1H", json.dumps(payload, ensure_ascii=False), now.isoformat()))
            conn.commit()
            conn.close()

        return payload
