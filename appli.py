import requests
import time
import random
from datetime import datetime, timedelta, timezone, date
from fastapi import FastAPI, Query, Request, HTTPException, Body
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional
from fastapi.staticfiles import StaticFiles
import json
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
import unicodedata
from fastapi.responses import JSONResponse
import traceback
import re
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache
import threading
from services.data_repo import DataRepo
from services.scheduler import start_scheduler

app = FastAPI()
repo = DataRepo()

@app.on_event("startup")
def _init_on_startup():
    create_all_tables()
    _refresh_league_whitelist(force=True)

    # index radi brzine
    with DB_WRITE_LOCK:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("CREATE INDEX IF NOT EXISTS idx_fixtures_date ON fixtures(date)")
        conn.commit()
        conn.close()

    # inicijalni ensure dana + warm stats (ako može)
    try:
        repo.ensure_day(
            datetime.now(USER_TZ).date(),
            last_n=DAY_PREFETCH_LAST_N,
            h2h_n=DAY_PREFETCH_H2H_N,
            prewarm_stats=True
        )
    except Exception as e:
        print(f"initial ensure_day failed: {e}")

    # pokreni JEDAN scheduler iz services/scheduler.py
    start_scheduler(
        repo, USER_TZ,
        last_n=DAY_PREFETCH_LAST_N,
        h2h_n=DAY_PREFETCH_H2H_N
    )

ACTIVE_MARKETS = {"1h_over05", "gg1h", "1h_over15"}

# Koliko istorije i H2H nam treba da bi analize radile bez API-ja
DAY_PREFETCH_LAST_N = 15
DAY_PREFETCH_H2H_N  = 10

# --- NEW: conversion priors & weights ---
FINISH_PRIOR_1H = 0.34   # ~g/SoT u 1. poluvremenu (emp. prior; možeš mijenjati)
LEAK_PRIOR_1H   = 0.34   # simetričan prior za primanje gola po SoT-u rivala
REST_REF_DAYS   = 5.0    # neutralni odmor

WEIGHTS = {
    "BIAS":   0.00,
    "Z_SOT":  0.55,
    "Z_DA":   0.22,
    "POS":    0.18,
    "FIN":    0.38,
    "LEAK":   0.32,
    "REST":   0.06,
    "HOME":   0.05,
    "ATT":    0.25,
    "DEF":    0.25,

    # Multiplikativni faktori (ulaze kao ln(mult))
    "REFEREE_MULT": 0.06,
    "WEATHER_MULT": 0.04,
    "LINEUP_MULT":  0.08,
    "PENVAR_MULT":  0.03,
    "STADIUM_MULT": 0.02,

    # Logit (globalni) adj-ovi — aditivno
    "REF":         0.08,
    "ENV_WEATHER": 0.05,  # ← bilo "WEATHER", preimenovano da ne sudara sa *_MULT
    "VENUE":       0.03,
    "LINEUPS":     0.05,
    "INJ":         0.05,

    # novi z-score / normalized signali
    "Z_SHOTS":   0.18,   # total shots 1H (po timu)
    "Z_XG":      0.28,   # xG 1H (po timu)
    "Z_BIGCH":   0.14,   # big chances 1H (po timu)
    "SETP":      0.10,   # set-piece xG (proxy) 1H (ukupno)
    "GK":        0.08,   # shot-stopping (save rate) proxy uticaj (suprotno LEAK-u)
    "CONGEST":   0.06,   # zagušenje rasporeda (− kad je gusto)
    "IMPORTANCE":0.05,   # važnost meča (global logit adj)
}


CALIBRATION = {
    "TEMP": 1.05,     # temperature scaling na kraju (veće → stišavanje)
    "FLOOR": 0.02,    # pod
    "CEIL":  0.98,    # plafon
}

ALPHA_MODEL = 0.7

# --- NEW: tier/class gap ---
WEIGHTS["TIER_GAP"] = 0.35   # uticaj razlike ranga (pozitivno -> jači tim)
CUP_TIER_MULT = 1.35         # u kupovima pojačaj uticaj class-gapa
DEFAULT_TEAM_TIER = 2        # ako ne znamo, tretiraj kao 2. nivo
MAX_TIER_GAP = 3             # clamp [-3, +3]

# --- NEW caps/proxies for 1H ---
SHOTS1H_CAP = 10.0       # total shots per team in 1H (proxy cap)
XG1H_CAP    = 1.20       # xG per team in 1H (proxy cap)
BIGCH1H_CAP = 3.0        # big chances per team in 1H (proxy cap)
CORN1H_CAP  = 6.0        # corners per team in 1H
FK1H_CAP    = 8.0        # free-kicks per team in 1H

# set-piece xG proxy (tunable)
SETP_XG_PER_CORNER_1H = 0.025
SETP_XG_PER_FK_1H     = 0.010

# GK shot-stopping (save%) prior in 1H i EB tau
GK_SAVE_PRIOR_1H = 0.70
GK_SAVE_TAU      = 8.0


def _rel(n, k=6.0):
    """Reliability factor in [0,1] ~ n/(n+k). k=6 je dobar default za 'koliko nam treba mečeva'."""
    try:
        n = float(n)
    except Exception:
        return 0.0
    return n / (n + float(k))

def _cap_league_per_team(x, base_q95_total, fallback_cap_per_team):
    """Cap per-team očekivanja koristeći league q95 (total/2) ili fallback."""
    if x is None:
        return None
    cap = (base_q95_total/2.0) if (base_q95_total is not None) else float(fallback_cap_per_team)
    return max(0.0, min(float(cap), float(x)))

def _ht_goals_for_against(m, team_id):
    teams = (m.get('teams') or {})
    ht = ((m.get('score') or {}).get('halftime') or {})
    hid = ((teams.get('home') or {}).get('id'))
    aid = ((teams.get('away') or {}).get('id'))
    if not ht or (hid is None) or (aid is None):
        return 0.0, 0.0
    if team_id == hid:
        return float(ht.get('home') or 0), float(ht.get('away') or 0)
    if team_id == aid:
        return float(ht.get('away') or 0), float(ht.get('home') or 0)
    return 0.0, 0.0

def _safe_div(a, b, default=0.0):
    try:
        b = float(b)
        if b <= 0: return default
        return float(a)/b
    except Exception:
        return default

def _geomean(x, y):
    vals = [v for v in (x, y) if v is not None]
    if len(vals) == 2 and vals[0] > 0 and vals[1] > 0:
        return (vals[0]*vals[1])**0.5
    if len(vals) == 1:
        return vals[0]
    return None

def _calibrate(p, temp=1.0, floor=0.0, ceil=1.0):
    # temperature scaling na logit + clamp
    z = _logit(min(1-1e-9, max(1e-9, p)))
    q = _inv_logit(z / max(1e-6, temp))
    return max(floor, min(ceil, q))

def _day_bounds_utc(d: date):
    start = datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=timezone.utc)
    end   = start + timedelta(days=1) - timedelta(microseconds=1)
    return start, end

def _db_has_fixtures_for_day(d: date) -> bool:
    s, e = _day_bounds_utc(d)
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM fixtures WHERE date>=? AND date<=? LIMIT 1",
                (s.isoformat(), e.isoformat()))
    row = cur.fetchone()
    conn.close()
    return row is not None

def _read_fixtures_for_day(d: date):
    s, e = _day_bounds_utc(d)
    return _read_fixtures_from_db(s, e)

try:
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
    try:
        USER_TZ = ZoneInfo("Europe/Sarajevo")
    except ZoneInfoNotFoundError:
        USER_TZ = timezone.utc
except Exception:
    USER_TZ = timezone.utc


API_KEY = '505703178f0eb0be24c37646ea9d06d9'
BASE_URL = 'https://v3.football.api-sports.io'
HEADERS = {'x-apisports-key': API_KEY}

ANALYZE_LOCK = threading.Lock()

from database import (
    get_db_connection,
    insert_team_matches,
    insert_h2h_matches,
    DB_WRITE_LOCK,                 # ✅ sad postoji
    try_read_fixture_statistics,   # ✅ samo čitanje
    create_all_tables,             # ili create_tables()
)

@app.post("/admin/seed-day")
def admin_seed_day(date_str: str | None = None):
    d = datetime.now(USER_TZ).date() if not date_str else date.fromisoformat(date_str)
    res = seed_day_into_db(d)
    return JSONResponse(content={"ok": True, "day": d.isoformat(), **(res or {})})

@app.post("/admin/ensure-today")
def admin_ensure_today():
    d = datetime.now(USER_TZ).date()
    if not _db_has_fixtures_for_day(d):
        res = seed_day_into_db(d)
    else:
        fx = _read_fixtures_for_day(d)
        fetch_and_store_all_historical_data(fx, no_api=False)
        prewarm_statistics_cache(fetch_last_matches_for_teams(fx, last_n=DAY_PREFETCH_LAST_N, no_api=False))
        res = {"fixtures": len(fx)}
    return JSONResponse(content={"ok": True, "day": d.isoformat(), **(res or {})})

@app.post("/admin/purge-yesterday")
def admin_purge_yesterday():
    d = datetime.now(USER_TZ).date() - timedelta(days=1)
    purge_fixtures_for_day(d)
    return JSONResponse(content={"ok": True, "purged_day": d.isoformat()})

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
FRONTEND_DIR = BASE_DIR / "FRONTEND"
DEBUG_FILTER = True

FIXTURES_FETCH_SOURCE = "api"

def _fetch_fixtures_api_for_day(d: date):
    # raw fixtures za konkretan datum (UTC), bez filtera
    resp = rate_limited_request(f"{BASE_URL}/fixtures",
                                params={"date": d.isoformat(), "timezone": "UTC"})
    return (resp or {}).get("response") or []

def seed_day_into_db(d: date):
    """
    1) Povuci sve fixtures za dan d sa API-ja i upiši u DB (RAW).
    2) Iz tih fixtures izračunaj skup timova/parova i popuni:
       - last matches (DAY_PREFETCH_LAST_N) + upis u team_history_cache i team_matches tabelu
       - H2H (DAY_PREFETCH_H2H_N) + upis u h2h_cache i h2h tabelu
       - prewarm stats (match_statistics) za ISTORIJSKE mečeve
    """
    fixtures_raw = _fetch_fixtures_api_for_day(d)
    if not fixtures_raw:
        print(f"⚠️ seed_day_into_db: nema fixtures za {d}")
        return {"fixtures": 0, "teams": 0, "pairs": 0, "stats_warmed": 0}

    # 1) upiši fixtures u DB (RAW)
    store_fixture_data_in_db(fixtures_raw)

    # 2) istorija/h2h/statistike za analize
    #    (iskoristi postojeći pipeline koji i upisuje u DB)
    all_team_matches, h2h_map = fetch_and_store_all_historical_data(fixtures_raw, no_api=False)

    # 3) prewarm statistike (ako fetch_and_store nije već pozvao – on poziva kad no_api=False)
    #    zadržavamo ovu liniju kao "osiguranje"
    prewarm_statistics_cache(all_team_matches, max_workers=2)

    # countovi (grubo)
    team_count = len(all_team_matches or {})
    pair_count = len(h2h_map or {})
    print(f"✅ SEED DONE {d}: fixtures={len(fixtures_raw)} teams={team_count} pairs={pair_count}")
    return {"fixtures": len(fixtures_raw), "teams": team_count, "pairs": pair_count}

def purge_fixtures_for_day(d: date):
    s, e = _day_bounds_utc(d)
    with DB_WRITE_LOCK:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("DELETE FROM fixtures WHERE date>=? AND date<=?", (s.isoformat(), e.isoformat()))
        conn.commit()
        conn.close()
    print(f"🧹 purged fixtures for {d}")

# def daily_maintenance_and_seed():
#     # lokalno vreme → današnji dan po Sarajevu
#     now_local = datetime.now(USER_TZ)
#     today_local = now_local.date()

#     # juče po lokalnom
#     yesterday_local = today_local - timedelta(days=1)

#     # 1) purge jučerašnjih fixtures (ne diramo istorijske cacheve!)
#     purge_fixtures_for_day(yesterday_local)

#     # 2) seed za današnji dan (ako već nije)
#     if not _db_has_fixtures_for_day(today_local):
#         seed_day_into_db(today_local)
#     else:
#         # već imamo fixtures; ipak proveri da li ima smisla osvežiti istoriju/stat keš
#         fx = _read_fixtures_for_day(today_local)
#         fetch_and_store_all_historical_data(fx, no_api=False)
#         prewarm_statistics_cache(fetch_last_matches_for_teams(fx, last_n=DAY_PREFETCH_LAST_N, no_api=False))
#         print(f"ℹ️ fixtures za {today_local} su već u DB – osvežen keš istorije/stat.")

# def _seconds_until_next_0001_local():
#     now = datetime.now(USER_TZ)
#     target = now.replace(hour=0, minute=1, second=0, microsecond=0)
#     if now >= target:
#         target = target + timedelta(days=1)
#     return (target - now).total_seconds()

# def _scheduler_loop():
#     while True:
#         try:
#             sleep_sec = _seconds_until_next_0001_local()
#             print(f"⏰ scheduler sleeping {sleep_sec:.0f}s until next 00:01 local...")
#             time.sleep(max(1, sleep_sec))
#             daily_maintenance_and_seed()
#         except Exception as e:
#             print(f"scheduler error: {e}")
#             time.sleep(30)

# def _maybe_seed_today_now_if_empty():
#     # ako je već prošlo 00:01, a današnji fixtures nisu u bazi → odmah seed
#     now = datetime.now(USER_TZ)
#     if now.hour > 0 or (now.hour == 0 and now.minute >= 1):
#         if not _db_has_fixtures_for_day(now.date()):
#             print("⚡ Nema današnjih fixtures u DB a prošlo je 00:01 → radim seed odmah.")
#             seed_day_into_db(now.date())

REJECTED_COMP_COUNTER = {}

def _bump_reject_counter(fixture, reason: str):
    if not DEBUG_FILTER:
        return
    lg = (fixture.get("league") or {})
    key = f"{_norm(lg.get('country'))} | {lg.get('name')} | type={lg.get('type')} | season={lg.get('season')}"
    REJECTED_COMP_COUNTER[key] = REJECTED_COMP_COUNTER.get(key, 0) + 1
    _dbg_comp_reject(fixture, reason)

def _dbg_time_reject(match, reason, start_dt, end_dt, from_hour, to_hour):
    if not DEBUG_FILTER: return
    fix = match.get("fixture", {}) or {}
    print(f"⛔ TIME REJECT: id={fix.get('id')} dt={fix.get('date')} | range=[{start_dt}..{end_dt}] | hours={from_hour}->{to_hour} | {reason}")

def _dbg_comp_reject(fixture, reason):
    if not DEBUG_FILTER: return
    lg = fixture.get("league", {}) or {}
    print(f"⛔ COMP REJECT: {lg.get('name')} | {lg.get('country')} | {reason}")

app.mount("/static", StaticFiles(directory=str(FRONTEND_DIR)), name="static")

@app.get("/")
def serve_home():
    index_path = FRONTEND_DIR / "index.html"
    if not index_path.exists():
        return {"error": f"index.html not found at {index_path}"}
    return FileResponse(str(index_path))


# CORS – da frontend može da pristupi backendu
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:8001",
        "http://127.0.0.1:8001"   # ← bez “/”
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ----------------------------- RATE LIMIT -----------------------------
SESSION = requests.Session()
SESSION.headers.update(HEADERS)

# Robustniji HTTP adapter sa retry/backoff (pored našeg rate_limited_request)
from requests.adapters import HTTPAdapter
try:
    # urllib3<2 i >=2 imaju različite puteve; ovaj radi i za nova izdanja
    from urllib3.util.retry import Retry
except Exception:
    Retry = None

if Retry is not None:
    retry = Retry(
        total=5, connect=5, read=5,
        backoff_factor=1.2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET"])
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
    SESSION.mount("http://", adapter)
    SESSION.mount("https://", adapter)


def rate_limited_request(url, params=None, max_retries=5, timeout=20):
    retries = 0
    while retries <= max_retries:
        try:
            response = SESSION.get(url, params=params, timeout=timeout)
        except requests.RequestException as e:
            print(f"Request error: {e}. Retrying...")
            time.sleep(2 ** retries)
            retries += 1
            continue

        if response.status_code == 200:
            return response.json()
        elif response.status_code == 429:
            retry_after = int(response.headers.get('Retry-After', '2'))
            backoff = retry_after + random.uniform(0.5, 1.5)
            print(f"Rate limit hit. Waiting {backoff:.2f}s before retry...")
            time.sleep(backoff)
            retries += 1
        else:
            print(f"Request failed with status {response.status_code}. Retrying...")
            time.sleep(2 ** retries)
            retries += 1
    print(f"Failed after {max_retries} retries.")
    return None

# -------------------------- FILTERING METHODS --------------------------
def is_fixture_in_range(fixture_datetime_str, start_dt, end_dt, from_hour=None, to_hour=None):
    dt_utc = datetime.fromisoformat(fixture_datetime_str.replace("Z", "+00:00"))  # UTC-aware

    # Bez satnog filtra -> striktno [start_dt, end_dt] u UTC
    if from_hour is None or to_hour is None:
        return start_dt <= dt_utc <= end_dt

    # Sa satnim filtrom -> poredi DATUME u LOKALNOJ zoni + sat u LOKALNOJ zoni
    dt_local    = dt_utc.astimezone(USER_TZ)
    start_local = start_dt.astimezone(USER_TZ)
    end_local   = end_dt.astimezone(USER_TZ)

    if not (start_local.date() <= dt_local.date() <= end_local.date()):
        return False

    return from_hour <= dt_local.hour < to_hour

def _norm(s: str) -> str:
    s = (s or "").lower().strip()
    # (opciono) skini akcentе da "segunda división" == "segunda division"
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    return s

def _is_youth_or_reserve_comp_name(name: str) -> bool:
    # koristi sirovo ime lige (sa zagradama) – mi ćemo normalizovati unutra
    s = " " + _norm(name).replace("-", " ") + " "
    return any(tok in s for tok in YOUTH_RESERVE_TOKENS)

def _is_reserve_team(name: str) -> bool:
    s = " " + _norm(name).replace("-", " ") + " "
    return any(tok in s for tok in [
        " ii ", " b ",
        " u17 ", " u18 ", " u19 ", " u20 ", " u21 ",
        " youth ", " juniors ", " junior ", " academy ",
        " reserves ", " reserve "
    ])
# ---- KONSTANTE (minimalno potrebne) ----
CONTINENTAL_KW = [
    "champions league","europa league","conference league",
    "libertadores","sudamericana",
    "afc champions","caf champions","concacaf champions","concacaf champions cup","ofc champions",
    "uefa super cup"
]

NATIONAL_TEAM_KW = [
    "world cup","euro","uefa nations","qualif","qualification",
    "african cup","asian cup","conmebol qualifiers","concacaf nations","friendlies"
]

INTERNATIONAL_COUNTRIES = [
    "world","europe","asia","africa","south america","north america","international"
]

CUP_KW = [
    " cup"," taca"," taça"," pokal"," coppa"," copa"," coupe"," kupa"," king's cup"," emperors cup"," fa cup"
]
LOW_TIER_CUP_EXCLUDE = ["primavera","u19","u20","u21","u23","youth","reserves","regional"]

# Detekcija nivoa lige po nazivu (bez lokalnih reči tipa "liga" na srp/bs/hr)
TOP_LEVEL_TOKENS = [
    "premier","primeira","primera","serie a","bundesliga","ligue 1",
    "ekstraklasa","eredivisie","allsvenskan","eliteserien",
    "super lig","superliga","super league","hnl","mls","a-league"
]
SECOND_LEVEL_TOKENS = [
    "championship","ligue 2","serie b","2. bundesliga","segunda","liga 2",
    "eerste divisie","superettan","1. divisjon","obos-ligaen",
    "1st division","1. division","i liga","1. liga","challenger pro league",
    "first league",          # ⇐ ARMENIA 2. rang i još par zemalja
    "pro league"             # ⇐ Neke zemlje 2. rang; ako Belgija top padne u 2, i dalje je OK (1/2 nam je svejedno)
]
# Očigledni indikatori 3. ranga i niže
LEVEL3_PLUS_TOKENS = [
    "ii liga","2. liga","third","3. division","iii liga","liga 3",
    "regionalliga","oberliga","girone","isthmian","npl"
    # "national league"  ⇐ maknuto da ne izbacujemo ligu ako negde nije 5. nivo
]
YOUTH_RESERVE_TOKENS = [
    " u17 ", " u18 ", " u19 ", " u20 ", " u21 ",
    " youth ", " junior ", " juniors ", " academy ",
    " primavera ", " reserves ", " reserve "
]
ROMAN_LEVEL = {"i":1,"ii":2,"iii":3,"iv":4}

def _is_cup(lname: str) -> bool:
    s = _norm(lname)
    # taça -> taca dolazi već iz _norm
    return re.search(r"\b(cup|taca|pokal|coppa|copa|coupe|kupa|king's cup|emperors cup|fa cup)\b", s) is not None

def _is_international(lname: str, country_norm: str) -> bool:
    return country_norm in INTERNATIONAL_COUNTRIES or any(k in lname for k in NATIONAL_TEAM_KW)

def _infer_level_from_name(name: str, _peer_names=None) -> int|None:
    s = _norm(name)
    # eksplicitni tokeni
    if any(tok in s for tok in TOP_LEVEL_TOKENS):
        return 1
    if any(tok in s for tok in SECOND_LEVEL_TOKENS):
        return 2
    # rimski/aritmetički oblici (npr. "Division I", "Division II")
    # NE koristi lokalne reči; hvatamo samo sufikse I/II/III/IV ili " 2." uobičajene formate
    parts = s.replace(".", " ").split()
    for p in parts:
        if p in ROMAN_LEVEL:
            return ROMAN_LEVEL[p] if ROMAN_LEVEL[p] in (1,2) else None
        if p.isdigit():
            n = int(p)
            if n in (1,2):
                return n
    # očigledno 3. nivo i niže?
    if any(tok in s for tok in LEVEL3_PLUS_TOKENS):
        return 3
    return None

LEAGUE_WHITELIST_IDS = set()   # id-jevi liga (tier 1 i 2)
LEAGUE_WHITELIST_UPDATED = None
LEAGUE_WHITELIST_TTL_H = 168   # 7 dana

def _refresh_league_whitelist(force=False):
    global LEAGUE_WHITELIST_IDS, LEAGUE_WHITELIST_UPDATED
    now = datetime.utcnow()
    if (not force 
        and LEAGUE_WHITELIST_UPDATED 
        and (now - LEAGUE_WHITELIST_UPDATED) <= timedelta(hours=LEAGUE_WHITELIST_TTL_H)):
        return

    season_year = now.year
    resp = rate_limited_request(f"{BASE_URL}/leagues", params={"season": season_year})
    arr = resp.get("response", []) if resp else []
    by_country = {}

    # grupiši po zemlji samo "current" sezone i tip "League/Cup"
    for row in arr:
        league = row.get("league") or {}
        country_obj = row.get("country") or {}
        seasons = row.get("seasons") or []

        name = league.get("name") or ""
        lid  = league.get("id")
        if not lid or not name:
            continue

        # zadrži samo one koje imaju current=True sezonu
        cur = next((s for s in seasons if s.get("current") is True), None)
        if not cur:
            continue

        # izbaci nesportske tipove (Futsal, Beach Soccer) ako API tako označi
        ltype = (league.get("type") or "").lower()
        if ltype and ltype not in ("league", "cup"):
            continue

        country_name = (country_obj.get("name") or "").strip().lower()
        if not country_name:
            country_name = _norm(league.get("country") or "")

        # ⛔ preskoči youth/reserve lige da NE uđu u whitelist
        if _is_youth_or_reserve_comp_name(name):
            continue

        by_country.setdefault(country_name, []).append((lid, name))

    wl = set()
    # iz svake zemlje zadrži samo levele 1 i 2
    for ctry, items in by_country.items():
        names_only = [nm for _, nm in items]
        for lid, nm in items:
            lvl = _infer_level_from_name(nm, names_only)
            if lvl in (1, 2):
                wl.add(lid)

    LEAGUE_WHITELIST_IDS = wl
    LEAGUE_WHITELIST_UPDATED = now
    print(f"✅ League whitelist refreshed: {len(LEAGUE_WHITELIST_IDS)} leagues (tier 1/2)")

def is_valid_competition_with_reason(fixture):
    league  = (fixture.get("league") or {})
    lid     = league.get("id")
    lname_raw = league.get("name") or ""        # sirovo ime (sa zagradama, crtama…)
    lname   = _norm(lname_raw)                  # normalizovano
    ltype   = (league.get("type") or "").lower()
    country = _norm(league.get("country") or "")

    home_name = ((fixture.get("teams") or {}).get("home") or {}).get("name") or ""
    away_name = ((fixture.get("teams") or {}).get("away") or {}).get("name") or ""

    # 0) Youth/Reserve HARD REJECT – PRE bilo čega (whitelist, level…)
    if _is_youth_or_reserve_comp_name(lname_raw) or _is_reserve_team(home_name) or _is_reserve_team(away_name):
        return False, "youth_or_reserve"

    # 1) ID whitelist – ali NIKAD ako je youth/reserve (čak i da je greškom upalo u whitelist)
    if lid in LEAGUE_WHITELIST_IDS:
        # dodatni guard (defanzivno)
        if _is_youth_or_reserve_comp_name(lname_raw):
            return False, "youth_or_reserve_whitelisted"
        return True, None

    # 2) Kontinentalna – DA
    if any(k in lname for k in CONTINENTAL_KW):
        return True, None

    # 3) Reprezentativna / International – DA
    if _is_international(lname, country):
        return True, None

    # 4) KUP – po imenu ili tipu – DA (osim youth/reserves/regional)
    if _is_cup(lname) or ltype == "cup":
        if any(x in lname for x in ["primavera","u19","u20","u21","youth","reserves","regional"]):
            return False, "low_tier_or_youth_cup"
        return True, None

    # 5) LIGA
    is_league = (ltype == "league") or (not ltype and country not in INTERNATIONAL_COUNTRIES and not _is_cup(lname))
    if is_league:
        lvl = _infer_level_from_name(lname)
        if lvl in (1, 2):
            return True, None
        if lvl and lvl >= 3:
            return False, "level_3_plus"

        if _is_reserve_team(home_name) or _is_reserve_team(away_name):
            return False, "reserve_or_youth_team"

        # Ako nivo NIJE jasno 1/2 i liga NIJE whitelisted → odbij
        return False, "unknown_level_not_whitelisted"

    # 6) Sve ostalo – NE
    return False, "not_league_or_cup"

def is_valid_competition(fixture) -> bool:
    ok, reason = is_valid_competition_with_reason(fixture)
    if not ok:
        _dbg_comp_reject(fixture, reason)
    return ok

def _infer_team_tier_from_matches(matches, fallback=DEFAULT_TEAM_TIER):
    levels = []
    for m in matches or []:
        nm = ((m.get('league') or {}).get('name') or '')
        lvl = _infer_level_from_name(nm)
        if lvl in (1, 2, 3, 4):
            levels.append(lvl)
    if not levels:
        return fallback
    # uzmi NAJNIŽI broj (najviši rang) koji je tim realno igrao; cap na [1..4]
    return int(max(1, min(4, min(levels))))

CACHE_TTL_HOURS = 48

def compute_team_profiles(team_last_matches, stats_fn, lam=5.0, max_n=15):
    """
    Dodato:
      - finishing_xg: (g_for_1h - xg_for_1h) / max(1, xg) (EB shrink)
      - gk_stop: save% proxy = 1 - goals_allowed_1h / SoT_allowed_1h (EB shrink na GK_SAVE_PRIOR_1H)
      - match_dates: liste datuma poslednjih mečeva (za zagušenje rasporeda relativno na fixture dt)
    """
    profiles = {}
    for team_id, matches in (team_last_matches or {}).items():
        arr = sorted(matches or [], key=lambda m: (m.get('fixture') or {}).get('date',''), reverse=True)[:max_n]

        w_sot_for = w_sot_alw = w_da_for = w_da_alw = w_pos = 0.0
        sum_sot_for = sum_sot_alw = sum_da_for = sum_da_alw = sum_pos = 0.0
        g_for_w = g_alw_w = 0.0
        xg_for_w = xg_alw_w = 0.0
        sot_alw_w = 0.0

        last_match_dt = None
        eff_n = 0.0
        match_dates = []

        for i, m in enumerate(arr):
            w = _exp_w(i, lam)
            fix = (m.get('fixture') or {})
            fid = fix.get('id')
            if not fid:
                continue
            teams = (m.get('teams') or {})
            hid = ((teams.get('home') or {}).get('id'))
            aid = ((teams.get('away') or {}).get('id'))
            if hid is None or aid is None:
                continue
            opp_id = aid if hid == team_id else hid

            stats = stats_fn(fid)
            micro = _extract_match_micro_for_team(stats, team_id, opp_id) if stats else None
            if micro:
                if micro.get('sot1h_for') is not None:
                    sum_sot_for += w * micro['sot1h_for']; w_sot_for += w
                if micro.get('sot1h_allowed') is not None:
                    sum_sot_alw += w * micro['sot1h_allowed']; w_sot_alw += w; sot_alw_w += w*micro['sot1h_allowed']
                if micro.get('da1h_for') is not None:
                    sum_da_for += w * micro['da1h_for']; w_da_for += w
                if micro.get('da1h_allowed') is not None:
                    sum_da_alw += w * micro['da1h_allowed']; w_da_alw += w
                if micro.get('pos1h') is not None:
                    sum_pos += w * micro['pos1h']; w_pos += w
                if micro.get('xg1h_for') is not None:
                    xg_for_w += w * micro['xg1h_for']
                if micro.get('xg1h_allowed') is not None:
                    xg_alw_w += w * micro['xg1h_allowed']

            gf, ga = _ht_goals_for_against(m, team_id)
            if gf is not None: g_for_w += w * float(gf)
            if ga is not None: g_alw_w += w * float(ga)

            eff_n += w
            if last_match_dt is None:
                try:
                    last_match_dt = datetime.fromisoformat((fix.get('date') or '').replace("Z","+00:00"))
                except Exception:
                    last_match_dt = None
            try:
                match_dates.append(datetime.fromisoformat((fix.get('date') or '').replace("Z","+00:00")))
            except:
                pass

        mean_sot_for = _safe_div(sum_sot_for, w_sot_for, None)
        mean_sot_alw = _safe_div(sum_sot_alw, w_sot_alw, None)
        mean_da_for  = _safe_div(sum_da_for,  w_da_for,  None)
        mean_da_alw  = _safe_div(sum_da_alw,  w_da_alw,  None)
        mean_pos     = _safe_div(sum_pos,     w_pos,     None)

        fin  = beta_shrunk_rate(g_for_w,  sum_sot_for if sum_sot_for>0 else None,  m=FINISH_PRIOR_1H, tau=6.0)
        leak = beta_shrunk_rate(g_alw_w,  sum_sot_alw if sum_sot_alw>0 else None, m=LEAK_PRIOR_1H,   tau=6.0)

        # finishing vs xG (ako nema xG -> neutralno 0.0)
        fin_xg = 0.0
        if xg_for_w > 0:
            raw = (g_for_w - xg_for_w) / max(1e-6, xg_for_w)
            fin_xg = beta_shrunk_rate(max(0.0, raw), eff_n, m=0.0, tau=6.0) - 0.5  # centrira oko 0

        # GK shot-stopping proxy: save% = 1 - goals_allowed/SoT_allowed
        gk_stop = GK_SAVE_PRIOR_1H
        if sot_alw_w > 0:
            save_rate = 1.0 - _safe_div(g_alw_w, sot_alw_w, 0.0)
            gk_stop = beta_shrunk_rate(save_rate * (eff_n or 1.0), eff_n, m=GK_SAVE_PRIOR_1H, tau=GK_SAVE_TAU)

        tier = _infer_team_tier_from_matches(arr, fallback=DEFAULT_TEAM_TIER)

        profiles[team_id] = {
            "sot_for": mean_sot_for, "sot_allowed": mean_sot_alw,
            "da_for":  mean_da_for,  "da_allowed":  mean_da_alw,
            "pos":     mean_pos,
            "finish":  fin,  "leak": leak,
            "eff_n":   eff_n,
            "last_match_dt": last_match_dt,
            "fin_effn":  max(0.0, float(sum_sot_for)),
            "leak_effn": max(0.0, float(sum_sot_alw)),
            "tier": tier,

            # NEW
            "fin_xg": fin_xg,        # finishing preko xG (centrirano)
            "gk_stop": gk_stop,      # shot-stopping proxy
            "match_dates": match_dates,  # za zagušenje
        }
    return profiles

def _rest_days_until_fixture(profile, fixture_dt_utc):
    lm = (profile or {}).get("last_match_dt")
    if not lm or not fixture_dt_utc:
        return None
    try:
        return max(0.0, (fixture_dt_utc - lm).total_seconds()/86400.0)
    except Exception:
        return None

def _ln_mult(x, default=1.0):
    """Pretvori multiplikator (≈1.00) u log-skalu radi additivnog unosa u z (robustno)."""
    try:
        return math.log(max(1e-6, float(x)))
    except Exception:
        return math.log(default)

def _safe_float(x, default=None):
    try:
        return float(x)
    except:
        return default

# --- Sudija (1H profil): penalnost + golovi + faulovi ---
def compute_referee_profile_1h(fixture, repo_get_ref_history=None):
    """Vrati RefereeProfile.multiplier ~ 0.96–1.06 na osnovu 1H penala/golova sudije."""
    ref_name = ((fixture.get('fixture') or {}).get('referee') 
                or (fixture.get('league') or {}).get('referee') 
                or (fixture.get('referee')))
    if not ref_name:
        return {"multiplier": 1.00, "fh_goals": None, "fh_pens": None}

    hist = []
    try:
        if repo_get_ref_history:
            hist = repo_get_ref_history(ref_name, limit=30) or []
    except:
        hist = []

    # očekivanje: hist su raw fixtures za tog sudiju; ako nema – neutralno
    if not hist:
        return {"multiplier": 1.00, "fh_goals": None, "fh_pens": None}

    # gruba procena: 1H golova i penala u mečevima tog sudije
    g_sum = 0.0; p_sum = 0.0; n = 0
    for m in hist:
        ht = ((m.get('score') or {}).get('halftime') or {})
        g_sum += float(ht.get('home') or 0) + float(ht.get('away') or 0)
        # penali (ako raspoloživo u tvom zapisu; često nije -> fallback 0)
        p_sum += float(((m.get('events') or {}).get('pens_first_half')) or 0)
        n += 1
    if n == 0:
        return {"multiplier": 1.00, "fh_goals": None, "fh_pens": None}

    fh_g = g_sum / n
    fh_p = p_sum / n

    # pretvori na multiplikator okrenut oko 1.0
    # baseline ~ 0.55 golova/event u 1H i penali ~ 0.10/1H (grubo)
    g_b = 0.55
    p_b = 0.10
    g_k = 0.06
    p_k = 0.04
    mult = (1.0 + g_k * (fh_g - g_b)) * (1.0 + p_k * (fh_p - p_b))
    mult = max(0.94, min(1.06, mult))
    return {"multiplier": mult, "fh_goals": fh_g, "fh_pens": fh_p}

# --- Vreme ---
def compute_weather_factor_1h(fixture):
    wx = ((fixture.get('weather') or {})
          or ((fixture.get('fixture') or {}).get('weather') or {}))
    wind = _safe_float(wx.get('wind') or wx.get('wind_kmh'), None)
    temp = _safe_float(wx.get('temp') or wx.get('temp_c'), None)
    rain = _safe_float(wx.get('rain') or wx.get('rain_mm'), None)

    mult = 1.00
    if wind is not None and wind >= 25:    # jak vetar
        mult *= 0.97
    if rain is not None and rain > 0.0:    # kiša/sneg
        mult *= 0.98
    if temp is not None and (temp <= -2 or temp >= 33):
        mult *= 0.985

    return {"multiplier": max(0.95, min(1.03, mult)), "wind": wind, "temp": temp, "rain": rain}

# --- Postave / izostanci ---
def compute_lineup_projection_1h(fixture_id, api_or_repo_get_lineups=None):
    """Broji ključne izostanke i vraća multiplikator 0.90–1.10."""
    if not api_or_repo_get_lineups:
        return {"home": {"multiplier": 1.00}, "away": {"multiplier": 1.00}}

    data = api_or_repo_get_lineups(fixture_id) or {}
    out = {}
    for side in ("home", "away"):
        s = data.get(side) or {}
        kf = int(s.get("key_forwards_out") or 0)
        kc = int(s.get("key_creators_out") or 0)
        kcb= int(s.get("key_cb_out") or 0)
        gk = bool(s.get("gk_out") or False)

        mult = 1.00
        mult *= (0.98 ** kf)
        mult *= (0.985 ** kc)
        mult *= (0.988 ** kcb)
        if gk:
            mult *= 0.97
        out[side] = {"multiplier": max(0.90, min(1.10, mult)), "kf": kf, "kc": kc, "kcb": kcb, "gk": gk}
    return out

# --- Penal/VAR profil ---
def compute_penvar_profile_1h(fixture, team_profiles, referee_profile=None):
    """Kombinuje penalnost tima & sudije u 1H u multiplikator 0.98–1.04."""
    # približno: ako su timovi “pen-happy” + sudija “pen-happy” ⇒ >1.00
    # koristi leak/finish kao zamenu i malu dozu referee
    fin_h = (team_profiles.get(((fixture.get('teams') or {}).get('home') or {}).get('id'), {}) or {}).get("finish", 0.34)
    fin_a = (team_profiles.get(((fixture.get('teams') or {}).get('away') or {}).get('id'), {}) or {}).get("finish", 0.34)
    base = (fin_h + fin_a)/2.0
    ref_mult = (referee_profile or {}).get("multiplier", 1.00)
    mult = 1.0 * (1.0 + 0.04*(base - 0.34)) * (1.0 + 0.5*(ref_mult - 1.0))
    return {"multiplier": max(0.98, min(1.04, mult))}

# --- Stadion / teren ---
def compute_stadium_pitch_1h(fixture):
    st = ((fixture.get('stadium') or {})
          or ((fixture.get('fixture') or {}).get('stadium') or {})
          or {})
    alt = _safe_float(st.get("altitude") or st.get("altitude_m"), None)
    turf = bool(st.get("turf") or st.get("artificial_turf") or False)
    quality = _safe_float(st.get("pitch_quality"), 0.9)

    mult = 1.0
    if alt is not None and alt >= 900:
        mult *= 0.985
    if turf:
        mult *= 0.992
    if quality is not None:
        mult *= 0.98 + 0.04*quality  # quality 0..1 -> 0.98..1.02
    return {"multiplier": max(0.98, min(1.03, mult)), "altitude": alt, "turf": turf, "quality": quality}

def matchup_features_enhanced(fixture, team_profiles, league_baselines, micro_db=None, extras: dict | None = None):
    base = _league_base_for_fixture(fixture, league_baselines)
    home_id = ((fixture.get('teams') or {}).get('home') or {}).get('id')
    away_id = ((fixture.get('teams') or {}).get('away') or {}).get('id')

    ph = (team_profiles or {}).get(home_id, {}) or {}
    pa = (team_profiles or {}).get(away_id, {}) or {}

    # očekivanja po timu (geometrijska sredina sa allowed)
    def _geox_h(f_for, a_alw): return _geomean(ph.get(f_for), pa.get(a_alw))
    def _geox_a(f_for, a_alw): return _geomean(pa.get(f_for), ph.get(a_alw))

    exp_sot_h = _cap_league_per_team(_geox_h("sot_for", "sot_allowed"), (base.get("q95_sot1h") or 6.0), SOT1H_CAP)
    exp_sot_a = _cap_league_per_team(_geox_a("sot_for", "sot_allowed"), (base.get("q95_sot1h") or 6.0), SOT1H_CAP)
    exp_da_h  = _cap_league_per_team(_geox_h("da_for",  "da_allowed"),  (base.get("q95_da1h")  or 65.0), DA1H_CAP)
    exp_da_a  = _cap_league_per_team(_geox_a("da_for",  "da_allowed"),  (base.get("q95_da1h")  or 65.0), DA1H_CAP)

    # dodatne mikro metrike (shots/xG/bigch/corners/fk) ako postoje u micro_db
    mh = (micro_db or {}).get(home_id, {}).get("home", {}) if micro_db else {}
    ma = (micro_db or {}).get(away_id, {}).get("away", {}) if micro_db else {}

    shots_h = mh.get("shots1h_for");  shots_a = ma.get("shots1h_for")
    xg_h    = mh.get("xg1h_for");     xg_a    = ma.get("xg1h_for")
    big_h   = mh.get("bigch1h_for");  big_a   = ma.get("bigch1h_for")
    corn_h  = mh.get("corn1h_for");   corn_a  = ma.get("corn1h_for")
    fk_h    = mh.get("fk1h_for");     fk_a    = ma.get("fk1h_for")

    # set-piece xG proxy (1H total)
    setp_xg_total = 0.0
    for val in (corn_h, corn_a):
        if val is not None: setp_xg_total += val * SETP_XG_PER_CORNER_1H
    for val in (fk_h, fk_a):
        if val is not None: setp_xg_total += val * SETP_XG_PER_FK_1H

    # posjed edge
    pos_edge = None
    if ph.get("pos") is not None and pa.get("pos") is not None:
        pos_edge = max(-1.0, min(1.0, (float(ph["pos"]) - float(pa["pos"])) / 100.0))

    # fixture dt (za odmor & zagušenje)
    try:
        fixture_dt_utc = datetime.fromisoformat(((fixture.get('fixture') or {}).get('date') or '').replace("Z","+00:00"))
    except Exception:
        fixture_dt_utc = None
    rest_h = _rest_days_until_fixture(ph, fixture_dt_utc)
    rest_a = _rest_days_until_fixture(pa, fixture_dt_utc)

    # zagušenje: broj mečeva u prethodnih 7/14 dana pre fixture-a
    def _congestion(profile):
        ds = profile.get("match_dates") or []
        if not fixture_dt_utc: return 0.0
        c7 = sum(1 for d in ds if (fixture_dt_utc - d).total_seconds()/(3600*24) <= 7.0 and d < fixture_dt_utc)
        c14= sum(1 for d in ds if (fixture_dt_utc - d).total_seconds()/(3600*24) <= 14.0 and d < fixture_dt_utc)
        # mapiraj u [-1, +1] – 0 = normalno (2 u 14d ~ ok)
        overload = max(0.0, (c7-2)*0.5 + (c14-4)*0.25)
        return min(1.0, overload)
    congest_h = _congestion(ph)
    congest_a = _congestion(pa)

    # reliabilnosti za FIN/LEAK
    fin_rel_h  = _rel(ph.get("fin_effn",0),   k=25.0)
    fin_rel_a  = _rel(pa.get("fin_effn",0),   k=25.0)
    leak_rel_h = _rel(ph.get("leak_effn",0),  k=25.0)
    leak_rel_a = _rel(pa.get("leak_effn",0),  k=25.0)

    # class gap & cup
    lgname_raw = ((fixture.get('league') or {}).get('name') or '')
    is_cup = _is_cup(lgname_raw) or ((fixture.get('league') or {}).get('type','').lower()=='cup')
    tier_h = int(ph.get("tier") or DEFAULT_TEAM_TIER); tier_h = max(1, min(4, tier_h))
    tier_a = int(pa.get("tier") or DEFAULT_TEAM_TIER); tier_a = max(1, min(4, tier_a))
    tier_gap_home = max(-MAX_TIER_GAP, min(MAX_TIER_GAP, (tier_a - tier_h)))
    tier_gap_away = max(-MAX_TIER_GAP, min(MAX_TIER_GAP, (tier_h - tier_a)))

    # FIN/LEAK + GK stop
    fin_h  = ph.get("finish", FINISH_PRIOR_1H);     fin_a  = pa.get("finish", FINISH_PRIOR_1H)
    leak_h = ph.get("leak",   LEAK_PRIOR_1H);       leak_a = pa.get("leak",   LEAK_PRIOR_1H)
    gk_h   = ph.get("gk_stop", GK_SAVE_PRIOR_1H);   gk_a   = pa.get("gk_stop", GK_SAVE_PRIOR_1H)
    finxg_h= ph.get("fin_xg", 0.0);                 finxg_a= pa.get("fin_xg", 0.0)

    # lineup multiplikator (sada stvarno povuci postave)
    lineup   = compute_lineup_projection_1h(((fixture.get('fixture') or {}).get('id')),
                api_or_repo_get_lineups=lambda fid: repo.get_lineups(fid, no_api=False))
    # sudija/vreme/penvar/stadion kao i ranije
    ref_prof = compute_referee_profile_1h(fixture, repo_get_ref_history=None)
    wx = compute_weather_factor_1h({"fixture": {"weather": (extras or {}).get("weather_obj")}}) \
        if (extras and extras.get("weather_obj")) else compute_weather_factor_1h(fixture)
    penvar   = compute_penvar_profile_1h(fixture, team_profiles, referee_profile=ref_prof)
    stadium  = compute_stadium_pitch_1h(fixture)

    # važnost meča (0..10) → normalizovan adj u [-0.5, +0.5] oko “5”
    try:
        imp_raw = float(calculate_match_importance(((fixture.get('fixture') or {}).get('id'))))
    except:
        imp_raw = 5.0
    importance_adj = max(-0.5, min(0.5, (imp_raw - 5.0) / 5.0))

    out = {
        "exp_sot1h_home": exp_sot_h, "exp_sot1h_away": exp_sot_a,
        "exp_da1h_home":  exp_da_h,  "exp_da1h_away":  exp_da_a,
        "pos_edge": pos_edge,
        "fin_home": fin_h, "fin_away": fin_a,
        "leak_home": leak_h, "leak_away": leak_a,
        "gk_home": gk_h,   "gk_away": gk_a,
        "finxg_home": finxg_h, "finxg_away": finxg_a,
        "rest_home": rest_h, "rest_away": rest_a,
        "pace_sot_total": (exp_sot_h or 0) + (exp_sot_a or 0),
        "pace_da_total":  (exp_da_h  or 0) + (exp_da_a  or 0),
        "tier_gap_home": tier_gap_home, "tier_gap_away": tier_gap_away, "is_cup": is_cup,

        # NEW per-team mikro prosjeci
        "shots1h_home": shots_h, "shots1h_away": shots_a,
        "xg1h_home": xg_h,       "xg1h_away": xg_a,
        "bigch1h_home": big_h,   "bigch1h_away": big_a,
        "setp_xg_total": setp_xg_total,
        "congest_home": congest_h, "congest_away": congest_a,

        # multiplikatori (ln(mult))
        "ref_mult": ref_prof.get("multiplier", 1.0),
        "weather_mult": wx.get("multiplier", 1.0),
        "penvar_mult": penvar.get("multiplier", 1.0),
        "stadium_mult": stadium.get("multiplier", 1.0),
        "lineup_mult_home": (lineup.get("home") or {}).get("multiplier", 1.0),
        "lineup_mult_away": (lineup.get("away") or {}).get("multiplier", 1.0),

        # logit adj okruženja
        "ref_adj":     (extras or {}).get("ref_adj", 0.0),
        "ref_used":    (extras or {}).get("ref_used", 0.0),
        "ref_name":    (extras or {}).get("ref_name"),
        "weather_adj": (extras or {}).get("weather_adj", 0.0),
        "venue_adj":   (extras or {}).get("venue_adj", 0.0),
        "lineup_adj":  (extras or {}).get("lineup_adj", 0.0),
        "lineups_have": bool((extras or {}).get("lineups_have", False)),
        "lineups_fw_count": (extras or {}).get("lineups_fw_count"),
        "inj_adj":     (extras or {}).get("inj_adj", 0.0),
        "inj_count":   (extras or {}).get("inj_count"),

        # new global adj
        "importance_adj": importance_adj,
    }
    return out

def predict_team_scores1h_enhanced(fixture, feats, league_baselines, team_strengths, side='home'):
    base = _league_base_for_fixture(fixture, league_baselines)
    m = base["m1h"]

    # per-team z-osnove iz league baseline (SOT/DA)
    mu_sot_h = (base.get("mu_sot1h") or 0.0) / 2.0
    sd_sot_h = max(1e-6, (base.get("sd_sot1h") or 1.0) / (2**0.5))
    mu_da_h  = (base.get("mu_da1h")  or 0.0) / 2.0
    sd_da_h  = max(1e-6, (base.get("sd_da1h")  or 1.0) / (2**0.5))

    # fallback global baseline za nove signale (konzervativno)
    mu_shots_h, sd_shots_h = 5.0, 2.0
    mu_xg_h,    sd_xg_h    = 0.55, 0.30
    mu_big_h,   sd_big_h   = 0.7,  0.8

    if side == 'home':
        exp_sot = feats.get('exp_sot1h_home'); exp_da = feats.get('exp_da1h_home')
        shots   = feats.get('shots1h_home');   xg     = feats.get('xg1h_home'); big = feats.get('bigch1h_home')
        fin     = feats.get('fin_home');       leak_opp = feats.get('leak_away')
        gk_me   = feats.get('gk_home');        finxg   = feats.get('finxg_home', 0.0)
        rest    = feats.get('rest_home');      congest = feats.get('congest_home', 0.0)
        pos_share = feats.get('pos_edge') or 0.0
        tier_gap = float(feats.get('tier_gap_home') or 0.0)
        lineup_mult = feats.get('lineup_mult_home', 1.0)
    else:
        exp_sot = feats.get('exp_sot1h_away'); exp_da = feats.get('exp_da1h_away')
        shots   = feats.get('shots1h_away');   xg     = feats.get('xg1h_away'); big = feats.get('bigch1h_away')
        fin     = feats.get('fin_away');       leak_opp = feats.get('leak_home')
        gk_me   = feats.get('gk_away');        finxg   = feats.get('finxg_away', 0.0)
        rest    = feats.get('rest_away');      congest = feats.get('congest_away', 0.0)
        pos_share = -(feats.get('pos_edge') or 0.0)
        tier_gap = float(feats.get('tier_gap_away') or 0.0)
        lineup_mult = feats.get('lineup_mult_away', 1.0)

    # z-score klasici
    z_sot = _z(exp_sot, mu_sot_h, sd_sot_h)
    z_da  = _z(exp_da,  mu_da_h,  sd_da_h)

    # novi z-score (fallback baseline)
    z_shots = 0.0 if shots is None else _z(shots, mu_shots_h, sd_shots_h)
    z_xg    = 0.0 if xg    is None else _z(xg,    mu_xg_h,    sd_xg_h)
    z_big   = 0.0 if big   is None else _z(big,   mu_big_h,   sd_big_h)

    # finishing/leak kao i ranije (logit space)
    fin_adj  = _logit(min(0.99, max(0.01, fin or FINISH_PRIOR_1H)))    - _logit(FINISH_PRIOR_1H)
    leak_adj = _logit(min(0.99, max(0.01, leak_opp or LEAK_PRIOR_1H))) - _logit(LEAK_PRIOR_1H)

    # gk (save%) – pomeraj oko priora
    gk_adj = (float(gk_me or GK_SAVE_PRIOR_1H) - GK_SAVE_PRIOR_1H)

    # finishing vs xG (centrirano oko 0)
    finxg_adj = float(finxg or 0.0)

    # odmor i zagušenje
    rest_z = 0.0
    if rest is not None:
        rest_z = max(-1.0, min(1.0, (float(rest) - REST_REF_DAYS) / 5.0))
    congest_adj = -float(congest or 0.0)  # više mečeva → minus

    # strength baseline (att tim & def_allow protivnik)
    home_id = ((fixture.get('teams') or {}).get('home') or {}).get('id')
    away_id = ((fixture.get('teams') or {}).get('away') or {}).get('id')
    team_id = home_id if side=='home' else away_id
    opp_id  = away_id if side=='home' else home_id

    sT  = (team_strengths or {}).get(team_id, {"att": m, "def_allow": m})
    sOpp = (team_strengths or {}).get(opp_id, {"att": m, "def_allow": m})
    att_adj  = _logit(sT['att'])         - _logit(m)
    defo_adj = _logit(sOpp['def_allow']) - _logit(m)

    # multiplikativni faktori → ln(mult)
    ln_ref   = _ln_mult(feats.get("ref_mult", 1.0))
    ln_wx    = _ln_mult(feats.get("weather_mult", 1.0))
    ln_line  = _ln_mult(lineup_mult)
    ln_pen   = _ln_mult(feats.get("penvar_mult", 1.0))
    ln_stad  = _ln_mult(feats.get("stadium_mult", 1.0))

    class_weight = WEIGHTS["TIER_GAP"] * (CUP_TIER_MULT if feats.get("is_cup") else 1.0)

    # coverage/reliability
    cov_sot = feats.get('cov_sot', 1.0)
    cov_da  = feats.get('cov_da',  1.0)
    cov_pos = feats.get('cov_pos', 1.0)
    fin_rel = feats.get('fin_rel_home' if side=='home' else 'fin_rel_away', 1.0)
    leak_rel= feats.get('leak_rel_away' if side=='home' else 'leak_rel_home',1.0)

    # težine
    w_zsot = WEIGHTS["Z_SOT"] * cov_sot
    w_zda  = WEIGHTS["Z_DA"]  * cov_da
    w_pos  = WEIGHTS["POS"]   * cov_pos
    w_fin  = WEIGHTS["FIN"]   * fin_rel
    w_leak = WEIGHTS["LEAK"]  * leak_rel

    # novi signali
    w_shots = WEIGHTS["Z_SHOTS"]
    w_xg    = WEIGHTS["Z_XG"]
    w_big   = WEIGHTS["Z_BIGCH"]
    w_setp  = WEIGHTS["SETP"]
    w_gk    = WEIGHTS["GK"]
    w_cong  = WEIGHTS["CONGEST"]

    # global adj (primeni samo jednom — u 'home' pozivu)
    global_adj = 0.0
    if side == 'home':
        global_adj += WEIGHTS.get("REF",0.0)         * float(feats.get("ref_adj") or 0.0)
        global_adj += WEIGHTS.get("ENV_WEATHER",0.0) * float(feats.get("weather_adj") or 0.0)
        global_adj += WEIGHTS.get("VENUE",0.0)       * float(feats.get("venue_adj") or 0.0)
        global_adj += WEIGHTS.get("LINEUPS",0.0)     * float(feats.get("lineup_adj") or 0.0)
        global_adj += WEIGHTS.get("INJ",0.0)         * float(feats.get("inj_adj") or 0.0)
        global_adj += WEIGHTS.get("IMPORTANCE",0.0)  * float(feats.get("importance_adj") or 0.0)
        # set-piece total (ukupan doprinos utakmice, ne stranican → dodaj ovde)
        global_adj += w_setp * float(feats.get("setp_xg_total") or 0.0)

    z = (
        _logit(m) + WEIGHTS["BIAS"]
        + w_zsot*z_sot + w_zda*z_da + w_pos*pos_share
        + w_fin*fin_adj + w_leak*leak_adj
        + WEIGHTS["REST"]*rest_z + w_cong*congest_adj
        + (WEIGHTS["HOME"] if side=='home' else 0.0)
        + WEIGHTS["ATT"]*att_adj + WEIGHTS["DEF"]*defo_adj
        + class_weight * tier_gap
        # novi z-score dodaci
        + w_shots*z_shots + w_xg*z_xg + w_big*z_big
        # finishing vs xG i GK
        + w_gk*gk_adj + 0.5*WEIGHTS["FIN"]*finxg_adj
        # multiplikatori
        + WEIGHTS["REFEREE_MULT"] * ln_ref
        + WEIGHTS["WEATHER_MULT"] * ln_wx
        + WEIGHTS["LINEUP_MULT"]  * ln_line
        + WEIGHTS["PENVAR_MULT"]  * ln_pen
        + WEIGHTS["STADIUM_MULT"] * ln_stad
        + global_adj
    )

    p = _inv_logit(z)
    p = _calibrate(p, temp=CALIBRATION["TEMP"], floor=CALIBRATION["FLOOR"], ceil=CALIBRATION["CEIL"])
    dbg = {
        "z_sot": round(z_sot,3), "z_da": round(z_da,3), "z_shots": round(z_shots,3),
        "z_xg": round(z_xg,3), "z_big": round(z_big,3),
        "pos_share": round(pos_share,3), "fin_adj": round(fin_adj,3),
        "leak_adj": round(leak_adj,3), "gk_adj": round(gk_adj,3), "finxg": round(finxg_adj,3),
        "rest_z": round(rest_z,3), "congest": round(congest_adj,3),
        "att_adj": round(att_adj,3), "def_adj": round(defo_adj,3),
        "ln_ref": round(ln_ref,3), "ln_wx": round(ln_wx,3), "ln_line": round(ln_line,3),
        "ln_pen": round(ln_pen,3), "ln_stad": round(ln_stad,3),
        "ref_adj": round(float(feats.get("ref_adj") or 0.0), 3),
        "weather_adj": round(float(feats.get("weather_adj") or 0.0), 3),
        "venue_adj": round(float(feats.get("venue_adj") or 0.0), 3),
        "lineup_adj": round(float(feats.get("lineup_adj") or 0.0), 3),
        "inj_adj": round(float(feats.get("inj_adj") or 0.0), 3),
        "importance_adj": round(float(feats.get("importance_adj") or 0.0), 3),
        "tier_gap": round(tier_gap,3), "is_cup": bool(feats.get("is_cup")),
    }
    return p, dbg

def get_or_fetch_team_history(team_id: int, last_n: int = 30, force_refresh: bool = False, no_api: bool = False):
    conn = get_db_connection()
    cur = conn.cursor()

    # CREATE TABLE – zaključaj jer menja šemu
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
    if not force_refresh:
        # 1) tačan ključ (team_id, last_n)
        cur.execute("SELECT data, updated_at FROM team_history_cache WHERE team_id=? AND last_n=?", (team_id, last_n))
        row = cur.fetchone()
        if row:
            try:
                updated_at = datetime.fromisoformat(row["updated_at"])
            except Exception:
                updated_at = now - timedelta(hours=CACHE_TTL_HOURS+1)
            if (now - updated_at) <= timedelta(hours=CACHE_TTL_HOURS) or no_api:
                conn.close()
                return json.loads(row["data"])[:last_n]

        # 2) superset fallback (najveći last_n za taj tim)
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
                return json.loads(row2["data"])[:last_n]

    if no_api:
        conn.close()
        return []  # striktno bez API-ja

    resp = rate_limited_request(f"{BASE_URL}/fixtures",
                                params={'team': team_id, 'last': last_n, 'timezone': 'UTC'})
    data = resp.get('response', []) if resp else []

    with DB_WRITE_LOCK:
        cur.execute("""
            INSERT OR REPLACE INTO team_history_cache(team_id,last_n,data,updated_at)
            VALUES(?,?,?,?)
        """, (team_id, last_n, json.dumps(data, ensure_ascii=False), now.isoformat()))
        conn.commit()

    conn.close()
    return data

def get_or_fetch_h2h(team_a: int, team_b: int, last_n: int = 10, no_api: bool = False):
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
            return json.loads(row["data"])

    if no_api:
        conn.close()
        return []  # striktno bez API-ja

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

def get_or_fetch_fixture_statistics(fixture_id: int):
    # 1) probaj da ČITAŠ iz DB keša (bez upisa)
    existing = try_read_fixture_statistics(fixture_id)
    if existing is not None:
        return existing

    # 2) nema u kešu -> pozovi API
    response = rate_limited_request(f"{BASE_URL}/fixtures/statistics", params={"fixture": fixture_id})
    stats = (response or {}).get('response') or None

    # 3) upiši u bazu pod lock-om (jedini WRITE ovde)
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

def get_fixture_statistics_cached_only(fixture_id: int):
    """Vrati statistiku iz lokalnog keša ili None. Nikad ne zove API."""
    return try_read_fixture_statistics(fixture_id)

# --- Parametri / kapovi (po poluvremenu) ---
SOT1H_CAP = 6.0         # Shots on target (oba tima zajedno u 1H retko prelazi 8; po timu 4-5 je high)
DA1H_CAP  = 65.0        # Dangerous attacks (oba tima zajedno u 1H ~ 80 plafon, po timu ~60 cap za safety)
POS_MIN, POS_MAX = 20.0, 80.0

# Poisson baza za 1H (gruba globalna): lambda_base ~0.8 -> p=1-exp(-0.8)≈0.55
LAMBDA_BASE_1H = 0.80

# Koeficijenti za mikro-signal -> lambda korekciju (fine-tune kasnije)
COEF_SOT = 0.25   # po 1 SOT iznad baseline-a (oba tima u 1H)
COEF_DA  = 0.012  # po 1 DA iznad baseline-a (oba tima u 1H)
COEF_POS = 0.35   # uticaj posjed edge-a (-1..+1) na lambda

# Empirijski baseline-ovi (oba tima zajedno u 1H)
BASE_SOT1H_TOTAL = 2.6
BASE_DA1H_TOTAL  = 40.0

EPS = 1e-9

# ---------- low-level parsiranje statistika ----------
def _num(v):
    if v is None: return None
    if isinstance(v, (int, float)): return float(v)
    s = str(v).strip()
    if s.endswith('%'):
        try: return float(s[:-1])
        except: return None
    try: return float(s)
    except: return None

def _team_block(stats_response, team_id):
    # stats_response: lista sa po jednim blokom po timu -> {'team':{'id':...}, 'statistics':[{'type':..., 'value':...}]}
    if not stats_response: return None
    for item in stats_response:
        if (item.get('team') or {}).get('id') == team_id:
            return item
    return None

def _stat_from_block(block, names):
    # names: lista kandidata naziva (case-insensitive, već ćeš proslediti normalizovane stringove)
    if not block: return None
    arr = block.get('statistics') or []
    for nm in names:
        for s in arr:
            t = (s.get('type') or '').strip().lower()
            if t == nm:
                return _num(s.get('value'))
    return None

def _first_half_or_half_of_full(block, name_full_list, name_1h_list, cap=None):
    """Vrati procenu 1H metrike. Ako postoji eksplicitni 1H ključ — koristi njega,
       inače uzmi full-match / 2. Primeni cap ako je dat."""
    v_1h = _stat_from_block(block, name_1h_list)
    if v_1h is not None:
        val = v_1h
    else:
        v_full = _stat_from_block(block, name_full_list)
        if v_full is None: 
            return None
        val = v_full / 2.0
    if cap is not None and val is not None:
        val = max(0.0, min(float(cap), float(val)))
    return val

def _extract_match_micro_for_team(stats_response, team_id, opp_id):
    """Iz jednog meča izvuci 1H mikro metrike za tim i 'allowed' preko protivnika.
       Dodato: total shots, xG, big chances, korneri, slobodnjaci (1H ako postoji; inače full/2)."""
    tb = _team_block(stats_response, team_id)
    ob = _team_block(stats_response, opp_id)
    if not tb or not ob:
        return None

    def one_half_or_half(block, full_list, half_list, cap=None):
        v = _first_half_or_half_of_full(block, [n.lower() for n in full_list],
                                        [n.lower() for n in half_list], cap=cap)
        return v

    # Shots on Target (1H)
    sot1h_for     = one_half_or_half(tb,
                        ["shots on goal", "shots on target"],
                        ["1st half shots on goal", "shots on goal 1st half", "first half shots on goal",
                         "1st half shots on target", "shots on target 1st half", "first half shots on target"],
                        cap=SOT1H_CAP)
    sot1h_allowed = one_half_or_half(ob,
                        ["shots on goal", "shots on target"],
                        ["1st half shots on goal", "shots on goal 1st half", "first half shots on goal",
                         "1st half shots on target", "shots on target 1st half", "first half shots on target"],
                        cap=SOT1H_CAP)

    # Total shots (1H)
    shots1h_for   = one_half_or_half(tb,
                        ["total shots", "shots total", "shots"],
                        ["1st half total shots", "shots 1st half", "first half total shots"],
                        cap=SHOTS1H_CAP)
    shots1h_allowed = one_half_or_half(ob,
                        ["total shots", "shots total", "shots"],
                        ["1st half total shots", "shots 1st half", "first half total shots"],
                        cap=SHOTS1H_CAP)

    # Dangerous Attacks (1H)
    da1h_for      = one_half_or_half(tb,
                        ["dangerous attacks"],
                        ["1st half dangerous attacks", "dangerous attacks 1st half", "first half dangerous attacks"],
                        cap=DA1H_CAP)
    da1h_allowed  = one_half_or_half(ob,
                        ["dangerous attacks"],
                        ["1st half dangerous attacks", "dangerous attacks 1st half", "first half dangerous attacks"],
                        cap=DA1H_CAP)

    # Possession (1H) – kao i ranije
    pos1h = _stat_from_block(tb, ["1st half possession", "possession 1st half", "first half possession",
                                  "1st half ball possession", "ball possession 1st half"])
    if pos1h is None:
        pos_full = _stat_from_block(tb, ["ball possession", "possession", "possession %", "ball possession %"])
        pos1h = pos_full
    if pos1h is not None:
        pos1h = max(POS_MIN, min(POS_MAX, float(pos1h)))

    # xG (1H) – ako nema 1H: full/2 (API-ju ime zna varirati)
    xg1h_for = one_half_or_half(tb,
                    ["expected goals", "xg", "xg expected"],
                    ["1st half xg", "xg 1st half", "first half xg"],
                    cap=XG1H_CAP)
    xg1h_allowed = one_half_or_half(ob,
                    ["expected goals", "xg", "xg expected"],
                    ["1st half xg", "xg 1st half", "first half xg"],
                    cap=XG1H_CAP)

    # Big chances (1H) – ako API ne daje, ostaje None
    bigch1h_for = one_half_or_half(tb,
                    ["big chances", "big chances created"],
                    ["1st half big chances", "big chances 1st half", "first half big chances"],
                    cap=BIGCH1H_CAP)
    bigch1h_allowed = one_half_or_half(ob,
                    ["big chances", "big chances created"],
                    ["1st half big chances", "big chances 1st half", "first half big chances"],
                    cap=BIGCH1H_CAP)

    # Set-pieces: korneri & slobodnjaci (1H)
    corn1h_for = one_half_or_half(tb,
                    ["corner kicks", "corners"],
                    ["1st half corners", "corners 1st half", "first half corners"],
                    cap=CORN1H_CAP)
    corn1h_allowed = one_half_or_half(ob,
                    ["corner kicks", "corners"],
                    ["1st half corners", "corners 1st half", "first half corners"],
                    cap=CORN1H_CAP)

    fk1h_for = one_half_or_half(tb,
                    ["free kicks", "free-kicks"],
                    ["1st half free kicks", "free kicks 1st half", "first half free kicks"],
                    cap=FK1H_CAP)
    fk1h_allowed = one_half_or_half(ob,
                    ["free kicks", "free-kicks"],
                    ["1st half free kicks", "free kicks 1st half", "first half free kicks"],
                    cap=FK1H_CAP)

    return {
        "sot1h_for": sot1h_for,          "sot1h_allowed": sot1h_allowed,
        "shots1h_for": shots1h_for,      "shots1h_allowed": shots1h_allowed,
        "da1h_for": da1h_for,            "da1h_allowed": da1h_allowed,
        "pos1h": pos1h,
        "xg1h_for": xg1h_for,            "xg1h_allowed": xg1h_allowed,
        "bigch1h_for": bigch1h_for,      "bigch1h_allowed": bigch1h_allowed,
        "corn1h_for": corn1h_for,        "corn1h_allowed": corn1h_allowed,
        "fk1h_for": fk1h_for,            "fk1h_allowed": fk1h_allowed,
    }
    
def _ht_total_ge2(m):
    ht = ((m.get('score') or {}).get('halftime') or {})
    h = ht.get('home') or 0
    a = ht.get('away') or 0
    return (h + a) >= 2

def _weighted_match_over15_rate(matches, lam=5.0, max_n=15):
    h, w, n = _weighted_counts(matches, _ht_total_ge2, lam, max_n)
    p = (h / w) if w > 0 else None
    return p, h, w

def _weighted_h2h_over15_rate(h2h_matches, lam=4.0, max_n=10):
    arr = sorted(h2h_matches or [], key=lambda m: (m.get('fixture') or {}).get('date',''), reverse=True)
    h, w, n = _weighted_counts(arr, _ht_total_ge2, lam, max_n)
    p = (h / w) if w > 0 else None
    return p, h, w

def team_1h_over15_stats(team_matches):
    total = 0; hits = 0
    for m in team_matches or []:
        if _ht_total_ge2(m): hits += 1
        total += 1
    pct = round((hits/total)*100, 2) if total else 0.0
    return pct, hits, total

def h2h_1h_over15_stats(h2h_matches):
    total = 0; hits = 0
    for m in h2h_matches or []:
        if _ht_total_ge2(m): hits += 1
        total += 1
    pct = round((hits/total)*100, 2) if total else 0.0
    return pct, hits, total

def _aggregate_team_micro(team_id, matches, get_stats_fn, context="all"):
    """
    Sabira mikro kroz zadnje mečeve. Dodato: shots total, xG, big chances, korneri, free kicks.
    """
    sums = {
        "sot_for":0.0,"sot_alw":0.0,"da_for":0.0,"da_alw":0.0,"pos":0.0,
        "shots_for":0.0,"shots_alw":0.0,
        "xg_for":0.0,"xg_alw":0.0,
        "bigch_for":0.0,"bigch_alw":0.0,
        "corn_for":0.0,"corn_alw":0.0,
        "fk_for":0.0,"fk_alw":0.0,
    }
    cnt  = {k:0 for k in ["sot","da","pos","shots","xg","bigch","corn","fk"]}
    used_any = 0

    for m in matches or []:
        fix = m.get("fixture") or {}
        fid = fix.get("id")
        teams = m.get("teams") or {}
        th = (teams.get("home") or {}).get("id")
        ta = (teams.get("away") or {}).get("id")
        if not fid or (th is None) or (ta is None):
            continue

        if context == "home" and th != team_id:  continue
        if context == "away" and ta != team_id:  continue

        opp_id = ta if th == team_id else th
        stats_response = get_stats_fn(fid)
        if not stats_response:
            continue

        micro = _extract_match_micro_for_team(stats_response, team_id, opp_id)
        if not micro:
            continue

        any_this = False

        # helpers
        def _acc(pair, key_for, key_alw, cap):
            nonlocal any_this
            v_for = micro.get(key_for); v_alw = micro.get(key_alw)
            if (v_for is not None) and (v_alw is not None):
                sums[pair[0]] += v_for; sums[pair[1]] += v_alw; return True
            return False

        # SOT
        if _acc(("sot_for","sot_alw"), "sot1h_for","sot1h_allowed", SOT1H_CAP): cnt["sot"] += 1; any_this = True
        # DA
        if _acc(("da_for","da_alw"), "da1h_for","da1h_allowed", DA1H_CAP): cnt["da"] += 1; any_this = True
        # POS (samo for)
        if micro.get("pos1h") is not None:
            sums["pos"] += micro["pos1h"]; cnt["pos"] += 1; any_this = True
        # SHOTS
        if _acc(("shots_for","shots_alw"), "shots1h_for","shots1h_allowed", SHOTS1H_CAP): cnt["shots"] += 1; any_this = True
        # xG
        if _acc(("xg_for","xg_alw"), "xg1h_for","xg1h_allowed", XG1H_CAP): cnt["xg"] += 1; any_this = True
        # Big chances
        if _acc(("bigch_for","bigch_alw"), "bigch1h_for","bigch1h_allowed", BIGCH1H_CAP): cnt["bigch"] += 1; any_this = True
        # Corners
        if _acc(("corn_for","corn_alw"), "corn1h_for","corn1h_allowed", CORN1H_CAP): cnt["corn"] += 1; any_this = True
        # Free kicks
        if _acc(("fk_for","fk_alw"), "fk1h_for","fk1h_allowed", FK1H_CAP): cnt["fk"] += 1; any_this = True

        if any_this: used_any += 1
        time.sleep(0.00)

    def _avg(k1, k2, ckey):
        return None if cnt[ckey]==0 else round(sums[k1]/cnt[ckey],3)

    return {
        "used": used_any,
        "used_sot": cnt["sot"], "used_da": cnt["da"], "used_pos": cnt["pos"],
        "used_shots": cnt["shots"], "used_xg": cnt["xg"], "used_bigch": cnt["bigch"],
        "used_corn": cnt["corn"], "used_fk": cnt["fk"],

        "sot1h_for": _avg("sot_for","sot_alw","sot"),
        "sot1h_allowed": _avg("sot_alw","sot_for","sot"),
        "da1h_for": _avg("da_for","da_alw","da"),
        "da1h_allowed": _avg("da_alw","da_for","da"),
        "pos1h": None if cnt["pos"]==0 else round(sums["pos"]/cnt["pos"],3),

        "shots1h_for": _avg("shots_for","shots_alw","shots"),
        "shots1h_allowed": _avg("shots_alw","shots_for","shots"),
        "xg1h_for": _avg("xg_for","xg_alw","xg"),
        "xg1h_allowed": _avg("xg_alw","xg_for","xg"),
        "bigch1h_for": _avg("bigch_for","bigch_alw","bigch"),
        "bigch1h_allowed": _avg("bigch_alw","bigch_for","bigch"),
        "corn1h_for": _avg("corn_for","corn_alw","corn"),
        "corn1h_allowed": _avg("corn_alw","corn_for","corn"),
        "fk1h_for": _avg("fk_for","fk_alw","fk"),
        "fk1h_allowed": _avg("fk_alw","fk_for","fk"),
    }

def build_micro_db(team_last_matches, stats_fn):
    """
    Za SVAKI tim izgradi home/away mikro formu.
    Ovdje *uvijek* koristi prosleđeni stats_fn (repo-backed).
    """
    micro = {}
    for team_id, matches in (team_last_matches or {}).items():
        micro[team_id] = {
            "home": _aggregate_team_micro(team_id, matches, stats_fn, context="home"),
            "away": _aggregate_team_micro(team_id, matches, stats_fn, context="away"),
        }
    return micro

def _read_fixtures_from_db(start_dt: datetime, end_dt: datetime):
    conn = get_db_connection()
    cur = conn.cursor()
    # u bazi je ISO string; leksikografski BETWEEN radi za ISO-8601
    s = start_dt.replace(tzinfo=timezone.utc).isoformat()
    e = end_dt.replace(tzinfo=timezone.utc).isoformat()
    cur.execute("SELECT fixture_json FROM fixtures WHERE date >= ? AND date <= ?", (s, e))
    rows = cur.fetchall()
    conn.close()
    out = []
    for (j,) in rows:
        try:
            out.append(json.loads(j))
        except Exception:
            continue
    return out

def _combine_for_allowed(v_for, v_allowed, cap):
    """Vrati srednju ako postoji bar jedna vrednost; ako nema nijedne -> None."""
    vals = [v for v in (v_for, v_allowed) if v is not None]
    if not vals:
        return None
    return max(0.0, min(cap, sum(vals) / len(vals)))

def _opponent_adjusted_expectations(home_id, away_id, micro_db):
    h = (micro_db.get(home_id) or {}).get("home") or {}
    a = (micro_db.get(away_id) or {}).get("away") or {}

    h_sot_for, a_sot_for = h.get("sot1h_for"), a.get("sot1h_for")
    h_sot_alw, a_sot_alw = h.get("sot1h_allowed"), a.get("sot1h_allowed")
    h_da_for,  a_da_for  = h.get("da1h_for"),  a.get("da1h_for")
    h_da_alw,  a_da_alw  = h.get("da1h_allowed"),  a.get("da1h_allowed")

    # očekivanja po timu (for vs allowed)
    exp_sot1h_home = _combine_for_allowed(h_sot_for, a_sot_alw, SOT1H_CAP)
    exp_sot1h_away = _combine_for_allowed(a_sot_for, h_sot_alw, SOT1H_CAP)
    exp_da1h_home  = _combine_for_allowed(h_da_for,  a_da_alw,  DA1H_CAP)
    exp_da1h_away  = _combine_for_allowed(a_da_for,  h_da_alw,  DA1H_CAP)

    def _sum_or_none(x, y, cap):
        vals = [v for v in (x, y) if v is not None]
        if not vals:
            return None
        return max(0.0, min(cap * 2.0, sum(vals)))

    exp_sot1h_total = _sum_or_none(exp_sot1h_home, exp_sot1h_away, SOT1H_CAP)
    exp_da1h_total  = _sum_or_none(exp_da1h_home,  exp_da1h_away,  DA1H_CAP)

    pos_h = h.get("pos1h")
    pos_a = a.get("pos1h")
    pos_edge = None
    if pos_h is not None and pos_a is not None:
        pos_edge = max(-1.0, min(1.0, (pos_h - pos_a) / 100.0))

    coverage = min(h.get("used") or 0, a.get("used") or 0)

    return {
        "exp_sot1h_total": exp_sot1h_total,
        "exp_da1h_total":  exp_da1h_total,
        "pos_edge": pos_edge,
        "coverage": coverage,
        # novo: po timu
        "exp_sot1h_home": exp_sot1h_home,
        "exp_sot1h_away": exp_sot1h_away,
        "exp_da1h_home":  exp_da1h_home,
        "exp_da1h_away":  exp_da1h_away,
    }

def _p_micro_from_expectations(exp_sot1h_total, exp_da1h_total, pos_edge):
    lam = LAMBDA_BASE_1H
    if exp_sot1h_total is not None:
        lam += COEF_SOT * (exp_sot1h_total - BASE_SOT1H_TOTAL)
    if exp_da1h_total is not None:
        lam += COEF_DA  * (exp_da1h_total  - BASE_DA1H_TOTAL)
    if pos_edge is not None:
        lam += COEF_POS * pos_edge
    lam = max(0.0, lam)
    return max(0.0, min(1.0, 1.0 - math.exp(-lam)))

def _logit(p):
    p = min(1 - 1e-6, max(1e-6, p))
    return math.log(p / (1 - p))

def _inv_logit(z):
    return 1.0 / (1.0 + math.exp(-z))

def blend_with_market(prob_model: float, odds_market: Optional[float], alpha: float = ALPHA_MODEL) -> float:
    """
    odds_market = decimal odds za traženi market (očisti marginu izvan ove funkcije ako imaš obe strane).
    Ako nemamo kvotu → vrati prob_model.
    """
    if not odds_market or odds_market <= 1.0:
        return prob_model
    p_mkt = 1.0 / odds_market
    z = alpha * _logit(prob_model) + (1.0 - alpha) * _logit(p_mkt)
    return _inv_logit(z)

# =============== NEW: Recency, EB shrink, league baselines, strengths, precision-merge ===============

def beta_shrunk_rate(hits, total, m=0.55, tau=8.0):
    """Empirical-Bayes shrink: (hits + m*tau)/(total + tau) ; total može biti i 'težinska suma'."""
    if total is None or total <= 0:
        return m
    a0 = m * tau
    b0 = (1 - m) * tau
    return (hits + a0) / max(1e-9, (total + a0 + b0))

def _exp_w(i, lam=5.0):
    return math.exp(-i / lam)

def _weighted_counts(matches, is_hit_fn, lam=5.0, max_n=15):
    """Vrati (weighted_hits, weight_sum, used_n) po recency eksponencijalnim težinama."""
    arr = sorted(matches or [], key=lambda m: (m.get('fixture') or {}).get('date',''), reverse=True)[:max_n]
    wsum = 0.0; hsum = 0.0; n = 0
    for i, m in enumerate(arr):
        w = _exp_w(i, lam)
        h = 1.0 if is_hit_fn(m) else 0.0
        hsum += w * h
        wsum += w
        n += 1
    return hsum, wsum, n

def _ht_total_ge1(m):
    ht = ((m.get('score') or {}).get('halftime') or {})
    h = ht.get('home') or 0
    a = ht.get('away') or 0
    return (h + a) >= 1

def _team_scored_1h(m, team_id):
    teams = (m.get('teams') or {})
    ht = ((m.get('score') or {}).get('halftime') or {})
    if not teams or not ht: return False
    hid = ((teams.get('home') or {}).get('id'))
    aid = ((teams.get('away') or {}).get('id'))
    if hid == team_id:
        return (ht.get('home') or 0) > 0
    if aid == team_id:
        return (ht.get('away') or 0) > 0
    return False

def _team_conceded_1h(m, team_id):
    teams = (m.get('teams') or {})
    ht = ((m.get('score') or {}).get('halftime') or {})
    if not teams or not ht: return False
    hid = ((teams.get('home') or {}).get('id'))
    aid = ((teams.get('away') or {}).get('id'))
    if hid == team_id:
        return (ht.get('away') or 0) > 0
    if aid == team_id:
        return (ht.get('home') or 0) > 0
    return False

def _z(x, mu, sigma):
    if x is None or mu is None or sigma is None or sigma < 1e-6:
        return 0.0
    return (float(x) - float(mu)) / float(sigma)

def _percentile(arr, q):
    if not arr:
        return None
    arr2 = sorted(arr)
    k = (len(arr2) - 1) * q
    f = math.floor(k); c = math.ceil(k)
    if f == c: return float(arr2[int(k)])
    return float(arr2[f] * (c - k) + arr2[c] * (k - f))

def compute_league_baselines(team_last_matches, stats_fn, max_scan_per_league=1500):
    """
    Skenira dostupne mečeve iz history-ja i gradi baseline po (league_id) i global:
    - mu/sigma za 1H SOT (ukupno), 1H DA (ukupno)
    - q95 cap-ovi
    - m1h = stopa 1H ≥ 1 gola u ligi
    """
    seen = set()
    by_lid = {}
    global_sot = []
    global_da = []
    g_hits = 0.0; g_tot = 0.0

    def _extract_1h_totals_both(stats):
        if not stats or len(stats) < 2:
            return (None, None)
        blocks = stats
        def _get_1h(block, full_names, half_names):
            v1 = _stat_from_block(block, [n.lower() for n in half_names])
            if v1 is not None: return float(v1)
            vfull = _stat_from_block(block, [n.lower() for n in full_names])
            if vfull is None: return None
            return float(vfull) / 2.0
        b0, b1 = blocks[0], blocks[1]
        s0 = _get_1h(b0, ["shots on goal"], ["1st half shots on goal","shots on goal 1st half","first half shots on goal"])
        s1 = _get_1h(b1, ["shots on goal"], ["1st half shots on goal","shots on goal 1st half","first half shots on goal"])
        d0 = _get_1h(b0, ["dangerous attacks"], ["1st half dangerous attacks","dangerous attacks 1st half","first half dangerous attacks"])
        d1 = _get_1h(b1, ["dangerous attacks"], ["1st half dangerous attacks","dangerous attacks 1st half","first half dangerous attacks"])
        sot = (s0 if s0 is not None else 0.0) + (s1 if s1 is not None else 0.0) if (s0 is not None or s1 is not None) else None
        da  = (d0 if d0 is not None else 0.0) + (d1 if d1 is not None else 0.0) if (d0 is not None or d1 is not None) else None
        return (sot, da)

    for team_id, matches in (team_last_matches or {}).items():
        for m in matches or []:
            fid = ((m.get('fixture') or {}).get('id'))
            if not fid or fid in seen:
                continue
            seen.add(fid)
            lid = ((m.get('league') or {}).get('id'))
            if lid is None:
                lid = -1  # global-only
            stats = stats_fn(fid)
            sot, da = _extract_1h_totals_both(stats)
            if sot is not None:
                by_lid.setdefault(lid, {"sot": [], "da": [], "hits": 0.0, "tot":0.0})
                by_lid[lid]["sot"].append(sot)
                global_sot.append(sot)
            if da is not None:
                by_lid.setdefault(lid, {"sot": [], "da": [], "hits": 0.0, "tot":0.0})
                by_lid[lid]["da"].append(da)
                global_da.append(da)
            # 1H≥1 gol
            if _ht_total_ge1(m):
                by_lid.setdefault(lid, {"sot": [], "da": [], "hits": 0.0, "tot":0.0})
                by_lid[lid]["hits"] += 1.0
                g_hits += 1.0
            by_lid.setdefault(lid, {"sot": [], "da": [], "hits": 0.0, "tot":0.0})
            by_lid[lid]["tot"] += 1.0
            g_tot += 1.0

    def _pack(arr_sot, arr_da, hits, tot):
        mu_sot = float(sum(arr_sot)/len(arr_sot)) if arr_sot else None
        mu_da  = float(sum(arr_da)/len(arr_da))  if arr_da  else None
        sd_sot = (sum((x-mu_sot)**2 for x in arr_sot)/max(1,len(arr_sot)-1))**0.5 if arr_sot and mu_sot is not None and len(arr_sot)>=2 else None
        sd_da  = (sum((x-mu_da)**2  for x in arr_da )/max(1,len(arr_da)-1))**0.5  if arr_da  and mu_da  is not None and len(arr_da) >=2 else None
        q95_sot = _percentile(arr_sot, 0.95)
        q95_da  = _percentile(arr_da, 0.95)
        m1h = float(hits/tot) if tot>0 else 0.55
        return {"mu_sot1h":mu_sot, "sd_sot1h":sd_sot, "q95_sot1h":q95_sot,
                "mu_da1h":mu_da,   "sd_da1h":sd_da,   "q95_da1h":q95_da,
                "m1h": m1h}

    leagues = {}
    for lid, obj in by_lid.items():
        leagues[lid] = _pack(obj["sot"], obj["da"], obj["hits"], obj["tot"])

    global_base = _pack(global_sot, global_da, g_hits, g_tot)
    # fallback: ako neki lid nema dovoljno, nasloni se na global
    for lid, base in leagues.items():
        for k,v in base.items():
            if v is None:
                base[k] = global_base.get(k)

    # zapamti global m1h za druge module (referee profile)
    try:
        compute_league_baselines._last_global_m1h = float(global_base.get("m1h") or 0.55)
    except Exception:
        compute_league_baselines._last_global_m1h = 0.55
    return {"global": global_base, "leagues": leagues}


def compute_team_strengths(team_last_matches, lam=5.0, max_n=15, m_global=0.55):
    """Napad (1H score >=1) i def_allow (1H conceded >=1) po timu, EB shrink na m_global."""
    strengths = {}
    for team_id, matches in (team_last_matches or {}).items():
        h_sc, w_sc, n_sc = _weighted_counts(matches, lambda m: _team_scored_1h(m, team_id), lam, max_n)
        h_con, w_con, n_con = _weighted_counts(matches, lambda m: _team_conceded_1h(m, team_id), lam, max_n)
        att = beta_shrunk_rate(h_sc, w_sc, m=m_global, tau=8.0)
        def_allow = beta_shrunk_rate(h_con, w_con, m=m_global, tau=8.0)
        strengths[team_id] = {"att": att, "def_allow": def_allow, "eff_n": (w_sc or 0)+(w_con or 0)}
    return strengths

# ====== EXTRA SIGNALS: referee / weather / venue / lineups / injuries ======

def _is_gg1h(m):
    ht = ((m.get('score') or {}).get('halftime') or {})
    return (ht.get('home') or 0) > 0 and (ht.get('away') or 0) > 0

@lru_cache(maxsize=2000)
def compute_referee_profile(ref_name: str, season: int | None) -> dict:
    """
    Povuče sezonske mečeve tog sudije (preko DataRepo) i vrati bias-e:
      - p1h_ge1, p1h_ge2, p1h_gg  (EB shrink na global m1h)
      - logit delte vs m1h za 1H≥1 (to koristimo kao 'ref_adj')
    Ako nema podataka → neutralno (0 adja).
    """
    try:
        if not ref_name:
            return {"p1h_ge1": None, "p1h_ge2": None, "p1h_gg": None, "ref_adj": 0.0, "used": 0.0}
        year = season or datetime.utcnow().year
        arr = repo.get_referee_fixtures(ref_name, season=year, last_n=200, no_api=False) or []
        # recency ponder (lakši)
        h1, w1, _ = _weighted_counts(sorted(arr, key=lambda m: (m.get('fixture') or {}).get('date',''), reverse=True),
                                     _ht_total_ge1, lam=8.0, max_n=200)
        h2, w2, _ = _weighted_counts(sorted(arr, key=lambda m: (m.get('fixture') or {}).get('date',''), reverse=True),
                                     _ht_total_ge2, lam=8.0, max_n=200)
        hg, wg, _ = _weighted_counts(sorted(arr, key=lambda m: (m.get('fixture') or {}).get('date',''), reverse=True),
                                     _is_gg1h, lam=8.0, max_n=200)
        # global m1h
        m_global = (compute_league_baselines.__dict__.get("_last_global_m1h")  # mali hack: setovaćemo ispod
                    if compute_league_baselines.__dict__.get("_last_global_m1h") is not None else 0.55)

        p1 = beta_shrunk_rate(h1, w1, m=m_global, tau=10.0) if w1 > 0 else m_global
        p2 = beta_shrunk_rate(h2, w2, m=max(0.05, m_global*m_global), tau=10.0) if w2 > 0 else max(0.05, m_global*m_global)
        pg = beta_shrunk_rate(hg, wg, m=(m_global*m_global*0.8 + 0.02), tau=10.0) if wg > 0 else (m_global*m_global*0.8 + 0.02)

        ref_adj = (_logit(min(0.99, max(0.01, p1))) - _logit(min(0.99, max(0.01, m_global))))
        return {"p1h_ge1": p1, "p1h_ge2": p2, "p1h_gg": pg, "ref_adj": ref_adj, "used": float(w1)}
    except Exception:
        return {"p1h_ge1": None, "p1h_ge2": None, "p1h_gg": None, "ref_adj": 0.0, "used": 0.0}

def _coerce_float(x):
    try:
        if x is None: return None
        if isinstance(x, (int,float)): return float(x)
        s = str(x)
        if s.endswith("km/h"): s = s.replace("km/h","").strip()
        if s.endswith("%"): s = s[:-1].strip()
        return float(s)
    except:
        return None

def _extract_weather(fx_full: dict) -> dict:
    """
    Vrati dict: {"temp_c":..., "wind_kmh":..., "humidity":...}
    API-Football ume da vrati 'weather' sa raznim ključevima; budimo tolerantni.
    """
    w = ((fx_full or {}).get("fixture") or {}).get("weather") or {}
    if not isinstance(w, dict):
        w = {}
    temp = _coerce_float(w.get("temperature") or w.get("temp") or w.get("temperature_c") or w.get("temp_c"))
    wind = _coerce_float(w.get("wind") or w.get("wind_kmh") or w.get("windSpeed") or w.get("wind_speed"))
    hum  = _coerce_float(w.get("humidity") or w.get("humid") or w.get("humidity_percent"))
    return {"temp_c": temp, "wind_kmh": wind, "humidity": hum}

def _weather_adj(temp_c: float | None, wind_kmh: float | None, humidity: float | None) -> float:
    """
    Daj mali logit adj u [-0.5, +0.5] ~ tempo.
      - vetar > 30 km/h → -0.25
      - ekstremne temp (<0 ili >30) → -0.20
      - vrlo ugodno (12..20C, vetar <15) → +0.08
    """
    adj = 0.0
    if wind_kmh is not None:
        if wind_kmh >= 35: adj -= 0.25
        elif wind_kmh >= 25: adj -= 0.12
    if temp_c is not None:
        if temp_c <= -2 or temp_c >= 32: adj -= 0.20
        elif 12 <= temp_c <= 20: adj += 0.08
    if humidity is not None and humidity >= 90:
        adj -= 0.05
    return max(-0.5, min(0.5, adj))

def _venue_adj(venue_obj: dict | None) -> float:
    """
    Blagi adj:
      - artificial / turf → +0.04 (brža lopta)
      - elevation > 1000m → -0.06 (umor, ređi vazduh → ponekad suprotno, ali budimo konzervativni)
    """
    if not venue_obj: return 0.0
    surf = (venue_obj.get("surface") or "").lower()
    elev = _coerce_float(venue_obj.get("elevation") or venue_obj.get("elevation_m"))
    adj = 0.0
    if "artificial" in surf or "turf" in surf: adj += 0.04
    if elev is not None and elev > 1000: adj -= 0.06
    return max(-0.2, min(0.2, adj))

def _lineups_adj(lineups: list | None) -> tuple[float, dict]:
    """
    Ako su lineup-ovi dostupni, proceni napadački sastav:
      - prebroj 'F'/'FW'/'Attacker' u startnih 11 oba tima; <2 ukupno → -0.12 ; >=4 → +0.06
    Ako lineups nije spreman → adj=0.0
    """
    try:
        if not lineups or len(lineups) < 1: 
            return 0.0, {"fw_count": None, "have": False}
        starters = 0
        fw = 0
        for team in lineups:
            sx = (team.get("startXI") or []) if isinstance(team, dict) else []
            for p in sx:
                pos = ((p.get("player") or {}).get("pos") or (p.get("player") or {}).get("position") or "" ).upper()
                grid = (p.get("player") or {}).get("grid") or ""
                starters += 1
                if pos in ("F","FW","ATTACKER") or ("-3-" in str(grid) or "-2-" in str(grid)):  # grubi heur.
                    fw += 1
        adj = 0.0
        if starters >= 18:  # lineup realno objavljen
            if fw <= 1: adj -= 0.12
            elif fw >= 4: adj += 0.06
            return adj, {"fw_count": fw, "have": True}
        return 0.0, {"fw_count": None, "have": False}
    except Exception:
        return 0.0, {"fw_count": None, "have": False}

def _injuries_adj(inj_list: list | None) -> tuple[float, dict]:
    """
    Nađi ukupan broj prijavljenih povreda/suspenzija; >6 → -0.10 ; >10 → -0.18
    """
    try:
        n = len(inj_list or [])
        adj = 0.0
        if n > 10: adj -= 0.18
        elif n > 6: adj -= 0.10
        return adj, {"inj_count": n}
    except Exception:
        return 0.0, {"inj_count": None}

def build_extras_for_fixture(fixture: dict, no_api: bool=False) -> dict:
    """
    Skupi: referee profile, weather, venue, lineups, injuries.
    Vraća dict sa adj-ovima (logit skala) i info za debug.
    """
    fid = ((fixture.get("fixture") or {}).get("id")) or ((fixture.get("fixture") or {}).get("id"))
    season = ((fixture.get("league") or {}).get("season"))
    fx_full = repo.get_fixture_full(fid, no_api=no_api)  # osvežava fixture_json ako treba
    # referee
    ref_name = (((fx_full or {}).get("fixture") or {}).get("referee") or
                ((fixture.get("fixture") or {}).get("referee")))
    ref_prof = compute_referee_profile(ref_name, season) if ref_name and not no_api else {"ref_adj": 0.0, "used": 0.0}
    # venue
    ven_id = (((fx_full or {}).get("fixture") or {}).get("venue") or {}).get("id") or \
             (((fixture.get("fixture") or {}).get("venue") or {}).get("id"))
    ven = repo.get_venue(ven_id, no_api=no_api) if ven_id else None
    ven_adj = _venue_adj(ven or {})
    # weather
    ww = _extract_weather(fx_full or {})
    w_adj = _weather_adj(ww.get("temp_c"), ww.get("wind_kmh"), ww.get("humidity"))
    # lineups (samo ako API dozvoljen; inače 0)
    lu = repo.get_lineups(fid, no_api=no_api) if not no_api else None
    lu_adj, lu_dbg = _lineups_adj(lu)
    # injuries
    inj = repo.get_injuries(fid, no_api=no_api) if not no_api else None
    inj_adj, inj_dbg = _injuries_adj(inj)

    return {
        "ref_adj": float(ref_prof.get("ref_adj") or 0.0),
        "ref_used": float(ref_prof.get("used") or 0.0),
        "ref_name": ref_name,

        "weather_adj": float(w_adj),
        "weather_obj": ww,

        "venue_adj": float(ven_adj),
        "venue_obj": ven,

        "lineup_adj": float(lu_adj),
        "lineups_have": bool(lu_dbg.get("have")),
        "lineups_fw_count": lu_dbg.get("fw_count"),

        "inj_adj": float(inj_adj),
        "inj_count": inj_dbg.get("inj_count"),
    }

def _weight_from_effn(p, eff_n):
    """Precizija ~ 1/Var(logit(p)). Aproksimiramo Var(p)=p(1-p)/eff_n, Var(logit)=Var(p)/(p^2(1-p)^2)."""
    p = min(1-1e-6, max(1e-6, p))
    var_p = p*(1-p)/max(1.0, eff_n)
    var_logit = var_p / (p*p*(1-p)*(1-p))
    return 1.0 / max(1e-6, var_logit)

def fuse_probs_by_precision(p1, effn1, p2, effn2):
    """Spoj dva izvora (npr. prior i micro) po preciziji (1/var). Vraća (p_merge, weight_of_p2)."""
    w1 = _weight_from_effn(p1, effn1)
    w2 = _weight_from_effn(p2, effn2)
    z = (_logit(p1)*w1 + _logit(p2)*w2) / (w1+w2)
    return _inv_logit(z), (w2/(w1+w2))

def _league_base_for_fixture(fixture, league_baselines):
    lid = ((fixture.get('league') or {}).get('id'))
    base = None
    if league_baselines and isinstance(league_baselines, dict):
        base = (league_baselines.get('leagues') or {}).get(lid)
        if not base:
            base = league_baselines.get('global')
    if not base:
        base = {"mu_sot1h": BASE_SOT1H_TOTAL, "sd_sot1h": 1.25, "q95_sot1h": SOT1H_CAP,
                "mu_da1h": BASE_DA1H_TOTAL, "sd_da1h": 12.0, "q95_da1h": DA1H_CAP,
                "m1h": 0.55}
    return base

def micro_over05_logistic(fixture, exp_dict, league_baselines, team_strengths):
    base = _league_base_for_fixture(fixture, league_baselines)
    m = base["m1h"]
    z_sot = _z(exp_dict.get('exp_sot1h_total'), base["mu_sot1h"], base["sd_sot1h"])
    z_da  = _z(exp_dict.get('exp_da1h_total'),  base["mu_da1h"],  base["sd_da1h"])
    pos   = exp_dict.get('pos_edge') or 0.0

    home_id = ((fixture.get('teams') or {}).get('home') or {}).get('id')
    away_id = ((fixture.get('teams') or {}).get('away') or {}).get('id')
    sA = (team_strengths or {}).get(home_id, {"att": m, "def_allow": m})
    sB = (team_strengths or {}).get(away_id, {"att": m, "def_allow": m})
    # opponent-adjusted net “pace-to-score”:
    net_strength = (sA['att'] - m) + (sB['def_allow'] - m) + (sB['att'] - m) + (sA['def_allow'] - m)

    b0 = _logit(m)  # kad nema signala -> ligaški prosjek
    # koeficijenti konzervativni (kalibrisati kasnije)
    z = b0 + 0.55*z_sot + 0.25*z_da + 0.25*pos + 0.80*net_strength + 0.08  # mini home-adv
    return max(0.0, min(1.0, _inv_logit(z)))

def micro_team_scores1h_logistic(fixture, exp_dict, league_baselines, team_strengths, side='home'):
    base = _league_base_for_fixture(fixture, league_baselines)
    m = base["m1h"]

    # per-team očekivanja
    if side == 'home':
        exp_sot = exp_dict.get('exp_sot1h_home'); exp_da = exp_dict.get('exp_da1h_home')
        pos_share = exp_dict.get('pos_edge') or 0.0
    else:
        exp_sot = exp_dict.get('exp_sot1h_away'); exp_da = exp_dict.get('exp_da1h_away')
        pos_share = -(exp_dict.get('pos_edge') or 0.0)

    # pretvori total mu/sigma na "po timu"
    mu_sot_h = (base["mu_sot1h"] or 0.0) / 2.0
    sd_sot_h = (base["sd_sot1h"] or 1.0) / (2**0.5)
    mu_da_h  = (base["mu_da1h"] or 0.0) / 2.0
    sd_da_h  = (base["sd_da1h"] or 1.0)  / (2**0.5)

    z_sot = _z(exp_sot, mu_sot_h, sd_sot_h)
    z_da  = _z(exp_da,  mu_da_h,  sd_da_h)

    home_id = ((fixture.get('teams') or {}).get('home') or {}).get('id')
    away_id = ((fixture.get('teams') or {}).get('away') or {}).get('id')
    team_id = home_id if side=='home' else away_id
    opp_id  = away_id if side=='home' else home_id

    sT  = (team_strengths or {}).get(team_id, {"att": m, "def_allow": m})
    sOpp = (team_strengths or {}).get(opp_id, {"att": m, "def_allow": m})

    # baseline za “tim postiže 1H gol” = EB napad tima
    b0 = _logit(sT['att'])
    opp_diff = (sOpp['def_allow'] - m)

    z = b0 + 0.45*z_sot + 0.20*z_da + 0.20*pos_share + 0.60*opp_diff + (0.04 if side=='home' else 0.0)
    return max(0.0, min(1.0, _inv_logit(z)))

def _weighted_match_over05_rate(matches, lam=5.0, max_n=15):
    h, w, n = _weighted_counts(matches, _ht_total_ge1, lam, max_n)
    p = (h / w) if w > 0 else None
    return p, h, w

def _weighted_h2h_over05_rate(h2h_matches, lam=4.0, max_n=10):
    # H2H često malo relevantno → kraći lam i max_n
    h, w, n = _weighted_counts(sorted(h2h_matches or [], key=lambda m: (m.get('fixture') or {}).get('date',''), reverse=True),
                               _ht_total_ge1, lam, max_n)
    p = (h / w) if w > 0 else None
    return p, h, w

def combine_prior_with_micro(prior_p, p_micro, coverage):
    # što više mečeva u mikro formi — veći uticaj; cap na 1
    w = min(1.0, (coverage or 0) / 8.0)  # 0..1, npr. 8 mečeva => full weight
    prior_logit = _logit(prior_p)
    micro_logit = _logit(p_micro)
    final_logit = (1 - w) * prior_logit + w * micro_logit
    return _inv_logit(final_logit), w

# --------------------------- DATA FETCHING ---------------------------
def get_fixtures_in_time_range(start_dt: datetime, end_dt: datetime, from_hour=None, to_hour=None, no_api: bool = True):
    """
    DB-ONLY: čita iz fixtures tabele (koju je seed popunio). Nema poziva ka API-ju.
    """
    global REJECTED_COMP_COUNTER, FIXTURES_FETCH_SOURCE
    REJECTED_COMP_COUNTER = {}

    raw_db = _read_fixtures_from_db(start_dt, end_dt)
    fixtures = []
    cut_time = 0
    cut_comp = 0
    for match in raw_db:
        ds = (match.get("fixture") or {}).get("date")
        if not ds:
            continue
        if not is_fixture_in_range(ds, start_dt, end_dt, from_hour, to_hour):
            cut_time += 1
            continue
        ok, reason = is_valid_competition_with_reason(match)
        if not ok:
            cut_comp += 1
            _bump_reject_counter(match, reason)
            continue
        lgname_raw = (match.get("league") or {}).get("name") or ""
        home_n = ((match.get("teams") or {}).get("home") or {}).get("name") or ""
        away_n = ((match.get("teams") or {}).get("away") or {}).get("name") or ""
        if _is_youth_or_reserve_comp_name(lgname_raw) or _is_reserve_team(home_n) or _is_reserve_team(away_n):
            cut_comp += 1
            _bump_reject_counter(match, "youth_or_reserve_extra_guard")
            continue
        fixtures.append(match)

    print(f"🗃️ DB fixtures: {len(fixtures)} (time_reject={cut_time}, comp_reject={cut_comp})")
    FIXTURES_FETCH_SOURCE = "db"
    return fixtures


# ---------------------------- ANALYTICS -----------------------------
def fetch_last_matches_for_teams(fixtures, last_n=30, no_api: bool = False):
    """
    Uvijek traži tačno last_n po timu (bez dinamičkog skraćivanja).
    Ako no_api=True i nema pun cache za traženi last_n, vrati šta ima u kešu (može biti 0),
    ali NE smanjuj last_n.
    """
    team_ids = {f['teams']['home']['id'] for f in fixtures} | {f['teams']['away']['id'] for f in fixtures}
    team_count = len(team_ids)

    last_n_eff = last_n  # ⬅️ nema više BUDGET//team_count
    print(f"ℹ️ unique_teams={team_count}, requested_last_n={last_n}, effective_last_n={last_n_eff}")

    team_last_matches = {}
    for team_id in team_ids:
        team_last_matches[team_id] = get_or_fetch_team_history(
            team_id, last_n=last_n_eff, force_refresh=False, no_api=no_api
        )
    return team_last_matches

def fetch_h2h_matches(fixtures, last_n=10, no_api: bool = False):
    h2h_results = {}
    for fixture in fixtures:
        a, b = sorted([fixture['teams']['home']['id'], fixture['teams']['away']['id']])
        key = f"{a}-{b}"
        if key in h2h_results:
            continue
        h2h_results[key] = get_or_fetch_h2h(a, b, last_n=last_n, no_api=no_api)
        time.sleep(0.1)
    return h2h_results

def fetch_fixture_statistics_bulk(team_matches):
    statistics_results = {}

    for team_id, matches in team_matches.items():
        for match in matches:
            fixture_id = match['fixture']['id']
            if fixture_id in statistics_results:
                continue  # već je povučeno

            response = rate_limited_request(f"{BASE_URL}/fixtures/statistics", params={"fixture": fixture_id})
            if response and 'response' in response and response['response']:
                statistics_results[fixture_id] = response['response']
            time.sleep(0.5)

    return statistics_results

def get_fixture_statistics(fixture_id):
    response = rate_limited_request(f"{BASE_URL}/fixtures/statistics", params={"fixture": fixture_id})
    if response and 'response' in response:
        return response['response']
    return None

# ---------------------------- DB STORAGE - FUTURE MATCHES -----------------------------
def store_fixture_data_in_db(fixtures):
    if not fixtures:
        return
    with DB_WRITE_LOCK:  # jedan “bulk” upis
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("BEGIN IMMEDIATE;")

        for fixture in fixtures:
            fixture_id = fixture['fixture']['id']
            fixture_date = fixture['fixture']['date']
            league_id = fixture['league']['id']
            team_home_id = fixture['teams']['home']['id']
            team_away_id = fixture['teams']['away']['id']

            # ⚠️ NEMA nikakvog poziva koji PIŠE u match_statistics ovde!
            # Ako želiš da popuniš stats_json kolonu — koristi samo READ helper:
            stats = try_read_fixture_statistics(fixture_id)  # ovo je NON-WRITE
            stats_json = json.dumps(stats, ensure_ascii=False, default=str) if stats else None
            fixture_json = json.dumps(fixture, ensure_ascii=False, default=str)

            cur.execute("""
                INSERT OR REPLACE INTO fixtures
                (id, date, league_id, team_home_id, team_away_id, stats_json, fixture_json)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                fixture_id, fixture_date, league_id, team_home_id, team_away_id, stats_json, fixture_json
            ))

        conn.commit()
        conn.close()
    print(f"✅ Stored {len(fixtures)} fixtures in DB.")

# ---------------------------- STORE TO DB ALL HISTORY DATA -----------------------------
def _has_fixtures_for_day(d: date) -> bool:
    return _db_has_fixtures_for_day(d)

def _list_fixtures_for_day(d: date):
    return _read_fixtures_for_day(d)

def _fresh_cutoff_iso(hours: int) -> str:
    return (datetime.utcnow() - timedelta(hours=hours)).isoformat()

def _history_missing(team_ids, last_n: int, ttl_h: int):
    """vrati listu timova kojima fali friška istorija (team_history_cache)"""
    cutoff = _fresh_cutoff_iso(ttl_h)
    conn = get_db_connection()
    cur = conn.cursor()
    # tabela možda ne postoji – napravi je benigno (kao u get_or_fetch_team_history)
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

    missing = []
    for tid in team_ids:
        cur.execute("SELECT updated_at FROM team_history_cache WHERE team_id=? AND last_n=?", (tid, last_n))
        row = cur.fetchone()
        if not row: 
            missing.append(tid); 
            continue
        try:
            if row["updated_at"] < cutoff:
                missing.append(tid)
        except Exception:
            missing.append(tid)
    conn.close()
    return missing

def _h2h_missing(pairs, last_n: int, ttl_h: int):
    """vrati listu parova (a,b) kojima fali friški h2h_cache"""
    cutoff = _fresh_cutoff_iso(ttl_h)
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

    missing = []
    for a, b in pairs:
        x, y = sorted([a, b])
        cur.execute("SELECT updated_at FROM h2h_cache WHERE team1_id=? AND team2_id=? AND last_n=?", (x, y, last_n))
        row = cur.fetchone()
        if not row:
            missing.append((x, y))
            continue
        try:
            if row["updated_at"] < cutoff:
                missing.append((x, y))
        except Exception:
            missing.append((x, y))
    conn.close()
    return missing

@app.post("/api/prepare-day")
async def api_prepare_day(request: Request):
    """
    Jedno dugme:
      - proveri fixtures za dan; ako nema → seed
      - proveri history/h2h; ako fali → dopuni samo nedostajuće
      - opciono prewarm statistika (povlači SAMO ono što fali)
    """
    try:
        try:
            payload = await request.json()
        except Exception:
            payload = {}
        date_str = (payload or {}).get("date") or request.query_params.get("date")
        prewarm  = (payload or {}).get("prewarm", True)
        if not date_str:
            # default: današnji lokalni dan
            d_local = datetime.now(USER_TZ).date()
        else:
            d_local = date.fromisoformat(date_str)

        # 1) fixtures
        seeded = False
        if not _has_fixtures_for_day(d_local):
            seed_day_into_db(d_local)
            seeded = True

        fixtures = _list_fixtures_for_day(d_local)
        team_ids = {((f.get("teams") or {}).get("home") or {}).get("id") for f in fixtures} | \
                   {((f.get("teams") or {}).get("away") or {}).get("id") for f in fixtures}
        team_ids = {t for t in team_ids if t is not None}
        pairs = set()
        for f in fixtures:
            h = ((f.get("teams") or {}).get("home") or {}).get("id")
            a = ((f.get("teams") or {}).get("away") or {}).get("id")
            if h is None or a is None: 
                continue
            x, y = sorted([h, a])
            pairs.add((x, y))

        # 2) šta fali od history/h2h?
        hist_missing = _history_missing(team_ids, DAY_PREFETCH_LAST_N, CACHE_TTL_HOURS)
        h2h_missing  = _h2h_missing(pairs,  DAY_PREFETCH_H2H_N,  CACHE_TTL_HOURS)

        if hist_missing or h2h_missing:
            # dopuni SAMO što fali (get_or_fetch* radi TTL check)
            # (fetch_and_store će pozvati get_or_fetch za sve, ali ono ne dira friške)
            fetch_and_store_all_historical_data(fixtures, no_api=False)
        else:
            # sve friško — ništa ne vučemo
            pass

        # 3) stats prewarm (samo missing)
        stats_missing_before = 0
        if prewarm:
            # prvo pročitaj istoriju (sada bi trebalo da je sve u cache-u)
            team_last = fetch_last_matches_for_teams(fixtures, last_n=DAY_PREFETCH_LAST_N, no_api=True)

            # izračunaj koliko fali u match_statistics
            all_fids = set()
            for matches in (team_last or {}).values():
                for m in matches or []:
                    fid = ((m.get("fixture") or {}).get("id"))
                    if fid:
                        all_fids.add(fid)
            existing = _select_existing_fixture_ids(list(all_fids))
            stats_missing_before = len(all_fids - existing)

            # prewarm povlači samo one koje fale (ima hard-cap i mali paralelizam)
            prewarm_statistics_cache(team_last, max_workers=2)

        return JSONResponse(content={
            "ok": True,
            "day": d_local.isoformat(),
            "fixtures_in_db": len(fixtures),
            "teams": len(team_ids),
            "pairs": len(pairs),
            "seeded": seeded,
            "history_missing_before": len(hist_missing),
            "h2h_missing_before": len(h2h_missing),
            "stats_missing_before": stats_missing_before
        }, status_code=200)

    except Exception as e:
        print("prepare-day error:", e)
        print(traceback.format_exc())
        return JSONResponse(status_code=500, content={"error":"prepare_failed","detail":str(e)})
def fetch_and_store_all_historical_data(fixtures, no_api: bool = False):
    # 1) last-30 po timu (sa kešom / no_api)
    all_team_matches = fetch_last_matches_for_teams(fixtures, last_n=15, no_api=no_api)

    # 2) Prewarm statistika samo ako smije API
    if not no_api:
        prewarm_statistics_cache(all_team_matches, max_workers=2)

    # 3) Upis u lokalnu bazu
    seen_teams = set()
    seen_h2h_keys = set()
    h2h_results_all = {}

    for fixture in fixtures:
        home_id = fixture['teams']['home']['id']
        away_id = fixture['teams']['away']['id']

        for team_id in (home_id, away_id):
            if team_id in seen_teams:
                continue
            seen_teams.add(team_id)
            matches = all_team_matches.get(team_id, [])
            if matches:
                insert_team_matches(team_id, matches)

        a, b = sorted([home_id, away_id])
        h2h_key = f"{a}-{b}"
        if h2h_key not in seen_h2h_keys:
            seen_h2h_keys.add(h2h_key)
            h2h = get_or_fetch_h2h(a, b, last_n=10, no_api=no_api)
            h2h_results_all[h2h_key] = h2h
            insert_h2h_matches(a, b, h2h)

    print("✅ Istorijski podaci povučeni i sačuvani (no_api=%s)." % no_api)
    return all_team_matches, h2h_results_all

def team_1h_goal_stats(team_matches):
    """Vrati (percent, hits, total) za 1H gol >=1 iz istorije tima."""
    total = 0
    hits = 0
    for m in team_matches or []:
        ht = (m.get('score', {}) or {}).get('halftime', {}) or {}
        h = ht.get('home') or 0
        a = ht.get('away') or 0
        if h is None: h = 0
        if a is None: a = 0
        total += 1
        if (h + a) >= 1:
            hits += 1
    pct = round((hits / total) * 100, 2) if total else 0
    return pct, hits, total

def h2h_1h_goal_stats(h2h_matches):
    """Vrati (percent, hits, total) za 1H gol >=1 iz H2H istorije."""
    total = 0
    hits = 0
    for m in h2h_matches or []:
        ht = (m.get('score', {}) or {}).get('halftime', {}) or {}
        h = ht.get('home') or 0
        a = ht.get('away') or 0
        if h is None: h = 0
        if a is None: a = 0
        total += 1
        if (h + a) >= 1:
            hits += 1
    pct = round((hits / total) * 100, 2) if total else 0
    return pct, hits, total

def team_1h_gg_stats(team_matches):
    """Koliko puta je bilo GG u 1. poluvremenu u mečevima jednog tima (poslednjih N)."""
    total = 0
    hits = 0
    for m in team_matches or []:
        ht = (m.get('score', {}) or {}).get('halftime', {}) or {}
        h = ht.get('home') or 0
        a = ht.get('away') or 0
        if h is None: h = 0
        if a is None: a = 0
        total += 1
        if h > 0 and a > 0:
            hits += 1
    pct = round((hits / total) * 100.0, 2) if total else 0.0
    return pct, hits, total

def h2h_1h_gg_stats(h2h_matches):
    """Koliko puta je bilo GG u 1. poluvremenu u H2H mečevima."""
    total = 0
    hits = 0
    for m in h2h_matches or []:
        ht = (m.get('score', {}) or {}).get('halftime', {}) or {}
        h = ht.get('home') or 0
        a = ht.get('away') or 0
        if h is None: h = 0
        if a is None: a = 0
        total += 1
        if h > 0 and a > 0:
            hits += 1
    pct = round((hits / total) * 100.0, 2) if total else 0.0
    return pct, hits, total

# def calculate_shot_attack_percentages(team_last_matches):
#     team_scores = {}
#     for team_id, matches in team_last_matches.items():
#         total_shots = 0
#         total_attacks = 0
#         valid_matches = 0

#         for match in matches:
#             fixture_id = match['fixture']['id']
#             stats_response = get_or_fetch_fixture_statistics(fixture_id)
#             if not stats_response:
#                 continue
#             team_stats = next((item for item in stats_response if item['team']['id'] == team_id), None)
#             if not team_stats:
#                 continue

#             shots = next((s['value'] for s in team_stats['statistics'] if s['type'] == 'Shots on Goal'), 0)
#             attacks = next((s['value'] for s in team_stats['statistics'] if s['type'] == 'Dangerous Attacks'), 0)

#             if isinstance(shots, int) and isinstance(attacks, int):
#                 total_shots += shots
#                 total_attacks += attacks
#                 valid_matches += 1

#             time.sleep(0.05)

#         if valid_matches == 0:
#             team_scores[team_id] = {'shots_percent': 0, 'attacks_percent': 0}
#             continue

#         avg_shots = total_shots / valid_matches
#         avg_attacks = total_attacks / valid_matches
#         shots_percent = min(round((avg_shots / 9) * 100, 2), 100)
#         attacks_percent = min(round((avg_attacks / 41) * 100, 2), 100)

#         team_scores[team_id] = {
#             'shots_percent': shots_percent,
#             'attacks_percent': attacks_percent,
#             'used': valid_matches,  # ➕ da znaš da li ima podataka
#         }

#     return team_scores
@lru_cache(maxsize=2000)
def get_fixture_details(fixture_id):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT fixture_json FROM fixtures WHERE id=?", (fixture_id,))
    row = cur.fetchone()
    conn.close()
    if not row or not row[0]:
        return {}
    try:
        return json.loads(row[0])
    except Exception:
        return {}

@lru_cache(maxsize=2000)
def get_standings(league_id, season):
    res = rate_limited_request(f"{BASE_URL}/standings", params={"league": league_id, "season": season})
    if not isinstance(res, dict):
        return []
    arr = res.get("response") or []
    if not arr:
        return []
    league_obj = arr[0].get("league", {}) if isinstance(arr[0], dict) else {}
    return league_obj.get("standings", []) or []

def calculate_match_importance(fixture_id):
    fixture = get_fixture_details(fixture_id)
    if not fixture:
        return 5  # neutralno (bez bias-a)
    league = fixture.get("league", {})
    home_id = fixture.get("teams", {}).get("home", {}).get("id")
    away_id = fixture.get("teams", {}).get("away", {}).get("id")
    importance_score = 0

    lname = league.get("name", "").lower()
    if any(x in lname for x in ["champions", "europa", "libertadores", "world cup", "nations"]):
        importance_score += 3
    elif any(x in lname for x in ["premier", "la liga", "serie", "bundesliga", "liga", "league"]):
        importance_score += 2
    elif "friendly" in lname:
        importance_score += 0
    else:
        importance_score += 1

    round_name = league.get("round", "").lower()
    if any(x in round_name for x in ["final", "semi", "quarter"]):
        importance_score += 3
    elif any(x in round_name for x in ["group", "regular"]):
        importance_score += 1

    try:
        standings = get_standings(league["id"], league["season"])
        for group in standings:
            for team in group:
                tid = team["team"]["id"]
                rank = team["rank"]
                if tid == home_id or tid == away_id:
                    if rank <= 3 or rank >= (len(group) - 2):
                        importance_score += 2
    except:
        pass

    return min(importance_score, 10)

def calculate_final_probability(fixture, team_last_matches, h2h_results, micro_db,
                                league_baselines, team_strengths, team_profiles,
                                extras: dict | None = None, no_api: bool = False,
                                market_odds_over05_1h: Optional[float] = None):
    """
    Finalna vjerovatnoća za 1H Over 0.5, sa konzervativnijim H2H i class-gap korekcijom korelacije.
    - H2H EB tau=12.0, eff_n skaliran 0.4x, w<2.5 -> ignorišemo H2H
    - ρ = ρ_base(tempo) * ρ_factor(class_gap)
    """
    # ---- H2H parametri ----
    H2H_TAU   = 12.0
    H2H_SCALE = 0.4
    H2H_MIN_W = 2.5

    home_id = ((fixture.get('teams') or {}).get('home') or {}).get('id')
    away_id = ((fixture.get('teams') or {}).get('away') or {}).get('id')
    a, b = sorted([home_id, away_id])
    h2h_key = f"{a}-{b}"

    base = _league_base_for_fixture(fixture, league_baselines)
    m = base["m1h"]

    # --- PRIOR: history oba tima + H2H (EB i skalirana preciznost) ---
    p_home_raw, h_home, w_home = _weighted_match_over05_rate(team_last_matches.get(home_id, []), lam=5.0, max_n=15)
    p_away_raw, h_away, w_away = _weighted_match_over05_rate(team_last_matches.get(away_id, []), lam=5.0, max_n=15)

    p_home = beta_shrunk_rate(h_home, w_home, m=m, tau=8.0) if p_home_raw is not None else m
    p_away = beta_shrunk_rate(h_away, w_away, m=m, tau=8.0) if p_away_raw is not None else m
    p_team_prior = (p_home + p_away) / 2.0

    p_h2h_raw, h_h2h, w_h2h = _weighted_h2h_over05_rate(h2h_results.get(h2h_key, []), lam=4.0, max_n=10)
    p_h2h = beta_shrunk_rate(h_h2h, w_h2h, m=m, tau=H2H_TAU) if p_h2h_raw is not None else m

    effn_h2h = (w_h2h or 0.0) * H2H_SCALE
    if (w_h2h or 0.0) < H2H_MIN_W:
        effn_h2h = 0.0
        p_h2h = m

    p_prior, _ = fuse_probs_by_precision(
        p_team_prior, (w_home or 0.0) + (w_away or 0.0),
        p_h2h,        effn_h2h
    )

    # --- MICRO: p(home scores) & p(away scores) + korelacija (tempo × class-gap) ---
    feats = matchup_features_enhanced(fixture, team_profiles, league_baselines, micro_db=micro_db, extras=extras)

    # totals za UI
    exp_sot_total = None
    if feats.get('exp_sot1h_home') is not None or feats.get('exp_sot1h_away') is not None:
        exp_sot_total = round((feats.get('exp_sot1h_home') or 0) + (feats.get('exp_sot1h_away') or 0), 3)
    exp_da_total = None
    if feats.get('exp_da1h_home') is not None or feats.get('exp_da1h_away') is not None:
        exp_da_total = round((feats.get('exp_da1h_home') or 0) + (feats.get('exp_da1h_away') or 0), 3)

    p_home_goal, dbg_h = predict_team_scores1h_enhanced(fixture, feats, league_baselines, team_strengths, side='home')
    p_away_goal, dbg_a = predict_team_scores1h_enhanced(fixture, feats, league_baselines, team_strengths, side='away')

    # ρ = ρ_base(tempo) * ρ_factor(class_gap)
    pace_z = _z(feats.get('pace_sot_total'), (base["mu_sot1h"] or 0.0), (base["sd_sot1h"] or 1.0))
    rho_base = max(-0.05, min(0.20, 0.05 + 0.05 * pace_z))
    class_gap_abs = abs(feats.get('tier_gap_home') or 0.0)
    rho_factor = max(0.5, 1.0 - 0.20 * class_gap_abs)  # gap=2 -> ×0.6
    rho = rho_base * rho_factor

    no_goal_indep = (1.0 - p_home_goal) * (1.0 - p_away_goal)
    cov_term = rho * math.sqrt(max(0.0, p_home_goal*(1.0-p_home_goal)*p_away_goal*(1.0-p_away_goal)))
    p_micro = 1.0 - max(0.0, no_goal_indep - cov_term)
    p_micro = max(0.0, min(1.0, p_micro))

    # precizije
    h_form = (micro_db.get(home_id) or {}).get("home") or {}
    a_form = (micro_db.get(away_id) or {}).get("away") or {}
    effn_micro = (h_form.get('used_sot',0) + a_form.get('used_sot',0) +
                  h_form.get('used_da',0)  + a_form.get('used_da',0)  +
                  h_form.get('used_pos',0) + a_form.get('used_pos',0)) / 2.0
    effn_micro = max(1.0, effn_micro + (team_profiles.get(home_id,{}).get('eff_n',0) +
                                        team_profiles.get(away_id,{}).get('eff_n',0))/4.0)

    effn_prior = max(1.0,
        (w_home or 0.0) + (w_away or 0.0)
        + (team_strengths.get(home_id,{}).get('eff_n',0))
        + (team_strengths.get(away_id,{}).get('eff_n',0))
        + effn_h2h
    )

    p_final, w_micro_share = fuse_probs_by_precision(p_prior, effn_prior, p_micro, effn_micro)

    debug = {
        "prior_percent": round(p_prior*100,2),
        "micro_percent": round(p_micro*100,2),
        "merge_weight_micro": round(w_micro_share,3),
        "p_home_scores_1h": round(p_home_goal*100,2),
        "p_away_scores_1h": round(p_away_goal*100,2),
        "rho_base": round(rho_base,3),
        "rho_factor": round(rho_factor,3),
        "rho": round(rho,3),
        "tier_home": team_profiles.get(home_id,{}).get("tier"),
        "tier_away": team_profiles.get(away_id,{}).get("tier"),
        "tier_gap_home": feats.get('tier_gap_home'),
        "tier_gap_away": feats.get('tier_gap_away'),
        "exp_sot1h_home": feats.get('exp_sot1h_home'),
        "exp_sot1h_away": feats.get('exp_sot1h_away'),
        "exp_da1h_home":  feats.get('exp_da1h_home'),
        "exp_da1h_away":  feats.get('exp_da1h_away'),
        "pos_edge_percent": None if feats.get('pos_edge') is None else round(feats['pos_edge']*100,2),
        "home_dbg": dbg_h, "away_dbg": dbg_a,
        "m_league": round(m*100,2),
        "effn_prior": round(effn_prior, 3),
        "effn_micro": round(effn_micro, 3),
        "exp_sot1h_total": exp_sot_total,
        "exp_da1h_total": exp_da_total,
        "ref_name": feats.get('ref_name'),
        "ref_used": feats.get('ref_used'),
        "weather": feats.get('weather_adj'),
        "venue": feats.get('venue_adj'),
        "lineups_have": feats.get('lineups_have'),
        "lineups_fw_count": feats.get('lineups_fw_count'),
        "inj_count": feats.get('inj_count'),
    }
    p_final_market = blend_with_market(p_final, market_odds_over05_1h, alpha=ALPHA_MODEL)
    return round(p_final_market*100,2), debug

def calculate_final_probability_gg(fixture, team_last_matches, h2h_results, micro_db,
                                   league_baselines, team_strengths, team_profiles,
                                   extras: dict | None = None, no_api: bool = False,
                                   market_odds_btts_1h: Optional[float] = None):
    """
    Finalna vjerovatnoća za 1H GG, sa konzervativnijim H2H i class-gap korekcijom korelacije.
    - H2H EB tau=12.0, eff_n skaliran 0.4x, w<2.5 -> ignorišemo H2H
    - ρ = ρ_base(tempo) * ρ_factor(class_gap)
    """
    # ---- H2H parametri ----
    H2H_TAU   = 12.0
    H2H_SCALE = 0.4
    H2H_MIN_W = 2.5

    home_id = ((fixture.get('teams') or {}).get('home') or {}).get('id')
    away_id = ((fixture.get('teams') or {}).get('away') or {}).get('id')
    a, b = sorted([home_id, away_id])
    h2h_key = f"{a}-{b}"

    base = _league_base_for_fixture(fixture, league_baselines)
    m = base["m1h"]

    # PRIOR baza: produkt EB napada (timovi) + H2H EB (konzervativniji)
    sA = (team_strengths or {}).get(home_id, {"att": m})
    sB = (team_strengths or {}).get(away_id, {"att": m})
    p_home_base = sA["att"]; p_away_base = sB["att"]

    def _is_gg1h(m_):
        ht = ((m_.get('score') or {}).get('halftime') or {})
        return (ht.get('home') or 0)>0 and (ht.get('away') or 0)>0

    h, w, _ = _weighted_counts(h2h_results.get(h2h_key, []), _is_gg1h, lam=4.0, max_n=10)
    p_h2h = beta_shrunk_rate(h, w, m=(p_home_base*p_away_base), tau=H2H_TAU) if w>0 else (p_home_base*p_away_base)

    effn_h2h = (w or 0.0) * H2H_SCALE
    if (w or 0.0) < H2H_MIN_W:
        effn_h2h = 0.0
        p_h2h = (p_home_base * p_away_base)

    # --- MICRO: p(home scores) & p(away scores) + korelacija (tempo × class-gap) ---
    feats = matchup_features_enhanced(fixture, team_profiles, league_baselines, micro_db=micro_db, extras=extras)

    exp_sot_total = None
    if feats.get('exp_sot1h_home') is not None or feats.get('exp_sot1h_away') is not None:
        exp_sot_total = round((feats.get('exp_sot1h_home') or 0) + (feats.get('exp_sot1h_away') or 0), 3)
    exp_da_total = None
    if feats.get('exp_da1h_home') is not None or feats.get('exp_da1h_away') is not None:
        exp_da_total = round((feats.get('exp_da1h_home') or 0) + (feats.get('exp_da1h_away') or 0), 3)

    p_home, dbg_h = predict_team_scores1h_enhanced(fixture, feats, league_baselines, team_strengths, side='home')
    p_away, dbg_a = predict_team_scores1h_enhanced(fixture, feats, league_baselines, team_strengths, side='away')

    # ρ = ρ_base(tempo) * ρ_factor(class_gap)
    pace_z = 0.5*(_z(feats.get('pace_sot_total'), (base["mu_sot1h"] or 0.0), (base["sd_sot1h"] or 1.0)) +
                  _z(feats.get('pace_da_total'),  (base["mu_da1h"]  or 0.0), (base["sd_da1h"]  or 1.0)))
    rho_base = max(0.0, min(0.35, 0.15 + 0.06 * pace_z))
    class_gap_abs = abs(feats.get('tier_gap_home') or 0.0)
    rho_factor = max(0.5, 1.0 - 0.20 * class_gap_abs)  # gap=2 -> ×0.6
    rho = rho_base * rho_factor

    p_micro = p_home * p_away + rho * math.sqrt(max(0.0, p_home*(1.0-p_home)*p_away*(1.0-p_away)))
    p_micro = max(0.0, min(1.0, p_micro))

    # precizije
    h_form = (micro_db.get(home_id) or {}).get("home") or {}
    a_form = (micro_db.get(away_id) or {}).get("away") or {}
    effn_micro = (h_form.get('used_sot',0)+a_form.get('used_sot',0)+
                  h_form.get('used_da',0)+a_form.get('used_da',0)+
                  h_form.get('used_pos',0)+a_form.get('used_pos',0)) / 2.0
    effn_micro = max(1.0, effn_micro + (team_profiles.get(home_id,{}).get('eff_n',0) +
                                        team_profiles.get(away_id,{}).get('eff_n',0))/4.0)

    effn_prior = max(1.0,
        (team_strengths.get(home_id,{}).get('eff_n',0)) +
        (team_strengths.get(away_id,{}).get('eff_n',0)) +
        effn_h2h
    )

    p_final, w_micro_share = fuse_probs_by_precision(p_h2h, effn_prior, p_micro, effn_micro)

    debug = {
        "p_home_scores_1h": round(p_home*100,2),
        "p_away_scores_1h": round(p_away*100,2),
        "prior_percent": round(p_h2h*100,2),
        "micro_percent": round(p_micro*100,2),
        "merge_weight_micro": round(w_micro_share,3),
        "rho_base": round(rho_base,3),
        "rho_factor": round(rho_factor,3),
        "rho": round(rho,3),
        "tier_home": team_profiles.get(home_id,{}).get("tier"),
        "tier_away": team_profiles.get(away_id,{}).get("tier"),
        "tier_gap_home": feats.get('tier_gap_home'),
        "tier_gap_away": feats.get('tier_gap_away'),
        "exp_sot1h_home": feats.get('exp_sot1h_home'),
        "exp_sot1h_away": feats.get('exp_sot1h_away'),
        "exp_da1h_home":  feats.get('exp_da1h_home'),
        "exp_da1h_away":  feats.get('exp_da1h_away'),
        "pos_edge_percent": None if feats.get('pos_edge') is None else round(feats['pos_edge']*100,2),
        "home_dbg": dbg_h, "away_dbg": dbg_a,
        "m_league": round(m*100,2),
        "effn_prior": round(effn_prior, 3),
        "effn_micro": round(effn_micro, 3),
        "exp_sot1h_total": exp_sot_total,
        "exp_da1h_total": exp_da_total,
        "ref_name": feats.get('ref_name'),
        "ref_used": feats.get('ref_used'),
        "weather": feats.get('weather_adj'),
        "venue": feats.get('venue_adj'),
        "lineups_have": feats.get('lineups_have'),
        "lineups_fw_count": feats.get('lineups_fw_count'),
        "inj_count": feats.get('inj_count'),
    }
    p_final_market = blend_with_market(p_final, market_odds_btts_1h, alpha=ALPHA_MODEL)
    return round(p_final_market*100,2), debug

def calculate_final_probability_over15(fixture, team_last_matches, h2h_results, micro_db,
                                       league_baselines, team_strengths, team_profiles,
                                       extras: dict | None = None, no_api: bool = False,
                                       market_odds_over15_1h: Optional[float] = None):
    """
    Finalna vjerovatnoća za 1H Over 1.5:
    - PRIOR: history (oba tima) + H2H (jače skupljen i skaliran)
    - MICRO: iz p(team scores 1H) -> λ_home, λ_away (Bernoulli→Poisson) i P(N≥2) = 1 - e^{-λ}(1+λ).
             λ_total multiplicativno stišavamo po class-gap (synergy).
    - Merge po preciziji.
    """
    # ---- H2H parametri ----
    H2H_TAU   = 12.0
    H2H_SCALE = 0.4
    H2H_MIN_W = 2.5

    home_id = ((fixture.get('teams') or {}).get('home') or {}).get('id')
    away_id = ((fixture.get('teams') or {}).get('away') or {}).get('id')
    a, b = sorted([home_id, away_id])
    h2h_key = f"{a}-{b}"

    base = _league_base_for_fixture(fixture, league_baselines)
    m = base["m1h"]  # koristi se samo kao centralni m pri EB shrink-u

    # --- PRIOR: match history za ≥2 u 1H + H2H (EB + skalirana preciznost) ---
    p_home_raw, h_home, w_home = _weighted_match_over15_rate(team_last_matches.get(home_id, []), lam=5.0, max_n=15)
    p_away_raw, h_away, w_away = _weighted_match_over15_rate(team_last_matches.get(away_id, []), lam=5.0, max_n=15)

    m_over15_guess = max(0.01, min(0.95, m*m))  # grubi prior ~ m^2
    p_home = beta_shrunk_rate(h_home, w_home, m=m_over15_guess, tau=8.0) if p_home_raw is not None else m_over15_guess
    p_away = beta_shrunk_rate(h_away, w_away, m=m_over15_guess, tau=8.0) if p_away_raw is not None else m_over15_guess
    p_team_prior = (p_home + p_away) / 2.0

    p_h2h_raw, h_h2h, w_h2h = _weighted_h2h_over15_rate(h2h_results.get(h2h_key, []), lam=4.0, max_n=10)
    p_h2h = beta_shrunk_rate(h_h2h, w_h2h, m=m_over15_guess, tau=H2H_TAU) if p_h2h_raw is not None else m_over15_guess

    effn_h2h = (w_h2h or 0.0) * H2H_SCALE
    if (w_h2h or 0.0) < H2H_MIN_W:
        effn_h2h = 0.0
        p_h2h = m_over15_guess

    p_prior, _ = fuse_probs_by_precision(
        p_team_prior, (w_home or 0.0) + (w_away or 0.0),
        p_h2h,        effn_h2h
    )

    # --- MICRO: iz p(score≥1) → λ_home, λ_away → λ_total, uz class-gap “synergy” ---
    feats = matchup_features_enhanced(fixture, team_profiles, league_baselines, micro_db=micro_db, extras=extras)

    exp_sot_total = None
    if feats.get('exp_sot1h_home') is not None or feats.get('exp_sot1h_away') is not None:
        exp_sot_total = round((feats.get('exp_sot1h_home') or 0) + (feats.get('exp_sot1h_away') or 0), 3)
    exp_da_total = None
    if feats.get('exp_da1h_home') is not None or feats.get('exp_da1h_away') is not None:
        exp_da_total = round((feats.get('exp_da1h_home') or 0) + (feats.get('exp_da1h_away') or 0), 3)

    p_home_goal, dbg_h = predict_team_scores1h_enhanced(fixture, feats, league_baselines, team_strengths, side='home')
    p_away_goal, dbg_a = predict_team_scores1h_enhanced(fixture, feats, league_baselines, team_strengths, side='away')

    # λ iz P(X≥1)=1-exp(-λ)  =>  λ=-ln(1-P)
    lam_h = -math.log(max(1e-9, 1.0 - p_home_goal))
    lam_a = -math.log(max(1e-9, 1.0 - p_away_goal))
    lam_tot_base = lam_h + lam_a

    mismatch = abs(feats.get('tier_gap_home') or 0.0)
    synergy = max(0.7, 1.0 - 0.12*mismatch)  # npr. gap=2 -> ×0.76 (clamp 0.7)
    lam_tot = lam_tot_base * synergy

    # P(N≥2) za Poisson(λ): 1 - e^{-λ}(1+λ)
    p_micro = 1.0 - math.exp(-lam_tot) * (1.0 + lam_tot)
    p_micro = max(0.0, min(1.0, p_micro))

    # precizije
    h_form = (micro_db.get(home_id) or {}).get("home") or {}
    a_form = (micro_db.get(away_id) or {}).get("away") or {}
    effn_micro = (h_form.get('used_sot',0) + a_form.get('used_sot',0) +
                  h_form.get('used_da',0)  + a_form.get('used_da',0)  +
                  h_form.get('used_pos',0) + a_form.get('used_pos',0)) / 2.0
    effn_micro = max(1.0, effn_micro + (team_profiles.get(home_id,{}).get('eff_n',0) +
                                        team_profiles.get(away_id,{}).get('eff_n',0))/4.0)

    effn_prior = max(1.0,
        (w_home or 0.0) + (w_away or 0.0)
        + (team_strengths.get(home_id,{}).get('eff_n',0))
        + (team_strengths.get(away_id,{}).get('eff_n',0))
        + effn_h2h
    )

    p_final, w_micro_share = fuse_probs_by_precision(p_prior, effn_prior, p_micro, effn_micro)

    debug = {
        "prior_percent": round(p_prior*100,2),
        "micro_percent": round(p_micro*100,2),
        "merge_weight_micro": round(w_micro_share,3),
        "p_home_scores_1h": round(p_home_goal*100,2),
        "p_away_scores_1h": round(p_away_goal*100,2),
        "lambda_home": round(lam_h,3),
        "lambda_away": round(lam_a,3),
        "lambda_total_base": round(lam_tot_base,3),
        "synergy": round(synergy,3),
        "lambda_total": round(lam_tot,3),
        "tier_home": team_profiles.get(home_id,{}).get("tier"),
        "tier_away": team_profiles.get(away_id,{}).get("tier"),
        "tier_gap_home": feats.get('tier_gap_home'),
        "tier_gap_away": feats.get('tier_gap_away'),
        "exp_sot1h_home": feats.get('exp_sot1h_home'),
        "exp_sot1h_away": feats.get('exp_sot1h_away'),
        "exp_da1h_home":  feats.get('exp_da1h_home'),
        "exp_da1h_away":  feats.get('exp_da1h_away'),
        "pos_edge_percent": None if feats.get('pos_edge') is None else round(feats['pos_edge']*100,2),
        "home_dbg": dbg_h, "away_dbg": dbg_a,
        "effn_prior": round(effn_prior, 3),
        "effn_micro": round(effn_micro, 3),
        "m_league": round(m*100, 2),
        "exp_sot1h_total": exp_sot_total,
        "exp_da1h_total": exp_da_total,
        "ref_name": feats.get('ref_name'),
        "ref_used": feats.get('ref_used'),
        "weather": feats.get('weather_adj'),
        "venue": feats.get('venue_adj'),
        "lineups_have": feats.get('lineups_have'),
        "lineups_fw_count": feats.get('lineups_fw_count'),
        "inj_count": feats.get('inj_count'),
    }
    p_final_market = blend_with_market(p_final, market_odds_over15_1h, alpha=ALPHA_MODEL)
    return round(p_final_market*100,2), debug

def _select_existing_fixture_ids(fid_list):
    if not fid_list:
        return set()
    conn = get_db_connection()
    cur = conn.cursor()
    existing = set()
    CHUNK = 900  # < 999 zbog SQLite limita za placeholder-e
    for i in range(0, len(fid_list), CHUNK):
        chunk = fid_list[i:i+CHUNK]
        placeholders = ",".join("?" * len(chunk))
        cur.execute(f"SELECT fixture_id FROM match_statistics WHERE fixture_id IN ({placeholders})", chunk)
        existing.update(r[0] for r in cur.fetchall())
    conn.close()
    return existing

def prewarm_statistics_cache(team_last_matches, max_workers=2, max_warm=1200):
    """
    Napuni lokalni match_statistics keš, ali:
    - smanjen paralelizam (max_workers=2)
    - hard cap na broj "missing" (max_warm)
    """
    # 1) Skupi sve fixture_id-e
    all_fids = set()
    for matches in (team_last_matches or {}).values():
        for m in matches or []:
            fid = ((m.get("fixture") or {}).get("id"))
            if fid:
                all_fids.add(fid)

    if not all_fids:
        print("🔥 Stats warm cache: ništa za raditi.")
        return

    # 2) Proveri koji već postoje u DB (nemoj ponovno)
    existing = _select_existing_fixture_ids(list(all_fids))
    missing = list(all_fids - existing)
    if not missing:
        print("🔥 Stats warm cache: sve već u kešu.")
        return

    # 3) Hard cap — ne pokušavaj 8k+ odjednom
    if len(missing) > max_warm:
        print(f"⚠️ Stats warm cache: missing {len(missing)} > cap {max_warm} — sečem listu.")
        missing = missing[:max_warm]

    print(f"🔥 Stats warm cache: vučem {len(missing)} nedostajućih utakmica (workers={max_workers})...")

    def worker(fid):
        try:
            return get_or_fetch_fixture_statistics(fid)
        except Exception as e:
            print(f"warm-cache error for {fid}: {e}")
            return None

    # 4) Mali batch-ovi da izbegnemo "burst"
    BATCH = 150
    for i in range(0, len(missing), BATCH):
        batch = missing[i:i+BATCH]
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = [ex.submit(worker, fid) for fid in batch]
            for _ in as_completed(futures):
                pass
        # kratka pauza između batch-eva da se host “ohladi”
        time.sleep(0.6)

    print("🔥 Stats warm cache: gotovo.")

# ------------------------- FINAL PIPELINE ---------------------------
def analyze_fixtures(start_date: datetime, end_date: datetime, from_hour=None, to_hour=None,
                     market: str = "1h_over05", no_api: bool = True,
                     odds_over05_1h: float | None = None,
                     odds_over15_1h: float | None = None,
                     odds_btts_1h: float | None = None):
    """
    Analiza mečeva u datom vremenskom opsegu.
    - Ako je no_api=False: repo će po DANIMA osigurati da fixtures postoje u bazi (fetch + upis),
      a zatim se sve ostalo (history/H2H/stats) čita on-demand kroz repo sloj.
    - Ako je no_api=True: ništa se ne vuče sa API-ja; čita se samo ono što već postoji u bazi.
    """
    # 0) sanity za market
    if market not in ACTIVE_MARKETS:
        print(f"⚠️ Nepoznat market '{market}', koristim '1h_over05'")
        market = "1h_over05"

    # 1) Ako smijemo API, osiguraj da *po danima* imamo fixtures u DB (idempotentno)
    if not no_api:
        d_local = start_date.astimezone(USER_TZ).date()
        end_local = end_date.astimezone(USER_TZ).date()
        while d_local <= end_local:
            try:
                # ensure_day je idempotentno: povuče fixtures za taj dan i pripremi keševe
                repo.ensure_day(
                    d_local,
                    last_n=DAY_PREFETCH_LAST_N,
                    h2h_n=DAY_PREFETCH_H2H_N,
                    prewarm_stats=False  # warming nije nužan; stats će se vući on-demand
                )
            except Exception as e:
                print(f"ensure_day({d_local}) failed: {e}")
            d_local += timedelta(days=1)

    # 2) Izvuci fixtures iz baze (DB-only) i filtriraj po satima/kompeticijama
    fixtures = get_fixtures_in_time_range(start_date, end_date, from_hour, to_hour, no_api=True)
    print(f"📅 Nađeno ukupno {len(fixtures)} mečeva u vremenskom opsegu.")
    # Fallback: ako nema ničega u DB i korisnik je tražio DB-only (no_api=True),
    # pokušaj da napuniš dan(e) pa ponovo čitaj iz DB.
    if not fixtures and no_api:
        d_local = start_date.astimezone(USER_TZ).date()
        end_local = end_date.astimezone(USER_TZ).date()
        dd = d_local
        while dd <= end_local:
            try:
                repo.ensure_day(
                    dd,
                    last_n=DAY_PREFETCH_LAST_N,
                    h2h_n=DAY_PREFETCH_H2H_N,
                    prewarm_stats=False
                )
            except Exception as e:
                print(f"[fallback ensure_day] {dd} failed: {e}")
            dd += timedelta(days=1)
        # probaj ponovo da pročitaš iz baze
        fixtures = get_fixtures_in_time_range(start_date, end_date, from_hour, to_hour, no_api=True)


    # 3) History i H2H (repo read-through; poštuje no_api flag)
    team_ids = {f['teams']['home']['id'] for f in fixtures} | {f['teams']['away']['id'] for f in fixtures}

    team_last_matches = {}
    for tid in team_ids:
        team_last_matches[tid] = repo.get_team_history(
            tid, last_n=DAY_PREFETCH_LAST_N, no_api=no_api
        )

    h2h_results = {}
    for f in fixtures:
        a, b = sorted([f['teams']['home']['id'], f['teams']['away']['id']])
        key = f"{a}-{b}"
        if key not in h2h_results:
            h2h_results[key] = repo.get_h2h(a, b, last_n=DAY_PREFETCH_H2H_N, no_api=no_api)

    # 4) League baselines & team strengths/profiles (stats_fn kroz repo)
    stats_fn = (lambda fid: repo.get_fixture_stats(fid, no_api=no_api))

    league_baselines = compute_league_baselines(team_last_matches, stats_fn)
    team_strengths = compute_team_strengths(
        team_last_matches,
        lam=5.0,
        max_n=15,
        m_global=(league_baselines.get('global') or {}).get('m1h', 0.55),
    )
    team_profiles = compute_team_profiles(team_last_matches, stats_fn, lam=5.0, max_n=15)

    # 5) Mikro forma (SOT/DA/POS agregati)
    micro_db = build_micro_db(team_last_matches, stats_fn)

    # 6) Per-fixture obračun za traženi market
    results = []
    for fixture in fixtures:
        home_id = fixture['teams']['home']['id']
        away_id = fixture['teams']['away']['id']
        a, b = sorted([home_id, away_id])
        h2h_key = f"{a}-{b}"
        # EXTRAS (ref/venue/weather/lineups/injuries)
        extras = build_extras_for_fixture(fixture, no_api=no_api)


        # (a) istorijske % po marketu
        if market == "gg1h":
            team1_percent, team1_hits, team1_total = team_1h_gg_stats(team_last_matches.get(home_id, []))
            team2_percent, team2_hits, team2_total = team_1h_gg_stats(team_last_matches.get(away_id, []))
            h2h_percent,  h2h_hits,  h2h_total     = h2h_1h_gg_stats(h2h_results.get(h2h_key, []))
        elif market == "1h_over15":
            team1_percent, team1_hits, team1_total = team_1h_over15_stats(team_last_matches.get(home_id, []))
            team2_percent, team2_hits, team2_total = team_1h_over15_stats(team_last_matches.get(away_id, []))
            h2h_percent,  h2h_hits,  h2h_total     = h2h_1h_over15_stats(h2h_results.get(h2h_key, []))
        else:  # "1h_over05" (default)
            team1_percent, team1_hits, team1_total = team_1h_goal_stats(team_last_matches.get(home_id, []))
            team2_percent, team2_hits, team2_total = team_1h_goal_stats(team_last_matches.get(away_id, []))
            h2h_percent,  h2h_hits,  h2h_total     = h2h_1h_goal_stats(h2h_results.get(h2h_key, []))

        # (b) mikro forma za UI
        home_form = (micro_db.get(home_id) or {}).get("home") or {}
        away_form = (micro_db.get(away_id) or {}).get("away") or {}

        def _pct_or_none(x, cap):
            try:
                if x is None or cap in (None, 0):
                    return None
                return round(min(100.0, max(0.0, (float(x) / float(cap)) * 100.0)), 2)
            except Exception:
                return None

        SOT1H_CAP_LOC = float(globals().get("SOT1H_CAP", 6.0))   # per-team cap
        DA1H_CAP_LOC  = float(globals().get("DA1H_CAP", 65.0))   # per-team cap

        home_shots_pct   = _pct_or_none(home_form.get("sot1h_for"),  SOT1H_CAP_LOC)
        away_shots_pct   = _pct_or_none(away_form.get("sot1h_for"),  SOT1H_CAP_LOC)
        home_attacks_pct = _pct_or_none(home_form.get("da1h_for"),   DA1H_CAP_LOC)
        away_attacks_pct = _pct_or_none(away_form.get("da1h_for"),   DA1H_CAP_LOC)

        form_vals = []
        if home_shots_pct is not None and home_attacks_pct is not None:
            form_vals.append((home_shots_pct + home_attacks_pct) / 2.0)
        if away_shots_pct is not None and away_attacks_pct is not None:
            form_vals.append((away_shots_pct + away_attacks_pct) / 2.0)
        form_percent = round(sum(form_vals)/len(form_vals), 2) if form_vals else 0.0


        # (c) konačna vjerovatnoća (prosledi kvote po marketu)
        if market == "gg1h":
            final_percent, debug = calculate_final_probability_gg(
                fixture, team_last_matches, h2h_results, micro_db,
                league_baselines, team_strengths, team_profiles, extras=extras, no_api=no_api,
                market_odds_btts_1h=odds_btts_1h
            )
        elif market == "1h_over15":
            final_percent, debug = calculate_final_probability_over15(
                fixture, team_last_matches, h2h_results, micro_db,
                league_baselines, team_strengths, team_profiles, extras=extras, no_api=no_api,
                market_odds_over15_1h=odds_over15_1h
            )
        else:  # "1h_over05"
            final_percent, debug = calculate_final_probability(
                fixture, team_last_matches, h2h_results, micro_db,
                league_baselines, team_strengths, team_profiles, extras=extras, no_api=no_api,
                market_odds_over05_1h=odds_over05_1h
            )

        # (d) paket za UI
        results.append({
            "debug": debug,
            "league": fixture['league']['name'],
            "team1": fixture['teams']['home']['name'],
            "team2": fixture['teams']['away']['name'],
            "team1_full": fixture['teams']['home']['name'],
            "team2_full": fixture['teams']['away']['name'],

            "team1_percent": team1_percent,
            "team2_percent": team2_percent,
            "team1_hits": team1_hits, "team1_total": team1_total,
            "team2_hits": team2_hits, "team2_total": team2_total,

            "h2h_percent": h2h_percent,
            "h2h_hits": h2h_hits, "h2h_total": h2h_total,

            "home_shots_percent":   home_shots_pct,
            "home_attacks_percent": home_attacks_pct,
            "home_shots_used":      home_form.get('used_sot', 0),
            "home_attacks_used":    home_form.get('used_da', 0),

            "away_shots_percent":   away_shots_pct,
            "away_attacks_percent": away_attacks_pct,
            "away_shots_used":      away_form.get('used_sot', 0),
            "away_attacks_used":    away_form.get('used_da', 0),

            "form_percent": form_percent,
            "final_percent": final_percent,
        })

    return results

@app.get("/api/analyze")
def analyze(request: Request):
    if not ANALYZE_LOCK.acquire(blocking=False):
        return JSONResponse(
            status_code=429,
            content={"error": "busy", "detail": "Analiza već traje, pokušaj za par sekundi."}
        )

    print("✅ USAO U /api/analyze (lock acquired)")

    try:
        from_date_str = request.query_params.get("from_date")
        to_date_str   = request.query_params.get("to_date")

        def parse_date(date_str):
            if not date_str:
                raise ValueError("Datum nije prosleđen.")
            s = date_str.strip().replace("Z", "+00:00")
            try:
                dt = datetime.fromisoformat(s)
            except ValueError:
                dt = datetime.strptime(s.split(".")[0], "%Y-%m-%dT%H:%M:%S")
                dt = dt.replace(tzinfo=timezone.utc)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt

        try:
            from_date = parse_date(from_date_str)
            to_date   = parse_date(to_date_str)
        except Exception as e:
            print("DATUM PARSING ERROR ===>", e)
            raise HTTPException(status_code=422, detail=f"Neispravan datum: {e}")

        from_hour_q = request.query_params.get("from_hour")
        to_hour_q   = request.query_params.get("to_hour")
        fh = int(from_hour_q) if from_hour_q is not None else None
        th = int(to_hour_q)   if to_hour_q   is not None else None

        market = request.query_params.get("market") or "1h_over05"
        no_api_flag = str(request.query_params.get("no_api", "1")).lower() in ("1", "true", "yes", "y")

        # --- NOVO: opcione kvote iz query stringa ---
        odds_over05 = request.query_params.get("odds_over05_1h")
        odds_over15 = request.query_params.get("odds_over15_1h")
        odds_btts1h = request.query_params.get("odds_btts_1h")
        try:
            odds_over05 = float(odds_over05) if odds_over05 else None
        except:
            odds_over05 = None
        try:
            odds_over15 = float(odds_over15) if odds_over15 else None
        except:
            odds_over15 = None
        try:
            odds_btts1h = float(odds_btts1h) if odds_btts1h else None
        except:
            odds_btts1h = None

        results = analyze_fixtures(
            from_date, to_date, fh, th, market, no_api=no_api_flag,
            odds_over05_1h=odds_over05, odds_over15_1h=odds_over15, odds_btts_1h=odds_btts1h
        )

        if results is None:
            results = []

        return JSONResponse(content=results, status_code=200)

    except HTTPException:
        raise
    except Exception as e:
        print("❌ ANALYZE FAILED:", e)
        print(traceback.format_exc())
        return JSONResponse(
            status_code=500,
            content={"error": "internal_error", "detail": str(e)}
        )
    finally:
        ANALYZE_LOCK.release()
        print("🔓 analyze: lock released")

@app.post("/api/save-pdf")
async def save_pdf(data: dict):
    file_path = "analysis_results.pdf"
    c = canvas.Canvas(file_path, pagesize=letter)
    width, height = letter
    y = height - 50

    c.setFont("Helvetica-Bold", 16)
    c.drawString(50, y, "Match Analysis Results")
    y -= 30

    c.setFont("Helvetica", 10)

    for match in data["matches"]:
        c.drawString(50, y, f"Liga: {match['league']}")
        y -= 15
        c.drawString(50, y, f"{match['team1']} ({match['team1_full']}) vs {match['team2']} ({match['team2_full']})")
        y -= 15
        c.drawString(60, y, f"{match['team1']}: {match['team1_percent']}% (Last {match.get('team1_total','?')})")
        y -= 15
        c.drawString(60, y, f"{match['team2']}: {match['team2_percent']}% (Last {match.get('team2_total','?')})")
        y -= 15
        c.drawString(60, y, f"H2H: {match['h2h_percent']}% | Form: {match['form_percent']}%")
        y -= 15
        c.drawString(60, y, f"Final Probability: {match['final_percent']}%")
        y -= 25
        d = match.get('debug', {}) or {}
        y -= 15
        c.drawString(60, y, f"Prior: {d.get('prior_percent','—')}% | Micro: {d.get('micro_percent','—')}% | micro share≈{d.get('merge_weight_micro','—')}")

        if y < 100:
            c.showPage()
            y = height - 50

    c.save()
    return FileResponse(file_path, filename="analysis_results.pdf")
