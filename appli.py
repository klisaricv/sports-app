import requests
import time
import uuid
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
from typing import Iterable, Set
from pydantic import BaseModel
import os
import hashlib
from fastapi import BackgroundTasks
import secrets
from datetime import datetime, timedelta

def _select_existing_fixture_ids(fixture_ids: Iterable[int]) -> Set[int]:
    """
    Vrati skup fixture_id vrednosti koje već postoje u tabeli match_statistics.
    Radi u delovima (IN (...) chunkovi) zbog SQLite ograničenja.
    """
    ids = [int(x) for x in set(fixture_ids) if x is not None]
    if not ids:
        return set()

    existing: Set[int] = set()
    with DB_WRITE_LOCK:  # držimo isti lock stil kao ostatak koda
        conn = get_db_connection()
        cur = conn.cursor()
        chunk = 900  # rezerva za SQLite var limit
        for i in range(0, len(ids), chunk):
            part = ids[i:i+chunk]
            placeholders = ",".join(["%s"] * len(part))
            cur.execute(f"SELECT fixture_id FROM match_statistics WHERE fixture_id IN ({placeholders})", part)
            rows = cur.fetchall()
            for (fid,) in rows:
                existing.add(int(fid))
        conn.close()
    return existing


def prewarm_statistics_cache(team_last_matches: dict[int, list], max_workers: int = 2) -> dict:
    """
    Za sve istorijske mečeve koji se pominju u team_last_matches:
      - pronađi koje statistike fale u match_statistics
      - povuci ih paralelno preko get_or_fetch_fixture_statistics (koji upisuje pod DB lock-om)
    Vraća mali rezime.
    """
    # 1) skupi sve fixture id-jeve
    all_fids = set()
    for matches in (team_last_matches or {}).values():
        for m in matches or []:
            fid = ((m.get("fixture") or {}).get("id"))
            if fid:
                all_fids.add(int(fid))

    # 2) šta već postoji?
    existing = _select_existing_fixture_ids(list(all_fids))
    missing = list(all_fids - existing)
    if not missing:
        return {"queued": 0, "fetched": 0}

    # 3) dovuci paralelno (thread-safe brojanje)
    fetched = 0
    errors = 0
    _cnt_lock = threading.Lock()

    def _pull(fid: int):
        nonlocal fetched, errors
        try:
            res = get_or_fetch_fixture_statistics(fid)
            if res is not None:
                with _cnt_lock:
                    fetched += 1
        except Exception:
            with _cnt_lock:
                errors += 1

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(_pull, fid) for fid in missing]
        for _ in as_completed(futures):
            pass

    return {"queued": len(missing), "fetched": fetched, "errors": errors}

app = FastAPI()
repo = DataRepo()

# --- MODELI i ADMIN endpointi za whitelist ---
class LeagueNameItem(BaseModel):
    country: str
    league: str

class LeagueWhitelistPayload(BaseModel):
    ids: list[int] | None = None
    names: list[LeagueNameItem] | None = None
    strict: bool | None = None

@app.post("/admin/set-league-whitelist")
async def admin_set_league_whitelist(payload: LeagueWhitelistPayload):
    """
    Postavi strogi whitelist. Ako je 'strict' True, propuštamo SAMO ono što je u 'ids' ili 'names'.
    Nazivi se porede kao (country, league), nebitna veličina slova/akcenti.
    """
    global STRICT_LEAGUE_FILTER, LEAGUE_NAME_WHITELIST, LEAGUE_ID_WHITELIST
    if payload.strict is not None:
        STRICT_LEAGUE_FILTER = bool(payload.strict)

    LEAGUE_ID_WHITELIST = set(int(x) for x in (payload.ids or []))
    LEAGUE_NAME_WHITELIST = {
        _comp_key(item.country, item.league) for item in (payload.names or [])
    }

    return {
        "ok": True,
        "strict": STRICT_LEAGUE_FILTER,
        "name_whitelist_count": len(LEAGUE_NAME_WHITELIST),
        "id_whitelist_count": len(LEAGUE_ID_WHITELIST),
    }

@app.post("/admin/load-league-whitelist-from-file")
def admin_load_league_whitelist_from_file(path: str = Body(..., embed=True)):
    """
    Učitaj whitelist iz JSON fajla (lista objekata).
    Očekivana polja po redu prioritetnosti:
      - league_id (int) [opciono]
      - country_name (str) i league_name (str)
    """
    global LEAGUE_NAME_WHITELIST, LEAGUE_ID_WHITELIST
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    names_set = set()
    ids_set = set()
    for row in data or []:
        lid = row.get("league_id")
        if lid is not None:
            try:
                ids_set.add(int(lid))
            except:
                pass
        cn = row.get("country_name") or row.get("country") or row.get("countryName")
        ln = row.get("league_name")  or row.get("league")  or row.get("leagueName")
        if cn and ln:
            names_set.add(_comp_key(cn, ln))

    LEAGUE_NAME_WHITELIST |= names_set
    LEAGUE_ID_WHITELIST   |= ids_set

    return {
        "ok": True,
        "added_name_pairs": len(names_set),
        "added_ids": len(ids_set),
        "tot_name_pairs": len(LEAGUE_NAME_WHITELIST),
        "tot_ids": len(LEAGUE_ID_WHITELIST),
    }

@app.on_event("startup")
def _init_on_startup():
    # -- tabele
    create_all_tables()
    ensure_model_outputs_table()
    ensure_analysis_cache_table()
    ensure_prepare_jobs_table()

    # -- whitelist sa diska (strogo)
    _load_strict_whitelist_from_file(WHITELIST_FILE)

    # -- odmah očisti stare analize (72h)
    try:
        purge_old_analyses()
    except Exception as e:
        print(f"initial purge_old_analyses failed: {e}")

    # -- PREWARM/ENSURE na startu je po defaultu ISKLJUČEN (da se ne troše API pozivi)
    #    Uključi samo ako eksplicitno postaviš PREWARM_ON_START=1 u env
    PREWARM_ON_START = os.getenv("PREWARM_ON_START", "0") == "1"
    if PREWARM_ON_START:
        try:
            repo.ensure_day(
                datetime.now(USER_TZ).date(),
                last_n=DAY_PREFETCH_LAST_N,
                h2h_n=DAY_PREFETCH_H2H_N,
                prewarm_stats=True
            )
        except Exception as e:
            print(f"initial ensure_day failed: {e}")

    # -- Scheduler: pokreći samo ako je eksplicitno zatražen i samo u JEDNOM workeru
    #    (inače će svaki uvicorn proces pokrenuti svoj)
    if os.getenv("SCHEDULER_ENABLED", "0") == "1":
        if acquire_db_lock("scheduler_runner", 0):
            start_scheduler(
                repo, USER_TZ,
                last_n=DAY_PREFETCH_LAST_N,
                h2h_n=DAY_PREFETCH_H2H_N
            )
        else:
            print("[startup] scheduler already running in another worker")

    # -- TTL sweeper: pokreni jednom (sa DB lock-om), radi na 60min
    if acquire_db_lock("ttl_sweeper_runner", 0):
        start_ttl_sweeper_thread()
    else:
        print("[startup] ttl sweeper already running in another worker")

ACTIVE_MARKETS = {"1h_over05", "gg1h", "1h_over15", "ft_over15"}  # + FT market

def ensure_model_outputs_table():
    with DB_WRITE_LOCK:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS model_outputs (
                fixture_id BIGINT NOT NULL,
                market     VARCHAR(64) NOT NULL,
                prob       DOUBLE NOT NULL,
                debug_json JSON NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (fixture_id, market)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        conn.commit()
        conn.close()

# ADD: tabela za cache kompletnih analiza
def ensure_analysis_cache_table():
    with DB_WRITE_LOCK:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS analysis_cache (
                cache_key VARCHAR(128) PRIMARY KEY,
                params_json JSON NOT NULL,
                results_json JSON NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        conn.commit()
        conn.close()

# ADD: jobs tabela za prepare
def ensure_prepare_jobs_table():
    with DB_WRITE_LOCK:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS prepare_jobs (
                job_id CHAR(36) PRIMARY KEY,
                day DATE NOT NULL,
                status ENUM('queued','running','done','error','skipped') NOT NULL DEFAULT 'queued',
                progress TINYINT UNSIGNED NOT NULL DEFAULT 0,
                detail VARCHAR(255) NULL,
                result_json JSON NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        conn.commit()
        conn.close()

def create_prepare_job(day_date):
    job_id = str(uuid.uuid4())
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("INSERT INTO prepare_jobs (job_id, day, status, progress) VALUES (%s, %s, 'queued', 0)", (job_id, day_date))
    conn.commit()
    conn.close()
    return job_id

def update_prepare_job(job_id, *, status=None, progress=None, detail=None, result=None):
    conn = get_db_connection()
    cur = conn.cursor()
    sets = []
    vals = []
    if status is not None:
        sets.append("status=%s"); vals.append(status)
    if progress is not None:
        sets.append("progress=%s"); vals.append(int(progress))
    if detail is not None:
        sets.append("detail=%s"); vals.append(str(detail)[:255])
    if result is not None:
        sets.append("result_json=%s"); vals.append(json.dumps(result, ensure_ascii=False))
    if not sets:
        conn.close(); return
    q = "UPDATE prepare_jobs SET " + ", ".join(sets) + " WHERE job_id=%s"
    vals.append(job_id)
    cur.execute(q, tuple(vals))
    conn.commit()
    conn.close()

def read_prepare_job(job_id):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT job_id, day, status, progress, detail, result_json FROM prepare_jobs WHERE job_id=%s LIMIT 1", (job_id,))
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    job = {
        "job_id": row[0],
        "day": row[1].isoformat() if hasattr(row[1], "isoformat") else row[1],
        "status": row[2],
        "progress": int(row[3] or 0),
        "detail": row[4],
        "result": None
    }
    try:
        job["result"] = json.loads(row[5]) if row[5] else None
    except Exception:
        job["result"] = None
    return job

# Opcioni distribuirani lok preko MySQL-a: da ne trče 2 prepare-a za isti dan u različitim procesima
def acquire_db_lock(lock_name: str, timeout_sec: int = 1) -> bool:
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT GET_LOCK(%s, %s)", (lock_name, timeout_sec))
    got = cur.fetchone()[0] == 1
    conn.close()
    return got

def release_db_lock(lock_name: str):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("DO RELEASE_LOCK(%s)", (lock_name,))
    conn.close()

def _build_cache_key(params: dict) -> str:
    # uključimo i verziju analize – ako promeniš formule, dobiješ novi ključ
    base = json.dumps({"v": ANALYSIS_VERSION, **(params or {})}, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(base.encode("utf-8")).hexdigest()

def read_analysis_cache(cache_key: str):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT results_json, expires_at
        FROM analysis_cache
        WHERE cache_key = %s
          AND (expires_at IS NULL OR expires_at > NOW())
        LIMIT 1
    """, (cache_key,))
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    # row[0] je JSON (MySQL JSON -> driver vraća str/dict zavisno od konektora)
    try:
        return row[0] if isinstance(row[0], list) else json.loads(row[0])
    except Exception:
        return None

def write_analysis_cache(cache_key: str, params: dict, results: list, ttl_hours: int | None = None):
    ttl = int(ttl_hours or CACHE_TTL_HOURS_TODAY)
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO analysis_cache (cache_key, params_json, results_json, created_at, expires_at)
        VALUES (%s, %s, %s, NOW(), DATE_ADD(NOW(), INTERVAL %s HOUR))
        ON DUPLICATE KEY UPDATE
            params_json=VALUES(params_json),
            results_json=VALUES(results_json),
            expires_at=VALUES(expires_at)
    """, (
        cache_key,
        json.dumps(params, ensure_ascii=False),
        json.dumps(results, ensure_ascii=False),
        ttl
    ))
    conn.commit()
    conn.close()

def upsert_model_output(fixture_id: int, market: str, prob: float, debug: dict):
    with DB_WRITE_LOCK:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO model_outputs (fixture_id, market, prob, debug_json, updated_at)
            VALUES (%s, %s, %s, %s, NOW())
            ON DUPLICATE KEY UPDATE
                prob=VALUES(prob),
                debug_json=VALUES(debug_json),
                updated_at=NOW()
        """, (int(fixture_id), str(market), float(prob), json.dumps(debug, ensure_ascii=False)))
        conn.commit()
        conn.close()

# Koliko istorije i H2H nam treba da bi analize radile bez API-ja
DAY_PREFETCH_LAST_N = 30
DAY_PREFETCH_H2H_N  = 10

ALLOW_API_DURING_ANALYZE = False

# ADD: verzija analitičkog koda; promeni kada menjaš formulu/težine -> invalidira cache
ANALYSIS_VERSION = "v2025-08-29-01"

# koliko traje cache rezultata (u satima)
CACHE_TTL_HOURS_TODAY = 6
CACHE_TTL_HOURS_PAST  = 48


CACHE_TTL_HOURS = 48
# === HARD MOD ===
# Analiza na /api/analyze NIKADA ne računa niti zove API; samo čita prekomputovane rezultate
ANALYZE_PRECOMPUTED_ONLY = True

# TTL brisanje analiza (strogo) – u satima
ANALYSIS_TTL_HOURS = 72



# --- NEW: conversion priors & weights ---
FINISH_PRIOR_1H = 0.34   # ~g/SoT u 1. poluvremenu (emp. prior; možeš mijenjati)
LEAK_PRIOR_1H   = 0.34   # simetričan prior za primanje gola po SoT-u rivala
REST_REF_DAYS   = 5.0    # neutralni odmor

# OVO zameni SA OVIM (ceo blok WEIGHTS):
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

    # globalni logit adj (scalar)
    "REF":         0.08,
    "ENV_WEATHER": 0.05,
    "VENUE":       0.03,
    "LINEUPS":     0.05,
    "INJ":         0.05,

    # novi z-score / normalized signali (postojeci + novi)
    "Z_SHOTS":   0.18,   # total shots 1H (po timu)
    "Z_XG":      0.28,   # xG 1H (po timu)
    "Z_BIGCH":   0.14,   # big chances 1H (po timu)
    "SETP":      0.10,   # set-piece xG (proxy) 1H (ukupno)
    "GK":        0.08,   # shot-stopping (save rate) proxy (suprotno LEAK-u)
    "CONGEST":   0.06,   # zagušenje

    # NOVO: dodatne micrometrije (slabi, ali korisni signali)
    "Z_OFFSIDES":  0.04,
    "Z_CROSSES":   0.06,
    "Z_COUNTERS":  0.05,
    "Z_SAVES":     0.04,   # kao protiv-signali napadu (više odbrana → malo niže)
    "Z_SIB":       0.10,   # shots inside box 1H
    "Z_SOB":       0.03,   # shots outside box 1H (slabiji)
    "Z_WOODWORK":  0.03,   # near-miss volatilnost

    # važnost & minute prior
    "IMPORTANCE": 0.05,   # važnost meča (global adj)
    "MINUTE_PRIOR_BLEND": 0.25,  # težina za teams/statistics minute prior u PRIOR mešavini
    "FTSCS_ADJ":  0.05,         # blaga korekcija priora iz FTS/CleanSheet
    "FORM_ADJ":   0.03,         # mala forma/streak korekcija
    "COACH_ADJ":  0.02,         # opc. efekat promene trenera
    
    # kritični feature-i koji se skupljaju ali ne koriste
    "PACE_DA_ADJ": 0.04,        # uticaj ukupnog tempa dangerous attacks
    "LINEUPS_HAVE_ADJ": 0.03,   # da li imamo lineup podatke
    "LINEUPS_FW_ADJ": 0.02,     # broj napadača u sastavu
    "INJ_COUNT_ADJ": 0.02,      # broj povreda (negativan uticaj)
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
# OVO ubaci ispod POSTOJEĆIH *_CAP konstanti:
OFFSIDES1H_CAP = 5.0
CROSSES1H_CAP  = 24.0
COUNTER1H_CAP  = 12.0
SAVES1H_CAP    = 8.0
SIB1H_CAP      = 10.0   # Shots Inside Box (1H)
SOB1H_CAP      = 12.0   # Shots Outside Box (1H)
WOODWORK1H_CAP = 2.0

# ======================= FT (FULL-TIME) OVER 1.5 (2+) =======================

# --- FT kapovi (konzervativni, možeš fino dašteluješ kasnije) ---
SOTFT_CAP      = 12.0
DAFT_CAP       = 120.0
SHOTSFT_CAP    = 20.0
XGFT_CAP       = 2.80
BIGCHFT_CAP    = 6.0
CORNFT_CAP     = 14.0
FKFT_CAP       = 18.0
OFFSIDESFT_CAP = 9.0
CROSSESFT_CAP  = 50.0
COUNTERFT_CAP  = 24.0
SAVESFT_CAP    = 16.0
SIBFT_CAP      = 18.0
SOBFT_CAP      = 24.0
WOODWORKFT_CAP = 4.0

# set-piece xG proxy (FT total)
SETP_XG_PER_CORNER_FT = 0.025
SETP_XG_PER_FK_FT     = 0.010

# Priori za FT (g/SoT i GK) – simetrični
FINISH_PRIOR_FT = 0.33
LEAK_PRIOR_FT   = 0.33
GK_SAVE_PRIOR_FT = 0.70
GK_SAVE_TAU_FT   = 12.0

# FT kalibracija (možeš odvojeno od 1H)
CALIBRATION_FT = {
    "TEMP": 1.08,
    "FLOOR": 0.02,
    "CEIL":  0.995,
}

# Ako želiš odvojene težine, kloniramo postojeće i koristićemo ih za FT:
WEIGHTS_FT = dict(WEIGHTS)  # start sa istim; kasnije podešavaj po validaciji
WEIGHTS_FT.setdefault("TIER_GAP", WEIGHTS.get("TIER_GAP", 0.35))
# Dodaj kritične feature-e za FT
WEIGHTS_FT.setdefault("PACE_DA_ADJ", WEIGHTS.get("PACE_DA_ADJ", 0.04))
WEIGHTS_FT.setdefault("LINEUPS_HAVE_ADJ", WEIGHTS.get("LINEUPS_HAVE_ADJ", 0.03))
WEIGHTS_FT.setdefault("LINEUPS_FW_ADJ", WEIGHTS.get("LINEUPS_FW_ADJ", 0.02))
WEIGHTS_FT.setdefault("INJ_COUNT_ADJ", WEIGHTS.get("INJ_COUNT_ADJ", 0.02))

# ========================= GENERIČKA OBJAŠNJENJA (BE) =========================
from fastapi import Response
from dataclasses import dataclass

def _read_model_output_row(fixture_id: int, market: str) -> dict | None:
    with DB_WRITE_LOCK:
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT prob, debug_json, updated_at
            FROM model_outputs
            WHERE fixture_id=%s AND market=%s
        """, (int(fixture_id), str(market)))
        row = cur.fetchone()
        conn.close()
    if not row:
        return None

    val = row.get("debug_json")
    if isinstance(val, (dict, list)):
        dbg = val
    else:
        try:
            dbg = json.loads(val or "{}")
        except Exception:
            dbg = {}
    return {"prob": float(row["prob"]), "debug": dbg, "updated_at": row["updated_at"]}


def _read_fixture_json(fixture_id: int) -> dict | None:
    with DB_WRITE_LOCK:
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT fixture_json FROM fixtures WHERE id=%s", (int(fixture_id),))
        row = cur.fetchone()
        conn.close()
    if not row:
        return None

    val = row.get("fixture_json")
    if isinstance(val, (dict, list)):
        return val
    try:
        return json.loads(val or "{}")
    except Exception:
        return None

def _odds_from_prob(p: float) -> float | None:
    try:
        p = max(1e-6, min(0.999999, float(p)))
        return round(1.0/p, 2)
    except:
        return None

def _fmt_pct(p: float) -> str:
    try:
        return f"{round(p*100,1)}%"
    except:
        return f"{p:.2%}"

def _local_kickoff(fix: dict) -> str | None:
    try:
        dt = datetime.fromisoformat(((fix.get("fixture") or {}).get("date") or "").replace("Z","+00:00"))
        return dt.astimezone(USER_TZ).strftime("%d.%m.%Y %H:%M")
    except:
        return None

def _team_names(fix: dict) -> tuple[str,str]:
    th = (((fix.get("teams") or {}).get("home") or {}).get("name") or "Home")
    ta = (((fix.get("teams") or {}).get("away") or {}).get("name") or "Away")
    return th, ta

def _league_name(fix: dict) -> str:
    lg = (fix.get("league") or {})
    country = lg.get("country") or ""
    name = lg.get("name") or ""
    season = lg.get("season") or ""
    chunks = [x for x in [country, name, str(season)] if x]
    return " • ".join(chunks) if chunks else "Liga"

# ---------- detekcija “drive” faktora iz dbg (FT over 1.5) ----------
def _driver_texts_ft(dbg: dict, lang="sr") -> tuple[list[str], list[str]]:
    """Vrati (Pozitivni razlozi, Rizici) kao liste stringova."""
    pos, neg = [], []
    h = (dbg.get("lam_home") or {})
    a = (dbg.get("lam_away") or {})

    # 1) Napad/šanse (z_shots, z_xg, z_big)
    def _zget(d, k): 
        try: return float(d.get(k))
        except: return 0.0
    z_combo = sum([
        abs(_zget(h,"z_shots")), abs(_zget(h,"z_xg")), abs(_zget(h,"z_big")),
        abs(_zget(a,"z_shots")), abs(_zget(a,"z_xg")), abs(_zget(a,"z_big"))
    ])
    if z_combo >= 1.5:
        pos.append("Stvaranje šansi iznad proseka (šutevi/xG/big chances).")

    # 2) Tempo po SOT/DA
    z_tempo = sum([abs(_zget(h,"z_sot")), abs(_zget(h,"z_da")), abs(_zget(a,"z_sot")), abs(_zget(a,"z_da"))])
    if z_tempo >= 1.5:
        pos.append("Viši očekivani tempo (SOT/DA signali).")

    # 3) Set-piece xG total
    setp = float(dbg.get("setp_xg_total") or 0.0)
    if setp >= 0.15:
        pos.append("Solidan set-piece potencijal (korneri/slobodni udarci).")

    # 4) P(ge1) timova → momentum
    p1h = float((h.get("p_ge1") or 0.0))
    p1a = float((a.get("p_ge1") or 0.0))
    if p1h >= 0.58 or p1a >= 0.58 or (p1h+p1a) >= 1.15:
        pos.append("Obe ekipe imaju dobru šansu da postignu bar 1 gol.")

    # 5) Negativni: jaki golmani, loše vreme, zagušenje, slab coverage
    if float((h.get("gk_adj") or 0.0)) < -0.06 or float((a.get("gk_adj") or 0.0)) < -0.06:
        neg.append("Golmanske odbrane iznad proseka (mogu da spuste broj golova).")
    cov = float(dbg.get("coverage") or 1.0)
    if cov < 0.55:
        neg.append("Nesigurniji ulazni podaci (manja pokrivenost mikrometrikom).")
    # Nema direktnog weather/ref u dbg, ali finalni model ih već uključuje → rizik ostaje implicitno.

    # 6) Offside/cross/counter/saves/sib/sob/wood — ako ima dosta negativnih (npr. saves↑)
    if _zget(h,"z_saves") > 0.8 or _zget(a,"z_saves") > 0.8:
        neg.append("Puno odbrana — možda ispod-efikasno završavanje.")
    if _zget(h,"z_sib") + _zget(a,"z_sib") > 1.2:
        pos.append("Dosta udaraca iz kaznenog (bliže golu).")

    return pos, neg

# ---------- render (JSON → markdown ili čist JSON) ----------
def _render_markdown(pack: dict, lang="sr") -> str:
    lines = []
    title = pack.get("title") or ""
    sub   = pack.get("subtitle") or ""
    lines.append(f"### {title}")
    if sub: lines.append(f"*{sub}*")
    lines.append("")
    lines.append(f"**Prognoza:** {pack.get('headline')}")
    lines.append("")
    if pack.get("positives"):
        lines.append("**Zašto da:**")
        for b in pack["positives"]:
            lines.append(f"- {b}")
    if pack.get("risks"):
        lines.append("")
        lines.append("**Rizici / Zašto ne:**")
        for b in pack["risks"]:
            lines.append(f"- {b}")
    if pack.get("notes"):
        lines.append("")
        lines.append(f"**Napomena:** {pack['notes']}")
    return "\n".join(lines)

def _mk_headline_generic(p: float, market: str, lang="sr") -> str:
    pct = _fmt_pct(p); odds = _odds_from_prob(p)
    if lang == "en":
        lab = "FT Over 1.5" if market=="ft_over15" else market
        return f"{lab}: {pct} (~{odds})"
    # sr
    lab = "FT 2+ gola" if market=="ft_over15" else market
    return f"{lab}: {pct} (~{odds})"

def _explain_ft_over15(fixture: dict, row: dict, lang="sr") -> dict:
    p = float(row["prob"])
    dbg = row.get("debug") or {}
    th, ta = _team_names(fixture)
    liga = _league_name(fixture)
    tko = _local_kickoff(fixture)

    positives, risks = _driver_texts_ft(dbg, lang=lang)

    pack = {
        "market": "ft_over15",
        "fixture_id": ((fixture.get("fixture") or {}).get("id")),
        "title": f"{th} – {ta}",
        "subtitle": f"{liga} • {tko}" if tko else liga,
        "probability": round(p,4),
        "headline": _mk_headline_generic(p, "ft_over15", lang=lang),
        "positives": positives,
        "risks": risks,
        "notes": "Objašnjenje generisano na osnovu mikrometrija (tempo/šanse), profila timova i konteksta (set-pieces, ugođaj)."
    }
    return pack

# ---------- router (jedan endpoint za sve markete) ----------
def _build_explanation_for_market(fixture_id: int, market: str, lang="sr") -> dict:
    row = _read_model_output_row(fixture_id, market)
    if not row:
        raise HTTPException(status_code=404, detail=f"Nema model output za fixture={fixture_id}, market={market}.")
    fixture = _read_fixture_json(fixture_id)
    if not fixture:
        raise HTTPException(status_code=404, detail=f"Nema fixture zapisa u bazi (id={fixture_id}).")
    if market == "ft_over15":
        return _explain_ft_over15(fixture, row, lang=lang)
    # fallback — generički header ako dodaš druge markete
    p = float(row["prob"])
    th, ta = _team_names(fixture)
    liga = _league_name(fixture); tko = _local_kickoff(fixture)
    return {
        "market": market,
        "fixture_id": fixture_id,
        "title": f"{th} – {ta}",
        "subtitle": f"{liga} • {tko}" if tko else liga,
        "probability": round(p,4),
        "headline": _mk_headline_generic(p, market, lang=lang),
        "positives": [],
        "risks": [],
        "notes": "Generičko objašnjenje (market još nema specifične razloge)."
    }

@app.get("/explain/{fixture_id}/{market}")
def explain_fixture_market(
    fixture_id: int,
    market: str,
    lang: str = "sr",
    format: str = "json"
):
    pack = _build_explanation_for_market(fixture_id, market, lang=lang)
    if format == "markdown":
        md = _render_markdown(pack, lang=lang)
        return Response(content=md, media_type="text/markdown")
    return JSONResponse(content=pack)

@app.get("/explain/batch")
def explain_batch(
    fixture_ids: str = Query(..., description="CSV lista fixture ID-jeva"),
    market: str = "ft_over15",
    lang: str = "sr",
    format: str = "json"
):
    out = []
    for s in str(fixture_ids).split(","):
        s = s.strip()
        if not s: 
            continue
        try:
            fid = int(s)
        except:
            continue
        try:
            pack = _build_explanation_for_market(fid, market, lang=lang)
            out.append(pack)
        except HTTPException:
            continue
    if format == "markdown":
        # spakuj u jedan markdown
        md_parts = []
        for p in out:
            md_parts.append(_render_markdown(p, lang=lang))
            md_parts.append("\n---\n")
        return Response(content="".join(md_parts), media_type="text/markdown")
    return JSONResponse(content=out)

# ---------- helpers (FT) ----------
def _ft_total_ge2(m):
    ft = ((m.get('score') or {}).get('fulltime') or {})
    return (float(ft.get('home') or 0) + float(ft.get('away') or 0)) >= 2

def _team_scored_ft(m, team_id):
    teams = (m.get('teams') or {})
    ft = ((m.get('score') or {}).get('fulltime') or {})
    hid = ((teams.get('home') or {}).get('id'))
    aid = ((teams.get('away') or {}).get('id'))
    if hid == team_id:
        return (ft.get('home') or 0) > 0
    if aid == team_id:
        return (ft.get('away') or 0) > 0
    return False

def _team_conceded_ft(m, team_id):
    teams = (m.get('teams') or {})
    ft = ((m.get('score') or {}).get('fulltime') or {})
    hid = ((teams.get('home') or {}).get('id'))
    aid = ((teams.get('away') or {}).get('id'))
    if hid == team_id:
        return (ft.get('away') or 0) > 0
    if aid == team_id:
        return (ft.get('home') or 0) > 0
    return False

def _poisson_p_ge2(lam_total):
    lam = max(0.0, float(lam_total or 0.0))
    # P(N>=2) = 1 - e^{-λ}(1 + λ)
    return max(0.0, min(1.0, 1.0 - math.exp(-lam) * (1.0 + lam)))

# ---------- ekstrakcija FT mikro metrika iz match_statistics ----------
def _extract_match_micro_for_team_ft(stats_response, team_id, opp_id):
    tb = _team_block(stats_response, team_id)
    ob = _team_block(stats_response, opp_id)
    if not tb or not ob:
        return None

    def full(block, names, cap=None):
        v = _stat_from_block(block, [n.lower() for n in names])
        if v is None:
            return None
        val = float(v)
        if cap is not None:
            val = max(0.0, min(cap, val))
        return val

    sot_for     = full(tb, ["shots on goal","shots on target"], cap=SOTFT_CAP)
    sot_allowed = full(ob, ["shots on goal","shots on target"], cap=SOTFT_CAP)

    shots_for     = full(tb, ["total shots","shots total","shots"], cap=SHOTSFT_CAP)
    shots_allowed = full(ob, ["total shots","shots total","shots"], cap=SHOTSFT_CAP)

    da_for      = full(tb, ["dangerous attacks"], cap=DAFT_CAP)
    da_allowed  = full(ob, ["dangerous attacks"], cap=DAFT_CAP)

    pos = full(tb, ["ball possession","possession","possession %","ball possession %"])
    if pos is not None:
        pos = max(POS_MIN, min(POS_MAX, pos))

    xg_for     = full(tb, ["expected goals","xg","x-goals"], cap=XGFT_CAP)
    xg_allowed = full(ob, ["expected goals","xg","x-goals"], cap=XGFT_CAP)

    big_for     = full(tb, ["big chances","big chances created"], cap=BIGCHFT_CAP)
    big_allowed = full(ob, ["big chances","big chances created"], cap=BIGCHFT_CAP)

    corn_for     = full(tb, ["corner kicks","corners"], cap=CORNFT_CAP)
    corn_allowed = full(ob, ["corner kicks","corners"], cap=CORNFT_CAP)

    fk_for     = full(tb, ["free kicks","free-kicks"], cap=FKFT_CAP)
    fk_allowed = full(ob, ["free kicks","free-kicks"], cap=FKFT_CAP)

    offs_for     = full(tb, ["offsides"], cap=OFFSIDESFT_CAP)
    offs_allowed = full(ob, ["offsides"], cap=OFFSIDESFT_CAP)

    cross_for     = full(tb, ["crosses","total crosses"], cap=CROSSESFT_CAP)
    cross_allowed = full(ob, ["crosses","total crosses"], cap=CROSSESFT_CAP)

    counter_for     = full(tb, ["counter attacks","counter-attacks"], cap=COUNTERFT_CAP)
    counter_allowed = full(ob, ["counter attacks","counter-attacks"], cap=COUNTERFT_CAP)

    saves_for     = full(tb, ["goalkeeper saves","saves"], cap=SAVESFT_CAP)
    saves_allowed = full(ob, ["goalkeeper saves","saves"], cap=SAVESFT_CAP)

    sib_for     = full(tb, ["shots insidebox","shots inside box"], cap=SIBFT_CAP)
    sib_allowed = full(ob, ["shots insidebox","shots inside box"], cap=SIBFT_CAP)

    sob_for     = full(tb, ["shots outsidebox","shots outside box"], cap=SOBFT_CAP)
    sob_allowed = full(ob, ["shots outsidebox","shots outside box"], cap=SOBFT_CAP)

    wood_for     = full(tb, ["hit woodwork","woodwork"], cap=WOODWORKFT_CAP)
    wood_allowed = full(ob, ["hit woodwork","woodwork"], cap=WOODWORKFT_CAP)

    return {
        "sot_for": sot_for,         "sot_allowed": sot_allowed,
        "shots_for": shots_for,     "shots_allowed": shots_allowed,
        "da_for": da_for,           "da_allowed": da_allowed,
        "pos": pos,
        "xg_for": xg_for,           "xg_allowed": xg_allowed,
        "big_for": big_for,         "big_allowed": big_allowed,
        "corn_for": corn_for,       "corn_allowed": corn_allowed,
        "fk_for": fk_for,           "fk_allowed": fk_allowed,
        "offs_for": offs_for,       "offs_allowed": offs_allowed,
        "cross_for": cross_for,     "cross_allowed": cross_allowed,
        "counter_for": counter_for, "counter_allowed": counter_allowed,
        "saves_for": saves_for,     "saves_allowed": saves_allowed,
        "sib_for": sib_for,         "sib_allowed": sib_allowed,
        "sob_for": sob_for,         "sob_allowed": sob_allowed,
        "wood_for": wood_for,       "wood_allowed": wood_allowed,
    }

# ---------- team mikro forma (FT) ----------
def _aggregate_team_micro_ft(team_id, matches, get_stats_fn, context="all"):
    sums = {k:0.0 for k in (
        "sot_for","sot_allowed","da_for","da_allowed","pos",
        "shots_for","shots_allowed","xg_for","xg_allowed","big_for","big_allowed",
        "corn_for","corn_allowed","fk_for","fk_allowed",
        "offs_for","offs_allowed","cross_for","cross_allowed","counter_for","counter_allowed",
        "saves_for","saves_allowed","sib_for","sib_allowed","sob_for","sob_allowed","wood_for","wood_allowed"
    )}
    cnt_for = {}; cnt_alw = {}; cnt_pos = 0; used_any = 0

    def _inc(d, key): d[key] = d.get(key, 0) + 1

    for m in matches or []:
        # ISPRAVKA: Bezbedno rukovanje sa podacima koji mogu biti tuple-ovi ili dict-ovi
        if not isinstance(m, dict):
            if isinstance(m, (list, tuple)):
                try:
                    converted = _coerce_fixture_row_to_api_dict(m)
                    if converted and isinstance(converted, dict):
                        m = converted
                    else:
                        continue
                except Exception:
                    continue
            else:
                continue
        fix = (m.get('fixture') or {})
        if context in ("home","away"):
            is_home = ((m.get('teams') or {}).get('home') or {}).get('id') == team_id
            if context == "home" and not is_home: continue
            if context == "away" and is_home: continue

        fid = fix.get('id'); 
        if not fid: continue
        stats = get_stats_fn(fid)
        if not stats: continue

        teams = (m.get('teams') or {})
        hid = ((teams.get('home') or {}).get('id'))
        aid = ((teams.get('away') or {}).get('id'))
        opp = aid if hid == team_id else hid

        micro = _extract_match_micro_for_team_ft(stats, team_id, opp)
        if not micro: continue

        def add_pair(tag):
            vf = micro.get(f"{tag}_for"); va = micro.get(f"{tag}_allowed")
            if vf is not None: sums[f"{tag}_for"] += float(vf); _inc(cnt_for, tag)
            if va is not None: sums[f"{tag}_allowed"] += float(va); _inc(cnt_alw, tag)

        for tag in ("sot","da","shots","xg","big","corn","fk","offs","cross","counter","saves","sib","sob","wood"):
            add_pair(tag)
        if micro.get("pos") is not None:
            sums["pos"] += float(micro["pos"]); cnt_pos += 1
        used_any += 1

    def _avg(tag, side):
        base = {"for": cnt_for.get(tag,0), "allowed": cnt_alw.get(tag,0)}[side]
        if base == 0: return None
        return round(sums[f"{tag}_{side}"]/base, 3)

    return {
        "used_matches": used_any,
        "sot_for": _avg("sot","for"), "sot_allowed": _avg("sot","allowed"),
        "da_for":  _avg("da","for"),  "da_allowed":  _avg("da","allowed"),
        "pos": None if cnt_pos==0 else round(sums["pos"]/cnt_pos,3),

        "shots_for": _avg("shots","for"), "shots_allowed": _avg("shots","allowed"),
        "xg_for": _avg("xg","for"),       "xg_allowed": _avg("xg","allowed"),
        "big_for": _avg("big","for"),     "big_allowed": _avg("big","allowed"),
        "corn_for": _avg("corn","for"),   "corn_allowed": _avg("corn","allowed"),
        "fk_for": _avg("fk","for"),       "fk_allowed": _avg("fk","allowed"),

        "offs_for": _avg("offs","for"),   "offs_allowed": _avg("offs","allowed"),
        "cross_for": _avg("cross","for"), "cross_allowed": _avg("cross","allowed"),
        "counter_for": _avg("counter","for"), "counter_allowed": _avg("counter","allowed"),
        "saves_for": _avg("saves","for"), "saves_allowed": _avg("saves","allowed"),
        "sib_for": _avg("sib","for"),     "sib_allowed": _avg("sib","allowed"),
        "sob_for": _avg("sob","for"),     "sob_allowed": _avg("sob","allowed"),
        "wood_for": _avg("wood","for"),   "wood_allowed": _avg("wood","allowed"),

        "used_sot": cnt_for.get("sot",0) + cnt_alw.get("sot",0),
        "used_da":  cnt_for.get("da",0)  + cnt_alw.get("da",0),
        "used_pos": cnt_pos,
    }

def purge_old_analyses():
    """Brisanje analiza starijih od ANALYSIS_TTL_HOURS iz model_outputs i analysis_cache."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM model_outputs WHERE updated_at < DATE_SUB(NOW(), INTERVAL %s HOUR)", (ANALYSIS_TTL_HOURS,))
    # analysis_cache: brišemo po created_at i/ili expires_at
    cur.execute("""
        DELETE FROM analysis_cache
        WHERE created_at < DATE_SUB(NOW(), INTERVAL %s HOUR)
           OR (expires_at IS NOT NULL AND expires_at < NOW())
    """, (ANALYSIS_TTL_HOURS,))
    conn.commit()
    conn.close()

def start_ttl_sweeper_thread():
    def _loop():
        while True:
            try:
                purge_old_analyses()
            except Exception as e:
                try: print("TTL sweeper error:", e)
                except: pass
            # spavaj 1h
            time.sleep(3600)
    t = threading.Thread(target=_loop, daemon=True)
    t.start()

def read_precomputed_results(from_dt: datetime, to_dt: datetime, fh, th, market: str) -> list[dict]:
    """
    Čita isključivo iz model_outputs.debug JSON-a:

    - debug.kickoff (ISO) → filtracija po datumu + from_hour/to_hour (lokalna satnica)
    - debug.league, debug.team1, debug.team2
    - prob → final_percent
    - Ovo vraća isti oblik kao analyze_fixtures, da frontend ništa ne menja
    """
    # pripremi UTC opseg; satnice filtriramo u lokalnoj zoni
    f_utc = from_dt.astimezone(timezone.utc).replace(tzinfo=None)
    t_utc = to_dt.astimezone(timezone.utc).replace(tzinfo=None)

    conn = get_db_connection()
    cur  = conn.cursor()

    # Izvuci sve za dati market i kickoff u dt-opsegu
    # kickoff je u debug_json JSON-u kao ISO string; kastujemo u DATETIME
    cur.execute(f"""
        SELECT fixture_id, prob, debug_json
        FROM model_outputs
        WHERE market=%s
          AND (
                JSON_EXTRACT(debug_json, '$.kickoff') IS NOT NULL
            AND CAST(JSON_UNQUOTE(JSON_EXTRACT(debug_json, '$.kickoff')) AS DATETIME) BETWEEN %s AND %s
          )
    """, (market, f_utc, t_utc))
    rows = cur.fetchall()
    conn.close()

    out = []
    # filtracija po from_hour/to_hour u LOKALNOM vremenu (ako su zadati)
    use_fh = fh not in (None, "", "null")
    use_th = th not in (None, "", "null")
    fh = int(fh) if use_fh else None
    th = int(th) if use_th else None

    for i, (fixture_id, prob, dbg) in enumerate(rows or []):
        try:
            d = dbg if isinstance(dbg, dict) else json.loads(dbg or "{}")
        except Exception:
            d = {}
        
        print(f"🔍 [DEBUG] read_precomputed_results row {i} (fixture {fixture_id}): debug keys: {list(d.keys())}", flush=True)
        print(f"🔍 [DEBUG] read_precomputed_results row {i}: exp_sot_total: {d.get('exp_sot_total')}", flush=True)
        print(f"🔍 [DEBUG] read_precomputed_results row {i}: exp_da_total: {d.get('exp_da_total')}", flush=True)
        print(f"🔍 [DEBUG] read_precomputed_results row {i}: ref_adj: {d.get('ref_adj')}", flush=True)
        
        kickoff_iso = d.get("kickoff")
        if not kickoff_iso:
            print(f"🔍 [DEBUG] read_precomputed_results row {i}: no kickoff, skipping", flush=True)
            continue
        try:
            k_dt = datetime.fromisoformat(kickoff_iso.replace("Z", "+00:00"))
        except Exception:
            # fallback: pokušaj bez TZ
            try:
                k_dt = datetime.fromisoformat(kickoff_iso)
                k_dt = k_dt.replace(tzinfo=timezone.utc)
            except Exception:
                continue

        # filtriraj po satnici u LOKALNOJ zoni ako tražiš from_hour/to_hour
        if use_fh or use_th:
            k_local = k_dt.astimezone(USER_TZ)
            if use_fh and k_local.hour < fh:
                continue
            if use_th and k_local.hour > th:
                continue

        league = d.get("league")
        t1 = d.get("team1")
        t2 = d.get("team2")

        out.append({
            "fixture_id": int(fixture_id),
            "kickoff": kickoff_iso,
            "debug": d,
            "league": league,
            "team1": t1,
            "team2": t2,
            "team1_full": t1,
            "team2_full": t2,

            # polja koja frontend očekuje
            "team1_percent": d.get("team1_percent"),
            "team2_percent": d.get("team2_percent"),
            "team1_hits": d.get("team1_hits"), "team1_total": d.get("team1_total"),
            "team2_hits": d.get("team2_hits"), "team2_total": d.get("team2_total"),
            "h2h_percent": d.get("h2h_percent"),
            "h2h_hits": d.get("h2h_hits"), "h2h_total": d.get("h2h_total"),
            "home_shots_percent": d.get("home_shots_percent"),
            "home_attacks_percent": d.get("home_attacks_percent"),
            "home_shots_used": d.get("home_shots_used"),
            "home_attacks_used": d.get("home_attacks_used"),
            "away_shots_percent": d.get("away_shots_percent"),
            "away_attacks_percent": d.get("away_attacks_percent"),
            "away_shots_used": d.get("away_shots_used"),
            "away_attacks_used": d.get("away_attacks_used"),
            "form_percent": d.get("form_percent"),

            "final_percent": round(float(prob or 0) * 100.0, 2),
            
            # Micro signals - all parameters
            "exp_sot_total": d.get("exp_sot_total"),
            "exp_da_total": d.get("exp_da_total"),
            "pos_edge": d.get("pos_edge"),
            "effN_prior": d.get("effN_prior"),
            "effN_micro": d.get("effN_micro"),
            "liga_baseline": d.get("liga_baseline"),
            
            # Referee, weather, venue, lineups, injuries
            "ref_adj": d.get("ref_adj"),
            "weather_adj": d.get("weather_adj"),
            "venue_adj": d.get("venue_adj"),
            "lineup_adj": d.get("lineup_adj"),
            "injuries_adj": d.get("injuries_adj"),
            
            # All micro features
            "z_sot_home": d.get("z_sot_home"),
            "z_sot_away": d.get("z_sot_away"),
            "z_da_home": d.get("z_da_home"),
            "z_da_away": d.get("z_da_away"),
            "z_shots_home": d.get("z_shots_home"),
            "z_shots_away": d.get("z_shots_away"),
            "z_xg_home": d.get("z_xg_home"),
            "z_xg_away": d.get("z_xg_away"),
            "z_bigch_home": d.get("z_bigch_home"),
            "z_bigch_away": d.get("z_bigch_away"),
            "z_corn_home": d.get("z_corn_home"),
            "z_corn_away": d.get("z_corn_away"),
            "z_fk_home": d.get("z_fk_home"),
            "z_fk_away": d.get("z_fk_away"),
            "z_offs_home": d.get("z_offs_home"),
            "z_offs_away": d.get("z_offs_away"),
            "z_cross_home": d.get("z_cross_home"),
            "z_cross_away": d.get("z_cross_away"),
            "z_counter_home": d.get("z_counter_home"),
            "z_counter_away": d.get("z_counter_away"),
            "z_saves_home": d.get("z_saves_home"),
            "z_saves_away": d.get("z_saves_away"),
            "z_sib_home": d.get("z_sib_home"),
            "z_sib_away": d.get("z_sib_away"),
            "z_sob_home": d.get("z_sob_home"),
            "z_sob_away": d.get("z_sob_away"),
            "z_wood_home": d.get("z_wood_home"),
            "z_wood_away": d.get("z_wood_away"),
            
            # Adjustments
            "fin_adj": d.get("fin_adj"),
            "leak_adj": d.get("leak_adj"),
            "gk_adj": d.get("gk_adj"),
            "rest_adj": d.get("rest_adj"),
            "congest_adj": d.get("congest_adj"),
            "att_adj": d.get("att_adj"),
            "def_adj": d.get("def_adj"),
            "tier_gap": d.get("tier_gap"),
            
            # FORM_ADJ i COACH_ADJ
            "form_adj": d.get("form_adj"),
            "coach_adj": d.get("coach_adj"),
        })
    return out

def run_prepare_job(job_id: str, day_iso: str, prewarm: bool = True, *_args, **_kwargs):
    """
    Pozadinski prepare:
      - seed fixtures (ako fale)
      - dopuni history/h2h/statistike (samo što fali)
      - izračunaj sve markete (DB-only) i upiši u model_outputs
      - popuni analysis_cache za ceo dan (po marketu)
      - upisuj progres u prepare_jobs
    """
    lock_name = f"prepare:{day_iso}"
    got_db_lock = False
    try:
        update_prepare_job(job_id, status="running", progress=1, detail="starting")

        # cross-process lock (MySQL)
        got_db_lock = acquire_db_lock(lock_name, timeout_sec=1)
        if not got_db_lock:
            update_prepare_job(job_id, status="skipped", detail="already running")
            return

        # in-process lock (isti worker)
        if not PREPARE_LOCK.acquire(blocking=False):
            update_prepare_job(job_id, status="skipped", detail="already running (process)")
            return

        # 1) Dan i opseg
        d_local = datetime.fromisoformat(day_iso).date()
        start_dt, end_dt = _day_bounds_utc(d_local)

        # 2) Fixtures (seed ako fale) + skupovi timova/parova
        update_prepare_job(job_id, progress=5, detail="fixtures")
        seeded = False
        if not _has_fixtures_for_day(d_local):
            seed_day_into_db(d_local)
            seeded = True

        fixtures = _list_fixtures_for_day(d_local)
        fixtures = [f for f in (fixtures or []) if isinstance(f, dict)]

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

        # (novo) kompletno pre-warm extras (referee, weather, venue, lineups, injuries, odds, team_stats)
        if prewarm:
            update_prepare_job(job_id, progress=12, detail="extras prewarm")
            try:
                prewarm_extras_for_fixtures(fixtures, include_odds=True, include_team_stats=True)
            except Exception as e:
                print("prewarm_extras failed:", e)

        # Update team stats table with latest data
        update_prepare_job(job_id, progress=18, detail="team stats update")
        try:
            # First ensure table exists and is populated
            populate_team_stats_if_needed()
            # Then update stats for teams playing today
            update_team_stats_for_teams(team_ids, fixtures)
        except Exception as e:
            print("team stats update failed:", e)

        # 3) History/H2H – dopuni samo nedostajuće
        update_prepare_job(job_id, progress=15, detail="history/h2h")
        hist_missing = _history_missing(team_ids, DAY_PREFETCH_LAST_N, CACHE_TTL_HOURS)
        h2h_missing  = _h2h_missing(pairs,  DAY_PREFETCH_H2H_N,  CACHE_TTL_HOURS)
        if hist_missing or h2h_missing:
            fetch_and_store_all_historical_data(fixtures, no_api=False)

        # 4) Stats prewarm (opciono)
        stats_missing_before = 0
        if prewarm:
            update_prepare_job(job_id, progress=25, detail="stats prewarm")
            team_last = fetch_last_matches_for_teams(fixtures, last_n=DAY_PREFETCH_LAST_N, no_api=True)

            all_fids = set()
            for matches in (team_last or {}).values():
                for m in matches or []:
                    fid = ((m.get("fixture") or {}).get("id"))
                    if fid:
                        all_fids.add(fid)
            existing = _select_existing_fixture_ids(list(all_fids))
            stats_missing_before = len(all_fids - existing)

            prewarm_statistics_cache(team_last, max_workers=2)

        # 5) Izračunaj sve markete (DB-only) i upiši u model_outputs
        markets = ["1h_over05", "1h_over15", "gg1h", "ft_over15"]
        market_summaries = {}
        rows_by_market = {}

        # precompute sve ulaze (DB-only, jer smo uradili prewarm/fetch u keš)
        preload = prepare_inputs_for_range(start_dt, end_dt)
        pre_tl  = preload["team_last"]
        pre_h2h = preload["h2h"]
        pre_ex  = preload["extras"]

        update_prepare_job(job_id, progress=45, detail="ft_over15 compute")
        rows_ft = compute_ft_over15_for_range(
            start_dt, end_dt, no_api=True,
            preloaded_team_last=pre_tl, preloaded_h2h=pre_h2h, preloaded_extras=pre_ex
        )
        persist_ft_over15(rows_ft)
        market_summaries["ft_over15"] = len(rows_ft or [])
        rows_by_market["ft_over15"] = rows_ft or []

        for mk, prog in [("1h_over05", 65), ("1h_over15", 80), ("gg1h", 90)]:
            update_prepare_job(job_id, progress=prog, detail=f"{mk} compute")
            rows = analyze_fixtures(
                start_dt, end_dt, None, None, mk,
                no_api=True,  # DB-only
                preloaded_team_last=pre_tl, preloaded_h2h=pre_h2h, preloaded_extras=pre_ex
            ) or []
            persist_market_outputs_from_results(mk, rows)
            market_summaries[mk] = len(rows)
            rows_by_market[mk] = rows

        # 6) analysis_cache za ceo dan (po marketu)
        update_prepare_job(job_id, progress=95, detail="cache build")
        for mk in markets:
            params = {
                "from_date": start_dt.isoformat(),
                "to_date": end_dt.isoformat(),
                "from_hour": None,
                "to_hour": None,
                "market": mk,
            }
            key = _build_cache_key(params)
            write_analysis_cache(key, params, rows_by_market.get(mk, []), ttl_hours=CACHE_TTL_HOURS_TODAY)

        # 7) Rezultat
        out = {
            "ok": True,
            "day": d_local.isoformat(),
            "fixtures_in_db": len(fixtures),
            "teams": len(team_ids),
            "pairs": len(pairs),
            "seeded": seeded,
            "history_missing_before": len(hist_missing),
            "h2h_missing_before": len(h2h_missing),
            "stats_missing_before": stats_missing_before,
            "computed": market_summaries,
        }
        update_prepare_job(job_id, status="done", progress=100, detail="finished", result=out)

    except Exception as e:
        update_prepare_job(job_id, status="error", detail=str(e)[:255])
        try:
            logging.exception("prepare job failed")
        except Exception:
            pass
    finally:
        if got_db_lock:
            release_db_lock(lock_name)
        try:
            PREPARE_LOCK.release()
        except Exception:
            pass

def build_micro_db_ft(team_last_matches, stats_fn):
    micro = {}
    for team_id, matches in (team_last_matches or {}).items():
        micro[team_id] = {
            "home": _aggregate_team_micro_ft(team_id, matches, stats_fn, context="home"),
            "away": _aggregate_team_micro_ft(team_id, matches, stats_fn, context="away"),
        }
    return micro

# ---------- FT Over 1.5: batch compute + persist ----------

def compute_ft_over15_for_range(start_dt: datetime, end_dt: datetime, no_api: bool = True,
                                preloaded_team_last: dict[int, list] | None = None,
                                preloaded_h2h: dict[str, list] | None = None,
                                preloaded_extras: dict[int, dict] | None = None):
    print(f"🔍 [DEBUG] compute_ft_over15_for_range START", flush=True)
    fixtures = get_fixtures_in_time_range(start_dt, end_dt, no_api=no_api)
    print(f"🔍 [DEBUG] get_fixtures_in_time_range returned {len(fixtures or [])} fixtures", flush=True)
    if not fixtures:
        return []

    print(f"🔍 [DEBUG] fetch_last_matches_for_teams START", flush=True)
    team_last = dict(preloaded_team_last or {}) or fetch_last_matches_for_teams(fixtures, last_n=DAY_PREFETCH_LAST_N, no_api=no_api)
    print(f"🔍 [DEBUG] fetch_last_matches_for_teams returned {len(team_last or {})} teams", flush=True)

    print(f"🔍 [DEBUG] compute_league_baselines_ft START", flush=True)
    league_bases_ft = compute_league_baselines_ft(team_last, stats_fn=get_fixture_statistics_cached_only)
    print(f"🔍 [DEBUG] compute_league_baselines_ft COMPLETED", flush=True)
    
    print(f"🔍 [DEBUG] compute_team_profiles_ft START", flush=True)
    team_profiles_ft = compute_team_profiles_ft(team_last, stats_fn=get_fixture_statistics_cached_only)
    print(f"🔍 [DEBUG] compute_team_profiles_ft COMPLETED", flush=True)
    
    print(f"🔍 [DEBUG] compute_team_strengths_ft START", flush=True)
    team_strengths_ft = compute_team_strengths_ft(team_last, m_global=(league_bases_ft["global"]["m2p"]*0.9 + 0.25))
    print(f"🔍 [DEBUG] compute_team_strengths_ft COMPLETED", flush=True)
    
    print(f"🔍 [DEBUG] build_micro_db_ft START", flush=True)
    micro_db_ft = build_micro_db_ft(team_last, stats_fn=get_fixture_statistics_cached_only)
    print(f"🔍 [DEBUG] build_micro_db_ft COMPLETED", flush=True)

    print(f"🔍 [DEBUG] fetch_h2h_matches START", flush=True)
    h2h_all = dict(preloaded_h2h or {}) or fetch_h2h_matches(fixtures, last_n=DAY_PREFETCH_H2H_N, no_api=no_api)
    print(f"🔍 [DEBUG] fetch_h2h_matches returned {len(h2h_all or {})} h2h pairs", flush=True)

    print(f"🔍 [DEBUG] Processing {len(fixtures or [])} fixtures START", flush=True)
    rows = []
    for i, fx in enumerate(fixtures or []):
        try:
            print(f"🔍 [DEBUG] Processing fixture {i+1}/{len(fixtures or [])}")
            if not isinstance(fx, dict):
                print(f"🔍 [DEBUG] Fixture {i+1} is not dict, type: {type(fx)}")
                # pokušaj da ga "coerce-uješ" (ako je zalutao tuple/list)
                fx = _coerce_fixture_row_to_api_dict(fx) or {}
                print(f"🔍 [DEBUG] After coercion, type: {type(fx)}")
            fid = int(((fx.get('fixture') or {}).get('id') or 0))
            if not fid:
                print(f"🔍 [DEBUG] Fixture {i+1} has no valid ID, skipping")
                continue
            print(f"🔍 [DEBUG] Fixture {i+1} ID: {fid}")
            
            print(f"🔍 [DEBUG] build_extras_for_fixture START for fixture {fid}")
            extras = build_extras_for_fixture(fx, no_api=True)
            print(f"🔍 [DEBUG] build_extras_for_fixture COMPLETED for fixture {fid}")
            
            print(f"🔍 [DEBUG] calculate_final_probability_ft_over15 START for fixture {fid}")
            p2p, dbg = calculate_final_probability_ft_over15(
                fx, team_last, h2h_all,
                micro_db_ft, league_bases_ft, team_strengths_ft, team_profiles_ft,
                extras=extras, no_api=no_api, market_odds_over15_ft=None
            )
            print(f"🔍 [DEBUG] calculate_final_probability_ft_over15 COMPLETED for fixture {fid}")
            
            rows.append({
                "fixture_id": ((fx.get("fixture") or {}).get("id")),
                "ft_over15_prob": float(round(p2p, 4)),
                "ft_over15_dbg": dbg,
                "kickoff": (fx.get("fixture") or {}).get("date"),
                "league": (fx.get("league") or {}).get("name"),
                "team1": (fx.get("teams") or {}).get("home", {}).get("name"),
                "team2": (fx.get("teams") or {}).get("away", {}).get("name"),
                "final_percent": round(p2p * 100, 2),
            })
            print(f"🔍 [DEBUG] Fixture {i+1} processed successfully")
        except Exception as e:
            print(f"❌ [ERROR] Exception in fixture {i+1}: {str(e)}")
            print(f"❌ [ERROR] Exception type: {type(e)}")
            import traceback
            print(f"❌ [ERROR] Traceback: {traceback.format_exc()}")
            raise e
    print(f"🔍 [DEBUG] compute_ft_over15_for_range COMPLETED, returning {len(rows)} rows")
    return rows

def persist_ft_over15(rows: list[dict]):
    print(f"🔍 [DEBUG] persist_ft_over15 called with {len(rows or [])} rows", flush=True)
    for i, r in enumerate(rows or []):
        fid = r.get("fixture_id")
        if not fid:
            print(f"🔍 [DEBUG] persist_ft_over15 row {i}: no fixture_id, skipping", flush=True)
            continue
        
        dbg = dict(r.get("ft_over15_dbg") or {})
        print(f"🔍 [DEBUG] persist_ft_over15 row {i} (fixture {fid}): debug keys: {list(dbg.keys())}", flush=True)
        print(f"🔍 [DEBUG] persist_ft_over15 row {i}: exp_sot_total: {dbg.get('exp_sot_total')}", flush=True)
        print(f"🔍 [DEBUG] persist_ft_over15 row {i}: exp_da_total: {dbg.get('exp_da_total')}", flush=True)
        print(f"🔍 [DEBUG] persist_ft_over15 row {i}: ref_adj: {dbg.get('ref_adj')}", flush=True)
        
        # obogati debug da bismo sve čitali ISKLJUČIVO iz model_outputs
        if r.get("league"):
            dbg.setdefault("league", r.get("league"))
        if r.get("team1"):
            dbg.setdefault("team1", r.get("team1"))
        if r.get("team2"):
            dbg.setdefault("team2", r.get("team2"))
        if r.get("kickoff"):
            dbg["kickoff"] = r["kickoff"]  # ISO string
        
        print(f"🔍 [DEBUG] persist_ft_over15 row {i}: calling upsert_model_output with prob: {r.get('ft_over15_prob')}", flush=True)
        upsert_model_output(
            fixture_id=int(fid),
            market="ft_over15",
            prob=float(r["ft_over15_prob"]),
            debug=dbg
        )

# ADD: generički upis u model_outputs za bilo koji market iz analyze_fixtures rezultata
def persist_market_outputs_from_results(market: str, results: list[dict]):
    for r in results or []:
        fid = r.get("fixture_id")
        if not fid:
            continue
        dbg = dict(r.get("debug") or {})
        # obogati debug da bismo sve čitali ISKLJUČIVO iz model_outputs
        dbg.setdefault("league", r.get("league"))
        dbg.setdefault("team1", r.get("team1"))
        dbg.setdefault("team2", r.get("team2"))
        if r.get("kickoff"):
            dbg["kickoff"] = r["kickoff"]  # ISO string
        
        # Dodaj sve podatke koji se koriste u read_precomputed_results
        dbg.setdefault("team1_percent", r.get("team1_percent"))
        dbg.setdefault("team2_percent", r.get("team2_percent"))
        dbg.setdefault("team1_hits", r.get("team1_hits"))
        dbg.setdefault("team1_total", r.get("team1_total"))
        dbg.setdefault("team2_hits", r.get("team2_hits"))
        dbg.setdefault("team2_total", r.get("team2_total"))
        dbg.setdefault("h2h_percent", r.get("h2h_percent"))
        dbg.setdefault("h2h_hits", r.get("h2h_hits"))
        dbg.setdefault("h2h_total", r.get("h2h_total"))
        dbg.setdefault("home_shots_percent", r.get("home_shots_percent"))
        dbg.setdefault("home_attacks_percent", r.get("home_attacks_percent"))
        dbg.setdefault("home_shots_used", r.get("home_shots_used"))
        dbg.setdefault("home_attacks_used", r.get("home_attacks_used"))
        dbg.setdefault("away_shots_percent", r.get("away_shots_percent"))
        dbg.setdefault("away_attacks_percent", r.get("away_attacks_percent"))
        dbg.setdefault("away_shots_used", r.get("away_shots_used"))
        dbg.setdefault("away_attacks_used", r.get("away_attacks_used"))
        dbg.setdefault("form_percent", r.get("form_percent"))
        
        # FORM_ADJ i COACH_ADJ
        dbg.setdefault("form_adj", r.get("form_adj"))
        dbg.setdefault("coach_adj", r.get("coach_adj"))

        upsert_model_output(
            fixture_id=int(fid),
            market=market,
            prob=float(r.get("final_percent", 0)) / 100.0,  # final_percent = 0–100
            debug=dbg
        )

# ---------- league baselines (FT totals) ----------
def compute_league_baselines_ft(team_last_matches, stats_fn):
    seen = set()
    by_lid = {}
    global_sot = []; global_da = []
    g_hits2p = 0.0; g_tot = 0.0

    def _extract_ft_totals_both(stats):
        if not stats or len(stats) < 2:
            return (None, None)
        b0, b1 = stats[0], stats[1]
        def full(block, names):
            return _stat_from_block(block, [n.lower() for n in names])
        s0 = full(b0, ["shots on goal","shots on target"])
        s1 = full(b1, ["shots on goal","shots on target"])
        d0 = full(b0, ["dangerous attacks"])
        d1 = full(b1, ["dangerous attacks"])
        sot = (s0 if s0 is not None else 0.0) + (s1 if s1 is not None else 0.0) if (s0 is not None or s1 is not None) else None
        da  = (d0 if d0 is not None else 0.0) + (d1 if d1 is not None else 0.0) if (d0 is not None or d1 is not None) else None
        return (sot, da)

    for team_id, matches in (team_last_matches or {}).items():
        # ISPRAVKA: Bezbedno rukovanje sa podacima koji mogu biti tuple-ovi ili dict-ovi
        safe_matches = []
        for m in (matches or []):
            if isinstance(m, dict):
                safe_matches.append(m)
            elif isinstance(m, (list, tuple)):
                try:
                    converted = _coerce_fixture_row_to_api_dict(m)
                    if converted and isinstance(converted, dict):
                        safe_matches.append(converted)
                except Exception:
                    continue
        for m in safe_matches:
            fid = ((m.get('fixture') or {}).get('id'))
            if not fid or fid in seen: continue
            seen.add(fid)
            lid = ((m.get('league') or {}).get('id')) or -1
            stats = stats_fn(fid)
            sot, da = _extract_ft_totals_both(stats)
            if sot is not None:
                by_lid.setdefault(lid, {"sot": [], "da": [], "hits2p": 0.0, "tot":0.0})
                by_lid[lid]["sot"].append(float(sot)); global_sot.append(float(sot))
            if da is not None:
                by_lid.setdefault(lid, {"sot": [], "da": [], "hits2p": 0.0, "tot":0.0})
                by_lid[lid]["da"].append(float(da)); global_da.append(float(da))
            if _ft_total_ge2(m):
                by_lid.setdefault(lid, {"sot": [], "da": [], "hits2p": 0.0, "tot":0.0})
                by_lid[lid]["hits2p"] += 1.0
                g_hits2p += 1.0
            by_lid.setdefault(lid, {"sot": [], "da": [], "hits2p": 0.0, "tot":0.0})
            by_lid[lid]["tot"] += 1.0
            g_tot += 1.0

    def _pack(arr_sot, arr_da, h2p, tot):
        mu_sot = float(sum(arr_sot)/len(arr_sot)) if arr_sot else None
        mu_da  = float(sum(arr_da)/len(arr_da)) if arr_da else None
        sd_sot = (sum((x-mu_sot)**2 for x in arr_sot)/max(1,len(arr_sot)-1))**0.5 if arr_sot and mu_sot is not None and len(arr_sot)>=2 else None
        sd_da  = (sum((x-mu_da)**2  for x in arr_da)/max(1,len(arr_da)-1))**0.5  if arr_da  and mu_da  is not None and len(arr_da) >=2 else None
        q95_sot = _percentile(arr_sot, 0.95)
        q95_da  = _percentile(arr_da, 0.95)
        m2p = float(h2p/tot) if tot>0 else 0.62
        return {"mu_sotFT":mu_sot, "sd_sotFT":sd_sot, "q95_sotFT":q95_sot,
                "mu_daFT":mu_da,   "sd_daFT":sd_da,   "q95_daFT":q95_da,
                "m2p": m2p}

    leagues = { lid:_pack(obj["sot"], obj["da"], obj["hits2p"], obj["tot"])
                for lid,obj in by_lid.items() }
    global_base = _pack(global_sot, global_da, g_hits2p, g_tot)
    for lid, base in leagues.items():
        for k,v in base.items():
            if v is None:
                base[k] = global_base.get(k)
    return {"global": global_base, "leagues": leagues}


def _league_base_ft_for_fixture(fixture, league_baselines_ft):
    lid = ((fixture.get('league') or {}).get('id'))
    base = None
    if league_baselines_ft and isinstance(league_baselines_ft, dict):
        base = (league_baselines_ft.get('leagues') or {}).get(lid)
        if not base: base = league_baselines_ft.get('global')
    if not base:
        base = {"mu_sotFT": 5.2, "sd_sotFT": 1.8, "q95_sotFT": SOTFT_CAP,
                "mu_daFT": 80.0, "sd_daFT": 18.0, "q95_daFT": DAFT_CAP,
                "m2p": 0.62}
    return base

# ---------- team profiles FT (finish/leak/gk, tier, recency) ----------
def compute_team_profiles_ft(team_last_matches, stats_fn, lam=6.0, max_n=15):
    profiles = {}
    for team_id, matches in (team_last_matches or {}).items():
        # ISPRAVKA: Bezbedno rukovanje sa podacima koji mogu biti tuple-ovi ili dict-ovi
        safe_matches = []
        for m in (matches or []):
            if isinstance(m, dict):
                safe_matches.append(m)
            elif isinstance(m, (list, tuple)):
                try:
                    converted = _coerce_fixture_row_to_api_dict(m)
                    if converted and isinstance(converted, dict):
                        safe_matches.append(converted)
                except Exception:
                    continue
        arr = sorted(safe_matches, key=lambda m: (m.get('fixture') or {}).get('date',''), reverse=True)[:max_n]
        w_sot_for = w_sot_alw = w_pos = 0.0
        sum_sot_for = sum_sot_alw = sum_pos = 0.0
        g_for_w = g_alw_w = 0.0
        xg_for_w = 0.0
        sot_alw_w = 0.0
        last_match_dt = None
        eff_n = 0.0
        match_dates = []

        for i, m in enumerate(arr):
            w = _exp_w(i, lam)
            fix = (m.get('fixture') or {}); fid = fix.get('id')
            if not fid: continue
            teams = (m.get('teams') or {}); hid = ((teams.get('home') or {}).get('id')); aid = ((teams.get('away') or {}).get('id'))
            if hid is None or aid is None: continue
            opp_id = aid if hid == team_id else hid

            stats = stats_fn(fid)
            micro = _extract_match_micro_for_team_ft(stats, team_id, opp_id) if stats else None
            if micro:
                if micro.get('sot_for') is not None:
                    sum_sot_for += w * micro['sot_for']; w_sot_for += w
                if micro.get('sot_allowed') is not None:
                    sum_sot_alw += w * micro['sot_allowed']; w_sot_alw += w; sot_alw_w += w*micro['sot_allowed']
                if micro.get('pos') is not None:
                    sum_pos += w * micro['pos']; w_pos += w
                if micro.get('xg_for') is not None:
                    xg_for_w += w * micro['xg_for']

            ft = ((m.get('score') or {}).get('fulltime') or {})
            g_for_w += w * float((ft.get('home') if hid==team_id else ft.get('away')) or 0)
            g_alw_w += w * float((ft.get('away') if hid==team_id else ft.get('home')) or 0)

            eff_n += w
            if last_match_dt is None:
                try: last_match_dt = datetime.fromisoformat((fix.get('date') or '').replace("Z","+00:00"))
                except: pass
            try: match_dates.append(datetime.fromisoformat((fix.get('date') or '').replace("Z","+00:00")))
            except: pass

        mean_sot_for = _safe_div(sum_sot_for, w_sot_for, None)
        mean_sot_alw = _safe_div(sum_sot_alw, w_sot_alw, None)
        mean_pos     = _safe_div(sum_pos,     w_pos,     None)

        fin  = beta_shrunk_rate(g_for_w,  sum_sot_for if sum_sot_for>0 else None,  m=FINISH_PRIOR_FT, tau=8.0)
        leak = beta_shrunk_rate(g_alw_w,  sum_sot_alw if sum_sot_alw>0 else None,  m=LEAK_PRIOR_FT,   tau=8.0)

        fin_xg = 0.0
        if xg_for_w > 0:
            raw = (g_for_w - xg_for_w) / max(1e-6, xg_for_w)
            fin_xg = (raw * eff_n + 0.0 * 8.0) / max(1e-6, eff_n + 8.0)

        gk_stop = GK_SAVE_PRIOR_FT
        if sot_alw_w > 0:
            save_rate = 1.0 - _safe_div(g_alw_w, sot_alw_w, 0.0)
            saves_est = save_rate * sot_alw_w
            gk_stop = beta_shrunk_rate(saves_est, sot_alw_w, m=GK_SAVE_PRIOR_FT, tau=GK_SAVE_TAU_FT)

        tier = _infer_team_tier_from_matches(arr, fallback=DEFAULT_TEAM_TIER)

        profiles[team_id] = {
            "sot_for": mean_sot_for, "sot_allowed": mean_sot_alw,
            "pos": mean_pos,
            "finish": fin, "leak": leak,
            "eff_n": eff_n,
            "fin_effn": max(0.0, float(sum_sot_for)),
            "leak_effn": max(0.0, float(sum_sot_alw)),
            "gk_stop": gk_stop,
            "fin_xg": fin_xg,
            "last_match_dt": last_match_dt,
            "match_dates": match_dates,
            "tier": tier,
        }
    return profiles

# ---------- FT matchup features (isti stil kao 1H, ali FT) ----------
def matchup_features_enhanced_ft(fixture, team_profiles_ft, league_baselines_ft, micro_db_ft=None, extras: dict|None=None):
    base = _league_base_ft_for_fixture(fixture, league_baselines_ft)
    home_id = ((fixture.get('teams') or {}).get('home') or {}).get('id')
    away_id = ((fixture.get('teams') or {}).get('away') or {}).get('id')

    ph = (team_profiles_ft or {}).get(home_id, {}) or {}
    pa = (team_profiles_ft or {}).get(away_id, {}) or {}

    mh = (micro_db_ft or {}).get(home_id, {}).get("home", {}) if micro_db_ft else {}
    ma = (micro_db_ft or {}).get(away_id, {}).get("away", {}) if micro_db_ft else {}

    def _comb(v_for, v_alw, cap): 
        vals = [v for v in (v_for, v_alw) if v is not None]
        if not vals: return None
        return max(0.0, min(cap, sum(vals)/len(vals)))

    exp_sot_home = _comb(mh.get("sot_for"), ma.get("sot_allowed"), SOTFT_CAP)
    exp_sot_away = _comb(ma.get("sot_for"), mh.get("sot_allowed"), SOTFT_CAP)
    exp_da_home  = _comb(mh.get("da_for"),  ma.get("da_allowed"),  DAFT_CAP)
    exp_da_away  = _comb(ma.get("da_for"),  mh.get("da_allowed"),  DAFT_CAP)

    # dodatne mikro
    shots_h, shots_a = mh.get("shots_for"), ma.get("shots_for")
    xg_h,    xg_a    = mh.get("xg_for"),    ma.get("xg_for")
    big_h,   big_a   = mh.get("big_for"),   ma.get("big_for")
    corn_h,  corn_a  = mh.get("corn_for"),  ma.get("corn_for")
    fk_h,    fk_a    = mh.get("fk_for"),    ma.get("fk_for")

    offs_h,  offs_a  = mh.get("offs_for"),  ma.get("offs_for")
    cross_h, cross_a = mh.get("cross_for"), ma.get("cross_for")
    cnt_h,   cnt_a   = mh.get("counter_for"), ma.get("counter_for")
    saves_h, saves_a = mh.get("saves_for"),   ma.get("saves_for")
    sib_h,   sib_a   = mh.get("sib_for"),     ma.get("sib_for")
    sob_h,   sob_a   = mh.get("sob_for"),     ma.get("sob_for")
    wood_h,  wood_a  = mh.get("wood_for"),    ma.get("wood_for")

    setp_xg_total = 0.0
    for val in (corn_h, corn_a):
        if val is not None: setp_xg_total += val * SETP_XG_PER_CORNER_FT
    for val in (fk_h, fk_a):
        if val is not None: setp_xg_total += val * SETP_XG_PER_FK_FT

    pos_edge = None
    if ph.get("pos") is not None and pa.get("pos") is not None:
        pos_edge = max(-1.0, min(1.0, (float(ph["pos"]) - float(pa["pos"])) / 100.0))

    try:
        fixture_dt_utc = datetime.fromisoformat(((fixture.get('fixture') or {}).get('date') or '').replace("Z","+00:00"))
    except Exception:
        fixture_dt_utc = None
    def _rest_days(profile):
        lm = profile.get("last_match_dt")
        if not lm or not fixture_dt_utc: return None
        return max(0.0, (fixture_dt_utc - lm).total_seconds()/86400.0)
    rest_h = _rest_days(ph); rest_a = _rest_days(pa)

    def _congestion(profile):
        ds = profile.get("match_dates") or []
        if not fixture_dt_utc: return 0.0
        c7 = sum(1 for d in ds if (fixture_dt_utc - d).total_seconds()/86400.0 <= 7.0 and d < fixture_dt_utc)
        c14= sum(1 for d in ds if (fixture_dt_utc - d).total_seconds()/86400.0 <= 14.0 and d < fixture_dt_utc)
        overload = max(0.0, (c7-2)*0.5 + (c14-4)*0.25)
        return min(1.0, overload)
    congest_h = _congestion(ph); congest_a = _congestion(pa)

    tier_h = int(ph.get("tier") or DEFAULT_TEAM_TIER); tier_h = max(1, min(4, tier_h))
    tier_a = int(pa.get("tier") or DEFAULT_TEAM_TIER); tier_a = max(1, min(4, tier_a))
    tier_gap_home = max(-MAX_TIER_GAP, min(MAX_TIER_GAP, (tier_a - tier_h)))
    tier_gap_away = max(-MAX_TIER_GAP, min(MAX_TIER_GAP, (tier_h - tier_a)))
    lgname_raw = ((fixture.get('league') or {}).get('name') or '')
    is_cup = _is_cup(lgname_raw) or ((fixture.get('league') or {}).get('type','').lower()=='cup')

    # FIN/LEAK/GK/FINxG
    fin_h, fin_a = ph.get("finish", FINISH_PRIOR_FT), pa.get("finish", FINISH_PRIOR_FT)
    leak_h, leak_a = ph.get("leak", LEAK_PRIOR_FT),   pa.get("leak", LEAK_PRIOR_FT)
    gk_h, gk_a = ph.get("gk_stop", GK_SAVE_PRIOR_FT), pa.get("gk_stop", GK_SAVE_PRIOR_FT)
    finxg_h, finxg_a = ph.get("fin_xg", 0.0), pa.get("fin_xg", 0.0)

    # multiplikatori (ref, weather, lineups, penvar/stadion) – recikliraj postojeće utilse
    lineup = compute_lineup_projection_1h(((fixture.get('fixture') or {}).get('id')),
                api_or_repo_get_lineups=lambda fid: repo.get_lineups(fid, no_api=False))
    ref_prof = compute_referee_profile(
        ((fixture.get('fixture') or {}).get('referee')), ((fixture.get('league') or {}).get('season'))
    ) or {"ref_adj":0.0,"used":0.0}
    wx = compute_weather_factor_1h({"fixture": {"weather": (extras or {}).get("weather_obj")}}) \
         if (extras and extras.get("weather_obj")) else compute_weather_factor_1h(fixture)
    penvar = compute_penvar_profile_1h(fixture, team_profiles_ft, referee_profile=None)  # ok kao mali mult
    stadium= compute_stadium_pitch_1h(fixture)

    # importance
    try:
        imp_raw = float(calculate_match_importance(((fixture.get('fixture') or {}).get('id'))))
    except:
        imp_raw = 5.0
    importance_adj = max(-0.5, min(0.5, (imp_raw - 5.0) / 5.0))

    cov_sot = min((mh.get("used_sot",0) or 0) + (ma.get("used_sot",0) or 0), 16)/16.0
    cov_da  = min((mh.get("used_da",0)  or 0) + (ma.get("used_da",0)  or 0), 16)/16.0
    cov_pos = min((mh.get("used_pos",0) or 0), 8)/8.0

    out = {
        "exp_sotFT_home": exp_sot_home, "exp_sotFT_away": exp_sot_away,
        "exp_daFT_home":  exp_da_home,  "exp_daFT_away":  exp_da_away,
        "pos_edge": pos_edge,

        "shotsFT_home": shots_h, "shotsFT_away": shots_a,
        "xgFT_home": xg_h,       "xgFT_away": xg_a,
        "bigFT_home": big_h,     "bigFT_away": big_a,
        "setp_xg_total": setp_xg_total,

        "offsFT_home": offs_h, "offsFT_away": offs_a,
        "crossFT_home": cross_h, "crossFT_away": cross_a,
        "counterFT_home": cnt_h, "counterFT_away": cnt_a,
        "savesFT_home": saves_h, "savesFT_away": saves_a,
        "sibFT_home": sib_h, "sibFT_away": sib_a,
        "sobFT_home": sob_h, "sobFT_away": sob_a,
        "woodFT_home": wood_h, "woodFT_away": wood_a,

        "fin_home": fin_h, "fin_away": fin_a,
        "leak_home": leak_h, "leak_away": leak_a,
        "gk_home": gk_h, "gk_away": gk_a,
        "finxg_home": finxg_h, "finxg_away": finxg_a,

        "rest_home": rest_h, "rest_away": rest_a,
        "congest_home": congest_h, "congest_away": congest_a,

        "tier_gap_home": tier_gap_home, "tier_gap_away": tier_gap_away, "is_cup": is_cup,

        "cov_sot": cov_sot, "cov_da": cov_da, "cov_pos": cov_pos,

        "ref_mult": 1.0,  # zadržavamo mult-e kao ln(mult) niže; ovde može ostati 1.0
        "weather_mult": wx.get("multiplier", 1.0),
        "penvar_mult": penvar.get("multiplier", 1.0),
        "stadium_mult": stadium.get("multiplier", 1.0),
        "lineup_mult_home": (lineup.get("home") or {}).get("multiplier", 1.0),
        "lineup_mult_away": (lineup.get("away") or {}).get("multiplier", 1.0),

        "ref_adj": float(ref_prof.get("ref_adj") or 0.0),
        "weather_adj": (extras or {}).get("weather_adj", 0.0),
        "venue_adj":   (extras or {}).get("venue_adj", 0.0),
        "lineup_adj":  (extras or {}).get("lineup_adj", 0.0),
        "inj_adj":     (extras or {}).get("inj_adj", 0.0),

        "importance_adj": importance_adj,
    }
    return out

# ---------- team strengths FT (att/def_allow ~ FT score≥1) ----------
def compute_team_strengths_ft(team_last_matches, lam=6.0, max_n=15, m_global=0.70):
    strengths = {}
    for team_id, matches in (team_last_matches or {}).items():
        # ISPRAVKA: Bezbedno rukovanje sa podacima koji mogu biti tuple-ovi ili dict-ovi
        safe_matches = []
        for m in (matches or []):
            if isinstance(m, dict):
                safe_matches.append(m)
            elif isinstance(m, (list, tuple)):
                try:
                    converted = _coerce_fixture_row_to_api_dict(m)
                    if converted and isinstance(converted, dict):
                        safe_matches.append(converted)
                except Exception:
                    continue
        h_sc, w_sc, _ = _weighted_counts(safe_matches, lambda m: _team_scored_ft(m, team_id), lam, max_n)
        h_con, w_con, _ = _weighted_counts(safe_matches, lambda m: _team_conceded_ft(m, team_id), lam, max_n)
        att = beta_shrunk_rate(h_sc, w_sc, m=m_global, tau=10.0)
        def_allow = beta_shrunk_rate(h_con, w_con, m=m_global, tau=10.0)
        strengths[team_id] = {"att": att, "def_allow": def_allow, "eff_n": (w_sc or 0)+(w_con or 0)}
    return strengths

# ---------- FT: per-team p(score≥1) → λ; pa P(2+) ----------
def predict_team_scores_ft_enhanced(fixture, feats, league_baselines_ft, team_strengths_ft, side='home'):
    base = _league_base_ft_for_fixture(fixture, league_baselines_ft)
    m2p = base["m2p"]  # ligaški P(Total≥2); za team baseline koristimo ~70% za score≥1
    # per-team mu/sigma iz total baseline (po timu ~ /2)
    mu_sot = (base.get("mu_sotFT") or 0.0)/2.0
    sd_sot = max(1e-6, (base.get("sd_sotFT") or 1.0)/(2**0.5))
    mu_da  = (base.get("mu_daFT")  or 0.0)/2.0
    sd_da  = max(1e-6, (base.get("sd_daFT")  or 1.0)/(2**0.5))

    if side=='home':
        exp_sot = feats.get("exp_sotFT_home"); exp_da = feats.get("exp_daFT_home")
        pos_share = feats.get("pos_edge") or 0.0
        lineup_mult = feats.get("lineup_mult_home", 1.0)
    else:
        exp_sot = feats.get("exp_sotFT_away"); exp_da = feats.get("exp_daFT_away")
        pos_share = -(feats.get("pos_edge") or 0.0)
        lineup_mult = feats.get("lineup_mult_away", 1.0)

    z_sot = _z(exp_sot, mu_sot, sd_sot)
    z_da  = _z(exp_da,  mu_da,  sd_da)

    # mikro z-score (FT)
    mu_shots, sd_shots = 12.0, 4.0
    mu_xg,    sd_xg    = 1.20, 0.55
    mu_big,   sd_big   = 1.6,  1.2

    shots = feats.get("shotsFT_home") if side=='home' else feats.get("shotsFT_away")
    xg    = feats.get("xgFT_home")    if side=='home' else feats.get("xgFT_away")
    big   = feats.get("bigFT_home")   if side=='home' else feats.get("bigFT_away")

    z_shots = _z(shots, mu_shots, sd_shots)
    z_xg    = _z(xg,    mu_xg,    sd_xg)
    z_big   = _z(big,   mu_big,   sd_big)

    # dodatni mikro (FT)
    def _zft(val, mu, sd): return _z(val, mu, sd)
    z_offs  = _zft(feats.get("offsFT_home") if side=='home' else feats.get("offsFT_away"), 1.8, 1.2)
    z_cross = _zft(feats.get("crossFT_home") if side=='home' else feats.get("crossFT_away"), 18.0, 7.0)
    z_cnt   = _zft(feats.get("counterFT_home") if side=='home' else feats.get("counterFT_away"), 6.0, 3.0)
    z_saves = _zft(feats.get("savesFT_home") if side=='home' else feats.get("savesFT_away"), 3.4, 2.0)
    z_sib   = _zft(feats.get("sibFT_home") if side=='home' else feats.get("sibFT_away"), 6.0, 3.0)
    z_sob   = _zft(feats.get("sobFT_home") if side=='home' else feats.get("sobFT_away"), 7.0, 3.0)
    z_wood  = _zft(feats.get("woodFT_home") if side=='home' else feats.get("woodFT_away"), 0.4, 0.7)

    # FIN/LEAK/GK/FINxG/REST/CONG
    fin = feats.get('fin_home') if side=='home' else feats.get('fin_away')
    leak_opp = feats.get('leak_away') if side=='home' else feats.get('leak_home')
    gk_me = feats.get('gk_home') if side=='home' else feats.get('gk_away')
    finxg = feats.get('finxg_home') if side=='home' else feats.get('finxg_away')
    rest  = feats.get('rest_home') if side=='home' else feats.get('rest_away')
    congest = feats.get('congest_home') if side=='home' else feats.get('congest_away')

    fin_adj  = _logit(min(0.99, max(0.01, fin or FINISH_PRIOR_FT)))    - _logit(FINISH_PRIOR_FT)
    leak_adj = _logit(min(0.99, max(0.01, leak_opp or LEAK_PRIOR_FT))) - _logit(LEAK_PRIOR_FT)
    gk_adj = (float(gk_me or GK_SAVE_PRIOR_FT) - GK_SAVE_PRIOR_FT)
    finxg_adj = float(finxg or 0.0)

    rest_z = 0.0
    if rest is not None:
        rest_z = max(-1.0, min(1.0, (float(rest) - REST_REF_DAYS) / 5.0))
    congest_adj = -float(congest or 0.0)

    # team/opp strength (FT score≥1 baseline ~0.70)
    base_att = 0.70
    home_id = ((fixture.get('teams') or {}).get('home') or {}).get('id')
    away_id = ((fixture.get('teams') or {}).get('away') or {}).get('id')
    team_id = home_id if side=='home' else away_id
    opp_id  = away_id if side=='home' else home_id
    sT  = (team_strengths_ft or {}).get(team_id, {"att": base_att, "def_allow": base_att})
    sOpp= (team_strengths_ft or {}).get(opp_id,  {"att": base_att, "def_allow": base_att})
    att_adj  = _logit(sT['att'])         - _logit(base_att)
    defo_adj = _logit(sOpp['def_allow']) - _logit(base_att)

    # ln multipliers
    ln_wx   = _ln_mult(feats.get("weather_mult",1.0))
    ln_line = _ln_mult(lineup_mult)
    ln_pen  = _ln_mult(feats.get("penvar_mult",1.0))
    ln_stad = _ln_mult(feats.get("stadium_mult",1.0))

    # težine (FT)
    W = WEIGHTS_FT
    cov_sot = feats.get('cov_sot',1.0); cov_da = feats.get('cov_da',1.0); cov_pos = feats.get('cov_pos',1.0)
    w_zsot = W["Z_SOT"] * cov_sot
    w_zda  = W["Z_DA"]  * cov_da
    w_pos  = W["POS"]   * cov_pos
    w_shots= W["Z_SHOTS"]; w_xg = W["Z_XG"]; w_big = W["Z_BIGCH"]
    w_offs = W["Z_OFFSIDES"]; w_cross=W["Z_CROSSES"]; w_cnt=W["Z_COUNTERS"]; w_saves=W["Z_SAVES"]
    w_sib  = W["Z_SIB"]; w_sob=W["Z_SOB"]; w_wood=W["Z_WOODWORK"]
    w_fin  = W["FIN"]; w_leak=W["LEAK"]; w_gk=W["GK"]; w_cong=W["CONGEST"]

    class_weight = W["TIER_GAP"] * (CUP_TIER_MULT if feats.get("is_cup") else 1.0)
    tier_gap = float(feats.get('tier_gap_home' if side=='home' else 'tier_gap_away') or 0.0)

    global_adj = 0.0
    if side=='home':
        global_adj += W.get("REF",0.0)         * float(feats.get("ref_adj") or 0.0)
        global_adj += W.get("ENV_WEATHER",0.0) * float(feats.get("weather_adj") or 0.0)
        global_adj += W.get("VENUE",0.0)       * float(feats.get("venue_adj") or 0.0)
        global_adj += W.get("LINEUPS",0.0)     * float(feats.get("lineup_adj") or 0.0)
        global_adj += W.get("INJ",0.0)         * float(feats.get("inj_adj") or 0.0)
        global_adj += W.get("IMPORTANCE",0.0)  * float(feats.get("importance_adj") or 0.0)
        global_adj += W.get("SETP",0.10)       * float(feats.get("setp_xg_total") or 0.0)

    z = (
        _logit(base_att) + W["BIAS"]
        + w_zsot*z_sot + w_zda*z_da + w_pos*pos_share
        + w_shots*z_shots + w_xg*z_xg + w_big*z_big
        + w_offs*z_offs + w_cross*z_cross + w_cnt*z_cnt
        + w_saves*(-z_saves) + w_sib*z_sib + w_sob*z_sob + w_wood*z_wood
        + w_fin*fin_adj + w_leak*leak_adj + w_gk*gk_adj + 0.5*w_fin*finxg_adj
        + W["REST"]*rest_z + w_cong*congest_adj
        + (W["HOME"] if side=='home' else 0.0)
        + W["ATT"]*att_adj + W["DEF"]*defo_adj
        + class_weight * tier_gap
        + W["WEATHER_MULT"]*ln_wx + W["LINEUP_MULT"]*ln_line + W["PENVAR_MULT"]*ln_pen + W["STADIUM_MULT"]*ln_stad
        + global_adj
    )

    p_score_ge1 = _inv_logit(z)
    p_score_ge1 = _calibrate(p_score_ge1, temp=CALIBRATION_FT["TEMP"], floor=CALIBRATION_FT["FLOOR"], ceil=CALIBRATION_FT["CEIL"])

    # pretvori u λ (Poisson) preko p = 1 - e^{-λ}  → λ = -ln(1-p)
    lam = -math.log(max(1e-9, 1.0 - p_score_ge1))
    return lam, {
        "p_ge1": round(p_score_ge1,3),
        "lam": round(lam,3),
        "z_sot": round(z_sot,3), "z_da": round(z_da,3), "z_shots": round(z_shots,3),
        "z_xg": round(z_xg,3), "z_big": round(z_big,3),
        "z_offs": round(z_offs,3), "z_cross": round(z_cross,3), "z_cnt": round(z_cnt,3),
        "z_saves": round(z_saves,3), "z_sib": round(z_sib,3), "z_sob": round(z_sob,3), "z_wood": round(z_wood,3),
        "fin_adj": round(fin_adj,3), "leak_adj": round(leak_adj,3), "gk_adj": round(gk_adj,3), "finxg": round(finxg_adj,3),
        "rest_z": round(rest_z,3), "congest": round(congest_adj,3),
        "att_adj": round(att_adj,3), "def_adj": round(defo_adj,3),
        "class_gap": round(tier_gap,3)
    }

# ---------- prior (teams, H2H, minute buckets FT) ----------
def _weighted_match_over15_ft_rate(matches, lam=6.0, max_n=15):
    h, w, _ = _weighted_counts(matches, _ft_total_ge2, lam, max_n)
    return ((h/w) if w>0 else None, h, w)

def _weighted_h2h_over15_ft_rate(h2h_matches, lam=5.0, max_n=10):
    # ISPRAVKA: Bezbedno rukovanje sa podacima koji mogu biti tuple-ovi ili dict-ovi
    safe_matches = []
    for m in (h2h_matches or []):
        if isinstance(m, dict):
            safe_matches.append(m)
        elif isinstance(m, (list, tuple)):
            # Pokušaj da konvertuješ tuple u dict
            try:
                converted = _coerce_fixture_row_to_api_dict(m)
                if converted and isinstance(converted, dict):
                    safe_matches.append(converted)
            except Exception:
                continue
    
    arr = sorted(safe_matches, key=lambda m: (m.get('fixture') or {}).get('date',''), reverse=True)
    h, w, _ = _weighted_counts(arr, _ft_total_ge2, lam, max_n)
    return ((h/w) if w>0 else None, h, w)

def _minute_bucket_prior_ft(repo, fixture, no_api=False):
    league_id = ((fixture.get('league') or {}).get('id'))
    season    = ((fixture.get('league') or {}).get('season'))
    home_id   = ((fixture.get('teams') or {}).get('home') or {}).get('id')
    away_id   = ((fixture.get('teams') or {}).get('away') or {}).get('id')
    if not all([league_id, season, home_id, away_id]): return (None, 0.0)

    th = repo.get_team_statistics(home_id, league_id, season, no_api=no_api) or {}
    ta = repo.get_team_statistics(away_id, league_id, season, no_api=no_api) or {}

    def _sum_minutes(d):
        # normalno vreme
        mins = (((d.get("goals") or {}).get("for") or {}).get("minute") or {})
        def _tot(key): 
            x = (mins.get(key) or {}).get("total")
            return 0.0 if x is None else float(x)
        # 0-15,16-30,31-45,46-60,61-75,76-90
        return sum(_tot(k) for k in ("0-15","16-30","31-45","46-60","61-75","76-90"))
    def _sum_minutes_against(d):
        mins = (((d.get("goals") or {}).get("against") or {}).get("minute") or {})
        def _tot(key):
            x = (mins.get(key) or {}).get("total")
            return 0.0 if x is None else float(x)
        return sum(_tot(k) for k in ("0-15","16-30","31-45","46-60","61-75","76-90"))

    gf_h = _sum_minutes(th); ga_h = _sum_minutes_against(th)
    gf_a = _sum_minutes(ta); ga_a = _sum_minutes_against(ta)

    played_h = (((th.get("fixtures") or {}).get("played") or {}).get("total") or 0) or 0
    played_a = (((ta.get("fixtures") or {}).get("played") or {}).get("total") or 0) or 0
    games = max(1, min(played_h, played_a))  # grubo

    lam_total = 0.0
    # for su pun signal; against malo slabije (kao i 1H)
    lam_total += (gf_h + gf_a) / games
    lam_total += 0.5 * (ga_h + ga_a) / games

    p2p = _poisson_p_ge2(lam_total)
    effn = 4.0  # blaga preciznost
    return (p2p, effn)

# ---------- glavna FT funkcija: 2+ ----------
def calculate_final_probability_ft_over15(
    fixture, team_last_matches, h2h_results, micro_db_ft,
    league_baselines_ft, team_strengths_ft, team_profiles_ft,
    extras: dict|None=None, no_api: bool=False, market_odds_over15_ft: Optional[float]=None
):
    base = _league_base_ft_for_fixture(fixture, league_baselines_ft)
    m2p = base["m2p"]

    # TEAM prior (FT≥2 na mečevima timova – kao "match total", pa uzmi sredinu)
    home_id = ((fixture.get('teams') or {}).get('home') or {}).get('id')
    away_id = ((fixture.get('teams') or {}).get('away') or {}).get('id')
    
    # Koristi team_ft_over15_stats za stvarne brojeve mečeva
    team1_percent, h_home, w_home = team_ft_over15_stats(team_last_matches.get(home_id, []))
    team2_percent, h_away, w_away = team_ft_over15_stats(team_last_matches.get(away_id, []))
    
    # Konvertuj procente u verovatnoće za kalkulacije
    p_home_raw = team1_percent / 100.0 if team1_percent is not None else None
    p_away_raw = team2_percent / 100.0 if team2_percent is not None else None

    p_home = beta_shrunk_rate(h_home, w_home, m=m2p, tau=10.0) if p_home_raw is not None else m2p
    p_away = beta_shrunk_rate(h_away, w_away, m=m2p, tau=10.0) if p_away_raw is not None else m2p
    p_team_prior = (p_home + p_away)/2.0

    # H2H prior
    a, b = sorted([home_id, away_id]); key = f"{a}-{b}"
    print(f"🔍 [DEBUG] calculate_final_probability_ft_over15 H2H key: {key}")
    print(f"🔍 [DEBUG] calculate_final_probability_ft_over15 h2h_results keys: {list(h2h_results.keys())}")
    h2h_matches = h2h_results.get(key, [])
    print(f"🔍 [DEBUG] calculate_final_probability_ft_over15 h2h_matches for key {key}: {len(h2h_matches)} matches")
    h2h_percent, h_h2h, w_h2h = h2h_ft_over15_stats(h2h_matches)
    p_h2h_raw = h2h_percent / 100.0 if h2h_percent is not None else None
    p_h2h = beta_shrunk_rate(h_h2h, w_h2h, m=m2p, tau=12.0) if p_h2h_raw is not None else m2p
    effn_h2h = (w_h2h or 0.0) * 0.4
    if (w_h2h or 0.0) < 2.5:
        p_h2h = m2p; effn_h2h = 0.0

    p_prior_tmp, _ = fuse_probs_by_precision(
        p_team_prior, (w_home or 0.0)+(w_away or 0.0),
        p_h2h,        effn_h2h
    )

    # minute-bucket prior FT (blagi blend)
    p_minute, effn_minute = _minute_bucket_prior_ft(repo, fixture, no_api=no_api)
    if p_minute is not None:
        w_min = float(WEIGHTS_FT.get("MINUTE_PRIOR_BLEND", 0.25))
        p_prior = (1.0 - w_min)*p_prior_tmp + w_min*p_minute
    else:
        p_prior = p_prior_tmp

    # mali prior adj iz FTS/CS/Form (isti helper koristi)
    fts_cs_adj = _fts_cs_form_coach_adj(repo, fixture, no_api=no_api)
    prior_logit_adj = WEIGHTS_FT.get("FTSCS_ADJ", 0.05) * fts_cs_adj
    p_prior = _inv_logit(_logit(p_prior) + prior_logit_adj)
    
    # --- FORM_ADJ i COACH_ADJ implementacija ---
    form_adj = _calculate_form_adjustment(fixture, team_last_matches, no_api=no_api)
    coach_adj = _calculate_coach_adjustment(fixture, no_api=no_api)
    
    # Dodaj u prior
    p_prior = _inv_logit(_logit(p_prior) + WEIGHTS_FT.get("FORM_ADJ", 0.03) * form_adj)
    p_prior = _inv_logit(_logit(p_prior) + WEIGHTS_FT.get("COACH_ADJ", 0.02) * coach_adj)
    
    # --- KRITIČNI FEATURE-I ---
    # Treba da uključimo 4 kritična feature-a u p_prior kalkulaciju
    feats_temp = matchup_features_enhanced_ft(fixture, team_profiles_ft, league_baselines_ft, micro_db_ft=micro_db_ft, extras=extras)
    
    # 1. pace_da_total - tempo dangerous attacks (FT verzija)
    pace_da_total = feats_temp.get("pace_da_total", 0.0)
    if pace_da_total > 0:
        base_da = _league_base_for_fixture(fixture, league_baselines_ft).get("mu_da_ft", 0.0)
        pace_da_z = _z(pace_da_total, base_da, max(1.0, base_da * 0.5)) if base_da > 0 else 0.0
        pace_da_adj = WEIGHTS_FT.get("PACE_DA_ADJ", 0.04) * pace_da_z
        p_prior = _inv_logit(_logit(p_prior) + pace_da_adj)
    
    # 2. lineups_have - da li imamo lineup podatke (pozitivno)
    lineups_have = feats_temp.get("lineups_have", False)
    if lineups_have:
        lineups_have_adj = WEIGHTS_FT.get("LINEUPS_HAVE_ADJ", 0.03)
        p_prior = _inv_logit(_logit(p_prior) + lineups_have_adj)
    
    # 3. lineups_fw_count - broj napadača (pozitivno)
    lineups_fw_count = feats_temp.get("lineups_fw_count")
    if lineups_fw_count is not None and lineups_fw_count > 0:
        # Normalizuj na 0-1 skalu (pretpostavljamo 1-4 napadača)
        fw_normalized = max(0.0, min(1.0, (lineups_fw_count - 1) / 3.0))
        lineups_fw_adj = WEIGHTS_FT.get("LINEUPS_FW_ADJ", 0.02) * fw_normalized
        p_prior = _inv_logit(_logit(p_prior) + lineups_fw_adj)
    
    # 4. inj_count - broj povreda (negativno)
    inj_count = feats_temp.get("inj_count")
    if inj_count is not None and inj_count > 0:
        # Negativan uticaj - više povreda = manja verovatnoća
        inj_normalized = min(1.0, inj_count / 10.0)  # Normalizuj na 0-1
        inj_count_adj = -WEIGHTS_FT.get("INJ_COUNT_ADJ", 0.02) * inj_normalized
        p_prior = _inv_logit(_logit(p_prior) + inj_count_adj)

    # MICRO -> λ_home, λ_away -> P(Total≥2)
    feats = matchup_features_enhanced_ft(fixture, team_profiles_ft, league_baselines_ft, micro_db_ft=micro_db_ft, extras=extras)
    lam_h, dbg_h = predict_team_scores_ft_enhanced(fixture, feats, league_baselines_ft, team_strengths_ft, side='home')
    lam_a, dbg_a = predict_team_scores_ft_enhanced(fixture, feats, league_baselines_ft, team_strengths_ft, side='away')
    lam_total = max(0.0, lam_h + lam_a)
    p_micro = _poisson_p_ge2(lam_total)

    # fuzija prior ⊕ micro po “coverage”
    coverage = (feats.get("cov_sot",1.0) + feats.get("cov_da",1.0) + feats.get("cov_pos",1.0))/3.0
    p_final, w_micro = combine_prior_with_micro(p_prior, p_micro, coverage)

    # opcioni market blend
    p_blend = blend_with_market(p_final, market_odds_over15_ft, alpha=ALPHA_MODEL)

    p_out = _calibrate(p_blend, temp=CALIBRATION_FT["TEMP"], floor=CALIBRATION_FT["FLOOR"], ceil=CALIBRATION_FT["CEIL"])

    # Dodaj team statistics, H2H statistics, i form statistics
    # team1_percent, team2_percent, h2h_percent su već izračunati gore
    
    # Form statistics (mikro signali)
    home_form = (micro_db_ft.get(home_id) or {}).get("home") or {}
    away_form = (micro_db_ft.get(away_id) or {}).get("away") or {}
    
    def _pct_or_none(x, cap):
        try:
            if x is None or cap in (None, 0):
                return None
            return round(min(100.0, max(0.0, (float(x) / float(cap)) * 100.0)), 2)
        except Exception:
            return None
    
    SOT1H_CAP_LOC = float(globals().get("SOT1H_CAP", 6.0))
    DA1H_CAP_LOC = float(globals().get("DA1H_CAP", 65.0))
    
    home_shots_pct = _pct_or_none(home_form.get("sot1h_for"), SOT1H_CAP_LOC)
    away_shots_pct = _pct_or_none(away_form.get("sot1h_for"), SOT1H_CAP_LOC)
    home_attacks_pct = _pct_or_none(home_form.get("da1h_for"), DA1H_CAP_LOC)
    away_attacks_pct = _pct_or_none(away_form.get("da1h_for"), DA1H_CAP_LOC)
    
    form_vals = []
    if home_shots_pct is not None and home_attacks_pct is not None:
        form_vals.append((home_shots_pct + home_attacks_pct) / 2.0)
    if away_shots_pct is not None and away_attacks_pct is not None:
        form_vals.append((away_shots_pct + away_attacks_pct) / 2.0)
    form_percent = round(sum(form_vals)/len(form_vals), 2) if form_vals else 0.0

    # DEBUG: Loguj mikro signale
    print(f"🔍 [DEBUG] calculate_final_probability_ft_over15 - feats keys: {list(feats.keys()) if feats else 'None'}", flush=True)
    print(f"🔍 [DEBUG] calculate_final_probability_ft_over15 - exp_sotFT_home: {feats.get('exp_sotFT_home')}", flush=True)
    print(f"🔍 [DEBUG] calculate_final_probability_ft_over15 - exp_sotFT_away: {feats.get('exp_sotFT_away')}", flush=True)
    print(f"🔍 [DEBUG] calculate_final_probability_ft_over15 - exp_daFT_home: {feats.get('exp_daFT_home')}", flush=True)
    print(f"🔍 [DEBUG] calculate_final_probability_ft_over15 - exp_daFT_away: {feats.get('exp_daFT_away')}", flush=True)
    print(f"🔍 [DEBUG] calculate_final_probability_ft_over15 - extras: {extras}", flush=True)

    debug = {
        "p_prior": round(p_prior,3), "p_micro": round(p_micro,3), "w_micro": round(w_micro,2),
        "lam_home": dbg_h, "lam_away": dbg_a,
        "exp_sot_home": feats.get("exp_sotFT_home"), "exp_sot_away": feats.get("exp_sotFT_away"),
        "exp_da_home": feats.get("exp_daFT_home"),   "exp_da_away": feats.get("exp_daFT_away"),
        "setp_xg_total": round(float(feats.get("setp_xg_total") or 0.0),3),
        "coverage": round(float(coverage),2),
        "market_blend": bool(market_odds_over15_ft),
        "kickoff": (fixture.get("fixture") or {}).get("date"),
        "league": (fixture.get("league") or {}).get("name"),
        "team1": (fixture.get("teams") or {}).get("home", {}).get("name"),
        "team2": (fixture.get("teams") or {}).get("away", {}).get("name"),
        
        # Team statistics
        "team1_percent": team1_percent,
        "team2_percent": team2_percent,
        "team1_hits": h_home,
        "team1_total": w_home,
        "team2_hits": h_away,
        "team2_total": w_away,
        
        # H2H statistics
        "h2h_percent": h2h_percent,
        "h2h_hits": h_h2h,
        "h2h_total": w_h2h,
        
        # Form statistics
        "home_shots_percent": home_shots_pct,
        "home_attacks_percent": home_attacks_pct,
        "home_shots_used": home_form.get('used_sot', 0),
        "home_attacks_used": home_form.get('used_da', 0),
        "away_shots_percent": away_shots_pct,
        "away_attacks_percent": away_attacks_pct,
        "away_shots_used": away_form.get('used_sot', 0),
        "away_attacks_used": away_form.get('used_da', 0),
        "form_percent": form_percent,
        
        # Micro signals - all parameters
        "exp_sot_total": (feats.get("exp_sotFT_home") or 0) + (feats.get("exp_sotFT_away") or 0),
        "exp_da_total": (feats.get("exp_daFT_home") or 0) + (feats.get("exp_daFT_away") or 0),
        "pos_edge": feats.get("pos_edge", 0),
        "effN_prior": (w_home or 0.0) + (w_away or 0.0) + effn_h2h,
        "effN_micro": feats.get("effN_micro", 0),
        "liga_baseline": m2p,
        
        # FORM_ADJ i COACH_ADJ
        "form_adj": form_adj,
        "coach_adj": coach_adj,
        
        # KRITIČNI FEATURE-I
        "pace_da_total": feats.get("pace_da_total"),
        "lineups_have": feats.get("lineups_have"),
        "lineups_fw_count": feats.get("lineups_fw_count"),
        "inj_count": feats.get("inj_count"),
        
        # Referee, weather, venue, lineups, injuries
        "ref_adj": extras.get("ref_adj", 0) if extras else 0,
        "weather_adj": extras.get("weather_adj", 0) if extras else 0,
        "venue_adj": extras.get("venue_adj", 0) if extras else 0,
        "lineup_adj": extras.get("lineup_adj", 0) if extras else 0,
        "injuries_adj": extras.get("injuries_adj", 0) if extras else 0,
        
        # All micro features
        "z_sot_home": feats.get("z_sot_home", 0),
        "z_sot_away": feats.get("z_sot_away", 0),
        "z_da_home": feats.get("z_da_home", 0),
        "z_da_away": feats.get("z_da_away", 0),
        "z_shots_home": feats.get("z_shots_home", 0),
        "z_shots_away": feats.get("z_shots_away", 0),
        "z_xg_home": feats.get("z_xg_home", 0),
        "z_xg_away": feats.get("z_xg_away", 0),
        "z_bigch_home": feats.get("z_bigch_home", 0),
        "z_bigch_away": feats.get("z_bigch_away", 0),
        "z_corn_home": feats.get("z_corn_home", 0),
        "z_corn_away": feats.get("z_corn_away", 0),
        "z_fk_home": feats.get("z_fk_home", 0),
        "z_fk_away": feats.get("z_fk_away", 0),
        "z_offs_home": feats.get("z_offs_home", 0),
        "z_offs_away": feats.get("z_offs_away", 0),
        "z_cross_home": feats.get("z_cross_home", 0),
        "z_cross_away": feats.get("z_cross_away", 0),
        "z_counter_home": feats.get("z_counter_home", 0),
        "z_counter_away": feats.get("z_counter_away", 0),
        "z_saves_home": feats.get("z_saves_home", 0),
        "z_saves_away": feats.get("z_saves_away", 0),
        "z_sib_home": feats.get("z_sib_home", 0),
        "z_sib_away": feats.get("z_sib_away", 0),
        "z_sob_home": feats.get("z_sob_home", 0),
        "z_sob_away": feats.get("z_sob_away", 0),
        "z_wood_home": feats.get("z_wood_home", 0),
        "z_wood_away": feats.get("z_wood_away", 0),
        
        # Adjustments
        "fin_adj": feats.get("fin_adj", 0),
        "leak_adj": feats.get("leak_adj", 0),
        "gk_adj": feats.get("gk_adj", 0),
        "rest_adj": feats.get("rest_adj", 0),
        "congest_adj": feats.get("congest_adj", 0),
        "att_adj": feats.get("att_adj", 0),
        "def_adj": feats.get("def_adj", 0),
        "tier_gap": feats.get("tier_gap", 0),
    }
    return p_out, debug

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
    cur.execute("SELECT 1 FROM fixtures WHERE `date` >= %s AND `date` <= %s LIMIT 1", (s, e))
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


API_KEY = os.getenv('APIFOOTBALL_KEY')
if not API_KEY:
    raise RuntimeError("Set APIFOOTBALL_KEY in environment")

BASE_URL = 'https://v3.football.api-sports.io'
HEADERS = {'x-apisports-key': API_KEY}

ANALYZE_LOCK = threading.Lock()
PREPARE_LOCK = threading.Lock()

from db_backend import (
    get_connection as get_db_connection,
    insert_team_matches,
    insert_h2h_matches,
    DB_WRITE_LOCK,
    try_read_fixture_statistics,
    create_all_tables,
)
from mysql_database import (
    create_user,
    authenticate_user,
    create_session,
    get_session,
    delete_session,
    cleanup_expired_sessions,
)

# ====== AUTHENTICATION MODELS ======
class LoginRequest(BaseModel):
    email: str
    password: str
    remember_me: bool = False

class RegisterRequest(BaseModel):
    email: str
    password: str
    first_name: str
    last_name: str

class AuthResponse(BaseModel):
    success: bool
    message: str
    user: dict = None
    session_id: str = None


# ====== AUTHENTICATION ENDPOINTS ======

@app.post("/api/auth/register", response_model=AuthResponse)
async def register_user(request: RegisterRequest):
    """Register a new user."""
    try:
        # Validate email format
        import re
        if not re.match(r'^[^\s@]+@[^\s@]+\.[^\s@]+$', request.email):
            return AuthResponse(success=False, message="Invalid email format")
        
        # Validate password strength
        if len(request.password) < 8:
            return AuthResponse(success=False, message="Password must be at least 8 characters long")
        
        # Create user
        result = create_user(
            email=request.email,
            password=request.password,
            first_name=request.first_name,
            last_name=request.last_name
        )
        
        if result["success"]:
            return AuthResponse(
                success=True,
                message="User created successfully",
                user=result["user"]
            )
        else:
            return AuthResponse(success=False, message=result["error"])
    
    except Exception as e:
        print(f"❌ [ERROR] Registration failed: {e}")
        return AuthResponse(success=False, message="Registration failed")

@app.post("/api/auth/login", response_model=AuthResponse)
async def login_user(request: LoginRequest):
    """Login user and create session."""
    try:
        # Authenticate user
        result = authenticate_user(request.email, request.password)
        
        if not result["success"]:
            return AuthResponse(success=False, message=result["error"])
        
        user = result["user"]
        
        # Create session
        session_id = secrets.token_urlsafe(32)
        expires_hours = 24 * 30 if request.remember_me else 24  # 30 days or 1 day
        expires_at = (datetime.now() + timedelta(hours=expires_hours)).isoformat()
        
        session_created = create_session(
            user_id=user["id"],
            session_id=session_id,
            expires_at=expires_at
        )
        
        if session_created:
            return AuthResponse(
                success=True,
                message="Login successful",
                user=user,
                session_id=session_id
            )
        else:
            return AuthResponse(success=False, message="Failed to create session")
    
    except Exception as e:
        print(f"❌ [ERROR] Login failed: {e}")
        return AuthResponse(success=False, message="Login failed")

@app.post("/api/auth/logout")
async def logout_user(session_id: str = None):
    """Logout user and delete session."""
    try:
        if session_id:
            deleted = delete_session(session_id)
            if deleted:
                return {"success": True, "message": "Logged out successfully"}
            else:
                return {"success": False, "message": "Session not found"}
        else:
            return {"success": False, "message": "Session ID required"}
    
    except Exception as e:
        print(f"❌ [ERROR] Logout failed: {e}")
        return {"success": False, "message": "Logout failed"}

@app.get("/api/auth/me")
async def get_current_user(session_id: str = None):
    """Get current user from session."""
    try:
        if not session_id:
            return {"success": False, "message": "Session ID required"}
        
        session = get_session(session_id)
        if session:
            return {
                "success": True,
                "user": {
                    "id": session["user_id"],
                    "email": session["email"],
                    "first_name": session["first_name"],
                    "last_name": session["last_name"],
                    "is_admin": session["is_admin"]
                }
            }
        else:
            return {"success": False, "message": "Invalid or expired session"}
    
    except Exception as e:
        print(f"❌ [ERROR] Get user failed: {e}")
        return {"success": False, "message": "Failed to get user"}

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
        # dodatno: izračunaj/persist FT Over 1.5 za današnje mečeve
        sdt, edt = _day_bounds_utc(d)
        try:
            ft_rows = compute_ft_over15_for_range(sdt, edt, no_api=False)
            persist_ft_over15(ft_rows)
        except Exception as e:
            print(f"FT Over 1.5 compute error (ensure_today): {e}")
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
    # raw fixtures za konkretan datum (UTC), sa league filtering-om
    resp = rate_limited_request(f"{BASE_URL}/fixtures",
                                params={"date": d.isoformat(), "timezone": "UTC"})
    raw_fixtures = (resp or {}).get("response") or []
    
    # NOVO: Filtriraj po liga whitelist-u odmah nakon API poziva
    valid_fixtures = [f for f in raw_fixtures if is_valid_competition(f)]
    
    if len(valid_fixtures) < len(raw_fixtures):
        print(f"ℹ️ API League filtering za {d}: zadržano {len(valid_fixtures)}/{len(raw_fixtures)} fixtures")
    
    return valid_fixtures

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

    # 4) izračunaj FT Over 1.5 za dan i upiši
    sdt, edt = _day_bounds_utc(d)
    try:
        ft_rows = compute_ft_over15_for_range(sdt, edt, no_api=False)
        persist_ft_over15(ft_rows)
    except Exception as e:
        print(f"FT Over 1.5 compute error (seed): {e}")


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
        cur.execute("DELETE FROM fixtures WHERE `date` >= %s AND `date` <= %s", (s, e))
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

app.mount("/static", StaticFiles(directory=str(FRONTEND_DIR), check_dir=False), name="static")

@app.get("/")
def serve_home():
    print("🔍 [DEBUG] / route called - serving home page")
    index_path = FRONTEND_DIR / "index.html"
    if not index_path.exists():
        return {"error": f"index.html not found at {index_path}"}
    print("✅ [DEBUG] Serving index.html")
    return FileResponse(str(index_path))

@app.get("/login")
def serve_login():
    login_path = FRONTEND_DIR / "login.html"
    if not login_path.exists():
        return {"error": f"login.html not found at {login_path}"}
    return FileResponse(str(login_path))

@app.get("/register")
def serve_register():
    register_path = FRONTEND_DIR / "register.html"
    if not register_path.exists():
        return {"error": f"register.html not found at {register_path}"}
    return FileResponse(str(register_path))

@app.get("/users")
def serve_users():
    print("🔍 [DEBUG] /users route called")
    users_path = FRONTEND_DIR / "users.html"
    print(f"🔍 [DEBUG] Looking for users.html at: {users_path}")
    if not users_path.exists():
        print(f"❌ [DEBUG] users.html not found at {users_path}")
        return {"error": f"users.html not found at {users_path}"}
    print("✅ [DEBUG] Serving users.html")
    return FileResponse(str(users_path))



ALLOWED_ORIGINS = [o.strip() for o in os.getenv("ALLOWED_ORIGINS", "").split(",") if o.strip()]
# CORS – da frontend može da pristupi backendu
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS or ["*"],
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

# --- STRICT WHITELIST (samo ono što mi damo) ---
STRICT_LEAGUE_FILTER = True  # ako je True, prolaze SAMO takmičenja iz whitelist fajla

LEAGUE_NAME_WHITELIST: set[tuple[str, str]] = set()  # (country_norm, league_norm)
LEAGUE_ID_WHITELIST: set[int] = set()                # API-Football league_id

WHITELIST_FILE = str((BASE_DIR / "league_whitelist.json").resolve())

def _comp_key(country: str | None, name: str | None) -> tuple[str, str]:
    return _norm(country or ""), _norm(name or "")

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

def _load_strict_whitelist_from_file(path: str) -> dict:
    """
    Učita whitelist iz JSON fajla.
    Podržani formati (list):
      1) {"league_id": 39, "country_name":"England", "league_name":"Premier League"}
      2) {"country":"England", "league":"Premier League"}
      3) "England|Premier League"    ili    "England - Premier League"
    """
    global LEAGUE_NAME_WHITELIST, LEAGUE_ID_WHITELIST
    LEAGUE_NAME_WHITELIST = set()
    LEAGUE_ID_WHITELIST = set()

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print(f"⚠️ Whitelist fajl nije učitan ({path}): {e}")
        return {"ids": 0, "names": 0, "path": path, "ok": False}

    if not isinstance(data, list):
        print(f"⚠️ Whitelist fajl mora biti lista (dobijeno: {type(data)})")
        return {"ids": 0, "names": 0, "path": path, "ok": False}

    ids, names = 0, 0
    for item in data:
        # varijanta 1: dict sa league_id
        if isinstance(item, dict) and "league_id" in item:
            try:
                LEAGUE_ID_WHITELIST.add(int(item["league_id"]))
                ids += 1
            except:
                pass
            cn = item.get("country_name") or item.get("country")
            ln = item.get("league_name")  or item.get("league")
            if cn and ln:
                LEAGUE_NAME_WHITELIST.add(_comp_key(cn, ln))
                names += 1
            continue

        # varijanta 2: dict bez league_id (country/league)
        if isinstance(item, dict) and ("country" in item and "league" in item):
            LEAGUE_NAME_WHITELIST.add(_comp_key(item["country"], item["league"]))
            names += 1
            continue

        # varijanta 3: string "Country|League" ili "Country - League"
        if isinstance(item, str):
            s = item.strip()
            if "|" in s:
                cn, ln = [x.strip() for x in s.split("|", 1)]
                LEAGUE_NAME_WHITELIST.add(_comp_key(cn, ln)); names += 1
            elif " - " in s:
                cn, ln = [x.strip() for x in s.split(" - ", 1)]
                LEAGUE_NAME_WHITELIST.add(_comp_key(cn, ln)); names += 1
            else:
                # fallback: ako je samo ime lige, bez države → preskoči
                pass
            continue

    print(f"✅ WHITELIST učitan: ids={len(LEAGUE_ID_WHITELIST)}, names={len(LEAGUE_NAME_WHITELIST)} iz {path}")
    return {"ids": len(LEAGUE_ID_WHITELIST), "names": len(LEAGUE_NAME_WHITELIST), "path": path, "ok": True}

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
    """
    STROGO: propuštamo samo ako je takmičenje na whitelist-i (po ID ili po (country, league) imenu).
    Sve ostalo odbijamo. Nema drugih filtera.
    """
    if not STRICT_LEAGUE_FILTER:
        return True, None  # ako ikad poželiš da isključiš strict

    league  = (fixture.get("league") or {})
    lid     = league.get("id")
    country_raw = (league.get("country") or "")
    lname_raw   = league.get("name") or ""

    # ID?
    if lid in LEAGUE_ID_WHITELIST:
        return True, None

    # (country, league) po imenu?
    key_raw = _comp_key(country_raw, lname_raw)
    if key_raw in LEAGUE_NAME_WHITELIST:
        return True, None

    # ništa drugo ne prolazi
    return False, "not_in_strict_whitelist"

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
            fin_xg = (raw * eff_n + 0.0 * 6.0) / max(1e-6, eff_n + 6.0)  # shrink ka 0 (simetrično)

        # GK shot-stopping proxy: save% = 1 - goals_allowed/SoT_allowed
        gk_stop = GK_SAVE_PRIOR_1H
        if sot_alw_w > 0:
            save_rate = 1.0 - _safe_div(g_alw_w, sot_alw_w, 0.0)
            saves_est = save_rate * sot_alw_w
            gk_stop = beta_shrunk_rate(saves_est, sot_alw_w, m=GK_SAVE_PRIOR_1H, tau=GK_SAVE_TAU)
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

    data = api_or_repo_get_lineups(fixture_id) or []
    out = {}
    
    # ISPRAVKA: data je lista, ne dict - treba da je konvertujemo u dict format
    if isinstance(data, list):
        # Konvertuj listu u dict format
        data_dict = {}
        for item in data:
            if isinstance(item, dict):
                team_id = item.get("team", {}).get("id")
                if team_id:
                    # Proveri da li je home ili away tim
                    fixture_data = item.get("fixture", {})
                    if fixture_data.get("home", {}).get("id") == team_id:
                        data_dict["home"] = item
                    elif fixture_data.get("away", {}).get("id") == team_id:
                        data_dict["away"] = item
        data = data_dict
    
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

    # OVO ubaci ispod OVOG DELA KODA (gde već čitaš shots/xg/bigch/corners/fk iz mh/ma):

    offs_h  = mh.get("offs1h_for");   offs_a  = ma.get("offs1h_for")
    cross_h = mh.get("cross1h_for");  cross_a = ma.get("cross1h_for")
    cnt_h   = mh.get("counter1h_for");cnt_a   = ma.get("counter1h_for")
    saves_h = mh.get("saves1h_for");  saves_a = ma.get("saves1h_for")
    sib_h   = mh.get("sib1h_for");    sib_a   = ma.get("sib1h_for")
    sob_h   = mh.get("sob1h_for");    sob_a   = ma.get("sob1h_for")
    wood_h  = mh.get("wood1h_for");   wood_a  = ma.get("wood1h_for")

    # zabeleži u out (ispod postojeceg out = {...}):
    # (potraži deo gde konstruišeš 'out' dict i DODAJ sledeće ključeve u taj dict)


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
    ref_prof = compute_referee_profile_1h(
        fixture,
        repo_get_ref_history=lambda name, limit=30: repo.get_referee_fixtures(
            name,
            season=((fixture.get('league') or {}).get('season')),
            last_n=limit,
            no_api=False
        )
    )
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

    # coverage faktori iz mikro uzorka (0..1) – koliko podataka imamo
    def _cov(sum_used, scale=8.0):
        try:
            return max(0.0, min(1.0, float(sum_used) / float(scale)))
        except Exception:
            return 1.0

    cov_sot = _cov((mh.get("used_sot", 0) or 0) + (ma.get("used_sot", 0) or 0))
    cov_da  = _cov((mh.get("used_da",  0) or 0) + (ma.get("used_da",  0) or 0))
    cov_pos = _cov((mh.get("used_pos", 0) or 0) + (ma.get("used_pos", 0) or 0))

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

        # coverage / reliability
        "cov_sot": cov_sot,
        "cov_da":  cov_da,
        "cov_pos": cov_pos,
        "fin_rel_home":  float(fin_rel_h),
        "fin_rel_away":  float(fin_rel_a),
        "leak_rel_home": float(leak_rel_h),
        "leak_rel_away": float(leak_rel_a),


        # OVO ubaci u 'out' dict:
        "offs1h_home": offs_h,    "offs1h_away": offs_a,
        "cross1h_home": cross_h,  "cross1h_away": cross_a,
        "counter1h_home": cnt_h,  "counter1h_away": cnt_a,
        "saves1h_home": saves_h,  "saves1h_away": saves_a,
        "sib1h_home": sib_h,      "sib1h_away": sib_a,
        "sob1h_home": sob_h,      "sob1h_away": sob_a,
        "wood1h_home": wood_h,    "wood1h_away": wood_a,

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

    # OVO ubaci ispod OVOG DELA KODA (gde su mu_shots_h, mu_xg_h, mu_big_h):

    mu_off_h,  sd_off_h  = 1.2, 0.9
    mu_cross_h,sd_cross_h= 6.0, 3.0
    mu_cnt_h,  sd_cnt_h  = 3.0, 2.0
    mu_saves_h,sd_saves_h= 2.0, 1.5
    mu_sib_h,  sd_sib_h  = 2.2, 1.6
    mu_sob_h,  sd_sob_h  = 2.8, 1.8
    mu_wood_h, sd_wood_h = 0.2, 0.5

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

    # NOVO: mikro signali po strani (home/away)
    offs   = feats.get('offs1h_home')    if side=='home' else feats.get('offs1h_away')
    cross  = feats.get('cross1h_home')   if side=='home' else feats.get('cross1h_away')
    cnt    = feats.get('counter1h_home') if side=='home' else feats.get('counter1h_away')
    saves  = feats.get('saves1h_home')   if side=='home' else feats.get('saves1h_away')
    sib    = feats.get('sib1h_home')     if side=='home' else feats.get('sib1h_away')
    sob    = feats.get('sob1h_home')     if side=='home' else feats.get('sob1h_away')
    wood   = feats.get('wood1h_home')    if side=='home' else feats.get('wood1h_away')

    # z-score klasici
    z_sot = _z(exp_sot, mu_sot_h, sd_sot_h)
    z_da  = _z(exp_da,  mu_da_h,  sd_da_h)

    # novi z-score (fallback baseline)
    z_shots = 0.0 if shots is None else _z(shots, mu_shots_h, sd_shots_h)
    z_xg    = 0.0 if xg    is None else _z(xg,    mu_xg_h,    sd_xg_h)
    z_big   = 0.0 if big   is None else _z(big,   mu_big_h,   sd_big_h)

    # OVO ubaci ispod OVOG DELA KODA (posle z_sot, z_da, z_shots, z_xg, z_big...):
    z_offs  = _z(offs,  mu_off_h,  sd_off_h)
    z_cross = _z(cross, mu_cross_h,sd_cross_h)
    z_cnt   = _z(cnt,   mu_cnt_h,  sd_cnt_h)
    z_saves = _z(saves, mu_saves_h,sd_saves_h)
    z_sib   = _z(sib,   mu_sib_h,  sd_sib_h)
    z_sob   = _z(sob,   mu_sob_h,  sd_sob_h)
    z_wood  = _z(wood,  mu_wood_h, sd_wood_h)

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

    # OVO ubaci ispod OVOG DELA KODA (gde dodeljuješ w_shots, w_xg, w_big...):
    w_offs  = WEIGHTS["Z_OFFSIDES"]
    w_cross = WEIGHTS["Z_CROSSES"]
    w_cnt   = WEIGHTS["Z_COUNTERS"]
    w_saves = WEIGHTS["Z_SAVES"]
    w_sib   = WEIGHTS["Z_SIB"]
    w_sob   = WEIGHTS["Z_SOB"]
    w_wood  = WEIGHTS["Z_WOODWORK"]

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
        # OVO ubaci u formulu za z (gde već sabiraš z-score signale):
        + w_offs*z_offs + w_cross*z_cross + w_cnt*z_cnt
        + w_saves*(-z_saves)        # više saves → teže do gola (negativno)
        + w_sib*z_sib + w_sob*z_sob # inside box jači od outside box
        + w_wood*z_wood             # near-miss volatilnost
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
        "z_offs": round(z_offs,3), "z_cross": round(z_cross,3),
        "z_cnt": round(z_cnt,3), "z_saves": round(z_saves,3),
        "z_sib": round(z_sib,3), "z_sob": round(z_sob,3),
        "z_wood": round(z_wood,3),
    }
    return p, dbg

def get_or_fetch_team_history(team_id: int, last_n: int = 30, force_refresh: bool = False, no_api: bool = False):
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)

    now = datetime.utcnow()
    if not force_refresh:
        cur.execute("SELECT data, updated_at FROM team_history_cache WHERE team_id=%s AND last_n=%s", (team_id, last_n))
        row = cur.fetchone()
        if row:
            updated_at = row["updated_at"]
            if isinstance(updated_at, str):
                try: updated_at = datetime.fromisoformat(updated_at)
                except: updated_at = now - timedelta(hours=CACHE_TTL_HOURS+1)
            if (now - updated_at) <= timedelta(hours=CACHE_TTL_HOURS) or no_api:
                conn.close()
                raw_data = json.loads(row["data"])[:last_n]
                # ISPRAVKA: Osiguraj da su svi elementi dict-ovi
                safe_data = []
                for item in raw_data:
                    if isinstance(item, dict):
                        safe_data.append(item)
                    elif isinstance(item, (list, tuple)):
                        converted = _coerce_fixture_row_to_api_dict(item)
                        if converted:
                            safe_data.append(converted)
                return safe_data

        cur.execute("""
            SELECT data, updated_at, last_n FROM team_history_cache
            WHERE team_id=%s ORDER BY last_n DESC LIMIT 1
        """, (team_id,))
        row2 = cur.fetchone()
        if row2:
            updated_at2 = row2["updated_at"]
            if isinstance(updated_at2, str):
                try: updated_at2 = datetime.fromisoformat(updated_at2)
                except: updated_at2 = now - timedelta(hours=CACHE_TTL_HOURS+1)
            have_n = row2.get("last_n") or 0
            if have_n >= last_n and ((now - updated_at2) <= timedelta(hours=CACHE_TTL_HOURS) or no_api):
                conn.close()
                raw_data = json.loads(row2["data"])[:last_n]
                # ISPRAVKA: Osiguraj da su svi elementi dict-ovi
                safe_data = []
                for item in raw_data:
                    if isinstance(item, dict):
                        safe_data.append(item)
                    elif isinstance(item, (list, tuple)):
                        converted = _coerce_fixture_row_to_api_dict(item)
                        if converted:
                            safe_data.append(converted)
                return safe_data

    if no_api:
        conn.close()
        return []

    resp = rate_limited_request(f"{BASE_URL}/fixtures",
                                params={'team': team_id, 'last': last_n, 'timezone': 'UTC'})
    data = resp.get('response', []) if resp else []

    with DB_WRITE_LOCK:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO team_history_cache(team_id,last_n,data,updated_at)
            VALUES(%s,%s,%s,NOW())
            ON DUPLICATE KEY UPDATE data=VALUES(data), updated_at=NOW()
        """, (team_id, last_n, json.dumps(data, ensure_ascii=False)))
        conn.commit()

    conn.close()
    return data


def get_or_fetch_h2h(team_a: int, team_b: int, last_n: int = 10, no_api: bool = False):
    a, b = sorted([team_a, team_b])
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)

    now = datetime.utcnow()
    cur.execute("SELECT data, updated_at FROM h2h_cache WHERE team1_id=%s AND team2_id=%s AND last_n=%s", (a,b,last_n))
    row = cur.fetchone()
    if row:
        updated_at = row["updated_at"]
        if isinstance(updated_at, str):
            try: updated_at = datetime.fromisoformat(updated_at)
            except: updated_at = now - timedelta(hours=CACHE_TTL_HOURS+1)
        if (now - updated_at) <= timedelta(hours=CACHE_TTL_HOURS) or no_api:
            conn.close()
            return json.loads(row["data"])

    if no_api:
        conn.close()
        return []

    h2h_key = f"{a}-{b}"
    resp = rate_limited_request(f"{BASE_URL}/fixtures/headtohead", params={'h2h': h2h_key, 'last': last_n})
    data = resp.get('response', []) if resp else []

    with DB_WRITE_LOCK:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO h2h_cache(team1_id,team2_id,last_n,data,updated_at)
            VALUES(%s,%s,%s,%s,NOW())
            ON DUPLICATE KEY UPDATE data=VALUES(data), updated_at=NOW()
        """, (a,b,last_n,json.dumps(data, ensure_ascii=False)))
        conn.commit()

    conn.close()
    return data

def get_or_fetch_fixture_statistics(fixture_id: int):
    existing = try_read_fixture_statistics(fixture_id)
    if existing is not None:
        return existing

    response = rate_limited_request(f"{BASE_URL}/fixtures/statistics", params={"fixture": fixture_id})
    stats = (response or {}).get('response') or None

    with DB_WRITE_LOCK:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO match_statistics (fixture_id, data, updated_at)
            VALUES (%s, %s, NOW())
            ON DUPLICATE KEY UPDATE
                data=VALUES(data),
                updated_at=NOW()
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

# OVO zameni SA OVIM (cela funkcija):
def _extract_match_micro_for_team(stats_response, team_id, opp_id):
    """Iz jednog meča izvuci 1H mikro metrike za tim i 'allowed' preko protivnika.
       Dodato: total shots, xG, big chances, korneri, free kicks,
       NOVO: offsides, crosses, counter attacks, saves, shots inside/outside box, woodwork."""
    tb = _team_block(stats_response, team_id)
    ob = _team_block(stats_response, opp_id)
    if not tb or not ob:
        return None

    def one_half_or_half(block, full_list, half_list, cap=None):
        v_1h = _stat_from_block(block, [n.lower() for n in half_list])
        if v_1h is not None:
            val = v_1h
        else:
            v_full = _stat_from_block(block, [n.lower() for n in full_list])
            if v_full is None:
                return None
            val = v_full / 2.0
        if cap is not None and val is not None:
            val = max(0.0, min(float(cap), float(val)))
        return val

    # -- već postojeći signali --
    sot1h_for     = one_half_or_half(tb, ["shots on goal", "shots on target"],
                                        ["1st half shots on target", "shots on target 1st half", "first half shots on target"],
                                        cap=SOT1H_CAP)
    sot1h_allowed = one_half_or_half(ob, ["shots on goal", "shots on target"],
                                        ["1st half shots on target", "shots on target 1st half", "first half shots on target"],
                                        cap=SOT1H_CAP)

    shots1h_for   = one_half_or_half(tb, ["total shots", "shots total", "shots"],
                                        ["1st half total shots", "shots 1st half", "first half total shots"],
                                        cap=SHOTS1H_CAP)
    shots1h_allowed = one_half_or_half(ob, ["total shots", "shots total", "shots"],
                                        ["1st half total shots", "shots 1st half", "first half total shots"],
                                        cap=SHOTS1H_CAP)

    da1h_for      = one_half_or_half(tb, ["dangerous attacks"],
                                        ["1st half dangerous attacks", "dangerous attacks 1st half", "first half dangerous attacks"],
                                        cap=DA1H_CAP)
    da1h_allowed  = one_half_or_half(ob, ["dangerous attacks"],
                                        ["1st half dangerous attacks", "dangerous attacks 1st half", "first half dangerous attacks"],
                                        cap=DA1H_CAP)

    pos1h = _stat_from_block(tb, ["1st half possession", "possession 1st half", "first half possession",
                                  "1st half ball possession", "ball possession 1st half"])
    if pos1h is None:
        pos_full = _stat_from_block(tb, ["ball possession", "possession", "possession %", "ball possession %"])
        pos1h = pos_full
    if pos1h is not None:
        pos1h = max(POS_MIN, min(POS_MAX, float(pos1h)))

    xg1h_for = one_half_or_half(tb, ["expected goals", "xg", "x-goals"],
                                   ["1st half expected goals", "xg 1st half", "first half xg"],
                                   cap=XG1H_CAP)
    xg1h_allowed = one_half_or_half(ob, ["expected goals", "xg", "x-goals"],
                                       ["1st half expected goals", "xg 1st half", "first half xg"],
                                       cap=XG1H_CAP)

    bigch1h_for = one_half_or_half(tb, ["big chances", "big chances created"],
                                      ["1st half big chances", "big chances 1st half", "first half big chances"],
                                      cap=BIGCH1H_CAP)
    bigch1h_allowed = one_half_or_half(ob, ["big chances", "big chances created"],
                                          ["1st half big chances", "big chances 1st half", "first half big chances"],
                                          cap=BIGCH1H_CAP)

    corn1h_for = one_half_or_half(tb, ["corner kicks", "corners"],
                                     ["1st half corners", "corners 1st half", "first half corners"],
                                     cap=CORN1H_CAP)
    corn1h_allowed = one_half_or_half(ob, ["corner kicks", "corners"],
                                         ["1st half corners", "corners 1st half", "first half corners"],
                                         cap=CORN1H_CAP)

    fk1h_for = one_half_or_half(tb, ["free kicks", "free-kicks"],
                                   ["1st half free kicks", "free kicks 1st half", "first half free kicks"],
                                   cap=FK1H_CAP)
    fk1h_allowed = one_half_or_half(ob, ["free kicks", "free-kicks"],
                                       ["1st half free kicks", "free kicks 1st half", "first half free kicks"],
                                       cap=FK1H_CAP)

    # -- NOVO: offsides, crosses, counters, saves, inside/outside box, woodwork --
    offs1h_for = one_half_or_half(tb, ["offsides"], ["offsides 1st half", "1st half offsides"], cap=OFFSIDES1H_CAP)
    offs1h_allowed = one_half_or_half(ob, ["offsides"], ["offsides 1st half", "1st half offsides"], cap=OFFSIDES1H_CAP)

    cross1h_for = one_half_or_half(tb, ["crosses", "total crosses"], ["crosses 1st half", "1st half crosses"], cap=CROSSES1H_CAP)
    cross1h_allowed = one_half_or_half(ob, ["crosses", "total crosses"], ["crosses 1st half", "1st half crosses"], cap=CROSSES1H_CAP)

    counter1h_for = one_half_or_half(tb, ["counter attacks", "counter-attacks"], ["counter attacks 1st half", "1st half counter attacks"], cap=COUNTER1H_CAP)
    counter1h_allowed = one_half_or_half(ob, ["counter attacks", "counter-attacks"], ["counter attacks 1st half", "1st half counter attacks"], cap=COUNTER1H_CAP)

    saves1h_for = one_half_or_half(tb, ["goalkeeper saves", "saves"], ["goalkeeper saves 1st half", "saves 1st half"], cap=SAVES1H_CAP)
    saves1h_allowed = one_half_or_half(ob, ["goalkeeper saves", "saves"], ["goalkeeper saves 1st half", "saves 1st half"], cap=SAVES1H_CAP)

    sib1h_for = one_half_or_half(tb, ["shots insidebox", "shots inside box"], ["shots inside box 1st half"], cap=SIB1H_CAP)
    sib1h_allowed = one_half_or_half(ob, ["shots insidebox", "shots inside box"], ["shots inside box 1st half"], cap=SIB1H_CAP)

    sob1h_for = one_half_or_half(tb, ["shots outsidebox", "shots outside box"], ["shots outside box 1st half"], cap=SOB1H_CAP)
    sob1h_allowed = one_half_or_half(ob, ["shots outsidebox", "shots outside box"], ["shots outside box 1st half"], cap=SOB1H_CAP)

    wood1h_for = one_half_or_half(tb, ["hit woodwork", "woodwork"], ["hit woodwork 1st half", "woodwork 1st half"], cap=WOODWORK1H_CAP)
    wood1h_allowed = one_half_or_half(ob, ["hit woodwork", "woodwork"], ["hit woodwork 1st half", "woodwork 1st half"], cap=WOODWORK1H_CAP)

    return {
        "sot1h_for": sot1h_for,          "sot1h_allowed": sot1h_allowed,
        "shots1h_for": shots1h_for,      "shots1h_allowed": shots1h_allowed,
        "da1h_for": da1h_for,            "da1h_allowed": da1h_allowed,
        "pos1h": pos1h,
        "xg1h_for": xg1h_for,            "xg1h_allowed": xg1h_allowed,
        "bigch1h_for": bigch1h_for,      "bigch1h_allowed": bigch1h_allowed,
        "corn1h_for": corn1h_for,        "corn1h_allowed": corn1h_allowed,
        "fk1h_for": fk1h_for,            "fk1h_allowed": fk1h_allowed,

        # NOVO:
        "offs1h_for": offs1h_for,        "offs1h_allowed": offs1h_allowed,
        "cross1h_for": cross1h_for,      "cross1h_allowed": cross1h_allowed,
        "counter1h_for": counter1h_for,  "counter1h_allowed": counter1h_allowed,
        "saves1h_for": saves1h_for,      "saves1h_allowed": saves1h_allowed,
        "sib1h_for": sib1h_for,          "sib1h_allowed": sib1h_allowed,
        "sob1h_for": sob1h_for,          "sob1h_allowed": sob1h_allowed,
        "wood1h_for": wood1h_for,        "wood1h_allowed": wood1h_allowed,
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

def h2h_ft_over15_stats(h2h_matches):
    """H2H FT Over 1.5 statistike"""
    print(f"🔍 [DEBUG] h2h_ft_over15_stats called with {len(h2h_matches or [])} matches")
    if h2h_matches:
        print(f"🔍 [DEBUG] h2h_ft_over15_stats first match type: {type(h2h_matches[0])}")
        print(f"🔍 [DEBUG] h2h_ft_over15_stats first match: {h2h_matches[0]}")
    
    total = 0; hits = 0
    for i, m in enumerate(h2h_matches or []):
        print(f"🔍 [DEBUG] h2h_ft_over15_stats processing match {i+1}: {m}")
        if _ft_total_ge2(m): 
            hits += 1
            print(f"🔍 [DEBUG] h2h_ft_over15_stats match {i+1} has >=2 goals")
        else:
            print(f"🔍 [DEBUG] h2h_ft_over15_stats match {i+1} has <2 goals")
        total += 1
    pct = round((hits/total)*100, 2) if total else 0.0
    print(f"🔍 [DEBUG] h2h_ft_over15_stats result: {pct}% ({hits}/{total})")
    return pct, hits, total

def _ft_total_ge2(m):
    """Proverava da li je FT total >= 2"""
    print(f"🔍 [DEBUG] _ft_total_ge2 called with match: {m}")
    ft = ((m.get('score') or {}).get('fulltime') or {})
    h = ft.get('home') or 0
    a = ft.get('away') or 0
    total = h + a
    result = total >= 2
    print(f"🔍 [DEBUG] _ft_total_ge2 result: {h}+{a}={total} >= 2? {result}")
    return result

def _aggregate_team_micro(team_id, matches, get_stats_fn, context="all"):
    """
    Sabira mikro kroz zadnje mečeve. Dodato: shots total, xG, big chances, korneri, free kicks,
    NOVO: offsides, crosses, counter attacks, saves, shots inside/outside box, woodwork.

    Ispravka: odvojeni brojači za *_for i *_allowed da prosjeci ne budu razvodenjeni.
    """
    sums = {
        "sot_for":0.0,"sot_alw":0.0,"da_for":0.0,"da_alw":0.0,"pos":0.0,
        "shots_for":0.0,"shots_alw":0.0,
        "xg_for":0.0,"xg_alw":0.0,
        "bigch_for":0.0,"bigch_alw":0.0,
        "corn_for":0.0,"corn_alw":0.0,
        "fk_for":0.0,"fk_alw":0.0,
        "offs_for":0.0,"offs_alw":0.0,
        "cross_for":0.0,"cross_alw":0.0,
        "counter_for":0.0,"counter_alw":0.0,
        "saves_for":0.0,"saves_alw":0.0,
        "sib_for":0.0,"sib_alw":0.0,
        "sob_for":0.0,"sob_alw":0.0,
        "wood_for":0.0,"wood_alw":0.0,
    }
    # odvojeni brojači
    cnt_for = {}
    cnt_alw = {}
    cnt_pos = 0
    used_any = 0

    def _inc(d, key):
        d[key] = d.get(key, 0) + 1

    for m in matches or []:
        fix = (m.get('fixture') or {})
        if context in ("home","away"):
            is_home = ((m.get('teams') or {}).get('home') or {}).get('id') == team_id
            if context == "home" and not is_home: 
                continue
            if context == "away" and is_home: 
                continue

        fid = fix.get('id')
        if not fid:
            continue
        stats = get_stats_fn(fid)
        if not stats:
            continue

        teams = (m.get('teams') or {})
        hid = ((teams.get('home') or {}).get('id'))
        aid = ((teams.get('away') or {}).get('id'))
        opp = aid if hid == team_id else hid
        micro = _extract_match_micro_for_team(stats, team_id, opp)
        if not micro:
            continue

        def add_pair(key_for, key_alw, tag):
            vf = micro.get(key_for)
            va = micro.get(key_alw)
            if vf is not None:
                sums[tag+"_for"] += float(vf)
                _inc(cnt_for, tag)
            if va is not None:
                sums[tag+"_alw"] += float(va)
                _inc(cnt_alw, tag)

        # postojeći + novi signali
        add_pair("sot1h_for","sot1h_allowed","sot")
        add_pair("da1h_for","da1h_allowed","da")
        if micro.get("pos1h") is not None:
            sums["pos"] += float(micro["pos1h"]); cnt_pos += 1
        add_pair("shots1h_for","shots1h_allowed","shots")
        add_pair("xg1h_for","xg1h_allowed","xg")
        add_pair("bigch1h_for","bigch1h_allowed","bigch")
        add_pair("corn1h_for","corn1h_allowed","corn")
        add_pair("fk1h_for","fk1h_allowed","fk")
        add_pair("offs1h_for","offs1h_allowed","offs")
        add_pair("cross1h_for","cross1h_allowed","cross")
        add_pair("counter1h_for","counter1h_allowed","counter")
        add_pair("saves1h_for","saves1h_allowed","saves")
        add_pair("sib1h_for","sib1h_allowed","sib")
        add_pair("sob1h_for","sob1h_allowed","sob")
        add_pair("wood1h_for","wood1h_allowed","wood")

        used_any += 1

    def _avg_for(tag):
        n = cnt_for.get(tag, 0)
        return None if n == 0 else round(sums[tag+"_for"]/n, 3)

    def _avg_alw(tag):
        n = cnt_alw.get(tag, 0)
        return None if n == 0 else round(sums[tag+"_alw"]/n, 3)

    return {
        "used_matches": used_any,

        "sot1h_for": _avg_for("sot"),
        "sot1h_allowed": _avg_alw("sot"),
        "da1h_for": _avg_for("da"),
        "da1h_allowed": _avg_alw("da"),
        "pos1h": None if cnt_pos==0 else round(sums["pos"]/cnt_pos,3),

        "shots1h_for": _avg_for("shots"),
        "shots1h_allowed": _avg_alw("shots"),
        "xg1h_for": _avg_for("xg"),
        "xg1h_allowed": _avg_alw("xg"),
        "bigch1h_for": _avg_for("bigch"),
        "bigch1h_allowed": _avg_alw("bigch"),
        "corn1h_for": _avg_for("corn"),
        "corn1h_allowed": _avg_alw("corn"),
        "fk1h_for": _avg_for("fk"),
        "fk1h_allowed": _avg_alw("fk"),

        "offs1h_for": _avg_for("offs"),
        "offs1h_allowed": _avg_alw("offs"),
        "cross1h_for": _avg_for("cross"),
        "cross1h_allowed": _avg_alw("cross"),
        "counter1h_for": _avg_for("counter"),
        "counter1h_allowed": _avg_alw("counter"),
        "saves1h_for": _avg_for("saves"),
        "saves1h_allowed": _avg_alw("saves"),
        "sib1h_for": _avg_for("sib"),
        "sib1h_allowed": _avg_alw("sib"),
        "sob1h_for": _avg_for("sob"),
        "sob1h_allowed": _avg_alw("sob"),
        "wood1h_for": _avg_for("wood"),
        "wood1h_allowed": _avg_alw("wood"),

        # “coverage” za mikro
        "used_sot": cnt_for.get("sot",0) + cnt_alw.get("sot",0),
        "used_da":  cnt_for.get("da",0)  + cnt_alw.get("da",0),
        "used_pos": cnt_pos,
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
    cur.execute("SELECT fixture_json FROM fixtures WHERE date >= %s AND date <= %s", (s, e))
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

# OVO ubaci ispod OVOG DELA KODA (npr. ispod blend_with_market):

def _minute_bucket_prior_1h(team_stats: dict | None) -> tuple[float, float]:
    """
    Iz teams/statistics (po ligi/sezoni) izvuče golove for/against po minutnim segmentima
    i vrati λ_for_1H, λ_against_1H (0–45).
    Očekuje shape poput:
      team_stats["goals"]["for"]["minute"]["0-15"]["total"] itd.
    Ako nema podataka → (None, None).
    """
    if not team_stats: 
        return (None, None)
    def _bucket_total(d, key):
        x = ((d or {}).get(key) or {})
        t = x.get("total")
        return None if t is None else float(t)

    try:
        mfor = (((team_stats.get("goals") or {}).get("for") or {}).get("minute") or {})
        mag  = (((team_stats.get("goals") or {}).get("against") or {}).get("minute") or {})
        for_1h = sum([_bucket_total(mfor, k) or 0.0 for k in ("0-15","16-30","31-45")])
        ag_1h  = sum([_bucket_total(mag,  k) or 0.0 for k in ("0-15","16-30","31-45")])

        played = (((team_stats.get("fixtures") or {}).get("played") or {}).get("total") or 0) or 0
        if played <= 0:
            return (None, None)
        lam_for = max(0.0, for_1h / float(played))
        lam_ag  = max(0.0, ag_1h  / float(played))
        return (lam_for, lam_ag)
    except:
        return (None, None)


def _prior_from_minute_buckets(repo, fixture, no_api=False) -> tuple[float, float]:
    """
    Vrati (p_minute_prior, eff_n) za 1H≥1 gol koristeći teams/statistics minute buckete oba tima.
    """
    league_id = ((fixture.get('league') or {}).get('id'))
    season    = ((fixture.get('league') or {}).get('season'))
    home_id   = ((fixture.get('teams') or {}).get('home') or {}).get('id')
    away_id   = ((fixture.get('teams') or {}).get('away') or {}).get('id')
    if not all([league_id, season, home_id, away_id]):
        return (None, 0.0)

    th = repo.get_team_statistics(home_id, league_id, season, no_api=no_api) or {}
    ta = repo.get_team_statistics(away_id, league_id, season, no_api=no_api) or {}

    lam_h_for, lam_h_ag = _minute_bucket_prior_1h(th)
    lam_a_for, lam_a_ag = _minute_bucket_prior_1h(ta)

    vals = [v for v in [lam_h_for, lam_h_ag, lam_a_for, lam_a_ag] if v is not None]
    if not vals:
        return (None, 0.0)

    lam_total = 0.0
    if lam_h_for is not None: lam_total += lam_h_for
    if lam_a_for is not None: lam_total += lam_a_for
    if lam_h_ag  is not None: lam_total += 0.5*lam_h_ag
    if lam_a_ag  is not None: lam_total += 0.5*lam_a_ag

    # Poisson P(N>=1) = 1 - e^{-λ}
    p = 1.0 - math.exp(-lam_total)
    effn = max(1.0, len(vals) * 2.0)  # grubo: 2 po timu
    return (max(0.0, min(1.0, p)), effn)


def _fts_cs_form_coach_adj(repo, fixture, no_api=False) -> float:
    """
    Lagani prior adj iz teams/statistics:
      - failed_to_score (away se više kažnjava)
      - clean_sheet (home česti clean sheet → malo dole)
      - forma/streak (W/D/L string)
      - (opciono) coach-change indikator (ako kasnije dodaš u repo)
    Vraća logit-adj skalar (≈ -0.08 .. +0.08).
    """
    league_id = ((fixture.get('league') or {}).get('id'))
    season    = ((fixture.get('league') or {}).get('season'))
    home_id   = ((fixture.get('teams') or {}).get('home') or {}).get('id')
    away_id   = ((fixture.get('teams') or {}).get('away') or {}).get('id')
    if not all([league_id, season, home_id, away_id]):
        return 0.0

    th = repo.get_team_statistics(home_id, league_id, season, no_api=no_api) or {}
    ta = repo.get_team_statistics(away_id, league_id, season, no_api=no_api) or {}

    def _safe_int(x, d=0): 
        try: return int(x)
        except: return d

    fts_a = _safe_int(((ta.get("failed_to_score") or {}).get("total")))
    cs_h  = _safe_int(((th.get("clean_sheet") or {}).get("home")))
    form_h = (th.get("form") or "")  # npr. "WWDLW"
    form_a = (ta.get("form") or "")

    adj = 0.0
    # failed to score (away)
    if fts_a >= 8: adj -= 0.05
    elif fts_a >= 5: adj -= 0.03

    # home clean sheets
    if cs_h >= 8: adj -= 0.04
    elif cs_h >= 5: adj -= 0.02

    # forma (vrlo blago)
    def score_form(s): 
        return (s.count("W")*2 + s.count("D") - s.count("L"))
    fscore = score_form(form_h) + score_form(form_a)
    if fscore >= 6: adj += 0.02
    elif fscore <= -6: adj -= 0.02

    # (opciono) coach-change signal → ako dodaš repo.get_coach_change(team_id)
    # if repo.get_coach_change(home_id): adj += 0.01
    # if repo.get_coach_change(away_id): adj += 0.01

    return max(-0.08, min(0.08, adj))

def _calculate_form_adjustment(fixture, team_last_matches, no_api=False) -> float:
    """
    Kalkuliše form adjustment na osnovu recentnih rezultata.
    Vraća logit-adj skalar (-0.1 .. +0.1).
    """
    home_id = ((fixture.get('teams') or {}).get('home') or {}).get('id')
    away_id = ((fixture.get('teams') or {}).get('away') or {}).get('id')
    
    if not home_id or not away_id:
        return 0.0
    
    # Uzmi poslednje mečeve
    home_matches = team_last_matches.get(home_id, [])[:5]  # poslednja 5
    away_matches = team_last_matches.get(away_id, [])[:5]
    
    def _score_match(match):
        """Score meča: 3 za pobedu, 1 za nerešeno, 0 za poraz"""
        if not match or not isinstance(match, dict):
            return 0
        score = match.get('score', {})
        if not score:
            return 0
        
        home_goals = int(score.get('fulltime', {}).get('home', 0) or 0)
        away_goals = int(score.get('fulltime', {}).get('away', 0) or 0)
        
        if home_goals > away_goals:
            return 3
        elif home_goals == away_goals:
            return 1
        else:
            return 0
    
    # Kalkuliši form score
    home_form_score = sum(_score_match(m) for m in home_matches) / max(1, len(home_matches))
    away_form_score = sum(_score_match(m) for m in away_matches) / max(1, len(away_matches))
    
    # Kombinuj form score (home advantage)
    combined_form = (home_form_score * 1.1 + away_form_score * 0.9) / 2.0
    
    # Konvertuj u logit adjustment
    # Neutral form = 1.5 (50% pobeda), dobra forma > 1.5, loša < 1.5
    form_adj = (combined_form - 1.5) * 0.1  # skaliramo
    
    return max(-0.1, min(0.1, form_adj))

def _calculate_coach_adjustment(fixture, no_api=False) -> float:
    """
    Kalkuliše coach adjustment na osnovu promene trenera.
    Vraća logit-adj skalar (-0.05 .. +0.05).
    """
    # TODO: Implementiraj kada bude dostupno u repo
    # Za sada vraćamo 0, ali struktura je spremna
    return 0.0

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
    # ISPRAVKA: Bezbedno rukovanje sa podacima koji mogu biti tuple-ovi ili dict-ovi
    safe_matches = []
    for m in (matches or []):
        if isinstance(m, dict):
            safe_matches.append(m)
        elif isinstance(m, (list, tuple)):
            # Pokušaj da konvertuješ tuple u dict
            try:
                converted = _coerce_fixture_row_to_api_dict(m)
                if converted and isinstance(converted, dict):
                    safe_matches.append(converted)
            except Exception:
                continue
    
    arr = sorted(safe_matches, key=lambda m: (m.get('fixture') or {}).get('date',''), reverse=True)[:max_n]
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
            names_full = [n.lower() for n in (full_names + ["shots on target"])]
            names_half = [n.lower() for n in (half_names + [
                "1st half shots on target", "shots on target 1st half", "first half shots on target"
            ])]
            v1 = _stat_from_block(block, names_half)
            if v1 is not None:
                return float(v1)
            vfull = _stat_from_block(block, names_full)
            if vfull is None:
                return None
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
            # ISPRAVKA: Bezbedno rukovanje sa podacima koji mogu biti tuple-ovi ili dict-ovi
            if not isinstance(m, dict):
                if isinstance(m, (list, tuple)):
                    try:
                        converted = _coerce_fixture_row_to_api_dict(m)
                        if converted and isinstance(converted, dict):
                            m = converted
                        else:
                            continue
                    except Exception:
                        continue
                else:
                    continue
                    
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
        # ISPRAVKA: Bezbedno rukovanje sa podacima koji mogu biti tuple-ovi ili dict-ovi
        safe_matches = []
        for m in (matches or []):
            if isinstance(m, dict):
                safe_matches.append(m)
            elif isinstance(m, (list, tuple)):
                try:
                    converted = _coerce_fixture_row_to_api_dict(m)
                    if converted and isinstance(converted, dict):
                        safe_matches.append(converted)
                except Exception:
                    continue
        h_sc, w_sc, n_sc = _weighted_counts(safe_matches, lambda m: _team_scored_1h(m, team_id), lam, max_n)
        h_con, w_con, n_con = _weighted_counts(safe_matches, lambda m: _team_conceded_1h(m, team_id), lam, max_n)
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
        print(f"🔍 [DEBUG] compute_referee_profile START for ref_name: {ref_name}, season: {season}")
        if not ref_name:
            print(f"🔍 [DEBUG] compute_referee_profile no ref_name, returning neutral")
            return {"p1h_ge1": None, "p1h_ge2": None, "p1h_gg": None, "ref_adj": 0.0, "used": 0.0}
        year = season or datetime.utcnow().year
        print(f"🔍 [DEBUG] compute_referee_profile calling repo.get_referee_fixtures for year: {year}")
        arr = repo.get_referee_fixtures(ref_name, season=year, last_n=200, no_api=False) or []
        print(f"🔍 [DEBUG] compute_referee_profile got {len(arr)} fixtures, type: {type(arr)}")
        if arr:
            print(f"🔍 [DEBUG] compute_referee_profile first fixture type: {type(arr[0])}")
            print(f"🔍 [DEBUG] compute_referee_profile first fixture: {arr[0]}")
        # recency ponder (lakši)
        print(f"🔍 [DEBUG] compute_referee_profile calling _weighted_counts for h1")
        h1, w1, _ = _weighted_counts(sorted(arr, key=lambda m: (m.get('fixture') or {}).get('date',''), reverse=True),
                                     _ht_total_ge1, lam=8.0, max_n=200)
        print(f"🔍 [DEBUG] compute_referee_profile calling _weighted_counts for h2")
        h2, w2, _ = _weighted_counts(sorted(arr, key=lambda m: (m.get('fixture') or {}).get('date',''), reverse=True),
                                     _ht_total_ge2, lam=8.0, max_n=200)
        print(f"🔍 [DEBUG] compute_referee_profile calling _weighted_counts for hg")
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
            # ISPRAVKA: Bezbedno rukovanje sa podacima koji mogu biti tuple-ovi ili dict-ovi
            if not isinstance(team, dict):
                continue
            sx = (team.get("startXI") or [])
            for p in sx:
                if not isinstance(p, dict):
                    continue
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
    print(f"🔍 [DEBUG] build_extras_for_fixture START for fixture {fixture}")
    fid = ((fixture.get("fixture") or {}).get("id")) or ((fixture.get("fixture") or {}).get("id"))
    print(f"🔍 [DEBUG] build_extras_for_fixture fid: {fid}")
    season = ((fixture.get("league") or {}).get("season"))
    print(f"🔍 [DEBUG] build_extras_for_fixture season: {season}")
    print(f"🔍 [DEBUG] build_extras_for_fixture calling repo.get_fixture_full")
    fx_full = repo.get_fixture_full(fid, no_api=no_api)  # osvežava fixture_json ako treba
    print(f"🔍 [DEBUG] build_extras_for_fixture fx_full type: {type(fx_full)}")
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
def _coerce_fixture_row_to_api_dict(row) -> dict | None:
    """
    Prihvata red iz MySQL-a (dict ili tuple) i vraća API-like dict:
      { 'fixture': {...}, 'league': {...}, 'teams': {'home': {...}, 'away': {...}}, ... }
    Ako je u bazi čuvano kompletno JSON polje 'fixture_json', radi json.loads na njemu.
    """
    # 1) ako je već dict sa ključevima 'fixture'/'league' – vrati direktno
    if isinstance(row, dict) and ("fixture" in row or "league" in row or "teams" in row):
        return row

    # 2) ako je dict red iz MySQL-a sa poljem fixture_json
    payload = None
    if isinstance(row, dict):
        payload = row.get("fixture_json")
    elif isinstance(row, (list, tuple)):
        # pretpostavljamo da je fixture_json na indeksu 6 po SELECT-u iz tvoje tabele
        # (id, date, league_id, team_home_id, team_away_id, stats_json, fixture_json)
        try:
            payload = row[6]
        except Exception:
            payload = None

    if payload is None:
        return None

    if isinstance(payload, (bytes, bytearray)):
        payload = payload.decode("utf-8", errors="ignore")

    try:
        fx = json.loads(payload) if isinstance(payload, str) else payload
    except Exception:
        return None

    # konačna provera
    return fx if isinstance(fx, dict) else None


def get_fixtures_in_time_range(start_dt, end_dt, from_hour=None, to_hour=None, no_api: bool = True):
    """
    Vraća listu API-like dict-ova (ne tuple/list redova), tako da ostatak koda
    (compute_* i analyze_*) bezbedno radi .get i ['fixture']['id'] itd.
    DODANO: Filtrira po satima i liga whitelist-u.
    """
    conn = get_db_connection()
    try:
        cur = conn.cursor(dictionary=True)  # dict cursor
        cur.execute(
            "SELECT id, date, league_id, team_home_id, team_away_id, stats_json, fixture_json "
            "FROM fixtures WHERE date BETWEEN %s AND %s",
            (start_dt, end_dt)
        )
        fixtures: list[dict] = []
        for row in cur:
            fx = _coerce_fixture_row_to_api_dict(row)
            if fx and isinstance(fx, dict) and fx.get("fixture"):
                # Primeni league filtering pre dodavanja u rezultat
                if not is_valid_competition(fx):
                    continue
                
                # Primeni time filtering ako je zadat
                if from_hour is not None or to_hour is not None:
                    fixture_date = (fx.get('fixture') or {}).get('date')
                    if fixture_date and not is_fixture_in_range(fixture_date, start_dt, end_dt, from_hour, to_hour):
                        continue
                
                fixtures.append(fx)
        return fixtures
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()

# ---------------------------- ANALYTICS -----------------------------
def fetch_last_matches_for_teams(fixtures, last_n=30, no_api: bool = False):
    """
    Uvijek traži tačno last_n po timu (bez dinamičkog skraćivanja).
    Ako no_api=True i nema pun cache za traženi last_n, vrati šta ima u kešu (može biti 0),
    ali NE smanjuj last_n.
    """
    # ISPRAVKA: Proverava da li je fixture dict i ima potrebne ključeve
    team_ids = set()
    for f in fixtures:
        if not isinstance(f, dict):
            continue
        teams = f.get('teams', {})
        if isinstance(teams, dict):
            home_id = (teams.get('home') or {}).get('id')
            away_id = (teams.get('away') or {}).get('id')
            if home_id:
                team_ids.add(home_id)
            if away_id:
                team_ids.add(away_id)
    
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
        if not isinstance(fixture, dict):
            continue
        teams = fixture.get('teams', {})
        if not isinstance(teams, dict):
            continue
        home_id = (teams.get('home') or {}).get('id')
        away_id = (teams.get('away') or {}).get('id')
        if not (home_id and away_id):
            continue
        a, b = sorted([home_id, away_id])
        key = f"{a}-{b}"
        if key in h2h_results:
            continue
        h2h_results[key] = get_or_fetch_h2h(a, b, last_n=last_n, no_api=no_api)
        time.sleep(0.1)
    return h2h_results

def prewarm_extras_for_fixtures(fixtures, *, include_odds=True, include_team_stats=True) -> dict:
    """
    Za svaku utakmicu:
      - fixtures full (referee + weather u fixture_json)
      - venue
      - lineups
      - injuries
      - team league stats (ako traženo)
      - odds 1H i FT (ako traženo)
    Sve se upisuje u postojeće *cache* tabele (MySQL). Vraća mali summary broja “hitova”.
    """
    warmed = {
        "fixture_full": 0, "venue": 0, "lineups": 0, "injuries": 0,
        "referee": 0, "team_stats": 0, "odds": 0
    }
    seen_ref = set()
    for fx in fixtures or []:
        fid    = ((fx.get("fixture") or {}).get("id"))
        season = ((fx.get("league")  or {}).get("season"))
        lid    = ((fx.get("league")  or {}).get("id"))
        hid    = ((fx.get("teams")   or {}).get("home") or {}).get("id")
        aid    = ((fx.get("teams")   or {}).get("away") or {}).get("id")
        if not fid: 
            continue

        # fixtures full (referee + weather)
        try:
            fx_full = repo.get_fixture_full(fid, no_api=False)
            warmed["fixture_full"] += 1
        except Exception:
            fx_full = None

        # referee fixtures -> referee_cache
        try:
            ref_name = (((fx_full or {}).get("fixture") or {}).get("referee") 
                        or ((fx.get("fixture") or {}).get("referee")))
            if ref_name and (ref_name, season) not in seen_ref:
                repo.get_referee_fixtures(ref_name, season=season, last_n=200, no_api=False)
                warmed["referee"] += 1
                seen_ref.add((ref_name, season))
        except Exception:
            pass

        # venue
        try:
            ven_id = (((fx_full or {}).get("fixture") or {}).get("venue") or {}).get("id") or \
                     (((fx.get("fixture")  or {}).get("venue")  or {}).get("id"))
            if ven_id:
                repo.get_venue(ven_id, no_api=False)
                warmed["venue"] += 1
        except Exception:
            pass

        # lineups + injuries
        try:
            repo.get_lineups(fid, no_api=False)
            warmed["lineups"] += 1
        except Exception:
            pass
        try:
            repo.get_injuries(fid, no_api=False)
            warmed["injuries"] += 1
        except Exception:
            pass

        # team league stats (ako se koriste u FT profilima)
        if include_team_stats and lid and season:
            try:
                if hid: 
                    repo.get_team_statistics(hid, lid, season, no_api=False)
                    warmed["team_stats"] += 1
                if aid:
                    repo.get_team_statistics(aid, lid, season, no_api=False)
                    warmed["team_stats"] += 1
            except Exception:
                pass

        # odds (1H i FT)
        if include_odds:
            try:
                repo.get_odds_1h(fid, no_api=False)
                warmed["odds"] += 1
            except Exception:
                pass
            try:
                repo.get_odds_ft(fid, no_api=False)
                warmed["odds"] += 1
            except Exception:
                pass

    return warmed

def prepare_inputs_for_range(start_dt: datetime, end_dt: datetime) -> dict:
    """Vrati dict sa preloaded mapama: team_last, h2h, extras (DB-only nakon prewarm-a)."""
    fixtures = [fx for fx in (get_fixtures_in_time_range(start_dt, end_dt, no_api=True) or []) if isinstance(fx, dict) and fx.get('fixture')]
    team_last = fetch_last_matches_for_teams(fixtures, last_n=DAY_PREFETCH_LAST_N, no_api=True)
    h2h_all   = fetch_h2h_matches(fixtures, last_n=DAY_PREFETCH_H2H_N, no_api=True)
    extras    = build_extras_map_for_fixtures(fixtures)
    return {"fixtures": fixtures, "team_last": team_last, "h2h": h2h_all, "extras": extras}

def build_extras_map_for_fixtures(fixtures) -> dict[int, dict]:
    """
    Izračunaj 'extras' (ref_adj, weather_adj, venue_adj, lineups_adj, injuries_adj) za svaku utakmicu
    i vrati mapu { fixture_id: extras }. Radi DB-only ako je keš već popunjen.
    """
    out = {}
    for fx in fixtures or []:
        fid = ((fx.get("fixture") or {}).get("id"))
        if not fid:
            continue
        # pošto je prewarm već odradio fetch u keš, ovde radimo no_api=True (DB-only)
        ex = build_extras_for_fixture(fx, no_api=True)
        out[int(fid)] = ex or {}
    return out

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
    """
    Bulk upis fixtures u MySQL.

    Zahtevi:
    - Tabela `fixtures` mora imati PRIMARY KEY na koloni `id`
      (npr. `id BIGINT PRIMARY KEY` ili UNIQUE KEY na `id`),
      da bi `ON DUPLICATE KEY UPDATE` radio.
    - (Opcionalno) kolona `updated_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP
      ON UPDATE CURRENT_TIMESTAMP` – ako želiš automatsko osvežavanje.
    DODANO: Filtrira fixtures po liga whitelist-u pre upisivanja.
    """
    fixtures = [f for f in (fixtures or []) if isinstance(f, dict)]
    if not fixtures:
        return 0
    
    # NOVO: Filtriraj po liga whitelist-u PRE upisivanja u bazu
    valid_fixtures = [f for f in fixtures if is_valid_competition(f)]
    if not valid_fixtures:
        print(f"⚠️ Svi fixtures ({len(fixtures)}) su odbačeni zbog league filtering-a")
        return 0
    
    if len(valid_fixtures) < len(fixtures):
        print(f"ℹ️ League filtering: zadržano {len(valid_fixtures)}/{len(fixtures)} fixtures")
    
    fixtures = valid_fixtures

    affected = 0
    with DB_WRITE_LOCK:  # jedan “bulk” upis
        conn = get_db_connection()
        cur = None
        try:
            # START TRANSACTION umesto SQLite BEGIN IMMEDIATE
            conn.start_transaction()  # MySQL-safe

            cur = conn.cursor()

            sql = """
                INSERT INTO fixtures
                    (id, date, league_id, team_home_id, team_away_id, stats_json, fixture_json, updated_at)
                VALUES
                    (%s, %s, %s, %s, %s, %s, %s, NOW())
                ON DUPLICATE KEY UPDATE
                    date        = VALUES(date),
                    league_id   = VALUES(league_id),
                    team_home_id= VALUES(team_home_id),
                    team_away_id= VALUES(team_away_id),
                    stats_json  = VALUES(stats_json),
                    fixture_json= VALUES(fixture_json),
                    updated_at  = NOW()
            """

            rows = []
            for fixture in fixtures:
                # Bezbedna ekstrakcija
                fx  = fixture.get('fixture') or {}
                lg  = fixture.get('league')  or {}
                tms = fixture.get('teams')   or {}
                th  = (tms.get('home') or {})
                ta  = (tms.get('away') or {})

                fixture_id   = fx.get('id')
                fixture_date = fx.get('date')  # očekuje se ISO string; MySQL ga prihvata kao DATETIME/VARCHAR po šemi
                league_id    = lg.get('id')
                team_home_id = th.get('id')
                team_away_id = ta.get('id')

                # NON-WRITE: čitanje statistika ako postoje u DB/kešu
                stats = try_read_fixture_statistics(fixture_id)
                stats_json   = json.dumps(stats, ensure_ascii=False, default=str) if stats else None
                fixture_json = json.dumps(fixture, ensure_ascii=False, default=str)

                rows.append((
                    fixture_id, fixture_date, league_id, team_home_id, team_away_id,
                    stats_json, fixture_json
                ))

            if rows:
                cur.executemany(sql, rows)
                affected = cur.rowcount or 0

            conn.commit()
        except Exception as e:
            # Rolbek da ne ostavimo polu-upisane redove
            try:
                conn.rollback()
            except Exception:
                pass
            # Propusti dalje – gornji sloj neka zaloguje
            raise
        finally:
            try:
                if cur is not None:
                    cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    # Opcionalno: log
    print(f"✅ Stored/updated {len(rows) if 'rows' in locals() else 0} fixtures in DB.")
    return affected

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
        cur.execute("SELECT updated_at FROM team_history_cache WHERE team_id=%s AND last_n=%s", (tid, last_n))
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
        cur.execute("SELECT updated_at FROM h2h_cache WHERE team1_id=%s AND team2_id=%s AND last_n=%s", (x, y, last_n))
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
async def api_prepare_day(request: Request, background_tasks: BackgroundTasks):
    """
    Enqueue prepare posla i odmah vraća job_id (202).
    Body ili query: { "date": "YYYY-MM-DD", "prewarm": true/false }
    """
    try:
        # Get request payload first
        try:
            payload = await request.json()
        except Exception:
            payload = {}
        
        # Admin check - only klisaricf@gmail.com can access
        session_id = request.headers.get('Authorization', '').replace('Bearer ', '')
        if not session_id:
            session_id = payload.get('session_id', '')
        
        if session_id:
            session_data = get_session(session_id)
            if session_data:
                # Email is stored directly in session_data, not in session_data['user']
                user_email = session_data.get('email')
                if user_email != 'klisaricf@gmail.com':
                    return {"error": "Access denied. Admin privileges required."}, 403
            else:
                return {"error": "Invalid session. Please log in again."}, 401
        else:
            return {"error": "Authentication required."}, 401
        date_str = (payload or {}).get("date") or request.query_params.get("date")
        prewarm  = bool((payload or {}).get("prewarm", True))

        if not date_str:
            d_local = datetime.now(USER_TZ).date()
        else:
            d_local = date.fromisoformat(date_str)

        # 0) sigurnosno: obezbedi šemu/tabele (idempotentno)
        try:
            create_all_tables()
            ensure_model_outputs_table()
            ensure_analysis_cache_table()
            ensure_prepare_jobs_table()
        except Exception as _schema_err:
            print("schema ensure failed:", _schema_err)

        # 1) napravi job u DB
        job_id = create_prepare_job(d_local)

        # 2) pokreni u pozadini preko FastAPI background task-a (pouzdanije od ručnog threada)
        background_tasks.add_task(run_prepare_job, job_id, d_local.isoformat(), prewarm)

        # 3) odmah odgovori
        return JSONResponse(status_code=202, content={"ok": True, "job_id": job_id})

    except Exception as e:
        print("prepare-day enqueue error:", e)
        print(traceback.format_exc())
        return JSONResponse(status_code=500, content={"ok": False, "error": "prepare_enqueue_failed", "detail": str(e)})

@app.get("/api/prepare-day/status")
async def api_prepare_day_status(job_id: str):
    job = read_prepare_job(job_id)
    if not job:
        return JSONResponse(status_code=404, content={"ok": False, "error": "job not found"})
    return JSONResponse(status_code=200, content={
        "ok": True,
        "status": job["status"],
        "progress": job["progress"],
        "detail": job["detail"],
        "result": job["result"]
    })

def fetch_and_store_all_historical_data(fixtures, no_api: bool = False):
    # 1) last-30 po timu (sa kešom / no_api)
    all_team_matches = fetch_last_matches_for_teams(fixtures, last_n=DAY_PREFETCH_LAST_N, no_api=no_api)

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

def team_ft_over15_stats(team_matches):
    """Vrati (percent, hits, total) za FT Over 1.5 iz istorije tima."""
    total = 0
    hits = 0
    for m in team_matches or []:
        ft = (m.get('score', {}) or {}).get('fulltime', {}) or {}
        h = ft.get('home') or 0
        a = ft.get('away') or 0
        if h is None: h = 0
        if a is None: a = 0
        total += 1
        if (h + a) >= 2:  # FT Over 1.5 = 2+ goals
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
    cur.execute("SELECT fixture_json FROM fixtures WHERE id=%s", (fixture_id,))
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

def calculate_final_probability(
    fixture, team_last_matches, h2h_results, micro_db,
    league_baselines, team_strengths, team_profiles,
    extras: dict | None = None, no_api: bool = False,
    market_odds_over05_1h: Optional[float] = None
):
    """
    Finalna vjerovatnoća za 1H Over 0.5.
    VRAĆA: (final_percent, debug)  ← TAČNO DVA
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

    # --- TEAM PRIOR (istorija timova, EB) ---
    p_home_raw, h_home, w_home = _weighted_match_over05_rate(team_last_matches.get(home_id, []), lam=5.0, max_n=15)
    p_away_raw, h_away, w_away = _weighted_match_over05_rate(team_last_matches.get(away_id, []), lam=5.0, max_n=15)

    p_home = beta_shrunk_rate(h_home, w_home, m=m, tau=8.0) if p_home_raw is not None else m
    p_away = beta_shrunk_rate(h_away, w_away, m=m, tau=8.0) if p_away_raw is not None else m
    p_team_prior = (p_home + p_away) / 2.0

    # --- H2H PRIOR ---
    p_h2h_raw, h_h2h, w_h2h = _weighted_h2h_over05_rate(h2h_results.get(h2h_key, []), lam=4.0, max_n=10)
    p_h2h = beta_shrunk_rate(h_h2h, w_h2h, m=m, tau=H2H_TAU) if p_h2h_raw is not None else m
    effn_h2h = (w_h2h or 0.0) * H2H_SCALE
    if (w_h2h or 0.0) < H2H_MIN_W:
        effn_h2h = 0.0
        p_h2h = m

    # --- Fuzija TEAM ⊕ H2H ---
    p_prior_tmp, _ = fuse_probs_by_precision(
        p_team_prior, (w_home or 0.0) + (w_away or 0.0),
        p_h2h,        effn_h2h
    )

    # --- Minute-bucket prior (blagi blend) ---
    p_minute, effn_minute = _prior_from_minute_buckets(repo, fixture, no_api=no_api)
    if p_minute is not None:
        w_min = float(WEIGHTS.get("MINUTE_PRIOR_BLEND", 0.25))
        p_prior = (1.0 - w_min) * p_prior_tmp + w_min * p_minute
    else:
        p_prior = p_prior_tmp

    # --- Blagi prior logit-adj iz FTS/CS/Form ---
    fts_cs_adj = _fts_cs_form_coach_adj(repo, fixture, no_api=no_api)
    prior_logit_adj = WEIGHTS.get("FTSCS_ADJ", 0.05) * fts_cs_adj
    p_prior = _inv_logit(_logit(p_prior) + prior_logit_adj)
    
    # --- FORM_ADJ i COACH_ADJ implementacija ---
    form_adj = _calculate_form_adjustment(fixture, team_last_matches, no_api=no_api)
    coach_adj = _calculate_coach_adjustment(fixture, no_api=no_api)
    
    # Dodaj u prior
    p_prior = _inv_logit(_logit(p_prior) + WEIGHTS.get("FORM_ADJ", 0.03) * form_adj)
    p_prior = _inv_logit(_logit(p_prior) + WEIGHTS.get("COACH_ADJ", 0.02) * coach_adj)
    
    # --- KRITIČNI FEATURE-I ---
    # Treba da uključimo 4 kritična feature-a u p_prior kalkulaciju
    feats_temp = matchup_features_enhanced(fixture, team_profiles, league_baselines, micro_db=micro_db, extras=extras)
    
    # 1. pace_da_total - tempo dangerous attacks
    pace_da_total = feats_temp.get("pace_da_total", 0.0)
    if pace_da_total > 0:
        base_da = _league_base_for_fixture(fixture, league_baselines).get("mu_da1h", 0.0)
        pace_da_z = _z(pace_da_total, base_da, max(1.0, base_da * 0.5)) if base_da > 0 else 0.0
        pace_da_adj = WEIGHTS.get("PACE_DA_ADJ", 0.04) * pace_da_z
        p_prior = _inv_logit(_logit(p_prior) + pace_da_adj)
    
    # 2. lineups_have - da li imamo lineup podatke (pozitivno)
    lineups_have = feats_temp.get("lineups_have", False)
    if lineups_have:
        lineups_have_adj = WEIGHTS.get("LINEUPS_HAVE_ADJ", 0.03)
        p_prior = _inv_logit(_logit(p_prior) + lineups_have_adj)
    
    # 3. lineups_fw_count - broj napadača (pozitivno)
    lineups_fw_count = feats_temp.get("lineups_fw_count")
    if lineups_fw_count is not None and lineups_fw_count > 0:
        # Normalizuj na 0-1 skalu (pretpostavljamo 1-4 napadača)
        fw_normalized = max(0.0, min(1.0, (lineups_fw_count - 1) / 3.0))
        lineups_fw_adj = WEIGHTS.get("LINEUPS_FW_ADJ", 0.02) * fw_normalized
        p_prior = _inv_logit(_logit(p_prior) + lineups_fw_adj)
    
    # 4. inj_count - broj povreda (negativno)
    inj_count = feats_temp.get("inj_count")
    if inj_count is not None and inj_count > 0:
        # Negativan uticaj - više povreda = manja verovatnoća
        inj_normalized = min(1.0, inj_count / 10.0)  # Normalizuj na 0-1
        inj_count_adj = -WEIGHTS.get("INJ_COUNT_ADJ", 0.02) * inj_normalized
        p_prior = _inv_logit(_logit(p_prior) + inj_count_adj)

    # --- MICRO ---
    feats = matchup_features_enhanced(fixture, team_profiles, league_baselines, micro_db=micro_db, extras=extras)

    # totals (za debug/UI)
    exp_sot_total = None
    if feats.get('exp_sot1h_home') is not None or feats.get('exp_sot1h_away') is not None:
        exp_sot_total = round((feats.get('exp_sot1h_home') or 0) + (feats.get('exp_sot1h_away') or 0), 3)
    exp_da_total = None
    if feats.get('exp_da1h_home') is not None or feats.get('exp_da1h_away') is not None:
        exp_da_total = round((feats.get('exp_da1h_home') or 0) + (feats.get('exp_da1h_away') or 0), 3)

    p_home_goal, dbg_h = predict_team_scores1h_enhanced(fixture, feats, league_baselines, team_strengths, side='home')
    p_away_goal, dbg_a = predict_team_scores1h_enhanced(fixture, feats, league_baselines, team_strengths, side='away')

    # korelacija ρ = f(tempo, class-gap)
    pace_z = _z(feats.get('pace_sot_total'), (base["mu_sot1h"] or 0.0), (base["sd_sot1h"] or 1.0))
    rho_base = max(-0.05, min(0.20, 0.05 + 0.05 * pace_z))
    class_gap_abs = abs(feats.get('tier_gap_home') or 0.0)
    rho_factor = max(0.5, 1.0 - 0.20 * class_gap_abs)
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

    # blend sa tržištem (ako imaš kvotu)
    if market_odds_over05_1h:
        p_final = blend_with_market(p_final, market_odds_over05_1h, alpha=ALPHA_MODEL)

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
        "exp_sot_total": exp_sot_total,
        "exp_da_total":  exp_da_total,
        "fetch_source": FIXTURES_FETCH_SOURCE,
        
        # FORM_ADJ i COACH_ADJ
        "form_adj": form_adj,
        "coach_adj": coach_adj,
        
        # KRITIČNI FEATURE-I
        "pace_da_total": feats.get("pace_da_total"),
        "lineups_have": feats.get("lineups_have"),
        "lineups_fw_count": feats.get("lineups_fw_count"),
        "inj_count": feats.get("inj_count"),
    }
    if market_odds_over05_1h:
        debug["market_odds_over05_1h"] = market_odds_over05_1h
        debug["market_prob_over05_1h"] = round(1.0/float(market_odds_over05_1h), 4)

    return float(p_final * 100.0), debug

def calculate_final_probability_gg(
    fixture, team_last_matches, h2h_results, micro_db,
    league_baselines, team_strengths, team_profiles,
    extras: dict | None = None, no_api: bool = False,
    market_odds_btts_1h: Optional[float] = None
):
    """
    Finalna vjerovatnoća za GG u 1. poluvremenu (oba tima daju gol).
    VRAĆA: (final_percent, debug)  ← TAČNO DVA
    """
    base = _league_base_for_fixture(fixture, league_baselines)
    m = base["m1h"]
    # bazni GG1H ~ funkcija m: nešto niže od m^2, uz mali offset
    m_gg = m*m*0.8 + 0.02

    home_id = ((fixture.get('teams') or {}).get('home') or {}).get('id')
    away_id = ((fixture.get('teams') or {}).get('away') or {}).get('id')

    # TEAM prior: koristimo timske GG1H stope (koliko puta je njihov meč imao GG u 1H)
    t_home = team_last_matches.get(home_id, [])
    t_away = team_last_matches.get(away_id, [])
    ph_pct, ph_hits, ph_tot = team_1h_gg_stats(t_home)
    pa_pct, pa_hits, pa_tot = team_1h_gg_stats(t_away)

    p_home = beta_shrunk_rate(ph_hits, ph_tot, m=m_gg, tau=8.0) if ph_tot > 0 else m_gg
    p_away = beta_shrunk_rate(pa_hits, pa_tot, m=m_gg, tau=8.0) if pa_tot > 0 else m_gg
    p_team_prior = (p_home + p_away) / 2.0

    # H2H prior (GG1H)
    a, b = sorted([home_id, away_id]); key = f"{a}-{b}"
    hh_pct, hh_hits, hh_tot = h2h_1h_gg_stats(h2h_results.get(key, []))
    p_h2h = beta_shrunk_rate(hh_hits, hh_tot, m=m_gg, tau=12.0) if hh_tot > 0 else m_gg
    effn_h2h = max(0.0, hh_tot * 0.35)

    p_prior, _ = fuse_probs_by_precision(
        p_team_prior, (ph_tot or 0.0) + (pa_tot or 0.0),
        p_h2h,        effn_h2h
    )
    
    # --- FORM_ADJ i COACH_ADJ implementacija ---
    form_adj = _calculate_form_adjustment(fixture, team_last_matches, no_api=no_api)
    coach_adj = _calculate_coach_adjustment(fixture, no_api=no_api)
    
    # Dodaj u prior
    p_prior = _inv_logit(_logit(p_prior) + WEIGHTS.get("FORM_ADJ", 0.03) * form_adj)
    p_prior = _inv_logit(_logit(p_prior) + WEIGHTS.get("COACH_ADJ", 0.02) * coach_adj)
    
    # --- KRITIČNI FEATURE-I ---
    # Treba da uključimo 4 kritična feature-a u p_prior kalkulaciju
    feats_temp = matchup_features_enhanced(fixture, team_profiles, league_baselines, micro_db=micro_db, extras=extras)
    
    # 1. pace_da_total - tempo dangerous attacks
    pace_da_total = feats_temp.get("pace_da_total", 0.0)
    if pace_da_total > 0:
        base_da = _league_base_for_fixture(fixture, league_baselines).get("mu_da1h", 0.0)
        pace_da_z = _z(pace_da_total, base_da, max(1.0, base_da * 0.5)) if base_da > 0 else 0.0
        pace_da_adj = WEIGHTS.get("PACE_DA_ADJ", 0.04) * pace_da_z
        p_prior = _inv_logit(_logit(p_prior) + pace_da_adj)
    
    # 2. lineups_have - da li imamo lineup podatke (pozitivno)
    lineups_have = feats_temp.get("lineups_have", False)
    if lineups_have:
        lineups_have_adj = WEIGHTS.get("LINEUPS_HAVE_ADJ", 0.03)
        p_prior = _inv_logit(_logit(p_prior) + lineups_have_adj)
    
    # 3. lineups_fw_count - broj napadača (pozitivno)
    lineups_fw_count = feats_temp.get("lineups_fw_count")
    if lineups_fw_count is not None and lineups_fw_count > 0:
        # Normalizuj na 0-1 skalu (pretpostavljamo 1-4 napadača)
        fw_normalized = max(0.0, min(1.0, (lineups_fw_count - 1) / 3.0))
        lineups_fw_adj = WEIGHTS.get("LINEUPS_FW_ADJ", 0.02) * fw_normalized
        p_prior = _inv_logit(_logit(p_prior) + lineups_fw_adj)
    
    # 4. inj_count - broj povreda (negativno)
    inj_count = feats_temp.get("inj_count")
    if inj_count is not None and inj_count > 0:
        # Negativan uticaj - više povreda = manja verovatnoća
        inj_normalized = min(1.0, inj_count / 10.0)  # Normalizuj na 0-1
        inj_count_adj = -WEIGHTS.get("INJ_COUNT_ADJ", 0.02) * inj_normalized
        p_prior = _inv_logit(_logit(p_prior) + inj_count_adj)

    # MICRO: p(home scores) i p(away scores) + korelacija
    feats = matchup_features_enhanced(fixture, team_profiles, league_baselines, micro_db=micro_db, extras=extras)
    pH, _ = predict_team_scores1h_enhanced(fixture, feats, league_baselines, team_strengths, side='home')
    pA, _ = predict_team_scores1h_enhanced(fixture, feats, league_baselines, team_strengths, side='away')

    # korelacija: veći class-gap → teže da oba daju (manji rho)
    pace_z = _z(feats.get('pace_sot_total'), (base["mu_sot1h"] or 0.0), (base["sd_sot1h"] or 1.0))
    rho_base = max(-0.10, min(0.15, 0.03 + 0.04 * pace_z))
    class_gap_abs = abs(feats.get('tier_gap_home') or 0.0)
    rho_factor = max(0.4, 1.0 - 0.25 * class_gap_abs)
    rho = rho_base * rho_factor

    micro_indep = pH * pA
    cov_term = rho * math.sqrt(max(0.0, pH*(1.0-pH)*pA*(1.0-pA)))
    p_micro = max(0.0, min(1.0, micro_indep + cov_term))

    # precizija
    h_form = (micro_db.get(home_id) or {}).get("home") or {}
    a_form = (micro_db.get(away_id) or {}).get("away") or {}
    effn_micro = (h_form.get('used_sot',0) + a_form.get('used_sot',0) +
                  h_form.get('used_da',0)  + a_form.get('used_da',0)) / 2.0
    effn_micro = max(1.0, effn_micro + (team_profiles.get(home_id,{}).get('eff_n',0) +
                                        team_profiles.get(away_id,{}).get('eff_n',0))/4.0)

    effn_prior = max(1.0, (ph_tot or 0.0) + (pa_tot or 0.0) + effn_h2h)

    p_final, w_micro_share = fuse_probs_by_precision(p_prior, effn_prior, p_micro, effn_micro)

    if market_odds_btts_1h:
        p_final = blend_with_market(p_final, market_odds_btts_1h, alpha=ALPHA_MODEL)

    debug = {
        "prior_percent": round(p_prior*100,2),
        "micro_percent": round(p_micro*100,2),
        "merge_weight_micro": round(w_micro_share,3),
        "p_home_scores_1h": round(pH*100,2),
        "p_away_scores_1h": round(pA*100,2),
        "rho": round(rho,3),
        
        # FORM_ADJ i COACH_ADJ
        "form_adj": form_adj,
        "coach_adj": coach_adj,
        
        # KRITIČNI FEATURE-I
        "pace_da_total": feats.get("pace_da_total"),
        "lineups_have": feats.get("lineups_have"),
        "lineups_fw_count": feats.get("lineups_fw_count"),
        "inj_count": feats.get("inj_count"),
    }
    if market_odds_btts_1h:
        debug["market_odds_btts_1h"] = market_odds_btts_1h
        debug["market_prob_btts_1h"] = round(1.0/float(market_odds_btts_1h), 4)

    return float(p_final * 100.0), debug

    
def calculate_final_probability_over15(
    fixture, team_last_matches, h2h_results, micro_db,
    league_baselines, team_strengths, team_profiles,
    extras: dict | None = None, no_api: bool = False,
    market_odds_over15_1h: Optional[float] = None
):
    """
    Finalna vjerovatnoća za 1H Over 1.5.
    VRAĆA: (final_percent, debug)  ← TAČNO DVA
    """
    # bazni m za ≥1; izvedi m2 iz Poisson λ
    base = _league_base_for_fixture(fixture, league_baselines)
    m1 = base["m1h"]
    lam_base = -math.log(max(1e-9, 1.0 - m1))
    m2 = 1.0 - math.exp(-lam_base) * (1.0 + lam_base)  # P(N>=2)

    # TEAM PRIOR (na ≥2)
    home_id = ((fixture.get('teams') or {}).get('home') or {}).get('id')
    away_id = ((fixture.get('teams') or {}).get('away') or {}).get('id')

    def _w_over15(matches):
        return _weighted_match_over15_rate(matches, lam=5.0, max_n=15)

    p_home_raw, h_home, w_home = _w_over15(team_last_matches.get(home_id, []))
    p_away_raw, h_away, w_away = _w_over15(team_last_matches.get(away_id, []))
    p_home = beta_shrunk_rate(h_home, w_home, m=m2, tau=10.0) if p_home_raw is not None else m2
    p_away = beta_shrunk_rate(h_away, w_away, m=m2, tau=10.0) if p_away_raw is not None else m2
    p_team_prior = (p_home + p_away) / 2.0

    # H2H prior (≥2)
    a, b = sorted([home_id, away_id]); key = f"{a}-{b}"
    p_h2h_raw, h_h2h, w_h2h = _weighted_h2h_over15_rate(h2h_results.get(key, []), lam=4.0, max_n=10)
    p_h2h = beta_shrunk_rate(h_h2h, w_h2h, m=m2, tau=14.0) if p_h2h_raw is not None else m2
    effn_h2h = (w_h2h or 0.0) * 0.35
    if (w_h2h or 0.0) < 2.0:
        effn_h2h = 0.0; p_h2h = m2

    p_prior_tmp, _ = fuse_probs_by_precision(
        p_team_prior, (w_home or 0.0) + (w_away or 0.0),
        p_h2h,        effn_h2h
    )

    # minute-bucket prior → iz p(≥1) do λ, pa p(≥2)
    p_min1, effn_min = _prior_from_minute_buckets(repo, fixture, no_api=no_api)
    if p_min1 is not None:
        lam_min = -math.log(max(1e-9, 1.0 - p_min1))
        p_min2  = 1.0 - math.exp(-lam_min) * (1.0 + lam_min)
        w_min = float(WEIGHTS.get("MINUTE_PRIOR_BLEND", 0.25))
        p_prior = (1.0 - w_min) * p_prior_tmp + w_min * p_min2
    else:
        p_prior = p_prior_tmp

    # blagi FTS/CS/Form adj ostavimo isti (malen)
    fts_cs_adj = _fts_cs_form_coach_adj(repo, fixture, no_api=no_api)
    prior_logit_adj = 0.8 * WEIGHTS.get("FTSCS_ADJ", 0.05) * fts_cs_adj
    p_prior = _inv_logit(_logit(p_prior) + prior_logit_adj)
    
    # --- FORM_ADJ i COACH_ADJ implementacija ---
    form_adj = _calculate_form_adjustment(fixture, team_last_matches, no_api=no_api)
    coach_adj = _calculate_coach_adjustment(fixture, no_api=no_api)
    
    # Dodaj u prior
    p_prior = _inv_logit(_logit(p_prior) + WEIGHTS.get("FORM_ADJ", 0.03) * form_adj)
    p_prior = _inv_logit(_logit(p_prior) + WEIGHTS.get("COACH_ADJ", 0.02) * coach_adj)
    
    # --- KRITIČNI FEATURE-I ---
    # Treba da uključimo 4 kritična feature-a u p_prior kalkulaciju
    feats_temp = matchup_features_enhanced(fixture, team_profiles, league_baselines, micro_db=micro_db, extras=extras)
    
    # 1. pace_da_total - tempo dangerous attacks
    pace_da_total = feats_temp.get("pace_da_total", 0.0)
    if pace_da_total > 0:
        base_da = _league_base_for_fixture(fixture, league_baselines).get("mu_da1h", 0.0)
        pace_da_z = _z(pace_da_total, base_da, max(1.0, base_da * 0.5)) if base_da > 0 else 0.0
        pace_da_adj = WEIGHTS.get("PACE_DA_ADJ", 0.04) * pace_da_z
        p_prior = _inv_logit(_logit(p_prior) + pace_da_adj)
    
    # 2. lineups_have - da li imamo lineup podatke (pozitivno)
    lineups_have = feats_temp.get("lineups_have", False)
    if lineups_have:
        lineups_have_adj = WEIGHTS.get("LINEUPS_HAVE_ADJ", 0.03)
        p_prior = _inv_logit(_logit(p_prior) + lineups_have_adj)
    
    # 3. lineups_fw_count - broj napadača (pozitivno)
    lineups_fw_count = feats_temp.get("lineups_fw_count")
    if lineups_fw_count is not None and lineups_fw_count > 0:
        # Normalizuj na 0-1 skalu (pretpostavljamo 1-4 napadača)
        fw_normalized = max(0.0, min(1.0, (lineups_fw_count - 1) / 3.0))
        lineups_fw_adj = WEIGHTS.get("LINEUPS_FW_ADJ", 0.02) * fw_normalized
        p_prior = _inv_logit(_logit(p_prior) + lineups_fw_adj)
    
    # 4. inj_count - broj povreda (negativno)
    inj_count = feats_temp.get("inj_count")
    if inj_count is not None and inj_count > 0:
        # Negativan uticaj - više povreda = manja verovatnoća
        inj_normalized = min(1.0, inj_count / 10.0)  # Normalizuj na 0-1
        inj_count_adj = -WEIGHTS.get("INJ_COUNT_ADJ", 0.02) * inj_normalized
        p_prior = _inv_logit(_logit(p_prior) + inj_count_adj)

    # MICRO: prvo p(≥1) kao u over05, pa u λ i p(≥2)
    feats = matchup_features_enhanced(fixture, team_profiles, league_baselines, micro_db=micro_db, extras=extras)
    pH, _ = predict_team_scores1h_enhanced(fixture, feats, league_baselines, team_strengths, side='home')
    pA, _ = predict_team_scores1h_enhanced(fixture, feats, league_baselines, team_strengths, side='away')

    pace_z = _z(feats.get('pace_sot_total'), (base["mu_sot1h"] or 0.0), (base["sd_sot1h"] or 1.0))
    rho_base = max(-0.05, min(0.20, 0.05 + 0.05 * pace_z))
    class_gap_abs = abs(feats.get('tier_gap_home') or 0.0)
    rho_factor = max(0.5, 1.0 - 0.20 * class_gap_abs)
    rho = rho_base * rho_factor

    no_goal_indep = (1.0 - pH) * (1.0 - pA)
    cov_term = rho * math.sqrt(max(0.0, pH*(1.0-pH)*pA*(1.0-pA)))
    p_ge1_micro = 1.0 - max(0.0, no_goal_indep - cov_term)
    p_ge1_micro = max(0.0, min(1.0, p_ge1_micro))

    lam_micro = -math.log(max(1e-9, 1.0 - p_ge1_micro))
    p_micro = 1.0 - math.exp(-lam_micro) * (1.0 + lam_micro)

    # precizije (kao i kod over05)
    home_id = ((fixture.get('teams') or {}).get('home') or {}).get('id')
    away_id = ((fixture.get('teams') or {}).get('away') or {}).get('id')
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

    if market_odds_over15_1h:
        p_final = blend_with_market(p_final, market_odds_over15_1h, alpha=ALPHA_MODEL)

    debug = {
        "prior_percent": round(p_prior*100,2),
        "micro_percent": round(p_micro*100,2),
        "merge_weight_micro": round(w_micro_share,3),
        "p_ge1_micro": round(p_ge1_micro*100,2),
        "rho": round(rho,3),
        
        # FORM_ADJ i COACH_ADJ
        "form_adj": form_adj,
        "coach_adj": coach_adj,
        
        # KRITIČNI FEATURE-I
        "pace_da_total": feats.get("pace_da_total"),
        "lineups_have": feats.get("lineups_have"),
        "lineups_fw_count": feats.get("lineups_fw_count"),
        "inj_count": feats.get("inj_count"),
    }
    if market_odds_over15_1h:
        debug["market_odds_over15_1h"] = market_odds_over15_1h
        debug["market_prob_over15_1h"] = round(1.0/float(market_odds_over15_1h), 4)

    return float(p_final * 100.0), debug

def _select_existing_fixture_ids(fixture_ids):
    ids = [int(x) for x in set(fixture_ids) if x is not None]
    if not ids:
        return set()
    existing = set()
    with DB_WRITE_LOCK:
        conn = get_db_connection()
        cur = conn.cursor()
        chunk = 900
        for i in range(0, len(ids), chunk):
            part = ids[i:i+chunk]
            placeholders = ",".join(["%s"] * len(part))
            cur.execute(f"SELECT fixture_id FROM match_statistics WHERE fixture_id IN ({placeholders})", tuple(part))
            for (fid,) in cur.fetchall():
                existing.add(int(fid))
        conn.close()
    return existing

def prewarm_statistics_cache(team_last_matches: dict[int, list], max_workers: int = 2) -> dict:
    """
    Za sve istorijske mečeve koji se pominju u team_last_matches:
      - pronađi koje statistike fale u match_statistics
      - povuci ih paralelno preko get_or_fetch_fixture_statistics (koji upisuje pod DB lock-om)
    Vraća mali rezime.
    """
    # 1) skupi sve fixture id-jeve
    all_fids = set()
    for matches in (team_last_matches or {}).values():
        for m in matches or []:
            fid = ((m.get("fixture") or {}).get("id"))
            if fid:
                all_fids.add(int(fid))

    # 2) šta već postoji?
    existing = _select_existing_fixture_ids(list(all_fids))
    missing = list(all_fids - existing)
    if not missing:
        return {"queued": 0, "fetched": 0}

    # 3) dovuci paralelno (thread-safe brojanje)
    fetched = 0
    errors = 0
    _cnt_lock = threading.Lock()

    def _pull(fid: int):
        nonlocal fetched, errors
        try:
            res = get_or_fetch_fixture_statistics(fid)
            if res is not None:
                with _cnt_lock:
                    fetched += 1
        except Exception:
            with _cnt_lock:
                errors += 1

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(_pull, fid) for fid in missing]
        for _ in as_completed(futures):
            pass

    return {"queued": len(missing), "fetched": fetched, "errors": errors}

# ------------------------- FINAL PIPELINE ---------------------------
def analyze_fixtures(start_date: datetime, end_date: datetime, from_hour=None, to_hour=None,
                     market: str = "1h_over05", no_api: bool = True,
                     odds_over05_1h: float | None = None,
                     odds_over15_1h: float | None = None,
                     odds_btts_1h: float | None = None,
                     preloaded_team_last: dict[int, list] | None = None,
                     preloaded_h2h: dict[str, list] | None = None,
                     preloaded_extras: dict[int, dict] | None = None):
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

    # preloaded (ako je prosleđeno)
    team_last_matches = dict(preloaded_team_last or {})
    missing_tids = [t for t in team_ids if t not in team_last_matches]
    for tid in missing_tids:
        team_last_matches[tid] = repo.get_team_history(
            tid, last_n=DAY_PREFETCH_LAST_N, no_api=no_api
        )

    h2h_results = dict(preloaded_h2h or {})
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
        if not isinstance(fixture, dict):
            fixture = _coerce_fixture_row_to_api_dict(fixture) or {}
        fid = int(((fixture.get('fixture') or {}).get('id') or 0))
        if not fid:
            continue
        extras = (preloaded_extras or {}).get(fid) or build_extras_for_fixture(fixture, no_api=no_api)

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
                league_baselines, team_strengths, team_profiles,
                extras=extras, no_api=no_api,
                market_odds_btts_1h=odds_btts_1h
            )
        elif market == "1h_over15":
            final_percent, debug = calculate_final_probability_over15(
                fixture, team_last_matches, h2h_results, micro_db,
                league_baselines, team_strengths, team_profiles,
                extras=extras, no_api=no_api,
                market_odds_over15_1h=odds_over15_1h
            )
        else:  # "1h_over05"
            final_percent, debug = calculate_final_probability(
                fixture, team_last_matches, h2h_results, micro_db,
                league_baselines, team_strengths, team_profiles,
                extras=extras, no_api=no_api,
                market_odds_over05_1h=odds_over05_1h
            )

        # (d) paket za UI
        results.append({
            "fixture_id": int((fixture.get('fixture') or {}).get('id')),
            "kickoff":    (fixture.get('fixture') or {}).get('date'),  # ISO datetime, npr. "2025-08-29T18:30:00+00:00"

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

@app.get("/api/global-loader-status")
async def api_global_loader_status():
    """API endpoint za proveru statusa globalnog loadera"""
    try:
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        
        # Prvo očisti zastarele job-ove (starije od 10 minuta)
        cur.execute("""
            UPDATE prepare_jobs 
            SET status = 'error', detail = 'Job timeout - automatically cancelled'
            WHERE status IN ('running', 'queued')
            AND created_at < DATE_SUB(NOW(), INTERVAL 10 MINUTE)
        """)
        if cur.rowcount > 0:
            print(f"🧹 [GLOBAL LOADER] Cleaned up {cur.rowcount} stale jobs")
        
        # Proveri da li postoji aktivan prepare job (ne stariji od 1 sata)
        cur.execute("""
            SELECT status, progress, detail, created_at
            FROM prepare_jobs 
            WHERE status IN ('running', 'queued')
            AND created_at > DATE_SUB(NOW(), INTERVAL 1 HOUR)
            ORDER BY created_at DESC 
            LIMIT 1
        """)
        
        job = cur.fetchone()
        conn.commit()
        conn.close()
        
        print(f"🔍 [GLOBAL LOADER STATUS] Found job: {job}")
        
        if job:
            result = {
                "active": True,
                "status": job["status"],
                "progress": job["progress"] or 0,
                "detail": job["detail"] or "Preparing analysis...",
                "started_at": job["created_at"].isoformat() if job["created_at"] else None
            }
            print(f"🔍 [GLOBAL LOADER STATUS] Returning active: {result}")
            return result
        else:
            print(f"🔍 [GLOBAL LOADER STATUS] No active job found, returning inactive")
            return {"active": False}
            
    except Exception as e:
        print(f"❌ [ERROR] Global loader status check failed: {e}")
        return {"active": False}

@app.get("/api/check-analysis-exists")
async def api_check_analysis_exists(request: Request):
    """
    Check if analysis already exists in database for a specific date.
    Query params: { "date": "YYYY-MM-DD" }
    """
    try:
        # Admin check - only klisaricf@gmail.com can access
        session_id = request.headers.get('Authorization', '').replace('Bearer ', '')
        if not session_id:
            return {"error": "Authentication required."}, 401
        
        session_data = get_session(session_id)
        if not session_data:
            return {"error": "Invalid session. Please log in again."}, 401
        
        user_email = session_data.get('email')
        if user_email != 'klisaricf@gmail.com':
            return {"error": "Access denied. Admin privileges required."}, 403
        
        # Get date parameter
        date_str = request.query_params.get("date")
        if not date_str:
            return {"error": "Date parameter is required"}, 400
        
        try:
            d_local = date.fromisoformat(date_str)
        except ValueError:
            return {"error": "Invalid date format. Use YYYY-MM-DD"}, 400
        
        # Check if analysis exists for this date
        start_dt, end_dt = _day_bounds_utc(d_local)
        
        # Check if we have fixtures for this day
        fixtures = _list_fixtures_for_day(d_local)
        has_fixtures = len(fixtures) > 0 if fixtures else False
        
        # Check if we have model outputs for this day
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Count model outputs for this day
        cur.execute("""
            SELECT COUNT(*) FROM model_outputs 
            WHERE fixture_id IN (
                SELECT id FROM fixtures 
                WHERE DATE(utc_datetime) = %s
            )
        """, (d_local.isoformat(),))
        model_outputs_count = cur.fetchone()[0] if cur.fetchone() else 0
        
        # Count analysis cache entries for this day
        cur.execute("""
            SELECT COUNT(*) FROM analysis_cache 
            WHERE created_at >= %s AND created_at < %s
        """, (start_dt.isoformat(), end_dt.isoformat()))
        cache_entries_count = cur.fetchone()[0] if cur.fetchone() else 0
        
        conn.close()
        
        # Determine if analysis is complete
        analysis_exists = has_fixtures and model_outputs_count > 0 and cache_entries_count > 0
        analysis_complete = analysis_exists and model_outputs_count >= len(fixtures) if fixtures else False
        
        return {
            "date": date_str,
            "analysis_exists": analysis_exists,
            "analysis_complete": analysis_complete,
            "fixtures_count": len(fixtures) if fixtures else 0,
            "model_outputs_count": model_outputs_count,
            "cache_entries_count": cache_entries_count,
            "needs_preparation": not analysis_complete
        }
        
    except Exception as e:
        print(f"❌ [ERROR] Check analysis exists failed: {e}")
        return {"error": "Failed to check analysis status"}, 500

@app.get("/api/users")
async def api_get_users(request: Request):
    """Get all registered users (admin only)."""
    print("🔍 [DEBUG] /api/users endpoint called")
    try:
        # Admin check - only klisaricf@gmail.com can access
        session_id = request.headers.get('Authorization', '').replace('Bearer ', '')
        print(f"🔍 [DEBUG] Session ID: {session_id[:20]}..." if session_id else "No session ID")
        
        if not session_id:
            print("❌ [DEBUG] No session ID provided")
            return {"error": "Authentication required."}, 401

        session_data = get_session(session_id)
        if not session_data:
            print("❌ [DEBUG] Invalid session data")
            return {"error": "Invalid session. Please log in again."}, 401

        user_email = session_data.get('email')
        print(f"🔍 [DEBUG] User email: {user_email}")
        
        if user_email != 'klisaricf@gmail.com':
            print("❌ [DEBUG] Access denied - not admin user")
            return {"error": "Access denied. Admin privileges required."}, 403
        
        # Get users from database
        print("🔍 [DEBUG] Getting users from database")
        conn = get_db_connection()
        cur = conn.cursor()

        # First, let's check if users table exists and has data
        cur.execute("SELECT COUNT(*) FROM users")
        user_count = cur.fetchone()[0]
        print(f"🔍 [DEBUG] Total users in database: {user_count}")

        # If no users, add a test user
        if user_count == 0:
            print("🔍 [DEBUG] No users found, adding test user")
            cur.execute("""
                INSERT INTO users (first_name, last_name, email, password_hash, created_at)
                VALUES (%s, %s, %s, %s, %s)
            """, ("Test", "User", "test@example.com", "dummy_hash", "2024-01-01 12:00:00"))
            conn.commit()
            print("✅ [DEBUG] Test user added")

        # Check what tables exist (MySQL syntax)
        cur.execute("SHOW TABLES")
        tables = cur.fetchall()
        print(f"🔍 [DEBUG] Available tables: {tables}")
        
        # Check if users table has the right structure
        cur.execute("DESCRIBE users")
        columns = cur.fetchall()
        print(f"🔍 [DEBUG] Users table columns: {columns}")

        # Get all users from the users table
        cur.execute("""
            SELECT first_name, last_name, email, created_at
            FROM users
            ORDER BY created_at DESC
        """)

        users = []
        rows = cur.fetchall()
        print(f"🔍 [DEBUG] Raw rows from database: {rows}")
        
        for row in rows:
            print(f"🔍 [DEBUG] User row: {row}")
            users.append({
                "first_name": row[0],
                "last_name": row[1],
                "email": row[2],
                "created_at": row[3]
            })

        conn.close()
        print(f"✅ [DEBUG] Found {len(users)} users")
        print(f"✅ [DEBUG] Users data: {users}")

        return {"users": users}
        
    except Exception as e:
        print(f"❌ [ERROR] Failed to get users: {e}")
        return {"error": "Failed to get users"}, 500

@app.get("/api/analyze")
async def api_analyze(request: Request):
    """
    INSTANT: vraća isključivo prekomputovane analize iz model_outputs / analysis_cache.
    Nikada ne računa i ne zove API (ANALYZE_PRECOMPUTED_ONLY = True).
    Ako nema prekomputovanog, vraća prazan niz + prepared=false (200 OK) ili 425 po želji.
    """
    try:
        q = request.query_params
        market   = (q.get("market") or "1h_over05").strip()
        fh       = q.get("from_hour")
        th       = q.get("to_hour")
        from_s   = q.get("from_date")
        to_s     = q.get("to_date")

        # default opseg: danas lokalno
        if from_s:
            from_date = datetime.fromisoformat(from_s)
        else:
            from_date = datetime.now(USER_TZ).replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc)

        if to_s:
            to_date = datetime.fromisoformat(to_s)
        else:
            to_date = datetime.now(USER_TZ).replace(hour=23, minute=59, second=59, microsecond=0).astimezone(timezone.utc)

        # 1) Probaj cache po paramima (ako postoji — instant)
        params = {
            "from_date": from_date.isoformat(),
            "to_date": to_date.isoformat(),
            "from_hour": int(fh) if fh not in (None, "", "null") else None,
            "to_hour":   int(th) if th not in (None, "", "null") else None,
            "market": market,
        }
        cache_key = _build_cache_key(params)
        hit = read_analysis_cache(cache_key)
        if hit is not None:
            return JSONResponse(content=hit, status_code=200)

        # 2) Nema cache? — pročitaj isključivo iz model_outputs (precomputed)
        results = read_precomputed_results(from_date, to_date, fh, th, market)

        prepared = len(results) > 0
        # Ako želiš da frontend zna da nije "prepared", vrati info-flagu
        return JSONResponse(status_code=200, content={
            "prepared": prepared,
            "results": results
        })

    except Exception as e:
        print("analyze error:", e)
        print(traceback.format_exc())
        return JSONResponse(status_code=500, content={"error": "analyze_failed", "detail": str(e)})

@app.get("/api/team-stats")
async def api_team_stats(request: Request):
    """
    Get team statistics for specific market and period.
    Returns top 10 teams with highest success rate for the given market.
    """
    try:
        q = request.query_params
        market = (q.get("market") or "gg1h").strip()
        fh = q.get("from_hour")
        th = q.get("to_hour")
        from_s = q.get("from_date")
        to_s = q.get("to_date")

        # Default range: today locally
        if from_s:
            from_date = datetime.fromisoformat(from_s)
        else:
            from_date = datetime.now(USER_TZ).replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc)

        if to_s:
            to_date = datetime.fromisoformat(to_s)
        else:
            to_date = datetime.now(USER_TZ).replace(hour=23, minute=59, second=59, microsecond=0).astimezone(timezone.utc)

        # Get team stats from database
        team_stats = get_team_stats_for_market(from_date, to_date, fh, th, market)
        
        return JSONResponse(status_code=200, content={
            "prepared": len(team_stats) > 0,
            "results": team_stats
        })

    except Exception as e:
        print("team-stats error:", e)
        print(traceback.format_exc())
        return JSONResponse(status_code=500, content={"error": "team_stats_failed", "detail": str(e)})

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
        c.drawString(60, y, f"{match['team1']}: {match['team1_percent']}% (Last {match.get('team1_total','%s')})")
        y -= 15
        c.drawString(60, y, f"{match['team2']}: {match['team2_percent']}% (Last {match.get('team2_total','%s')})")
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

def get_team_stats_for_market(from_date: datetime, to_date: datetime, fh: int, th: int, market: str) -> list:
    """
    Get team statistics for a specific market and period.
    Returns list of teams with their success rates.
    """
    try:
        # First, try to populate team_stats table if it's empty
        populate_team_stats_if_needed()
        
        conn = get_mysql_connection()
        cur = conn.cursor()
        
        # Map market to database column
        market_columns = {
            'gg1h': 'gg_1h_success_rate',
            '1h_over05': 'over05_1h_success_rate', 
            '1h_over15': 'over15_1h_success_rate',
            'ft_over15': 'over15_ft_success_rate',
            'ft_over25': 'over25_ft_success_rate',
            'ggft': 'gg_ft_success_rate',
            'gg3plus_ft': 'gg3plus_ft_success_rate',
            'x_ht': 'x_ht_success_rate'
        }
        
        success_rate_col = market_columns.get(market, 'gg_1h_success_rate')
        
        # Build query based on market type
        if market in ['gg1h', '1h_over05', '1h_over15', 'x_ht']:
            # First half markets
            query = f"""
                SELECT 
                    t.name as team_name,
                    l.name as league,
                    ts.{success_rate_col} as success_rate,
                    ts.gg_1h_total_matches as total_matches,
                    ts.gg_1h_successful_matches as successful_matches,
                    ts.avg_goals_scored
                FROM team_stats ts
                JOIN teams t ON ts.team_id = t.id
                JOIN leagues l ON ts.league_id = l.id
                WHERE ts.{success_rate_col} IS NOT NULL
                AND ts.{success_rate_col} > 0
                AND ts.gg_1h_total_matches >= 5
                ORDER BY ts.{success_rate_col} DESC
                LIMIT 10
            """
        else:
            # Full time markets
            query = f"""
                SELECT 
                    t.name as team_name,
                    l.name as league,
                    ts.{success_rate_col} as success_rate,
                    ts.gg_ft_total_matches as total_matches,
                    ts.gg_ft_successful_matches as successful_matches,
                    ts.avg_goals_scored
                FROM team_stats ts
                JOIN teams t ON ts.team_id = t.id
                JOIN leagues l ON ts.league_id = l.id
                WHERE ts.{success_rate_col} IS NOT NULL
                AND ts.{success_rate_col} > 0
                AND ts.gg_ft_total_matches >= 5
                ORDER BY ts.{success_rate_col} DESC
                LIMIT 10
            """
        
        cur.execute(query)
        results = cur.fetchall()
        conn.close()
        
        # Convert to list of dictionaries
        team_stats = []
        for row in results:
            team_stats.append({
                'team_name': row[0],
                'league': row[1], 
                'success_rate': float(row[2]) * 100,  # Convert to percentage
                'total_matches': row[3],
                'successful_matches': row[4],
                'avg_goals_scored': float(row[5]) if row[5] else None
            })
        
        return team_stats
        
    except Exception as e:
        print(f"Error getting team stats: {e}")
        return []

def populate_team_stats_if_needed():
    """
    Populate team_stats table with data from fixtures if it's empty.
    This is a simplified version that calculates basic statistics.
    """
    try:
        conn = get_mysql_connection()
        cur = conn.cursor()
        
        # Check if team_stats table has any data
        cur.execute("SELECT COUNT(*) FROM team_stats")
        count = cur.fetchone()[0]
        
        if count > 0:
            conn.close()
            return  # Already populated
        
        print("Populating team_stats table...")
        
        # Get all teams and leagues from fixtures
        cur.execute("""
            SELECT DISTINCT f.team_home_id, f.league_id, f.date
            FROM fixtures f
            WHERE f.stats_json IS NOT NULL
            UNION
            SELECT DISTINCT f.team_away_id, f.league_id, f.date
            FROM fixtures f
            WHERE f.stats_json IS NOT NULL
        """)
        
        team_league_pairs = cur.fetchall()
        
        for team_id, league_id, match_date in team_league_pairs:
            # Get season from date (simplified)
            season = match_date.year if match_date else 2024
            
            # Calculate basic stats for this team
            stats = calculate_team_basic_stats(team_id, league_id, season)
            
            if stats:
                # Insert or update team stats
                cur.execute("""
                    INSERT INTO team_stats (
                        team_id, league_id, season,
                        gg_1h_success_rate, gg_1h_total_matches, gg_1h_successful_matches,
                        over05_1h_success_rate, over05_1h_total_matches, over05_1h_successful_matches,
                        over15_1h_success_rate, over15_1h_total_matches, over15_1h_successful_matches,
                        over15_ft_success_rate, over15_ft_total_matches, over15_ft_successful_matches,
                        over25_ft_success_rate, over25_ft_total_matches, over25_ft_successful_matches,
                        gg_ft_success_rate, gg_ft_total_matches, gg_ft_successful_matches,
                        gg3plus_ft_success_rate, gg3plus_ft_total_matches, gg3plus_ft_successful_matches,
                        x_ht_success_rate, x_ht_total_matches, x_ht_successful_matches,
                        avg_goals_scored, avg_goals_conceded
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    ) ON DUPLICATE KEY UPDATE
                        gg_1h_success_rate = VALUES(gg_1h_success_rate),
                        gg_1h_total_matches = VALUES(gg_1h_total_matches),
                        gg_1h_successful_matches = VALUES(gg_1h_successful_matches),
                        over05_1h_success_rate = VALUES(over05_1h_success_rate),
                        over05_1h_total_matches = VALUES(over05_1h_total_matches),
                        over05_1h_successful_matches = VALUES(over05_1h_successful_matches),
                        over15_1h_success_rate = VALUES(over15_1h_success_rate),
                        over15_1h_total_matches = VALUES(over15_1h_total_matches),
                        over15_1h_successful_matches = VALUES(over15_1h_successful_matches),
                        over15_ft_success_rate = VALUES(over15_ft_success_rate),
                        over15_ft_total_matches = VALUES(over15_ft_total_matches),
                        over15_ft_successful_matches = VALUES(over15_ft_successful_matches),
                        over25_ft_success_rate = VALUES(over25_ft_success_rate),
                        over25_ft_total_matches = VALUES(over25_ft_total_matches),
                        over25_ft_successful_matches = VALUES(over25_ft_successful_matches),
                        gg_ft_success_rate = VALUES(gg_ft_success_rate),
                        gg_ft_total_matches = VALUES(gg_ft_total_matches),
                        gg_ft_successful_matches = VALUES(gg_ft_successful_matches),
                        gg3plus_ft_success_rate = VALUES(gg3plus_ft_success_rate),
                        gg3plus_ft_total_matches = VALUES(gg3plus_ft_total_matches),
                        gg3plus_ft_successful_matches = VALUES(gg3plus_ft_successful_matches),
                        x_ht_success_rate = VALUES(x_ht_success_rate),
                        x_ht_total_matches = VALUES(x_ht_total_matches),
                        x_ht_successful_matches = VALUES(x_ht_successful_matches),
                        avg_goals_scored = VALUES(avg_goals_scored),
                        avg_goals_conceded = VALUES(avg_goals_conceded)
                """, (
                    team_id, league_id, season,
                    stats.get('gg_1h_success_rate', 0), stats.get('gg_1h_total_matches', 0), stats.get('gg_1h_successful_matches', 0),
                    stats.get('over05_1h_success_rate', 0), stats.get('over05_1h_total_matches', 0), stats.get('over05_1h_successful_matches', 0),
                    stats.get('over15_1h_success_rate', 0), stats.get('over15_1h_total_matches', 0), stats.get('over15_1h_successful_matches', 0),
                    stats.get('over15_ft_success_rate', 0), stats.get('over15_ft_total_matches', 0), stats.get('over15_ft_successful_matches', 0),
                    stats.get('over25_ft_success_rate', 0), stats.get('over25_ft_total_matches', 0), stats.get('over25_ft_successful_matches', 0),
                    stats.get('gg_ft_success_rate', 0), stats.get('gg_ft_total_matches', 0), stats.get('gg_ft_successful_matches', 0),
                    stats.get('gg3plus_ft_success_rate', 0), stats.get('gg3plus_ft_total_matches', 0), stats.get('gg3plus_ft_successful_matches', 0),
                    stats.get('x_ht_success_rate', 0), stats.get('x_ht_total_matches', 0), stats.get('x_ht_successful_matches', 0),
                    stats.get('avg_goals_scored', 0), stats.get('avg_goals_conceded', 0)
                ))
        
        conn.commit()
        conn.close()
        print("Team stats table populated successfully!")
        
    except Exception as e:
        print(f"Error populating team stats: {e}")

def update_team_stats_for_teams(team_ids: set, fixtures: list):
    """
    Update team stats for specific teams playing today.
    This is more efficient than recalculating all teams.
    """
    try:
        conn = get_mysql_connection()
        cur = conn.cursor()
        
        # Get league info for teams
        team_leagues = {}
        for fixture in fixtures:
            teams = fixture.get('teams', {})
            home_team = teams.get('home', {})
            away_team = teams.get('away', {})
            league_id = fixture.get('league', {}).get('id')
            
            if home_team.get('id') and league_id:
                team_leagues[home_team['id']] = league_id
            if away_team.get('id') and league_id:
                team_leagues[away_team['id']] = league_id
        
        # Update stats for each team
        for team_id in team_ids:
            league_id = team_leagues.get(team_id)
            if not league_id:
                continue
                
            # Get season from current date
            season = datetime.now().year
            
            # Calculate stats for this team
            stats = calculate_team_basic_stats(team_id, league_id, season)
            
            if stats:
                # Update team stats
                cur.execute("""
                    INSERT INTO team_stats (
                        team_id, league_id, season,
                        gg_1h_success_rate, gg_1h_total_matches, gg_1h_successful_matches,
                        over05_1h_success_rate, over05_1h_total_matches, over05_1h_successful_matches,
                        over15_1h_success_rate, over15_1h_total_matches, over15_1h_successful_matches,
                        over15_ft_success_rate, over15_ft_total_matches, over15_ft_successful_matches,
                        over25_ft_success_rate, over25_ft_total_matches, over25_ft_successful_matches,
                        gg_ft_success_rate, gg_ft_total_matches, gg_ft_successful_matches,
                        gg3plus_ft_success_rate, gg3plus_ft_total_matches, gg3plus_ft_successful_matches,
                        x_ht_success_rate, x_ht_total_matches, x_ht_successful_matches,
                        avg_goals_scored, avg_goals_conceded
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    ) ON DUPLICATE KEY UPDATE
                        gg_1h_success_rate = VALUES(gg_1h_success_rate),
                        gg_1h_total_matches = VALUES(gg_1h_total_matches),
                        gg_1h_successful_matches = VALUES(gg_1h_successful_matches),
                        over05_1h_success_rate = VALUES(over05_1h_success_rate),
                        over05_1h_total_matches = VALUES(over05_1h_total_matches),
                        over05_1h_successful_matches = VALUES(over05_1h_successful_matches),
                        over15_1h_success_rate = VALUES(over15_1h_success_rate),
                        over15_1h_total_matches = VALUES(over15_1h_total_matches),
                        over15_1h_successful_matches = VALUES(over15_1h_successful_matches),
                        over15_ft_success_rate = VALUES(over15_ft_success_rate),
                        over15_ft_total_matches = VALUES(over15_ft_total_matches),
                        over15_ft_successful_matches = VALUES(over15_ft_successful_matches),
                        over25_ft_success_rate = VALUES(over25_ft_success_rate),
                        over25_ft_total_matches = VALUES(over25_ft_total_matches),
                        over25_ft_successful_matches = VALUES(over25_ft_successful_matches),
                        gg_ft_success_rate = VALUES(gg_ft_success_rate),
                        gg_ft_total_matches = VALUES(gg_ft_total_matches),
                        gg_ft_successful_matches = VALUES(gg_ft_successful_matches),
                        gg3plus_ft_success_rate = VALUES(gg3plus_ft_success_rate),
                        gg3plus_ft_total_matches = VALUES(gg3plus_ft_total_matches),
                        gg3plus_ft_successful_matches = VALUES(gg3plus_ft_successful_matches),
                        x_ht_success_rate = VALUES(x_ht_success_rate),
                        x_ht_total_matches = VALUES(x_ht_total_matches),
                        x_ht_successful_matches = VALUES(x_ht_successful_matches),
                        avg_goals_scored = VALUES(avg_goals_scored),
                        avg_goals_conceded = VALUES(avg_goals_conceded),
                        updated_at = CURRENT_TIMESTAMP
                """, (
                    team_id, league_id, season,
                    stats.get('gg_1h_success_rate', 0), stats.get('gg_1h_total_matches', 0), stats.get('gg_1h_successful_matches', 0),
                    stats.get('over05_1h_success_rate', 0), stats.get('over05_1h_total_matches', 0), stats.get('over05_1h_successful_matches', 0),
                    stats.get('over15_1h_success_rate', 0), stats.get('over15_1h_total_matches', 0), stats.get('over15_1h_successful_matches', 0),
                    stats.get('over15_ft_success_rate', 0), stats.get('over15_ft_total_matches', 0), stats.get('over15_ft_successful_matches', 0),
                    stats.get('over25_ft_success_rate', 0), stats.get('over25_ft_total_matches', 0), stats.get('over25_ft_successful_matches', 0),
                    stats.get('gg_ft_success_rate', 0), stats.get('gg_ft_total_matches', 0), stats.get('gg_ft_successful_matches', 0),
                    stats.get('gg3plus_ft_success_rate', 0), stats.get('gg3plus_ft_total_matches', 0), stats.get('gg3plus_ft_successful_matches', 0),
                    stats.get('x_ht_success_rate', 0), stats.get('x_ht_total_matches', 0), stats.get('x_ht_successful_matches', 0),
                    stats.get('avg_goals_scored', 0), stats.get('avg_goals_conceded', 0)
                ))
        
        conn.commit()
        conn.close()
        print(f"Updated team stats for {len(team_ids)} teams")
        
    except Exception as e:
        print(f"Error updating team stats for teams: {e}")

def calculate_team_basic_stats(team_id: int, league_id: int, season: int) -> dict:
    """
    Calculate basic team statistics from fixtures data.
    This is a simplified version for demonstration.
    """
    try:
        conn = get_mysql_connection()
        cur = conn.cursor()
        
        # Get team's matches
        cur.execute("""
            SELECT f.id, f.stats_json, f.fixture_json
            FROM fixtures f
            WHERE (f.team_home_id = %s OR f.team_away_id = %s)
            AND f.stats_json IS NOT NULL
            AND f.fixture_json IS NOT NULL
            ORDER BY f.date DESC
            LIMIT 50
        """, (team_id, team_id))
        
        matches = cur.fetchall()
        conn.close()
        
        if not matches:
            return None
        
        # Initialize counters
        stats = {
            'gg_1h_total_matches': 0,
            'gg_1h_successful_matches': 0,
            'over05_1h_total_matches': 0,
            'over05_1h_successful_matches': 0,
            'over15_1h_total_matches': 0,
            'over15_1h_successful_matches': 0,
            'over15_ft_total_matches': 0,
            'over15_ft_successful_matches': 0,
            'over25_ft_total_matches': 0,
            'over25_ft_successful_matches': 0,
            'gg_ft_total_matches': 0,
            'gg_ft_successful_matches': 0,
            'gg3plus_ft_total_matches': 0,
            'gg3plus_ft_successful_matches': 0,
            'x_ht_total_matches': 0,
            'x_ht_successful_matches': 0,
            'total_goals_scored': 0,
            'total_goals_conceded': 0,
            'total_matches': 0
        }
        
        for fixture_id, stats_json, fixture_json in matches:
            try:
                stats_data = json.loads(stats_json) if isinstance(stats_json, str) else stats_json
                fixture_data = json.loads(fixture_json) if isinstance(fixture_json, str) else fixture_json
                
                if not stats_data or not fixture_data:
                    continue
                
                # Determine if team is home or away
                teams = fixture_data.get('teams', {})
                home_team_id = teams.get('home', {}).get('id')
                away_team_id = teams.get('away', {}).get('id')
                
                if team_id not in [home_team_id, away_team_id]:
                    continue
                
                is_home = team_id == home_team_id
                
                # Get goals
                goals = fixture_data.get('goals', {})
                home_goals = goals.get('home', 0) or 0
                away_goals = goals.get('away', 0) or 0
                
                team_goals = home_goals if is_home else away_goals
                opponent_goals = away_goals if is_home else home_goals
                
                stats['total_goals_scored'] += team_goals
                stats['total_goals_conceded'] += opponent_goals
                stats['total_matches'] += 1
                
                # First half goals (simplified - assume half of total goals)
                team_1h_goals = team_goals // 2
                opponent_1h_goals = opponent_goals // 2
                
                # Check various markets
                # GG 1H
                stats['gg_1h_total_matches'] += 1
                if team_1h_goals > 0 and opponent_1h_goals > 0:
                    stats['gg_1h_successful_matches'] += 1
                
                # Over 0.5 1H
                stats['over05_1h_total_matches'] += 1
                if team_1h_goals + opponent_1h_goals > 0:
                    stats['over05_1h_successful_matches'] += 1
                
                # Over 1.5 1H
                stats['over15_1h_total_matches'] += 1
                if team_1h_goals + opponent_1h_goals > 1:
                    stats['over15_1h_successful_matches'] += 1
                
                # Over 1.5 FT
                stats['over15_ft_total_matches'] += 1
                if team_goals + opponent_goals > 1:
                    stats['over15_ft_successful_matches'] += 1
                
                # Over 2.5 FT
                stats['over25_ft_total_matches'] += 1
                if team_goals + opponent_goals > 2:
                    stats['over25_ft_successful_matches'] += 1
                
                # GG FT
                stats['gg_ft_total_matches'] += 1
                if team_goals > 0 and opponent_goals > 0:
                    stats['gg_ft_successful_matches'] += 1
                
                # GG3+ FT
                stats['gg3plus_ft_total_matches'] += 1
                if team_goals > 0 and opponent_goals > 0 and team_goals + opponent_goals >= 3:
                    stats['gg3plus_ft_successful_matches'] += 1
                
                # X HT (draw at half time)
                stats['x_ht_total_matches'] += 1
                if team_1h_goals == opponent_1h_goals:
                    stats['x_ht_successful_matches'] += 1
                
            except Exception as e:
                print(f"Error processing match {fixture_id}: {e}")
                continue
        
        # Calculate success rates
        for market in ['gg_1h', 'over05_1h', 'over15_1h', 'over15_ft', 'over25_ft', 'gg_ft', 'gg3plus_ft', 'x_ht']:
            total_key = f'{market}_total_matches'
            successful_key = f'{market}_successful_matches'
            success_rate_key = f'{market}_success_rate'
            
            if stats[total_key] > 0:
                stats[success_rate_key] = stats[successful_key] / stats[total_key]
            else:
                stats[success_rate_key] = 0
        
        # Calculate average goals
        if stats['total_matches'] > 0:
            stats['avg_goals_scored'] = stats['total_goals_scored'] / stats['total_matches']
            stats['avg_goals_conceded'] = stats['total_goals_conceded'] / stats['total_matches']
        else:
            stats['avg_goals_scored'] = 0
            stats['avg_goals_conceded'] = 0
        
        return stats
        
    except Exception as e:
        print(f"Error calculating team stats: {e}")
        return None
