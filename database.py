# database.py
from pathlib import Path
import sqlite3, json, threading, os

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "app.db"  # koristi jedan path konzistentno

DB_WRITE_LOCK = threading.RLock()

def get_db_connection():
    # check_same_thread=False je praktično za FastAPI + threadpool
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=8000;")
    return conn

def create_all_tables():
    with DB_WRITE_LOCK:
        conn = get_db_connection()
        cur = conn.cursor()

        # osnovna fixtures tabela
        cur.execute("""
        CREATE TABLE IF NOT EXISTS fixtures(
            id INTEGER PRIMARY KEY,
            date TEXT,
            league_id INTEGER,
            team_home_id INTEGER,
            team_away_id INTEGER,
            stats_json TEXT,
            fixture_json TEXT
        )
        """)

        # match statistics cache
        cur.execute("""
        CREATE TABLE IF NOT EXISTS match_statistics(
            fixture_id INTEGER PRIMARY KEY,
            data TEXT,
            updated_at TEXT
        )
        """)

        # istorija timova (keš)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS team_history_cache(
            team_id INTEGER,
            last_n INTEGER,
            data TEXT,
            updated_at TEXT,
            PRIMARY KEY(team_id, last_n)
        )
        """)

        # h2h cache
        cur.execute("""
        CREATE TABLE IF NOT EXISTS h2h_cache(
            team1_id INTEGER,
            team2_id INTEGER,
            last_n INTEGER,
            data TEXT,
            updated_at TEXT,
            PRIMARY KEY(team1_id, team2_id, last_n)
        )
        """)

        # sirovi "team_matches" (za brz list/slice)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS team_matches(
            team_id INTEGER,
            fixture_id INTEGER,
            data TEXT,
            PRIMARY KEY(team_id, fixture_id)
        )
        """)

        # sirovi "h2h_matches" (usklađeno sa insert_h2h_matches)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS h2h_matches(
            team1_id INTEGER,
            team2_id INTEGER,
            fixture_id INTEGER,
            data TEXT,
            PRIMARY KEY(team1_id, team2_id, fixture_id)
        )
        """)

        # === Artefakti analitike koje želiš da čuvaš ===

        # league baselines: jedan JSON blob (global + per-league)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS league_baselines_store(
            id INTEGER PRIMARY KEY CHECK(id=1),
            data TEXT,
            updated_at TEXT
        )
        """)

        # team_strengths: po timu
        cur.execute("""
        CREATE TABLE IF NOT EXISTS team_strengths_store(
            team_id INTEGER PRIMARY KEY,
            data TEXT,
            updated_at TEXT
        )
        """)

        # team_profiles (finish/leak/gk_stop/tier/pos itd.)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS team_profiles_store(
            team_id INTEGER PRIMARY KEY,
            data TEXT,
            updated_at TEXT
        )
        """)

        # micro form (po timu i strani)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS team_micro_form_store(
            team_id INTEGER,
            side TEXT CHECK(side IN ('home','away')),
            data TEXT,
            updated_at TEXT,
            PRIMARY KEY(team_id, side)
        )
        """)

        # extras per fixture (ref/weather/venue/lineups/injuries...)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS fixture_extras_store(
            fixture_id INTEGER PRIMARY KEY,
            data TEXT,
            updated_at TEXT
        )
        """)
                # ---- dodatne cache tabele koje koristi DataRepo ----
        cur.execute("""
        CREATE TABLE IF NOT EXISTS venues_cache(
            venue_id INTEGER PRIMARY KEY,
            data TEXT,
            updated_at TEXT
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS lineups_cache(
            fixture_id INTEGER PRIMARY KEY,
            data TEXT,
            updated_at TEXT
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS injuries_cache(
            fixture_id INTEGER PRIMARY KEY,
            data TEXT,
            updated_at TEXT
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS referee_cache(
            ref_name TEXT,
            season INTEGER,
            last_n INTEGER,
            data TEXT,
            updated_at TEXT,
            PRIMARY KEY (ref_name, season, last_n)
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS odds_cache(
            fixture_id INTEGER,
            market TEXT,
            data TEXT,
            updated_at TEXT,
            PRIMARY KEY (fixture_id, market)
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS team_stats_cache(
            team_id INTEGER,
            league_id INTEGER,
            season INTEGER,
            data TEXT,
            updated_at TEXT,
            PRIMARY KEY (team_id, league_id, season)
        )
        """)

        # ---- korisni indeksi za performanse ----
        cur.execute("CREATE INDEX IF NOT EXISTS idx_fixtures_date ON fixtures(date)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_fixtures_league ON fixtures(league_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_team_matches_team ON team_matches(team_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_team_matches_fixture ON team_matches(fixture_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_h2h_pair ON h2h_matches(team1_id, team2_id)")

        conn.commit()
        conn.close()

# ---------------- INSERT HELPERS ----------------

def insert_team_matches(team_id, matches):
    if not matches:
        return
    with DB_WRITE_LOCK:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("BEGIN IMMEDIATE;")
        for match in matches:
            fid = (match.get("fixture") or {}).get("id")
            if not fid:
                continue
            data_json = json.dumps(match, ensure_ascii=False, default=str)
            cur.execute("""
                INSERT OR REPLACE INTO team_matches (team_id, fixture_id, data)
                VALUES (?, ?, ?)
            """, (team_id, fid, data_json))
        conn.commit()
        conn.close()

def insert_match_statistics(fixture_id: int, stats_data):
    data_json = json.dumps(stats_data, ensure_ascii=False, default=str) if stats_data else None
    with DB_WRITE_LOCK:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("BEGIN IMMEDIATE;")
        cur.execute("""
            INSERT OR REPLACE INTO match_statistics(fixture_id, data, updated_at)
            VALUES (?, ?, datetime('now'))
        """, (fixture_id, data_json))
        conn.commit()
        conn.close()

def insert_h2h_matches(team1_id, team2_id, matches):
    if not matches:
        return
    a, b = sorted([team1_id, team2_id])
    with DB_WRITE_LOCK:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("BEGIN IMMEDIATE;")
        for match in matches:
            fid = (match.get("fixture") or {}).get("id")
            if not fid:
                continue
            data_json = json.dumps(match, ensure_ascii=False, default=str)
            cur.execute("""
                INSERT OR REPLACE INTO h2h_matches (team1_id, team2_id, fixture_id, data)
                VALUES (?, ?, ?, ?)
            """, (a, b, fid, data_json))
        conn.commit()
        conn.close()

def try_read_fixture_statistics(fixture_id: int):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT data FROM match_statistics WHERE fixture_id=?", (fixture_id,))
    row = cur.fetchone()
    conn.close()
    if row and row[0]:
        try:
            return json.loads(row[0])
        except Exception:
            return None
    return None

if __name__ == "__main__":
    create_all_tables()