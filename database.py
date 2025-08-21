# database.py
from pathlib import Path
import sqlite3, json, threading, os

BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.path.join(DATA_DIR, "sports_analysis.db")

DB_WRITE_LOCK = threading.RLock()

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=8000;")
    return conn

def create_tables():
    conn = get_db_connection()
    cur = conn.cursor()

    # === CORE ===
    cur.execute("""
        CREATE TABLE IF NOT EXISTS fixtures (
            id INTEGER PRIMARY KEY,
            date TEXT,
            league_id INTEGER,
            team_home_id INTEGER,
            team_away_id INTEGER,
            stats_json TEXT,
            fixture_json TEXT
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS team_matches (
            team_id INTEGER NOT NULL,
            fixture_id INTEGER NOT NULL,
            data TEXT,
            PRIMARY KEY (team_id, fixture_id)
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS h2h_matches (
            team1_id INTEGER NOT NULL,
            team2_id INTEGER NOT NULL,
            fixture_id INTEGER NOT NULL,
            data TEXT,
            CHECK (team1_id < team2_id),
            PRIMARY KEY (team1_id, team2_id, fixture_id)
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS match_statistics (
            fixture_id INTEGER PRIMARY KEY,
            data TEXT,
            updated_at TEXT
        );
    """)
    try:
        cur.execute("ALTER TABLE match_statistics ADD COLUMN updated_at TEXT;")
    except sqlite3.OperationalError:
        pass

    # === CACHES (da analiza bude 100% DB-only) ===
    cur.execute("""
        CREATE TABLE IF NOT EXISTS team_history_cache (
            team_id INTEGER,
            last_n INTEGER,
            data TEXT,
            updated_at TEXT,
            PRIMARY KEY (team_id, last_n)
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS h2h_cache (
            team1_id INTEGER,
            team2_id INTEGER,
            last_n INTEGER,
            data TEXT,
            updated_at TEXT,
            PRIMARY KEY (team1_id, team2_id, last_n)
        );
    """)

    # === ODDS (market-implied prior za 1H) ===
    cur.execute("""
        CREATE TABLE IF NOT EXISTS odds_cache (
            fixture_id INTEGER,
            market TEXT,               -- npr. 'OU_1H', 'BTTS_1H'
            data TEXT,                 -- raw JSON sa svih kladionica/marketima
            updated_at TEXT,
            PRIMARY KEY (fixture_id, market)
        );
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_odds_updated ON odds_cache(updated_at)")

    # (opciono) centralizuj keševe koje sada pravi DataRepo._cache_table dinamčki:
    cur.execute("""
        CREATE TABLE IF NOT EXISTS venues_cache (
            venue_id INTEGER PRIMARY KEY,
            data TEXT,
            updated_at TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lineups_cache (
            fixture_id INTEGER PRIMARY KEY,
            data TEXT,
            updated_at TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS injuries_cache (
            fixture_id INTEGER PRIMARY KEY,
            data TEXT,
            updated_at TEXT
        )
    """)
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

    # (opciono, za brže podizanje modela)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS league_baselines_cache (
            league_id INTEGER,
            season INTEGER,
            data TEXT,
            updated_at TEXT,
            PRIMARY KEY (league_id, season)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS team_strengths_cache (
            team_id INTEGER,
            season INTEGER,
            data TEXT,
            updated_at TEXT,
            PRIMARY KEY (team_id, season)
        )
    """)


    # === INDEXES za brzinu ===
    cur.execute("CREATE INDEX IF NOT EXISTS idx_fixtures_date ON fixtures(date)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_team_matches_team ON team_matches(team_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_team_matches_fixture ON team_matches(fixture_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_h2h_matches_pair ON h2h_matches(team1_id, team2_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_match_stats_updated ON match_statistics(updated_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_thc_team_n ON team_history_cache(team_id, last_n)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_h2hc_pair_n ON h2h_cache(team1_id, team2_id, last_n)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_fixtures_league ON fixtures(league_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_fixtures_home ON fixtures(team_home_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_fixtures_away ON fixtures(team_away_id)")


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

def create_all_tables():
    create_tables()

if __name__ == "__main__":
    create_all_tables()
