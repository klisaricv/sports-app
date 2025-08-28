# mysql_database.py
import os
from dotenv import load_dotenv
import mysql.connector
from mysql.connector import pooling

# čita /etc/statsfk.env koji već imaš
load_dotenv("/etc/statsfk.env")

required = ["DB_HOST","DB_PORT","DB_NAME","DB_USER","DB_PASS","DB_SSL_CA"]
missing = [k for k in required if not os.getenv(k)]
if missing:
    raise RuntimeError(f"Missing DB env vars: {', '.join(missing)}")

dbconfig = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT")),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASS"),
    "ssl_ca": os.getenv("DB_SSL_CA"),
}

_connection_pool = None 

def _load_env():
    """
    Učita konfiguraciju iz os.environ i iz /etc/statsfk.env (KEY=VALUE).
    Ignoriše prazne linije i komentare. Trimuje navodnike.
    """
    cfg = {}

    # 1) start od postojećeg env-a (ako si već export-ovao nešto)
    for k in (
        "DB_HOST","DB_PORT","DB_USER","DB_PASSWORD","DB_PASS","DB_NAME","DB_SSL_CA"
    ):
        v = os.environ.get(k)
        if v:
            cfg[k] = v

    # 2) dopuni iz .env fajla (ako postoji)
    try:
        with open(ENV_FILE, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k = k.strip()
                v = v.strip().strip('"').strip("'")
                # nemoj pregaziti već postavljene iz os.environ
                if k and v and k not in cfg:
                    cfg[k] = v
    except FileNotFoundError:
        pass

    # 3) validacija minimalnog skupa
    missing = []
    if not cfg.get("DB_HOST"): missing.append("DB_HOST")
    if not cfg.get("DB_PORT"): missing.append("DB_PORT")
    if not cfg.get("DB_USER"): missing.append("DB_USER")
    if not cfg.get("DB_NAME"): missing.append("DB_NAME")
    if not (cfg.get("DB_PASSWORD") or cfg.get("DB_PASS")):
        missing.append("DB_PASSWORD/DB_PASS")
    if missing:
        raise RuntimeError(f"Missing DB env vars: {', '.join(missing)}")

    return cfg
    
def _build_dbconfig():
    cfg = _load_env()
    host = cfg.get("DB_HOST") or cfg.get("MYSQL_HOST") or "127.0.0.1"
    user = cfg.get("DB_USER") or cfg.get("MYSQL_USER") or "root"
    password = cfg.get("DB_PASSWORD") or cfg.get("DB_PASS") or cfg.get("MYSQL_PASSWORD") or ""
    database = cfg.get("DB_NAME") or cfg.get("MYSQL_DATABASE") or "statsfk_db"
    port = int(cfg.get("DB_PORT") or cfg.get("MYSQL_PORT") or "3306")

    kw = dict(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        autocommit=False,
        ssl_disabled=False,   # SSL je obavezan (REQUIRED), ali bez verifikacije ako nemamo CA
    )

    # dodaj ssl_ca SAMO ako fajl stvarno postoji i nije prazan
    ssl_ca = cfg.get("DB_SSL_CA") or cfg.get("MYSQL_SSL_CA")
    if ssl_ca and os.path.isfile(ssl_ca) and os.path.getsize(ssl_ca) > 0:
        kw["ssl_ca"] = ssl_ca  # VERIFY_CA će važiti samo ako je CA stvaran

    return kw

def get_mysql_connection():
    global _connection_pool
    if _connection_pool is None:
        _connection_pool = pooling.MySQLConnectionPool(
            pool_name="statsfk_pool",
            pool_size=5,
            **_build_dbconfig()
        )
    return _connection_pool.get_connection()

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
    cur.execute("SELECT data FROM match_statistics WHERE fixture_id = %s", (fixture_id,))
    row = cur.fetchone()
    conn.close()
    if row and row[0]:
        try:
            # row[0] može biti str (JSON) ili dict, oba su ok
            return json.loads(row[0]) if isinstance(row[0], str) else row[0]
        except Exception:
            return None
    return None

def create_all_tables():
    """Kreira SVE tabele iz sqlite varijante, ali u MySQL-u."""
    conn = get_mysql_connection()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS fixtures (
        id BIGINT PRIMARY KEY,
        date DATE,
        league_id INT,
        team_home_id INT,
        team_away_id INT,
        stats_json JSON,
        fixture_json JSON,
        INDEX idx_fixtures_date (date),
        INDEX idx_fixtures_league (league_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS match_statistics (
        fixture_id BIGINT PRIMARY KEY,
        data JSON,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS team_history_cache (
        team_id INT NOT NULL,
        last_n INT NOT NULL,
        data JSON,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (team_id, last_n)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS h2h_cache (
        team1_id INT NOT NULL,
        team2_id INT NOT NULL,
        last_n INT NOT NULL,
        data JSON,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (team1_id, team2_id, last_n)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS team_matches (
        team_id INT NOT NULL,
        fixture_id BIGINT NOT NULL,
        data JSON,
        PRIMARY KEY (team_id, fixture_id),
        INDEX idx_team_matches_team (team_id),
        INDEX idx_team_matches_fixture (fixture_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS h2h_matches (
        team1_id INT NOT NULL,
        team2_id INT NOT NULL,
        fixture_id BIGINT NOT NULL,
        data JSON,
        PRIMARY KEY (team1_id, team2_id, fixture_id),
        INDEX idx_h2h_pair (team1_id, team2_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS league_baselines_store (
        id TINYINT PRIMARY KEY,
        data JSON,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS team_strengths_store (
        team_id INT PRIMARY KEY,
        data JSON,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS team_profiles_store (
        team_id INT PRIMARY KEY,
        data JSON,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS team_micro_form_store (
        team_id INT NOT NULL,
        side ENUM('home','away') NOT NULL,
        data JSON,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (team_id, side)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS fixture_extras_store (
        fixture_id BIGINT PRIMARY KEY,
        data JSON,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS venues_cache (
        venue_id INT PRIMARY KEY,
        data JSON,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS lineups_cache (
        fixture_id BIGINT PRIMARY KEY,
        data JSON,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS injuries_cache (
        fixture_id BIGINT PRIMARY KEY,
        data JSON,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS referee_cache (
        ref_name VARCHAR(128) NOT NULL,
        season INT NOT NULL,
        last_n INT NOT NULL,
        data JSON,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (ref_name, season, last_n)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS odds_cache (
        fixture_id BIGINT NOT NULL,
        market VARCHAR(64) NOT NULL,
        data JSON,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (fixture_id, market)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS team_stats_cache (
        team_id INT NOT NULL,
        league_id INT NOT NULL,
        season INT NOT NULL,
        data JSON,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (team_id, league_id, season)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    conn.commit()
    cur.close()
    conn.close()
