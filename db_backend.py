import os, json, threading
import mysql.connector

ENV_PATH = "/etc/statsfk.env"
DB_WRITE_LOCK = threading.RLock()

def _load_env(path=ENV_PATH):
    cfg = {}
    if os.path.exists(path):
        with open(path, "r") as f:
            for line in f:
                line=line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k,v = line.split("=",1)
                cfg[k.strip()] = v.strip()
    return cfg

def get_mysql_connection():
    cfg = _load_env()
    return mysql.connector.connect(
        host=cfg.get("MYSQL_HOST","127.0.0.1"),
        port=int(cfg.get("MYSQL_PORT","3306")),
        user=cfg.get("MYSQL_USER","root"),
        password=cfg.get("MYSQL_PASSWORD",""),
        database=cfg.get("MYSQL_DATABASE","statsfk_db"),
        autocommit=False
    )

def create_all_tables():
    conn = get_mysql_connection()
    cur = conn.cursor()

    # fixtures
    cur.execute("""
    CREATE TABLE IF NOT EXISTS fixtures(
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

    # match statistics cache
    cur.execute("""
    CREATE TABLE IF NOT EXISTS match_statistics(
        fixture_id BIGINT PRIMARY KEY,
        data JSON,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    # team history cache
    cur.execute("""
    CREATE TABLE IF NOT EXISTS team_history_cache(
        team_id INT NOT NULL,
        last_n INT NOT NULL,
        data JSON,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY(team_id, last_n)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    # h2h cache
    cur.execute("""
    CREATE TABLE IF NOT EXISTS h2h_cache(
        team1_id INT NOT NULL,
        team2_id INT NOT NULL,
        last_n INT NOT NULL,
        data JSON,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY(team1_id, team2_id, last_n)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    # team_matches
    cur.execute("""
    CREATE TABLE IF NOT EXISTS team_matches(
        team_id INT NOT NULL,
        fixture_id BIGINT NOT NULL,
        data JSON,
        PRIMARY KEY(team_id, fixture_id),
        INDEX idx_team_matches_team (team_id),
        INDEX idx_team_matches_fixture (fixture_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    # h2h_matches
    cur.execute("""
    CREATE TABLE IF NOT EXISTS h2h_matches(
        team1_id INT NOT NULL,
        team2_id INT NOT NULL,
        fixture_id BIGINT NOT NULL,
        data JSON,
        PRIMARY KEY(team1_id, team2_id, fixture_id),
        INDEX idx_h2h_pair (team1_id, team2_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    # league_baselines_store
    cur.execute("""
    CREATE TABLE IF NOT EXISTS league_baselines_store(
        id INT PRIMARY KEY,
        data JSON,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    # team_strengths_store
    cur.execute("""
    CREATE TABLE IF NOT EXISTS team_strengths_store(
        team_id INT PRIMARY KEY,
        data JSON,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    # team_profiles_store
    cur.execute("""
    CREATE TABLE IF NOT EXISTS team_profiles_store(
        team_id INT PRIMARY KEY,
        data JSON,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    # team_micro_form_store
    cur.execute("""
    CREATE TABLE IF NOT EXISTS team_micro_form_store(
        team_id INT NOT NULL,
        side ENUM('home','away') NOT NULL,
        data JSON,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY(team_id, side)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    # fixture_extras_store
    cur.execute("""
    CREATE TABLE IF NOT EXISTS fixture_extras_store(
        fixture_id BIGINT PRIMARY KEY,
        data JSON,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    # venues_cache
    cur.execute("""
    CREATE TABLE IF NOT EXISTS venues_cache(
        venue_id INT PRIMARY KEY,
        data JSON,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    # lineups_cache
    cur.execute("""
    CREATE TABLE IF NOT EXISTS lineups_cache(
        fixture_id BIGINT PRIMARY KEY,
        data JSON,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    # injuries_cache
    cur.execute("""
    CREATE TABLE IF NOT EXISTS injuries_cache(
        fixture_id BIGINT PRIMARY KEY,
        data JSON,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    # referee_cache
    cur.execute("""
    CREATE TABLE IF NOT EXISTS referee_cache(
        ref_name VARCHAR(255) NOT NULL,
        season INT NOT NULL,
        last_n INT NOT NULL,
        data JSON,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (ref_name, season, last_n)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    # odds_cache
    cur.execute("""
    CREATE TABLE IF NOT EXISTS odds_cache(
        fixture_id BIGINT NOT NULL,
        market VARCHAR(128) NOT NULL,
        data JSON,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (fixture_id, market)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    # team_stats_cache
    cur.execute("""
    CREATE TABLE IF NOT EXISTS team_stats_cache(
        team_id INT NOT NULL,
        league_id INT NOT NULL,
        season INT NOT NULL,
        data JSON,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (team_id, league_id, season)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    conn.commit()
    conn.close()

# ---------------- INSERT HELPERS ----------------

def insert_team_matches(team_id, matches):
    if not matches:
        return
    with DB_WRITE_LOCK:
        conn = get_mysql_connection()
        cur = conn.cursor()
        for match in matches:
            fid = (match.get("fixture") or {}).get("id")
            if not fid:
                continue
            data_json = json.dumps(match, ensure_ascii=False, default=str)
            cur.execute("""
                INSERT INTO team_matches (team_id, fixture_id, data)
                VALUES (%s, %s, CAST(%s AS JSON))
                ON DUPLICATE KEY UPDATE data=VALUES(data)
            """, (team_id, fid, data_json))
        conn.commit()
        conn.close()

def insert_match_statistics(fixture_id: int, stats_data):
    data_json = json.dumps(stats_data, ensure_ascii=False, default=str) if stats_data else None
    with DB_WRITE_LOCK:
        conn = get_mysql_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO match_statistics(fixture_id, data, updated_at)
            VALUES (%s, CAST(%s AS JSON), NOW())
            ON DUPLICATE KEY UPDATE data=VALUES(data), updated_at=NOW()
        """, (fixture_id, data_json))
        conn.commit()
        conn.close()

def insert_h2h_matches(team1_id, team2_id, matches):
    if not matches:
        return
    a, b = sorted([team1_id, team2_id])
    with DB_WRITE_LOCK:
        conn = get_mysql_connection()
        cur = conn.cursor()
        for match in matches:
            fid = (match.get("fixture") or {}).get("id")
            if not fid:
                continue
            data_json = json.dumps(match, ensure_ascii=False, default=str)
            cur.execute("""
                INSERT INTO h2h_matches (team1_id, team2_id, fixture_id, data)
                VALUES (%s, %s, %s, CAST(%s AS JSON))
                ON DUPLICATE KEY UPDATE data=VALUES(data)
            """, (a, b, fid, data_json))
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
            return json.loads(row[0])
        except Exception:
            return None
    return None
