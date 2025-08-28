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