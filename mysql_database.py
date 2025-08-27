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

connection_pool = pooling.MySQLConnectionPool(
    pool_name="statsfk_pool",
    pool_size=5,
    **dbconfig
)

def get_mysql_connection():
    return connection_pool.get_connection()

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