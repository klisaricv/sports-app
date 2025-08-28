# db_backend.py — MySQL shim za backend
import threading, json

from mysql_database import (
    get_mysql_connection,
    create_all_tables as _create_all_tables,
    insert_team_matches as _ins_tm,
    insert_h2h_matches as _ins_h2h,
    try_read_fixture_statistics as _try_read_stats,
)

# Pokušaj da uvezeš insert_match_statistics iz mysql_database; ako ga nema, uradi fallback
try:
    from mysql_database import insert_match_statistics as _ins_match_stats
except Exception:
    _ins_match_stats = None

DB_WRITE_LOCK = threading.RLock()

def get_connection():
    """Vrati MySQL konekciju (iz pool-a)."""
    return get_mysql_connection()

def create_all_tables():
    """Kreiraj/alter tabele u MySQL-u (delegirano)."""
    return _create_all_tables()

def insert_team_matches(team_id: int, matches: list):
    return _ins_tm(team_id, matches)

def insert_h2h_matches(a: int, b: int, matches: list):
    return _ins_h2h(a, b, matches)

def try_read_fixture_statistics(fixture_id: int):
    return _try_read_stats(fixture_id)

def insert_match_statistics(fixture_id: int, stats_data):
    """
    Ako mysql_database ima svoju implementaciju, koristi nju.
    U suprotnom — minimalni fallback upsert u MySQL.
    """
    if _ins_match_stats is not None:
        return _ins_match_stats(fixture_id, stats_data)

    conn = get_connection()
    cur = conn.cursor()
    data_json = json.dumps(stats_data, ensure_ascii=False, default=str) if stats_data else None
    # upsert
    cur.execute(
        """
        INSERT INTO match_statistics (fixture_id, data, updated_at)
        VALUES (%s, %s, CURRENT_TIMESTAMP)
        ON DUPLICATE KEY UPDATE
            data = VALUES(data),
            updated_at = CURRENT_TIMESTAMP
        """,
        (fixture_id, data_json),
    )
    conn.commit()
    conn.close()
