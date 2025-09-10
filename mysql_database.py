import os
import json
from dotenv import load_dotenv
import mysql.connector
from mysql.connector import pooling

# čita /etc/statsfk.env (može da se promeni preko STATSFK_ENV_FILE)
ENV_FILE = os.getenv("STATSFK_ENV_FILE", "/etc/statsfk.env")
load_dotenv(ENV_FILE)  # ne diže grešku ako fajl ne postoji

_connection_pool = None

def _load_env():
    """
    Učita konfiguraciju iz os.environ i iz ENV_FILE (.env stil: KEY=VALUE).
    Ignoriše prazne linije i komentare. Trimuje navodnike.
    DB_SSL_CA je OPCIONO.
    Prihvata i DB_PASSWORD i DB_PASS.
    """
    cfg = {}

    # 1) Postojeći env
    for k in ("DB_HOST","DB_PORT","DB_USER","DB_PASSWORD","DB_PASS","DB_NAME","DB_SSL_CA","MYSQL_HOST","MYSQL_USER","MYSQL_PASSWORD","MYSQL_DATABASE","MYSQL_PORT","MYSQL_SSL_CA"):
        v = os.environ.get(k)
        if v:
            cfg[k] = v

    # 2) Dopuni iz fajla
    try:
        with open(ENV_FILE, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k = k.strip()
                v = v.strip().strip('"').strip("'")
                if k and v and k not in cfg:
                    cfg[k] = v
    except FileNotFoundError:
        pass

    # 3) Validacija minimuma (SSL CA opcionalno)
    missing = []
    for k in ("DB_HOST", "DB_PORT", "DB_USER", "DB_NAME"):
        if not cfg.get(k) and not cfg.get(k.replace("DB_", "MYSQL_")):
            missing.append(k)
    if not (cfg.get("DB_PASSWORD") or cfg.get("DB_PASS") or cfg.get("MYSQL_PASSWORD")):
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
    )

    # TLS: Ako imamo CA, verifikuj; ako nemamo, koristi TLS bez verifikacije
    ssl_ca = cfg.get("DB_SSL_CA") or cfg.get("MYSQL_SSL_CA")
    if ssl_ca and os.path.isfile(ssl_ca) and os.path.getsize(ssl_ca) > 0:
        kw["ssl_ca"] = ssl_ca
        # opcionalno eksplicitno:
        kw["ssl_verify_cert"] = True
        kw["ssl_disabled"] = False
    else:
        # bez CA: i dalje insistiraj na TLS, ali bez verifikacije
        # (ovo prolazi kada server forsira SSL)
        kw["ssl_disabled"] = False
        kw["ssl_verify_cert"] = False

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
    try:
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
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()

def insert_h2h_matches(a: int, b: int, matches: list):
    if not matches:
        return
    x, y = sorted([a, b])
    conn = get_mysql_connection()
    try:
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
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()

def try_read_fixture_statistics(fixture_id: int):
    conn = get_mysql_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT data FROM match_statistics WHERE fixture_id = %s", (fixture_id,))
        row = cur.fetchone()
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()

    if not row:
        return None
    val = row[0]
    try:
        if isinstance(val, (bytes, bytearray)):
            val = val.decode("utf-8", "ignore")
        if isinstance(val, str):
            return json.loads(val)
        # ako konektor vrati već dict/list
        if isinstance(val, (dict, list)):
            return val
    except Exception:
        return None
    return None

def create_all_tables():
    """Kreira SVE tabele iz sqlite varijante, ali u MySQL-u."""
    conn = get_mysql_connection()
    try:
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

        # Teams table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS teams (
            id INT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            country VARCHAR(100),
            logo VARCHAR(500),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_teams_name (name)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)

        # Leagues table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS leagues (
            id INT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            country VARCHAR(100),
            logo VARCHAR(500),
            type VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_leagues_name (name)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)

        # Team stats table for aggregated statistics
        cur.execute("""
        CREATE TABLE IF NOT EXISTS team_stats (
            team_id INT NOT NULL,
            league_id INT NOT NULL,
            season INT NOT NULL,
            gg_1h_success_rate DECIMAL(5,4) DEFAULT 0,
            gg_1h_total_matches INT DEFAULT 0,
            gg_1h_successful_matches INT DEFAULT 0,
            over05_1h_success_rate DECIMAL(5,4) DEFAULT 0,
            over05_1h_total_matches INT DEFAULT 0,
            over05_1h_successful_matches INT DEFAULT 0,
            over15_1h_success_rate DECIMAL(5,4) DEFAULT 0,
            over15_1h_total_matches INT DEFAULT 0,
            over15_1h_successful_matches INT DEFAULT 0,
            over15_ft_success_rate DECIMAL(5,4) DEFAULT 0,
            over15_ft_total_matches INT DEFAULT 0,
            over15_ft_successful_matches INT DEFAULT 0,
            over25_ft_success_rate DECIMAL(5,4) DEFAULT 0,
            over25_ft_total_matches INT DEFAULT 0,
            over25_ft_successful_matches INT DEFAULT 0,
            gg_ft_success_rate DECIMAL(5,4) DEFAULT 0,
            gg_ft_total_matches INT DEFAULT 0,
            gg_ft_successful_matches INT DEFAULT 0,
            gg3plus_ft_success_rate DECIMAL(5,4) DEFAULT 0,
            gg3plus_ft_total_matches INT DEFAULT 0,
            gg3plus_ft_successful_matches INT DEFAULT 0,
            x_ht_success_rate DECIMAL(5,4) DEFAULT 0,
            x_ht_total_matches INT DEFAULT 0,
            x_ht_successful_matches INT DEFAULT 0,
            avg_goals_scored DECIMAL(4,2) DEFAULT 0,
            avg_goals_conceded DECIMAL(4,2) DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (team_id, league_id, season),
            INDEX idx_team_stats_team (team_id),
            INDEX idx_team_stats_league (league_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)

        # Users table for authentication
        cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INT AUTO_INCREMENT PRIMARY KEY,
            email VARCHAR(255) UNIQUE NOT NULL,
            password_hash VARCHAR(255) NOT NULL,
            first_name VARCHAR(100) NOT NULL,
            last_name VARCHAR(100) NOT NULL,
            is_active BOOLEAN DEFAULT TRUE,
            is_admin BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            last_login TIMESTAMP NULL,
            INDEX idx_users_email (email),
            INDEX idx_users_active (is_active)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)

        # User sessions table for session management
        cur.execute("""
        CREATE TABLE IF NOT EXISTS user_sessions (
            id VARCHAR(255) PRIMARY KEY,
            user_id INT NOT NULL,
            expires_at TIMESTAMP NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_accessed TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            ip_address VARCHAR(45),
            user_agent TEXT,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
            INDEX idx_sessions_user (user_id),
            INDEX idx_sessions_expires (expires_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)

        # User preferences table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS user_preferences (
            user_id INT PRIMARY KEY,
            theme ENUM('light', 'dark', 'auto') DEFAULT 'auto',
            timezone VARCHAR(50) DEFAULT 'UTC',
            notifications BOOLEAN DEFAULT TRUE,
            email_notifications BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)

        conn.commit()
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()

# ====== USER AUTHENTICATION FUNCTIONS ======

def hash_password(password: str) -> str:
    """Hash password using bcrypt or similar."""
    import hashlib
    import secrets
    salt = secrets.token_hex(16)
    return hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), 100000).hex() + ':' + salt

def verify_password(password: str, password_hash: str) -> bool:
    """Verify password against hash."""
    import hashlib
    try:
        hash_part, salt = password_hash.split(':')
        return hash_part == hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), 100000).hex()
    except:
        return False

def create_user(email: str, password: str, first_name: str, last_name: str) -> dict:
    """Create a new user."""
    conn = get_mysql_connection()
    try:
        cur = conn.cursor(dictionary=True)
        
        # Check if user already exists
        cur.execute("SELECT id FROM users WHERE email = %s", (email,))
        if cur.fetchone():
            return {"success": False, "error": "User already exists"}
        
        # Hash password
        password_hash = hash_password(password)
        
        # Insert user
        cur.execute("""
            INSERT INTO users (email, password_hash, first_name, last_name)
            VALUES (%s, %s, %s, %s)
        """, (email, password_hash, first_name, last_name))
        
        user_id = cur.lastrowid
        
        # Create default preferences
        cur.execute("""
            INSERT INTO user_preferences (user_id)
            VALUES (%s)
        """, (user_id,))
        
        conn.commit()
        
        return {
            "success": True,
            "user": {
                "id": user_id,
                "email": email,
                "first_name": first_name,
                "last_name": last_name
            }
        }
    except Exception as e:
        conn.rollback()
        return {"success": False, "error": str(e)}
    finally:
        conn.close()

def authenticate_user(email: str, password: str) -> dict:
    """Authenticate user with email and password."""
    conn = get_mysql_connection()
    try:
        cur = conn.cursor(dictionary=True)
        
        # Get user
        cur.execute("""
            SELECT id, email, password_hash, first_name, last_name, is_active, is_admin
            FROM users WHERE email = %s
        """, (email,))
        
        user = cur.fetchone()
        if not user:
            return {"success": False, "error": "Invalid credentials"}
        
        if not user['is_active']:
            return {"success": False, "error": "Account is disabled"}
        
        # Verify password
        if not verify_password(password, user['password_hash']):
            return {"success": False, "error": "Invalid credentials"}
        
        # Update last login
        cur.execute("""
            UPDATE users SET last_login = CURRENT_TIMESTAMP WHERE id = %s
        """, (user['id'],))
        
        conn.commit()
        
        # Remove password hash from response
        del user['password_hash']
        
        return {
            "success": True,
            "user": user
        }
    except Exception as e:
        return {"success": False, "error": str(e)}
    finally:
        conn.close()

def create_session(user_id: int, session_id: str, expires_at: str, ip_address: str = None, user_agent: str = None) -> bool:
    """Create a new user session."""
    conn = get_mysql_connection()
    try:
        cur = conn.cursor()
        
        cur.execute("""
            INSERT INTO user_sessions (id, user_id, expires_at, ip_address, user_agent)
            VALUES (%s, %s, %s, %s, %s)
        """, (session_id, user_id, expires_at, ip_address, user_agent))
        
        conn.commit()
        return True
    except Exception as e:
        return False
    finally:
        conn.close()

def get_session(session_id: str) -> dict:
    """Get session by ID."""
    conn = get_mysql_connection()
    try:
        cur = conn.cursor(dictionary=True)
        
        cur.execute("""
            SELECT s.*, u.email, u.first_name, u.last_name, u.is_active, u.is_admin
            FROM user_sessions s
            JOIN users u ON s.user_id = u.id
            WHERE s.id = %s AND s.expires_at > CURRENT_TIMESTAMP
        """, (session_id,))
        
        session = cur.fetchone()
        if session:
            # Update last accessed
            cur.execute("""
                UPDATE user_sessions SET last_accessed = CURRENT_TIMESTAMP WHERE id = %s
            """, (session_id,))
            conn.commit()
        
        return session
    except Exception as e:
        return None
    finally:
        conn.close()

def delete_session(session_id: str) -> bool:
    """Delete a session."""
    conn = get_mysql_connection()
    try:
        cur = conn.cursor()
        
        cur.execute("DELETE FROM user_sessions WHERE id = %s", (session_id,))
        
        conn.commit()
        return cur.rowcount > 0
    except Exception as e:
        return False
    finally:
        conn.close()

def cleanup_expired_sessions() -> int:
    """Clean up expired sessions."""
    conn = get_mysql_connection()
    try:
        cur = conn.cursor()
        
        cur.execute("DELETE FROM user_sessions WHERE expires_at < CURRENT_TIMESTAMP")
        
        conn.commit()
        return cur.rowcount
    except Exception as e:
        return 0
    finally:
        conn.close()
