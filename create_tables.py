# import sqlite3

# def create_tables():
#     conn = sqlite3.connect("sports_analysis.db")
#     conn.execute("PRAGMA foreign_keys = ON;")
#     cursor = conn.cursor()

#     # Tabela fixtures (ako je nema već)
#     cursor.execute("""
#         CREATE TABLE IF NOT EXISTS fixtures (
#             id INTEGER PRIMARY KEY,
#             date TEXT NOT NULL,
#             league_id INTEGER NOT NULL,
#             team_home_id INTEGER NOT NULL,
#             team_away_id INTEGER NOT NULL,
#             stats_json TEXT,
#             fixture_json TEXT
#         );
#     """)

#     # Poslednji/mečevi tima: složeni ključ (team_id, fixture_id)
#     cursor.execute("""
#         CREATE TABLE IF NOT EXISTS team_matches (
#             team_id INTEGER NOT NULL,
#             fixture_id INTEGER NOT NULL,
#             data TEXT,
#             PRIMARY KEY (team_id, fixture_id),
#             FOREIGN KEY (fixture_id) REFERENCES fixtures(id) ON DELETE CASCADE
#         );
#     """)

#     # H2H mečevi: normalizovan par (team1_id < team2_id) + fixture_id u složenom ključu
#     cursor.execute("""
#         CREATE TABLE IF NOT EXISTS h2h_matches (
#             team1_id INTEGER NOT NULL,
#             team2_id INTEGER NOT NULL,
#             fixture_id INTEGER NOT NULL,
#             data TEXT,
#             CHECK (team1_id < team2_id),
#             PRIMARY KEY (team1_id, team2_id, fixture_id),
#             FOREIGN KEY (fixture_id) REFERENCES fixtures(id) ON DELETE CASCADE
#         );
#     """)

#     # Statistika meča: 1:1 sa fixture-om
#     cursor.execute("""
#         CREATE TABLE IF NOT EXISTS match_statistics (
#             fixture_id INTEGER PRIMARY KEY,
#             data TEXT,
#             FOREIGN KEY (fixture_id) REFERENCES fixtures(id) ON DELETE CASCADE
#         );
#     """)

#     # Indeksi za brže upite
#     cursor.execute("CREATE INDEX IF NOT EXISTS idx_team_matches_team ON team_matches(team_id);")
#     cursor.execute("CREATE INDEX IF NOT EXISTS idx_h2h_team1 ON h2h_matches(team1_id);")
#     cursor.execute("CREATE INDEX IF NOT EXISTS idx_h2h_team2 ON h2h_matches(team2_id);")

#     conn.commit()
#     conn.close()
#     print("✅ Sve tabele i indeksi su uspešno kreirani!")
