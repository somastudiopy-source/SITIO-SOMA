import os
import sqlite3

db_path = os.environ.get("DB_PATH") or os.path.abspath("db.sqlite")
print("DB FILE:", db_path)

conn = sqlite3.connect(db_path)
cur = conn.cursor()

tables = cur.execute(
    "SELECT name FROM sqlite_master WHERE type='table'"
).fetchall()
print("TABLES:", tables)

try:
    count = cur.execute("SELECT COUNT(1) FROM messages").fetchone()[0]
    print("COUNT:", count)
except Exception as e:
    print("ERROR leyendo messages:", e)

conn.close()
