import sqlite3

conn = sqlite3.connect("db.sqlite")
cur = conn.cursor()

cur.execute("""
UPDATE messages
SET wa_peer = '54' || substr(wa_peer, 4)
WHERE wa_peer LIKE '549%';
""")

conn.commit()
print("OK, filas actualizadas:", cur.rowcount)

conn.close()
