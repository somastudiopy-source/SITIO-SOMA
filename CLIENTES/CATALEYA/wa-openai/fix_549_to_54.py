import sqlite3

conn = sqlite3.connect("db.sqlite")
cur = conn.cursor()

# 549 + area+numero  -> 54 + area+numero (quita el '9' después del 54)
cur.execute("""
UPDATE messages
SET wa_peer = '54' || substr(wa_peer, 4)
WHERE wa_peer LIKE '549%';
""")

conn.commit()
print("OK, filas actualizadas:", cur.rowcount)

conn.close()
