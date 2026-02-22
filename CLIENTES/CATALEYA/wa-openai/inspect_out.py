import sqlite3
from collections import Counter

c = sqlite3.connect("db.sqlite")
cur = c.cursor()

rows = cur.execute("""
SELECT wa_peer, COUNT(1)
FROM messages
WHERE direction='out'
GROUP BY wa_peer
ORDER BY COUNT(1) DESC
""").fetchall()

print("OUT por wa_peer:")
for wa_peer, n in rows:
    print(wa_peer, n)

print("\nUltimos 10 OUT:")
rows2 = cur.execute("""
SELECT id, wa_peer, text, ts_utc
FROM messages
WHERE direction='out'
ORDER BY id DESC
LIMIT 10
""").fetchall()

for r in rows2:
    print(r[0], r[1], (r[2] or "")[:60].replace("\n"," "), r[3])

c.close()
