import sqlite3

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

c.close()
