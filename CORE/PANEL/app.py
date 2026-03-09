import os
import json
import sqlite3
from datetime import datetime
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

DATABASE = os.getenv("DATABASE_PATH", "db.sqlite3")
CLIENT_ID = os.getenv("CLIENT_ID", "CATALEYA")


def db():
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    return conn


def now():
    return datetime.utcnow().isoformat()


def ensure_tables():
    conn = db()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        client_id TEXT,
        direction TEXT,
        wa_peer TEXT,
        name TEXT,
        text TEXT,
        msg_type TEXT,
        wa_msg_id TEXT,
        ts_utc TEXT,
        raw_json TEXT
    )
    """)

    conn.commit()
    conn.close()


ensure_tables()


@app.get("/health")
async def health():
    return {"ok": True}


@app.post("/webhook")
async def whatsapp_webhook(request: Request):

    data = await request.json()

    try:

        entry = data["entry"][0]
        changes = entry["changes"][0]
        value = changes["value"]

        messages = value.get("messages")

        if not messages:
            return {"ok": True}

        msg = messages[0]

        wa_peer = msg.get("from")
        msg_type = msg.get("type", "text")
        wa_msg_id = msg.get("id")

        text = ""
        if msg_type == "text":
            text = msg["text"]["body"]

        contact_name = ""
        contacts = value.get("contacts", [])
        if contacts:
            contact_name = contacts[0].get("profile", {}).get("name", "")

        conn = db()
        cur = conn.cursor()

        cur.execute("""
        INSERT INTO messages
        (client_id, direction, wa_peer, name, text, msg_type, wa_msg_id, ts_utc, raw_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            CLIENT_ID,
            "in",
            wa_peer,
            contact_name,
            text,
            msg_type,
            wa_msg_id,
            now(),
            json.dumps(msg)
        ))

        conn.commit()
        conn.close()

    except Exception as e:
        print("Webhook error:", e)

    return {"ok": True}


@app.get("/api/conversations")
async def conversations():

    conn = db()
    cur = conn.cursor()

    cur.execute("""
    SELECT wa_peer,
           MAX(ts_utc) as last_ts,
           MAX(name) as name,
           MAX(text) as last_text
    FROM messages
    WHERE client_id = ?
    GROUP BY wa_peer
    ORDER BY last_ts DESC
    """, (CLIENT_ID,))

    rows = cur.fetchall()
    conn.close()

    data = []
    for r in rows:
        data.append({
            "wa_peer": r["wa_peer"],
            "name": r["name"],
            "last_text": r["last_text"],
            "last_ts": r["last_ts"]
        })

    return {"conversations": data}


@app.get("/api/chat")
async def chat(wa_peer: str):

    conn = db()
    cur = conn.cursor()

    cur.execute("""
    SELECT *
    FROM messages
    WHERE client_id = ?
    AND wa_peer = ?
    ORDER BY ts_utc ASC
    """, (CLIENT_ID, wa_peer))

    rows = cur.fetchall()
    conn.close()

    messages = []
    for r in rows:
        messages.append(dict(r))

    return {"messages": messages}


@app.post("/api/send")
async def send(request: Request):

    body = await request.json()

    to = body.get("to")
    text = body.get("text")

    if not to or not text:
        raise HTTPException(400, "Missing to/text")

    conn = db()
    cur = conn.cursor()

    cur.execute("""
    INSERT INTO messages
    (client_id, direction, wa_peer, name, text, msg_type, ts_utc)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        CLIENT_ID,
        "out",
        to,
        "",
        text,
        "text",
        now()
    ))

    conn.commit()
    conn.close()

    return {"ok": True}


@app.post("/api/delete_conversation")
async def delete_conversation(request: Request):

    body = await request.json()
    wa_peer = body.get("wa_peer")

    if not wa_peer:
        raise HTTPException(400, "Missing wa_peer")

    conn = db()
    cur = conn.cursor()

    cur.execute("""
    DELETE FROM messages
    WHERE client_id = ?
    AND wa_peer = ?
    """, (CLIENT_ID, wa_peer))

    conn.commit()
    conn.close()

    return {"ok": True}
