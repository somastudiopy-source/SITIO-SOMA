import os
import json
import hmac
import base64
import hashlib
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List

from dotenv import load_dotenv
from fastapi import FastAPI, Request, Form, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles

# WhatsApp Cloud API (Meta) - standard library HTTP
import urllib.request
import urllib.error


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

PANEL_SECRET = os.getenv("PANEL_SECRET", "dev_secret_change_me")
USERS_FILE = os.path.join(BASE_DIR, "users.json")

# WhatsApp defaults (optional; per-user in users.json overrides)
WA_GRAPH_VERSION = os.getenv("WA_GRAPH_VERSION", "v19.0")
WA_TOKEN_DEFAULT = os.getenv("WA_TOKEN", "")  # optional fallback
WA_PHONE_NUMBER_ID_DEFAULT = os.getenv("WA_PHONE_NUMBER_ID", "")  # optional fallback

app = FastAPI()

PANEL_STATIC_DIR = os.path.join(BASE_DIR, "panel_static")
os.makedirs(PANEL_STATIC_DIR, exist_ok=True)
app.mount("/panel-static", StaticFiles(directory=PANEL_STATIC_DIR), name="panel-static")


# ---------------- Users + Session ----------------
def load_users_data() -> Dict[str, Any]:
    # 1) Si existe variable de entorno USERS_JSON (Railway), usarla
    env_json = os.getenv("USERS_JSON", "").strip()
    if env_json:
        try:
            return json.loads(env_json)
        except Exception:
            return {"users": []}

    # 2) Si no, usar archivo local users.json
    if not os.path.exists(USERS_FILE):
        return {"users": []}

    with open(USERS_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def load_users() -> List[Dict[str, Any]]:
    data = load_users_data()
    return data.get("users", [])


def find_user(email: str) -> Optional[Dict[str, Any]]:
    email_norm = (email or "").strip().lower()
    for u in load_users():
        if (u.get("email") or "").strip().lower() == email_norm:
            return u
    return None


def get_calendar_config_for_session(session: Dict[str, Any]) -> Dict[str, str]:
    # Calendar config is stored in users.json (per-user).
    # For admin (or any user without a match), we fall back to defaults stored in users.json root.
    email = (session.get("email") or "").lower()
    role = session.get("role") or "user"

    user = find_user(email) if email else None

    data = load_users_data()
    defaults = {
        "calendar_id": data.get("default_calendar_id", "") or "",
        "calendar_tz": data.get("default_calendar_tz", "America/Argentina/Buenos_Aires") or "America/Argentina/Buenos_Aires",
    }

    if not user and role == "admin":
        # If admin logs in, prefer first user's config, else defaults.
        users = data.get("users", []) or []
        user = users[0] if users else None

    cal_id = (user or {}).get("calendar_id") or defaults["calendar_id"]
    cal_tz = (user or {}).get("calendar_tz") or defaults["calendar_tz"]
    return {"calendar_id": cal_id, "calendar_tz": cal_tz}


def sign(data: str) -> str:
    mac = hmac.new(PANEL_SECRET.encode("utf-8"), data.encode("utf-8"), hashlib.sha256).digest()
    return base64.urlsafe_b64encode(mac).decode("utf-8").rstrip("=")


def make_session(email: str, minutes: int = 60 * 24 * 7) -> str:
    exp = int((datetime.now(timezone.utc) + timedelta(minutes=minutes)).timestamp())
    payload = f"{email}|{exp}"
    sig = sign(payload)
    token = f"{payload}|{sig}"
    return base64.urlsafe_b64encode(token.encode("utf-8")).decode("utf-8")


def read_session(token_b64: str) -> Optional[str]:
    try:
        raw = base64.urlsafe_b64decode(token_b64.encode("utf-8")).decode("utf-8")
        parts = raw.split("|")
        if len(parts) != 3:
            return None
        email, exp_str, sig = parts
        payload = f"{email}|{exp_str}"
        if not hmac.compare_digest(sign(payload), sig):
            return None
        exp = int(exp_str)
        if int(datetime.now(timezone.utc).timestamp()) > exp:
            return None
        return email
    except Exception:
        return None


class RedirectToLogin(Exception):
    pass


@app.exception_handler(RedirectToLogin)
async def redirect_login_handler(request: Request, exc: RedirectToLogin):
    return RedirectResponse(url="/login", status_code=302)


def get_current_user(request: Request) -> Optional[Dict[str, Any]]:
    token = request.cookies.get("soma_session")
    if not token:
        return None
    email = read_session(token)
    if not email:
        return None
    return find_user(email)


def require_user(request: Request) -> Dict[str, Any]:
    u = get_current_user(request)
    if not u:
        raise RedirectToLogin()
    return u


# ---------------- WhatsApp (Meta Cloud API) ----------------
def get_whatsapp_config_for_user(u: Dict[str, Any]) -> Dict[str, str]:
    """
    Read WhatsApp Cloud API config from users.json (per user) with optional env fallbacks.
    Expected keys in users.json user:
      - wa_token
      - phone_number_id
    """
    token = (u.get("wa_token") or WA_TOKEN_DEFAULT or "").strip()
    phone_number_id = (u.get("phone_number_id") or WA_PHONE_NUMBER_ID_DEFAULT or "").strip()
    return {"wa_token": token, "phone_number_id": phone_number_id}


def wa_cloud_api_send_text(*, token: str, phone_number_id: str, to: str, body: str) -> Dict[str, Any]:
    """
    Sends a WhatsApp text message via Meta Cloud API.
    Returns dict:
      - ok: bool
      - status: int
      - data: response json (if any)
      - error: string (if any)
    """
    url = f"https://graph.facebook.com/{WA_GRAPH_VERSION}/{phone_number_id}/messages"
    payload = {
        "messaging_product": "whatsapp",
        "to": str(to),
        "type": "text",
        "text": {"body": body},
    }
    data = json.dumps(payload).encode("utf-8")

    req = urllib.request.Request(
        url=url,
        data=data,
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )

    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            try:
                parsed = json.loads(raw) if raw else {}
            except Exception:
                parsed = {"raw": raw}
            return {"ok": True, "status": int(getattr(resp, "status", 200)), "data": parsed}
    except urllib.error.HTTPError as e:
        raw = (e.read() or b"").decode("utf-8", errors="replace")
        try:
            parsed = json.loads(raw) if raw else {}
        except Exception:
            parsed = {"raw": raw}
        return {"ok": False, "status": int(getattr(e, "code", 500)), "error": "WHATSAPP_HTTP_ERROR", "data": parsed}
    except Exception as e:
        return {"ok": False, "status": 500, "error": f"WHATSAPP_NETWORK_ERROR: {e}"}


# ---------------- DB helpers ----------------
def db_connect(db_path: str) -> sqlite3.Connection:
    abs_path = os.path.abspath(os.path.join(BASE_DIR, db_path))
    con = sqlite3.connect(abs_path)
    con.row_factory = sqlite3.Row
    return con


def ensure_tables(con: sqlite3.Connection):
    con.execute("""
    CREATE TABLE IF NOT EXISTS messages (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      direction TEXT NOT NULL,           -- 'in' o 'out'
      wa_peer TEXT NOT NULL,
      name TEXT,
      text TEXT,
      msg_type TEXT,
      wa_msg_id TEXT,
      ts_utc TEXT NOT NULL,
      raw_json TEXT
    )
    """)
    con.execute("""
    CREATE TABLE IF NOT EXISTS conv_reads (
      user_email TEXT NOT NULL,
      wa_peer TEXT NOT NULL,
      last_read_id INTEGER DEFAULT 0,
      updated_utc TEXT,
      PRIMARY KEY (user_email, wa_peer)
    )
    """)
    con.commit()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_basename(name: str) -> str:
    return os.path.basename(name).replace("..", "").replace("\\", "").replace("/", "")


def client_media_abs(user: Dict[str, Any]) -> str:
    media_dir = user.get("media_dir", "")
    abs_media = os.path.abspath(os.path.join(BASE_DIR, media_dir))
    os.makedirs(abs_media, exist_ok=True)
    return abs_media


def conv_day_label(ts_iso: str) -> str:
    """
    WhatsApp-like label:
    - If today: HH:MM
    - If yesterday: AYER
    - Else: DD/MM/YYYY
    """
    if not ts_iso:
        return ""
    try:
        dt = datetime.fromisoformat(ts_iso.replace("Z", "+00:00"))
        local_dt = dt.astimezone()
        today = datetime.now().astimezone().date()
        d = local_dt.date()
        if d == today:
            return local_dt.strftime("%H:%M")
        if d == (today - timedelta(days=1)):
            return "AYER"
        return local_dt.strftime("%d/%m/%Y")
    except Exception:
        return ""


# ---------------- Pages ----------------
@app.get("/login", response_class=HTMLResponse)
def login_page():
    return HTMLResponse("""
<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>SOMA. | Login</title>
  <link rel="stylesheet" href="/panel-static/panel.css">
</head>
<body class="theme-dark">
  <header class="topbar glass">
    <div class="topbar-inner">
      <div class="brand">
        <img id="brandLogo"
             src="/panel-static/logo-soma-white.png"
             data-dark="/panel-static/logo-soma-white.png"
             data-light="/panel-static/logo-soma-dark.png"
             alt="SOMA." />
        <div>
          <div class="title">SOMA.</div>
          <div class="sub">Acceso al panel</div>
        </div>
      </div>
      <div class="actions">
        <button class="icon-btn" id="themeToggle" aria-label="Cambiar tema">
          <span id="themeIcon">☾</span>
        </button>
      </div>
    </div>
  </header>

  <div class="app center">
    <div class="glass auth-card">
      <div class="h1">Iniciar sesión</div>
      <div class="muted">Entrá con tu correo y contraseña.</div>

      <form method="post" action="/login" class="form">
        <label>Email</label>
        <input name="email" class="textbox" required />
        <label>Contraseña</label>
        <input name="password" type="password" class="textbox" required />
        <button class="send-btn" type="submit">Entrar</button>
      </form>

      <div class="tiny">Si no tenés acceso, pedilo a SOMA.</div>
    </div>
  </div>

  <!-- Calendar Modal -->
  <div class="modal" id="calModal" aria-hidden="true">
    <div class="modal-card glass">
      <div class="modal-head">
        <div class="modal-title"><img class="gcal-icon" src="/panel-static/google-calendar.png" alt="Google Calendar" /> Google Calendar</div>
        <div class="modal-actions">
          <button class="ghost-btn cal-tab-btn" id="calWeekBtn" type="button">Semana</button>
          <button class="ghost-btn cal-tab-btn" id="calMonthBtn" type="button">Mes</button>
          <button class="ghost-btn cal-add-toggle-btn" id="calAddToggleBtn" type="button">+ Evento</button>
          <button class="icon-btn" id="calCloseBtn" type="button" aria-label="Cerrar">✕</button>
        </div>
      </div>

      <div class="cal-add" id="calAdd" hidden>
        <div class="cal-add-row">
          <input id="calTitle" class="textbox" placeholder="Título del evento" />
          <input id="calDate" class="textbox" type="date" />
          <input id="calStartTime" class="textbox" type="time" />
          <input id="calEndTime" class="textbox" type="time" />
          <button class="send-btn cal-create-btn" id="calCreateBtn" type="button">Crear</button>
        </div>
        <div class="cal-add-row">
          <input id="calDetails" class="textbox" placeholder="Descripción (opcional)" />
        </div>
        <div class="muted tiny cal-hint">
          Se abrirá Google Calendar para guardar el evento en el calendario configurado.
        </div>
      </div>

      <iframe
        id="calFrame"
        class="cal-iframe"
        frameborder="0"
        scrolling="no"
        referrerpolicy="no-referrer-when-downgrade"
        loading="lazy"
        title="Google Calendar"
      ></iframe>
    </div>
  </div>

  <script src="/panel-static/panel.js"></script>
</body>
</html>
""")


@app.post("/login")
def login_action(email: str = Form(...), password: str = Form(...)):
    u = find_user(email)
    if not u or (u.get("password") != password):
        return HTMLResponse("<h3>Login incorrecto</h3><a href='/login'>Volver</a>", status_code=401)

    token = make_session(u["email"])
    resp = RedirectResponse(url="/", status_code=302)
    resp.set_cookie("soma_session", token, httponly=True, samesite="lax", max_age=60 * 60 * 24 * 7)
    return resp


@app.get("/logout")
def logout():
    resp = RedirectResponse(url="/login", status_code=302)
    resp.delete_cookie("soma_session")
    return resp


@app.get("/media-file")
def media_file(request: Request, file: str):
    u = require_user(request)
    abs_media = client_media_abs(u)
    safe = safe_basename(file)
    path = os.path.join(abs_media, safe)
    if not os.path.exists(path):
        return JSONResponse({"error": "not found"}, status_code=404)
    return FileResponse(path)


@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request):
    u = require_user(request)
    client_name = u.get("client_name", u.get("client_id", "Cliente"))

    return HTMLResponse(f"""
<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>SOMA. | Panel</title>
  <link rel="stylesheet" href="/panel-static/panel.css">
</head>
<body class="theme-dark">
  <header class="topbar glass">
    <div class="topbar-inner">
      <div class="brand">
        <img id="brandLogo"
             src="/panel-static/logo-soma-white.png"
             data-dark="/panel-static/logo-soma-white.png"
             data-light="/panel-static/logo-soma-dark.png"
             alt="SOMA." />
        <div>
          <div class="title">Panel SOMA.</div>
          <div class="sub">{client_name} • WhatsApp</div>
        </div>
      </div>

      <div class="actions">
        <button class="icon-btn" id="themeToggle" aria-label="Cambiar tema">
          <span id="themeIcon">☾</span>
        </button>
        <a class="link-pill" href="http://127.0.0.1:5500/">Sitio</a>
        <button class="link-pill calendar-pill" id="openCalendarBtn" type="button">
          <img class="gcal-icon" src="/panel-static/google-calendar.png" alt="Google Calendar" />
          Calendario
        </button>
        <a class="link-pill" href="/logout">Salir</a>
      </div>
    </div>
  </header>

  <div class="app">
    <div class="shell">
      <aside class="sidebar glass">
        <div class="sidebar-head">
          <div class="row">
            <div class="label">Chats</div>
            <div class="sync" id="sync">…</div>
          </div>
          <input id="q" class="search" placeholder="Buscar por número o nombre" />
        </div>
        <div class="conv-list" id="convs"></div>
        <div class="sidebar-foot">Enter envía • Shift+Enter salto ✅</div>
      </aside>

      <main class="chat glass">
        <div class="chat-head">
          <div>
            <div class="chat-title" id="chatTitle">Seleccioná un chat</div>
            <div class="chat-sub" id="chatSubtitle">—</div>
          </div>

          <div class="spacer"></div>

          <div class="find-wrap">
            <input id="chatSearch" class="chat-search" placeholder="Buscar en este chat…" />
            <button class="nav-btn" id="findUp" title="Anterior">▲</button>
            <button class="nav-btn" id="findDown" title="Siguiente">▼</button>
            <div class="find-count" id="findCount">0/0</div>
          </div>

          <button class="danger-btn" id="deleteChatBtn" disabled>Eliminar chat</button>
        </div>

        <div class="msgs" id="msgs">
          <div class="center-hint">Elegí una conversación para ver los mensajes.</div>
        </div>

        <div class="composer">
          <div class="composer-row">
            <button class="icon-action" id="emojiBtn" title="Emojis" disabled>😊</button>

            <textarea id="text" class="textbox textarea" placeholder="Escribí un mensaje… (emojis ✅)"></textarea>

            <input id="file" class="file-hidden" type="file" />
            <button class="icon-action" id="attachBtn" title="Adjuntar" disabled>📎</button>

            <button id="send" class="send-btn" disabled>Enviar</button>
          </div>

          <div class="emoji-panel glass" id="emojiPanel" style="display:none;"></div>

          <div class="file-chip" id="fileChip" style="display:none;">
            <span id="fileChipName"></span>
            <button class="chip-x" id="fileChipClear" title="Quitar">×</button>
          </div>

          <div class="result" id="sendResult"></div>
        </div>
      </main>
    </div>
  </div>

  <!-- Calendar Modal -->
  <div class="modal" id="calModal" aria-hidden="true">
    <div class="modal-card glass">
      <div class="modal-head">
        <div class="modal-title"><img class="gcal-icon" src="/panel-static/google-calendar.png" alt="Google Calendar" /> Google Calendar</div>
        <div class="modal-actions">
          <button class="ghost-btn cal-tab-btn" id="calWeekBtn" type="button">Semana</button>
          <button class="ghost-btn cal-tab-btn" id="calMonthBtn" type="button">Mes</button>
          <button class="ghost-btn cal-add-toggle-btn" id="calAddToggleBtn" type="button">+ Evento</button>
          <button class="icon-btn" id="calCloseBtn" type="button" aria-label="Cerrar">✕</button>
        </div>
      </div>

      <div class="cal-add" id="calAdd" hidden>
        <div class="cal-add-row">
          <input id="calTitle" class="textbox" placeholder="Título del evento" />
          <input id="calDate" class="textbox" type="date" />
          <input id="calStartTime" class="textbox" type="time" />
          <input id="calEndTime" class="textbox" type="time" />
          <button class="send-btn cal-create-btn" id="calCreateBtn" type="button">Crear</button>
        </div>
        <div class="cal-add-row">
          <input id="calDetails" class="textbox" placeholder="Descripción (opcional)" />
        </div>
        <div class="muted tiny cal-hint">
          Se abrirá Google Calendar para guardar el evento en el calendario configurado.
        </div>
      </div>

      <iframe
        id="calFrame"
        class="cal-iframe"
        frameborder="0"
        scrolling="no"
        referrerpolicy="no-referrer-when-downgrade"
        loading="lazy"
        title="Google Calendar"
      ></iframe>
    </div>
  </div>

  <script src="/panel-static/panel.js"></script>
</body>
</html>
""")


# ---------------- API ----------------
@app.get("/api/me")
def api_me(request: Request):
    session = get_current_user(request)
    if not session:
        return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)

    cal = get_calendar_config_for_session(session)
    return {
        "ok": True,
        "email": session.get("email"),
        "role": session.get("role"),
        "calendar_id": cal.get("calendar_id", ""),
        "calendar_tz": cal.get("calendar_tz", "America/Argentina/Buenos_Aires"),
    }


@app.get("/api/conversations")
def api_conversations(request: Request, limit: int = 300):
    u = require_user(request)
    email = u.get("email", "")

    con = db_connect(u["db_path"])
    ensure_tables(con)

    rows = con.execute("""
      SELECT wa_peer,
             MAX(id) as last_id,
             COALESCE(MAX(ts_utc), '') as last_ts,
             COALESCE((SELECT text FROM messages m2 WHERE m2.wa_peer = m.wa_peer ORDER BY id DESC LIMIT 1), '') as last_text,
             COALESCE((SELECT name FROM messages m2 WHERE m2.wa_peer = m.wa_peer ORDER BY id DESC LIMIT 1), '') as name
      FROM messages m
      GROUP BY wa_peer
      ORDER BY last_id DESC
      LIMIT ?
    """, (limit,)).fetchall()

    conversations = []
    for r in rows:
        last_read = con.execute("""
          SELECT last_read_id FROM conv_reads
          WHERE user_email = ? AND wa_peer = ?
        """, (email, r["wa_peer"])).fetchone()
        last_read_id = int(last_read["last_read_id"]) if last_read else 0

        unread_row = con.execute("""
          SELECT COUNT(*) as n
          FROM messages
          WHERE wa_peer = ?
            AND direction = 'in'
            AND id > ?
        """, (r["wa_peer"], last_read_id)).fetchone()
        unread = int(unread_row["n"] or 0)

        conversations.append({
            "wa_peer": r["wa_peer"],
            "name": (r["name"] or r["wa_peer"]),
            "last_id": int(r["last_id"] or 0),
            "last_ts": r["last_ts"],
            "day_label": conv_day_label(r["last_ts"]),
            "last_text": r["last_text"],
            "unread": unread
        })

    return {"conversations": conversations}


@app.get("/api/chat")
def api_chat(request: Request, wa_peer: str, limit: int = 50, before_id: Optional[int] = None):
    u = require_user(request)
    con = db_connect(u["db_path"])
    ensure_tables(con)

    where = "WHERE wa_peer = ?"
    params: List[Any] = [wa_peer]

    if before_id is not None:
        where += " AND id < ?"
        params.append(int(before_id))

    params.append(int(limit))

    rows = con.execute(f"""
      SELECT id, direction, wa_peer, name, text, msg_type, ts_utc, raw_json
      FROM messages
      {where}
      ORDER BY id DESC
      LIMIT ?
    """, tuple(params)).fetchall()

    msgs = []
    for r in reversed(list(rows)):
        media_url = None
        media_kind = None
        content_type = None

        try:
            raw = json.loads(r["raw_json"] or "{}")
        except Exception:
            raw = {}

        if isinstance(raw, dict) and isinstance(raw.get("media"), dict):
            filename = raw["media"].get("filename")
            media_kind = raw["media"].get("kind")
            content_type = raw["media"].get("content_type")
            if filename:
                media_url = f"/media-file?file={safe_basename(filename)}"

        msgs.append({
            "id": int(r["id"]),
            "direction": r["direction"],
            "wa_peer": r["wa_peer"],
            "name": r["name"] or "",
            "msg_type": r["msg_type"] or "text",
            "text": r["text"] or "",
            "ts_utc": r["ts_utc"],
            "media_url": media_url,
            "media_kind": media_kind,
            "content_type": content_type
        })

    has_more = False
    if rows:
        oldest = int(rows[-1]["id"])
        more_row = con.execute("SELECT 1 FROM messages WHERE wa_peer = ? AND id < ? LIMIT 1", (wa_peer, oldest)).fetchone()
        has_more = bool(more_row)

    return {"messages": msgs, "has_more": has_more}


@app.post("/api/mark_read")
async def api_mark_read(request: Request):
    u = require_user(request)
    email = u.get("email", "")

    con = db_connect(u["db_path"])
    ensure_tables(con)

    body = await request.json()
    wa_peer = body.get("wa_peer")
    if not wa_peer:
        return JSONResponse({"error": "missing wa_peer"}, status_code=400)

    last = con.execute("SELECT MAX(id) as m FROM messages WHERE wa_peer = ?", (wa_peer,)).fetchone()
    last_id = int(last["m"] or 0)

    con.execute("""
      INSERT INTO conv_reads (user_email, wa_peer, last_read_id, updated_utc)
      VALUES (?, ?, ?, ?)
      ON CONFLICT(user_email, wa_peer) DO UPDATE SET
        last_read_id=excluded.last_read_id,
        updated_utc=excluded.updated_utc
    """, (email, wa_peer, last_id, now_iso()))
    con.commit()

    return {"ok": True, "last_read_id": last_id}


@app.post("/api/upload")
async def api_upload(request: Request, file: UploadFile = File(...)):
    u = require_user(request)
    abs_media = client_media_abs(u)

    original = safe_basename(file.filename or "file.bin")
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{ts}_{original}"
    out_path = os.path.join(abs_media, filename)

    content = await file.read()
    with open(out_path, "wb") as f:
        f.write(content)

    return {
        "ok": True,
        "filename": filename,
        "url": f"/media-file?file={filename}",
        "content_type": file.content_type or "application/octet-stream"
    }


@app.post("/api/send")
async def api_send(request: Request):
    """
    Envía el mensaje:
      1) Lo guarda en la DB local (como ya hacía).
      2) Lo envía por WhatsApp Cloud API usando wa_token + phone_number_id del usuario (users.json).
    """
    u = require_user(request)
    con = db_connect(u["db_path"])
    ensure_tables(con)

    body = await request.json()
    to = body.get("to")
    text = (body.get("text") or "").strip()
    filename = body.get("filename")
    content_type = body.get("content_type") or ""

    if not to:
        return JSONResponse({"error": "missing to"}, status_code=400)
    if not text and not filename:
        return JSONResponse({"error": "missing text or file"}, status_code=400)

    msg_type = "text"
    raw = {}
    if filename:
        kind = "document"
        msg_type = "document"
        if (content_type or "").startswith("image/"):
            kind = "image"
            msg_type = "image"
        raw = {"media": {"filename": safe_basename(filename), "kind": kind, "content_type": content_type}}

    # 1) Guardar en DB (igual que antes)
    con.execute("""
      INSERT INTO messages (direction, wa_peer, name, text, msg_type, wa_msg_id, ts_utc, raw_json)
      VALUES ('out', ?, ?, ?, ?, NULL, ?, ?)
    """, (to, u.get("client_name", ""), text, msg_type, now_iso(), json.dumps(raw)))
    con.commit()

    # 2) Enviar por WhatsApp (por ahora, soporte seguro para TEXT)
    if filename:
        # Si querés que también mande adjuntos por WhatsApp Cloud API, hay que implementar:
        #   - upload media a /{phone_number_id}/media
        #   - luego enviar message con media_id
        # Esto lo dejamos listo para el próximo paso sin romper tu flujo actual.
        return {
            "ok": True,
            "warning": "MEDIA_NOT_SENT_YET",
            "detail": "El archivo se guardó en el panel, pero todavía no está implementado el envío de adjuntos por WhatsApp Cloud API.",
        }

    cfg = get_whatsapp_config_for_user(u)
    token = cfg.get("wa_token", "")
    phone_number_id = cfg.get("phone_number_id", "")

    if not token or not phone_number_id:
        # Esto es exactamente lo que te estaba pasando: “WHATSAPP_NOT_CONFIGURED”
        return JSONResponse(
            {"ok": False, "error": "WHATSAPP_NOT_CONFIGURED", "detail": "Falta wa_token o phone_number_id en users.json (o en variables de entorno)."},
            status_code=502,
        )

    wa_resp = wa_cloud_api_send_text(token=token, phone_number_id=phone_number_id, to=str(to), body=text)

    if not wa_resp.get("ok"):
        # devolvemos 502 con detalles para que lo veas en consola/panel
        return JSONResponse(
            {"ok": False, "error": wa_resp.get("error", "WHATSAPP_SEND_FAILED"), "status": wa_resp.get("status", 500), "data": wa_resp.get("data")},
            status_code=502,
        )

    return {"ok": True, "whatsapp": wa_resp.get("data", {})}


@app.post("/api/delete_conversation")
async def api_delete_conversation(request: Request):
    u = require_user(request)
    con = db_connect(u["db_path"])
    ensure_tables(con)

    body = await request.json()
    wa_peer = body.get("wa_peer")
    if not wa_peer:
        return JSONResponse({"error": "missing wa_peer"}, status_code=400)

    con.execute("DELETE FROM messages WHERE wa_peer = ?", (wa_peer,))
    con.execute("DELETE FROM conv_reads WHERE wa_peer = ?", (wa_peer,))
    con.commit()

    return {"ok": True

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

        wa_peer = msg["from"]
        text = msg.get("text", {}).get("body", "")
        msg_type = msg.get("type", "text")
        wa_msg_id = msg.get("id")

        users = load_users()
        if not users:
            return {"ok": False}

        u = users[0]

        con = db_connect(u["db_path"])
        ensure_tables(con)

        con.execute("""
        INSERT INTO messages
        (direction, wa_peer, name, text, msg_type, wa_msg_id, ts_utc, raw_json)
        VALUES ('in', ?, ?, ?, ?, ?, ?, ?)
        """, (
            wa_peer,
            value.get("contacts",[{}])[0].get("profile",{}).get("name",""),
            text,
            msg_type,
            wa_msg_id,
            now_iso(),
            json.dumps(msg)
        ))

        con.commit()

    except Exception as e:
        print("Webhook error:", e)

    return {"ok": True}
