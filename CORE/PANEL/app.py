import os
import json
import hmac
import base64
import hashlib
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List

from dotenv import load_dotenv
from fastapi import FastAPI, Request, Form, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, FileResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles

# WhatsApp Cloud API (Meta) - standard library HTTP
import urllib.request
import urllib.error

# PostgreSQL
import psycopg2
import psycopg2.extras
import mimetypes
import uuid

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

PANEL_SECRET = os.getenv("PANEL_SECRET", "dev_secret_change_me")
USERS_FILE = os.path.join(BASE_DIR, "users.json")
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "mi_token_123")

# DB URL pública / privada
DATABASE_URL = (
    os.getenv("PANEL_DATABASE_URL")
    or os.getenv("DATABASE_URL")
    or os.getenv("DB_URL")
    or ""
)

# WhatsApp defaults (optional; per-user in users.json overrides)
WA_GRAPH_VERSION = os.getenv("WA_GRAPH_VERSION", "v19.0")
WA_TOKEN_DEFAULT = os.getenv("WA_TOKEN", "")
WA_PHONE_NUMBER_ID_DEFAULT = os.getenv("WA_PHONE_NUMBER_ID", "")

app = FastAPI()

PANEL_STATIC_DIR = os.path.join(BASE_DIR, "panel_static")
os.makedirs(PANEL_STATIC_DIR, exist_ok=True)
app.mount("/panel-static", StaticFiles(directory=PANEL_STATIC_DIR), name="panel-static")


# ---------------- Users + Session ----------------
def load_users_data() -> Dict[str, Any]:
    env_json = os.getenv("USERS_JSON", "").strip()
    if env_json:
        try:
            return json.loads(env_json)
        except Exception:
            return {"users": []}

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
    email = (session.get("email") or "").lower()
    role = session.get("role") or "user"

    user = find_user(email) if email else None

    data = load_users_data()
    defaults = {
        "calendar_id": data.get("default_calendar_id", "") or "",
        "calendar_tz": data.get("default_calendar_tz", "America/Argentina/Buenos_Aires") or "America/Argentina/Buenos_Aires",
    }

    if not user and role == "admin":
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


def get_panel_client_id(u: Dict[str, Any]) -> str:
    return (u.get("client_id") or "").strip()


# ---------------- WhatsApp (Meta Cloud API) ----------------
def get_whatsapp_config_for_user(u: Dict[str, Any]) -> Dict[str, str]:
    token = (u.get("wa_token") or WA_TOKEN_DEFAULT or "").strip()
    phone_number_id = (u.get("phone_number_id") or WA_PHONE_NUMBER_ID_DEFAULT or "").strip()
    return {"wa_token": token, "phone_number_id": phone_number_id}


def wa_cloud_api_send_text(*, token: str, phone_number_id: str, to: str, body: str) -> Dict[str, Any]:
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


def guess_extension(content_type: str, fallback: str = ".bin") -> str:
    ext = mimetypes.guess_extension(content_type or "")
    return ext or fallback


def wa_cloud_api_upload_media(*, token: str, phone_number_id: str, file_path: str, content_type: str) -> Dict[str, Any]:
    url = f"https://graph.facebook.com/{WA_GRAPH_VERSION}/{phone_number_id}/media"

    boundary = f"----WebKitFormBoundary{uuid.uuid4().hex}"
    filename = os.path.basename(file_path)

    with open(file_path, "rb") as f:
        file_bytes = f.read()

    parts = []
    parts.append(
        (
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="messaging_product"\r\n\r\n'
            f"whatsapp\r\n"
        ).encode("utf-8")
    )
    parts.append(
        (
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="type"\r\n\r\n'
            f"{content_type}\r\n"
        ).encode("utf-8")
    )
    parts.append(
        (
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="file"; filename="{filename}"\r\n'
            f"Content-Type: {content_type}\r\n\r\n"
        ).encode("utf-8")
    )
    parts.append(file_bytes)
    parts.append(f"\r\n--{boundary}--\r\n".encode("utf-8"))

    data = b"".join(parts)

    req = urllib.request.Request(
        url=url,
        data=data,
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": f"multipart/form-data; boundary={boundary}",
        },
    )

    try:
        with urllib.request.urlopen(req, timeout=40) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            parsed = json.loads(raw) if raw else {}
            return {"ok": True, "status": int(getattr(resp, "status", 200)), "data": parsed}
    except urllib.error.HTTPError as e:
        raw = (e.read() or b"").decode("utf-8", errors="replace")
        try:
            parsed = json.loads(raw) if raw else {}
        except Exception:
            parsed = {"raw": raw}
        return {"ok": False, "status": int(getattr(e, "code", 500)), "error": "WHATSAPP_MEDIA_UPLOAD_ERROR", "data": parsed}
    except Exception as e:
        return {"ok": False, "status": 500, "error": f"WHATSAPP_MEDIA_UPLOAD_NETWORK_ERROR: {e}"}


def wa_cloud_api_send_media(*, token: str, phone_number_id: str, to: str, media_id: str, media_kind: str, caption: str = "", filename: str = "") -> Dict[str, Any]:
    url = f"https://graph.facebook.com/{WA_GRAPH_VERSION}/{phone_number_id}/messages"

    payload = {
        "messaging_product": "whatsapp",
        "to": str(to),
        "type": media_kind,
        media_kind: {
            "id": media_id,
        },
    }

    if media_kind == "image" and caption:
        payload["image"]["caption"] = caption

    if media_kind == "document":
        if filename:
            payload["document"]["filename"] = filename
        if caption:
            payload["document"]["caption"] = caption

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
            parsed = json.loads(raw) if raw else {}
            return {"ok": True, "status": int(getattr(resp, "status", 200)), "data": parsed}
    except urllib.error.HTTPError as e:
        raw = (e.read() or b"").decode("utf-8", errors="replace")
        try:
            parsed = json.loads(raw) if raw else {}
        except Exception:
            parsed = {"raw": raw}
        return {"ok": False, "status": int(getattr(e, "code", 500)), "error": "WHATSAPP_MEDIA_SEND_ERROR", "data": parsed}
    except Exception as e:
        return {"ok": False, "status": 500, "error": f"WHATSAPP_MEDIA_SEND_NETWORK_ERROR: {e}"}


def wa_cloud_api_get_media_meta(*, token: str, media_id: str) -> Dict[str, Any]:
    url = f"https://graph.facebook.com/{WA_GRAPH_VERSION}/{media_id}"

    req = urllib.request.Request(
        url=url,
        method="GET",
        headers={"Authorization": f"Bearer {token}"},
    )

    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            parsed = json.loads(raw) if raw else {}
            return {"ok": True, "status": int(getattr(resp, "status", 200)), "data": parsed}
    except urllib.error.HTTPError as e:
        raw = (e.read() or b"").decode("utf-8", errors="replace")
        try:
            parsed = json.loads(raw) if raw else {}
        except Exception:
            parsed = {"raw": raw}
        return {"ok": False, "status": int(getattr(e, "code", 500)), "error": "WHATSAPP_MEDIA_META_ERROR", "data": parsed}
    except Exception as e:
        return {"ok": False, "status": 500, "error": f"WHATSAPP_MEDIA_META_NETWORK_ERROR: {e}"}


def wa_cloud_api_download_media(*, token: str, download_url: str) -> Dict[str, Any]:
    req = urllib.request.Request(
        url=download_url,
        method="GET",
        headers={"Authorization": f"Bearer {token}"},
    )

    try:
        with urllib.request.urlopen(req, timeout=40) as resp:
            content = resp.read()
            content_type = resp.headers.get("Content-Type", "application/octet-stream")
            return {
                "ok": True,
                "status": int(getattr(resp, "status", 200)),
                "content": content,
                "content_type": content_type,
            }
    except urllib.error.HTTPError as e:
        raw = (e.read() or b"").decode("utf-8", errors="replace")
        return {"ok": False, "status": int(getattr(e, "code", 500)), "error": "WHATSAPP_MEDIA_DOWNLOAD_ERROR", "data": raw}
    except Exception as e:
        return {"ok": False, "status": 500, "error": f"WHATSAPP_MEDIA_DOWNLOAD_NETWORK_ERROR: {e}"}


# ---------------- DB helpers ----------------
def db_connect(_db_path: str = ""):
    if not DATABASE_URL:
        raise RuntimeError("Falta DATABASE_URL / PANEL_DATABASE_URL / DB_URL")
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    return conn


def ensure_tables(con):
    with con.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS messages (
          id SERIAL PRIMARY KEY,
          client_id TEXT,
          direction TEXT NOT NULL,
          wa_peer TEXT NOT NULL,
          name TEXT,
          text TEXT,
          msg_type TEXT,
          wa_msg_id TEXT,
          ts_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          raw_json JSONB
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS conv_reads (
          user_email TEXT NOT NULL,
          client_id TEXT NOT NULL,
          wa_peer TEXT NOT NULL,
          last_read_id INTEGER DEFAULT 0,
          updated_utc TIMESTAMPTZ,
          PRIMARY KEY (user_email, client_id, wa_peer)
        )
        """)
    con.commit()


def fetchone_dict(cur):
    row = cur.fetchone()
    if row is None:
        return None
    cols = [desc[0] for desc in cur.description]
    return dict(zip(cols, row))


def fetchall_dicts(cur):
    rows = cur.fetchall()
    cols = [desc[0] for desc in cur.description]
    return [dict(zip(cols, row)) for row in rows]


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
    if not ts_iso:
        return ""
    try:
        dt = datetime.fromisoformat(str(ts_iso).replace("Z", "+00:00"))
        arg_tz = timezone(timedelta(hours=-3))
        local_dt = dt.astimezone(arg_tz)
        today = datetime.now(arg_tz).date()
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
        "client_id": session.get("client_id", ""),
        "calendar_id": cal.get("calendar_id", ""),
        "calendar_tz": cal.get("calendar_tz", "America/Argentina/Buenos_Aires"),
    }


@app.get("/api/conversations")
def api_conversations(request: Request, limit: int = 300):
    u = require_user(request)
    email = u.get("email", "")
    client_id = get_panel_client_id(u)

    con = db_connect(u.get("db_path", ""))
    ensure_tables(con)

    with con.cursor() as cur:
        cur.execute("""
          SELECT wa_peer,
                 MAX(id) as last_id,
                 COALESCE(MAX(ts_utc)::text, '') as last_ts,
                 COALESCE(
                   (SELECT text
                    FROM messages m2
                    WHERE m2.client_id = %s AND m2.wa_peer = m.wa_peer
                    ORDER BY id DESC
                    LIMIT 1), ''
                 ) as last_text,
                 COALESCE(
                   (SELECT name
                    FROM messages m2
                    WHERE m2.client_id = %s AND m2.wa_peer = m.wa_peer
                    ORDER BY id DESC
                    LIMIT 1), ''
                 ) as name
          FROM messages m
          WHERE m.client_id = %s
          GROUP BY wa_peer
          ORDER BY last_id DESC
          LIMIT %s
        """, (client_id, client_id, client_id, limit))
        rows = fetchall_dicts(cur)

        conversations = []
        for r in rows:
            cur.execute("""
              SELECT last_read_id FROM conv_reads
              WHERE user_email = %s AND client_id = %s AND wa_peer = %s
            """, (email, client_id, r["wa_peer"]))
            last_read = fetchone_dict(cur)
            last_read_id = int(last_read["last_read_id"]) if last_read else 0

            cur.execute("""
              SELECT COUNT(*) as n
              FROM messages
              WHERE client_id = %s
                AND wa_peer = %s
                AND direction = 'in'
                AND id > %s
            """, (client_id, r["wa_peer"], last_read_id))
            unread_row = fetchone_dict(cur)
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

    con.close()
    return {"conversations": conversations}


@app.get("/api/chat")
def api_chat(request: Request, wa_peer: str, limit: int = 50, before_id: Optional[int] = None):
    u = require_user(request)
    client_id = get_panel_client_id(u)

    con = db_connect(u.get("db_path", ""))
    ensure_tables(con)

    query = """
      SELECT id, direction, wa_peer, name, text, msg_type, ts_utc::text, raw_json
      FROM messages
      WHERE client_id = %s AND wa_peer = %s
    """
    params: List[Any] = [client_id, wa_peer]

    if before_id is not None:
        query += " AND id < %s"
        params.append(int(before_id))

    query += " ORDER BY id DESC LIMIT %s"
    params.append(int(limit))

    with con.cursor() as cur:
        cur.execute(query, tuple(params))
        rows = fetchall_dicts(cur)

        msgs = []
        for r in reversed(list(rows)):
            media_url = None
            media_kind = None
            content_type = None

            try:
                raw = r["raw_json"] or {}
                if isinstance(raw, str):
                    raw = json.loads(raw)
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
            cur.execute("""
              SELECT 1
              FROM messages
              WHERE client_id = %s AND wa_peer = %s AND id < %s
              LIMIT 1
            """, (client_id, wa_peer, oldest))
            more_row = fetchone_dict(cur)
            has_more = bool(more_row)

    con.close()
    return {"messages": msgs, "has_more": has_more}


@app.post("/api/mark_read")
async def api_mark_read(request: Request):
    u = require_user(request)
    email = u.get("email", "")
    client_id = get_panel_client_id(u)

    con = db_connect(u.get("db_path", ""))
    ensure_tables(con)

    body = await request.json()
    wa_peer = body.get("wa_peer")
    if not wa_peer:
        return JSONResponse({"error": "missing wa_peer"}, status_code=400)

    with con.cursor() as cur:
        cur.execute("""
          SELECT MAX(id) as m
          FROM messages
          WHERE client_id = %s AND wa_peer = %s
        """, (client_id, wa_peer))
        last = fetchone_dict(cur)
        last_id = int(last["m"] or 0)

        cur.execute("""
          INSERT INTO conv_reads (user_email, client_id, wa_peer, last_read_id, updated_utc)
          VALUES (%s, %s, %s, %s, %s)
          ON CONFLICT(user_email, client_id, wa_peer)
          DO UPDATE SET
            last_read_id = EXCLUDED.last_read_id,
            updated_utc = EXCLUDED.updated_utc
        """, (email, client_id, wa_peer, last_id, now_iso()))
    con.commit()
    con.close()

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
    u = require_user(request)
    client_id = get_panel_client_id(u)

    body = await request.json()
    to = body.get("to")
    text = (body.get("text") or "").strip()
    filename = body.get("filename")
    content_type = body.get("content_type") or "application/octet-stream"

    if not to:
        return JSONResponse({"error": "missing to"}, status_code=400)
    if not text and not filename:
        return JSONResponse({"error": "missing text or file"}, status_code=400)

    cfg = get_whatsapp_config_for_user(u)
    token = cfg.get("wa_token", "")
    phone_number_id = cfg.get("phone_number_id", "")

    if not token or not phone_number_id:
        return JSONResponse(
            {"ok": False, "error": "WHATSAPP_NOT_CONFIGURED", "detail": "Falta wa_token o phone_number_id."},
            status_code=502,
        )

    wa_resp = None
    msg_type = "text"
    raw = {}

    if filename:
        abs_media = client_media_abs(u)
        safe_name = safe_basename(filename)
        file_path = os.path.join(abs_media, safe_name)

        if not os.path.exists(file_path):
            return JSONResponse(
                {"ok": False, "error": "FILE_NOT_FOUND", "path": file_path},
                status_code=404
            )

        media_kind = "document"
        msg_type = "document"
        if (content_type or "").startswith("image/"):
            media_kind = "image"
            msg_type = "image"

        upload_resp = wa_cloud_api_upload_media(
            token=token,
            phone_number_id=phone_number_id,
            file_path=file_path,
            content_type=content_type,
        )
        if not upload_resp.get("ok"):
            return JSONResponse(
                {
                    "ok": False,
                    "error": upload_resp.get("error", "WHATSAPP_MEDIA_UPLOAD_FAILED"),
                    "status": upload_resp.get("status", 500),
                    "data": upload_resp.get("data"),
                },
                status_code=502,
            )

        media_id = (upload_resp.get("data") or {}).get("id")
        if not media_id:
            return JSONResponse(
                {"ok": False, "error": "WHATSAPP_MEDIA_UPLOAD_NO_ID", "data": upload_resp.get("data")},
                status_code=502,
            )

        wa_resp = wa_cloud_api_send_media(
            token=token,
            phone_number_id=phone_number_id,
            to=str(to),
            media_id=media_id,
            media_kind=media_kind,
            caption=text,
            filename=safe_name,
        )

        if not wa_resp.get("ok"):
            return JSONResponse(
                {
                    "ok": False,
                    "error": wa_resp.get("error", "WHATSAPP_MEDIA_SEND_FAILED"),
                    "status": wa_resp.get("status", 500),
                    "data": wa_resp.get("data"),
                },
                status_code=502,
            )

        raw = {
            "media": {
                "filename": safe_name,
                "kind": media_kind,
                "content_type": content_type,
                "media_id": media_id,
            }
        }

    else:
        wa_resp = wa_cloud_api_send_text(
            token=token,
            phone_number_id=phone_number_id,
            to=str(to),
            body=text,
        )

        if not wa_resp.get("ok"):
            return JSONResponse(
                {
                    "ok": False,
                    "error": wa_resp.get("error", "WHATSAPP_SEND_FAILED"),
                    "status": wa_resp.get("status", 500),
                    "data": wa_resp.get("data"),
                },
                status_code=502,
            )

    con = db_connect(u.get("db_path", ""))
    ensure_tables(con)

    with con.cursor() as cur:
        cur.execute("""
          INSERT INTO messages (client_id, direction, wa_peer, name, text, msg_type, wa_msg_id, ts_utc, raw_json)
          VALUES (%s, 'out', %s, %s, %s, %s, NULL, %s, %s::jsonb)
        """, (
            client_id,
            to,
            u.get("client_name", ""),
            text,
            msg_type,
            now_iso(),
            json.dumps(raw)
        ))
    con.commit()
    con.close()

    return {"ok": True, "whatsapp": wa_resp.get("data", {})}


@app.post("/webhook")
async def whatsapp_webhook(request: Request):
    data = await request.json()

    try:
        entry = data.get("entry", [])
        if not entry:
            return {"ok": True}

        changes = entry[0].get("changes", [])
        if not changes:
            return {"ok": True}

        value = changes[0].get("value", {})
        messages = value.get("messages", [])
        if not messages:
            return {"ok": True}

        msg = messages[0]

        wa_peer = msg.get("from", "")
        text = msg.get("text", {}).get("body", "")
        msg_type = msg.get("type", "text")
        wa_msg_id = msg.get("id")
        phone_number_id = str(value.get("metadata", {}).get("phone_number_id", "")).strip()

        contacts = value.get("contacts", [])
        contact_name = ""
        if contacts:
            contact_name = contacts[0].get("profile", {}).get("name", "")

        users = load_users()
        if not users:
            return {"ok": False, "error": "NO_USERS"}

        u = None
        for user in users:
            if str(user.get("phone_number_id", "")).strip() == phone_number_id:
                u = user
                break

        if not u:
            u = users[0]

        client_id = get_panel_client_id(u)
        cfg = get_whatsapp_config_for_user(u)
        token = cfg.get("wa_token", "")

        raw_to_store = msg
        media_info = None

        if msg_type in ("image", "document", "audio", "video", "sticker"):
            media_obj = msg.get(msg_type, {}) or {}
            media_id = media_obj.get("id")

            if media_id and token:
                meta_resp = wa_cloud_api_get_media_meta(token=token, media_id=media_id)
                if meta_resp.get("ok"):
                    meta = meta_resp.get("data") or {}
                    download_url = meta.get("url")
                    mime = meta.get("mime_type", "application/octet-stream")

                    if download_url:
                        bin_resp = wa_cloud_api_download_media(token=token, download_url=download_url)
                        if bin_resp.get("ok"):
                            abs_media = client_media_abs(u)
                            ext = guess_extension(mime, ".bin")
                            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                            filename = f"{ts}_{media_id}{ext}"
                            out_path = os.path.join(abs_media, filename)

                            with open(out_path, "wb") as f:
                                f.write(bin_resp["content"])

                            media_kind = "document"
                            if mime.startswith("image/"):
                                media_kind = "image"
                            elif mime.startswith("audio/"):
                                media_kind = "audio"
                            elif mime.startswith("video/"):
                                media_kind = "video"

                            media_info = {
                                "filename": filename,
                                "kind": media_kind,
                                "content_type": mime,
                                "media_id": media_id,
                            }

        if media_info:
            raw_to_store = dict(msg)
            raw_to_store["media"] = media_info

        con = db_connect(u.get("db_path", ""))
        ensure_tables(con)

        with con.cursor() as cur:
            cur.execute("""
            INSERT INTO messages
            (client_id, direction, wa_peer, name, text, msg_type, wa_msg_id, ts_utc, raw_json)
            VALUES (%s, 'in', %s, %s, %s, %s, %s, %s, %s::jsonb)
            """, (
                client_id,
                wa_peer,
                contact_name,
                text,
                msg_type,
                wa_msg_id,
                now_iso(),
                json.dumps(raw_to_store)
            ))
        con.commit()
        con.close()

    except Exception as e:
        print("Webhook error:", e)

    return {"ok": True}


# ---------------- Webhook Meta / WhatsApp ----------------
@app.get("/webhook")
async def verify_webhook(request: Request):
    mode = request.query_params.get("hub.mode")
    token = request.query_params.get("hub.verify_token")
    challenge = request.query_params.get("hub.challenge")

    if mode == "subscribe" and token == VERIFY_TOKEN:
        return PlainTextResponse(challenge or "", status_code=200)

    return PlainTextResponse("Forbidden", status_code=403)

