const express = require("express");
const axios = require("axios");
const dotenv = require("dotenv");
const OpenAI = require("openai");
const { google } = require("googleapis");
const fs = require("fs");
const path = require("path");
const os = require("os");
const Database = require("better-sqlite3");
// ===================== ✅ PDF (documentos) =====================
// Intentamos extraer texto de PDFs si está instalado 'pdf-parse'.
// Si no está, el bot pedirá una captura/imagen del PDF para poder leerlo.
let pdfParse = null;
try { pdfParse = require("pdf-parse"); } catch {}

async function tryParsePdfBuffer(buf) {
  if (!pdfParse) return "";
  try {
    const data = await pdfParse(buf);
    return String(data?.text || "").trim();
  } catch {
    return "";
  }
}



dotenv.config();
// ===================== DB (para panel) =====================
const DB_PATH = process.env.DB_PATH || "db.sqlite";
const db = new Database(DB_PATH);

const MEDIA_DIR = process.env.MEDIA_DIR || path.join(__dirname, "media");
try { fs.mkdirSync(MEDIA_DIR, { recursive: true }); } catch {}

// Tabla igual a la del panel (FastAPI)
db.exec(`
CREATE TABLE IF NOT EXISTS messages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  direction TEXT NOT NULL,           -- 'in' o 'out'
  wa_peer TEXT NOT NULL,             -- numero del cliente
  name TEXT,
  text TEXT,
  msg_type TEXT,
  wa_msg_id TEXT,
  ts_utc TEXT NOT NULL,
  raw_json TEXT
)
`);

// ✅ Normalización única: 549XXXXXXXX -> 54XXXXXXXX
function normalizePhone(s) {
  s = String(s || "").trim().replace(/[ \+\-\(\)]/g, "");
  if (s.endsWith(".0")) s = s.slice(0, -2);
  if (s.startsWith("549")) s = "54" + s.slice(3);
  return s;
}

function dbInsertMessage({ direction, wa_peer, name, text, msg_type, wa_msg_id, raw }) {
  const ts_utc = new Date().toISOString();
  const stmt = db.prepare(`
    INSERT INTO messages(direction, wa_peer, name, text, msg_type, wa_msg_id, ts_utc, raw_json)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `);

  // ✅ Blindado: siempre guardamos wa_peer normalizado
  const peerNorm = normalizePhone(wa_peer);

  stmt.run(
    direction,
    peerNorm,
    name || null,
    text || null,
    msg_type || null,
    wa_msg_id || null,
    ts_utc,
    JSON.stringify(raw || {}, null, 0)
  );
}

const app = express();
app.use(express.json({ limit: "25mb" }));

// ===================== CONFIG =====================
const DRIVE_FOLDER_ID = "1pKCqh1HEvQaI6XQ85ST8yvzxYWRXpxM1";
const TIMEZONE = "America/Argentina/Salta";

// ===================== REQUIRED ENV CHECK (evita demos rotas) =====================
const REQUIRED_ENV = ["OPENAI_API_KEY", "WHATSAPP_TOKEN", "PHONE_NUMBER_ID", "VERIFY_TOKEN", "GOOGLE_SA_FILE"];
for (const k of REQUIRED_ENV) {
  if (!process.env[k]) throw new Error(`Falta variable en .env: ${k}`);
}
if (!fs.existsSync(process.env.GOOGLE_SA_FILE)) {
  throw new Error(`No existe GOOGLE_SA_FILE en la ruta: ${process.env.GOOGLE_SA_FILE}`);
}

// ===================== OpenAI =====================
const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
// ===================== ✅ IA: detectar comprobante / datos de pago =====================
async function extractPagoInfoWithAI(text) {
  const t = String(text || "").trim();
  if (!t) return { ok: false, es_comprobante: false, pagador: "", monto: "", receptor: "" };

  // Heurística rápida antes de IA (barato)
  const quick = /(transferencia|comprobante|mercado pago|mp|cvu|alias|aprobado|recibida|\$\s*\d|monica pacheco|cataleya178)/i.test(t);
  if (!quick && t.length < 50) return { ok: false, es_comprobante: false, pagador: "", monto: "", receptor: "" };

  try {
    const completion = await openai.chat.completions.create({
      model: PRIMARY_MODEL,
      messages: [
        {
          role: "system",
          content:
`Analizá el texto (puede venir de una imagen/recibo) y devolvé SOLO JSON.
Campos:
- es_comprobante: boolean (si parece comprobante de transferencia/pago)
- pagador: string (nombre de quien paga / "A nombre de ...", si aparece)
- receptor: string (a quién se pagó, si aparece)
- monto: string (ej "$10.000", si aparece)
- ok: boolean (true si pudiste inferir algo útil)`,
        },
        { role: "user", content: t.slice(0, 6000) },
      ],
      response_format: { type: "json_object" },
    });
    const obj = JSON.parse(completion.choices[0].message.content || "{}");
    return {
      ok: !!obj.ok,
      es_comprobante: !!obj.es_comprobante,
      pagador: String(obj.pagador || "").trim(),
      receptor: String(obj.receptor || "").trim(),
      monto: String(obj.monto || "").trim(),
    };
  } catch {
    return { ok: false, es_comprobante: false, pagador: "", monto: "", receptor: "" };
  }
}


const PRIMARY_MODEL = process.env.PRIMARY_MODEL || "gpt-4.1-mini";
const COMPLEX_MODEL = process.env.COMPLEX_MODEL || PRIMARY_MODEL;
const TRANSCRIBE_MODEL = process.env.TRANSCRIBE_MODEL || "gpt-4o-mini-transcribe";

// ===================== BOT =====================
const SYSTEM_PROMPT = `
Sos la asistente oficial de un salón de belleza de estética llamado "Cataleya" en Cafayate (Salta).
Tu rol es vender y ayudar con consultas de forma rápida, clara y muy amable.
Hablás en español rioplatense, con mensajes cortos y prácticos. Profesional, nada de tutear.

ESTILO:
- Respuestas en 1–2 líneas, con viñetas si ayuda.
- Si inicia con “hola”, responder saludo + “¿en qué puedo ayudarte?” solamente.
- Emojis ocasionales: al inicio de la conversación con un corazón o una estrella; después tono profesional. usar letras negrita donde sea necesario cuando aclares cosas. 

Ofrecés:
- Servicios estéticos
- Productos e insumos
- Muebles y equipamiento (espejos, sillones, camillas)
- Cursos de estética y capacitaciones

TURNOS:
- Información de turnos (siempre):
  - Estilista: Flavia Rueda.
  - Horarios de turnos: Lunes a Sábados de 10 a 12 hs y de 17 a 20 hs.
  - Para CONFIRMAR TURNO, se requiere seña obligatoria de $10.000. Si no lo abona, no se guardará el turno.
  - Alias para transferir: Cataleya178
  sale a nombre Monica Pachecho. Luego debe enviar foto/captura del comprobante.

  - Si el cliente pide turno para color/tintura/teñirse/retocar: luego de elegir día y horario, responder que queda en confirmar y que se consulta con la estilista.
  - Al registrar un turno, solicitar nombre completo y teléfono de contacto. Si ya pagó seña, marcar como SEÑADO.
- No inventes precios ni servicios: solo los que figuran en el Excel de servicios. 
- NO se ofrece lifting de pestañas, cejas, perfilado, uñas, limpiezas faciales ni otros servicios fuera del Excel.

- si busca Corte masculino / varón / hombre: es SOLO por orden de llegada, no se toma turno. Horario: Lunes a Sábados 10 a 13 hs y 17 a 22 hs. Precio final: $10.000 PESOS

- Horario del salón comercial: Lunes a Viernes de 17 a 22 hs.

Si preguntan por precios, stock u opciones, usá los catálogos cuando sea posible.
Cuando respondas con productos/servicios/cursos, NO reescribas los nombres: copiá el "Nombre" tal cual figura en el Excel.
`.trim();

// ===================== MEMORIA DIARIA (leads) =====================
const dailyLeads = new Map();

// ===================== HISTORIAL CORTO =====================
const conversations = new Map();
// ✅ MEMORIA: mantener contexto por al menos 10 horas
const CONV_TTL_MS = 7 * 24 * 60 * 60 * 1000; // 7 días // 10 horas


function ensureConv(waId) {
  let c = conversations.get(waId);
  const now = Date.now();
  if (!c) {
    c = { messages: [], updatedAt: now };
    conversations.set(waId, c);
    return c;
  }
  // ✅ Si pasó el TTL, reiniciamos la conversación (memoria expira)
  if ((now - (c.updatedAt || 0)) > CONV_TTL_MS) {
    c.messages = [];
  }
  return c;
}

function pushHistory(waId, role, content) {
  const conv = ensureConv(waId);
  conv.messages.push({ role, content });
  conv.messages = conv.messages.slice(-30);
  conv.updatedAt = Date.now();
}

// ===================== ✅ INACTIVIDAD (FOLLOW-UP + CIERRE/LOG) =====================
const inactivityTimers = new Map();
const closeTimers = new Map();
const lastCloseContext = new Map(); // waId -> { phone, name, lastUserText, intentType, interest }

const INACTIVITY_MS = 10 * 60 * 1000; // 10 minutos (mensaje de cierre)
const CLOSE_LOG_MS = 10 * 60 * 1000;  // 10 minutos más (si no responde, se registra)

async function logConversationClose(waId) {
  const ctx = lastCloseContext.get(waId);
  if (!ctx?.phone) return;

  const spreadsheetId = await getOrCreateMonthlySpreadsheet();
  const sheetName = getTodaySheetName();
  await ensureDailySheet(spreadsheetId, sheetName);

  const lastText = (ctx.lastUserText || "").trim();
  const categoria = pickCategoria({ intentType: ctx.intentType || "OTHER", text: lastText });
  const productos = pickProductos(lastText);

  const resumenBase = (ctx.interest || lastText || "(sin detalle)").trim();
  const resumenCorto = resumenBase.length > 80 ? resumenBase.slice(0, 80) : resumenBase;

  const observacion = `${ddmmyyyyAR()} ${resumenCorto}`;

  await upsertContactRow({
    spreadsheetId,
    sheetName,
    name: ctx.name || "",
    phone: ctx.phone,
    observacion,
    categoria,
    productos,
    ultimo_contacto: ddmmyyyyAR(),
  });
}

function scheduleInactivityFollowUp(waId, phone) {
  if (!waId || !phone) return;

  // si ya había uno, lo reiniciamos
  if (inactivityTimers.has(waId)) clearTimeout(inactivityTimers.get(waId));
  if (closeTimers.has(waId)) clearTimeout(closeTimers.get(waId));

  const timer = setTimeout(async () => {
    try {
      await sendWhatsAppText(
        phone,
        "¿Quiere que le ayudemos en algo más o damos por finalizada la consulta?"
      );
    } catch (e) {
      console.error("Error enviando mensaje por inactividad:", e?.response?.data || e?.message || e);
    }

    // Si NO respondió luego del mensaje de cierre, registramos el seguimiento
    const timer2 = setTimeout(async () => {
      try {
        await logConversationClose(waId);
      } catch (e) {
        console.error("Error guardando seguimiento por cierre:", e?.response?.data || e?.message || e);
      }
    }, CLOSE_LOG_MS);

    closeTimers.set(waId, timer2);
  }, INACTIVITY_MS);

  inactivityTimers.set(waId, timer);
}

// Último producto por usuario (para “mandame foto” sin repetir nombre)
const lastProductByUser = new Map();
// ✅ Último servicio consultado por usuario (para no repetir pregunta en turnos)
const lastServiceByUser = new Map();
// ===================== ✅ ANTI-SPAM (evita repetir el mismo texto) =====================
// Evita que WhatsApp envíe el mismo mensaje predeterminado varias veces por reintentos/doble flujo.
// Key: `${to}::${text}` -> ts
const lastSentOutByPeer = new Map();
const OUT_DEDUP_MS = 20 * 1000; // 20s

// ===================== ✅ TURNOS: no repetir bloque informativo =====================
// Se envía SOLO 1 vez por día por usuario (zona del salón).
const lastTurnoInfoSentDay = new Map(); // waId -> 'YYYY-MM-DD' en TIMEZONE

function todayYMDInTZ() {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: TIMEZONE,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(new Date());
  const get = (t) => parts.find(p => p.type === t)?.value || "";
  return `${get("year")}-${get("month")}-${get("day")}`;
}

function maybeTurnoInfoBlock(waId) {
  const today = todayYMDInTZ();
  const last = lastTurnoInfoSentDay.get(waId) || "";
  if (last === today) return "";
  lastTurnoInfoSentDay.set(waId, today);
  return turnoInfoBlock();
}



const LAST_SERVICE_TTL_MS = 6 * 60 * 60 * 1000; // 6 horas


// ===================== DEDUPE =====================
const processedMsgIds = new Set();

// ===================== GOOGLE APIS =====================
let sheetsClient = null;
let driveClient = null;
let calendarClient = null;

async function getAuth() {
  return new google.auth.GoogleAuth({
    keyFile: process.env.GOOGLE_SA_FILE,
    scopes: [
      "https://www.googleapis.com/auth/spreadsheets",
      "https://www.googleapis.com/auth/drive",
      // ✅ Turnos: Google Calendar (si se usa)
      "https://www.googleapis.com/auth/calendar",
    ],
  });
}
async function getSheetsClient() {
  if (sheetsClient) return sheetsClient;
  const auth = await getAuth();
  sheetsClient = google.sheets({ version: "v4", auth });
  return sheetsClient;
}
async function getDriveClient() {
  if (driveClient) return driveClient;
  const auth = await getAuth();
  driveClient = google.drive({ version: "v3", auth });
  return driveClient;
}

async function getCalendarClient() {
  if (calendarClient) return calendarClient;
  const auth = await getAuth();
  calendarClient = google.calendar({ version: "v3", auth });
  return calendarClient;
}

// ===================== ✅ TURNOS (Calendar + Sheet dedicada) =====================
// Sheet donde se anotan turnos
const TURNOS_SHEET_ID = process.env.TURNOS_SHEET_ID ||"18mxtcDoD5BbkulG8g1fXE7YRZpt_CFFPvt8JBGzh4S8";

const TURNOS_TAB = process.env.TURNOS_TAB || "TURNOS";

// Calendar ID (compartir el calendario con el service account)
// Si no se configura, igual se anota en la planilla.
const CALENDAR_ID = process.env.CALENDAR_ID || process.env.GCALENDAR_ID || "";

const TURNOS_HEADERS = [
  "FECHA",
  "DIA",
  "HORA",
  "CLIENTE",
  "TELEFONO",
  "SERVICIO",
  "DURACION_MIN",
  "CALENDAR_EVENT_ID",
  "CREADO_EN",
];

// Estado para reserva en 2 o más mensajes (si el cliente responde solo fecha/hora, igual agenda)
const pendingTurnos = new Map(); // waId -> { fecha, hora, servicio, duracion_min, notas }

// ===================== ✅ INFO FIJA DE TURNOS (NO CAMBIAR ARQUITECTURA) =====================
const TURNOS_STYLIST_NAME = "Flavia Rueda";
const TURNOS_HORARIOS_TXT = "Lunes a Sábados de 10 a 12 hs y de 17 a 20 hs";
const TURNOS_SENA_TXT = "$10.000";
const TURNOS_ALIAS = "Cataleya178";
const TURNOS_ALIAS_TITULAR = "Monica Pachecho";

function turnoInfoBlock() {
  return (
`ℹ️ Información para turnos:

Estilista ${TURNOS_STYLIST_NAME} 💇‍♀️
- Horarios disponibles para turnos: de ${TURNOS_HORARIOS_TXT}.

📆 Para CONFIRMAR TURNO, debe dejar *una seña obligatoria de ${TURNOS_SENA_TXT} PESOS. Si no lo abona, no se guardará el turno*.

Alias para transferir: ${TURNOS_ALIAS}
A nombre ${TURNOS_ALIAS_TITULAR}.
*Debe enviar foto/captura del comprobante por favor*`
  );
}

function pedirDatosRegistroTurnoBlock() {
  return (
`

Para registrar el turno, por favor envíe:
• Nombre completo (nombre y apellido)
• Número de teléfono de contacto

Si ya realizó la transferencia, envíe la captura del comprobante y lo dejo como *SEÑADO* ✅`
  );
}

function detectSenaPaid({ text, msgType }) {
  const t = normalize(text || "");
  if (/(comprobant|transfer|transferi|transferí|señad|seña|pagu|pago|abon|abono)/i.test(t)) return true;
  if (msgType && (msgType === "image" || msgType === "document")) return true;
  return false;
}

function extractContactInfo(text) {
  const raw = String(text || "").trim();

  const mPhone = raw.match(/(\+?\d[\d\s().-]{6,}\d)/);
  let telefono = "";
  if (mPhone) {
    telefono = mPhone[1].replace(/[^\d+]/g, "");
    if (!telefono.startsWith("+")) telefono = `+${telefono}`;
  }


function mergeContactIntoTurno({ turno, text, waPhone }) {
  const t = String(text || "").trim();
  const info = extractContactInfo(t);

  const out = { ...(turno || {}) };

  // Si el cliente manda "A nombre de X", lo tomamos como nombre completo si falta.
  if (info.nombre && !out.cliente_full) out.cliente_full = info.nombre;

  // Si no manda teléfono, usamos el número de WhatsApp (sirve para registro)
  if (!out.telefono_contacto) out.telefono_contacto = waPhone || "";

  // Si mandó un teléfono explícito, preferimos ese
  if (info.telefono) out.telefono_contacto = info.telefono;

  return out;
}

  let nombre = "";
  const cleaned = raw.replace(/\s+/g, " ").trim();
  const candidate = cleaned.replace(/^(me llamo|soy|mi nombre es)\s+/i, "").trim();
  if (candidate && !/\d/.test(candidate) && candidate.split(" ").length >= 2 && candidate.length >= 5) {
    nombre = candidate;
  }

  return { nombre, telefono };
}

function isColorOrTinturaService(text) {
  const t = normalize(text);
  return /(\bcolor\b|tintur|tinte|mecha|balayage|decolor|reflej|raic|raiz|ilumin|tonaliz)/i.test(t);
}

function weekdayEsFromYMD(ymd) {
  try {
    const d = new Date(`${ymd}T12:00:00-03:00`);
    return new Intl.DateTimeFormat("es-AR", { weekday: "long", timeZone: TIMEZONE }).format(d);
  } catch {
    return "";
  }
}

function formatYMDHMInTZ(dateObj) {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: TIMEZONE,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).formatToParts(dateObj);
  const get = (t) => parts.find(p => p.type === t)?.value || "";
  return {
    ymd: `${get("year")}-${get("month")}-${get("day")}`,
    hm: `${get("hour")}:${get("minute")}`,
  };
}

function addMinutesToYMDHM({ ymd, hm }, minutes) {
  const base = new Date(`${ymd}T${hm}:00-03:00`);
  const d2 = new Date(base.getTime() + (Number(minutes) || 0) * 60000);
  return formatYMDHMInTZ(d2);
}

async function ensureTurnosSheet(spreadsheetId, sheetName) {
  const sheets = await getSheetsClient();

  // Intentamos ver si existe
  const spreadsheet = await sheets.spreadsheets.get({ spreadsheetId });
  const exists = (spreadsheet.data.sheets || [])
    .map((s) => s.properties.title)
    .includes(sheetName);

  if (!exists) {
    try {
      await sheets.spreadsheets.batchUpdate({
        spreadsheetId,
        requestBody: { requests: [{ addSheet: { properties: { title: sheetName } } }] },
      });
      console.log(`📄 Hoja de turnos creada: ${sheetName}`);
    } catch (e) {
      // ✅ Si ya existe, lo ignoramos (idempotente)
      const msg = e?.response?.data?.error?.message || e?.message || "";
      if (!/already exists/i.test(msg)) {
        throw e;
      }
    }
  }

  // Header siempre
  await sheets.spreadsheets.values.update({
    spreadsheetId,
    range: `${sheetName}!A1:I1`,
    valueInputOption: "USER_ENTERED",
    requestBody: { values: [TURNOS_HEADERS] },
  });
}


async function appendTurnoRow({ fechaYMD, dia, horaHM, cliente, telefono, servicio, duracionMin, calendarEventId }) {
  const sheets = await getSheetsClient();
  await ensureTurnosSheet(TURNOS_SHEET_ID, TURNOS_TAB);

  const createdAt = new Date().toLocaleString("es-AR", { timeZone: TIMEZONE });

  const row = [[
    ymdToDMY(fechaYMD) || "",
    dia || "",
    horaHM || "",
    cliente || "",
    normalizePhone(telefono || ""),
    servicio || "",
    String(duracionMin || ""),
    calendarEventId || "",
    createdAt,
  ]];

  await sheets.spreadsheets.values.append({
    spreadsheetId: TURNOS_SHEET_ID,
    range: `${TURNOS_TAB}!A:I`,
    valueInputOption: "USER_ENTERED",
    requestBody: { values: row },
  });
}

async function extractTurnoFromText({ text, customerName, context }) {
  const nowTxt = nowARString();
  const ctx = context || {};
  const completion = await openai.chat.completions.create({
    model: PRIMARY_MODEL,
    messages: [
      {
        role: "system",
        content:
`Extraé datos de un pedido de turno para un salón de estética.
Respondé JSON estricto con:
- ok: boolean
- fecha: YYYY-MM-DD o ""
- hora: HH:MM (24h) o ""
- duracion_min: number (default 60 si hay turno)
- servicio: string corto (puede ser "")
- notas: string corto ("" si no aplica)
- faltantes: array de strings ("fecha", "hora", "servicio")

Reglas:
- Interpretá fechas relativas ("mañana", "el viernes") usando ${nowTxt} y zona ${TIMEZONE}.
- Si el texto NO es un pedido de turno NI una continuación de reserva, ok=false.
- Si te paso un contexto con datos ya conocidos, mantenelos y completá lo faltante.
`,
      },
      {
        role: "user",
        content: `Cliente: ${customerName || ""}\nContexto: ${JSON.stringify({
          fecha: ctx.fecha || "",
          hora: ctx.hora || "",
          servicio: ctx.servicio || "",
          duracion_min: ctx.duracion_min || "",
          notas: ctx.notas || "",
        })}\nMensaje: ${text}`
      },
    ],
    response_format: { type: "json_object" },
  });

  try {
    const obj = JSON.parse(completion.choices[0].message.content);
    return {
      ok: !!obj.ok,
      fecha: (obj.fecha || "").trim(),
      hora: (obj.hora || "").trim(),
      duracion_min: Number(obj.duracion_min || 60) || 60,
      servicio: (obj.servicio || "").trim(),
      notas: (obj.notas || "").trim(),
      faltantes: Array.isArray(obj.faltantes) ? obj.faltantes.map(x => String(x || "").trim()).filter(Boolean) : [],
    };
  } catch {
    return { ok: false, fecha: "", hora: "", duracion_min: 60, servicio: "", notas: "", faltantes: [] };
  }
}

async function calendarHasConflict({ dateYMD, startHM, durationMin }) {
  if (!CALENDAR_ID) return false;
  const cal = await getCalendarClient();

  const start = new Date(`${dateYMD}T${startHM}:00-03:00`);
  const end = new Date(start.getTime() + (Number(durationMin) || 60) * 60000);

  const timeMin = start.toISOString();
  const timeMax = end.toISOString();

  const resp = await cal.events.list({
    calendarId: CALENDAR_ID,
    timeMin,
    timeMax,
    singleEvents: true,
    orderBy: "startTime",
    maxResults: 10,
  });

  const items = resp?.data?.items || [];
  return items.length > 0;
}

async function createCalendarTurno({ dateYMD, startHM, durationMin, cliente, telefono, servicio, notas }) {
  if (!CALENDAR_ID) return { eventId: "", skipped: true };

  const cal = await getCalendarClient();
  const startLocal = { ymd: dateYMD, hm: startHM };
  const endLocal = addMinutesToYMDHM(startLocal, Number(durationMin) || 60);

  const summary = `Turno Cataleya - ${cliente || "Cliente"}${servicio ? ` (${servicio})` : ""}`;
  const description = [
    telefono ? `Tel: ${normalizePhone(telefono)}` : "",
    servicio ? `Servicio: ${servicio}` : "",
    notas ? `Notas: ${notas}` : "",
  ].filter(Boolean).join("\n");

  const event = {
    summary,
    description,
    start: { dateTime: `${startLocal.ymd}T${startLocal.hm}:00`, timeZone: TIMEZONE },
    end: { dateTime: `${endLocal.ymd}T${endLocal.hm}:00`, timeZone: TIMEZONE },
  };

  const inserted = await cal.events.insert({
    calendarId: CALENDAR_ID,
    requestBody: event,
  });

  return { eventId: inserted?.data?.id || "", skipped: false };
}

// ===================== FECHAS =====================
function nowARString() {
  return new Date().toLocaleString("es-AR", { timeZone: TIMEZONE });
}
function getMonthFileName() {
  const date = new Date().toLocaleDateString("es-AR", {
    month: "long",
    year: "numeric",
    timeZone: TIMEZONE,
  });
  return `SEGUIMIENTO - ${date.toUpperCase()}`;
}
function getTodaySheetName() {
  const date = new Date().toLocaleDateString("es-AR", {
    weekday: "long",
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
    timeZone: TIMEZONE,
  });
  return date.charAt(0).toUpperCase() + date.slice(1);
}

// ===================== DRIVE (SEGUIMIENTO mensual) =====================
async function getOrCreateMonthlySpreadsheet() {
  const drive = await getDriveClient();
  const fileName = getMonthFileName();

  const search = await drive.files.list({
    q: `name='${fileName}' and mimeType='application/vnd.google-apps.spreadsheet' and '${DRIVE_FOLDER_ID}' in parents`,
    fields: "files(id)",
  });

  if (search.data.files.length > 0) return search.data.files[0].id;

  const file = await drive.files.create({
    requestBody: {
      name: fileName,
      mimeType: "application/vnd.google-apps.spreadsheet",
      parents: [DRIVE_FOLDER_ID],
    },
    fields: "id",
  });

  console.log(`📊 Excel creado: ${fileName}`);
  return file.data.id;
}

// ===================== SHEETS (SEGUIMIENTO) =====================
// Columnas requeridas por el cliente:
const TRACK_HEADERS = ["NAME", "PHONE", "OBSERVACION", "CATEGORIA", "PRODUCTOS", "ULTIMO_CONTACTO"];

function ddmmyyyyAR() {
  return new Date().toLocaleDateString("es-AR", { timeZone: TIMEZONE });
}

// ✅ Formatos de fecha (sin cambiar arquitectura)
// Internamente seguimos usando YYYY-MM-DD para Calendar/lógica.
// Para registrar/enviar al cliente usamos DD/MM/YYYY.
function ymdToDMY(ymd) {
  const s = String(ymd || "").trim();
  const m = s.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!m) return s;
  return `${m[3]}/${m[2]}/${m[1]}`;
}

// Acepta "DD/MM/YYYY", "DD-MM-YYYY", "YYYY-MM-DD" y devuelve siempre "YYYY-MM-DD" (si puede).
function toYMD(dateStr) {
  const s = String(dateStr || "").trim();
  if (!s) return "";
  // ya viene en YMD
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;

  // DD/MM/YYYY o DD-MM-YYYY
  let m = s.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})$/);
  if (m) {
    const dd = String(m[1]).padStart(2, "0");
    const mm = String(m[2]).padStart(2, "0");
    const yyyy = String(m[3]);
    return `${yyyy}-${mm}-${dd}`;
  }

  // DD/MM o DD-MM (asumimos año actual en TIMEZONE)
  m = s.match(/^(\d{1,2})[\/\-](\d{1,2})$/);
  if (m) {
    const dd = String(m[1]).padStart(2, "0");
    const mm = String(m[2]).padStart(2, "0");
    const yyyy = todayYMDInTZ().slice(0, 4);
    return `${yyyy}-${mm}-${dd}`;
  }

  return s;
}

async function ensureDailySheet(spreadsheetId, sheetName) {
  const sheets = await getSheetsClient();
  const spreadsheet = await sheets.spreadsheets.get({ spreadsheetId });

  const exists = (spreadsheet.data.sheets || [])
    .map((s) => s.properties.title)
    .includes(sheetName);

  if (!exists) {
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId,
      requestBody: {
        requests: [{ addSheet: { properties: { title: sheetName } } }],
      },
    });
    console.log(`📄 Hoja creada: ${sheetName}`);
  }

  // ✅ Header fijo (si ya existía, lo deja correcto)
  await sheets.spreadsheets.values.update({
    spreadsheetId,
    range: `${sheetName}!A1:F1`,
    valueInputOption: "USER_ENTERED",
    requestBody: { values: [TRACK_HEADERS] },
  });
}

async function upsertContactRow({ spreadsheetId, sheetName, name, phone, observacion, categoria, productos, ultimo_contacto }) {
  const sheets = await getSheetsClient();
  const phoneNorm = normalizePhone(phone);

  // Buscar phone en columna B (PHONE) desde fila 2
  const col = await sheets.spreadsheets.values.get({
    spreadsheetId,
    range: `${sheetName}!B2:B`,
  });

  const values = col.data.values || [];
  let rowIndex = -1;
  for (let i = 0; i < values.length; i++) {
    const cell = (values[i]?.[0] || "").trim();
    if (normalizePhone(cell) === phoneNorm) { rowIndex = i; break; }
  }

  const row = [[
    name || "",
    phoneNorm,
    observacion || "",
    categoria || "",
    productos || "",
    ultimo_contacto || "",
  ]];

  if (rowIndex >= 0) {
    const targetRowNumber = 2 + rowIndex;
    await sheets.spreadsheets.values.update({
      spreadsheetId,
      range: `${sheetName}!A${targetRowNumber}:F${targetRowNumber}`,
      valueInputOption: "USER_ENTERED",
      requestBody: { values: row },
    });
    return;
  }

  await sheets.spreadsheets.values.append({
    spreadsheetId,
    range: `${sheetName}!A:F`,
    valueInputOption: "USER_ENTERED",
    requestBody: { values: row },
  });
}

// Mantengo el nombre para respetar arquitectura (se usa en endOfDayJob).
async function appendToSheet({ name, phone, interest }) {
  const spreadsheetId = await getOrCreateMonthlySpreadsheet();
  const sheetName = getTodaySheetName();
  await ensureDailySheet(spreadsheetId, sheetName);

  const obs = interest
    ? `${ddmmyyyyAR()} ${String(interest).trim()}`
    : `${ddmmyyyyAR()} (sin detalle)`;

  await upsertContactRow({
    spreadsheetId,
    sheetName,
    name: name || "",
    phone,
    observacion: obs,
    categoria: "",
    productos: "",
    ultimo_contacto: ddmmyyyyAR(),
  });
}

// ===================== INTERÉS (LEADS) =====================
async function detectInterest(text) {
  const completion = await openai.chat.completions.create({
    model: PRIMARY_MODEL,
    messages: [
      { role: "system", content: "Detectá solo interés real. Respondé una frase corta o 'NINGUNO'." },
      { role: "user", content: text },
    ],
  });

  const result = completion.choices[0].message.content.trim();
  return result.toUpperCase() === "NINGUNO" ? null : result;
}

// ===================== UTIL =====================
function normalize(str) {
  return String(str || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/\s+/g, " ")
    .trim();
}


function isYesNoShortReply(text) {
  const t = normalize(text);
  return [
    "si", "sí", "sii", "siii", "ok", "oka", "dale", "de una", "claro", "perfecto",
    "no", "nop", "nah"
  ].includes(t);
}

function lastAssistantWasQuestion(waId) {
  const conv = ensureConv(waId);
  const last = [...(conv.messages || [])].reverse().find((m) => m.role === "assistant");
  if (!last?.content) return false;
  return String(last.content).includes("?");
}


function canonicalizeQuery(text) {
  const t = normalize(text);
  return t
    .replace(/\bchampu\b/g, "shampoo")
    .replace(/\bshampu\b/g, "shampoo")
    .replace(/\bshampoo\b/g, "shampoo");
}

// ===================== CATEGORIA / PRODUCTOS (SOLO LISTA PERMITIDA) =====================
const CATEGORIAS_OK = [
  "CURSOS📝",
  "MUEBLES🪑",
  "SERVICIOS DE BELLEZA💄",
  "BARBER💈",
  "INSUMOS🧴",
  "MÁQUINAS⚙️",
];

const PRODUCTOS_OK = [
  "Silla Hidráulica", "Camillas", "Puff", "Espejos / Muebles", "Mesas", "Planchas", "Secadores",
  "Shampoo ácido", "Nutrición", "Tintura", "Baño de crema", "Matizador",
  "Ojos", "Peinado", "Limpieza Facial", "Lifting", "Pestañas", "Cejas", "Pies", "Uñas", "Cera",
  "Alisado", "Corte", "Tintura", "Maquillaje",
  "Aceite maquina", "Tijeras", "Trenzas", "Depilación", "Masajes", "Permanente",
  "Mesa", "Manicura", "Barber"
];

function pickCategoria({ intentType, text }) {
  const t = normalize(text);

  if (intentType === "COURSE" || t.includes("curso") || t.includes("taller")) return "CURSOS📝";

  if (intentType === "SERVICE" || t.includes("turno") || t.includes("servicio") ||
      t.includes("limpieza facial") || t.includes("lifting") || t.includes("pesta") || t.includes("ceja") ||
      t.includes("uñas") || t.includes("unas") || t.includes("depil") || t.includes("masaje") ||
      t.includes("alisado") || t.includes("peinado") || t.includes("maquill")) {
    return "SERVICIOS DE BELLEZA💄";
  }

  if (t.includes("barber") || t.includes("barbero") || t.includes("maquina de cortar") ||
      t.includes("máquina de cortar") || t.includes("tijera") || t.includes("aceite maquina") ||
      t.includes("aceite máquina") || t.includes("insumo para maquina") || t.includes("insumo para máquina")) {
    return "BARBER💈";
  }

  if (t.includes("camilla") || t.includes("espejo") || t.includes("respaldo") || t.includes("maquillador") ||
      t.includes("mueble") || t.includes("mesa") || t.includes("puff") || t.includes("silla")) {
    return "MUEBLES🪑";
  }

  if (t.includes("maquina") || t.includes("máquina") || t.includes("herramienta") ||
      t.includes("plancha") || t.includes("secador")) {
    return "MÁQUINAS⚙️";
  }

  if (intentType === "PRODUCT" || t.includes("shampoo") || t.includes("baño de crema") || t.includes("baño de crema") ||
      t.includes("matizador") || t.includes("tintura") || t.includes("nutricion") || t.includes("nutrición")) {
    return "INSUMOS🧴";
  }

  return "";
}

function pickProductos(text) {
  const t = normalize(text);
  const hits = [];
  for (const p of PRODUCTOS_OK) {
    const k = normalize(p);
    if (k && t.includes(k)) hits.push(p);
  }
  return Array.from(new Set(hits)).slice(0, 6).join(" / ");
}

// ===================== ✅ FUZZY MATCH (stock) =====================
// Objetivo: el cliente suele usar variaciones (plurales, marcas, typos, etc.).
// Buscamos "lo más probable" sin devolver cualquier cosa.
// - Para DETAIL: si hay ambigüedad, preferimos NO responder un producto incorrecto.
// - Para LIST: devolvemos las mejores coincidencias ordenadas: más relevante arriba.

const STOCK_STOPWORDS = new Set([
  // verbos / muletillas
  "tenes", "tenés", "tienen", "hay", "disponible", "disponibles",
  "precio", "cuanto", "cuánto", "sale", "valen", "vale",
  "me", "pasas", "pasás", "mandame", "enviame", "enviáme", "mostrar", "mostrame", "ver",
  "foto", "fotos", "imagen", "imagenes", "queria", "quería",
  "hola", "buenas", "por", "favor", "gracias", "quiero", "quisiera",
  // conectores
  "de", "del", "la", "las", "el", "los", "un", "una", "unos", "unas",
  "para", "con", "sin", "y", "o", "en", "a"
]);

// Sinónimos suaves (solo para ampliar intención sin inventar productos)
// Nota: se aplica SOLO a la query del cliente, no al catálogo.
// Usamos raíces (stems) para que coincida con stemEs().
const STOCK_SYNONYMS = {
  // alisados
  alisad: ["keratin", "nanoplast", "botox", "cirugi"],
  keratin: ["alisad"],
  nanoplast: ["alisad"],
  botox: ["alisad"],
  // depilación
  depil: ["cera", "laser"],
  laser: ["depil"],
};

function stemEs(token) {
  // stem mínimo para plurales y variantes comunes
  let t = String(token || "").trim();
  if (!t) return "";

  // quitar puntas no alfanum
  t = t.replace(/^[^\p{L}\p{N}]+|[^\p{L}\p{N}]+$/gu, "");
  if (t.length <= 3) return t;

  // plurales muy comunes
  if (t.endsWith("es") && t.length > 4) t = t.slice(0, -2);
  else if (t.endsWith("s") && t.length > 4) t = t.slice(0, -1);

  // diminutivos frecuentes (suave)
  if (t.endsWith("cito") && t.length > 6) t = t.slice(0, -4);
  if (t.endsWith("cita") && t.length > 6) t = t.slice(0, -4);

  return t;
}

function tokenize(text, { expandSynonyms = false } = {}) {
  const t = canonicalizeQuery(text)
    .replace(/[^\p{L}\p{N}\s]/gu, " ")
    .replace(/\s+/g, " ")
    .trim();
  if (!t) return [];

  const base = t
    .split(" ")
    .map(stemEs)
    .filter(w => w && !STOCK_STOPWORDS.has(w) && w.length >= 2);

  if (!expandSynonyms) return base;

  const out = new Set(base);
  for (const tok of base) {
    const syn = STOCK_SYNONYMS[tok];
    if (syn?.length) for (const s of syn) out.add(s);
  }
  return Array.from(out);
}

function jaccard(a, b) {
  const A = new Set(a);
  const B = new Set(b);
  if (!A.size || !B.size) return 0;
  let inter = 0;
  for (const x of A) if (B.has(x)) inter++;
  const uni = A.size + B.size - inter;
  return uni ? inter / uni : 0;
}

function levenshtein(a, b) {
  a = String(a || "");
  b = String(b || "");
  if (a === b) return 0;
  const al = a.length, bl = b.length;
  if (!al) return bl;
  if (!bl) return al;

  // DP (optimizado en 1 fila)
  let prev = new Array(bl + 1);
  let cur = new Array(bl + 1);
  for (let j = 0; j <= bl; j++) prev[j] = j;
  for (let i = 1; i <= al; i++) {
    cur[0] = i;
    const ca = a.charCodeAt(i - 1);
    for (let j = 1; j <= bl; j++) {
      const cost = ca === b.charCodeAt(j - 1) ? 0 : 1;
      cur[j] = Math.min(
        prev[j] + 1,
        cur[j - 1] + 1,
        prev[j - 1] + cost
      );
    }
    [prev, cur] = [cur, prev];
  }
  return prev[bl];
}

function levenshteinSim(a, b) {
  const A = canonicalizeQuery(a);
  const B = canonicalizeQuery(b);
  const maxLen = Math.max(A.length, B.length);
  if (!maxLen) return 0;
  const d = levenshtein(A, B);
  return 1 - d / maxLen;
}

function scoreField(queryRaw, fieldRaw) {
  const q = canonicalizeQuery(queryRaw);
  const f = canonicalizeQuery(fieldRaw);
  if (!q || !f) return 0;

  if (q === f) return 1;
  if (f.includes(q)) return 0.92;
  if (q.includes(f) && f.length >= 4) return 0.85;

  const qt = tokenize(q, { expandSynonyms: true });
  const ft = tokenize(f);
  const jac = jaccard(qt, ft);

  // Similaridad por typos solo si hay suficiente señal
  let lev = 0;
  if (q.length >= 5 && f.length >= 5) lev = levenshteinSim(q, f);

  // Mezcla suave (no sobre-premiar Levenshtein)
  return Math.max(jac, lev * 0.85);
}

function pickBestByScore(scored, mode) {
  if (!scored.length) return [];
  scored.sort((a, b) => b.score - a.score);

  if (mode === "LIST") {
    // Umbral moderado: solo mostrar opciones con coincidencia real
    const strong = scored.filter(x => x.score >= 0.48);
    if (strong.length) return strong.map(x => x.row);

    // Relajación leve si la query trae varios tokens (ej: "camilla facial")
    const qTokens = tokenize(scored[0].q);
    if (qTokens.length >= 2) {
      const relaxed = scored.filter(x => x.score >= 0.38);
      if (relaxed.length) return relaxed.map(x => x.row);
    }
    return [];
  }

  // DETAIL: evitar respuestas equivocadas por ambigüedad
  const best = scored[0];
  const second = scored[1];

  // Si la mejor coincidencia no es suficientemente fuerte, no devolvemos nada.
  if (best.score < 0.72) return [];

  // Si la segunda es muy cercana, está ambiguo → mejor pedir aclaración.
  if (second && (best.score - second.score) < 0.08 && best.score < 0.88) return [];

  return [best.row];
}

function guessQueryFromText(text) {
  const t = canonicalizeQuery(text);

  const stop = [
    "tenes", "tenés", "tienen", "hay", "disponible", "disponibles",
    "precio", "cuanto", "cuánto", "sale", "valen", "vale",
    "me pasas", "me pasás", "mandame", "enviame", "enviáme",
    "foto", "imagen", "fotos", "imagenes", "queria", "quería",
    "hola", "buenas", "por favor", "gracias", "quiero", "quisiera"
  ];

  let cleaned = t;
  for (const w of stop) cleaned = cleaned.replace(new RegExp(`\\b${w}\\b`, "g"), " ");
  cleaned = cleaned.replace(/[^\p{L}\p{N}\s]/gu, " ").replace(/\s+/g, " ").trim();

  return cleaned || t;
}

function moneyOrConsult(price) {
  if (!price) return "consultar";
  const s = String(price).trim();
  return s.includes("$") ? s : `$${s}`;
}

function userAsksForPhoto(text) {
  const t = normalize(text);
  return /(foto|fotos|imagen|imagenes|mostrame|mandame|enviame|ver)/.test(t);
}

function pickModelForText(userText) {
  const t = String(userText || "");
  const long = t.length > 350;
  const hasManyQuestions = (t.match(/\?/g) || []).length >= 2;
  const complexKeywords =
    /(precio|stock|turno|promo|promocion|envio|delivery|reserva|seña|agenda|pago|transferencia|tarjeta|camilla|sillon|espejo|equipamiento|mueble|curso|capacitacion|inscripcion|foto|imagen|audio|champu|champú|shampoo)/i.test(t);

  if (long || hasManyQuestions || complexKeywords) return COMPLEX_MODEL;
  return PRIMARY_MODEL;
}

function getTmpDir() {
  const dir = os.tmpdir();
  try { fs.mkdirSync(dir, { recursive: true }); } catch {}
  return dir;
}

// ===================== DRIVE FILE ID (desde link o desde texto) =====================
function extractDriveFileId(value) {
  const v = String(value || "").trim();
  if (!v) return "";

  // Caso: ya es un ID directo
  if (/^[a-zA-Z0-9_-]{15,}$/.test(v) && !v.includes("http")) return v;

  const m1 = v.match(/drive\.google\.com\/file\/d\/([^/]+)\//i);
  if (m1?.[1]) return m1[1];

  const m2 = v.match(/drive\.google\.com\/open\?id=([^&]+)/i);
  if (m2?.[1]) return m2[1];

  const m3 = v.match(/[?&]id=([^&]+)/i);
  if (m3?.[1]) return m3[1];

  return "";
}

async function downloadDriveFileToPath(fileId, outPath) {
  const drive = await getDriveClient();
  const res = await drive.files.get(
    { fileId, alt: "media" },
    { responseType: "arraybuffer" }
  );
  fs.writeFileSync(outPath, Buffer.from(res.data));
}

// ===================== ✅ IDs de catálogos =====================
const STOCK_SHEET_ID = "1ZepzBhDUl7BlevNSjassowotiR0l_iCB-3ExDaYTW5U";
const SERVICES_SHEET_ID = "19JeiyNLRu31Frt46Md9W7fsI1V42dEncYxPUFNCU0jY";
const COURSES_SHEET_ID = "1kXoX8GeZfJkEPylLG49xbKmwHVIe39vSBDYxsomfsOo";

const STOCK_TABS = ["Productos", "Equipamiento", "Muebles"];
const SERVICES_TAB = "Hoja 1";
const COURSES_TAB = "Hoja 1";

// ✅ CLAVE: leemos hasta Z para incluir “Foto del producto” aunque esté en H/I/J
const STOCK_RANGE = (tab) => `${tab}!A1:Z`;
const SERVICES_RANGE = `${SERVICES_TAB}!A1:Z`;
const COURSES_RANGE = `${COURSES_TAB}!A1:Z`;

// Cache
const CATALOG_CACHE_TTL_MS = Number(process.env.CATALOG_CACHE_TTL_MS || 180000);
const catalogCache = {
  stock: { loadedAt: 0, rows: [] },
  services: { loadedAt: 0, rows: [] },
  courses: { loadedAt: 0, rows: [] },
};

async function readSheetRange(spreadsheetId, range) {
  const sheets = await getSheetsClient();
  const resp = await sheets.spreadsheets.values.get({ spreadsheetId, range });
  return resp.data.values || [];
}

/**
 * ✅ LECTOR CON LINKS (para la columna de foto)
 * Devuelve:
 * - headers: array de headers (fila 1)
 * - rows: array de rows con:
 *   - values: array de strings (formattedValue)
 *   - links: array de links (si la celda tiene hyperlink)
 */
async function readSheetGridWithLinks(spreadsheetId, a1Range) {
  const sheets = await getSheetsClient();

  const resp = await sheets.spreadsheets.get({
    spreadsheetId,
    ranges: [a1Range],
    includeGridData: true,
    fields: "sheets(data(rowData(values(formattedValue,hyperlink,textFormatRuns(format(link(uri)))))))",
  });

  const data = resp.data.sheets?.[0]?.data?.[0];
  const rowData = data?.rowData || [];

  const out = {
    headers: [],
    rows: [],
  };

  if (!rowData.length) return out;

  // Helper para leer link real
  const getLinkFromCell = (cell) => {
    if (!cell) return "";
    if (cell.hyperlink) return cell.hyperlink;

    // A veces el link viene dentro de textFormatRuns
    const runs = cell.textFormatRuns || [];
    for (const r of runs) {
      const uri = r?.format?.link?.uri;
      if (uri) return uri;
    }
    return "";
  };

  // headers = primera fila
  const headerCells = rowData[0]?.values || [];
  out.headers = headerCells.map(c => (c?.formattedValue || "").trim());

  // data rows
  for (let i = 1; i < rowData.length; i++) {
    const cells = rowData[i]?.values || [];
    const values = cells.map(c => (c?.formattedValue || "").trim());
    const links = cells.map(c => getLinkFromCell(c));
    out.rows.push({ values, links });
  }

  return out;
}

// ===================== STOCK CATALOG (con link de foto real) =====================
// Columnas esperadas: Nombre | Categoría | Marca | Precio | Stock | Descripción | Foto del producto
async function getStockCatalog() {
  const now = Date.now();
  if (catalogCache.stock.rows.length && (now - catalogCache.stock.loadedAt) < CATALOG_CACHE_TTL_MS) {
    return catalogCache.stock.rows;
  }

  const all = [];

  for (const tab of STOCK_TABS) {
    // ✅ usamos GRID para poder leer hyperlink real de “Foto del producto”
    const grid = await readSheetGridWithLinks(STOCK_SHEET_ID, STOCK_RANGE(tab));
    if (!grid.headers.length) continue;

    const header = grid.headers;

    const idx = {
      nombre: header.findIndex(h => normalize(h) === "nombre"),
      categoria: header.findIndex(h => normalize(h) === "categoria"),
      marca: header.findIndex(h => normalize(h) === "marca"),
      precio: header.findIndex(h => normalize(h) === "precio"),
      stock: header.findIndex(h => normalize(h) === "stock"),
      descripcion: header.findIndex(h => normalize(h) === "descripcion"),
      foto: header.findIndex(h => normalize(h).includes("foto")), // “Foto del producto”
    };

    for (const row of grid.rows) {
      const r = row.values;
      const links = row.links;

      // ✅ si “foto” es un chip/link, el texto puede ser “sedal.jpg”
      // y el link real viene en links[idx.foto]
      const fotoText = idx.foto >= 0 ? (r[idx.foto] || "").trim() : "";
      const fotoLink = idx.foto >= 0 ? (links[idx.foto] || "").trim() : "";

      const item = {
        tab,
        nombre: idx.nombre >= 0 ? (r[idx.nombre] || "").trim() : "",
        categoria: idx.categoria >= 0 ? (r[idx.categoria] || "").trim() : "",
        marca: idx.marca >= 0 ? (r[idx.marca] || "").trim() : "",
        precio: idx.precio >= 0 ? (r[idx.precio] || "").trim() : "",
        stock: idx.stock >= 0 ? (r[idx.stock] || "").trim() : "",
        descripcion: idx.descripcion >= 0 ? (r[idx.descripcion] || "").trim() : "",
        // ✅ guardamos link real si existe, si no el texto
        foto: fotoLink || fotoText,
      };

      if (item.nombre) all.push(item);
    }
  }

  catalogCache.stock = { loadedAt: now, rows: all };
  return all;
}

// ===================== SERVICES / COURSES (values normal) =====================
async function getServicesCatalog() {
  const now = Date.now();
  if (catalogCache.services.rows.length && (now - catalogCache.services.loadedAt) < CATALOG_CACHE_TTL_MS) {
    return catalogCache.services.rows;
  }

  const values = await readSheetRange(SERVICES_SHEET_ID, SERVICES_RANGE);
  if (!values.length) return [];

  const [header, ...data] = values;

  const idx = {
    nombre: header.findIndex(h => normalize(h) === "nombre"),
    categoria: header.findIndex(h => normalize(h) === "categoria"),
    subcategoria: header.findIndex(h => normalize(h).includes("subcategoria")),
    duracion: header.findIndex(h => normalize(h).includes("duracion")),
    precio: header.findIndex(h => normalize(h) === "precio"),
  };

  const rows = data
    .map(r => ({
      nombre: (r[idx.nombre] || "").trim(),
      categoria: (r[idx.categoria] || "").trim(),
      subcategoria: (r[idx.subcategoria] || "").trim(),
      duracion: (r[idx.duracion] || "").trim(),
      precio: (r[idx.precio] || "").trim(),
    }))
    .filter(x => x.nombre);

  catalogCache.services = { loadedAt: now, rows };
  return rows;
}

async function getCoursesCatalog() {
  const now = Date.now();
  if (catalogCache.courses.rows.length && (now - catalogCache.courses.loadedAt) < CATALOG_CACHE_TTL_MS) {
    return catalogCache.courses.rows;
  }

  const values = await readSheetRange(COURSES_SHEET_ID, COURSES_RANGE);
  if (!values.length) return [];

  const [header, ...data] = values;

  const idx = {
    nombre: header.findIndex(h => normalize(h) === "nombre"),
    categoria: header.findIndex(h => normalize(h) === "categoria"),
    duracionTotal: header.findIndex(h => normalize(h).includes("duracion total")),
    inicio: header.findIndex(h => normalize(h).includes("fecha de inicio")),
    fin: header.findIndex(h => normalize(h).includes("fecha de finalizacion")),
    precio: header.findIndex(h => normalize(h) === "precio"),
    info: header.findIndex(h => normalize(h).includes("informacion detallada")),
  };

  const rows = data
    .map(r => ({
      nombre: (r[idx.nombre] || "").trim(),
      categoria: (r[idx.categoria] || "").trim(),
      duracionTotal: (r[idx.duracionTotal] || "").trim(),
      fechaInicio: (r[idx.inicio] || "").trim(),
      fechaFin: (r[idx.fin] || "").trim(),
      precio: (r[idx.precio] || "").trim(),
      info: (r[idx.info] || "").trim(),
    }))
    .filter(x => x.nombre);

  catalogCache.courses = { loadedAt: now, rows };
  return rows;
}

// ===================== BUSCADORES =====================
function findStock(rows, query, mode) {
  const q = canonicalizeQuery(query);
  if (!q) return [];

  // 1) Atajos: si la query es suficientemente específica y matchea fuerte por nombre
  // (evita gasto y reduce errores)
  const exact = rows.filter(r => canonicalizeQuery(r.nombre) === q);
  if (exact.length) return exact;

  const containsStrong = rows.filter(r => canonicalizeQuery(r.nombre).includes(q) && q.length >= 4);
  if (mode !== "LIST" && containsStrong.length === 1) return containsStrong;

  // 2) Scoring: nombre tiene más peso que categoría/marca/tab.
  const scored = [];
  for (const r of rows) {
    const sNombre = scoreField(q, r.nombre);
    const sCat = scoreField(q, r.categoria) * 0.78;
    const sMarca = scoreField(q, r.marca) * 0.72;
    const sDesc = scoreField(q, r.descripcion) * 0.64;
    const sTab = scoreField(q, r.tab) * 0.65;
    // score combinado (evita que una sola coincidencia débil gane)
    const score = Math.max(sNombre, sCat, sMarca, sDesc, sTab);

    // Pequeño boost si la query es multi-token y aparece en el nombre por tokens
    const qTok = tokenize(q, { expandSynonyms: true });
    if (qTok.length >= 2) {
      const bag = tokenize(`${r.nombre} ${r.categoria} ${r.marca} ${r.descripcion}`);
      const jac = jaccard(qTok, bag);
      if (jac >= 0.5) {
        // boost suave (cap a 1)
        const boosted = Math.min(1, score + 0.07);
        scored.push({ row: r, score: boosted, q });
        continue;
      }
    }

    if (score > 0) scored.push({ row: r, score, q });
  }

  return pickBestByScore(scored, mode);
}

function findServices(rows, query, mode) {
  const q = normalize(query);
  if (!q) return [];

  const match = (x) => {
    const nombre = normalize(x.nombre);
    const categoria = normalize(x.categoria);
    const sub = normalize(x.subcategoria);
    return nombre.includes(q) || categoria.includes(q) || sub.includes(q);
  };

  if (mode === "LIST") return rows.filter(match);

  const exact = rows.filter(r => normalize(r.nombre) === q);
  if (exact.length) return exact;

  const contains = rows.filter(r => normalize(r.nombre).includes(q));
  if (contains.length) return contains;

  return rows.filter(match);
}

function findCourses(rows, query, mode) {
  const q = normalize(query);
  if (!q) return [];

  const match = (x) => {
    const nombre = normalize(x.nombre);
    const categoria = normalize(x.categoria);
    return nombre.includes(q) || categoria.includes(q);
  };

  if (mode === "LIST") return rows.filter(match);

  const exact = rows.filter(r => normalize(r.nombre) === q);
  if (exact.length) return exact;

  const contains = rows.filter(r => normalize(r.nombre).includes(q));
  if (contains.length) return contains;

  return rows.filter(match);
}

// ===================== RESPUESTAS =====================
function formatStockReply(matches, mode) {
  if (!matches.length) return null;

  // Mensaje corto estilo “ficha”
  const items = mode === "LIST" ? matches.slice(0, 10) : matches.slice(0, 1);

  const blocks = items.map((p) => {
    const precio = moneyOrConsult(p.precio);
    // ✅ Por pedido: al listar/opciones o detalle, mostrar SOLO nombre y precio.
    return [
      `✨ *${p.nombre}*`,
      `• Precio: *${precio}*`,
    ].join("\n");
  });

  const header = mode === "LIST"
    ? `Encontré estas opciones:`
    : `Está en catálogo:`;

  const footer = mode === "LIST"
    ? `\n\nPara recomendarle mejor: ¿lo necesita para uso personal o para trabajar? ¿Qué tipo de cabello tiene y qué objetivo busca (alisado, reparación, hidratación, color, rulos)?`
    : `\n\nPara recomendarle mejor: ¿lo necesita para uso personal o para trabajar? ¿Qué tipo de cabello tiene y qué resultado busca?`;

  return `${header}\n\n${blocks.join("\n\n— — —\n\n")}${footer}`.trim();
}

// LISTA COMPLETA (sin filtrar por stock): se manda en varios mensajes para no cortar WhatsApp.
function formatStockListAll(rows, chunkSize = 12) {
  const items = Array.isArray(rows) ? rows.filter(r => r?.nombre) : [];
  if (!items.length) return [];

  const chunks = [];
  for (let i = 0; i < items.length; i += chunkSize) {
    const part = items.slice(i, i + chunkSize);
    const blocks = part.map((p) => {
      const precio = moneyOrConsult(p.precio);
      return [
        `✨ *${p.nombre}*`,
        `• Precio: *${precio}*`,
      ].join("\n");
    });

    const header = i === 0 ? `Catálogo completo:` : `Más opciones del catálogo:`;
    const footer = (i + chunkSize) >= items.length
      ? `\n\nPara recomendarle mejor: ¿lo necesita para uso personal o para trabajar? ¿Qué tipo de cabello tiene y qué objetivo busca (alisado, reparación, hidratación, color, rulos)?`
      : `\n\n(Sigo con más opciones…)`;

    chunks.push(`${header}\n\n${blocks.join("\n\n— — —\n\n")}${footer}`.trim());
  }
  return chunks;
}

function formatServicesReply(matches, mode) {
  if (!matches.length) return null;
  const limited = mode === "LIST" ? matches.slice(0, 10) : matches.slice(0, 3);

  const lines = limited.map(s => {
    const priceTxt = moneyOrConsult(s.precio);
    const durTxt = s.duracion ? ` | ${s.duracion}` : "";
    return `• ${s.nombre}: *${priceTxt}*${durTxt}`;
  });

  const header = mode === "LIST" ? `Servicios:` : `Precio del servicio:`;
  return `${header}\n${lines.join("\n")}`.trim();
}

function formatCoursesReply(matches, mode) {
  if (!matches.length) return null;
  const limited = mode === "LIST" ? matches.slice(0, 10) : matches.slice(0, 3);

  const lines = limited.map(c => {
    const priceTxt = moneyOrConsult(c.precio);
    return `• ${c.nombre}: *${priceTxt}*`;
  });

  const header = mode === "LIST" ? `Cursos:` : `Curso:`;
  return `${header}\n${lines.join("\n")}\n\nSi quiere, le paso requisitos e inscripción.`.trim();
}

// ===================== INTENCIÓN =====================
async function classifyAndExtract(text) {
  const completion = await openai.chat.completions.create({
    model: PRIMARY_MODEL,
    messages: [
      {
        role: "system",
        content:
`Clasificá el mensaje del cliente en JSON estricto.
Tipos:
- PRODUCT
- SERVICE
- COURSE
- OTHER

Además:
- query: lo que hay que buscar (nombre o categoría)
- mode: LIST si pide opciones/lista; DETAIL si pide algo puntual.

Respondé SOLO JSON.`
      },
      { role: "user", content: text }
    ],
    response_format: { type: "json_object" },
  });

  try {
    const obj = JSON.parse(completion.choices[0].message.content);
    return {
      type: obj.type || "OTHER",
      query: (obj.query || "").trim(),
      mode: obj.mode || "DETAIL",
    };
  } catch {
    return { type: "OTHER", query: "", mode: "DETAIL" };
  }
}

// ===================== WHATSAPP SEND =====================
async function sendWhatsAppText(to, text) {
const body = String(text || "");
const dedupKey = `${to}::${body}`;
const now = Date.now();
const prevTs = lastSentOutByPeer.get(dedupKey) || 0;
if (body && (now - prevTs) < OUT_DEDUP_MS) {
  // No reenviar el mismo texto en ventana corta
  return { deduped: true };
}
lastSentOutByPeer.set(dedupKey, now);

  const resp = await axios.post(
    `https://graph.facebook.com/v19.0/${process.env.PHONE_NUMBER_ID}/messages`,
    { messaging_product: "whatsapp", to, text: { body: text } },
    {
      headers: {
        Authorization: `Bearer ${process.env.WHATSAPP_TOKEN}`,
        "Content-Type": "application/json",
      },
    }
  );

  const wa_msg_id = resp?.data?.messages?.[0]?.id || null;

  // ✅ Guardar mensaje SALIENTE (OUT) para que el panel lo vea
  dbInsertMessage({
    direction: "out",
    wa_peer: to,
    name: null,
    text,
    msg_type: "text",
    wa_msg_id,
    raw: resp?.data || {},
  });

  return resp.data;
}

async function sendWhatsAppImageById(to, mediaId, caption) {
  await axios.post(
    `https://graph.facebook.com/v19.0/${process.env.PHONE_NUMBER_ID}/messages`,
    {
      messaging_product: "whatsapp",
      to,
      type: "image",
      image: { id: mediaId, ...(caption ? { caption } : {}) },
    },
    {
      headers: {
        Authorization: `Bearer ${process.env.WHATSAPP_TOKEN}`,
        "Content-Type": "application/json",
      },
    }
  );
  // ✅ Guardar imagen enviada (OUT)
  dbInsertMessage({
    direction: "out",
    wa_peer: to,
    name: null,
    text: caption || "",
    msg_type: "image",
    wa_msg_id: mediaId || null,
    raw: { sent: true, media: { id: mediaId, filename: `out-${mediaId}.jpg` } },
  });
}

// ===================== WHATSAPP MEDIA DOWNLOAD (incoming) =====================
async function getWhatsAppMediaUrl(mediaId) {
  const resp = await axios.get(`https://graph.facebook.com/v19.0/${mediaId}`, {
    headers: { Authorization: `Bearer ${process.env.WHATSAPP_TOKEN}` },
    params: { fields: "url,mime_type" },
  });
  return resp.data;
}

async function downloadWhatsAppMediaToFile(url, outPath) {
  const resp = await axios.get(url, {
    responseType: "arraybuffer",
    headers: { Authorization: `Bearer ${process.env.WHATSAPP_TOKEN}` },
  });
  fs.writeFileSync(outPath, Buffer.from(resp.data));
}

// ===================== UPLOAD MEDIA TO WHATSAPP (outgoing photo) =====================
async function uploadMediaToWhatsApp(filePath, mimeType) {
  // Intento 1: FormData nativo (Node 18+)
  if (globalThis.FormData && globalThis.Blob) {
    const form = new FormData();
    form.append("messaging_product", "whatsapp");
    form.append("type", mimeType || "image/jpeg");
    const buf = fs.readFileSync(filePath);
    form.append("file", new Blob([buf], { type: mimeType || "image/jpeg" }), path.basename(filePath));

    const resp = await fetch(
      `https://graph.facebook.com/v19.0/${process.env.PHONE_NUMBER_ID}/media`,
      {
        method: "POST",
        headers: { Authorization: `Bearer ${process.env.WHATSAPP_TOKEN}` },
        body: form,
      }
    );

    const data = await resp.json();
    if (!resp.ok) throw new Error(`WhatsApp media upload failed: ${JSON.stringify(data)}`);
    return data.id;
  }

  // Intento 2: fallback (si tu Node no trae FormData). Requiere: npm i form-data
  let FormDataPkg;
  try {
    FormDataPkg = require("form-data");
  } catch {
    throw new Error("Tu Node no soporta FormData/Blob. Solución: instalar 'form-data' con: npm i form-data");
  }

  const form = new FormDataPkg();
  form.append("messaging_product", "whatsapp");
  form.append("type", mimeType || "image/jpeg");
  form.append("file", fs.createReadStream(filePath));

  const resp = await axios.post(
    `https://graph.facebook.com/v19.0/${process.env.PHONE_NUMBER_ID}/media`,
    form,
    {
      headers: {
        Authorization: `Bearer ${process.env.WHATSAPP_TOKEN}`,
        ...form.getHeaders(),
      },
    }
  );

  return resp.data.id;
}

// ===================== AUDIO -> TRANSCRIBE =====================
async function transcribeAudioFile(filePath) {
  const transcription = await openai.audio.transcriptions.create({
    model: TRANSCRIBE_MODEL,
    file: fs.createReadStream(filePath),
  });
  return (transcription.text || "").trim();
}

// ===================== IMAGE -> VISION =====================
function fileToDataUrl(filePath, mimeType) {
  const bytes = fs.readFileSync(filePath);
  const b64 = bytes.toString("base64");
  const mt = mimeType || "image/jpeg";
  return `data:${mt};base64,${b64}`;
}
async function describeImageWithVision(dataUrl) {
  const resp = await openai.chat.completions.create({
    model: COMPLEX_MODEL,
    messages: [
      { role: "system", content: "Describí la imagen en español y extraé info útil para atender al cliente." },
      {
        role: "user",
        content: [
          { type: "text", text: "Describí esta imagen:" },
          { type: "image_url", image_url: { url: dataUrl } },
        ],
      },
    ],
  });
  return (resp.choices?.[0]?.message?.content || "").trim();
}

// ===================== INPUT MULTIMODAL =====================
async function extractTextFromIncomingMessage(msg) {
  if (msg.type === "text") {
    return { text: msg.text?.body || "", kind: "text" };
  }

  if (msg.type === "audio") {
    const mediaId = msg.audio?.id;
    if (!mediaId) return { text: "", kind: "audio_no_id" };

    const tmpFile = path.join(getTmpDir(), `wa-audio-${mediaId}.ogg`);
    const mediaInfo = await getWhatsAppMediaUrl(mediaId);
    await downloadWhatsAppMediaToFile(mediaInfo.url, tmpFile);

    const transcript = await transcribeAudioFile(tmpFile);
    try { fs.unlinkSync(tmpFile); } catch {}
    return { text: transcript, kind: "audio" };
  }

        if (msg.type === "document") {
        const mediaId = msg.document?.id;
        const caption = msg.document?.caption || "";
        const filename = msg.document?.filename || "";
        if (!mediaId) return { text: caption, kind: "document_no_id" };

        const mediaInfo = await getWhatsAppMediaUrl(mediaId);
        const mime = mediaInfo.mime_type || "";
        const ext = mime.includes("pdf") ? ".pdf" : (mime.includes("png") ? ".png" : ".bin");

        // tmp (para parse)
        const tmpFile = path.join(getTmpDir(), `wa-doc-${mediaId}${ext}`);
        await downloadWhatsAppMediaToFile(mediaInfo.url, tmpFile);

        // ✅ persistente (para el panel)
        const savedName = `in-${mediaId}${ext}`;
        const savedPath = path.join(MEDIA_DIR, savedName);
        try { fs.copyFileSync(tmpFile, savedPath); } catch {}

        let extractedTxt = "";

        // PDF: intentamos extraer texto
        if (mime.includes("pdf")) {
          const buf = fs.readFileSync(tmpFile);
          extractedTxt = await tryParsePdfBuffer(buf);
        } else if (mime.startsWith("text/")) {
          try {
            extractedTxt = fs.readFileSync(tmpFile, "utf-8");
          } catch {}
        }

        // Si no pudimos extraer, pedimos una captura
        if (!extractedTxt && mime.includes("pdf")) {
          try { fs.unlinkSync(tmpFile); } catch {}
          return {
            text: [
              caption ? `Texto adjunto del cliente: "${caption}"` : "",
              filename ? `Documento: ${filename}` : "",
              "El documento es PDF. Para poder leerlo mejor, por favor envíe una captura (imagen) de la parte importante o copie el texto aquí.",
            ].filter(Boolean).join("\n"),
            kind: "document_pdf_no_text",
            media: { id: mediaId, mime_type: mime, filename: savedName },
          };
        }

        const combined = [
          caption ? `Texto adjunto del cliente: "${caption}"` : "",
          filename ? `Documento: ${filename}` : "",
          extractedTxt ? `Texto del documento: ${extractedTxt.slice(0, 6000)}` : "",
        ].filter(Boolean).join("\n");

        try { fs.unlinkSync(tmpFile); } catch {}

        return {
          text: combined,
          kind: "document",
          media: { id: mediaId, mime_type: mime, filename: savedName },
        };
      }

      if (msg.type === "image") {
  const mediaId = msg.image?.id;
  const caption = msg.image?.caption || "";
  if (!mediaId) return { text: caption, kind: "image_no_id" };

  const mediaInfo = await getWhatsAppMediaUrl(mediaId);
  const ext = (mediaInfo.mime_type || "").includes("png") ? ".png" : ".jpg";

  // tmp (para vision)
  const tmpFile = path.join(getTmpDir(), `wa-image-${mediaId}${ext}`);
  await downloadWhatsAppMediaToFile(mediaInfo.url, tmpFile);

  // ✅ persistente (para el panel)
  const savedName = `in-${mediaId}${ext}`;
  const savedPath = path.join(MEDIA_DIR, savedName);
  try { fs.copyFileSync(tmpFile, savedPath); } catch {}

  // vision
  const dataUrl = fileToDataUrl(tmpFile, mediaInfo.mime_type);
  const description = await describeImageWithVision(dataUrl);
  try { fs.unlinkSync(tmpFile); } catch {}

  const combined = [
    caption ? `Texto adjunto del cliente: "${caption}"` : "",
    description ? `Descripción de la imagen: ${description}` : "",
  ].filter(Boolean).join("\n");

  // ✅ devolvemos también metadata para guardar en DB
  return {
    text: combined,
    kind: "image",
    media: {
      id: mediaId,
      mime_type: mediaInfo.mime_type || "",
      filename: savedName,
    }
  };
}

return { text: "", kind: `ignored_${msg.type}` };
}

// ===================== ENVIAR FOTO DEL PRODUCTO (DESCARGA DRIVE -> SUBE WHATSAPP) =====================
async function maybeSendProductPhoto(phone, product, userText) {
  if (!product) return false;
  if (!userAsksForPhoto(userText)) return false;

  const driveFileId = extractDriveFileId(product.foto);

  if (!driveFileId) {
    await sendWhatsAppText(
      phone,
      "No tengo la foto vinculada correctamente en la columna “Foto del producto”. Si quiere, le paso alternativas o la descripción."
    );
    return true;
  }

  const tmpPath = path.join(getTmpDir(), `prod-${driveFileId}.jpg`);

  try {
    await downloadDriveFileToPath(driveFileId, tmpPath);

    const mediaId = await uploadMediaToWhatsApp(tmpPath, "image/jpeg");

    // ✅ copiar a media para el panel
    try {
      const savedName = `out-${mediaId}.jpg`;
      fs.copyFileSync(tmpPath, path.join(MEDIA_DIR, savedName));
    } catch {}

    const caption = [
      product.nombre || "Producto",
      product.precio ? `Precio: ${moneyOrConsult(product.precio)}` : "",
    ].filter(Boolean).join(" | ");

    await sendWhatsAppImageById(phone, mediaId, caption);
    return true;
  } catch (e) {
    console.error("❌ Error enviando foto:", e?.response?.data || e?.message || e);
    await sendWhatsAppText(
      phone,
      "No pude enviar la foto en este momento. Revise que la imagen de Drive esté compartida con el service account."
    );
    return true;
  } finally {
    try { fs.unlinkSync(tmpPath); } catch {}
  }
}

// ===================== WEBHOOK VERIFY =====================
app.get("/webhook", (req, res) => {
  const mode = req.query["hub.mode"];
  const token = req.query["hub.verify_token"];
  const challenge = req.query["hub.challenge"];

  if (mode === "subscribe" && token === process.env.VERIFY_TOKEN) {
    return res.status(200).send(challenge);
  }
  return res.sendStatus(403);
});

// ===================== HEALTH =====================
app.get("/health", (req, res) => {
  res.json({
    ok: true,
    timeAR: nowARString(),
    tmpDir: getTmpDir(),
    models: { primary: PRIMARY_MODEL, complex: COMPLEX_MODEL, transcribe: TRANSCRIBE_MODEL },
  });
});

// ===================== WEBHOOK =====================
app.post("/webhook", async (req, res) => {
  res.sendStatus(200);

  try {
    const value = req.body.entry?.[0]?.changes?.[0]?.value;
    const msg = value?.messages?.[0];
    const contact = value?.contacts?.[0];
    if (!msg) return;

    // dedupe
    if (msg.id && processedMsgIds.has(msg.id)) return;
    if (msg.id) {
      processedMsgIds.add(msg.id);
      if (processedMsgIds.size > 5000) processedMsgIds.clear();
    }

    const phoneRaw = msg.from;
    const phone = phoneRaw?.startsWith("549") ? "54" + phoneRaw.slice(3) : phoneRaw;
    const waId = contact?.wa_id || phoneRaw;
    const name = contact?.profile?.name || "";

    
// ✅ INACTIVIDAD: si el cliente habló, cancelamos timers anteriores (si existían)
if (inactivityTimers.has(waId)) {
  clearTimeout(inactivityTimers.get(waId));
  inactivityTimers.delete(waId);
}
if (closeTimers.has(waId)) {
  clearTimeout(closeTimers.get(waId));
  closeTimers.delete(waId);
}

    // texto / audio / imagen
    const extracted = await extractTextFromIncomingMessage(msg);
    const text = (extracted.text || "").trim();
    const mediaMeta = extracted.media || null;


// ✅ Contexto para seguimiento al cierre (se actualiza durante la conversación)
lastCloseContext.set(waId, {
  phone,
  name,
  lastUserText: text,
  intentType: "OTHER",
  interest: null,
});

    // ✅ Guardar mensaje ENTRANTE (IN) para que el panel lo vea
    dbInsertMessage({
      direction: "in",
      wa_peer: phone, // ✅ SIEMPRE el normalizado (no phoneRaw)
      name,
      text,
      msg_type: msg.type,
      wa_msg_id: msg.id,
      raw: {
        webhook: req.body,
        media: mediaMeta,
      },
    });

    if (!text) {
      await sendWhatsAppText(phone, "¿Me lo puede enviar en texto, audio o imagen? Así lo reviso 😊");
      // ✅ INACTIVIDAD: programar follow-up luego de la respuesta del bot
      scheduleInactivityFollowUp(waId, phone);
      return;
    }

    pushHistory(waId, "user", text);

    
// lead detection
const interest = await detectInterest(text);
if (interest) dailyLeads.set(phone, { name, interest });

// ✅ guardar interés en el contexto de cierre
const ctx0 = lastCloseContext.get(waId);
if (ctx0) ctx0.interest = interest || ctx0.interest;

    // Si piden foto sin decir cuál: usar último producto
    if (userAsksForPhoto(text)) {
      const last = lastProductByUser.get(waId);
      if (last) {
        const sent = await maybeSendProductPhoto(phone, last, text);
        if (sent) {
          // ✅ INACTIVIDAD: programar follow-up luego de la respuesta del bot
          scheduleInactivityFollowUp(waId, phone);
          return;
        }
      }
    }


    // ===================== ✅ REGLAS ESPECIALES (sin inventar servicios) =====================
    const normTxt = normalize(text);

    // Corte masculino: solo por orden de llegada (no se toma turno)
    if (/(\bcorte\b.*\b(mascul|varon|hombre)\b|\bcorte\s+masculino\b|\bbarber\b|\bbarberia\b)/i.test(normTxt)) {
      const msgMasc = `✂️ Corte masculino: es SOLO por orden de llegada (no se toma turno).

🕒 Horarios: Lunes a Sábados 10 a 13 hs y 17 a 22 hs.
💲 Precio final: $10.000.`;
      pushHistory(waId, "assistant", msgMasc);
      await sendWhatsAppText(phone, msgMasc);
      scheduleInactivityFollowUp(waId, phone);
      return;
    }

    // Horario del salón comercial
    if (/(horario|horarios|abren|abrir|cierran|cerrar)\b.*(sal[oó]n|local)|\bhorario\b.*\bcomercial\b/i.test(normTxt)) {
      const msgHor = `🕒 Horario del salón comercial: Lunes a Viernes de 17 a 22 hs.`;
      pushHistory(waId, "assistant", msgHor);
      await sendWhatsAppText(phone, msgHor);
      scheduleInactivityFollowUp(waId, phone);
      return;
    }

    // ===================== ✅ TURNOS (agendar + planilla) =====================
    // 1) Si hay una reserva "en curso" (faltaban datos), este mensaje puede ser solo fecha/hora/servicio
    if (pendingTurnos.has(waId) && !(isYesNoShortReply(text) && lastAssistantWasQuestion(waId))) {
      const prev = pendingTurnos.get(waId) || {};
const turno = await extractTurnoFromText({ text, customerName: name, context: prev });

// ✅ Si estábamos esperando nombre/teléfono (y el mensaje no parece "pedido de turno"), igual lo tomamos
if (!turno?.ok && prev?.awaiting_contact) {
  const ci0 = extractContactInfo(text);
  const merged0 = {
    ...prev,
    cliente_full: prev.cliente_full || (ci0.nombre || ""),
    telefono_contacto: prev.telefono_contacto || (ci0.telefono || ""),
    senado: Boolean(prev.senado) || detectSenaPaid({ text, msgType: msg.type }),
  };

  if (!merged0.cliente_full || !merged0.telefono_contacto) {
    pendingTurnos.set(waId, merged0);
    const msgDatos2 = `${maybeTurnoInfoBlock(waId)}${pedirDatosRegistroTurnoBlock()}`.trim();
    pushHistory(waId, "assistant", msgDatos2);
    await sendWhatsAppText(phone, msgDatos2);
    scheduleInactivityFollowUp(waId, phone);
    return;
  }

  // Ya tenemos todo: registramos el turno usando lo que había quedado guardado
  const isColorJob0 = isColorOrTinturaService(merged0.servicio) || isColorOrTinturaService(merged0.notas || "");
  const servicioToSave0 = isColorJob0 ? `${merged0.servicio} (A CONFIRMAR)` : merged0.servicio;

  // Conflicto de Calendar
  try {
    const conflict0 = await calendarHasConflict({
      dateYMD: merged0.fecha,
      startHM: merged0.hora,
      durationMin: merged0.duracion_min,
    });
    if (conflict0) {
      const diaC0 = weekdayEsFromYMD(merged0.fecha);
      const msgBusy0 = `Ese horario ya está ocupado (${diaC0} ${merged0.fecha} ${merged0.hora}). ¿Le sirve otro horario?`;
      pushHistory(waId, "assistant", msgBusy0);
      await sendWhatsAppText(phone, msgBusy0);
      scheduleInactivityFollowUp(waId, phone);
      return;
    }
  } catch (e) {
    console.error("Error chequeando conflicto de calendar:", e?.response?.data || e?.message || e);
  }

  // Crear evento si NO es color/tintura
  let eventId0 = "";
  if (!isColorJob0) {
    try {
      const created0 = await createCalendarTurno({
        dateYMD: merged0.fecha,
        startHM: merged0.hora,
        durationMin: merged0.duracion_min,
        cliente: merged0.cliente_full || name || "",
        telefono: merged0.telefono_contacto || phone,
        servicio: merged0.servicio,
        notas: merged0.notas || "",
      });
      eventId0 = created0?.eventId || "";
    } catch (e) {
      console.error("❌ Error creando evento en calendar:", e?.response?.data || e?.message || e);
    }
  }

  // Guardar en sheet
  try {
    const dia0 = weekdayEsFromYMD(merged0.fecha);
    await appendTurnoRow({
      fechaYMD: merged0.fecha,
      dia: dia0,
      horaHM: merged0.hora,
      cliente: merged0.cliente_full || name || "",
      telefono: merged0.telefono_contacto || phone,
      servicio: merged0.senado ? `${servicioToSave0} - SEÑADO` : servicioToSave0,
      duracionMin: merged0.duracion_min,
      calendarEventId: eventId0,
    });
  } catch (e) {
    console.error("❌ Error guardando turno en sheet:", e?.response?.data || e?.message || e);
    await sendWhatsAppText(phone, "Tuve un problema registrando el turno en la planilla. ¿Me confirma fecha, hora y servicio para reintentar?");
    scheduleInactivityFollowUp(waId, phone);
    return;
  }

  pendingTurnos.delete(waId);
  const diaOk0 = weekdayEsFromYMD(merged0.fecha);
  const extra0 = !CALENDAR_ID
    ? "\n\nQuedó registrado. Si quiere, también le confirmo el turno por acá el mismo día."
    : "";
  if (isColorJob0) {
    const senaTag0 = merged0.senado ? "\n\nEstado: *SEÑADO* ✅" : "";
    const msgPend0 = `🕒 Solicitud de turno recibida (color/tintura):\n• ${merged0.servicio}\n• ${diaOk0} ${merged0.fecha} ${merged0.hora}${senaTag0}\n\nQueda en confirmar: reviso con la estilista ${TURNOS_STYLIST_NAME} si puede en ese horario y le aviso por acá.${maybeTurnoInfoBlock(waId)}`.trim();
    pushHistory(waId, "assistant", msgPend0);
    await sendWhatsAppText(phone, msgPend0);
    scheduleInactivityFollowUp(waId, phone);
    return;
  }

  const senaTagOk0 = merged0.senado ? "\n\nEstado: *SEÑADO* ✅" : "";
  const msgOk0 = `✅ Turno reservado:\n• ${merged0.servicio}\n• ${diaOk0} ${merged0.fecha} ${merged0.hora}${extra0}${senaTagOk0}${maybeTurnoInfoBlock(waId)}`.trim();
  pushHistory(waId, "assistant", msgOk0);
  await sendWhatsAppText(phone, msgOk0);
  scheduleInactivityFollowUp(waId, phone);
  return;
}

if (turno?.ok) {
        const merged = {
          fecha: turno.fecha || prev.fecha || "",
          hora: turno.hora || prev.hora || "",
          servicio: turno.servicio || prev.servicio || "",
          duracion_min: Number(turno.duracion_min || prev.duracion_min || 60) || 60,
          notas: turno.notas || prev.notas || "",
        };

        merged.fecha = toYMD(merged.fecha);

        const falt = new Set(turno.faltantes || []);
        if (!merged.fecha) falt.add("fecha");
        if (!merged.hora) falt.add("hora");
        if (!merged.servicio) falt.add("servicio");


        // ✅ Autocompletar servicio si el cliente lo consultó recién
        if (falt.has("servicio")) {
          const lastSvc = lastServiceByUser.get(waId);
          if (lastSvc && (Date.now() - (lastSvc.ts || 0)) < LAST_SERVICE_TTL_MS) {
            merged.servicio = merged.servicio || lastSvc.nombre || "";
            if (merged.servicio) falt.delete("servicio");
          }
        }

// ✅ Capturar nombre/teléfono y estado de seña (si lo manda en cualquier orden)
const ci = extractContactInfo(text);
merged.cliente_full = merged.cliente_full || prev.cliente_full || (ci.nombre || "");
merged.telefono_contacto = merged.telefono_contacto || prev.telefono_contacto || (ci.telefono || "");
merged.senado = Boolean(prev.senado) || detectSenaPaid({ text, msgType: msg.type });

        if (falt.size) {
          pendingTurnos.set(waId, merged);
          const pedir = [];
          if (falt.has("servicio")) pedir.push("• ¿Qué servicio desea? (Escríbalo como figura en nuestra lista. Ej: Alisado)");
          if (falt.has("fecha")) pedir.push("• ¿Para qué fecha? (ej: 20/02 o mañana)");
          if (falt.has("hora")) pedir.push("• ¿En qué horario? (ej: 10:30)");
          const msgFalt = `${maybeTurnoInfoBlock(waId)}\n\nPara reservar el turno necesito:\n${pedir.join("\n")}`.trim();
          pushHistory(waId, "assistant", msgFalt);
          await sendWhatsAppText(phone, msgFalt);
          scheduleInactivityFollowUp(waId, phone);
          return;
        }


// ✅ Antes de registrar, pedir nombre completo y teléfono de contacto
// Nota: el teléfono lo tomamos del WhatsApp si no lo envía explícito.
merged.telefono_contacto = merged.telefono_contacto || phone || "";
merged.cliente_full = merged.cliente_full || name || "";

// Solo pedimos nombre si realmente falta (evita repetir mensajes)
if (!merged.cliente_full) {
  pendingTurnos.set(waId, { ...merged, awaiting_contact: true });
  const msgSoloNombre = `Para registrar el turno, por favor envíe:\n• Nombre completo (nombre y apellido)`;
  pushHistory(waId, "assistant", msgSoloNombre);
  await sendWhatsAppText(phone, msgSoloNombre);
  scheduleInactivityFollowUp(waId, phone);
  return;
}


        const isColorJob = isColorOrTinturaService(merged.servicio) || isColorOrTinturaService(merged.notas);
        const servicioToSave = isColorJob ? `${merged.servicio} (A CONFIRMAR)` : merged.servicio;

        // 1) Chequeo de conflicto en Calendar (si está configurado)
        try {
          const conflict = await calendarHasConflict({
            dateYMD: merged.fecha,
            startHM: merged.hora,
            durationMin: merged.duracion_min,
          });
          if (conflict) {
            const diaC = weekdayEsFromYMD(merged.fecha);
            const msgBusy = `Ese horario ya está ocupado (${diaC} ${ymdToDMY(merged.fecha)} ${merged.hora}). ¿Le sirve otro horario?`;
            pushHistory(waId, "assistant", msgBusy);
            await sendWhatsAppText(phone, msgBusy);
            scheduleInactivityFollowUp(waId, phone);
            return;
          }
        } catch (e) {
          console.error("Error chequeando conflicto de calendar:", e?.response?.data || e?.message || e);
        }

        // 2) Crear evento (si NO es color/tintura)
        let eventId = "";
        if (!isColorJob) {
          try {
            const created = await createCalendarTurno({
              dateYMD: merged.fecha,
              startHM: merged.hora,
              durationMin: merged.duracion_min,
              cliente: name || "",
              telefono: phone,
              servicio: merged.servicio,
              notas: merged.notas,
            });
            eventId = created?.eventId || "";
          } catch (e) {
            console.error("❌ Error creando evento en calendar:", e?.response?.data || e?.message || e);
          }
        }

        // 3) Anotar en planilla
        try {
          const dia = weekdayEsFromYMD(merged.fecha);
          await appendTurnoRow({
            fechaYMD: merged.fecha,
            dia,
            horaHM: merged.hora,
            cliente: merged.cliente_full || name || "",
            telefono: merged.telefono_contacto || phone,
            servicio: merged.senado ? `${servicioToSave} - SEÑADO` : servicioToSave,
            duracionMin: merged.duracion_min,
            calendarEventId: eventId,
          });
        } catch (e) {
          console.error("❌ Error guardando turno en sheet:", e?.response?.data || e?.message || e);
          await sendWhatsAppText(phone, "Tuve un problema registrando el turno en la planilla. ¿Me confirma fecha, hora y servicio para reintentar?");
          scheduleInactivityFollowUp(waId, phone);
          return;
        }

        pendingTurnos.delete(waId);
        const diaOk = weekdayEsFromYMD(merged.fecha);
        const extra = !CALENDAR_ID
          ? "\n\nQuedó registrado. Si quiere, también le confirmo el turno por acá el mismo día."
          : "";
        if (isColorJob) {
          const senaTag = merged.senado ? "\n\nEstado: *SEÑADO* ✅" : "";
          const msgPend = `🕒 Solicitud de turno recibida (color/tintura):\n• ${merged.servicio}\n• ${diaOk} ${ymdToDMY(merged.fecha)} ${merged.hora}${senaTag}\n\nQueda en confirmar: reviso con la estilista ${TURNOS_STYLIST_NAME} si puede en ese horario y le aviso por acá.${maybeTurnoInfoBlock(waId)}`.trim();
          pushHistory(waId, "assistant", msgPend);
          await sendWhatsAppText(phone, msgPend);
          scheduleInactivityFollowUp(waId, phone);
          return;
        }

        const senaTag2 = merged.senado ? "\n\nEstado: *SEÑADO* ✅" : "";
        const msgOk = `✅ Turno reservado:\n• ${merged.servicio}\n• ${diaOk} ${ymdToDMY(merged.fecha)} ${merged.hora}${extra}${senaTag2}${maybeTurnoInfoBlock(waId)}`.trim();
        pushHistory(waId, "assistant", msgOk);
        await sendWhatsAppText(phone, msgOk);
        scheduleInactivityFollowUp(waId, phone);
        return;
      }
    }

    // 2) Nuevo pedido de turno (mensaje que menciona turno/reserva/agendar)
    const looksLikeTurno = /(\bturno\b|\breserv\w*\b|\bagend\w*\b|\bcita\b)/i.test(text);
    if (looksLikeTurno && !(isYesNoShortReply(text) && lastAssistantWasQuestion(waId))) {
      const turno = await extractTurnoFromText({ text, customerName: name, context: {} });
      if (turno?.ok) {
        const falt = new Set(turno.faltantes || []);
        if (!turno.fecha) falt.add("fecha");
        if (!turno.hora) falt.add("hora");
        if (!turno.servicio) falt.add("servicio");


        // ✅ Si el cliente recién consultó precio de un servicio, lo usamos como default
        if (falt.has("servicio")) {
          const lastSvc = lastServiceByUser.get(waId);
          if (lastSvc && (Date.now() - (lastSvc.ts || 0)) < LAST_SERVICE_TTL_MS) {
            turno.servicio = turno.servicio || lastSvc.nombre || "";
            if (turno.servicio) falt.delete("servicio");
          }
        }
        if (falt.size) {
          pendingTurnos.set(waId, {
            fecha: turno.fecha || "",
            hora: turno.hora || "",
            servicio: turno.servicio || "",
            duracion_min: Number(turno.duracion_min || 60) || 60,
            notas: turno.notas || "",
            cliente_full: "",
            telefono_contacto: "",
            senado: detectSenaPaid({ text, msgType: msg.type }),
            awaiting_contact: false,
          });
          const pedir = [];
          if (falt.has("servicio")) pedir.push("• ¿Qué servicio desea? (Escríbalo como figura en nuestra lista. Ej: Alisado)");
          if (falt.has("fecha")) pedir.push("• ¿Para qué fecha? (ej: 20/02 o mañana)");
          if (falt.has("hora")) pedir.push("• ¿En qué horario? (ej: 10:30)");
          const msgFalt = `${maybeTurnoInfoBlock(waId)}\n\nPara reservar el turno necesito:\n${pedir.join("\n")}`.trim();
          pushHistory(waId, "assistant", msgFalt);
          await sendWhatsAppText(phone, msgFalt);
          scheduleInactivityFollowUp(waId, phone);
          return;
        }

        // Si ya vino completo en un solo mensaje, lo procesamos reutilizando el flujo de "pending".
        pendingTurnos.set(waId, {
          fecha: turno.fecha,
          hora: turno.hora,
          servicio: turno.servicio,
          duracion_min: turno.duracion_min,
          notas: turno.notas,
        });

        // Re-ejecutamos el bloque de arriba con el mismo texto (completísimo) sin duplicar código:
        // → simulamos que “hay pending” y seguimos inmediatamente.
        const prev = pendingTurnos.get(waId) || {};
        const merged = {
          fecha: prev.fecha || "",
          hora: prev.hora || "",
          servicio: prev.servicio || "",
          duracion_min: Number(prev.duracion_min || 60) || 60,
          notas: prev.notas || "",
        };

        merged.fecha = toYMD(merged.fecha);


// ✅ Antes de registrar, pedir nombre completo y teléfono de contacto
// ✅ Completar datos de contacto automáticamente (si no los mandó, usamos el número de WhatsApp)
Object.assign(merged, mergeContactIntoTurno({ turno: merged, text, waPhone: phone }));

// ✅ IA: si el mensaje parece comprobante (imagen/PDF/texto), marcar SEÑADO y guardar nombre si aparece
if (!merged.senado) {
  const pagoAI = await extractPagoInfoWithAI(text);
  if (pagoAI?.es_comprobante) {
    merged.senado = true;
    if (pagoAI.pagador && !merged.cliente_full) merged.cliente_full = pagoAI.pagador;
  }
}


if (!merged.cliente_full || !merged.telefono_contacto) {
  pendingTurnos.set(waId, { ...merged, awaiting_contact: true });
  const msgDatos = `${maybeTurnoInfoBlock(waId)}${pedirDatosRegistroTurnoBlock()}`.trim();
  pushHistory(waId, "assistant", msgDatos);
  await sendWhatsAppText(phone, msgDatos);
  scheduleInactivityFollowUp(waId, phone);
  return;
}

        const isColorJob = isColorOrTinturaService(merged.servicio) || isColorOrTinturaService(merged.notas);
        const servicioToSave = isColorJob ? `${merged.servicio} (A CONFIRMAR)` : merged.servicio;

        // Conflicto Calendar
        try {
          const conflict = await calendarHasConflict({
            dateYMD: merged.fecha,
            startHM: merged.hora,
            durationMin: merged.duracion_min,
          });
          if (conflict) {
            const diaC = weekdayEsFromYMD(merged.fecha);
            const msgBusy = `Ese horario ya está ocupado (${diaC} ${ymdToDMY(merged.fecha)} ${merged.hora}). ¿Le sirve otro horario?`;
            pushHistory(waId, "assistant", msgBusy);
            await sendWhatsAppText(phone, msgBusy);
            scheduleInactivityFollowUp(waId, phone);
            return;
          }
        } catch (e) {
          console.error("Error chequeando conflicto de calendar:", e?.response?.data || e?.message || e);
        }

        // Crear evento (si NO es color/tintura)
        let eventId = "";
        if (!isColorJob) {
          try {
            const created = await createCalendarTurno({
              dateYMD: merged.fecha,
              startHM: merged.hora,
              durationMin: merged.duracion_min,
              cliente: name || "",
              telefono: phone,
              servicio: merged.servicio,
              notas: merged.notas,
            });
            eventId = created?.eventId || "";
          } catch (e) {
            console.error("❌ Error creando evento en calendar:", e?.response?.data || e?.message || e);
          }
        }

        // Guardar en sheet
        try {
          const dia = weekdayEsFromYMD(merged.fecha);
          await appendTurnoRow({
            fechaYMD: merged.fecha,
            dia,
            horaHM: merged.hora,
            cliente: merged.cliente_full || name || "",
            telefono: merged.telefono_contacto || phone,
            servicio: merged.senado ? `${servicioToSave} - SEÑADO` : servicioToSave,
            duracionMin: merged.duracion_min,
            calendarEventId: eventId,
          });
        } catch (e) {
          console.error("❌ Error guardando turno en sheet:", e?.response?.data || e?.message || e);
          await sendWhatsAppText(phone, "Tuve un problema registrando el turno en la planilla. ¿Me confirma fecha, hora y servicio para reintentar?");
          scheduleInactivityFollowUp(waId, phone);
          return;
        }

        pendingTurnos.delete(waId);
        const diaOk = weekdayEsFromYMD(merged.fecha);
        const extra = !CALENDAR_ID
          ? "\n\nQuedó registrado. Si quiere, también le confirmo el turno por acá el mismo día."
          : "";
        if (isColorJob) {
          const senaTag = merged.senado ? "\n\nEstado: *SEÑADO* ✅" : "";
        const msgPend = `🕒 Solicitud de turno recibida (color/tintura):\n• ${merged.servicio}\n• ${diaOk} ${ymdToDMY(merged.fecha)} ${merged.hora}${senaTag}\n\nQueda en confirmar: reviso con la estilista ${TURNOS_STYLIST_NAME} si puede en ese horario y le aviso por acá.${maybeTurnoInfoBlock(waId)}`.trim();
          pushHistory(waId, "assistant", msgPend);
          await sendWhatsAppText(phone, msgPend);
          scheduleInactivityFollowUp(waId, phone);
          return;
        }

        const senaTag2 = merged.senado ? "\n\nEstado: *SEÑADO* ✅" : "";
      const msgOk = `✅ Turno reservado:\n• ${merged.servicio}\n• ${diaOk} ${ymdToDMY(merged.fecha)} ${merged.hora}${extra}${senaTag2}${maybeTurnoInfoBlock(waId)}`.trim();
        pushHistory(waId, "assistant", msgOk);
        await sendWhatsAppText(phone, msgOk);
        scheduleInactivityFollowUp(waId, phone);
        return;
      }
    }

    const intent = await classifyAndExtract(text);

    // ✅ actualizar tipo de intención para el seguimiento
    const ctx1 = lastCloseContext.get(waId);
    if (ctx1) ctx1.intentType = intent?.type || ctx1.intentType || "OTHER";

    // Si el clasificador falla, igual intentamos buscar en stock con el texto del cliente
    // ✅ Evitar confusión: "SI/NO/OK/DALE" como respuesta a la última pregunta del bot NO debe disparar catálogo.
    if (intent.type === "OTHER" && !(isYesNoShortReply(text) && lastAssistantWasQuestion(waId))) {
      const stock = await getStockCatalog();
      const q = guessQueryFromText(text);
      const matches = findStock(stock, q, "DETAIL");

      if (matches.length) {
        lastProductByUser.set(waId, matches[0]);

        if (userAsksForPhoto(text)) {
          const sent = await maybeSendProductPhoto(phone, matches[0], text);
          if (sent) {
            // ✅ INACTIVIDAD: programar follow-up luego de la respuesta del bot
            scheduleInactivityFollowUp(waId, phone);
            return;
          }
        }

        const replyCatalog = formatStockReply(matches, "DETAIL");
        if (replyCatalog) {
          pushHistory(waId, "assistant", replyCatalog);
          await sendWhatsAppText(phone, replyCatalog);
          // ✅ INACTIVIDAD
          scheduleInactivityFollowUp(waId, phone);
          return;
        }
      }
    }

    // PRODUCT
    if (intent.type === "PRODUCT") {
      const stock = await getStockCatalog();

      // ✅ Si pide "la lista / catálogo / todos" (o el extractor dejó query vacía), ofrecemos TODO el catálogo
      const qCleanTokens = tokenize(intent.query || "", { expandSynonyms: true });
      const wantsAll = intent.mode === "LIST" && (
        !intent.query || !intent.query.trim() || qCleanTokens.length === 0 ||
        /\b(catalogo|catálogo|lista|todo|toda|todos|todas|productos|stock)\b/i.test(intent.query)
      );

      if (wantsAll) {
        const parts = formatStockListAll(stock, 12);
        for (const part of parts) {
          pushHistory(waId, "assistant", part);
          await sendWhatsAppText(phone, part);
        }
        // ✅ INACTIVIDAD (después del último envío)
        scheduleInactivityFollowUp(waId, phone);
        return;
      }

      const matches = intent.query ? findStock(stock, intent.query, intent.mode) : [];

      if (matches.length) {
        lastProductByUser.set(waId, matches[0]);

        if (userAsksForPhoto(text)) {
          const sent = await maybeSendProductPhoto(phone, matches[0], text);
          if (sent) {
            // ✅ INACTIVIDAD
            scheduleInactivityFollowUp(waId, phone);
            return;
          }
        }

        const replyCatalog = formatStockReply(matches, intent.mode);
        if (replyCatalog) {
          pushHistory(waId, "assistant", replyCatalog);
          await sendWhatsAppText(phone, replyCatalog);
          // ✅ INACTIVIDAD
          scheduleInactivityFollowUp(waId, phone);
          return;
        }
      } else {
        // ✅ Más inteligente: si no matchea, probamos mostrar opciones más amplias por categoría/descripcion
        const broader = findStock(stock, guessQueryFromText(intent.query || text), "LIST");
        const replyBroader = formatStockReply(broader, "LIST");
        if (replyBroader) {
          pushHistory(waId, "assistant", replyBroader);
          await sendWhatsAppText(phone, replyBroader);
          // ✅ INACTIVIDAD
          scheduleInactivityFollowUp(waId, phone);
          return;
        }

        await sendWhatsAppText(phone, "No lo encuentro en el catálogo con ese nombre. ¿Me dice la marca o para qué tratamiento lo necesita?");
        // ✅ INACTIVIDAD
        scheduleInactivityFollowUp(waId, phone);
        return;
      }
    }

    // SERVICE
    if (intent.type === "SERVICE") {
      const services = await getServicesCatalog();
      const matches = intent.query ? findServices(services, intent.query, intent.mode) : [];
      const replyCatalog = formatServicesReply(matches, intent.mode);

      if (replyCatalog) {
        // ✅ Guardamos "último servicio" para cuando luego pidan turno
        if (matches.length && intent.mode !== "LIST") {
          lastServiceByUser.set(waId, { nombre: matches[0].nombre, ts: Date.now() });
        }

        pushHistory(waId, "assistant", replyCatalog);
        await sendWhatsAppText(phone, replyCatalog);
        // ✅ INACTIVIDAD
        scheduleInactivityFollowUp(waId, phone);
        return;
      }

      // ✅ Evitar inventar servicios: si no está en el Excel, lo decimos y mostramos opciones reales
      const some = services.slice(0, 12).map(s => `• ${s.nombre}`).join("\n");
      const msgNo = `No encuentro ese servicio en nuestra lista.

Servicios disponibles (algunos):
${some}

¿Con cuál desea sacar turno o consultar precio?`;
      pushHistory(waId, "assistant", msgNo);
      await sendWhatsAppText(phone, msgNo);
      scheduleInactivityFollowUp(waId, phone);
      return;
    }
// COURSE
    if (intent.type === "COURSE") {
      const courses = await getCoursesCatalog();
      const matches = intent.query ? findCourses(courses, intent.query, intent.mode) : [];
      const replyCatalog = formatCoursesReply(matches, intent.mode);
      if (replyCatalog) {
        pushHistory(waId, "assistant", replyCatalog);
        await sendWhatsAppText(phone, replyCatalog);
        // ✅ INACTIVIDAD
        scheduleInactivityFollowUp(waId, phone);
        return;
      }
    }

    // fallback
    const model = pickModelForText(text);
    const conv = ensureConv(waId);

    const messages = [
      { role: "system", content: SYSTEM_PROMPT },
      { role: "system", content: `Fecha y hora actual: ${nowARString()} (${TIMEZONE}).` },
    ];
    if (name) messages.push({ role: "system", content: `Nombre del cliente: ${name}.` });

    for (const m of conv.messages) messages.push(m);
    messages.push({ role: "user", content: text });

    const reply = await openai.chat.completions.create({ model, messages });
    const out = reply.choices?.[0]?.message?.content || "No pude responder.";

    pushHistory(waId, "assistant", out);
    await sendWhatsAppText(phone, out);
    // ✅ INACTIVIDAD
    scheduleInactivityFollowUp(waId, phone);
  } catch (e) {
    console.error("❌ ERROR webhook:", e?.response?.data || e?.message || e);
  }
});

// ===================== CIERRE DEL DÍA =====================
async function endOfDayJob() {
  for (const [phone, data] of dailyLeads.entries()) {
    await appendToSheet({ name: data.name, phone, interest: data.interest });
  }
  dailyLeads.clear();
  console.log("✅ Seguimiento diario guardado");
}

// 23:59 hora Argentina
setInterval(() => {
  const now = new Date().toLocaleTimeString("es-AR", { timeZone: TIMEZONE });
  if (now.startsWith("23:59")) endOfDayJob();
}, 60000);

// ===================== START =====================
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log("🚀 Bot de estética activo");
  console.log(`Webhook: http://localhost:${PORT}/webhook`);
  console.log(`Health:  http://localhost:${PORT}/health`);
});