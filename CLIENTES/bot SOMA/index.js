const express = require("express");
const axios = require("axios");
const dotenv = require("dotenv");
const OpenAI = require("openai");
const fs = require("fs");
const path = require("path");
const os = require("os");
const { Pool } = require("pg");

// ===================== PDF (documentos) =====================
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

// ===================== CONFIG BASE =====================
const CLIENT_ID = process.env.CLIENT_ID || "SOMA";
const COMPANY_NAME = process.env.COMPANY_NAME || "SOMA";
const COMPANY_CITY = process.env.COMPANY_CITY || "Cafayate";
const COMPANY_PROVINCE = process.env.COMPANY_PROVINCE || "Salta";
const COMPANY_COUNTRY = process.env.COMPANY_COUNTRY || "Argentina";
const COMPANY_EMAIL = process.env.COMPANY_EMAIL || "";
const COMPANY_SITE = process.env.COMPANY_SITE || "";
const TIMEZONE = process.env.TIMEZONE || "America/Argentina/Salta";

const PRIMARY_MODEL = process.env.PRIMARY_MODEL || "gpt-4.1-mini";
const COMPLEX_MODEL = process.env.COMPLEX_MODEL || PRIMARY_MODEL;
const TRANSCRIBE_MODEL = process.env.TRANSCRIBE_MODEL || "gpt-4o-mini-transcribe";

const INACTIVITY_MS = Number(process.env.INACTIVITY_FOLLOWUP_MS || 10 * 60 * 1000);
const CLOSE_LOG_MS = Number(process.env.CLOSE_LOG_MS || 10 * 60 * 1000);
const CONV_TTL_MS = Number(process.env.CONV_TTL_MS || 7 * 24 * 60 * 60 * 1000);
const OUT_DEDUP_MS = Number(process.env.OUT_DEDUP_MS || 20 * 1000);

// ===================== REQUIRED ENV =====================
const REQUIRED_ENV = ["OPENAI_API_KEY", "WHATSAPP_TOKEN", "PHONE_NUMBER_ID", "VERIFY_TOKEN"];
for (const key of REQUIRED_ENV) {
  if (!process.env[key]) throw new Error(`Falta variable en .env: ${key}`);
}

// ===================== OPENAI =====================
const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

// ===================== DB =====================
const DB_URL = process.env.DB_URL || process.env.DATABASE_URL || "";
if (!DB_URL) {
  throw new Error("Falta variable DB_URL o DATABASE_URL para conectar a PostgreSQL");
}

const db = new Pool({
  connectionString: DB_URL,
  ssl: DB_URL.includes("render.com") || DB_URL.includes("railway")
    ? { rejectUnauthorized: false }
    : false,
});

const MEDIA_DIR = process.env.MEDIA_DIR || path.join(__dirname, "media");
try { fs.mkdirSync(MEDIA_DIR, { recursive: true }); } catch {}

async function ensureDb() {
  await db.query(`
    CREATE TABLE IF NOT EXISTS messages (
      id BIGSERIAL PRIMARY KEY,
      client_id TEXT NOT NULL,
      direction TEXT NOT NULL,
      wa_peer TEXT NOT NULL,
      name TEXT,
      text TEXT,
      msg_type TEXT,
      wa_msg_id TEXT,
      ts_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      raw_json JSONB
    )
  `);

  await db.query(`ALTER TABLE messages ADD COLUMN IF NOT EXISTS client_id TEXT`);
  await db.query(`UPDATE messages SET client_id = $1 WHERE client_id IS NULL`, [CLIENT_ID]);
  await db.query(`ALTER TABLE messages ALTER COLUMN client_id SET NOT NULL`);
}

async function dbInsertMessage({ direction, wa_peer, name, text, msg_type, wa_msg_id, raw }) {
  const peerNorm = normalizePhone(wa_peer);
  await db.query(
    `INSERT INTO messages(client_id, direction, wa_peer, name, text, msg_type, wa_msg_id, raw_json)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
    [
      CLIENT_ID,
      direction,
      peerNorm,
      name || null,
      text || null,
      msg_type || null,
      wa_msg_id || null,
      raw || {},
    ]
  );
}

// ===================== UTILS =====================
function getTmpDir() {
  const dir = os.tmpdir();
  try { fs.mkdirSync(dir, { recursive: true }); } catch {}
  return dir;
}

function nowARString() {
  return new Date().toLocaleString("es-AR", { timeZone: TIMEZONE });
}

function ddmmyyAR() {
  return new Date().toLocaleDateString("es-AR", {
    timeZone: TIMEZONE,
    year: "2-digit",
    month: "2-digit",
    day: "2-digit",
  });
}

function normalize(str) {
  return String(str || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeShortReply(text) {
  return normalize(text || "")
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function sentenceCase(value) {
  const txt = String(value || "").replace(/\s+/g, " ").trim();
  if (!txt) return "";
  return txt.charAt(0).toUpperCase() + txt.slice(1);
}

function normalizePhone(s) {
  s = String(s || "").trim().replace(/[ \+\-\(\)]/g, "");
  if (s.endsWith(".0")) s = s.slice(0, -2);
  if (s.startsWith("549")) s = "54" + s.slice(3);
  return s;
}

function normalizePhoneDigits(raw) {
  let digits = String(raw || "").trim().replace(/[^\d]/g, "");
  if (!digits) return "";
  if (String(raw || "").trim().endsWith(".0") && digits.endsWith("0")) digits = digits.slice(0, -1);
  if (digits.startsWith("00")) digits = digits.slice(2);
  return digits;
}

function normalizeComparablePhone(raw) {
  let digits = normalizePhoneDigits(raw);
  if (!digits) return "";
  if (digits.startsWith("549")) digits = "54" + digits.slice(3);
  return digits;
}

function normalizeHubSpotPhone(raw) {
  const digits = normalizePhoneDigits(raw);
  return digits ? `+${digits}` : "";
}

function normalizeWhatsAppRecipient(s) {
  let digits = String(s || "").trim().replace(/[^\d]/g, "");
  if (!digits) return "";
  if (digits.endsWith("0")) {
    const asFloat = String(s || "").trim();
    if (asFloat.endsWith(".0")) digits = digits.slice(0, -1);
  }
  if (digits.startsWith("549")) return "54" + digits.slice(3);
  if (digits.startsWith("54")) return digits;
  if (digits.length === 10) return "54" + digits;
  if (digits.length === 11 && digits.startsWith("0")) return "54" + digits.slice(1);
  return digits;
}

function buildPhoneCandidates(raw) {
  const digits = normalizePhoneDigits(raw);
  const set = new Set();
  if (!digits) return [];

  const add = (value) => {
    const v = String(value || "").trim();
    if (!v) return;
    set.add(v);
    set.add(v.replace(/^\+/, ""));
    if (!v.startsWith("+")) set.add(`+${v}`);
  };

  add(digits);
  if (digits.startsWith("54")) {
    const local = digits.slice(2);
    add(`54${local}`);
    add(`549${local}`);
  }
  if (digits.startsWith("549")) {
    const local = digits.slice(3);
    add(`54${local}`);
    add(`549${local}`);
  }

  return Array.from(set);
}

function titleCaseName(value) {
  const txt = String(value || "").replace(/\s+/g, " ").trim();
  if (!txt) return "";
  return txt
    .toLowerCase()
    .split(" ")
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function cleanNameCandidate(value) {
  const raw = String(value || "").trim();
  if (!raw) return "";
  if (/[!?@#$%^&*_+=<>\[\]{}\/|~`",;:.0-9]/.test(raw)) return "";

  const txt = raw
    .replace(/[^\p{L}\p{M}' -]/gu, " ")
    .replace(/\s+/g, " ")
    .trim();
  if (!txt) return "";

  const parts = txt.split(" ").filter(Boolean);
  if (!parts.length || parts.length > 4) return "";

  const banned = new Set([
    "cliente", "empresa", "negocio", "consulta", "presupuesto", "bot", "crm",
    "marketing", "base", "datos", "servicio", "servicios", "hola", "buen",
    "bueno", "dia", "dias", "tarde", "tardes", "noche", "noches", "gracias",
    "quiero", "quisiera", "necesito", "busco", "info", "informacion", "soma"
  ]);

  const normalizedParts = parts.map((p) => normalize(p));
  if (normalizedParts.some((p) => banned.has(p))) return "";
  if (parts.some((p) => p.length < 2)) return "";

  return titleCaseName(txt);
}

function splitNameParts(value) {
  const full = cleanNameCandidate(value);
  if (!full) return { firstName: "", lastName: "", fullName: "" };
  const parts = full.split(" ").filter(Boolean);
  return {
    firstName: parts[0] || "",
    lastName: parts.slice(1).join(" "),
    fullName: full,
  };
}

function isLikelyGenericContactName(value) {
  const cleaned = cleanNameCandidate(value);
  const t = normalize(value || "");
  if (!cleaned) return true;
  if (cleaned.length < 3) return true;
  if (/^(cliente|empresa|sin nombre|nombre pendiente)$/.test(t)) return true;
  return /(consulta|presupuesto|marketing|crm|bot|base de datos|automatizacion|automatización|servicio)/i.test(t);
}

function isStrongProfileNameCandidate(value) {
  const full = cleanNameCandidate(value);
  if (!full) return false;
  const parts = full.split(" ").filter(Boolean);
  if (!parts.length || parts.length > 4) return false;
  if (parts.some((p) => p.length < 2)) return false;
  return true;
}

function chooseBestContactName({ existingName = "", existingLastName = "", explicitName = "", profileName = "" } = {}) {
  const current = splitNameParts([existingName, existingLastName].filter(Boolean).join(" ").trim());
  const explicit = splitNameParts(explicitName);
  const profile = splitNameParts(profileName);

  if (explicit.fullName && !isLikelyGenericContactName(explicit.fullName)) {
    return { ...explicit, source: "chat_explicit" };
  }
  if (current.fullName && !isLikelyGenericContactName(current.fullName)) {
    return { ...current, source: "existing" };
  }
  if (isStrongProfileNameCandidate(profileName) && !isLikelyGenericContactName(profile.fullName)) {
    return { ...profile, source: "whatsapp_profile" };
  }
  return { firstName: "", lastName: "", fullName: "", source: "" };
}

function extractExplicitNameFromSnippet(snippet = "") {
  const raw = String(snippet || "").replace(/\s+/g, " ").trim();
  if (!raw) return "";
  const patterns = [
    /(?:me llamo|mi nombre es|soy|habla)\s+([A-Za-zÁÉÍÓÚÑáéíóúñ' ]{2,60})/i,
    /(?:soy)\s+([A-ZÁÉÍÓÚÑ][A-Za-zÁÉÍÓÚÑáéíóúñ']{1,30})(?:\s+([A-ZÁÉÍÓÚÑ][A-Za-zÁÉÍÓÚÑáéíóúñ']{1,30}))?/,
  ];
  for (const rx of patterns) {
    const m = raw.match(rx);
    if (!m) continue;
    const cand = [m[1], m[2]].filter(Boolean).join(" ").replace(/\s+/g, " ").trim();
    const cleaned = cleanNameCandidate(cand);
    if (cleaned) return cleaned;
  }
  return "";
}

function sanitizePossiblePhone(raw) {
  let digits = String(raw || "").replace(/[^\d+]/g, "");
  if (!digits) return "";
  digits = digits.replace(/[^\d]/g, "");
  if (digits.startsWith("549")) digits = "54" + digits.slice(3);
  if (digits.startsWith("0")) digits = digits.replace(/^0+/, "");
  if (digits.length < 8 || digits.length > 15) return "";
  return `+${digits}`;
}

function extractContactInfo(text) {
  const raw = String(text || "").trim();
  const norm = normalize(raw);

  let telefono = "";
  const phoneMatches = raw.match(/(\+?\d[\d\s().-]{6,}\d)/g) || [];
  for (const cand of phoneMatches) {
    const cleanPhone = sanitizePossiblePhone(cand);
    if (cleanPhone) {
      telefono = cleanPhone;
      break;
    }
  }

  const explicitPhone = raw.match(/(?:telefono|tel|cel|celular|whatsapp|wsp|numero|número)\s*(?:es|:)?\s*(\+?\d[\d\s().-]{6,}\d)/i);
  if (explicitPhone) {
    const cleanPhone = sanitizePossiblePhone(explicitPhone[1]);
    if (cleanPhone) telefono = cleanPhone;
  }

  let nombre = "";
  const explicitName = raw.match(/(?:me llamo|mi nombre es|soy)\s+([A-Za-zÁÉÍÓÚÑáéíóúñ' ]{5,60})/i);
  if (explicitName) {
    const cand = explicitName[1].replace(/\s+/g, " ").trim();
    if (!/\d/.test(cand) && cand.split(" ").length >= 2) nombre = cand;
  }

  if (!nombre) {
    const looksLikePureName = (
      !/\d/.test(raw) &&
      raw.split(" ").length >= 2 &&
      raw.length >= 5 &&
      raw.length <= 60 &&
      !/(quiero|quisiera|consulto|consulta|pregunto|pregunta|hola|buen dia|buen día|buenas|gracias|marketing|crm|bot|automatizacion|automatización|base de datos)/i.test(norm)
    );
    if (looksLikePureName) nombre = raw;
  }

  return { nombre: cleanNameCandidate(nombre), telefono };
}

function hubspotDateValue(dateInput) {
  const d = dateInput instanceof Date ? dateInput : new Date(dateInput || Date.now());
  if (Number.isNaN(d.getTime())) return "";
  return String(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));
}

function hubspotDateTimeValue(dateInput) {
  const d = dateInput instanceof Date ? dateInput : new Date(dateInput || Date.now());
  if (Number.isNaN(d.getTime())) return "";
  return d.toISOString();
}

// ===================== NEGOCIO =====================
const BUSINESS_SERVICES = [
  {
    key: "bots_automaticos",
    title: "Bots automáticos para WhatsApp",
    short: "Automatizamos respuestas, captación, seguimiento y orden de consultas.",
    aliases: ["bot", "bots", "whatsapp", "automatizacion", "automatización", "chatbot", "ia", "inteligencia artificial"],
    category: "BOTS AUTOMÁTICOS",
  },
  {
    key: "base_datos",
    title: "Bases de datos y seguimiento comercial",
    short: "Centralizamos consultas, historial, métricas y orden interno.",
    aliases: ["base de datos", "base", "datos", "seguimiento", "panel", "reportes", "estadisticas", "estadísticas"],
    category: "BASES DE DATOS",
  },
  {
    key: "marketing_enfocado",
    title: "Marketing enfocado",
    short: "Diseñamos campañas y mensajes orientados a conseguir mejores consultas.",
    aliases: ["marketing", "campaña", "campanas", "campañas", "publicidad", "anuncios", "meta ads", "difusion", "difusión"],
    category: "MARKETING ENFOCADO",
  },
  {
    key: "crm",
    title: "CRM y gestión de clientes",
    short: "Ordenamos contactos, estados, seguimiento y oportunidades.",
    aliases: ["crm", "pipeline", "clientes", "leads", "seguimiento comercial"],
    category: "CRM",
  },
  {
    key: "administracion",
    title: "Administración profesional",
    short: "Estandarizamos procesos, atención y control operativo para empresas.",
    aliases: ["administracion", "administración", "gestion", "gestión", "orden", "procesos", "operacion", "operación"],
    category: "ADMINISTRACIÓN PROFESIONAL",
  },
];

const CATEGORIAS_OK = [
  "BOTS AUTOMÁTICOS",
  "BASES DE DATOS",
  "MARKETING ENFOCADO",
  "CRM",
  "ADMINISTRACIÓN PROFESIONAL",
  "CONSULTA GENERAL",
];

function detectBusinessServices(text) {
  const t = normalize(text || "");
  const hits = [];
  for (const item of BUSINESS_SERVICES) {
    if (item.aliases.some((alias) => t.includes(normalize(alias)))) hits.push(item);
  }
  return Array.from(new Map(hits.map((x) => [x.key, x])).values());
}

function pickCategorias({ text = "" } = {}) {
  const detected = detectBusinessServices(text);
  const found = detected.map((x) => x.category);
  if (!found.length) found.push("CONSULTA GENERAL");
  return Array.from(new Set(found)).filter((x) => CATEGORIAS_OK.includes(x));
}

function pickCategoria({ text = "" } = {}) {
  return pickCategorias({ text })[0] || "CONSULTA GENERAL";
}

function pickPrimaryService(text) {
  const detected = detectBusinessServices(text);
  return detected[0]?.title || "";
}

function mergeHubSpotMulti(existingValue, newValues = []) {
  const list = [];
  const pushMany = (raw, splitter) => {
    String(raw || "")
      .split(splitter)
      .map((x) => x.trim())
      .filter(Boolean)
      .forEach((x) => { if (!list.includes(x)) list.push(x); });
  };
  pushMany(existingValue, ";");
  (Array.isArray(newValues) ? newValues : []).forEach((x) => {
    const v = String(x || "").trim();
    if (v && !list.includes(v)) list.push(v);
  });
  return list.join("; ");
}

function mergeSlashText(existingValue, newValues = [], maxItems = 6) {
  const list = [];
  const pushMany = (raw) => {
    String(raw || "")
      .split(/[\/\n;,]+/)
      .map((x) => x.trim())
      .filter(Boolean)
      .forEach((x) => { if (!list.includes(x)) list.push(x); });
  };
  pushMany(existingValue);
  (Array.isArray(newValues) ? newValues : []).forEach((x) => {
    const v = String(x || "").trim();
    if (v && !list.includes(v)) list.push(v);
  });
  return list.slice(0, maxItems).join(" / ");
}

function fallbackTopicFromText(raw) {
  const clean = String(raw || "")
    .replace(/\s+/g, " ")
    .replace(/^(hola+|buenas+|buen dia|buen día|buenas tardes|buenas noches)[,!\s]*/i, "")
    .replace(/^(quiero saber|quisiera saber|queria saber|quería saber|consulto por|consultaba por|consulta sobre|consulta por)\s+/i, "")
    .replace(/^(precio de|precio por|busco|necesito)\s+/i, "")
    .trim();

  return clean ? clean.slice(0, 70) : "";
}

function buildMiniObservacion({ text = "", servicios = [], categorias = [] } = {}) {
  const raw = String(text || "").replace(/\s+/g, " ").trim();
  const foco = servicios.length ? servicios.join(" / ") : fallbackTopicFromText(raw) || (categorias[0] || "su consulta");
  const cuerpo = sentenceCase(`Consulta sobre ${foco}`.trim());
  return `${ddmmyyAR()} ${cuerpo}`.slice(0, 120);
}

function mergeObservationHistory(existingValue, newLine, maxLines = 8) {
  const existingLines = String(existingValue || "")
    .split(/\r?\n+/)
    .map((x) => x.trim())
    .filter(Boolean);

  const cleanNew = String(newLine || "").trim();
  if (!cleanNew) return existingLines.join("\n");
  if (existingLines[existingLines.length - 1] === cleanNew) return existingLines.slice(-maxLines).join("\n");

  existingLines.push(cleanNew);
  return existingLines.slice(-maxLines).join("\n");
}

function getServicesListMessage() {
  const lines = BUSINESS_SERVICES.map((item) => `• *${item.title}*: ${item.short}`);
  return [
    `Trabajamos con estas soluciones para empresas:`,
    "",
    ...lines,
    "",
    `Estamos en *${COMPANY_CITY}, ${COMPANY_PROVINCE}* y trabajamos con empresas de toda *${COMPANY_COUNTRY}*.`,
    "",
    "Si quiere, contame qué necesita ordenar o automatizar y lo veo con usted 😊",
  ].join("\n");
}

function getPricingMessage() {
  return [
    "Trabajamos con propuesta personalizada.",
    "",
    "El valor depende de:",
    "• qué proceso quiere automatizar u ordenar",
    "• cuántas consultas recibe",
    "• si usa WhatsApp, Instagram, Meta o CRM",
    "• si necesita base de datos, seguimiento y reportes",
    "",
    "Para orientarlo bien, dígame:",
    "• rubro de la empresa",
    "• ciudad",
    "• principal problema que hoy quiere resolver",
  ].join("\n");
}

function getLocationMessage() {
  return `Actualmente estamos en *${COMPANY_CITY}, ${COMPANY_PROVINCE}*, y trabajamos con empresas de toda *${COMPANY_COUNTRY}*.`;
}

function getQualificationMessage() {
  return [
    "Perfecto 😊",
    "",
    "Para preparar una propuesta inicial necesito:",
    "• nombre de la empresa",
    "• rubro",
    "• ciudad",
    "• qué proceso quiere automatizar, ordenar o mejorar",
  ].join("\n");
}

function looksLikeGreetingOnly(text) {
  const t = normalizeShortReply(text || "");
  return ["hola", "holaa", "buenas", "buen dia", "buen día", "buenas tardes", "buenas noches"].includes(t);
}

function looksLikeServiceListRequest(text) {
  const t = normalize(text || "");
  return /(que ofrecen|qué ofrecen|servicios|que hacen|qué hacen|a que se dedican|a qué se dedican|en que trabajan|en qué trabajan|soluciones|que brindan|qué brindan)/i.test(t);
}

function looksLikePricingRequest(text) {
  const t = normalize(text || "");
  return /(precio|precios|cuanto sale|cuánto sale|cuanto cuesta|cuánto cuesta|valor|presupuesto|cotizacion|cotización)/i.test(t);
}

function looksLikeLocationRequest(text) {
  const t = normalize(text || "");
  return /(de donde son|de dónde son|donde estan|dónde están|ubicacion|ubicación|direccion|dirección|cafayate|salta|argentina)/i.test(t);
}

function looksLikeQualificationRequest(text) {
  const t = normalize(text || "");
  return /(quiero info|quiero mas info|quiero más info|quiero una propuesta|quiero presupuesto|quiero cotizacion|quiero cotización|quiero contratar|me interesa|podemos hablar|coordinar|reunion|reunión|llamada|asesoria|asesoría)/i.test(t);
}

async function detectInterest(text) {
  const raw = String(text || "").trim();
  if (!raw) return null;

  const detected = detectBusinessServices(raw);
  if (detected.length === 1) return detected[0].title;
  if (detected.length > 1) return detected.map((x) => x.title).join(" / ");

  try {
    const completion = await openai.chat.completions.create({
      model: PRIMARY_MODEL,
      messages: [
        {
          role: "system",
          content: "Detectá solo interés comercial real. Respondé una frase corta o 'NINGUNO'. Temas válidos: bots automáticos, base de datos, marketing enfocado, CRM, administración profesional.",
        },
        { role: "user", content: raw },
      ],
    });
    const result = String(completion.choices?.[0]?.message?.content || "").trim();
    return result.toUpperCase() === "NINGUNO" ? null : result;
  } catch {
    return null;
  }
}

// ===================== HUBSPOT =====================
const HUBSPOT_ACCESS_TOKEN = process.env.HUBSPOT_ACCESS_TOKEN || process.env.HUBSPOT_TOKEN || "";
const HUBSPOT_BASE_URL = (process.env.HUBSPOT_BASE_URL || "https://api.hubapi.com").replace(/\/$/, "");

const HUBSPOT_PROPERTY = {
  firstname: "firstname",
  lastname: "lastname",
  phone: "phone",
  observacion: "observacion",
  producto: "producto",
  cliente: "cliente",
  categoria: "categoria",
  fechaIngresoBase: "fecha_de_ingreso_base",
  ultimoContacto: "ultimo_contacto",
  empresa: "empresa",
  whatsappContact: "whatsapp_contact",
  whatsappWaId: "whatsapp_wa_id",
  whatsappPhoneRaw: "whatsapp_phone_raw",
  whatsappPhoneNormalized: "whatsapp_phone_normalized",
  whatsappProfileName: "whatsapp_profile_name",
  nameSource: "name_source",
  nameUpdatedAt: "name_updated_at",
};

const HUBSPOT_OPTION = {
  clienteSi: "true",
  empresa: COMPANY_NAME,
};

function hasHubSpotEnabled() {
  return !!HUBSPOT_ACCESS_TOKEN;
}

async function hubspotRequest(method, pathUrl, payload) {
  if (!HUBSPOT_ACCESS_TOKEN) throw new Error("HUBSPOT_ACCESS_TOKEN no configurado");
  const resp = await axios({
    method,
    url: `${HUBSPOT_BASE_URL}${pathUrl}`,
    headers: {
      Authorization: `Bearer ${HUBSPOT_ACCESS_TOKEN}`,
      "Content-Type": "application/json",
    },
    data: payload || undefined,
  });
  return resp.data;
}

async function searchHubSpotContacts(filterGroups = [], limit = 10) {
  if (!HUBSPOT_ACCESS_TOKEN || !Array.isArray(filterGroups) || !filterGroups.length) return [];

  const body = {
    filterGroups,
    limit,
    properties: [
      HUBSPOT_PROPERTY.firstname,
      HUBSPOT_PROPERTY.lastname,
      HUBSPOT_PROPERTY.phone,
      HUBSPOT_PROPERTY.observacion,
      HUBSPOT_PROPERTY.producto,
      HUBSPOT_PROPERTY.cliente,
      HUBSPOT_PROPERTY.categoria,
      HUBSPOT_PROPERTY.fechaIngresoBase,
      HUBSPOT_PROPERTY.ultimoContacto,
      HUBSPOT_PROPERTY.empresa,
      HUBSPOT_PROPERTY.whatsappContact,
      HUBSPOT_PROPERTY.whatsappWaId,
      HUBSPOT_PROPERTY.whatsappPhoneRaw,
      HUBSPOT_PROPERTY.whatsappPhoneNormalized,
      HUBSPOT_PROPERTY.whatsappProfileName,
      HUBSPOT_PROPERTY.nameSource,
      HUBSPOT_PROPERTY.nameUpdatedAt,
      "mobilephone",
    ],
  };

  try {
    const data = await hubspotRequest("post", "/crm/v3/objects/contacts/search", body);
    return Array.isArray(data?.results) ? data.results : [];
  } catch (e) {
    console.error("❌ Error buscando contactos en HubSpot:", e?.response?.data || e?.message || e);
    return [];
  }
}

async function findHubSpotContactByWaId(waId) {
  if (!waId) return null;
  const results = await searchHubSpotContacts([
    { filters: [{ propertyName: HUBSPOT_PROPERTY.whatsappWaId, operator: "EQ", value: String(waId) }] }
  ], 5);
  return results[0] || null;
}

async function findHubSpotContactsByPhone(phoneRaw) {
  const candidates = buildPhoneCandidates(phoneRaw).slice(0, 6);
  if (!candidates.length) return [];

  const propertyNames = [
    HUBSPOT_PROPERTY.phone,
    "mobilephone",
    HUBSPOT_PROPERTY.whatsappPhoneNormalized,
    HUBSPOT_PROPERTY.whatsappPhoneRaw,
  ];

  const unique = new Map();

  async function runSearchBatch(groups) {
    if (!groups.length) return;
    const results = await searchHubSpotContacts(groups, 20);
    for (const row of results) {
      if (row?.id && !unique.has(row.id)) unique.set(row.id, row);
    }
  }

  const primary = candidates[0];
  await runSearchBatch(
    propertyNames.map((propertyName) => ({
      filters: [{ propertyName, operator: "EQ", value: primary }],
    }))
  );
  if (unique.size) return Array.from(unique.values());

  const groups = [];
  for (const value of candidates.slice(1)) {
    for (const propertyName of propertyNames) {
      groups.push({ filters: [{ propertyName, operator: "EQ", value }] });
    }
  }

  for (let i = 0; i < groups.length; i += 4) {
    await runSearchBatch(groups.slice(i, i + 4));
    if (unique.size) break;
  }

  return Array.from(unique.values());
}

function chooseBestHubSpotMatch(results, phoneRaw) {
  const rows = Array.isArray(results) ? results : [];
  if (!rows.length) return null;
  const target = normalizeComparablePhone(phoneRaw);
  if (!target) return rows[0] || null;

  const scored = rows.map((row) => {
    const props = row?.properties || {};
    const phones = [
      props[HUBSPOT_PROPERTY.phone],
      props.mobilephone,
      props[HUBSPOT_PROPERTY.whatsappPhoneNormalized],
      props[HUBSPOT_PROPERTY.whatsappPhoneRaw],
    ].filter(Boolean);

    const exact = phones.some((p) => normalizeComparablePhone(p) === target);
    return { row, exact };
  });

  return scored.find((x) => x.exact)?.row || rows[0] || null;
}

function safeJsonParseFromText(raw) {
  const txt = String(raw || "").trim();
  if (!txt) return null;
  try { return JSON.parse(txt); } catch {}
  const match = txt.match(/\{[\s\S]*\}/);
  if (!match) return null;
  try { return JSON.parse(match[0]); } catch { return null; }
}

function getConversationSnippetForClose(waId, maxMessages = 12) {
  if (!waId) return "";
  const conv = ensureConv(waId);
  const rows = Array.isArray(conv?.messages) ? conv.messages.slice(-maxMessages) : [];
  return rows
    .map((m) => `${m.role === "assistant" ? "Bot" : "Cliente"}: ${String(m?.content || "").replace(/\s+/g, " ").trim()}`)
    .filter(Boolean)
    .join("\n")
    .slice(0, 3000);
}

async function analyzeCloseSummaryWithOpenAI({ ctx, conversationSnippet = "", servicios = [], categorias = [] } = {}) {
  const latestFocusText = [ctx?.interest, ctx?.lastUserText].filter(Boolean).join(" | ");
  const fallback = buildMiniObservacion({ text: [latestFocusText, conversationSnippet].filter(Boolean).join(" | "), servicios, categorias });
  const fallbackService = pickPrimaryService(latestFocusText || conversationSnippet || "");

  try {
    const completion = await openai.chat.completions.create({
      model: PRIMARY_MODEL,
      temperature: 0.2,
      messages: [
        {
          role: "system",
          content: "Sos un analista de CRM. Devolvés solo JSON válido con tres claves: observacion, nombre_completo y servicio_principal. observacion debe ser una conclusión muy breve, natural y concreta en español sobre qué consultó la persona. Máximo 12 palabras, sin copiar textual el chat, sin preguntas, sin saludo y sin comillas. nombre_completo solo si la persona dijo claramente su nombre o si el nombre de perfil de WhatsApp parece un nombre real; si no, devolvé cadena vacía. servicio_principal debe contener solo una solución realmente consultada, nunca una lista.",
        },
        {
          role: "user",
          content: JSON.stringify({
            fecha: ddmmyyAR(),
            nombre_perfil_whatsapp: ctx?.profileName || ctx?.name || "",
            nombre_detectado_previo: ctx?.explicitName || "",
            ultimo_mensaje_cliente: ctx?.lastUserText || "",
            interes_detectado: ctx?.interest || "",
            servicios_detectados: servicios,
            categorias_detectadas: categorias,
            historial_reciente: conversationSnippet || "",
          }),
        },
      ],
    });

    const parsed = safeJsonParseFromText(completion?.choices?.[0]?.message?.content || "");
    let observacion = String(parsed?.observacion || "").replace(/\s+/g, " ").trim();
    let nombreCompleto = cleanNameCandidate(parsed?.nombre_completo || "");
    let servicioPrincipal = String(parsed?.servicio_principal || "").trim();

    observacion = observacion.replace(/^\d{2}\/\d{2}\/\d{2}\s*/, "").trim();
    if (!observacion) observacion = fallback.replace(/^\d{2}\/\d{2}\/\d{2}\s*/, "").trim();
    observacion = `${ddmmyyAR()} ${sentenceCase(observacion)}`.slice(0, 120);
    if (!servicioPrincipal) servicioPrincipal = fallbackService;

    return { observacion, explicitName: nombreCompleto, servicioPrincipal };
  } catch {
    return {
      observacion: fallback,
      explicitName: cleanNameCandidate(ctx?.explicitName || "") || extractExplicitNameFromSnippet(conversationSnippet),
      servicioPrincipal: fallbackService,
    };
  }
}

async function buildHubSpotContactProperties(ctx, existingContact) {
  const existing = existingContact?.properties || {};
  const conversationSnippet = getConversationSnippetForClose(ctx?.waId || "");
  const focusText = [ctx.interest, ctx.lastUserText].filter(Boolean).join(" | ");
  const combinedText = [conversationSnippet, focusText].filter(Boolean).join(" | ");
  const categoriasFound = pickCategorias({ text: combinedText });
  const primaryService = pickPrimaryService(combinedText);
  const serviciosFound = primaryService ? [primaryService] : [];
  const aiAnalysis = await analyzeCloseSummaryWithOpenAI({ ctx, conversationSnippet, servicios: serviciosFound, categorias: categoriasFound });
  const finalPrimaryService = String(aiAnalysis?.servicioPrincipal || "").trim() || primaryService;
  const finalServicios = finalPrimaryService ? [finalPrimaryService] : [];
  const observacionLine = aiAnalysis?.observacion || buildMiniObservacion({ text: combinedText, servicios: finalServicios, categorias: categoriasFound });
  const observacion = mergeObservationHistory(existing?.[HUBSPOT_PROPERTY.observacion] || "", observacionLine);

  const explicitFromSnippet = extractExplicitNameFromSnippet(conversationSnippet);
  const chosenName = chooseBestContactName({
    existingName: existing?.[HUBSPOT_PROPERTY.firstname] || "",
    existingLastName: existing?.[HUBSPOT_PROPERTY.lastname] || "",
    explicitName: aiAnalysis?.explicitName || ctx.explicitName || explicitFromSnippet || "",
    profileName: ctx.profileName || ctx.name || "",
  });

  const normalizedPhone = normalizeHubSpotPhone(ctx.phoneRaw || ctx.phone || "");
  const rawPhone = String(ctx.phoneRaw || ctx.phone || "").trim();
  const now = new Date();
  const existingFullName = [existing?.[HUBSPOT_PROPERTY.firstname], existing?.[HUBSPOT_PROPERTY.lastname]].filter(Boolean).join(" ").trim();
  const existingIsGeneric = isLikelyGenericContactName(existingFullName);

  const properties = {
    [HUBSPOT_PROPERTY.phone]: normalizedPhone || rawPhone || existing?.[HUBSPOT_PROPERTY.phone] || "",
    [HUBSPOT_PROPERTY.observacion]: observacion,
    [HUBSPOT_PROPERTY.producto]: mergeSlashText(existing?.[HUBSPOT_PROPERTY.producto] || "", finalServicios, 8) || existing?.[HUBSPOT_PROPERTY.producto] || "",
    [HUBSPOT_PROPERTY.categoria]: mergeHubSpotMulti(existing?.[HUBSPOT_PROPERTY.categoria] || "", categoriasFound) || existing?.[HUBSPOT_PROPERTY.categoria] || "",
    [HUBSPOT_PROPERTY.ultimoContacto]: hubspotDateValue(now),
    [HUBSPOT_PROPERTY.empresa]: existing?.[HUBSPOT_PROPERTY.empresa] || HUBSPOT_OPTION.empresa,
    [HUBSPOT_PROPERTY.whatsappContact]: "true",
    [HUBSPOT_PROPERTY.whatsappWaId]: ctx.waId || existing?.[HUBSPOT_PROPERTY.whatsappWaId] || "",
    [HUBSPOT_PROPERTY.whatsappPhoneRaw]: rawPhone || existing?.[HUBSPOT_PROPERTY.whatsappPhoneRaw] || "",
    [HUBSPOT_PROPERTY.whatsappPhoneNormalized]: normalizedPhone || existing?.[HUBSPOT_PROPERTY.whatsappPhoneNormalized] || "",
    [HUBSPOT_PROPERTY.whatsappProfileName]: cleanNameCandidate(ctx.profileName || ctx.name || "") || existing?.[HUBSPOT_PROPERTY.whatsappProfileName] || "",
  };

  if (chosenName.firstName && (chosenName.source === "chat_explicit" || chosenName.source === "whatsapp_profile" || existingIsGeneric)) {
    properties[HUBSPOT_PROPERTY.firstname] = chosenName.firstName;
    properties[HUBSPOT_PROPERTY.lastname] = chosenName.lastName || existing?.[HUBSPOT_PROPERTY.lastname] || "";
    properties[HUBSPOT_PROPERTY.nameSource] = chosenName.source || existing?.[HUBSPOT_PROPERTY.nameSource] || "";
    properties[HUBSPOT_PROPERTY.nameUpdatedAt] = hubspotDateTimeValue(now);
  }

  if (!existing?.[HUBSPOT_PROPERTY.fechaIngresoBase]) {
    properties[HUBSPOT_PROPERTY.fechaIngresoBase] = hubspotDateValue(now);
  }

  return Object.fromEntries(Object.entries(properties).filter(([, value]) => value !== undefined && value !== null));
}

async function upsertHubSpotContactFromClose(ctx) {
  if (!hasHubSpotEnabled()) return;

  const phoneRaw = String(ctx?.phoneRaw || ctx?.phone || "").trim();
  const waId = String(ctx?.waId || "").trim();
  if (!phoneRaw && !waId) return;

  let contact = null;
  if (waId) contact = await findHubSpotContactByWaId(waId);
  if (!contact && phoneRaw) {
    const phoneMatches = await findHubSpotContactsByPhone(phoneRaw);
    contact = chooseBestHubSpotMatch(phoneMatches, phoneRaw);
  }

  const properties = await buildHubSpotContactProperties(ctx, contact);

  try {
    if (contact?.id) {
      await hubspotRequest("patch", `/crm/v3/objects/contacts/${contact.id}`, { properties });
      return { action: "updated", id: contact.id };
    }

    const created = await hubspotRequest("post", "/crm/v3/objects/contacts", { properties });
    return { action: "created", id: created?.id || "" };
  } catch (e) {
    console.error("❌ Error creando/actualizando contacto en HubSpot:", e?.response?.data || e?.message || e);
    throw e;
  }
}

// ===================== MEMORIA =====================
const conversations = new Map();
const processedMsgIds = new Set();
const lastSentOutByPeer = new Map();
const inactivityTimers = new Map();
const closeTimers = new Map();
const lastCloseContext = new Map();

function ensureConv(waId) {
  let c = conversations.get(waId);
  const now = Date.now();
  if (!c) {
    c = { messages: [], updatedAt: now };
    conversations.set(waId, c);
    return c;
  }
  if ((now - (c.updatedAt || 0)) > CONV_TTL_MS) c.messages = [];
  return c;
}

function pushHistory(waId, role, content) {
  const conv = ensureConv(waId);
  conv.messages.push({ role, content });
  conv.messages = conv.messages.slice(-30);
  conv.updatedAt = Date.now();
}

function updateLastCloseContext(waId, patch = {}) {
  if (!waId) return null;
  const prev = lastCloseContext.get(waId) || {};
  const next = { ...prev, ...patch };
  lastCloseContext.set(waId, next);
  return next;
}

async function logConversationClose(waId) {
  const ctx = lastCloseContext.get(waId);
  if (!ctx?.phone && !ctx?.phoneRaw && !ctx?.waId) return;
  await upsertHubSpotContactFromClose(ctx);
}

function scheduleInactivityFollowUp(waId, phone) {
  if (!waId || !phone) return;

  if (inactivityTimers.has(waId)) clearTimeout(inactivityTimers.get(waId));
  if (closeTimers.has(waId)) clearTimeout(closeTimers.get(waId));

  const timer = setTimeout(async () => {
    try {
      await sendWhatsAppText(phone, "¿Quiere que lo ayudemos en algo más o damos por finalizada la consulta?");
    } catch (e) {
      console.error("Error enviando mensaje por inactividad:", e?.response?.data || e?.message || e);
      return;
    }

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

async function getRecentDbMessages(waPeer, limit = 12) {
  const peerNorm = normalizePhone(waPeer);
  const r = await db.query(
    `SELECT direction, text
       FROM messages
      WHERE client_id = $1 AND wa_peer = $2 AND COALESCE(text, '') <> ''
      ORDER BY ts_utc DESC
      LIMIT $3`,
    [CLIENT_ID, peerNorm, limit]
  );
  return r.rows.reverse().map((row) => ({
    role: row.direction === "out" ? "assistant" : "user",
    content: String(row.text || "").trim(),
  })).filter((x) => x.content);
}

function mergeConversationForAI(dbMessages, localMessages) {
  const merged = [];
  const seen = new Set();
  for (const m of [...(dbMessages || []), ...(localMessages || [])]) {
    const key = `${m.role}::${String(m.content || "").trim()}`;
    if (!m?.content || seen.has(key)) continue;
    seen.add(key);
    merged.push({ role: m.role, content: String(m.content || "").trim() });
  }
  return merged.slice(-20);
}

// ===================== PROMPT =====================
const SYSTEM_PROMPT = `
Sos la asistente oficial de ${COMPANY_NAME}, un negocio que ofrece soluciones tecnológicas y de gestión para empresas.
Actualmente la empresa está en ${COMPANY_CITY}, ${COMPANY_PROVINCE}, y trabaja con clientes de toda ${COMPANY_COUNTRY}.

ROL:
- Ayudar a empresas que consultan por automatización, bots automáticos, base de datos, CRM, marketing enfocado y administración profesional.
- Vender con claridad, cercanía y criterio comercial.
- Detectar necesidad, ordenar la conversación y llevar al potencial cliente a una propuesta o diagnóstico inicial.

SERVICIOS QUE OFRECE LA EMPRESA:
- Bots inteligentes automáticos para WhatsApp que agendan y venden
- Base de datos y seguimiento comercial
- Marketing y publicidad
- CRM y orden de clientes
- Administración profesional para empresa
Después ofrecemos imagen profesional, por ejemplo, diseño de marca e imagen

ESTILO:
- Español rioplatense.
- Mensajes cortos, claros y profesionales.
- Soná humana, cercana y resolutiva.
- Si el mensaje es solo un saludo, respondé saludo + “¿en qué puedo ayudarte?” solamente.
- Podés usar emojis suaves cuando sumen claridad: ✨📊🤖📈📁
- No inventes precios, plazos ni funcionalidades cerradas si el cliente no dio suficiente contexto.
- Si preguntan precio o presupuesto, explicá que se arma propuesta personalizada y pedí contexto del negocio.
- Si preguntan qué hacen, explicá los servicios con foco en el problema que resuelven.
- Si el cliente está perdido, guiá con preguntas breves: rubro, ciudad, volumen de consultas, canal actual y objetivo.
- Si se nota interés real, intentá calificar la oportunidad con pocas preguntas.
- Nunca hables de turnos de salón, señas, estilistas, calendarios ni servicios de estética.

REGLAS COMERCIALES:
- No prometas integraciones o desarrollos específicos si no están confirmados.
- No inventes una agenda ni una reunión ya cerrada.
- Si la consulta es ambigua, pedí una sola aclaración y seguí.
- Si el usuario pide una propuesta, pedí nombre de empresa, rubro, ciudad y necesidad principal.
`.trim();

// ===================== WHATSAPP =====================
async function sendWhatsAppText(to, text) {
  const body = String(text || "");
  const recipient = normalizeWhatsAppRecipient(to);
  if (!recipient) throw new Error(`Número inválido para WhatsApp: ${to || "(vacío)"}`);

  const dedupKey = `${recipient}::${body}`;
  const now = Date.now();
  const prevTs = lastSentOutByPeer.get(dedupKey) || 0;
  if (body && (now - prevTs) < OUT_DEDUP_MS) return { deduped: true };
  lastSentOutByPeer.set(dedupKey, now);

  const resp = await axios.post(
    `https://graph.facebook.com/v19.0/${process.env.PHONE_NUMBER_ID}/messages`,
    { messaging_product: "whatsapp", to: recipient, text: { body } },
    {
      headers: {
        Authorization: `Bearer ${process.env.WHATSAPP_TOKEN}`,
        "Content-Type": "application/json",
      },
    }
  );

  const wa_msg_id = resp?.data?.messages?.[0]?.id || null;
  await dbInsertMessage({
    direction: "out",
    wa_peer: recipient,
    name: null,
    text: body,
    msg_type: "text",
    wa_msg_id,
    raw: resp?.data || {},
  });

  return resp.data;
}

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

// ===================== MULTIMODAL =====================
async function transcribeAudioFile(filePath) {
  const transcription = await openai.audio.transcriptions.create({
    model: TRANSCRIBE_MODEL,
    file: fs.createReadStream(filePath),
  });
  return (transcription.text || "").trim();
}

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
      {
        role: "system",
        content: "Analizá la imagen en español y devolvé una descripción útil para atención comercial. Si hay texto visible, transcribilo de forma fiel. No inventes datos que no se vean.",
      },
      {
        role: "user",
        content: [
          { type: "text", text: "Analizá esta imagen y extraé el texto visible y los datos importantes:" },
          { type: "image_url", image_url: { url: dataUrl } },
        ],
      },
    ],
  });
  return (resp.choices?.[0]?.message?.content || "").trim();
}

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

    const tmpFile = path.join(getTmpDir(), `wa-doc-${mediaId}${ext}`);
    await downloadWhatsAppMediaToFile(mediaInfo.url, tmpFile);

    const savedName = `in-${mediaId}${ext}`;
    const savedPath = path.join(MEDIA_DIR, savedName);
    try { fs.copyFileSync(tmpFile, savedPath); } catch {}

    let extractedTxt = "";
    if (mime.includes("pdf")) {
      const buf = fs.readFileSync(tmpFile);
      extractedTxt = await tryParsePdfBuffer(buf);
    } else if (mime.startsWith("text/")) {
      try { extractedTxt = fs.readFileSync(tmpFile, "utf-8"); } catch {}
    }

    if (!extractedTxt && mime.includes("pdf")) {
      try { fs.unlinkSync(tmpFile); } catch {}
      return {
        text: [
          caption ? `Texto adjunto del cliente: "${caption}"` : "",
          filename ? `Documento: ${filename}` : "",
          "El documento es PDF. Para revisarlo mejor, por favor envíe una captura de la parte importante o copie el texto aquí.",
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

    const tmpFile = path.join(getTmpDir(), `wa-image-${mediaId}${ext}`);
    await downloadWhatsAppMediaToFile(mediaInfo.url, tmpFile);

    const savedName = `in-${mediaId}${ext}`;
    const savedPath = path.join(MEDIA_DIR, savedName);
    try { fs.copyFileSync(tmpFile, savedPath); } catch {}

    const dataUrl = fileToDataUrl(tmpFile, mediaInfo.mime_type);
    const description = await describeImageWithVision(dataUrl);
    try { fs.unlinkSync(tmpFile); } catch {}

    const combined = [
      caption ? `Texto adjunto del cliente: "${caption}"` : "",
      description ? `Descripción de la imagen: ${description}` : "",
    ].filter(Boolean).join("\n");

    return {
      text: combined,
      kind: "image",
      media: {
        id: mediaId,
        mime_type: mediaInfo.mime_type || "",
        filename: savedName,
      },
    };
  }

  return { text: "", kind: `ignored_${msg.type}` };
}

// ===================== APP =====================
const app = express();
app.use(express.json({ limit: "25mb" }));

app.get("/ping", (req, res) => {
  console.log("PING HIT");
  res.status(200).send("pong");
});

app.get("/webhook", (req, res) => {
  const mode = req.query["hub.mode"];
  const token = req.query["hub.verify_token"];
  const challenge = req.query["hub.challenge"];

  if (mode === "subscribe" && token === process.env.VERIFY_TOKEN) {
    return res.status(200).send(challenge);
  }
  return res.sendStatus(403);
});

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    company: COMPANY_NAME,
    location: `${COMPANY_CITY}, ${COMPANY_PROVINCE}, ${COMPANY_COUNTRY}`,
    timeAR: nowARString(),
    tmpDir: getTmpDir(),
    models: { primary: PRIMARY_MODEL, complex: COMPLEX_MODEL, transcribe: TRANSCRIBE_MODEL },
    hubspot: { enabled: hasHubSpotEnabled() },
  });
});

app.post("/webhook", async (req, res) => {
  res.sendStatus(200);

  try {
    const value = req.body.entry?.[0]?.changes?.[0]?.value;
    const msg = value?.messages?.[0];
    const contact = value?.contacts?.[0];
    if (!msg) return;

    if (msg.id && processedMsgIds.has(msg.id)) return;
    if (msg.id) {
      processedMsgIds.add(msg.id);
      if (processedMsgIds.size > 5000) processedMsgIds.clear();
    }

    const phoneRaw = msg.from;
    const phone = String(contact?.wa_id || phoneRaw || "").replace(/[^\d]/g, "");
    const waId = contact?.wa_id || phoneRaw;
    const name = contact?.profile?.name || "";

    if (inactivityTimers.has(waId)) {
      clearTimeout(inactivityTimers.get(waId));
      inactivityTimers.delete(waId);
    }
    if (closeTimers.has(waId)) {
      clearTimeout(closeTimers.get(waId));
      closeTimers.delete(waId);
    }

    const extracted = await extractTextFromIncomingMessage(msg);
    const mediaMeta = extracted.media || null;
    const text = String(extracted.text || "").trim();
    const contactInfoFromText = extractContactInfo(text);

    lastCloseContext.set(waId, {
      waId,
      phone,
      phoneRaw,
      name,
      profileName: name,
      explicitName: contactInfoFromText?.nombre || "",
      lastUserText: text,
      intentType: "OTHER",
      interest: null,
    });

    await dbInsertMessage({
      direction: "in",
      wa_peer: phone,
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
      const out = "¿Me lo puede enviar en texto, audio o imagen? Así lo reviso 😊";
      pushHistory(waId, "assistant", out);
      await sendWhatsAppText(phone, out);
      scheduleInactivityFollowUp(waId, phone);
      return;
    }

    pushHistory(waId, "user", text);

    const interest = await detectInterest(text);
    updateLastCloseContext(waId, {
      interest: interest || (lastCloseContext.get(waId)?.interest || null),
      explicitName: contactInfoFromText?.nombre || (lastCloseContext.get(waId)?.explicitName || ""),
      lastUserText: text,
      intentType: pickCategoria({ text }),
    });

    if (looksLikeGreetingOnly(text)) {
      const out = `Hola 😊 ¿En qué puedo ayudarlo?`;
      pushHistory(waId, "assistant", out);
      await sendWhatsAppText(phone, out);
      scheduleInactivityFollowUp(waId, phone);
      return;
    }

    if (looksLikeServiceListRequest(text)) {
      const out = getServicesListMessage();
      pushHistory(waId, "assistant", out);
      await sendWhatsAppText(phone, out);
      scheduleInactivityFollowUp(waId, phone);
      return;
    }

    if (looksLikePricingRequest(text)) {
      const out = getPricingMessage();
      pushHistory(waId, "assistant", out);
      await sendWhatsAppText(phone, out);
      scheduleInactivityFollowUp(waId, phone);
      return;
    }

    if (looksLikeLocationRequest(text) && !looksLikeServiceListRequest(text)) {
      const out = getLocationMessage();
      pushHistory(waId, "assistant", out);
      await sendWhatsAppText(phone, out);
      scheduleInactivityFollowUp(waId, phone);
      return;
    }

    if (looksLikeQualificationRequest(text) && !looksLikePricingRequest(text)) {
      const out = getQualificationMessage();
      pushHistory(waId, "assistant", out);
      await sendWhatsAppText(phone, out);
      scheduleInactivityFollowUp(waId, phone);
      return;
    }

    const recentDbMessages = await getRecentDbMessages(phone, 12);
    const convForAI = mergeConversationForAI(recentDbMessages, ensureConv(waId).messages || []);
    const detectedServices = detectBusinessServices(text).map((x) => x.title);

    const messages = [
      { role: "system", content: SYSTEM_PROMPT },
      { role: "system", content: `Fecha y hora actual: ${nowARString()} (${TIMEZONE}).` },
      { role: "system", content: `Servicios detectados en la consulta actual: ${JSON.stringify(detectedServices)}` },
      { role: "system", content: `Categorías detectadas: ${JSON.stringify(pickCategorias({ text }))}` },
    ];
    if (name) messages.push({ role: "system", content: `Nombre de perfil del cliente: ${name}.` });
    if (interest) messages.push({ role: "system", content: `Interés probable del cliente: ${interest}.` });

    for (const m of convForAI) messages.push(m);
    messages.push({ role: "user", content: text });

    const reply = await openai.chat.completions.create({ model: COMPLEX_MODEL, messages });
    const out = String(reply.choices?.[0]?.message?.content || "No pude responder.").trim() || "No pude responder.";

    pushHistory(waId, "assistant", out);
    await sendWhatsAppText(phone, out);
    scheduleInactivityFollowUp(waId, phone);
  } catch (e) {
    console.error("❌ ERROR webhook:", e?.response?.data || e?.message || e);
  }
});

// ===================== START =====================
const PORT = process.env.PORT || 3000;

(async () => {
  await ensureDb();
  console.log(hasHubSpotEnabled()
    ? "✅ HubSpot CRM habilitado para seguimiento al cierre de charla"
    : "ℹ️ HubSpot CRM no configurado");

  app.listen(PORT, () => {
    console.log(`🚀 Bot de ${COMPANY_NAME} activo`);
    console.log(`Webhook: http://localhost:${PORT}/webhook`);
    console.log(`Health:  http://localhost:${PORT}/health`);
  });
})();
