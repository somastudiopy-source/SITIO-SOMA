const express = require("express");
const axios = require("axios");
const dotenv = require("dotenv");
const OpenAI = require("openai");
const { google } = require("googleapis");
const fs = require("fs");
const path = require("path");
const os = require("os");
const { Pool } = require("pg");
const CLIENT_ID = "CATALEYA";
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

  // Compatibilidad: si la tabla ya existía sin client_id, la agregamos.
  await db.query(`
    ALTER TABLE messages
    ADD COLUMN IF NOT EXISTS client_id TEXT
  `);

  await db.query(`
    UPDATE messages
    SET client_id = $1
    WHERE client_id IS NULL
  `, [CLIENT_ID]);

  await db.query(`
    ALTER TABLE messages
    ALTER COLUMN client_id SET NOT NULL
  `);
}

async function ensureAppointmentTables() {
  await db.query(`
    CREATE TABLE IF NOT EXISTS appointment_drafts (
      wa_id TEXT PRIMARY KEY,
      wa_phone TEXT NOT NULL,
      client_name TEXT,
      contact_phone TEXT,
      service_name TEXT,
      service_notes TEXT,
      appointment_date DATE,
      appointment_time TIME,
      duration_min INTEGER NOT NULL DEFAULT 60,
      wants_color_confirmation BOOLEAN NOT NULL DEFAULT FALSE,
      payment_status TEXT NOT NULL DEFAULT 'not_paid',
      payment_amount NUMERIC(10,2),
      payment_sender TEXT,
      payment_receiver TEXT,
      payment_proof_text TEXT,
      payment_proof_media_id TEXT,
      payment_proof_filename TEXT,
      awaiting_contact BOOLEAN NOT NULL DEFAULT FALSE,
      flow_step TEXT,
      last_intent TEXT,
      last_service_name TEXT,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  await db.query(`ALTER TABLE appointment_drafts ADD COLUMN IF NOT EXISTS flow_step TEXT`);
  await db.query(`ALTER TABLE appointment_drafts ADD COLUMN IF NOT EXISTS last_intent TEXT`);
  await db.query(`ALTER TABLE appointment_drafts ADD COLUMN IF NOT EXISTS last_service_name TEXT`);

  await db.query(`
    CREATE TABLE IF NOT EXISTS appointments (
      id BIGSERIAL PRIMARY KEY,
      wa_id TEXT NOT NULL,
      wa_phone TEXT NOT NULL,
      client_name TEXT,
      contact_phone TEXT,
      service_name TEXT,
      service_notes TEXT,
      appointment_date DATE,
      appointment_time TIME,
      duration_min INTEGER NOT NULL DEFAULT 60,
      status TEXT NOT NULL DEFAULT 'pending_payment',
      payment_status TEXT NOT NULL DEFAULT 'not_paid',
      payment_amount NUMERIC(10,2),
      payment_sender TEXT,
      payment_receiver TEXT,
      payment_proof_text TEXT,
      payment_proof_media_id TEXT,
      payment_proof_filename TEXT,
      calendar_event_id TEXT,
      is_color_service BOOLEAN NOT NULL DEFAULT FALSE,
      stylist_notified_at TIMESTAMPTZ,
      reminder_client_24h_at TIMESTAMPTZ,
      reminder_client_2h_at TIMESTAMPTZ,
      reminder_stylist_24h_at TIMESTAMPTZ,
      reminder_stylist_2h_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  await db.query(`
    CREATE TABLE IF NOT EXISTS appointment_notifications (
      id BIGSERIAL PRIMARY KEY,
      appointment_id BIGINT NOT NULL REFERENCES appointments(id) ON DELETE CASCADE,
      notification_type TEXT NOT NULL,
      recipient_phone TEXT NOT NULL,
      template_name TEXT,
      sent_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      wa_message_id TEXT,
      payload JSONB
    )
  `);
}

// ✅ Normalización única: 549XXXXXXXX -> 54XXXXXXXX
function normalizePhone(s) {
  s = String(s || "").trim().replace(/[ \+\-\(\)]/g, "");
  if (s.endsWith(".0")) s = s.slice(0, -2);
  if (s.startsWith("549")) s = "54" + s.slice(3);
  return s;
}


function normalizeWhatsAppRecipient(s) {
  let digits = String(s || "").trim().replace(/[^\d]/g, "");
  if (!digits) return "";
  if (digits.endsWith("0")) {
    const asFloat = String(s || "").trim();
    if (asFloat.endsWith('.0')) digits = digits.slice(0, -1);
  }
  if (digits.startsWith("549")) return "54" + digits.slice(3);
  if (digits.startsWith("54")) return digits;
  if (digits.length === 10) return "54" + digits;
  if (digits.length === 11 && digits.startsWith("0")) return "54" + digits.slice(1);
  return digits;
}


function normalizePhoneDigits(raw) {
  let digits = String(raw || "").trim().replace(/[^\d]/g, "");
  if (!digits) return "";
  if (String(raw || "").trim().endsWith('.0') && digits.endsWith('0')) digits = digits.slice(0, -1);
  if (digits.startsWith('00')) digits = digits.slice(2);
  return digits;
}

function normalizeComparablePhone(raw) {
  let digits = normalizePhoneDigits(raw);
  if (!digits) return "";
  if (digits.startsWith('549')) digits = '54' + digits.slice(3);
  return digits;
}

function normalizeHubSpotPhone(raw) {
  const digits = normalizePhoneDigits(raw);
  return digits ? `+${digits}` : "";
}

function buildPhoneCandidates(raw) {
  const digits = normalizePhoneDigits(raw);
  const set = new Set();
  if (!digits) return [];

  const add = (value) => {
    const v = String(value || '').trim();
    if (!v) return;
    set.add(v);
    set.add(v.replace(/^\+/, ''));
    if (!v.startsWith('+')) set.add(`+${v}`);
  };

  add(digits);

  if (digits.startsWith('54')) {
    const local = digits.slice(2);
    add(`54${local}`);
    add(`549${local}`);
  }

  if (digits.startsWith('549')) {
    const local = digits.slice(3);
    add(`54${local}`);
    add(`549${local}`);
  }

  return Array.from(set);
}

function titleCaseName(value) {
  const txt = String(value || '').replace(/\s+/g, ' ').trim();
  if (!txt) return '';
  return txt
    .toLowerCase()
    .split(' ')
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ');
}

function cleanNameCandidate(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';
  if (/[!?@#$%^&*_+=<>\[\]{}\/|~`",;:.0-9]/.test(raw)) return '';

  const txt = raw
    .replace(/[^\p{L}\p{M}' -]/gu, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  if (!txt) return '';

  const parts = txt.split(' ').filter(Boolean);
  if (!parts.length || parts.length > 4) return '';

  const banned = new Set([
    'interesada', 'interesado', 'estimada', 'estimado', 'cliente', 'clienta',
    'servicio', 'servicios', 'producto', 'productos', 'curso', 'cursos', 'turno', 'turnos',
    'grupo', 'cata', 'cataleya', 'nuevo', 'nueva', 'salon', 'belleza',
    'hola', 'buen', 'bueno', 'dia', 'dias', 'tarde', 'tardes', 'noche', 'noches',
    'consulta', 'consulto', 'consultar', 'pregunta', 'pregunto', 'quisiera', 'quiero',
    'precio', 'info', 'informacion', 'gracias', 'te', 'me', 'mi', 'soy', 'que', 'por', 'para', 'necesito', 'busco'
  ]);

  const normalizedParts = parts.map((p) => normalize(p));
  if (normalizedParts.some((p) => banned.has(p))) return '';
  if (parts.some((p) => p.length < 2)) return '';

  const t = normalize(txt);
  if (/(alisado|tintura|corte|unas|uñas|depil|pestan|pestañ|ceja|curso|mueble|shampoo|matizador|nutricion|nutrición|bano de crema|baño de crema|camilla|silla|mesa|barber|consult|precio|gracias|hola|buen dia|buen día|necesito|busco|quisiera|ampolla)/i.test(t)) {
    return '';
  }

  return titleCaseName(txt);
}

function isLikelyGenericContactName(value) {
  const cleaned = cleanNameCandidate(value);
  const t = normalize(value || '');
  if (!cleaned) return true;
  if (cleaned.length < 3) return true;
  if (/^(cliente|clienta|nuevo cliente|nueva clienta|sin nombre|sin apellido|nombre pendiente)$/i.test(t)) return true;
  if (/(interesad|estimad|servicio|producto|curso|turno|alisado|depilacion|depilación|mechita|mechas|tintura|corte|cliente salon|cliente salon de belleza|consult|pregunt|hola|buen dia|buen día|gracias|quisiera|quiero|ampolla)/i.test(t)) return true;
  return false;
}

function splitNameParts(value) {
  const full = cleanNameCandidate(value);
  if (!full) return { firstName: '', lastName: '', fullName: '' };
  const parts = full.split(' ').filter(Boolean);
  return {
    firstName: parts[0] || '',
    lastName: parts.slice(1).join(' '),
    fullName: full,
  };
}

function isStrongProfileNameCandidate(value) {
  const full = cleanNameCandidate(value);
  if (!full) return false;
  const parts = full.split(' ').filter(Boolean);
  if (!parts.length || parts.length > 4) return false;
  if (parts.some((p) => p.length < 2)) return false;
  return true;
}

function chooseBestContactName({ existingName = '', existingLastName = '', explicitName = '', profileName = '' } = {}) {
  const current = splitNameParts([existingName, existingLastName].filter(Boolean).join(' ').trim());
  const explicit = splitNameParts(explicitName);
  const profile = splitNameParts(profileName);

  if (explicit.fullName && !isLikelyGenericContactName(explicit.fullName)) {
    return { ...explicit, source: 'chat_explicit' };
  }

  if (current.fullName && !isLikelyGenericContactName(current.fullName)) {
    return { ...current, source: 'existing' };
  }

  if (isStrongProfileNameCandidate(profileName) && !isLikelyGenericContactName(profile.fullName)) {
    return { ...profile, source: 'whatsapp_profile' };
  }

  return { firstName: '', lastName: '', fullName: '', source: '' };
}


function extractExplicitNameFromSnippet(snippet = '') {
  const raw = String(snippet || '').replace(/\s+/g, ' ').trim();
  if (!raw) return '';
  const patterns = [
    /(?:me llamo|mi nombre es|soy|habla)\s+([A-Za-zÁÉÍÓÚÑáéíóúñ' ]{2,60})/i,
    /(?:soy)\s+([A-ZÁÉÍÓÚÑ][A-Za-zÁÉÍÓÚÑáéíóúñ']{1,30})(?:\s+([A-ZÁÉÍÓÚÑ][A-Za-zÁÉÍÓÚÑáéíóúñ']{1,30}))?/,
  ];
  for (const rx of patterns) {
    const m = raw.match(rx);
    if (!m) continue;
    const cand = [m[1], m[2]].filter(Boolean).join(' ').replace(/\s+/g, ' ').trim();
    const cleaned = cleanNameCandidate(cand);
    if (cleaned) return cleaned;
  }
  return '';
}

function hubspotDateValue(dateInput) {
  const d = dateInput instanceof Date ? dateInput : new Date(dateInput || Date.now());
  if (Number.isNaN(d.getTime())) return '';
  return String(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));
}

function hubspotDateTimeValue(dateInput) {
  const d = dateInput instanceof Date ? dateInput : new Date(dateInput || Date.now());
  if (Number.isNaN(d.getTime())) return '';
  return d.toISOString();
}

async function hubspotRequest(method, pathUrl, payload) {
  if (!HUBSPOT_ACCESS_TOKEN) throw new Error('HUBSPOT_ACCESS_TOKEN no configurado');
  const resp = await axios({
    method,
    url: `${HUBSPOT_BASE_URL}${pathUrl}`,
    headers: {
      Authorization: `Bearer ${HUBSPOT_ACCESS_TOKEN}`,
      'Content-Type': 'application/json',
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
      'mobilephone',
    ],
  };

  try {
    const data = await hubspotRequest('post', '/crm/v3/objects/contacts/search', body);
    return Array.isArray(data?.results) ? data.results : [];
  } catch (e) {
    console.error('❌ Error buscando contactos en HubSpot:', e?.response?.data || e?.message || e);
    return [];
  }
}

async function findHubSpotContactByWaId(waId) {
  if (!waId) return null;
  const results = await searchHubSpotContacts([
    { filters: [{ propertyName: HUBSPOT_PROPERTY.whatsappWaId, operator: 'EQ', value: String(waId) }] }
  ], 5);
  return results[0] || null;
}

async function findHubSpotContactsByPhone(phoneRaw) {
  const candidates = buildPhoneCandidates(phoneRaw).slice(0, 6);
  if (!candidates.length) return [];

  const propertyNames = [
    HUBSPOT_PROPERTY.phone,
    'mobilephone',
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

  // 1) Primero probamos con el candidato principal sobre los 4 campos más importantes.
  const primary = candidates[0];
  await runSearchBatch(
    propertyNames.map((propertyName) => ({
      filters: [{ propertyName, operator: 'EQ', value: primary }],
    }))
  );
  if (unique.size) return Array.from(unique.values());

  // 2) Luego probamos el resto en tandas chicas para respetar el límite de HubSpot.
  const groups = [];
  for (const value of candidates.slice(1)) {
    for (const propertyName of propertyNames) {
      groups.push({ filters: [{ propertyName, operator: 'EQ', value }] });
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

async function buildHubSpotContactProperties(ctx, existingContact, { isClient = false } = {}) {
  const existing = existingContact?.properties || {};
  const conversationSnippet = getConversationSnippetForClose(ctx?.waId || '');
  const focusText = [ctx.interest, ctx.lastUserText].filter(Boolean).join(' | ');
  const combinedText = [conversationSnippet, focusText].filter(Boolean).join(' | ');
  const categoriasFound = pickCategorias({ intentType: ctx.intentType || 'OTHER', text: combinedText });
  const primaryProduct = pickPrimaryProducto(focusText || conversationSnippet || '');
  const productosFound = primaryProduct ? [primaryProduct] : [];
  const aiAnalysis = await analyzeCloseSummaryWithOpenAI({ ctx, conversationSnippet, productos: productosFound, categorias: categoriasFound });
  const finalPrimaryProduct = cleanProductLabel(aiAnalysis?.productoPrincipal || '') || primaryProduct;
  const finalProductos = finalPrimaryProduct ? [finalPrimaryProduct] : [];
  const observacionLine = aiAnalysis?.observacion || buildMiniObservacion({ text: combinedText, productos: finalProductos, categorias: categoriasFound });
  const observacion = mergeObservationHistory(existing?.[HUBSPOT_PROPERTY.observacion] || '', observacionLine);

  const explicitFromSnippet = extractExplicitNameFromSnippet(conversationSnippet);
  const chosenName = chooseBestContactName({
    existingName: existing?.[HUBSPOT_PROPERTY.firstname] || '',
    existingLastName: existing?.[HUBSPOT_PROPERTY.lastname] || '',
    explicitName: aiAnalysis?.explicitName || ctx.explicitName || explicitFromSnippet || '',
    profileName: ctx.profileName || ctx.name || '',
  });

  const normalizedPhone = normalizeHubSpotPhone(ctx.phoneRaw || ctx.phone || '');
  const rawPhone = String(ctx.phoneRaw || ctx.phone || '').trim();
  const now = new Date();

  const mergedCategoria = mergeHubSpotMulti(existing?.[HUBSPOT_PROPERTY.categoria] || '', categoriasFound);
  const mergedProducto = mergeSlashText(existing?.[HUBSPOT_PROPERTY.producto] || '', finalProductos, 8);
  const existingFullName = [existing?.[HUBSPOT_PROPERTY.firstname], existing?.[HUBSPOT_PROPERTY.lastname]].filter(Boolean).join(' ').trim();
  const existingIsGeneric = isLikelyGenericContactName(existingFullName);

  const properties = {
    [HUBSPOT_PROPERTY.phone]: normalizedPhone || rawPhone || existing?.[HUBSPOT_PROPERTY.phone] || '',
    [HUBSPOT_PROPERTY.observacion]: observacion,
    [HUBSPOT_PROPERTY.producto]: mergedProducto || existing?.[HUBSPOT_PROPERTY.producto] || '',
    [HUBSPOT_PROPERTY.categoria]: mergedCategoria || existing?.[HUBSPOT_PROPERTY.categoria] || '',
    [HUBSPOT_PROPERTY.ultimoContacto]: hubspotDateValue(now),
    [HUBSPOT_PROPERTY.empresa]: existing?.[HUBSPOT_PROPERTY.empresa] || HUBSPOT_OPTION.empresaCataleya,
    [HUBSPOT_PROPERTY.whatsappContact]: 'true',
    [HUBSPOT_PROPERTY.whatsappWaId]: ctx.waId || existing?.[HUBSPOT_PROPERTY.whatsappWaId] || '',
    [HUBSPOT_PROPERTY.whatsappPhoneRaw]: rawPhone || existing?.[HUBSPOT_PROPERTY.whatsappPhoneRaw] || '',
    [HUBSPOT_PROPERTY.whatsappPhoneNormalized]: normalizedPhone || existing?.[HUBSPOT_PROPERTY.whatsappPhoneNormalized] || '',
    [HUBSPOT_PROPERTY.whatsappProfileName]: cleanNameCandidate(ctx.profileName || ctx.name || '') || existing?.[HUBSPOT_PROPERTY.whatsappProfileName] || '',
  };

  if (chosenName.firstName && (chosenName.source === 'chat_explicit' || chosenName.source === 'whatsapp_profile' || existingIsGeneric)) {
    properties[HUBSPOT_PROPERTY.firstname] = chosenName.firstName;
    if (HUBSPOT_PROPERTY.lastname) {
      properties[HUBSPOT_PROPERTY.lastname] = chosenName.lastName || existing?.[HUBSPOT_PROPERTY.lastname] || '';
    }
    properties[HUBSPOT_PROPERTY.nameSource] = chosenName.source || existing?.[HUBSPOT_PROPERTY.nameSource] || '';
    properties[HUBSPOT_PROPERTY.nameUpdatedAt] = hubspotDateTimeValue(now);
  }

  if (!existing?.[HUBSPOT_PROPERTY.fechaIngresoBase]) {
    properties[HUBSPOT_PROPERTY.fechaIngresoBase] = hubspotDateValue(now);
  }

  if (isClient) {
    properties[HUBSPOT_PROPERTY.cliente] = HUBSPOT_OPTION.clienteSi;
  }

  return Object.fromEntries(Object.entries(properties).filter(([, value]) => value !== undefined && value !== null));
}

async function hasAnyAppointmentForHubSpotContact({ waId, phoneRaw }) {
  const phoneNorm = normalizePhone(phoneRaw || '');
  const r = await db.query(
    `SELECT 1
       FROM appointments
      WHERE wa_id = $1
         OR wa_phone = $2
         OR contact_phone = $2
      LIMIT 1`,
    [waId || '', phoneNorm || '']
  );
  return !!r.rows?.length;
}

async function upsertHubSpotContactFromClose(ctx) {
  if (!hasHubSpotEnabled()) {
    console.warn('⚠️ HubSpot no configurado. Se omite seguimiento CRM.');
    return;
  }

  const phoneRaw = String(ctx?.phoneRaw || ctx?.phone || '').trim();
  const waId = String(ctx?.waId || '').trim();
  if (!phoneRaw && !waId) return;

  let contact = null;
  if (waId) contact = await findHubSpotContactByWaId(waId);
  if (!contact && phoneRaw) {
    const phoneMatches = await findHubSpotContactsByPhone(phoneRaw);
    contact = chooseBestHubSpotMatch(phoneMatches, phoneRaw);
  }

  const isClient = await hasAnyAppointmentForHubSpotContact({ waId, phoneRaw });
  const properties = await buildHubSpotContactProperties(ctx, contact, { isClient });

  try {
    if (contact?.id) {
      await hubspotRequest('patch', `/crm/v3/objects/contacts/${contact.id}`, { properties });
      return { action: 'updated', id: contact.id };
    }

    const created = await hubspotRequest('post', '/crm/v3/objects/contacts', { properties });
    return { action: 'created', id: created?.id || '' };
  } catch (e) {
    console.error('❌ Error creando/actualizando contacto en HubSpot:', e?.response?.data || e?.message || e);
    throw e;
  }
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

const app = express();
app.use(express.json({ limit: "25mb" }));

// ===================== CONFIG =====================
const DRIVE_FOLDER_ID = "1pKCqh1HEvQaI6XQ85ST8yvzxYWRXpxM1";
const TIMEZONE = "America/Argentina/Salta";

const WHATSAPP_TEMPLATE_LANGUAGE = process.env.WHATSAPP_TEMPLATE_LANGUAGE || "es_AR";
const TEMPLATE_RECORDATORIO_CLIENTE = process.env.TEMPLATE_RECORDATORIO_CLIENTE || "recordatorio_turno_cataleya";
const TEMPLATE_ALERTA_PELUQUERA = process.env.TEMPLATE_ALERTA_PELUQUERA || "alerta_turno_proximo";
const TEMPLATE_NUEVO_TURNO_PELUQUERA = process.env.TEMPLATE_NUEVO_TURNO_PELUQUERA || "nuevo_turno_peluquera";
const STYLIST_NOTIFY_PHONE_RAW = process.env.STYLIST_NOTIFY_PHONE || "3868 466370";
const APPOINTMENT_TEMPLATE_SCAN_MS = Number(process.env.APPOINTMENT_TEMPLATE_SCAN_MS || 60000);


// ===================== HUBSPOT (CRM seguimiento) =====================
const HUBSPOT_ACCESS_TOKEN = process.env.HUBSPOT_ACCESS_TOKEN || process.env.HUBSPOT_TOKEN || "";
const HUBSPOT_BASE_URL = (process.env.HUBSPOT_BASE_URL || "https://api.hubapi.com").replace(/\/$/, "");
const ENABLE_END_OF_DAY_TRACKING = String(process.env.ENABLE_END_OF_DAY_TRACKING || "false").toLowerCase() === "true";
const ENABLE_APPOINTMENT_TEMPLATES = String(process.env.ENABLE_APPOINTMENT_TEMPLATES || "true").toLowerCase() === "true";

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
  empresaCataleya: "CATALEYA Salón de Belleza",
};

function hasHubSpotEnabled() {
  return !!HUBSPOT_ACCESS_TOKEN;
}

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

function getConversationSnippetForClose(waId, maxMessages = 12) {
  if (!waId) return '';
  const conv = ensureConv(waId);
  const rows = Array.isArray(conv?.messages) ? conv.messages.slice(-maxMessages) : [];
  return rows
    .map((m) => `${m.role === 'assistant' ? 'Bot' : 'Cliente'}: ${String(m?.content || '').replace(/\s+/g, ' ').trim()}`)
    .filter(Boolean)
    .join('\n')
    .slice(0, 3000);
}

function safeJsonParseFromText(raw) {
  const txt = String(raw || '').trim();
  if (!txt) return null;
  try { return JSON.parse(txt); } catch {}
  const match = txt.match(/\{[\s\S]*\}/);
  if (!match) return null;
  try { return JSON.parse(match[0]); } catch { return null; }
}

async function analyzeCloseSummaryWithOpenAI({ ctx, conversationSnippet = '', productos = [], categorias = [] } = {}) {
  const latestFocusText = [ctx?.interest, ctx?.lastUserText].filter(Boolean).join(' | ');
  const fallback = buildMiniObservacion({ text: [latestFocusText, conversationSnippet].filter(Boolean).join(' | '), productos, categorias });
  const fallbackProduct = pickPrimaryProducto(latestFocusText || conversationSnippet || '');
  try {
    const completion = await openai.chat.completions.create({
      model: PRIMARY_MODEL,
      temperature: 0.2,
      messages: [
        {
          role: 'system',
          content: 'Sos un analista de CRM. Devolvés solo JSON válido con tres claves: observacion, nombre_completo y producto_principal. observacion debe ser una conclusión muy breve, natural y concreta en español sobre qué consultó la persona. Máximo 12 palabras, sin copiar textual el chat, sin preguntas, sin saludo y sin comillas. nombre_completo solo si la persona dijo claramente su nombre o si el nombre de perfil de WhatsApp parece un nombre real; si no, devolvé cadena vacía. producto_principal debe contener solo un producto o tema puntual realmente consultado, nunca una lista.'
        },
        {
          role: 'user',
          content: JSON.stringify({
            fecha: ddmmyyAR(),
            nombre_perfil_whatsapp: ctx?.profileName || ctx?.name || '',
            nombre_detectado_previo: ctx?.explicitName || '',
            ultimo_mensaje_cliente: ctx?.lastUserText || '',
            interes_detectado: ctx?.interest || '',
            productos_detectados: productos,
            categorias_detectadas: categorias,
            historial_reciente: conversationSnippet || ''
          })
        }
      ]
    });

    const parsed = safeJsonParseFromText(completion?.choices?.[0]?.message?.content || '');
    let observacion = String(parsed?.observacion || '').replace(/\s+/g, ' ').trim();
    let nombreCompleto = cleanNameCandidate(parsed?.nombre_completo || '');
    let productoPrincipal = cleanProductLabel(parsed?.producto_principal || '');

    observacion = observacion.replace(/^\d{2}\/\d{2}\/\d{2}\s*/, '').trim();
    if (!observacion) observacion = fallback.replace(/^\d{2}\/\d{2}\/\d{2}\s*/, '').trim();
    observacion = `${ddmmyyAR()} ${sentenceCase(observacion)}`.slice(0, 120);

    if (!productoPrincipal) productoPrincipal = fallbackProduct;

    return { observacion, explicitName: nombreCompleto, productoPrincipal };
  } catch (e) {
    return {
      observacion: fallback,
      explicitName: cleanNameCandidate(ctx?.explicitName || '') || extractExplicitNameFromSnippet(conversationSnippet),
      productoPrincipal: fallbackProduct,
    };
  }
}

// ===================== BOT =====================
const SYSTEM_PROMPT = `
Sos la asistente oficial de un salón de belleza de estética llamado "Cataleya" en Cafayate (Salta).
Tu rol es vender y ayudar con consultas de forma rápida, clara, muy amable y cercana.
Hablás en español rioplatense, con mensajes cortos y naturales. Profesional, cálida y humana.

ESTILO:
- Soná como una asistente real del salón: cercana, amable y simple.
- Mensajes cortos, claros y por etapas. Evitá bloques largos.
- Si inicia con “hola”, responder saludo + “¿en qué puedo ayudarte?” solamente.
- Emojis suaves y lindos cuando sumen claridad: ✨😊📅🕐💳📩
- Cuando des datos sensibles, presentalos en líneas separadas y prolijas.
- Si ya venían hablando de un tema, no vuelvas a preguntar lo mismo. Continuá desde el contexto.
- Si un término puede ser producto o servicio, preguntá cuál de las dos cosas busca antes de responder.
- Usar letras negrita donde sea necesario para aclarar algo importante. 

Ofrecés:
- Servicios estéticos
- Productos e insumos
- Muebles y equipamiento (espejos, sillones, camillas)
- Cursos de estética y capacitaciones
- Si preguntan por cursos, respondé SOLO con lo que exista en la hoja CURSOS. Nunca enumeres ni inventes cursos por tu cuenta. Si no encontrás coincidencias, decilo claramente.

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

function updateLastCloseContext(waId, patch = {}) {
  if (!waId) return null;
  const prev = lastCloseContext.get(waId) || {};
  const next = { ...prev, ...patch };
  lastCloseContext.set(waId, next);
  return next;
}

const INACTIVITY_MS = Number(process.env.INACTIVITY_FOLLOWUP_MS || 10 * 60 * 1000); // 10 minutos por defecto (mensaje de cierre)
const CLOSE_LOG_MS = Number(process.env.CLOSE_LOG_MS || 10 * 60 * 1000);  // 10 minutos más por defecto (si no responde, se registra)

async function logConversationClose(waId) {
  const ctx = lastCloseContext.get(waId);
  if (!ctx?.phone && !ctx?.phoneRaw && !ctx?.waId) return;
  await upsertHubSpotContactFromClose(ctx);
}

async function shouldSuppressInactivityFollowUp(waId) {
  if (!waId) return false;

  const ctx = lastCloseContext.get(waId) || {};
  if (ctx.suppressInactivityPrompt) return true;

  try {
    const draft = await getAppointmentDraft(waId);
    if (draft) return true;
  } catch (e) {
    console.error("Error verificando borrador de turno para inactividad:", e?.response?.data || e?.message || e);
  }

  if (lastAssistantLooksLikeTurnoMessage(waId)) return true;
  return false;
}

function scheduleInactivityFollowUp(waId, phone) {
  if (!waId || !phone) return;

  // si ya había uno, lo reiniciamos
  if (inactivityTimers.has(waId)) clearTimeout(inactivityTimers.get(waId));
  if (closeTimers.has(waId)) clearTimeout(closeTimers.get(waId));

  const timer = setTimeout(async () => {
    try {
      const suppress = await shouldSuppressInactivityFollowUp(waId);
      if (suppress) {
        return;
      }

      await sendWhatsAppText(
        phone,
        "¿Quiere que le ayudemos en algo más o damos por finalizada la consulta?"
      );
    } catch (e) {
      console.error("Error enviando mensaje por inactividad:", e?.response?.data || e?.message || e);
      return;
    }

    // Si NO respondió luego del mensaje de cierre, registramos el seguimiento
    const timer2 = setTimeout(async () => {
      try {
        const suppress = await shouldSuppressInactivityFollowUp(waId);
        if (suppress) return;
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
// ✅ Contexto de producto por usuario (para continuar la charla de forma fluida)
const lastProductContextByUser = new Map();
const PRODUCT_CONTEXT_TTL_MS = Number(process.env.PRODUCT_CONTEXT_TTL_MS || 45 * 60 * 1000);

function getLastProductContext(waId) {
  const ctx = lastProductContextByUser.get(waId);
  if (!ctx) return null;
  if ((Date.now() - (ctx.ts || 0)) > PRODUCT_CONTEXT_TTL_MS) {
    lastProductContextByUser.delete(waId);
    return null;
  }
  return ctx;
}

function setLastProductContext(waId, patch = {}) {
  if (!waId) return null;
  const prev = getLastProductContext(waId) || {};
  const next = {
    ...prev,
    ...patch,
    ts: Date.now(),
  };
  lastProductContextByUser.set(waId, next);
  return next;
}

function clearLastProductContext(waId) {
  lastProductContextByUser.delete(waId);
}

function clearProductMemory(waId) {
  clearLastProductContext(waId);
  if (waId) lastProductByUser.delete(waId);
}

function getLastAssistantMessage(waId) {
  const conv = ensureConv(waId);
  return [...(conv.messages || [])].reverse().find((m) => m.role === "assistant") || null;
}

function lastAssistantLooksLikeTurnoMessage(waId) {
  const last = getLastAssistantMessage(waId);
  const t = normalize(last?.content || '');
  if (!t) return false;
  return /(turno reservado|solicitud recibida|sena recibida|comprobante recibido|lo estoy validando|datos para la transferencia|cuando haga la transferencia|ahora necesito este dato)/i.test(t);
}

function lastAssistantLooksLikeCatalogMessage(waId) {
  const last = getLastAssistantMessage(waId);
  const t = normalize(last?.content || '');
  if (!t) return false;
  return /(esta en catalogo|catalogo completo|mas opciones del catalogo|si quiere, le ayudo a elegir|no lo encuentro asi en el catalogo)/i.test(t);
}

function normalizeShortReply(text) {
  return normalize(text || '')
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function isPoliteClosureAfterTurno(text) {
  const t = normalizeShortReply(text || '');
  if (!t) return false;
  return /^(gracias|muchas gracias|mil gracias|genial gracias|perfecto gracias|perfecto muchas gracias|buenisimo gracias|buenisimo muchas gracias|buenisimo|buenisima|genial|perfecto|listo gracias|ok gracias|oka gracias|dale gracias|barbaro gracias|barbaro|joya|joya gracias)$/.test(t);
}

function isPoliteCatalogDecline(text) {
  const t = normalizeShortReply(text || '');
  if (!t) return false;
  return /^(no gracias|no necesito|no necesito nada|no busco nada|no busco nada deja de enviarme|deja de enviarme|dejame ahi|dejalo ahi|dejala ahi|dejalo|dejala|no hace falta|no hace falta gracias)$/.test(t);
}

function looksLikeProductPreferenceReply(text) {
  const t = normalize(text || '');
  if (!t) return false;
  return /(cabello|pelo|rulos|rizado|lacio|liso|rubio|decolorado|canas|canoso|seco|graso|fino|grueso|quebradizo|dañado|danado|frizz|hidrata|hidratar|hidratacion|hidratación|reparar|reparacion|reparación|alisar|alisado|matizar|antiamarillo|violeta|uso personal|para trabajar|profesional|personal|me conviene|cual me recomendas|cuál me recomendás|recomendame|recomendáme|quiero opciones|que me recomendas|qué me recomendás)/i.test(t);
}
// ✅ Último servicio consultado por usuario (para no repetir pregunta en turnos)
const lastServiceByUser = new Map();
// ✅ Contexto de cursos por usuario (para no perder continuidad entre preguntas)
const lastCourseContextByUser = new Map();
const COURSE_CONTEXT_TTL_MS = Number(process.env.COURSE_CONTEXT_TTL_MS || 45 * 60 * 1000);

function getLastCourseContext(waId) {
  const ctx = lastCourseContextByUser.get(waId);
  if (!ctx) return null;
  if ((Date.now() - (ctx.ts || 0)) > COURSE_CONTEXT_TTL_MS) {
    lastCourseContextByUser.delete(waId);
    return null;
  }
  return ctx;
}

function setLastCourseContext(waId, patch = {}) {
  if (!waId) return null;
  const prev = getLastCourseContext(waId) || {};
  const next = {
    ...prev,
    ...patch,
    ts: Date.now(),
  };
  lastCourseContextByUser.set(waId, next);
  return next;
}

function clearLastCourseContext(waId) {
  if (waId) lastCourseContextByUser.delete(waId);
}
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

// ===================== ENTRADA AGRUPADA (mensajes seguidos) =====================
const INBOUND_MERGE_MS = Number(process.env.INBOUND_MERGE_MS || 1800);
const inboundMergeState = new Map();

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function appendInboundMergeChunk(waId, chunk) {
  const prev = inboundMergeState.get(waId) || { version: 0, items: [] };
  const next = {
    version: Number(prev.version || 0) + 1,
    items: [...(Array.isArray(prev.items) ? prev.items : []), { ...chunk, ts: Date.now() }].slice(-12),
  };
  inboundMergeState.set(waId, next);
  return next.version;
}

function consumeInboundMergeChunk(waId, version) {
  const state = inboundMergeState.get(waId);
  if (!state || state.version !== version) return null;
  inboundMergeState.delete(waId);

  const items = Array.isArray(state.items) ? state.items : [];
  const latest = items[items.length - 1] || {};
  const mergedText = items
    .map((item) => String(item?.text || '').trim())
    .filter(Boolean)
    .join('\n')
    .trim();
  const mergedUserIntentText = items
    .map((item) => String(item?.userIntentText || item?.text || '').trim())
    .filter(Boolean)
    .join('\n')
    .trim();
  const latestMedia = [...items].reverse().find((item) => !!item?.mediaMeta)?.mediaMeta || null;

  return {
    ...latest,
    itemCount: items.length,
    items,
    text: mergedText,
    userIntentText: mergedUserIntentText || mergedText,
    mediaMeta: latestMedia,
  };
}

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
const TURNOS_SHEET_ID = process.env.TURNOS_SHEET_ID ||"";

const TURNOS_TAB = process.env.TURNOS_TAB || "TURNOS";

// Calendar ID (compartir el calendario con el service account)
// Si no se configura, igual se anota en la planilla.
const CALENDAR_ID = String(process.env.CALENDAR_ID || process.env.GCALENDAR_ID || process.env.GOOGLE_CALENDAR_ID || "").trim();

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
const TURNOS_ALLOWED_BLOCKS = [
  { label: "mañana", start: "10:00", end: "12:00" },
  { label: "tarde", start: "17:00", end: "20:00" },
];

function turnoInfoBlock() {
  return (
`✨ Turnos en Cataleya

Profesional: ${TURNOS_STYLIST_NAME}
📅 Horarios para turnos:
• 10:00 a 12:00
• 17:00 a 20:00

Para confirmar el turno se solicita una seña de ${TURNOS_SENA_TXT}.`
  );
}

function pedirDatosRegistroTurnoBlock() {
  return (
`Perfecto 😊

Para registrar el turno necesito:
👤 Nombre completo
📱 Teléfono de contacto`
  );
}

function normalizeHourHM(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';

  const compact = raw
    .toLowerCase()
    .replace(/\bhoras?\b/g, 'hs')
    .replace(/\s+/g, '')
    .replace(/\.$/, '');

  let m = compact.match(/^(\d{1,2})$/);
  if (m) {
    const hh = Number(m[1]);
    if (hh >= 0 && hh <= 23) return `${String(hh).padStart(2, '0')}:00`;
    return '';
  }

  m = compact.match(/^(\d{1,2})(?:[:\.h])(\d{1,2})$/);
  if (m) {
    const hh = Number(m[1]);
    const mm = Number(m[2]);
    if (hh >= 0 && hh <= 23 && mm >= 0 && mm <= 59) {
      return `${String(hh).padStart(2, '0')}:${String(mm).padStart(2, '0')}`;
    }
    return '';
  }

  m = compact.match(/^(\d{1,2})hs$/);
  if (m) {
    const hh = Number(m[1]);
    if (hh >= 0 && hh <= 23) return `${String(hh).padStart(2, '0')}:00`;
    return '';
  }

  m = raw.match(/\b(?:a\s*las|alas|tipo|para\s*las|desde\s*las)?\s*(\d{1,2})(?:[:\.h](\d{1,2}))?\s*(hs?|horas?)\b/i);
  if (m) {
    const hh = Number(m[1]);
    const mm = Number(m[2] || 0);
    if (hh >= 0 && hh <= 23 && mm >= 0 && mm <= 59) {
      return `${String(hh).padStart(2, '0')}:${String(mm).padStart(2, '0')}`;
    }
  }

  return '';
}

function extractLikelyHourFromText(text) {
  const raw = String(text || '').trim();
  if (!raw) return '';

  const explicit = normalizeHourHM(raw);
  if (explicit) return explicit;

  const t = normalize(raw);

  let m = t.match(/(?:a las|alas|tipo|para las|desde las)?\s*(\d{1,2})(?:[:\.](\d{1,2}))?\s*(?:hs|hora|horas)?\b/);
  if (m) {
    const hh = Number(m[1]);
    const mm = Number(m[2] || 0);
    if (hh >= 7 && hh <= 22 && mm >= 0 && mm <= 59) {
      return `${String(hh).padStart(2, '0')}:${String(mm).padStart(2, '0')}`;
    }
  }

  return '';
}

function detectSenaPaid({ text }) {

  const t = normalize(text || "");
  return /(comprobant|transfer|transferi|transferí|señad|seña|pagu|pago|abon|abono|mercado pago|alias|cvu)/i.test(t);
}

function isLikelyPaymentText(text) {
  const t = normalize(text || "");
  return /(transfer|comprobante|mercado pago|mp|cvu|cbu|alias|titular|operacion|operación|seña|senia|pago|abon)/i.test(t);
}

function sanitizePossiblePhone(raw) {
  let digits = String(raw || '').replace(/[^\d+]/g, '');
  if (!digits) return '';
  const plus = digits.startsWith('+');
  digits = digits.replace(/[^\d]/g, '');
  if (digits.startsWith('549')) digits = '54' + digits.slice(3);
  if (digits.startsWith('0')) digits = digits.replace(/^0+/, '');
  if (digits.length < 8 || digits.length > 15) return '';
  return plus ? `+${digits}` : `+${digits}`;
}

function extractContactInfo(text) {
  const raw = String(text || "").trim();
  const norm = normalize(raw);

  let telefono = "";
  if (!isLikelyPaymentText(raw)) {
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
  }

  let nombre = "";
  if (!isLikelyPaymentText(raw)) {
    const explicitName = raw.match(/(?:me llamo|mi nombre es|soy)\s+([A-Za-zÁÉÍÓÚÑáéíóúñ' ]{5,60})/i);
    if (explicitName) {
      const cand = explicitName[1].replace(/\s+/g, ' ').trim();
      if (!/\d/.test(cand) && cand.split(' ').length >= 2) nombre = cand;
    }

    if (!nombre) {
      const nameBeforePhone = raw.match(/^\s*([A-Za-zÁÉÍÓÚÑáéíóúñ' ]{5,80}?)(?:\s*,\s*|\s+y\s+)?(?:(?:y\s+)?(?:su\s+)?(?:numero|número|telefono|tel|cel|celular|whatsapp|wsp)\b)/i);
      if (nameBeforePhone) {
        const cand = String(nameBeforePhone[1] || '').replace(/\s+/g, ' ').trim().replace(/[,:;.-]+$/, '');
        if (!/\d/.test(cand) && cand.split(' ').length >= 2) nombre = cand;
      }
    }

    if (!nombre) {
      const cleaned = raw.replace(/\s+/g, ' ').trim();
      const looksLikePureName = (
        !/\d/.test(cleaned) &&
        cleaned.split(' ').length >= 2 &&
        cleaned.length >= 5 &&
        cleaned.length <= 60 &&
        !/(quiero|quisiera|consulto|consulta|pregunto|pregunta|hola|buen dia|buen día|buenas|gracias|turno|mañana|lunes|martes|miercoles|miércoles|jueves|viernes|sabado|sábado|domingo|servicio|producto|hora|hs|alisado|botox|keratina|shampoo|matizador|nutricion|nutrición)/i.test(norm)
      );
      if (looksLikePureName) nombre = cleaned;
    }
  }

  return { nombre, telefono };
}

function shouldExtractContactNow(turno, text) {
  const flow = String(turno?.flow_step || '');
  if (['awaiting_contact', 'awaiting_name', 'awaiting_phone', 'ready_to_book'].includes(flow)) return true;
  const raw = String(text || '').trim();
  if (!raw || isLikelyPaymentText(raw)) return false;
  if (/(me llamo|mi nombre es|soy|telefono|tel|cel|celular|whatsapp|wsp|numero|número)/i.test(raw)) return true;
  return false;
}

function mergeContactIntoTurno({ turno, text, waPhone }) {
  const out = { ...(turno || {}) };
  if (!shouldExtractContactNow(out, text)) return out;

  const info = extractContactInfo(text);

  if (info.nombre && !out.cliente_full) out.cliente_full = info.nombre;
  if (info.telefono) out.telefono_contacto = normalizePhone(info.telefono);

  return out;
}


function normalizeMoney(value) {
  const raw = String(value || "").trim();
  if (!raw) return null;
  const cleaned = raw.replace(/[^\d,.-]/g, "");
  if (!cleaned) return null;
  const normalized = cleaned.includes(",")
    ? cleaned.replace(/\./g, "").replace(",", ".")
    : cleaned;
  const n = Number(normalized);
  return Number.isFinite(n) ? n : null;
}

function extractMoneyAmountFromText(text) {
  const raw = String(text || "");
  if (!raw) return null;

  const candidates = [];

  const reCurrency = /\$\s*([0-9]{1,3}(?:[\.,][0-9]{3})+|[0-9]{4,6})(?:([\.,][0-9]{1,2}))?/g;
  for (const m of raw.matchAll(reCurrency)) {
    const whole = [m[1] || "", m[2] || ""].join("");
    const n = normalizeMoney(whole);
    if (Number.isFinite(n)) candidates.push(n);
  }

  const rePlain = /(?:monto|importe|total|transferencia(?:\s+recibida)?|seña|senia)\s*[:\-]?\s*([0-9]{1,3}(?:[\.,][0-9]{3})+|[0-9]{4,6})(?:([\.,][0-9]{1,2}))?/gi;
  for (const m of raw.matchAll(rePlain)) {
    const whole = [m[1] || "", m[2] || ""].join("");
    const n = normalizeMoney(whole);
    if (Number.isFinite(n)) candidates.push(n);
  }

  if (!candidates.length) return null;
  if (candidates.includes(10000)) return 10000;

  const reasonable = candidates.find((n) => n >= 1000 && n <= 200000);
  return reasonable ?? candidates[0] ?? null;
}

function looksLikePaymentProofText(text) {
  const t = normalize(text || "");
  return /(transferencia|transferencia recibida|comprobante|mercado pago|aprobado|operacion|operacion nro|nro de operacion|alias|cvu|dinero disponible|monica pacheco|cataleya178|seña|senia)/i.test(t);
}

function isExpectedReceiver(receiver) {
  const r = normalize(receiver || "");
  return !!r && (
    r.includes(normalize(TURNOS_ALIAS)) ||
    r.includes(normalize(TURNOS_ALIAS_TITULAR))
  );
}

function mapDraftRowToTurno(row) {
  if (!row) return null;
  return {
    fecha: row.appointment_date ? String(row.appointment_date).slice(0, 10) : "",
    hora: row.appointment_time ? String(row.appointment_time).slice(0, 5) : "",
    servicio: row.service_name || "",
    duracion_min: Number(row.duration_min || 60) || 60,
    notas: row.service_notes || "",
    cliente_full: row.client_name || "",
    telefono_contacto: row.contact_phone || row.wa_phone || "",
    payment_status: row.payment_status || "not_paid",
    payment_amount: row.payment_amount == null ? null : Number(row.payment_amount),
    payment_sender: row.payment_sender || "",
    payment_receiver: row.payment_receiver || "",
    payment_proof_text: row.payment_proof_text || "",
    payment_proof_media_id: row.payment_proof_media_id || "",
    payment_proof_filename: row.payment_proof_filename || "",
    awaiting_contact: !!row.awaiting_contact,
    flow_step: row.flow_step || "",
    last_intent: row.last_intent || "",
    last_service_name: row.last_service_name || row.service_name || "",
    wa_phone: row.wa_phone || "",
  };
}

async function getAppointmentDraft(waId) {
  const r = await db.query(`SELECT * FROM appointment_drafts WHERE wa_id = $1 LIMIT 1`, [waId]);
  return mapDraftRowToTurno(r.rows[0]);
}

async function saveAppointmentDraft(waId, waPhone, draft) {
  const d = draft || {};
  await db.query(
    `INSERT INTO appointment_drafts (
      wa_id, wa_phone, client_name, contact_phone, service_name, service_notes,
      appointment_date, appointment_time, duration_min, wants_color_confirmation,
      payment_status, payment_amount, payment_sender, payment_receiver,
      payment_proof_text, payment_proof_media_id, payment_proof_filename,
      awaiting_contact, flow_step, last_intent, last_service_name, updated_at
    ) VALUES (
      $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,NOW()
    )
    ON CONFLICT (wa_id) DO UPDATE SET
      wa_phone = EXCLUDED.wa_phone,
      client_name = EXCLUDED.client_name,
      contact_phone = EXCLUDED.contact_phone,
      service_name = EXCLUDED.service_name,
      service_notes = EXCLUDED.service_notes,
      appointment_date = EXCLUDED.appointment_date,
      appointment_time = EXCLUDED.appointment_time,
      duration_min = EXCLUDED.duration_min,
      wants_color_confirmation = EXCLUDED.wants_color_confirmation,
      payment_status = EXCLUDED.payment_status,
      payment_amount = EXCLUDED.payment_amount,
      payment_sender = EXCLUDED.payment_sender,
      payment_receiver = EXCLUDED.payment_receiver,
      payment_proof_text = EXCLUDED.payment_proof_text,
      payment_proof_media_id = EXCLUDED.payment_proof_media_id,
      payment_proof_filename = EXCLUDED.payment_proof_filename,
      awaiting_contact = EXCLUDED.awaiting_contact,
      flow_step = EXCLUDED.flow_step,
      last_intent = EXCLUDED.last_intent,
      last_service_name = EXCLUDED.last_service_name,
      updated_at = NOW()`,
    [
      waId,
      normalizePhone(waPhone || d.telefono_contacto || ""),
      d.cliente_full || null,
      normalizePhone(d.telefono_contacto || waPhone || "") || null,
      d.servicio || null,
      d.notas || null,
      toYMD(d.fecha) || null,
      d.hora || null,
      Number(d.duracion_min || 60) || 60,
      !!d.wants_color_confirmation,
      d.payment_status || "not_paid",
      d.payment_amount == null ? null : Number(d.payment_amount),
      d.payment_sender || null,
      d.payment_receiver || null,
      d.payment_proof_text || null,
      d.payment_proof_media_id || null,
      d.payment_proof_filename || null,
      !!d.awaiting_contact,
      d.flow_step || null,
      d.last_intent || null,
      d.last_service_name || d.servicio || null,
    ]
  );
}

async function deleteAppointmentDraft(waId) {
  await db.query(`DELETE FROM appointment_drafts WHERE wa_id = $1`, [waId]);
}

function buildPaymentPendingMessage() {
  return `Para confirmar el turno se solicita una seña de ${TURNOS_SENA_TXT}.

💳 Datos para la transferencia

Alias
${TURNOS_ALIAS}

Titular
${TURNOS_ALIAS_TITULAR}

Cuando haga la transferencia, envíe por aquí el comprobante 📩`;
}

function detectMonto10000(text) {
  const t = normalize(String(text || ""));
  return /(^|\D)10[\.,\s]?000(\D|$)/.test(t) || /(^|\D)10000(\D|$)/.test(t);
}

function detectTitularMonicaPacheco(text) {
  const t = normalize(String(text || ""));
  return t.includes("monica pacheco") || t.includes("monica pachecho");
}

function isValidComprobanteSimple(text) {
  const t = String(text || "");
  return detectMonto10000(t) && detectTitularMonicaPacheco(t);
}

function looksLikePaymentIntentOnly(text) {
  const t = normalize(String(text || ""));
  return /(ahora te transfiero|ahora transfiero|te transfiero ahora|ya transfiero|te pago ahora|ahora pago|en un rato te transfiero|despues te transfiero|despues transfiero)/i.test(t);
}

function looksLikeProofAlreadySent(text) {
  const t = normalize(String(text || ""));
  return /(ya te lo envie|ya te lo envié|ya lo envie|ya lo envié|te lo envie|te lo envié|te mande el comprobante|te mandé el comprobante|mande el comprobante|mandé el comprobante|adjunte el comprobante|adjunté el comprobante|ya esta enviado|ya está enviado|comprobante enviado|ya esta|ya está|ahi va el comprobante|ahí va el comprobante|te mande la transferencia|te mandé la transferencia)/i.test(t);
}

function paymentMessageIsTooWeakToVerify(text) {
  const t = normalize(String(text || ""));
  if (!t) return true;
  return /^(ok|dale|listo|perfecto|gracias|chau|bueno|si|sí)$/i.test(t) || looksLikePaymentIntentOnly(t);
}

async function tryApplyPaymentToDraft(base, { text, mediaMeta } = {}) {
  const next = { ...(base || {}) };
  const rawText = String(text || "").trim();
  const previousProofText = String(next.payment_proof_text || "").trim();
  const previousProofExists = !!(next.payment_proof_media_id || previousProofText);
  const userSaysProofWasSent = looksLikeProofAlreadySent(rawText);
  const maybeProof = !!mediaMeta || detectSenaPaid({ text: rawText }) || looksLikePaymentProofText(rawText) || userSaysProofWasSent;
  if (!maybeProof) return next;

  next.payment_proof_text = rawText || next.payment_proof_text || "";
  next.payment_proof_media_id = mediaMeta?.id || next.payment_proof_media_id || "";
  next.payment_proof_filename = mediaMeta?.filename || mediaMeta?.file_name || next.payment_proof_filename || "";

  const analysisText = [rawText, previousProofText].filter(Boolean).join("\n").slice(0, 7000);
  const comprobanteOk = isValidComprobanteSimple(analysisText);
  const aiPago = await extractPagoInfoWithAI(analysisText);
  const aiMonto = extractMoneyAmountFromText(aiPago?.monto || "") || null;
  const heuristicAmount = extractMoneyAmountFromText(analysisText) || null;
  const receiverDetected = detectTitularMonicaPacheco(analysisText) || isExpectedReceiver(aiPago?.receptor || "");
  const amountLooksRight = aiMonto === 10000 || heuristicAmount === 10000;
  const aiLooksLikeProof = !!aiPago?.es_comprobante;
  const canVerifyWithConfidence = comprobanteOk || (aiLooksLikeProof && (receiverDetected || amountLooksRight || !!mediaMeta));

  if (canVerifyWithConfidence) {
    next.payment_status = "paid_verified";
    next.payment_amount = 10000;
    next.payment_receiver = "Monica Pacheco";
    if (aiPago?.pagador && !next.payment_sender) next.payment_sender = aiPago.pagador;
    return next;
  }

  if ((mediaMeta || userSaysProofWasSent || previousProofExists) && !paymentMessageIsTooWeakToVerify(rawText)) {
    next.payment_status = next.payment_status === "paid_verified" ? "paid_verified" : "payment_review";
    if (next.payment_status !== "paid_verified") {
      next.payment_amount = aiMonto || heuristicAmount || next.payment_amount || null;
      next.payment_receiver = receiverDetected ? "Monica Pacheco" : (next.payment_receiver || "");
      if (aiPago?.pagador && !next.payment_sender) next.payment_sender = aiPago.pagador;
    }
    return next;
  }

  if (mediaMeta || previousProofExists || userSaysProofWasSent) {
    next.payment_status = next.payment_status === "paid_verified" ? "paid_verified" : "payment_review";
    if (next.payment_status !== "paid_verified") {
      next.payment_amount = aiMonto || heuristicAmount || next.payment_amount || null;
      next.payment_receiver = receiverDetected ? "Monica Pacheco" : (next.payment_receiver || "");
      if (aiPago?.pagador && !next.payment_sender) next.payment_sender = aiPago.pagador;
    }
    return next;
  }

  next.payment_status = next.payment_status === "paid_verified" ? "paid_verified" : "not_paid";
  if (next.payment_status !== "paid_verified") {
    next.payment_amount = null;
    next.payment_receiver = "";
  }

  return next;
}

async function createAppointmentRecord({ waId, waPhone, merged, status, calendarEventId }) {
  const r = await db.query(
    `INSERT INTO appointments (
      wa_id, wa_phone, client_name, contact_phone, service_name, service_notes,
      appointment_date, appointment_time, duration_min, status, payment_status,
      payment_amount, payment_sender, payment_receiver, payment_proof_text,
      payment_proof_media_id, payment_proof_filename, calendar_event_id, is_color_service,
      created_at, updated_at
    ) VALUES (
      $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,
      $12,$13,$14,$15,$16,$17,$18,$19,NOW(),NOW()
    ) RETURNING id`,
    [
      waId,
      normalizePhone(waPhone || merged.telefono_contacto || ""),
      merged.cliente_full || null,
      normalizePhone(merged.telefono_contacto || "") || null,
      merged.servicio || null,
      merged.notas || null,
      toYMD(merged.fecha) || null,
      merged.hora || null,
      Number(merged.duracion_min || 60) || 60,
      status || "booked",
      merged.payment_status || "paid_verified",
      merged.payment_amount == null ? null : Number(merged.payment_amount),
      merged.payment_sender || null,
      merged.payment_receiver || null,
      merged.payment_proof_text || null,
      merged.payment_proof_media_id || null,
      merged.payment_proof_filename || null,
      calendarEventId || null,
      isColorOrTinturaService(merged.servicio || merged.notas || ""),
    ]
  );
  return r.rows[0];
}


function getAppointmentStartDate(appt) {
  const ymd = toYMD(appt?.appointment_date || '');
  const hm = formatAppointmentTimeForTemplate(appt?.appointment_time || '');
  if (!ymd || !hm) return null;
  const dt = new Date(`${ymd}T${hm}:00-03:00`);
  if (Number.isNaN(dt.getTime())) return null;
  return dt;
}

async function getAppointmentRowById(appointmentId) {
  const r = await db.query(
    `SELECT id, wa_id, wa_phone, client_name, contact_phone, service_name,
            appointment_date::text AS appointment_date,
            to_char(appointment_time, 'HH24:MI') AS appointment_time,
            status,
            stylist_notified_at,
            reminder_client_2h_at,
            reminder_client_24h_at,
            reminder_stylist_24h_at,
            reminder_stylist_2h_at
       FROM appointments
      WHERE id = $1
      LIMIT 1`,
    [appointmentId]
  );
  return r.rows[0] ? buildAppointmentData(r.rows[0]) : null;
}

async function processAppointmentTemplateNotifications() {
  const stylistRecipient = normalizeWhatsAppRecipient(STYLIST_NOTIFY_PHONE_RAW);
  if (!stylistRecipient) return;

  const r = await db.query(
    `SELECT id, wa_id, wa_phone, client_name, contact_phone, service_name,
            appointment_date::text AS appointment_date,
            to_char(appointment_time, 'HH24:MI') AS appointment_time,
            status,
            stylist_notified_at,
            reminder_client_2h_at,
            reminder_client_24h_at,
            reminder_stylist_24h_at,
            reminder_stylist_2h_at
       FROM appointments
      WHERE appointment_date IS NOT NULL
        AND appointment_time IS NOT NULL
        AND status IN ('booked', 'pending_stylist_confirmation')
        AND appointment_date >= CURRENT_DATE - INTERVAL '1 day'
      ORDER BY appointment_date ASC, appointment_time ASC`
  );

  const now = Date.now();
  for (const raw of (r.rows || [])) {
    const appt = buildAppointmentData(raw);
    const startAt = getAppointmentStartDate(appt);
    if (!startAt) continue;

    const msUntil = startAt.getTime() - now;
    if (msUntil <= 0) continue;

    try {
      if (!appt.stylist_notified_at) {
        await sendNewAppointmentTemplateToStylist(appt);
      }

      const clientRecipient = normalizeWhatsAppRecipient(appt.contact_phone || appt.wa_phone || '');
      if (!appt.reminder_client_2h_at && clientRecipient && msUntil <= (2 * 60 * 60 * 1000)) {
        await sendAppointmentTemplateAndLog({
          appointmentId: appt.id,
          recipientPhone: clientRecipient,
          templateName: TEMPLATE_RECORDATORIO_CLIENTE,
          notificationType: 'client_reminder_2h',
          vars: buildAppointmentTemplateVarsForClient(appt),
          markField: 'reminder_client_2h_at',
        });
      }

      if (!appt.reminder_stylist_2h_at && msUntil <= (2 * 60 * 60 * 1000)) {
        await sendAppointmentTemplateAndLog({
          appointmentId: appt.id,
          recipientPhone: stylistRecipient,
          templateName: TEMPLATE_ALERTA_PELUQUERA,
          notificationType: 'stylist_reminder_2h',
          vars: buildAppointmentTemplateVarsForStylistReminder(appt),
          markField: 'reminder_stylist_2h_at',
        });
      }
    } catch (e) {
      console.error(`❌ Error enviando plantillas de turno para appointment ${appt.id}:`, e?.response?.data || e?.message || e);
    }
  }
}

async function finalizeAppointmentFlow({ waId, phone, merged }) {
  merged.fecha = toYMD(merged.fecha);
  merged.hora = normalizeHourHM(merged.hora);
  if (!merged?.servicio || !merged?.fecha || !merged?.hora) return { type: "missing_core" };

  if (isPastAppointmentDateTime(merged.fecha, merged.hora)) return { type: "invalid_past_date" };

  const busy = await calendarHasConflict({
    dateYMD: merged.fecha,
    startHM: merged.hora,
    durationMin: Number(merged.duracion_min || 60) || 60,
  });
  if (busy) return { type: "busy" };

  if (!merged?.cliente_full) return { type: "need_name" };
  if (!merged?.telefono_contacto) return { type: "need_phone" };
  if (merged.payment_status !== "paid_verified") return { type: "need_payment" };

  if (isColorOrTinturaService(`${merged.servicio} ${merged.notas || ""}`)) {
    const apptRow = await createAppointmentRecord({
      waId,
      waPhone: phone,
      merged,
      status: "pending_stylist_confirmation",
      calendarEventId: null,
    });
    try {
      const apptData = buildAppointmentData({
        id: apptRow?.id,
        wa_id: waId,
        wa_phone: normalizePhone(phone || merged.telefono_contacto || ""),
        client_name: merged.cliente_full || "",
        contact_phone: normalizePhone(merged.telefono_contacto || ""),
        service_name: merged.servicio || "",
        appointment_date: merged.fecha,
        appointment_time: merged.hora,
        status: "pending_stylist_confirmation",
      });
      await sendNewAppointmentTemplateToStylist(apptData);
    } catch (e) {
      console.error('❌ Error notificando nuevo turno pendiente a la peluquera:', e?.response?.data || e?.message || e);
    }
    await deleteAppointmentDraft(waId);
        updateLastCloseContext(waId, { suppressInactivityPrompt: false });
    return { type: "pending_stylist_confirmation" };
  }

  const notasCalendar = [merged.notas || '', `SEÑA OK ${TURNOS_SENA_TXT}`].filter(Boolean).join(' | ');
  const ev = await createCalendarTurno({
    dateYMD: merged.fecha,
    startHM: merged.hora,
    durationMin: Number(merged.duracion_min || 60) || 60,
    cliente: merged.cliente_full || "",
    telefono: normalizePhone(merged.telefono_contacto || ""),
    servicio: merged.servicio || "",
    notas: notasCalendar,
  });

  const bookedRow = await createAppointmentRecord({
    waId,
    waPhone: phone,
    merged,
    status: "booked",
    calendarEventId: ev?.eventId || null,
  });

  try {
    const apptData = buildAppointmentData({
      id: bookedRow?.id,
      wa_id: waId,
      wa_phone: normalizePhone(phone || merged.telefono_contacto || ""),
      client_name: merged.cliente_full || "",
      contact_phone: normalizePhone(merged.telefono_contacto || ""),
      service_name: merged.servicio || "",
      appointment_date: merged.fecha,
      appointment_time: merged.hora,
      status: "booked",
    });
    await sendNewAppointmentTemplateToStylist(apptData);
  } catch (e) {
    console.error('❌ Error notificando nuevo turno a la peluquera:', e?.response?.data || e?.message || e);
  }

  if (TURNOS_SHEET_ID) {
    try {
      await appendTurnoRow({
        fechaYMD: merged.fecha,
        dia: weekdayEsFromYMD(merged.fecha),
        horaHM: merged.hora,
        cliente: merged.cliente_full || '',
        telefono: normalizePhone(merged.telefono_contacto || ''),
        servicio: merged.servicio || '',
        duracionMin: Number(merged.duracion_min || 60) || 60,
        calendarEventId: ev?.eventId || '',
      });
    } catch (e) {
      console.error('❌ Error guardando turno en planilla:', e?.response?.data || e?.message || e);
    }
  }

  await deleteAppointmentDraft(waId);
        updateLastCloseContext(waId, { suppressInactivityPrompt: false });
  return { type: "booked", eventId: ev?.eventId || null };
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

function buildValidYMD(year, month, day) {
  const yyyy = String(year || '').padStart(4, '0');
  const mm = String(month || '').padStart(2, '0');
  const dd = String(day || '').padStart(2, '0');
  if (!/^\d{4}$/.test(yyyy) || !/^\d{2}$/.test(mm) || !/^\d{2}$/.test(dd)) return '';
  const dt = new Date(`${yyyy}-${mm}-${dd}T12:00:00-03:00`);
  if (Number.isNaN(dt.getTime())) return '';
  const normalized = formatYMDHMInTZ(dt).ymd;
  return normalized === `${yyyy}-${mm}-${dd}` ? normalized : '';
}

function extractExplicitDateFromText(text) {
  const raw = String(text || '').trim();
  if (!raw) return '';

  let m = raw.match(/\b(\d{4})[\/-](\d{1,2})[\/-](\d{1,2})\b/);
  if (m) {
    return buildValidYMD(m[1], m[2], m[3]);
  }

  m = raw.match(/\b(\d{1,2})[\/-](\d{1,2})(?:[\/-](\d{2,4}))?\b/);
  if (m) {
    const day = Number(m[1]);
    const month = Number(m[2]);
    let year = m[3] ? Number(m[3]) : Number(todayYMDInTZ().slice(0, 4));
    if (year < 100) year += 2000;
    return buildValidYMD(year, month, day);
  }

  const t = normalize(raw);
  const monthMapEs = {
    enero: 1,
    febrero: 2,
    marzo: 3,
    abril: 4,
    mayo: 5,
    junio: 6,
    julio: 7,
    agosto: 8,
    septiembre: 9,
    setiembre: 9,
    octubre: 10,
    noviembre: 11,
    diciembre: 12,
  };

  m = t.match(/\b(\d{1,2})\s+de\s+(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|setiembre|octubre|noviembre|diciembre)(?:\s+de\s+(\d{2,4}))?\b/);
  if (m) {
    const day = Number(m[1]);
    const month = monthMapEs[m[2]];
    let year = m[3] ? Number(m[3]) : Number(todayYMDInTZ().slice(0, 4));
    if (year < 100) year += 2000;
    return buildValidYMD(year, month, day);
  }

  return '';
}

function nextDateForWeekday(targetWeekday, options = {}) {
  const includeToday = !!options.includeToday;
  const weekOffset = Math.max(0, Number(options.weekOffset || 0) || 0);

  const today = new Date(`${todayYMDInTZ()}T12:00:00-03:00`);
  const current = today.getDay();
  let delta = (targetWeekday - current + 7) % 7;

  if (!includeToday && delta === 0) delta = 7;
  delta += weekOffset * 7;

  const d = new Date(today.getTime() + delta * 24 * 60 * 60 * 1000);
  return formatYMDHMInTZ(d).ymd;
}

function extractLikelyDateFromText(text) {
  const raw = String(text || '').trim();
  if (!raw) return '';
  const t = normalize(raw);

  const explicit = extractExplicitDateFromText(raw);
  if (explicit) return explicit;

  if (/\bpasado manana\b/.test(t)) return addDaysToYMD(todayYMDInTZ(), 2);
  if (/\bhoy\b/.test(t)) return todayYMDInTZ();
  if (/\bmanana\b/.test(t)) return addDaysToYMD(todayYMDInTZ(), 1);

  const weekdays = {
    domingo: 0,
    lunes: 1,
    martes: 2,
    miercoles: 3,
    miércoles: 3,
    jueves: 4,
    viernes: 5,
    sabado: 6,
    sábado: 6,
  };

  for (const [name, num] of Object.entries(weekdays)) {
    const rxNextWeek = new RegExp(`\\b(?:el\\s+)?${escapeRegex(name)}\\s+(?:que\\s+viene|de\\s+la\\s+semana\\s+que\\s+viene|de\\s+la\\s+proxima\\s+semana|de\\s+la\\s+próxima\\s+semana|proximo|próximo|siguiente)\\b`, 'i');
    if (rxNextWeek.test(t)) {
      return nextDateForWeekday(num, { includeToday: false, weekOffset: 1 });
    }
  }

  for (const [name, num] of Object.entries(weekdays)) {
    const rxThisWeek = new RegExp(`\\b(?:este|esta)\\s+${escapeRegex(name)}\\b`, 'i');
    if (rxThisWeek.test(t)) {
      return nextDateForWeekday(num, { includeToday: true });
    }
  }

  for (const [name, num] of Object.entries(weekdays)) {
    if (new RegExp(`\\b${escapeRegex(name)}\\b`, 'i').test(t)) {
      return nextDateForWeekday(num, { includeToday: true });
    }
  }

  return '';
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
    const horaNormalizada = normalizeHourHM((obj.hora || '').trim());
    const fechaInterpretadaPorTexto = extractLikelyDateFromText(text);
    const fechaInterpretadaPorIA = toYMD((obj.fecha || "").trim());

    return {
      ok: !!obj.ok,
      fecha: fechaInterpretadaPorTexto || fechaInterpretadaPorIA || "",
      hora: horaNormalizada,
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

  const safeDate = toYMD(dateYMD);
  const safeHM = normalizeHourHM(startHM);
  if (!safeDate || !safeHM) return false;

  const start = new Date(`${safeDate}T${safeHM}:00-03:00`);
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

  const safeDate = toYMD(dateYMD);
  const safeHM = normalizeHourHM(startHM);
  if (!safeDate || !safeHM) throw new Error('Valor de tiempo no válido');

  const cal = await getCalendarClient();
  const startLocal = { ymd: safeDate, hm: safeHM };
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


function hmToMinutes(value) {
  const hm = normalizeHourHM(value);
  if (!hm) return NaN;
  const [hh, mm] = hm.split(':').map(Number);
  return (hh * 60) + mm;
}

function minutesToHM(totalMinutes) {
  const mins = Math.max(0, Number(totalMinutes) || 0);
  const hh = Math.floor(mins / 60);
  const mm = mins % 60;
  return `${String(hh).padStart(2, '0')}:${String(mm).padStart(2, '0')}`;
}

function addDaysToYMD(ymd, days) {
  const base = new Date(`${toYMD(ymd)}T12:00:00-03:00`);
  const next = new Date(base.getTime() + (Number(days) || 0) * 24 * 60 * 60 * 1000);
  return formatYMDHMInTZ(next).ymd;
}

function getTurnoSlotStepMin(durationMin) {
  const safeDuration = Math.max(30, Number(durationMin) || 60);
  if (safeDuration <= 30) return 30;
  if (safeDuration % 60 === 0) return 60;
  return 30;
}

function isSundayYMD(ymd) {
  try {
    const d = new Date(`${toYMD(ymd)}T12:00:00-03:00`);
    return d.getDay() === 0;
  } catch {
    return false;
  }
}

function capitalizeEs(text) {
  const s = String(text || '').trim();
  return s ? s.charAt(0).toUpperCase() + s.slice(1) : '';
}

function eventOverlapsSlot(event, { dateYMD, startHM, durationMin }) {
  if (!event || event.status === 'cancelled') return false;

  const safeDate = toYMD(dateYMD);
  const safeHM = normalizeHourHM(startHM);
  const duration = Math.max(30, Number(durationMin) || 60);
  if (!safeDate || !safeHM) return false;

  const slotStart = new Date(`${safeDate}T${safeHM}:00-03:00`);
  const slotEnd = new Date(slotStart.getTime() + duration * 60000);

  let eventStart = null;
  let eventEnd = null;

  if (event?.start?.dateTime) eventStart = new Date(event.start.dateTime);
  else if (event?.start?.date) eventStart = new Date(`${event.start.date}T00:00:00-03:00`);

  if (event?.end?.dateTime) eventEnd = new Date(event.end.dateTime);
  else if (event?.end?.date) eventEnd = new Date(`${event.end.date}T00:00:00-03:00`);

  if (!eventStart || !eventEnd || Number.isNaN(eventStart.getTime()) || Number.isNaN(eventEnd.getTime())) return false;
  return slotStart < eventEnd && slotEnd > eventStart;
}

async function listCalendarEventsInRange({ fromYMD, toYMDExclusive }) {
  if (!CALENDAR_ID) return [];
  const cal = await getCalendarClient();

  const timeMin = new Date(`${toYMD(fromYMD)}T00:00:00-03:00`).toISOString();
  const timeMax = new Date(`${toYMD(toYMDExclusive)}T00:00:00-03:00`).toISOString();

  let pageToken = undefined;
  const items = [];

  do {
    const resp = await cal.events.list({
      calendarId: CALENDAR_ID,
      timeMin,
      timeMax,
      singleEvents: true,
      orderBy: 'startTime',
      maxResults: 250,
      pageToken,
    });

    items.push(...(resp?.data?.items || []));
    pageToken = resp?.data?.nextPageToken || undefined;
  } while (pageToken);

  return items;
}

function getUpcomingTurnoDays(limit = 6) {
  const out = [];
  let cursor = todayYMDInTZ();
  let guard = 0;

  while (out.length < limit && guard < 21) {
    if (!isSundayYMD(cursor)) out.push(cursor);
    cursor = addDaysToYMD(cursor, 1);
    guard += 1;
  }

  return out;
}

function getAvailableSlotsForDate({ dateYMD, durationMin, events = [] }) {
  const safeDate = toYMD(dateYMD);
  const safeDuration = Math.max(30, Number(durationMin) || 60);
  if (!safeDate || isSundayYMD(safeDate) || safeDate < todayYMDInTZ()) return [];

  const step = getTurnoSlotStepMin(safeDuration);
  const nowLocal = formatYMDHMInTZ(new Date());
  const nowMinutes = safeDate === nowLocal.ymd ? hmToMinutes(nowLocal.hm) : -1;
  const slots = [];

  for (const block of TURNOS_ALLOWED_BLOCKS) {
    const blockStart = hmToMinutes(block.start);
    const blockEnd = hmToMinutes(block.end);
    if (Number.isNaN(blockStart) || Number.isNaN(blockEnd)) continue;

    let cursor = blockStart;
    if (safeDate === nowLocal.ymd && nowMinutes >= 0) {
      const roundedNow = Math.ceil(nowMinutes / step) * step;
      cursor = Math.max(cursor, roundedNow);
    }

    while ((cursor + safeDuration) <= blockEnd) {
      const hm = minutesToHM(cursor);
      const hasConflict = events.some((ev) => eventOverlapsSlot(ev, {
        dateYMD: safeDate,
        startHM: hm,
        durationMin: safeDuration,
      }));

      if (!hasConflict) {
        slots.push({ hm, label: block.label });
      }

      cursor += step;
    }
  }

  return slots;
}

function isPastAppointmentDateTime(dateYMD, timeHM) {
  const safeDate = toYMD(dateYMD);
  if (!safeDate) return false;

  const nowLocal = formatYMDHMInTZ(new Date());
  if (safeDate < nowLocal.ymd) return true;
  if (safeDate > nowLocal.ymd) return false;

  const safeHM = normalizeHourHM(timeHM);
  if (!safeHM) return false;

  return hmToMinutes(safeHM) <= hmToMinutes(nowLocal.hm);
}

function formatSlotsByBlock(slots = []) {
  if (!Array.isArray(slots) || !slots.length) return '';

  const grouped = TURNOS_ALLOWED_BLOCKS
    .map((block) => {
      const values = slots.filter((slot) => slot.label === block.label).map((slot) => slot.hm);
      return values.length ? `${block.label}: ${values.join(', ')}` : '';
    })
    .filter(Boolean);

  return grouped.join(' | ');
}

function formatSlotsByBlockMultiline(slots = []) {
  if (!Array.isArray(slots) || !slots.length) return '• Sin horarios disponibles';

  return TURNOS_ALLOWED_BLOCKS
    .map((block) => {
      const values = slots.filter((slot) => slot.label === block.label).map((slot) => slot.hm);
      return values.length ? `• ${capitalizeEs(block.label)}: ${values.join(', ')}` : '';
    })
    .filter(Boolean)
    .join('\n');
}

async function getAvailabilitySummaries({ daysYMD, durationMin }) {
  const safeDays = (Array.isArray(daysYMD) ? daysYMD : []).map((d) => toYMD(d)).filter(Boolean);
  if (!safeDays.length) return [];

  if (!CALENDAR_ID) {
    return safeDays.map((dateYMD) => ({
      dateYMD,
      weekday: weekdayEsFromYMD(dateYMD),
      slots: getAvailableSlotsForDate({ dateYMD, durationMin, events: [] }),
    }));
  }

  const first = safeDays[0];
  const lastExclusive = addDaysToYMD(safeDays[safeDays.length - 1], 1);
  const items = await listCalendarEventsInRange({ fromYMD: first, toYMDExclusive: lastExclusive });

  return safeDays.map((dateYMD) => ({
    dateYMD,
    weekday: weekdayEsFromYMD(dateYMD),
    slots: getAvailableSlotsForDate({ dateYMD, durationMin, events: items }),
  }));
}

async function buildWeeklyAvailabilityMessage({ servicio, durationMin, limitDays = 6 }) {
  const days = getUpcomingTurnoDays(limitDays);
  const summaries = await getAvailabilitySummaries({ daysYMD: days, durationMin });
  const available = summaries.filter((item) => item.slots.length > 0);

  if (!available.length) {
    return `En este momento no me quedan turnos disponibles en los próximos días dentro de los horarios del salón. Si quiere, después le reviso la próxima semana 😊`;
  }

  const daysToShow = available.slice(0, 3);
  const lines = daysToShow.map((item) => {
    const dayHeader = `*${capitalizeEs(item.weekday)} ${ymdToDM(item.dateYMD)}*`;
    const slotLines = formatSlotsByBlockMultiline(item.slots);
    return `${dayHeader}\n${slotLines}`;
  });

  return `Perfecto 😊\n\n${servicio ? `Servicio: ${servicio}\n\n` : ''}Te paso los próximos turnos disponibles:\n\n${lines.join('\n\n')}\n\nDecime qué día y horario te queda mejor y lo reservo.`;
}

async function buildDateAvailabilityMessage({ dateYMD, servicio, durationMin }) {
  const safeDate = toYMD(dateYMD);
  if (!safeDate) return '';

  if (isSundayYMD(safeDate)) {
    return `Los domingos no trabajamos con turnos 😊\n\nSi quiere, le paso las opciones disponibles de lunes a sábado.`;
  }

  const [summary] = await getAvailabilitySummaries({ daysYMD: [safeDate], durationMin });
  const dayLabel = `${capitalizeEs(summary?.weekday || weekdayEsFromYMD(safeDate))} ${ymdToDM(safeDate)}`;

  if (summary?.slots?.length) {
    return `Te digo lo que nos queda disponible:\n\n${servicio ? `Servicio: ${servicio}\n\n` : ''}*${dayLabel}*\n${formatSlotsByBlockMultiline(summary.slots)}\n\nDecime cuál le queda mejor y lo dejo listo.`;
  }

  const weekly = await buildWeeklyAvailabilityMessage({ servicio, durationMin, limitDays: 6 });
  return `Ese día no me queda lugar disponible dentro del horario del salón.\n\n${weekly}`;
}

async function buildBusyTurnoMessage({ base }) {
  const safeDate = toYMD(base?.fecha || '');
  const safeDuration = Math.max(30, Number(base?.duracion_min || 60) || 60);
  const servicio = base?.servicio || base?.last_service_name || '';

  if (safeDate) {
    const sameDayMsg = await buildDateAvailabilityMessage({ dateYMD: safeDate, servicio, durationMin: safeDuration });
    const diaC = capitalizeEs(weekdayEsFromYMD(safeDate));
    return `Ese horario ya está ocupado (${diaC} ${ymdToDM(safeDate)} ${normalizeHourHM(base?.hora) || base?.hora}).\n\n${sameDayMsg}`;
  }

  return buildWeeklyAvailabilityMessage({ servicio, durationMin: safeDuration, limitDays: 6 });
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

function ymdToDM(ymd) {
  const s = String(ymd || "").trim();
  const m = s.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!m) return s;
  return `${m[3]}/${m[2]}`;
}

// Acepta "DD/MM/YYYY", "DD-MM-YYYY", "YYYY-MM-DD" y devuelve siempre "YYYY-MM-DD" (si puede).
function toYMD(dateStr) {
  const s = String(dateStr || "").trim();
  if (!s) return "";

  let m = s.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (m) return buildValidYMD(m[1], m[2], m[3]);

  m = s.match(/^(\d{4})[\/\-](\d{1,2})[\/\-](\d{1,2})$/);
  if (m) return buildValidYMD(m[1], m[2], m[3]);

  m = s.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})$/);
  if (m) return buildValidYMD(m[3], m[2], m[1]);

  m = s.match(/^(\d{1,2})[\/\-](\d{1,2})$/);
  if (m) {
    const yyyy = todayYMDInTZ().slice(0, 4);
    return buildValidYMD(yyyy, m[2], m[1]);
  }

  const rel = normalize(s);
  if (rel === "hoy") return todayYMDInTZ();
  if (rel === "manana" || rel === "mañana") return addDaysToYMD(todayYMDInTZ(), 1);

  const explicit = extractExplicitDateFromText(s);
  if (explicit) return explicit;

  const monthMap = {
    jan: '01', feb: '02', mar: '03', apr: '04', may: '05', jun: '06',
    jul: '07', aug: '08', sep: '09', oct: '10', nov: '11', dec: '12'
  };
  const cleaned = s.replace(/,/g, ' ').replace(/\s+/g, ' ').trim();
  m = cleaned.match(/^(?:sun|mon|tue|wed|thu|fri|sat)\s+([A-Za-z]{3})\s+(\d{1,2})(?:\s+(\d{4}))?$/i)
    || cleaned.match(/^([A-Za-z]{3})\s+(\d{1,2})(?:\s+(\d{4}))?$/i);
  if (m) {
    const mon = monthMap[String(m[1]).slice(0,3).toLowerCase()];
    const dd = String(m[2]).padStart(2, '0');
    const yyyy = String(m[3] || todayYMDInTZ().slice(0, 4));
    if (mon) return buildValidYMD(yyyy, mon, dd);
  }

  const looksSafeForNativeDate =
    /T\d{2}:\d{2}/.test(s) ||
    /\b(?:GMT|UTC)\b/i.test(s) ||
    /^[A-Za-z]{3}\s+[A-Za-z]{3}\s+\d{1,2}\s+\d{4}/.test(s);

  if (looksSafeForNativeDate) {
    const jsDate = new Date(s);
    if (!isNaN(jsDate.getTime())) {
      return formatYMDHMInTZ(jsDate).ymd;
    }
  }

  return "";
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

function escapeRegex(str) {
  return String(str || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
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

const PRODUCT_KEYWORDS = [
  { label: "Baño de crema", patterns: [/bano de crema/, /baño de crema/] },
  { label: "Limpieza Facial", patterns: [/limpieza facial/] },
  { label: "Mesa Manicura", patterns: [/mesa manicura/] },
  { label: "Silla Hidráulica", patterns: [/silla hidraulica/, /silla hidráulica/] },
  { label: "Aceite máquina", patterns: [/aceite maquina/, /aceite máquina/] },
  { label: "Curso Barbería", patterns: [/curso barber/, /curso barberia/, /curso barbería/] },
  { label: "Curso Peluquería", patterns: [/curso peluqueria/, /curso peluquería/] },
  { label: "Curso Niños", patterns: [/curso ninos/, /curso niños/] },
  { label: "Espejos", patterns: [/\bespejo\b/, /\bespejos\b/] },
  { label: "Camillas", patterns: [/\bcamilla\b/, /\bcamillas\b/] },
  { label: "Mesas", patterns: [/\bmesa\b/, /\bmesas\b/] },
  { label: "Respaldo", patterns: [/respaldo/] },
  { label: "Puff", patterns: [/\bpuff\b/] },
  { label: "Planchas", patterns: [/\bplancha\b/, /\bplanchas\b/] },
  { label: "Secadores", patterns: [/\bsecador\b/, /\bsecadores\b/, /\bsecadora\b/] },
  { label: "Shampoo", patterns: [/\bshampoo\b/, /\bchampu\b/, /\bshampu\b/] },
  { label: "Ampolla", patterns: [/\bampolla\b/, /\bampollas\b/] },
  { label: "Acondicionador", patterns: [/\bacondicionador\b/] },
  { label: "Sérum", patterns: [/\bserum\b/, /\bsérum\b/] },
  { label: "Ácido", patterns: [/\bacido\b/, /\bácido\b/] },
  { label: "Nutrición", patterns: [/nutricion/, /nutrición/] },
  { label: "Tintura", patterns: [/\btintura\b/] },
  { label: "Matizador", patterns: [/matizador/] },
  { label: "Decolorante", patterns: [/decolorante/] },
  { label: "Cera", patterns: [/\bcera\b/] },
  { label: "Alisado", patterns: [/\balisado\b/] },
  { label: "Corte", patterns: [/\bcorte\b/] },
  { label: "Uñas", patterns: [/\bunas\b/, /\buñas\b/, /manicura/] },
  { label: "Pestañas", patterns: [/pestan/, /pestañ/] },
  { label: "Lifting", patterns: [/lifting/] },
  { label: "Cejas", patterns: [/\bcejas\b/, /\bceja\b/] },
  { label: "Depilación", patterns: [/depil/] },
  { label: "Peinado", patterns: [/peinad/] },
  { label: "Maquillaje", patterns: [/maquill/] },
  { label: "Masajes", patterns: [/masaj/] },
  { label: "Trenzas", patterns: [/trenza/] },
  { label: "Permanente", patterns: [/permanente/] },
  { label: "Tijeras", patterns: [/tijera/] },
  { label: "Barbería", patterns: [/barber/, /\bbarba\b/] },
  { label: "Cursos", patterns: [/\bcurso\b/, /\bcursos\b/, /\btaller\b/, /\btalleres\b/] },
  { label: "Muebles", patterns: [/\bmueble\b/, /\bmuebles\b/] },
];

function mergeHubSpotMulti(existingValue, newValues = []) {
  const list = [];
  const pushMany = (raw, splitter) => {
    String(raw || '')
      .split(splitter)
      .map((x) => x.trim())
      .filter(Boolean)
      .forEach((x) => { if (!list.includes(x)) list.push(x); });
  };
  pushMany(existingValue, ';');
  (Array.isArray(newValues) ? newValues : []).forEach((x) => {
    const v = String(x || '').trim();
    if (v && !list.includes(v)) list.push(v);
  });
  return list.join('; ');
}

function mergeSlashText(existingValue, newValues = [], maxItems = 6) {
  const list = [];
  const pushMany = (raw) => {
    String(raw || '')
      .split(/[\/\n;,]+/)
      .map((x) => x.trim())
      .filter(Boolean)
      .forEach((x) => { if (!list.includes(x)) list.push(x); });
  };
  pushMany(existingValue);
  (Array.isArray(newValues) ? newValues : []).forEach((x) => {
    const v = String(x || '').trim();
    if (v && !list.includes(v)) list.push(v);
  });
  return list.slice(0, maxItems).join(' / ');
}

function detectProductKeywords(text) {
  const t = canonicalizeQuery(text);
  const hits = [];
  for (const item of PRODUCT_KEYWORDS) {
    if (item.patterns.some((rx) => rx.test(t))) hits.push(item.label);
  }
  return Array.from(new Set(hits));
}

function pickCategorias({ intentType, text }) {
  const t = canonicalizeQuery(text);
  const found = [];

  const hasServiceSignal =
    intentType === "SERVICE" ||
    /\bturno\b|\bservicio\b|\bagenda\b|\bcita\b|\bhacerme\b|\bpara hacerme\b|\bme quiero hacer\b|\bme gustaria hacerme\b/.test(t);

  const hasProductSignal =
    intentType === "PRODUCT" ||
    /\bproducto\b|\binsumo\b|\binsumos\b|\btenes\b|\btenés\b|\bvenden\b|\bcomprar\b|\bstock\b|\bprecio\b/.test(t);

  const hasCourseSignal =
    intentType === "COURSE" || /\bcurso\b|\bcursos\b|\btaller\b|\btalleres\b/.test(t);

  const productHits = detectProductKeywords(t);

  if (hasCourseSignal || productHits.some((p) => String(p).startsWith('Curso')) || productHits.includes('Cursos')) {
    found.push("CURSOS📝");
  }

  if (
    /\bcamilla\b|\bcamillas\b|\bespejo\b|\bespejos\b|\brespaldo\b|\bmueble\b|\bmuebles\b|\bmesa\b|\bmesas\b|\bpuff\b|\bsilla\b/.test(t)
  ) {
    found.push("MUEBLES🪑");
  }

  if (
    /\bmaquina\b|\bmáquina\b|\bmaquinas\b|\bmáquinas\b|\bplancha\b|\bplanchas\b|\bsecador\b|\bsecadores\b/.test(t)
  ) {
    found.push("MÁQUINAS⚙️");
  }

  if (
    /\bbarber\b|\bbarberia\b|\bbarbería\b|\bbarba\b|\btijera\b|\baceite maquina\b|\baceite máquina\b/.test(t)
  ) {
    found.push("BARBER💈");
  }

  const serviceProducts = ["Alisado", "Corte", "Uñas", "Pestañas", "Lifting", "Cejas", "Depilación", "Peinado", "Maquillaje", "Limpieza Facial", "Masajes", "Trenzas", "Permanente"];
  const productProducts = ["Shampoo", "Ampolla", "Ácido", "Nutrición", "Tintura", "Matizador", "Decolorante", "Cera", "Aceite máquina"];

  if (hasServiceSignal || productHits.some((p) => serviceProducts.includes(p))) {
    found.push("SERVICIOS DE BELLEZA💄");
  }

  if (hasProductSignal || productHits.some((p) => productProducts.includes(p))) {
    found.push("INSUMOS🧴");
  }

  // Ambiguos como "tintura" o "nutrición" sin pista: respetar intención.
  if (!found.length) {
    if (intentType === "SERVICE") found.push("SERVICIOS DE BELLEZA💄");
    if (intentType === "PRODUCT") found.push("INSUMOS🧴");
    if (intentType === "COURSE") found.push("CURSOS📝");
  }

  return Array.from(new Set(found)).filter((x) => CATEGORIAS_OK.includes(x));
}

function pickCategoria({ intentType, text }) {
  return pickCategorias({ intentType, text })[0] || "";
}

function pickProductosList(text) {
  return detectProductKeywords(text).slice(0, 6);
}

function pickProductos(text) {
  return pickProductosList(text).join(" / ");
}

function cleanProductLabel(value) {
  const txt = String(value || '').replace(/\s+/g, ' ').trim();
  if (!txt) return '';
  const direct = PRODUCT_KEYWORDS.find((item) => normalize(item.label) === normalize(txt));
  if (direct) return direct.label;
  const detected = detectProductKeywords(txt);
  return detected[0] || '';
}

function pickPrimaryProducto(text) {
  const source = String(text || '').trim();
  if (!source) return '';
  const hits = detectProductKeywords(source);
  return hits[0] || '';
}

function ddmmyyAR() {
  const d = new Date();
  return d.toLocaleDateString("es-AR", { timeZone: TIMEZONE, year: '2-digit', month: '2-digit', day: '2-digit' });
}

function fallbackTopicFromText(raw) {
  const clean = String(raw || '')
    .replace(/\s+/g, ' ')
    .replace(/^(hola+|buenas+|buen dia|buen día|buenas tardes|buenas noches)[,!\s]*/i, '')
    .replace(/^(quiero saber|quisiera saber|queria saber|quería saber|consulto por|consultaba por|consulta sobre|consulta por)\s+/i, '')
    .replace(/^(precio de|precio por|busco|necesito)\s+/i, '')
    .trim();

  return clean ? clean.slice(0, 70) : '';
}

function sentenceCase(value) {
  const txt = String(value || '').replace(/\s+/g, ' ').trim();
  if (!txt) return '';
  return txt.charAt(0).toUpperCase() + txt.slice(1);
}

function extractObservationNote(rawText) {
  const t = normalize(rawText || '');
  const notes = [];

  if (/pelo seco/.test(t)) notes.push('tiene pelo seco');
  if (/pelo danado|pelo dañado|cabello danado|cabello dañado|cabello arruinado|pelo arruinado/.test(t)) notes.push('tiene el cabello dañado');
  if (/frizz/.test(t)) notes.push('menciona frizz');
  if (/para trabajar|para uso profesional|tengo una peluqueria|tiene una peluqueria|tengo una peluquería|tiene una peluquería|para la peluqueria|para la peluquería|salon|salón/.test(t)) notes.push('busca para trabajar, tiene una peluquería');
  if (/para mi|para mí|para ella/.test(t)) notes.push('consulta para ella');
  if (/para el|para él/.test(t)) notes.push('consulta para él');
  if (/curso/.test(t) && /para mi|para mí|para ella/.test(t)) notes.push('consulta sobre cursos para ella');

  return Array.from(new Set(notes)).join(', ');
}

function buildMiniObservacion({ text = '', productos = [], categorias = [] } = {}) {
  const raw = String(text || '').replace(/\s+/g, ' ').trim();
  const foco = productos.length ? productos.join(' / ') : fallbackTopicFromText(raw) || (categorias[0] || 'su consulta');
  const note = extractObservationNote(raw);
  const cuerpo = sentenceCase(`Consulta sobre ${foco}${note ? `. ${note}` : ''}`.trim());
  const line = `${ddmmyyAR()} ${cuerpo}`;
  return line.slice(0, 120);
}

function mergeObservationHistory(existingValue, newLine, maxLines = 8) {
  const existingLines = String(existingValue || '')
    .split(/\r?\n+/)
    .map((x) => x.trim())
    .filter(Boolean);

  const cleanNew = String(newLine || '').trim();
  if (!cleanNew) return existingLines.join('\n');

  if (existingLines[existingLines.length - 1] === cleanNew) {
    return existingLines.slice(-maxLines).join('\n');
  }

  existingLines.push(cleanNew);
  return existingLines.slice(-maxLines).join('\n');
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

function userAsksForAllPhotos(text) {
  const t = normalize(text);
  if (!userAsksForPhoto(t)) return false;
  return /(de todo|de todos|de todas|de esas|de esos|de estas|de estos|todos|todas|todas las opciones|todas las que tengas|todas las fotos|todo eso|de todo eso|de los productos|de las opciones|de todo lo que me mostraste|de todo lo que me mostras|de todo lo que me mostro)/.test(t);
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
const COURSES_SHEET_ID = SERVICES_SHEET_ID;

const STOCK_TABS = ["Productos", "Equipamiento", "Muebles"];
const SERVICES_TAB = "SERVICIOS";
const COURSES_TAB = "CURSOS";

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


function parseServiceDurationToMinutes(value) {
  const raw = String(value || '').trim();
  if (!raw) return 0;

  const t = normalize(raw)
    .replace(/\s+/g, ' ')
    .replace(/,/g, '.')
    .trim();

  if (!t) return 0;

  let m = t.match(/(\d+(?:\.\d+)?)\s*(?:hora|horas|hs|h)\s*y\s*media\b/);
  if (m) return Math.round(Number(m[1]) * 60 + 30);

  m = t.match(/(\d+(?:\.\d+)?)\s*y\s*media\s*(?:hora|horas|hs|h)?\b/);
  if (m) return Math.round(Number(m[1]) * 60 + 30);

  m = t.match(/(\d+(?:\.\d+)?)\s*(?:hora|horas|hs|h)\s*(?:y\s*)?(\d{1,2})\s*(?:min|minutos)\b/);
  if (m) return Math.round(Number(m[1]) * 60 + Number(m[2]));

  m = t.match(/(\d+(?:\.\d+)?)\s*(?:hora|horas|hs|h)\b/);
  if (m) return Math.round(Number(m[1]) * 60);

  m = t.match(/\bmedia\s*(?:hora|hs|h)\b/);
  if (m) return 30;

  m = t.match(/(\d{1,3})\s*(?:min|minutos)\b/);
  if (m) return Number(m[1]) || 0;

  return 0;
}

function resolveServiceCatalogMatch(rows, serviceName) {
  const query = String(serviceName || '').trim();
  if (!query) return null;

  const cleanQuery = normalize(cleanServiceName(query));
  if (!cleanQuery) return null;

  const exactClean = rows.filter((r) => normalize(cleanServiceName(r?.nombre || '')) === cleanQuery);
  if (exactClean.length) return exactClean[0];

  const exactRaw = rows.filter((r) => normalize(String(r?.nombre || '')) === normalize(query));
  if (exactRaw.length) return exactRaw[0];

  const detailMatches = findServices(rows, query, 'DETAIL');
  if (detailMatches.length) return detailMatches[0];

  const baseQuery = getServiceBaseFromName(query);
  if (baseQuery) {
    const byBase = rows.filter((r) => getServiceBaseFromName(r?.nombre || '') === baseQuery);
    if (byBase.length === 1) return byBase[0];
  }

  return null;
}

async function applyCatalogServiceDataToTurno(turno) {
  const base = turno ? { ...turno } : {};
  const servicioTxt = String(base.servicio || base.last_service_name || '').trim();
  if (!servicioTxt) return base;

  try {
    const services = await getServicesCatalog();
    const match = resolveServiceCatalogMatch(services, servicioTxt);
    const durationMin = parseServiceDurationToMinutes(match?.duracion || '');

    if (durationMin > 0) {
      base.duracion_min = durationMin;
    }

    if (match?.nombre && !base.last_service_name) {
      base.last_service_name = match.nombre;
    }

    return base;
  } catch {
    return base;
  }
}

async function getCoursesCatalog() {
  const now = Date.now();
  if (catalogCache.courses.rows.length && (now - catalogCache.courses.loadedAt) < CATALOG_CACHE_TTL_MS) {
    return catalogCache.courses.rows;
  }

  const grid = await readSheetGridWithLinks(COURSES_SHEET_ID, COURSES_RANGE);
  const header = Array.isArray(grid?.headers) ? grid.headers : [];
  const data = Array.isArray(grid?.rows) ? grid.rows : [];
  if (!header.length) return [];

  const idx = {
    nombre: header.findIndex(h => normalize(h) === "nombre"),
    categoria: header.findIndex(h => normalize(h) === "categoria"),
    modalidad: header.findIndex(h => normalize(h).includes("modalidad")),
    duracionTotal: header.findIndex(h => normalize(h).includes("duracion total")),
    inicio: header.findIndex(h => normalize(h).includes("fecha de inicio")),
    fin: header.findIndex(h => normalize(h).includes("fecha de finalizacion")),
    diasHorarios: header.findIndex(h => normalize(h).includes("dias y horarios")),
    precio: header.findIndex(h => normalize(h) === "precio"),
    sena: header.findIndex(h => {
      const v = normalize(h);
      return v.includes("sena") || v.includes("seña") || v.includes("inscripcion") || v.includes("inscripción");
    }),
    cupos: header.findIndex(h => normalize(h).includes("cupos")),
    requisitos: header.findIndex(h => normalize(h).includes("requisitos")),
    info: header.findIndex(h => {
      const v = normalize(h);
      return v.includes("informacion detallada") || v.includes("descripcion") || v.includes("descripción") || v === "info";
    }),
    estado: header.findIndex(h => normalize(h) === "estado"),
    link: header.findIndex(h => {
      const v = normalize(h);
      return v === "link" || v.includes("link") || v.includes("foto") || v.includes("imagen");
    }),
  };

  const rows = data.map((row) => {
    const values = Array.isArray(row?.values) ? row.values : [];
    const links = Array.isArray(row?.links) ? row.links : [];
    const linkValue = idx.link >= 0 ? ((links[idx.link] || values[idx.link] || "").trim()) : "";

    return {
      nombre: (values[idx.nombre] || "").trim(),
      categoria: (values[idx.categoria] || "").trim(),
      modalidad: (values[idx.modalidad] || "").trim(),
      duracionTotal: (values[idx.duracionTotal] || "").trim(),
      fechaInicio: (values[idx.inicio] || "").trim(),
      fechaFin: (values[idx.fin] || "").trim(),
      diasHorarios: (values[idx.diasHorarios] || "").trim(),
      precio: (values[idx.precio] || "").trim(),
      sena: (values[idx.sena] || "").trim(),
      cupos: (values[idx.cupos] || "").trim(),
      requisitos: (values[idx.requisitos] || "").trim(),
      info: (values[idx.info] || "").trim(),
      estado: (values[idx.estado] || "").trim(),
      link: linkValue,
    };
  }).filter(x => x.nombre);

  catalogCache.courses = { loadedAt: now, rows };
  return rows;
}

// ===================== BUSCADORES =====================
const PRODUCT_FAMILY_DEFS = [
  { id: 'shampoo', label: 'shampoo', aliases: ['shampoo', 'champu', 'champú'] },
  { id: 'acondicionador', label: 'acondicionador', aliases: ['acondicionador'] },
  { id: 'bano_de_crema', label: 'baño de crema', aliases: ['baño de crema', 'bano de crema', 'mascara capilar', 'mascarilla capilar'] },
  { id: 'tratamiento', label: 'tratamiento', aliases: ['tratamiento', 'nutricion', 'nutrición', 'hidratacion', 'hidratación', 'reparador', 'reparacion', 'reparación', 'ampolla'] },
  { id: 'serum', label: 'serum', aliases: ['serum', 'sérum'] },
  { id: 'aceite', label: 'aceite', aliases: ['aceite', 'oleo', 'óleo'] },
  { id: 'tintura', label: 'tintura', aliases: ['tintura', 'coloracion', 'coloración', 'color'] },
  { id: 'oxidante', label: 'oxidante', aliases: ['oxidante', 'revelador'] },
  { id: 'decolorante', label: 'decolorante', aliases: ['decolorante', 'aclarante'] },
  { id: 'matizador', label: 'matizador', aliases: ['matizador', 'violeta', 'plata'] },
  { id: 'keratina', label: 'keratina', aliases: ['keratina', 'keratin'] },
  { id: 'protector', label: 'protector', aliases: ['protector', 'protector termico', 'protector térmico'] },
  { id: 'spray', label: 'spray', aliases: ['spray'] },
  { id: 'gel', label: 'gel', aliases: ['gel'] },
  { id: 'cera', label: 'cera', aliases: ['cera'] },
  { id: 'botox', label: 'botox capilar', aliases: ['botox', 'botox capilar'] },
  { id: 'alisado', label: 'alisado', aliases: ['alisado', 'alisante', 'nanoplastia', 'cirugia capilar', 'cirugía capilar'] },
  { id: 'secador', label: 'secador', aliases: ['secador', 'secadora'] },
  { id: 'plancha', label: 'plancha', aliases: ['plancha', 'planchita'] },
];

const PRODUCT_TYPE_KEYWORDS = PRODUCT_FAMILY_DEFS.flatMap((x) => x.aliases);
const PRODUCT_LIST_HINTS_RE = /(catalogo|catálogo|lista|todo|toda|todos|todas|opciones|mostrame|mandame|enviame|pasame|que tenes|qué tenes|que tienen|qué tienen|que hay|qué hay|stock|venden|venta)/i;

function normalizeCatalogSearchText(str) {
  return canonicalizeQuery(str || '')
    .replace(/[^\p{L}\p{N}\s]/gu, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function containsCatalogPhrase(text, phrase) {
  const t = ` ${normalizeCatalogSearchText(text)} `;
  const p = ` ${normalizeCatalogSearchText(phrase)} `;
  return !!p.trim() && t.includes(p);
}

function getProductFamilyDef(family) {
  const f = normalizeCatalogSearchText(family);
  if (!f) return null;
  return PRODUCT_FAMILY_DEFS.find((item) =>
    normalizeCatalogSearchText(item.id) === f ||
    normalizeCatalogSearchText(item.label) === f ||
    item.aliases.some((alias) => normalizeCatalogSearchText(alias) === f)
  ) || null;
}

function getProductFamilyLabel(family) {
  const def = getProductFamilyDef(family);
  if (def?.label) return def.label;
  return normalizeCatalogSearchText(family || '');
}

function detectProductFamily(query) {
  const q = normalizeCatalogSearchText(query);
  if (!q) return '';

  let best = null;
  for (const def of PRODUCT_FAMILY_DEFS) {
    let score = 0;
    for (const alias of def.aliases) {
      if (containsCatalogPhrase(q, alias)) score += normalizeCatalogSearchText(alias).split(' ').length >= 2 ? 4 : 2;
    }
    if (!score) continue;
    if (!best || score > best.score) best = { id: def.id, score };
  }
  return best?.id || '';
}

function getProductFamilyAliases(family) {
  const def = getProductFamilyDef(family) || getProductFamilyDef(detectProductFamily(family));
  return def?.aliases || [];
}

function detectProductFocusTerm(query) {
  const q = normalizeCatalogSearchText(query || '');
  if (!q) return '';

  let best = '';
  for (const def of PRODUCT_FAMILY_DEFS) {
    for (const alias of (def.aliases || [])) {
      const normalizedAlias = normalizeCatalogSearchText(alias);
      if (!normalizedAlias) continue;
      if (containsCatalogPhrase(q, normalizedAlias) && normalizedAlias.length > best.length) {
        best = normalizedAlias;
      }
    }
  }

  return best;
}

function buildProductFocusHaystack(row) {
  return normalizeCatalogSearchText(`${row?.nombre || ''} ${row?.categoria || ''} ${row?.marca || ''} ${row?.tab || ''}`);
}

function filterRowsByProductFocus(rows, focusTerm) {
  const list = Array.isArray(rows) ? rows : [];
  const focus = normalizeCatalogSearchText(focusTerm || '');
  if (!focus || !list.length) return list;

  const focused = list.filter((row) => containsCatalogPhrase(buildProductFocusHaystack(row), focus));
  return focused.length ? focused : list;
}

function isGenericProductOptionsFollowUp(text) {
  const t = normalize(text || '');
  if (!t) return false;
  return /(que otras opciones|qué otras opciones|otras opciones|que mas tenes|qué más tenés|que más tenes|qué mas tenés|que mas hay|qué más hay|alguna otra|alguna mas|alguna más|otras que tengas|otras tenes|otras tenés|de ese|de esa|de eso|de estas|de estos|de esa linea|de esa línea)/i.test(t);
}

function extractProductTypeKeywords(query) {
  const family = detectProductFamily(query);
  if (!family) return [];
  const aliases = getProductFamilyAliases(family);
  return aliases.length ? aliases : [family];
}

function isGenericProductQuery(query) {
  const q = normalizeCatalogSearchText(query || '');
  if (!q) return false;
  const family = detectProductFamily(q);
  const tokens = tokenize(q, { expandSynonyms: false });
  return !!family && (PRODUCT_LIST_HINTS_RE.test(q) || tokens.length <= 4);
}

function applyProductTypeGuard(rows, query) {
  const family = detectProductFamily(query);
  const aliases = getProductFamilyAliases(family);
  if (!aliases.length) return rows;

  const guarded = rows.filter((r) => {
    const haystack = `${r.nombre} ${r.categoria} ${r.marca} ${r.descripcion} ${r.tab}`;
    return aliases.some((alias) => containsCatalogPhrase(haystack, alias));
  });

  return guarded.length ? guarded : rows;
}

function buildStockHaystack(row) {
  return normalizeCatalogSearchText(`${row?.nombre || ''} ${row?.categoria || ''} ${row?.marca || ''} ${row?.descripcion || ''} ${row?.tab || ''}`);
}

function findStock(rows, query, mode) {
  const q = canonicalizeQuery(query);
  if (!q) return [];

  const guardedRows = applyProductTypeGuard(rows, q);

  const exact = guardedRows.filter(r => canonicalizeQuery(r.nombre) === q);
  if (exact.length) return exact;

  const containsStrong = guardedRows.filter(r => canonicalizeQuery(r.nombre).includes(q) && q.length >= 4);
  if (mode !== 'LIST' && containsStrong.length === 1) return containsStrong;

  const hasTypeGuard = extractProductTypeKeywords(q).length > 0;
  const scored = [];
  for (const r of guardedRows) {
    const sNombre = scoreField(q, r.nombre);
    const sCat = scoreField(q, r.categoria) * 0.82;
    const sMarca = scoreField(q, r.marca) * 0.66;
    const sDesc = hasTypeGuard ? scoreField(q, r.descripcion) * 0.26 : scoreField(q, r.descripcion) * 0.58;
    const sTab = scoreField(q, r.tab) * 0.62;
    const score = Math.max(sNombre, sCat, sMarca, sDesc, sTab);

    const qTok = tokenize(q, { expandSynonyms: true });
    if (qTok.length >= 2) {
      const bag = tokenize(`${r.nombre} ${r.categoria} ${r.marca} ${r.descripcion}`);
      const jac = jaccard(qTok, bag);
      if (jac >= 0.5) {
        scored.push({ row: r, score: Math.min(1, score + 0.07), q });
        continue;
      }
    }

    if (score > 0) scored.push({ row: r, score, q });
  }

  return pickBestByScore(scored, mode);
}

function findStockRelated(rows, rawQuery, { family = '', focusTerm = '', limit = 200 } = {}) {
  const q = normalizeCatalogSearchText(rawQuery || '');
  const resolvedFamily = family || detectProductFamily(q);
  const resolvedFocusTerm = normalizeCatalogSearchText(focusTerm || detectProductFocusTerm(q) || '');
  const aliases = getProductFamilyAliases(resolvedFamily);
  const familyTokenSet = new Set(aliases.flatMap((alias) => tokenize(alias, { expandSynonyms: false })));
  const extraTokens = tokenize(q, { expandSynonyms: true }).filter((tok) =>
    tok &&
    tok.length >= 3 &&
    !familyTokenSet.has(tok) &&
    !STOCK_STOPWORDS.has(tok) &&
    !['producto', 'productos', 'stock', 'precio', 'foto', 'fotos', 'imagen', 'imagenes', 'venta', 'comprar'].includes(tok)
  );

  const scored = [];

  for (const row of rows) {
    const hay = buildStockHaystack(row);
    const bag = tokenize(hay, { expandSynonyms: false });
    const bagSet = new Set(bag);

    let familyHits = 0;
    if (aliases.length) {
      for (const alias of aliases) {
        if (containsCatalogPhrase(hay, alias)) familyHits += 1;
      }
    }

    let extraHits = 0;
    for (const tok of extraTokens) {
      if (bagSet.has(tok) || containsCatalogPhrase(hay, tok)) extraHits += 1;
    }

    const queryScore = q ? Math.max(
      scoreField(q, row.nombre) * 1.15,
      scoreField(q, row.categoria) * 0.95,
      scoreField(q, row.marca) * 0.72,
      scoreField(q, row.descripcion) * 0.62,
      scoreField(q, row.tab) * 0.55
    ) : 0;

    const score = queryScore + (familyHits * 4.5) + (extraHits * 1.85);

    if (aliases.length && !familyHits && score < 1.2) continue;
    if (!aliases.length && score < 0.55) continue;

    scored.push({ row, score, familyHits, extraHits });
  }

  scored.sort((a, b) => {
    if (b.familyHits !== a.familyHits) return b.familyHits - a.familyHits;
    if (b.extraHits !== a.extraHits) return b.extraHits - a.extraHits;
    return b.score - a.score;
  });

  let out = scored.map((x) => x.row);

  if (resolvedFocusTerm) {
    out = filterRowsByProductFocus(out, resolvedFocusTerm);
  }

  if (aliases.length && extraTokens.length) {
    const narrowed = scored.filter((x) => x.familyHits > 0 && x.extraHits > 0).map((x) => x.row);
    if (narrowed.length) out = narrowed;
    else {
      const familyOnly = scored.filter((x) => x.familyHits > 0).map((x) => x.row);
      if (familyOnly.length) out = familyOnly;
    }
  } else if (aliases.length) {
    const familyOnly = scored.filter((x) => x.familyHits > 0).map((x) => x.row);
    if (familyOnly.length) out = familyOnly;
  }

  const dedup = [];
  const seen = new Set();
  for (const row of out) {
    const key = normalizeCatalogSearchText(`${row?.nombre || ''} ${row?.marca || ''}`);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    dedup.push(row);
    if (dedup.length >= limit) break;
  }

  return dedup;
}

async function extractProductIntentWithAI(text, context = {}) {
  const raw = String(text || '').trim();
  if (!raw) return null;

  try {
    const completion = await openai.chat.completions.create({
      model: PRIMARY_MODEL,
      messages: [
        {
          role: 'system',
          content:
`Analizá si el cliente está consultando PRODUCTOS del catálogo (stock, precios, opciones, fotos o recomendación).
Devolvé SOLO JSON con estas claves:
- is_product_query: boolean
- family: string (una de estas o vacío: shampoo, acondicionador, baño de crema, tratamiento, serum, aceite, tintura, oxidante, decolorante, matizador, keratina, protector, spray, gel, cera, botox, alisado, secador, plancha, otro)
- search_text: string
- specific_name: string
- wants_all_related: boolean
- wants_photo: boolean
- wants_price: boolean
- wants_recommendation: boolean
- hair_type: string
- need: string
- use_type: string (personal, profesional o vacío)

Reglas:
- Si pregunta genéricamente por una familia de productos, wants_all_related=true.
- Si pide stock, precios, lista, catálogo, opciones o qué hay de una familia, is_product_query=true.
- Si pide foto o precio de un producto puntual, specific_name debe contener ese producto.
- Si el mensaje trae un tipo de cabello, una necesidad o dice “cuál me conviene”, wants_recommendation=true.
- Si existe familia_actual y el cliente responde solo con datos como “tengo el pelo seco”, “es para trabajar”, “quiero algo anti frizz”, seguí la continuidad y marcá is_product_query=true.
- Si parece servicio/turno y no producto, is_product_query=false.
- No inventes nombres de productos.`,
        },
        {
          role: 'user',
          content: JSON.stringify({
            mensaje: raw,
            ultimo_producto: context.lastProductName || '',
            ultimo_servicio: context.lastServiceName || '',
            familia_actual: context.lastFamily || '',
            cabello_actual: context.lastHairType || '',
            necesidad_actual: context.lastNeed || '',
            uso_actual: context.lastUseType || '',
          }),
        },
      ],
      response_format: { type: 'json_object' },
    });

    const obj = JSON.parse(completion.choices?.[0]?.message?.content || '{}');
    return {
      is_product_query: !!obj.is_product_query,
      family: String(obj.family || '').trim(),
      search_text: String(obj.search_text || '').trim(),
      specific_name: String(obj.specific_name || '').trim(),
      wants_all_related: !!obj.wants_all_related,
      wants_photo: !!obj.wants_photo,
      wants_price: !!obj.wants_price,
      wants_recommendation: !!obj.wants_recommendation,
      hair_type: String(obj.hair_type || '').trim(),
      need: String(obj.need || '').trim(),
      use_type: String(obj.use_type || '').trim(),
    };
  } catch {
    return null;
  }
}

function compactProductDescription(desc, maxLen = 260) {
  const clean = String(desc || '').replace(/\s+/g, ' ').trim();
  if (!clean) return '';
  if (clean.length <= maxLen) return clean;
  return `${clean.slice(0, maxLen - 1).trim()}…`;
}

function normalizeUseType(value) {
  const t = normalize(value || '');
  if (!t) return '';
  if (/profes|trabaj/.test(t)) return 'profesional';
  if (/personal|casa|hogar/.test(t)) return 'personal';
  return '';
}

function deriveProductTags(row) {
  const bag = normalizeCatalogSearchText(`${row?.nombre || ''} ${row?.categoria || ''} ${row?.marca || ''} ${row?.descripcion || ''}`);
  const tags = [];
  const checks = [
    ['rubio', /(rubio|platin|decolorad|canas|canoso|gris)/],
    ['anti frizz', /(frizz|anti frizz|encresp)/],
    ['hidratación', /(hidrata|hidratacion|hidratación|nutric)/],
    ['reparación', /(repar|dañado|danado|quebrad|elasticidad)/],
    ['rulos', /(rulos|ondas|riz)/],
    ['alisado', /(alisad|liso|keratina|plastificado|laminado|botox)/],
    ['color', /(color|tintura|oxidante|decolorante|matizador|reflejos|canas)/],
    ['barbería', /(barba|afeitad|barber|shaving|after shave)/],
    ['profesional', /(uso profesional|profesional|salon|salón)/],
    ['personal', /(uso personal|personal|hogar|casa)/],
  ];
  for (const [label, rx] of checks) {
    if (rx.test(bag)) tags.push(label);
  }
  return tags;
}

function buildProductAICandidate(row) {
  return {
    nombre: row.nombre,
    categoria: row.categoria || '',
    marca: row.marca || '',
    precio: row.precio || '',
    descripcion: compactProductDescription(row.descripcion || ''),
    tags: deriveProductTags(row),
  };
}

function scoreProductCandidate(row, { query = '', family = '', hairType = '', need = '', useType = '' } = {}) {
  const hay = buildStockHaystack(row);
  let score = 0;

  if (query) {
    score += Math.max(
      scoreField(query, row.nombre) * 1.2,
      scoreField(query, row.categoria) * 0.92,
      scoreField(query, row.descripcion) * 0.78,
      scoreField(query, row.marca) * 0.55
    );
  }

  const aliases = getProductFamilyAliases(family);
  for (const alias of aliases) {
    if (containsCatalogPhrase(hay, alias)) score += 3.2;
  }

  const hair = normalizeCatalogSearchText(hairType);
  if (hair) {
    const hairTokens = tokenize(hair, { expandSynonyms: true });
    for (const tok of hairTokens) {
      if (tok.length >= 3 && hay.includes(tok)) score += 1.15;
    }
  }

  const needNorm = normalizeCatalogSearchText(need);
  if (needNorm) {
    const needTokens = tokenize(needNorm, { expandSynonyms: true });
    for (const tok of needTokens) {
      if (tok.length >= 3 && hay.includes(tok)) score += 1.35;
    }
  }

  const useNorm = normalizeUseType(useType);
  if (useNorm === 'profesional' && /(profesional|salon|salón|barber)/i.test(hay)) score += 1.1;
  if (useNorm === 'personal' && /(personal|hogar|casa)/i.test(hay)) score += 0.9;

  return score;
}

function shortlistProductsForRecommendation(rows, criteria = {}) {
  const items = Array.isArray(rows) ? rows.filter((r) => r?.nombre) : [];
  if (!items.length) return [];

  const scopedItems = criteria?.focusTerm ? filterRowsByProductFocus(items, criteria.focusTerm) : items;

  const scored = scopedItems
    .map((row) => ({ row, score: scoreProductCandidate(row, criteria) }))
    .filter((x) => x.score > 0.15 || !criteria.family);

  scored.sort((a, b) => b.score - a.score);

  const out = [];
  const seen = new Set();
  for (const item of scored) {
    const key = normalizeCatalogSearchText(`${item.row?.nombre || ''} ${item.row?.marca || ''}`);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push(item.row);
    if (out.length >= (criteria.limit || 10)) break;
  }

  return out.length ? out : scopedItems.slice(0, criteria.limit || 10);
}

async function recommendProductsWithAI({ text, familyLabel = '', hairType = '', need = '', useType = '', products = [] } = {}) {
  const candidates = Array.isArray(products) ? products.filter((p) => p?.nombre).slice(0, 10).map(buildProductAICandidate) : [];
  if (!candidates.length) return null;

  try {
    const completion = await openai.chat.completions.create({
      model: PRIMARY_MODEL,
      messages: [
        {
          role: 'system',
          content:
`Sos un asistente de peluquería que recomienda productos SOLO entre las opciones enviadas.
Tu trabajo:
- Elegir hasta 4 productos que mejor encajen.
- Priorizar coincidencia con tipo de cabello, necesidad y uso personal/profesional.
- No inventar productos ni cambiar nombres.
- Si la consulta es amplia y faltan datos, igual podés proponer 2 a 4 opciones razonables y una pregunta de seguimiento.

Respondé SOLO JSON con:
- intro: string
- recommended_names: string[] (hasta 4 nombres exactos)
- follow_up: string
- rationale: string (breve, 1 oración)`,
        },
        {
          role: 'user',
          content: JSON.stringify({
            mensaje_cliente: text || '',
            familia: familyLabel || '',
            tipo_cabello: hairType || '',
            necesidad: need || '',
            uso: useType || '',
            opciones: candidates,
          }),
        },
      ],
      response_format: { type: 'json_object' },
    });

    const obj = JSON.parse(completion.choices?.[0]?.message?.content || '{}');
    return {
      intro: String(obj.intro || '').trim(),
      recommended_names: Array.isArray(obj.recommended_names) ? obj.recommended_names.map((x) => String(x || '').trim()).filter(Boolean) : [],
      follow_up: String(obj.follow_up || '').trim(),
      rationale: String(obj.rationale || '').trim(),
    };
  } catch {
    return null;
  }
}

function formatRecommendedProductsReply(aiPayload, rows, { familyLabel = '', hairType = '', need = '', useType = '' } = {}) {
  const items = Array.isArray(rows) ? rows.filter((r) => r?.nombre).slice(0, 4) : [];
  if (!items.length) return null;

  const readableFamily = familyLabel ? getProductFamilyLabel(familyLabel) : '';
  const intro = aiPayload?.intro
    ? aiPayload.intro
    : readableFamily
      ? `Según lo que me dice, estas opciones de *${readableFamily}* le pueden servir:`
      : `Según lo que me dice, estas opciones le pueden servir:`;

  const rationale = aiPayload?.rationale ? `\n${aiPayload.rationale}` : '';
  const lines = items.map((p) => {
    const precio = moneyOrConsult(p.precio);
    const desc = compactProductDescription(p.descripcion || '', 170);
    return `${getCatalogItemEmoji(p.nombre, { kind: 'product' })} *${p.nombre}*\n• Precio: *${precio}*${desc ? `\n• ${desc}` : ''}`;
  });

  const followUp = aiPayload?.follow_up
    || `Cuénteme un poquito más: ¿es para uso personal o para trabajar? ¿Qué tipo de cabello tiene y qué busca lograr?`;

  return `${intro}${rationale}\n\n${lines.join("\n\n— — —\n\n")}\n\n${followUp}`.trim();
}

function detectFemaleContext(text) {
  const t = normalize(text || '');
  return /(\bella\b|\bmi hija\b|\botra hija\b|\bhija\b|\bmi tia\b|\btia\b|\bmi señora\b|\bmi senora\b|\bseñora\b|\bsenora\b|\bmujer\b|\bfemenin[oa]\b|\bdama\b)/i.test(t);
}

function detectMaleContext(text) {
  const t = normalize(text || '');
  return /(\bmi hijo\b|\botro hijo\b|\bhijo\b|\bmi marido\b|\bmi esposo\b|\bvaron\b|\bhombre\b|\bmasculin[oa]\b|\bbarber\b)/i.test(t);
}

function applyServiceGenderContext(rows, query) {
  const q = normalize(query || '');
  if (!q || !/\bcorte\b/i.test(q)) return rows;

  const female = detectFemaleContext(query);
  const male = detectMaleContext(query) && !female;
  if (!female && !male) return rows;

  const filtered = rows.filter((r) => {
    const hay = normalize(`${r.nombre} ${r.categoria} ${r.subcategoria}`);
    if (female) return !/(mascul|varon|hombre|barber)/i.test(hay);
    if (male) return /(mascul|varon|hombre|barber)/i.test(hay);
    return true;
  });

  return filtered.length ? filtered : rows;
}


function getCatalogItemEmoji(label, { kind = 'product' } = {}) {
  const t = normalize(label || '');
  if (!t) return kind === 'service' ? '💇‍♀️' : '✨';
  if (/(tijera|corte)/i.test(t)) return '✂️';
  if (/(navaj|afeitad|barba|shaving|after shave|perfilad)/i.test(t)) return '🪒';
  if (/(shampoo|acondicionador|mascara|mascarilla|baño de crema|bano de crema|crema|serum|sérum|aceite|oleo|óleo|ampolla|tratamiento|keratina|botox|alisado|protector|gel|cera|matizador|nutricion|nutrición)/i.test(t)) return '🧴';
  if (/(tintura|color|mechit|balayage|reflejo|decolor|emulsion|emulsión|oxidante)/i.test(t)) return '🌸';
  if (/(plancha|secador|brushing)/i.test(t)) return '🔥';
  if (/(curso|capacitacion|capacitación)/i.test(t)) return '🎓';
  return kind === 'service' ? '💇‍♀️' : '✨';
}

function cleanServiceName(name) {
  const raw = String(name || '').replace(/\s+/g, ' ').trim();
  if (!raw) return '';
  let clean = raw.replace(/^[^\p{L}\p{N}]+/u, '').trim();
  const colonIdx = clean.indexOf(':');
  if (colonIdx > 0) {
    const first = clean.slice(0, colonIdx).trim();
    const rest = clean.slice(colonIdx + 1).trim();
    if (rest && /(orden de llegada|no se toma turno|horario|horarios|duracion|duración|hora|hs\b|solo|sólo)/i.test(rest)) {
      clean = first;
    }
  }
  return clean;
}

function cleanServicePriceText(value) {
  const raw = String(value || '').replace(/\s+/g, ' ').trim();
  if (!raw) return 'consultar';
  const fromMatch = raw.match(/desde\s*\$\s*[\d\.]+/i);
  if (fromMatch) return fromMatch[0].replace(/\s+/g, ' ').replace(/desde/i, 'Desde');
  const priceMatch = raw.match(/\$\s*[\d\.]+/);
  if (priceMatch) return priceMatch[0].replace(/\s+/g, '');
  return moneyOrConsult(raw);
}

function getServiceBaseFromName(name) {
  const clean = normalize(cleanServiceName(name || ''));
  if (!clean) return '';
  if (/\bcorte\b/i.test(clean)) return 'corte';
  return clean
    .replace(/\b(femenin[oa]|masculin[oa]|varon|varón|hombre|mujer|dama|caballero|barberia|barbería)\b/gi, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function resolveImplicitServiceFollowupQuery(text, lastServiceName = '') {
  const t = normalize(text || '');
  const last = normalize(lastServiceName || '');
  if (!t || !last) return '';

  const mentionsMale = detectMaleContext(text);
  const mentionsFemale = detectFemaleContext(text);
  const hasExplicitServiceTerm = /(corte|keratina|botox|alisado|nutricion|nutrición|mechit|balayage|color|emulsion|emulsión|reflejos|servicio)/i.test(t);
  const isShortFollowUp = t.length <= 48 || /^(y\b|para\b|el\b|la\b)/i.test(t);

  if ((mentionsMale || mentionsFemale) && !hasExplicitServiceTerm && isShortFollowUp) {
    const base = getServiceBaseFromName(last);
    if (base) return `${base} ${mentionsMale ? 'masculino' : 'femenino'}`.trim();
  }

  if (/^(y ese\?|y ese cuanto sale\?|y ese cuánto sale\?|y ese cuanto demora\?|y ese cuánto demora\?)$/i.test(t)) {
    return cleanServiceName(lastServiceName);
  }

  return '';
}

function recentConversationWasAboutServicePrice(history = []) {
  const recent = Array.isArray(history) ? history.slice(-6).map((m) => normalize(m?.content || '')).join(' | ') : '';
  return /(precio:|cuanto sale|cuánto sale|esta \$|está \$|precio final|desde \$)/i.test(recent);
}

function findServices(rows, query, mode) {
  const q = normalize(query);
  if (!q) return [];

  const scopedRows = applyServiceGenderContext(rows, query);

  const match = (x) => {
    const nombre = normalize(x.nombre);
    const categoria = normalize(x.categoria);
    const sub = normalize(x.subcategoria);
    return nombre.includes(q) || categoria.includes(q) || sub.includes(q);
  };

  if (mode === "LIST") return scopedRows.filter(match);

  const exact = scopedRows.filter(r => normalize(r.nombre) === q);
  if (exact.length) return exact;

  const contains = scopedRows.filter(r => normalize(r.nombre).includes(q));
  if (contains.length) {
    if (/\bcorte\b/i.test(q) && detectFemaleContext(query)) {
      const preferred = contains.filter(r => !/(mascul|varon|hombre|barber)/i.test(normalize(`${r.nombre} ${r.categoria} ${r.subcategoria}`)));
      if (preferred.length) return preferred;
    }
    return contains;
  }

  const matched = scopedRows.filter(match);
  if (/\bcorte\b/i.test(q) && detectFemaleContext(query)) {
    const preferred = matched.filter(r => !/(mascul|varon|hombre|barber)/i.test(normalize(`${r.nombre} ${r.categoria} ${r.subcategoria}`)));
    if (preferred.length) return preferred;
  }
  return matched;
}

function findCourses(rows, query, mode) {
  const q = normalize(query);
  if (!q) return [];

  const match = (x) => {
    const hay = normalize([
      x.nombre,
      x.categoria,
      x.modalidad,
      x.duracionTotal,
      x.fechaInicio,
      x.fechaFin,
      x.diasHorarios,
      x.info,
      x.estado,
    ].filter(Boolean).join(' | '));
    return hay.includes(q);
  };

  if (mode === "LIST") return rows.filter(match);

  const exact = rows.filter(r => normalize(r.nombre) === q);
  if (exact.length) return exact;

  const contains = rows.filter(r => normalize(r.nombre).includes(q));
  if (contains.length) return contains;

  const categoryContains = rows.filter(r => normalize(r.categoria).includes(q));
  if (categoryContains.length) return categoryContains;

  return rows.filter(match);
}

function isExplicitCourseKeyword(text) {
  const t = normalize(text || '');
  return /(\bcurso\b|\bcursos\b|\btaller\b|\btalleres\b|\bcapacitacion\b|\bcapacitaciones\b|\bcapacitación\b)/i.test(t);
}

function looksLikeCourseFollowUp(text) {
  const t = normalize(text || '');
  if (!t) return false;
  return /(algun|alguno|alguna|de barberia|de barbería|de maquillaje|de peinados|de recogidos|de estetica|de estética|de auxiliar|de colorimetria|de colorimetría|mas info|más info|quiero info|info|precio|cuanto sale|cuánto sale|cuanto cuesta|cuánto cuesta|cuando empieza|cuándo empieza|cuando arranca|cuándo arranca|inicio|duracion|duración|horario|dias|días|cupo|cupos|inscripcion|inscripción|requisitos|ese curso|de ese|de ese curso)/i.test(t);
}

function resolveImplicitCourseFollowupQuery(text, lastCourseContext = null) {
  const t = normalize(text || '');
  if (!t || !lastCourseContext) return '';

  if (/^(ese|ese curso|de ese|de ese curso|de ese nomas|de ese no mas|mas info|más info|info|precio|cuanto sale|cuánto sale|cuando empieza|cuándo empieza|duracion|duración|horario|dias|días|cupos?|inscripcion|inscripción|requisitos)$/.test(t)) {
    return lastCourseContext.selectedName || lastCourseContext.query || '';
  }

  if (looksLikeCourseFollowUp(text)) {
    return text;
  }

  return '';
}

function detectCourseIntentFromContext(text, { lastCourseContext = null } = {}) {
  const raw = String(text || '').trim();
  const t = normalize(raw);
  if (!t) return { isCourse: false, query: '', mode: 'DETAIL' };

  const explicit = isExplicitCourseKeyword(raw);
  const genericList = explicit && /(que|qué|cuales|cuáles|tenes|tenés|hay|ofrecen|ofreces|disponibles|mostrar|mostrame|mandame|pasame|lista|opciones|algun|algún|alguna)/i.test(t);

  if (explicit) {
    return {
      isCourse: true,
      query: genericList ? 'cursos' : raw,
      mode: genericList ? 'LIST' : 'DETAIL',
    };
  }

  if (lastCourseContext && looksLikeCourseFollowUp(raw) && !/(\bturno\b|\breserv\w*\b|\bagend\w*\b|\bcita\b)/i.test(t)) {
    return {
      isCourse: true,
      query: resolveImplicitCourseFollowupQuery(raw, lastCourseContext) || raw,
      mode: 'DETAIL',
    };
  }

  return { isCourse: false, query: '', mode: 'DETAIL' };
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
      `${getCatalogItemEmoji(p.nombre, { kind: 'product' })} *${p.nombre}*`,
      `• Precio: *${precio}*`,
    ].join("\n");
  });

  const header = mode === "LIST"
    ? `Encontré estas opciones:`
    : `Está en catálogo:`;

  const footer = mode === "LIST"
    ? `\n\nSi quiere, le ayudo a elegir la mejor opción 😊 ¿Lo necesita para uso personal o para trabajar? ¿Qué tipo de cabello tiene y qué objetivo busca (alisado, reparación, hidratación, color, rulos)?`
    : `\n\nSi quiere, le ayudo a elegir la mejor opción 😊 ¿Lo necesita para uso personal o para trabajar? ¿Qué tipo de cabello tiene y qué resultado busca?`;

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
        `${getCatalogItemEmoji(p.nombre, { kind: 'product' })} *${p.nombre}*`,
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

function formatStockRelatedListAll(rows, { familyLabel = '', chunkSize = 10 } = {}) {
  const items = Array.isArray(rows) ? rows.filter(r => r?.nombre) : [];
  if (!items.length) return [];

  const readableFamily = familyLabel ? getProductFamilyLabel(familyLabel) : '';
  const intro = readableFamily ? ` de *${readableFamily}*` : '';
  const chunks = [];

  for (let i = 0; i < items.length; i += chunkSize) {
    const part = items.slice(i, i + chunkSize);
    const blocks = part.map((p) => {
      const precio = moneyOrConsult(p.precio);
      return [
        `${getCatalogItemEmoji(p.nombre, { kind: 'product' })} *${p.nombre}*`,
        `• Precio: *${precio}*`,
      ].join("\n");
    });

    const header = i === 0
      ? `✨ Tenemos estas opciones${intro}:`
      : `✨ Más opciones${intro}:`;

    const footer = (i + chunkSize) >= items.length
      ? `\n\nSi quiere, le ayudo a elegir la mejor opción 😊 ¿Es para uso personal o para trabajar? ¿Qué tipo de cabello tiene y qué resultado busca?`
      : `\n\n(Sigo con más opciones…)`;

    chunks.push(`${header}\n\n${blocks.join("\n\n— — —\n\n")}${footer}`.trim());
  }

  return chunks;
}

function formatServicesReply(matches, mode, opts = {}) {
  if (!matches.length) return null;

  const options = {
    showDuration: false,
    showDescription: false,
    ...opts,
  };

  const limited = mode === "LIST" ? matches.slice(0, 10) : matches.slice(0, 1);

  if (mode !== 'LIST') {
    const s = limited[0];
    const cleanName = cleanServiceName(s.nombre);
    const priceTxt = cleanServicePriceText(s.precio);
    const emoji = getCatalogItemEmoji(cleanName || s.nombre, { kind: 'service' });
    const parts = [
      `${emoji} *${cleanName || s.nombre}*`,
      `Precio: *${priceTxt}*`,
    ];

    if (options.showDuration && s.duracion) {
      parts.push(`Duración: *${s.duracion}*`);
    }

    if (options.showDescription && s.descripcion) {
      parts.push(`Info: ${String(s.descripcion).trim()}`);
    }

    const footer = options.showDuration || options.showDescription
      ? `

Si quiere, también puedo ayudarle a sacar un turno 😊`
      : `

Si quiere, también le digo cuánto demora o le ayudo a sacar un turno 😊`;

    return `${parts.join("\n")}${footer}`.trim();
  }

  const lines = limited.map((s) => {
    const cleanName = cleanServiceName(s.nombre);
    const priceTxt = cleanServicePriceText(s.precio);
    const emoji = getCatalogItemEmoji(cleanName || s.nombre, { kind: 'service' });
    return `${emoji} *${cleanName || s.nombre}* — *${priceTxt}*`;
  });

  return `💇‍♀️ Estos son algunos servicios disponibles:

${lines.join("\n")}

Si quiere, también le digo cuánto demora cada uno 😊`.trim();
}

function textAsksForServicesList(text) {
  const t = normalize(text || "");
  return /(que servicios|qué servicios|otros servicios|lista de servicios|servicios tienen|todos los servicios|mostrar servicios|mandame servicios)/i.test(t);
}

function textAsksForServicePrice(text) {
  const t = normalize(text || "");
  return /(precio|cuanto sale|cuánto sale|cuanto cuesta|cuánto cuesta|valor)/i.test(t);
}

function textAsksForServiceDuration(text) {
  const t = normalize(text || "");
  return /(cuanto demora|cuánto demora|cuanto tarda|cuánto tarda|demora|demore|duracion|duración|cuantas horas|cuántas horas|tiempo del servicio|cuanto dura|cuánto dura)/i.test(t);
}

function formatServicesListAll(rows, chunkSize = 8) {
  const items = Array.isArray(rows) ? rows.filter(r => r?.nombre) : [];
  if (!items.length) return [];
  const chunks = [];

  for (let i = 0; i < items.length; i += chunkSize) {
    const part = items.slice(i, i + chunkSize);
    const lines = part.map((s) => {
      const cleanName = cleanServiceName(s.nombre);
      const priceTxt = cleanServicePriceText(s.precio);
      const emoji = getCatalogItemEmoji(cleanName || s.nombre, { kind: 'service' });
      return `${emoji} *${cleanName || s.nombre}* — *${priceTxt}*`;
    });

    const header = i === 0 ? "💇‍♀️ Servicios disponibles:" : "💇‍♀️ Más servicios:";
    const footer = (i + chunkSize) >= items.length
      ? `

Si quiere, también le digo cuánto demora cada uno 😊`
      : `

(Le sigo con más servicios…)`;

    chunks.push(`${header}

${lines.join("\n")}${footer}`.trim());
  }
  return chunks;
}

function resolveReliableTurnService({ services = [], text = '', pendingDraft = null, lastKnownService = null, aiService = '' } = {}) {
  const rows = Array.isArray(services) ? services : [];
  const currentDraftService = pendingDraft?.servicio || pendingDraft?.last_service_name || '';
  const currentKnownService = currentDraftService || lastKnownService?.nombre || '';

  const directMatches = text ? findServices(rows, text, 'DETAIL') : [];
  const directName = directMatches[0]?.nombre || '';
  if (directName) return directName;

  if (currentKnownService) return currentKnownService;

  const aiResolved = resolveServiceCatalogMatch(rows, aiService || '');
  if (!aiResolved?.nombre) return '';

  const cleanText = normalize(text || '');
  const cleanAi = normalize(cleanServiceName(aiResolved.nombre));
  const aiBase = getServiceBaseFromName(aiResolved.nombre);

  if (cleanAi && cleanText.includes(cleanAi)) return aiResolved.nombre;
  if (aiBase && cleanText.includes(aiBase)) return aiResolved.nombre;

  return '';
}

function findServiceByContext(rows, query, lastServiceName) {
  if (query) {
    const matches = findServices(rows, query, "DETAIL");
    if (matches.length) return matches;

    const implicit = resolveImplicitServiceFollowupQuery(query, lastServiceName);
    if (implicit) {
      const implicitMatches = findServices(rows, implicit, "DETAIL");
      if (implicitMatches.length) return implicitMatches;
    }
  }
  if (lastServiceName) {
    const matches = findServices(rows, lastServiceName, "DETAIL");
    if (matches.length) return matches;
  }
  return [];
}

function formatCoursesReply(matches, mode) {
  if (!matches.length) return null;

  const limited = mode === "LIST" ? matches.slice(0, 10) : matches.slice(0, 3);

  const blocks = limited.map(c => {
    const lines = [
      `🎓 *${c.nombre}*`,
      c.categoria ? `• Categoría: ${c.categoria}` : "",
      c.modalidad ? `• Modalidad: ${c.modalidad}` : "",
      c.duracionTotal ? `• Duración: ${c.duracionTotal}` : "",
      c.fechaInicio ? `• Inicio: ${c.fechaInicio}` : "",
      c.fechaFin ? `• Finalización: ${c.fechaFin}` : "",
      c.diasHorarios ? `• Días y horarios: ${c.diasHorarios}` : "",
      c.precio ? `• Precio: *${moneyOrConsult(c.precio)}*` : "",
      c.sena ? `• Seña / inscripción: ${c.sena}` : "",
      c.cupos ? `• Cupos: ${c.cupos}` : "",
      c.requisitos ? `• Requisitos: ${c.requisitos}` : "",
      c.estado ? `• Estado: ${c.estado}` : "",
      c.info ? `• Info: ${c.info}` : "",
    ].filter(Boolean);

    return lines.join("\n");
  });

  const header = mode === "LIST"
    ? "🎓 Estos son los cursos disponibles:"
    : "🎓 Este es el curso que encontré:";

  return `${header}\n\n${blocks.join("\n\n— — —\n\n")}\n\nSi quiere, también le paso la foto del curso 😊`.trim();
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
    role: row.direction === 'out' ? 'assistant' : 'user',
    content: String(row.text || '').trim(),
  })).filter((x) => x.content);
}

function mergeConversationForAI(dbMessages, localMessages) {
  const merged = [];
  const seen = new Set();
  for (const m of [...(dbMessages || []), ...(localMessages || [])]) {
    const key = `${m.role}::${String(m.content || '').trim()}`;
    if (!m?.content || seen.has(key)) continue;
    seen.add(key);
    merged.push({ role: m.role, content: String(m.content || '').trim() });
  }
  return merged.slice(-20);
}

function getLastKnownService(waId, draft) {
  const fromDraft = draft?.servicio || draft?.last_service_name || '';
  if (fromDraft) return { nombre: fromDraft, ts: Date.now() };
  const fromMap = lastServiceByUser.get(waId);
  if (fromMap && fromMap.nombre && (Date.now() - (fromMap.ts || 0)) < LAST_SERVICE_TTL_MS) return fromMap;
  return null;
}

async function getLastBookedAppointmentForUser({ waId, waPhone }) {
  const phoneNorm = normalizePhone(waPhone || "");
  const r = await db.query(
    `SELECT client_name, service_name, appointment_date, appointment_time, duration_min, created_at
       FROM appointments
      WHERE wa_id = $1 OR wa_phone = $2
      ORDER BY created_at DESC
      LIMIT 5`,
    [waId, phoneNorm]
  );

  const rows = Array.isArray(r?.rows) ? r.rows : [];
  if (!rows.length) return null;

  return rows.map((row) => ({
    client_name: row.client_name || "",
    service_name: row.service_name || "",
    fecha: row.appointment_date ? String(row.appointment_date).slice(0, 10) : "",
    hora: row.appointment_time ? String(row.appointment_time).slice(0, 5) : "",
    duracion_min: Number(row.duration_min || 60) || 60,
    created_at: row.created_at || null,
  }))[0] || null;
}

function resolveRelativeTurnoReference(text, { pendingDraft, lastBooked } = {}) {
  const raw = String(text || "").trim();
  const t = normalize(raw);
  if (!t) return null;

  const asksAfter = /(despues|después|luego|a continuacion|a continuación)/i.test(t);
  if (!asksAfter) return null;

  const refName = normalize(lastBooked?.client_name || "");
  const refFirstName = refName ? refName.split(" ")[0] : "";
  const mentionsReference = (
    /\b(ella|el|él|mi hija|su turno|ese turno|el turno anterior)\b/i.test(t) ||
    (refName && t.includes(refName)) ||
    (refFirstName && t.includes(refFirstName))
  );

  if (!mentionsReference) return null;

  const baseDate = toYMD(pendingDraft?.fecha || lastBooked?.fecha || "");
  const baseHour = normalizeHourHM(pendingDraft?.hora || lastBooked?.hora || "");
  const baseDuration = Number(lastBooked?.duracion_min || pendingDraft?.duracion_min || 60) || 60;

  if (!baseDate || !baseHour) return { fecha: baseDate || "", hora: "", resolved: false };

  const nextSlot = addMinutesToYMDHM({ ymd: baseDate, hm: baseHour }, baseDuration);
  return {
    fecha: baseDate,
    hora: normalizeHourHM(nextSlot?.hm || ""),
    resolved: !!(baseDate && nextSlot?.hm),
  };
}

function looksLikeAppointmentContextFollowUp(text, { pendingDraft, lastService } = {}) {
  if (!pendingDraft && !lastService) return false;
  if (isExplicitProductIntent(text)) return false;

  const t = normalize(text || '');
  return /(?:lunes|martes|miercoles|miércoles|jueves|viernes|sabado|sábado|domingo|hoy|mañana|pasado\s+mañana|proximo|próximo|el\s+dia|el\s+día|y\s+el|que\s+horarios|qué\s+horarios|que\s+disponibilidad|qué\s+disponibilidad|tenes\s+lugar|tenés\s+lugar|disponible|disponibilidad|a\s+la\s+mañana|por\s+la\s+mañana|a\s+la\s+tarde|por\s+la\s+tarde|\d{1,2}[:.]\d{2}|\d{1,2}\s*(?:hs|horas?)|\d{1,2}[\/-]\d{1,2}(?:[\/-]\d{2,4})?)/i.test(t);
}

function looksLikeAppointmentIntent(text, { pendingDraft, lastService } = {}) {
  const t = normalize(text || '');
  if (/(\bturno\b|\breserv\w*\b|\bagend\w*\b|\bcita\b)/i.test(t)) return true;
  if (pendingDraft && /(si|sí|dale|ok|oka|quiero|quiero seguir|continuar|confirmar|bien|perfecto)/i.test(t)) return true;
  if (lastService && /(quiero( ese| el)? turno|bien,? quiero el turno|dale|ok|me gustaria sacar turno|me gustaria un turno|reservame|agendame)/i.test(t)) return true;
  if (looksLikeAppointmentContextFollowUp(text, { pendingDraft, lastService })) return true;
  return false;
}

function isWarmAffirmativeReply(text) {
  const t = normalize(text || '');
  return /^(si|sí|sii+|dale|ok|oka|perfecto|bueno|de una|claro|quiero|quiero turno|quiero reservar|quiero sacar turno)$/i.test(t);
}

function extractTurnoPauseIntent(text) {
  const raw = String(text || '').trim();
  if (!raw) return { matched: false, remainder: '' };

  const patterns = [
    /\bdespu[eé]s(?: te| le)? confirmo\b/i,
    /\ben otro momento(?: te aviso)?\b/i,
    /\bdespu[eé]s(?: te| le)? aviso\b/i,
    /\bluego(?: te| le)? aviso\b/i,
    /\bm[aá]s tarde(?: te| le)? aviso\b/i,
    /\bte confirmo despu[eé]s\b/i,
    /\bpor ahora no\b/i,
    /\bno por ahora\b/i,
    /\bmejor otro momento\b/i,
    /\blo vemos despu[eé]s\b/i,
    /\bdespu[eé]s coordinamos\b/i,
    /\bdej[aá]lo para despu[eé]s\b/i,
    /\bdej[aá]moslo para despu[eé]s\b/i,
    /\bte cancelo\b/i,
    /\bcancel[aá]lo\b/i,
    /\bcancelar(?: el)? turno\b/i,
    /\bcancel[aá] el turno\b/i,
    /\bno lo reserves\b/i,
    /\bno me lo reserves\b/i,
    /\bfrenemos ac[aá]\b/i,
    /\bparamos ac[aá]\b/i,
  ];

  const matched = patterns.some((rx) => rx.test(raw));
  if (!matched) return { matched: false, remainder: raw };

  const stripRx = /(?:^|[\s,;:.\-])(?:despu[eé]s(?: te| le)? confirmo|en otro momento(?: te aviso)?|despu[eé]s(?: te| le)? aviso|luego(?: te| le)? aviso|m[aá]s tarde(?: te| le)? aviso|te confirmo despu[eé]s|por ahora no|no por ahora|mejor otro momento|lo vemos despu[eé]s|despu[eé]s coordinamos|dej[aá]lo para despu[eé]s|dej[aá]moslo para despu[eé]s|te cancelo|cancel[aá]lo|cancelar(?: el)? turno|cancel[aá] el turno|no lo reserves|no me lo reserves|frenemos ac[aá]|paramos ac[aá])(?=$|[\s,;:.!?\-])/gi;

  let remainder = raw.replace(stripRx, ' ')
    .replace(/^[\s,;:.!?\-]+|[\s,;:.!?\-]+$/g, '')
    .replace(/\s{2,}/g, ' ')
    .trim();

  const softOnly = normalize(remainder);
  if (/^(gracias|ok|oka|dale|perfecto|bueno|listo|joya|genial|barbaro|b[aá]rbaro)$/.test(softOnly)) {
    remainder = '';
  }

  return { matched: true, remainder };
}

function isExplicitProductIntent(text) {
  const t = normalize(text || '');
  return /(producto|stock|insumo|shampoo|acondicionador|mascara|mascarilla|serum|aceite|oleo|tintura|oxidante|decolorante|matizador|ampolla|protector|spray|crema|gel|cera|comprar|venden|tenes|tenés|hay disponible|te queda|les queda)/i.test(t);
}

function isExplicitServiceIntent(text) {
  const t = normalize(text || '');
  return /(turno|otro turno|servicio|reservar|agendar|cita|precio|cuanto sale|cuánto sale|cuanto cuesta|cuánto cuesta|valor|hac(en|en)|realizan|para mi|para mí|despues de|después de|corte femenino|femenino|femenina|mi hija|otra hija|mi tia|mi señora|ella)/i.test(t);
}

function extractAmbiguousBeautyTerm(text) {
  const t = normalize(text || '');
  const terms = ['alisado', 'botox', 'keratina', 'nutricion', 'tratamiento'];
  return terms.find(term => new RegExp(`\\b${escapeRegex(term)}\\b`, 'i').test(t)) || '';
}

function formatWarmAssistant(text) {
  return String(text || '').replace(/^Perfecto 😊/m, 'Perfecto 😊').trim();
}

function inferDraftFlowStep(base) {
  if (!base?.servicio) return 'awaiting_service';
  if (!base?.fecha) return 'awaiting_date';
  if (!base?.hora) return 'awaiting_time';
  if (!base?.cliente_full && !base?.telefono_contacto) return 'awaiting_contact';
  if (!base?.cliente_full) return 'awaiting_name';
  if (!base?.telefono_contacto) return 'awaiting_phone';
  if (base?.payment_status !== 'paid_verified') return 'awaiting_payment';
  return 'ready_to_book';
}

// ===================== INTENCIÓN =====================
async function classifyAndExtract(text, context = {}) {
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

Tené en cuenta el contexto previo:
- servicio_actual: si existe, mensajes como "quiero el turno", "dale", "quiero ese", "bien" suelen referirse a ese servicio.
- curso_actual y curso_contexto_activo: si venían hablando de cursos, mensajes como "alguno de barbería", "más info", "de ese", "cuándo empieza", "precio" deben clasificarse como COURSE, no como SERVICE.
- flujo_actual: si el cliente ya estaba hablando de reservar, priorizá continuidad y no lo mandes a catálogo de nuevo.
- Si el mensaje es solo 'si', 'dale', 'ok' o similar y venían hablando de un servicio, priorizá la continuidad de ese tema.
- Si una palabra puede ser producto o servicio y el cliente no lo aclaró, devolvé OTHER.
- Frases como “qué tenés de shampoo”, “precio de baños de crema”, “qué stock hay de tinturas” son PRODUCT con mode LIST.

Respondé SOLO JSON.`
      },
      {
        role: "user",
        content: JSON.stringify({
          mensaje: text,
          servicio_actual: context.lastServiceName || "",
          curso_actual: context.lastCourseName || "",
          curso_contexto_activo: !!context.hasCourseContext,
          cursos_recientes: context.courseOptions || [],
          flujo_actual: context.flowStep || "",
          tiene_borrador_turno: !!context.hasDraft,
          historial_reciente: context.historySnippet || "",
        })
      }
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
  await dbInsertMessage({
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


async function sendWhatsAppTemplate(to, templateName, bodyVars = [], meta = {}) {
  const recipient = normalizeWhatsAppRecipient(to);
  if (!recipient) throw new Error(`Número inválido para plantilla: ${to || '(vacío)'}`);
  if (!templateName) throw new Error('Falta templateName');

  const body = (Array.isArray(bodyVars) ? bodyVars : []).map((v) => String(v ?? '').trim());
  const dedupKey = `${recipient}::template::${templateName}::${body.join('|')}`;
  const now = Date.now();
  const prevTs = lastSentOutByPeer.get(dedupKey) || 0;
  if ((now - prevTs) < OUT_DEDUP_MS) return { deduped: true };
  lastSentOutByPeer.set(dedupKey, now);

  const components = body.length
    ? [{
        type: 'body',
        parameters: body.map((value) => ({ type: 'text', text: value || '-' })),
      }]
    : [];

  const payload = {
    messaging_product: 'whatsapp',
    to: recipient,
    type: 'template',
    template: {
      name: templateName,
      language: { code: WHATSAPP_TEMPLATE_LANGUAGE },
      ...(components.length ? { components } : {}),
    },
  };

  const resp = await axios.post(
    `https://graph.facebook.com/v19.0/${process.env.PHONE_NUMBER_ID}/messages`,
    payload,
    {
      headers: {
        Authorization: `Bearer ${process.env.WHATSAPP_TOKEN}`,
        'Content-Type': 'application/json',
      },
    }
  );

  const wa_msg_id = resp?.data?.messages?.[0]?.id || null;
  await dbInsertMessage({
    direction: 'out',
    wa_peer: recipient,
    name: null,
    text: `[TEMPLATE:${templateName}] ${body.join(' | ')}`,
    msg_type: 'template',
    wa_msg_id,
    raw: { ...(resp?.data || {}), template_payload: payload, meta },
  });

  return { ...(resp?.data || {}), wa_msg_id };
}

function formatAppointmentDateForTemplate(dateYMD) {
  const ymd = toYMD(dateYMD);
  return ymd ? ymdToDMY(ymd) : '';
}

function formatAppointmentTimeForTemplate(timeHM) {
  return normalizeHourHM(timeHM) || String(timeHM || '').trim();
}

function buildAppointmentData(row = {}) {
  return {
    id: row.id,
    wa_id: row.wa_id || '',
    wa_phone: normalizePhone(row.wa_phone || ''),
    contact_phone: normalizePhone(row.contact_phone || ''),
    client_name: String(row.client_name || '').trim(),
    service_name: String(row.service_name || '').trim(),
    appointment_date: toYMD(row.appointment_date || ''),
    appointment_time: formatAppointmentTimeForTemplate(row.appointment_time || ''),
    status: String(row.status || '').trim(),
    stylist_notified_at: row.stylist_notified_at || null,
    reminder_client_2h_at: row.reminder_client_2h_at || null,
    reminder_client_24h_at: row.reminder_client_24h_at || null,
    reminder_stylist_24h_at: row.reminder_stylist_24h_at || null,
    reminder_stylist_2h_at: row.reminder_stylist_2h_at || null,
  };
}

function buildAppointmentTemplateVarsForStylist(appt) {
  return [
    appt.client_name || 'Cliente',
    formatAppointmentDateForTemplate(appt.appointment_date),
    formatAppointmentTimeForTemplate(appt.appointment_time),
    appt.service_name || 'Servicio',
    normalizePhone(appt.contact_phone || appt.wa_phone || ''),
  ];
}

function buildAppointmentTemplateVarsForStylistReminder(appt) {
  return [
    appt.client_name || 'Cliente',
    appt.service_name || 'Servicio',
    formatAppointmentDateForTemplate(appt.appointment_date),
    formatAppointmentTimeForTemplate(appt.appointment_time),
    normalizePhone(appt.contact_phone || appt.wa_phone || ''),
  ];
}

function buildAppointmentTemplateVarsForClient(appt) {
  return [
    appt.client_name || 'Cliente',
    formatAppointmentDateForTemplate(appt.appointment_date),
    formatAppointmentTimeForTemplate(appt.appointment_time),
    appt.service_name || 'Servicio',
  ];
}

async function insertAppointmentNotificationLog({ appointmentId, notificationType, recipientPhone, templateName, waMessageId, payload }) {
  await db.query(
    `INSERT INTO appointment_notifications (appointment_id, notification_type, recipient_phone, template_name, wa_message_id, payload)
     VALUES ($1, $2, $3, $4, $5, $6)`,
    [
      appointmentId,
      notificationType,
      normalizePhone(recipientPhone || ''),
      templateName || null,
      waMessageId || null,
      payload || {},
    ]
  );
}

async function markAppointmentNotificationField(appointmentId, fieldName) {
  const allowed = new Set([
    'stylist_notified_at',
    'reminder_client_2h_at',
    'reminder_client_24h_at',
    'reminder_stylist_24h_at',
    'reminder_stylist_2h_at',
  ]);
  if (!allowed.has(fieldName)) throw new Error(`Campo de notificación no permitido: ${fieldName}`);
  await db.query(`UPDATE appointments SET ${fieldName} = NOW(), updated_at = NOW() WHERE id = $1`, [appointmentId]);
}

async function sendAppointmentTemplateAndLog({ appointmentId, recipientPhone, templateName, notificationType, vars, markField }) {
  const response = await sendWhatsAppTemplate(recipientPhone, templateName, vars, {
    appointment_id: appointmentId,
    notification_type: notificationType,
  });

  await insertAppointmentNotificationLog({
    appointmentId,
    notificationType,
    recipientPhone,
    templateName,
    waMessageId: response?.wa_msg_id || response?.messages?.[0]?.id || null,
    payload: { vars },
  });

  if (markField) await markAppointmentNotificationField(appointmentId, markField);
  return response;
}

async function sendNewAppointmentTemplateToStylist(appt) {
  const recipient = normalizeWhatsAppRecipient(STYLIST_NOTIFY_PHONE_RAW);
  if (!recipient || !appt?.id || !appt?.appointment_date || !appt?.appointment_time) return false;

  await sendAppointmentTemplateAndLog({
    appointmentId: appt.id,
    recipientPhone: recipient,
    templateName: TEMPLATE_NUEVO_TURNO_PELUQUERA,
    notificationType: 'stylist_new_booking',
    vars: buildAppointmentTemplateVarsForStylist(appt),
    markField: 'stylist_notified_at',
  });

  return true;
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
  await dbInsertMessage({
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
      {
        role: "system",
        content: "Analizá la imagen en español y devolvé una descripción útil para atención al cliente. Si hay texto visible, transcribilo de forma fiel. Si parece un comprobante, transferencia o recibo, priorizá extraer monto, titular, alias, estado y nombres visibles. No inventes datos que no se vean."
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
async function sendProductPhotoDirect(phone, product) {
  if (!product) return { ok: false, reason: 'no_product' };

  const driveFileId = extractDriveFileId(product.foto);
  if (!driveFileId) {
    return { ok: false, reason: 'missing_link' };
  }

  const tmpPath = path.join(getTmpDir(), `prod-${driveFileId}.jpg`);

  try {
    await downloadDriveFileToPath(driveFileId, tmpPath);
    const mediaId = await uploadMediaToWhatsApp(tmpPath, 'image/jpeg');

    try {
      const savedName = `out-${mediaId}.jpg`;
      fs.copyFileSync(tmpPath, path.join(MEDIA_DIR, savedName));
    } catch {}

    const caption = [
      product.nombre || 'Producto',
      product.precio ? `Precio: ${moneyOrConsult(product.precio)}` : '',
    ].filter(Boolean).join(' | ');

    await sendWhatsAppImageById(phone, mediaId, caption);
    return { ok: true };
  } catch (e) {
    console.error('❌ Error enviando foto:', e?.response?.data || e?.message || e);
    return { ok: false, reason: 'send_error', error: e };
  } finally {
    try { fs.unlinkSync(tmpPath); } catch {}
  }
}

function resolveProductsByNames(rows, names = []) {
  if (!Array.isArray(rows) || !rows.length || !Array.isArray(names) || !names.length) return [];
  const used = new Set();
  const out = [];

  for (const rawName of names) {
    const target = normalizeCatalogSearchText(rawName || '');
    if (!target) continue;

    const match = rows.find((row) => normalizeCatalogSearchText(row?.nombre || '') === target);
    if (!match) continue;

    const key = normalizeCatalogSearchText(`${match?.nombre || ''} ${match?.marca || ''}`);
    if (used.has(key)) continue;
    used.add(key);
    out.push(match);
  }

  return out;
}

async function maybeSendMultipleProductPhotos(phone, products, userText) {
  if (!Array.isArray(products) || !products.length) return false;
  if (!userAsksForPhoto(userText)) return false;

  const unique = [];
  const seen = new Set();
  for (const product of products) {
    const key = normalizeCatalogSearchText(`${product?.nombre || ''} ${product?.marca || ''}`);
    if (!product?.nombre || seen.has(key)) continue;
    seen.add(key);
    unique.push(product);
  }

  const limited = unique.slice(0, 8);
  if (!limited.length) return false;

  if (limited.length > 1) {
    await sendWhatsAppText(phone, 'Le paso las fotos de estas opciones 😊');
  }

  let sentCount = 0;
  const missing = [];
  const failed = [];

  for (const product of limited) {
    const result = await sendProductPhotoDirect(phone, product);
    if (result.ok) {
      sentCount += 1;
    } else if (result.reason === 'missing_link') {
      missing.push(product.nombre || 'Producto');
    } else {
      failed.push(product.nombre || 'Producto');
    }
  }

  if (missing.length) {
    await sendWhatsAppText(phone, `No tengo la foto vinculada correctamente de: ${missing.join(', ')}.`);
  }

  if (failed.length && !sentCount) {
    await sendWhatsAppText(phone, 'No pude enviar las fotos en este momento. Revise que las imágenes de Drive estén compartidas con el service account.');
  } else if (failed.length) {
    await sendWhatsAppText(phone, `No pude enviar algunas fotos ahora mismo: ${failed.join(', ')}.`);
  }

  return sentCount > 0 || missing.length > 0 || failed.length > 0;
}

async function maybeSendProductPhoto(phone, product, userText) {
  if (!product) return false;
  if (!userAsksForPhoto(userText)) return false;

  const result = await sendProductPhotoDirect(phone, product);
  if (result.ok) return true;

  if (result.reason === 'missing_link') {
    await sendWhatsAppText(
      phone,
      'No tengo la foto vinculada correctamente en la columna “Foto del producto”. Si quiere, le paso alternativas o la descripción.'
    );
    return true;
  }

  await sendWhatsAppText(
    phone,
    'No pude enviar la foto en este momento. Revise que la imagen de Drive esté compartida con el service account.'
  );
  return true;
}

function imageExtFromMime(mimeType) {
  const mime = String(mimeType || '').toLowerCase();
  if (mime.includes('png')) return '.png';
  if (mime.includes('webp')) return '.webp';
  if (mime.includes('jpeg') || mime.includes('jpg')) return '.jpg';
  return '.jpg';
}

function imageMimeFromPathish(value) {
  const v = String(value || '').toLowerCase();
  if (/\.png($|\?)/.test(v)) return 'image/png';
  if (/\.webp($|\?)/.test(v)) return 'image/webp';
  if (/\.jpe?g($|\?)/.test(v)) return 'image/jpeg';
  return 'image/jpeg';
}

async function downloadImageFromSharedLink(link, tmpPrefix = 'img') {
  const value = String(link || '').trim();
  if (!value) throw new Error('missing_link');

  const driveFileId = extractDriveFileId(value);
  if (driveFileId) {
    let mimeType = 'image/jpeg';
    try {
      const drive = await getDriveClient();
      const meta = await drive.files.get({ fileId: driveFileId, fields: 'mimeType,name' });
      mimeType = meta?.data?.mimeType || mimeType;
    } catch {}

    const ext = imageExtFromMime(mimeType);
    const tmpPath = path.join(getTmpDir(), `${tmpPrefix}-${driveFileId}${ext}`);
    await downloadDriveFileToPath(driveFileId, tmpPath);
    return { tmpPath, mimeType };
  }

  const resp = await axios.get(value, { responseType: 'arraybuffer' });
  const mimeType = String(resp?.headers?.['content-type'] || imageMimeFromPathish(value)).split(';')[0].trim() || 'image/jpeg';
  const ext = imageExtFromMime(mimeType);
  const tmpPath = path.join(getTmpDir(), `${tmpPrefix}-${Date.now()}${ext}`);
  fs.writeFileSync(tmpPath, Buffer.from(resp.data));
  return { tmpPath, mimeType };
}

async function sendCoursePhotoDirect(phone, course) {
  if (!course) return { ok: false, reason: 'no_course' };

  const imageLink = String(course.link || '').trim();
  if (!imageLink) {
    return { ok: false, reason: 'missing_link' };
  }

  let tmpPath = '';
  try {
    const downloaded = await downloadImageFromSharedLink(imageLink, 'course');
    tmpPath = downloaded.tmpPath;
    const mimeType = downloaded.mimeType || 'image/jpeg';
    const mediaId = await uploadMediaToWhatsApp(tmpPath, mimeType);

    try {
      const savedName = `out-${mediaId}${imageExtFromMime(mimeType)}`;
      fs.copyFileSync(tmpPath, path.join(MEDIA_DIR, savedName));
    } catch {}

    const caption = [
      course.nombre || 'Curso',
      course.precio ? `Precio: ${moneyOrConsult(course.precio)}` : '',
      course.fechaInicio ? `Inicio: ${course.fechaInicio}` : '',
    ].filter(Boolean).join(' | ');

    await sendWhatsAppImageById(phone, mediaId, caption);
    return { ok: true };
  } catch (e) {
    console.error('❌ Error enviando foto del curso:', e?.response?.data || e?.message || e);
    return { ok: false, reason: 'send_error', error: e };
  } finally {
    if (tmpPath) {
      try { fs.unlinkSync(tmpPath); } catch {}
    }
  }
}

async function maybeSendCoursePhotos(phone, courses) {
  const unique = Array.isArray(courses) ? courses.filter((c) => c?.nombre) : [];
  if (!unique.length) return false;

  const limited = unique.slice(0, 8);
  let sentCount = 0;
  const missing = [];
  const failed = [];

  if (limited.length > 1) {
    await sendWhatsAppText(phone, 'Le paso también las fotos de los cursos 😊');
  }

  for (const course of limited) {
    const result = await sendCoursePhotoDirect(phone, course);
    if (result.ok) {
      sentCount += 1;
    } else if (result.reason === 'missing_link') {
      missing.push(course.nombre || 'Curso');
    } else {
      failed.push(course.nombre || 'Curso');
    }
  }

  if (missing.length) {
    await sendWhatsAppText(phone, `No tengo la foto vinculada correctamente en la columna “Link” para: ${missing.join(', ')}.`);
  }

  if (failed.length && !sentCount) {
    await sendWhatsAppText(phone, 'No pude enviar las fotos de los cursos en este momento. Revise que el link de imagen esté accesible o compartido correctamente.');
  } else if (failed.length) {
    await sendWhatsAppText(phone, `No pude enviar algunas fotos de cursos ahora mismo: ${failed.join(', ')}.`);
  }

  return sentCount > 0 || missing.length > 0 || failed.length > 0;
}

app.get("/ping", (req, res) => {
  console.log("PING HIT");
  res.status(200).send("pong");
});

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
    hubspot: { enabled: hasHubSpotEnabled(), endOfDayTracking: ENABLE_END_OF_DAY_TRACKING },
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
    const phone = String(contact?.wa_id || phoneRaw || '').replace(/[^\d]/g, '');
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
    let text = (extracted.text || "").trim();
    let mediaMeta = extracted.media || null;
    let userIntentText = (
      msg.type === "image" ? (msg.image?.caption || "") :
      msg.type === "document" ? (msg.document?.caption || "") :
      text
    ).trim();


// ✅ Contexto para seguimiento al cierre (se actualiza durante la conversación)
let contactInfoFromText = extractContactInfo(text);
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
  suppressInactivityPrompt: false,
});

    // ✅ Guardar mensaje ENTRANTE (IN) para que el panel lo vea
    await dbInsertMessage({
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

    const inboundVersion = appendInboundMergeChunk(waId, {
      phone,
      phoneRaw,
      name,
      text,
      userIntentText,
      mediaMeta,
      msgType: msg.type,
      msgId: msg.id,
    });

    await sleep(INBOUND_MERGE_MS);

    const mergedInbound = consumeInboundMergeChunk(waId, inboundVersion);
    if (!mergedInbound) return;

    text = String(mergedInbound.text || '').trim();
    userIntentText = String(mergedInbound.userIntentText || userIntentText || text).trim();
    mediaMeta = mergedInbound.mediaMeta || mediaMeta || null;
    contactInfoFromText = extractContactInfo(text);

    updateLastCloseContext(waId, {
      explicitName: contactInfoFromText?.nombre || lastCloseContext.get(waId)?.explicitName || '',
      lastUserText: text,
      profileName: name || lastCloseContext.get(waId)?.profileName || '',
    });

    pushHistory(waId, "user", text);

    // ✅ Si el cliente frena o cancela la toma de turno en medio del flujo, cortamos ahí mismo.
    const pauseDraftEarly = await getAppointmentDraft(waId);
    if (pauseDraftEarly) {
      const pauseIntentEarly = extractTurnoPauseIntent(text);
      if (pauseIntentEarly.matched) {
        await deleteAppointmentDraft(waId);

        if (!pauseIntentEarly.remainder) {
          const msgPauseTurno = `Perfecto 😊

No hay problema. Frené la gestión del turno por ahora.

Cuando quiera retomarlo, me escribe y le paso nuevamente los horarios disponibles.`;
          pushHistory(waId, "assistant", msgPauseTurno);
          await sendWhatsAppText(phone, msgPauseTurno);
          scheduleInactivityFollowUp(waId, phone);
          return;
        }

        text = pauseIntentEarly.remainder;
      }
    }

// lead detection
const interest = await detectInterest(text);
if (interest) dailyLeads.set(phone, { name, interest });

// ✅ guardar interés y mejores datos en el contexto de cierre
updateLastCloseContext(waId, {
  interest: interest || (lastCloseContext.get(waId)?.interest || null),
  explicitName: contactInfoFromText?.nombre || (lastCloseContext.get(waId)?.explicitName || ''),
  lastUserText: text,
});

    // Si piden foto sin decir cuál: usar el último producto o las últimas opciones listadas
    if (userAsksForPhoto(userIntentText)) {
      const stockForPhotos = await getStockCatalog();
      const lastCtxForPhotos = getLastProductContext(waId);

      if (userAsksForAllPhotos(userIntentText) && lastCtxForPhotos?.lastOptions?.length) {
        const optionProducts = resolveProductsByNames(stockForPhotos, lastCtxForPhotos.lastOptions);
        const sentAll = await maybeSendMultipleProductPhotos(phone, optionProducts, userIntentText);
        if (sentAll) {
          scheduleInactivityFollowUp(waId, phone);
          return;
        }
      }

      const last = lastProductByUser.get(waId);
      if (last) {
        const sent = await maybeSendProductPhoto(phone, last, userIntentText);
        if (sent) {
          // ✅ INACTIVIDAD: programar follow-up luego de la respuesta del bot
          scheduleInactivityFollowUp(waId, phone);
          return;
        }
      }
    }


    // ===================== ✅ REGLAS ESPECIALES (sin inventar servicios) =====================
    let normTxt = normalize(text);
    const activeCourseContextEarly = getLastCourseContext(waId);
    const shouldSkipBarberWalkInRule = !!activeCourseContextEarly && (isExplicitCourseKeyword(text) || looksLikeCourseFollowUp(text));

    // Corte masculino: solo por orden de llegada (no se toma turno)
    if (/(\bcorte\b.*\b(mascul|varon|hombre)\b|\bcorte\s+masculino\b|\bbarber\b|\bbarberia\b)/i.test(normTxt) && !detectFemaleContext(text) && !shouldSkipBarberWalkInRule) {
      const msgMasc = `✂️ Corte masculino: es SOLO por orden de llegada (no se toma turno).

🕒 Horarios: Lunes a Sábados 10 a 13 hs y 17 a 22 hs.
💲 Precio final: $10.000.`;
      clearLastCourseContext(waId);
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

    // ===================== ✅ TURNOS (Calendar + Railway Postgres) =====================
    let pendingDraft = await getAppointmentDraft(waId);

    if (pendingDraft) {
      const pauseIntent = extractTurnoPauseIntent(text);
      if (pauseIntent.matched) {
        await deleteAppointmentDraft(waId);
        pendingDraft = null;

        if (!pauseIntent.remainder) {
          const msgPauseTurno = `Perfecto 😊

No hay problema. Frené la gestión del turno por ahora.

Cuando quiera retomarlo, me escribe y le paso nuevamente los horarios disponibles.`;
          pushHistory(waId, "assistant", msgPauseTurno);
          await sendWhatsAppText(phone, msgPauseTurno);
          scheduleInactivityFollowUp(waId, phone);
          return;
        }

        text = pauseIntent.remainder;
        normTxt = normalize(text);
      }
    }

    const recentDbMessages = await getRecentDbMessages(phone, 12);
    const convForAI = mergeConversationForAI(recentDbMessages, ensureConv(waId).messages || []);
    const lastKnownService = getLastKnownService(waId, pendingDraft);
    const lastBookedTurno = await getLastBookedAppointmentForUser({ waId, waPhone: phone });

    // ✅ Si el cliente responde afirmativamente a una propuesta de turno y ya veníamos hablando de un servicio,
    // iniciamos el flujo sin volver a preguntar el servicio.
    if (!pendingDraft && lastKnownService?.nombre && isWarmAffirmativeReply(text) && lastAssistantWasQuestion(waId)) {
      const baseTurno = {
        servicio: lastKnownService.nombre,
        fecha: '',
        hora: '',
        duracion_min: 60,
        notas: '',
        cliente_full: '',
        telefono_contacto: '',
        payment_status: 'not_paid',
        payment_amount: null,
        payment_sender: '',
        payment_receiver: '',
        payment_proof_text: '',
        payment_proof_media_id: '',
        payment_proof_filename: '',
        awaiting_contact: false,
        flow_step: 'awaiting_date',
        last_intent: 'book_appointment',
        last_service_name: lastKnownService.nombre,
      };
      await saveAppointmentDraft(waId, phone, baseTurno);
      await askForMissingTurnoData(baseTurno);
      return;
    }

    // ✅ Si el término es ambiguo (ej: alisado), preguntamos si busca servicio o producto.
    const ambiguousBeautyTerm = extractAmbiguousBeautyTerm(text);
    if (ambiguousBeautyTerm && !pendingDraft && !looksLikeAppointmentIntent(text, { pendingDraft, lastService: lastKnownService }) && !isExplicitProductIntent(text) && !isExplicitServiceIntent(text)) {
      const msgAclara = `✨ ${ambiguousBeautyTerm.charAt(0).toUpperCase() + ambiguousBeautyTerm.slice(1)} puede referirse a dos cosas.

¿Está buscando:
• el *servicio* para hacerse en el salón
• o un *producto* de alisado / tratamiento?

Si quiere, dígame “servicio” o “producto” y sigo por ahí 😊`;
      pushHistory(waId, 'assistant', msgAclara);
      await sendWhatsAppText(phone, msgAclara);
      scheduleInactivityFollowUp(waId, phone);
      return;
    }

    async function askForMissingTurnoData(base) {
      const servicioTxt = base?.servicio || base?.last_service_name || "";

      if (!servicioTxt) {
        const msgFalt = `Perfecto 😊 vamos a coordinar su turno.

✨ Primero necesito que me diga qué servicio quiere reservar.`;
        pushHistory(waId, "assistant", msgFalt);
        await sendWhatsAppText(phone, msgFalt);
        scheduleInactivityFollowUp(waId, phone);
        return;
      }

      if (!base?.fecha && !base?.hora) {
        const msgFalt = await buildWeeklyAvailabilityMessage({
          servicio: servicioTxt,
          durationMin: Number(base?.duracion_min || 60) || 60,
          limitDays: 6,
        });
        pushHistory(waId, "assistant", msgFalt);
        await sendWhatsAppText(phone, msgFalt);
        scheduleInactivityFollowUp(waId, phone);
        return;
      }

      if (!base?.fecha) {
        const msgFalt = await buildWeeklyAvailabilityMessage({
          servicio: servicioTxt,
          durationMin: Number(base?.duracion_min || 60) || 60,
          limitDays: 6,
        });
        pushHistory(waId, "assistant", msgFalt);
        await sendWhatsAppText(phone, msgFalt);
        scheduleInactivityFollowUp(waId, phone);
        return;
      }

      if (!base?.hora) {
        const msgFalt = await buildDateAvailabilityMessage({
          dateYMD: base.fecha,
          servicio: servicioTxt,
          durationMin: Number(base?.duracion_min || 60) || 60,
        });
        pushHistory(waId, "assistant", msgFalt);
        await sendWhatsAppText(phone, msgFalt);
        scheduleInactivityFollowUp(waId, phone);
        return;
      }

      if (!base?.cliente_full && !base?.telefono_contacto) {
        await askForContactData(base);
        return;
      }

      if (!base?.cliente_full) {
        await askForName(base);
        return;
      }

      if (!base?.telefono_contacto) {
        await askForPhone(base);
        return;
      }

      if (base?.payment_status !== 'paid_verified') {
        await askForPayment(base);
        return;
      }
    }

    async function askForContactData(base) {
      const toSave = { ...base, awaiting_contact: true, flow_step: 'awaiting_contact', last_intent: 'book_appointment', last_service_name: base.servicio || base.last_service_name || '' };
      await saveAppointmentDraft(waId, phone, toSave);
      updateLastCloseContext(waId, { suppressInactivityPrompt: true });
      const msgContacto = `Perfecto 😊

Para dejar el turno listo necesito estos datos 😊

👤 Nombre y apellido de la persona que va a asistir
📱 Número de teléfono de contacto`;
      pushHistory(waId, "assistant", msgContacto);
      await sendWhatsAppText(phone, msgContacto);
      scheduleInactivityFollowUp(waId, phone);
    }

    async function askForName(base) {
      const toSave = { ...base, awaiting_contact: true, flow_step: 'awaiting_name', last_intent: 'book_appointment', last_service_name: base.servicio || base.last_service_name || '' };
      await saveAppointmentDraft(waId, phone, toSave);
      updateLastCloseContext(waId, { suppressInactivityPrompt: true });
      const msgSoloNombre = `Perfecto 

Ahora necesito este dato 😊

👤 Nombre y apellido de la persona que va a asistir`;
      pushHistory(waId, "assistant", msgSoloNombre);
      await sendWhatsAppText(phone, msgSoloNombre);
      scheduleInactivityFollowUp(waId, phone);
    }

    async function askForPhone(base) {
      const toSave = { ...base, awaiting_contact: true, flow_step: 'awaiting_phone', last_intent: 'book_appointment', last_service_name: base.servicio || base.last_service_name || '' };
      await saveAppointmentDraft(waId, phone, toSave);
      updateLastCloseContext(waId, { suppressInactivityPrompt: true });
      const msgTelefono = `Perfecto 

Ahora necesito este dato 😊

📱 Número de teléfono de contacto`;
      pushHistory(waId, "assistant", msgTelefono);
      await sendWhatsAppText(phone, msgTelefono);
      scheduleInactivityFollowUp(waId, phone);
    }

    async function askForPayment(base) {
      await saveAppointmentDraft(waId, phone, { ...base, awaiting_contact: false, flow_step: 'awaiting_payment', last_intent: 'book_appointment', last_service_name: base.servicio || base.last_service_name || '' });
      updateLastCloseContext(waId, { suppressInactivityPrompt: true });
      const diaOk = base.fecha ? weekdayEsFromYMD(base.fecha) : '';
      const fechaTxt = base.fecha ? ymdToDMY(base.fecha) : '';
      const lines = [
        '',
        `Servicio: ${base.servicio || base.last_service_name || ''}`,
        `📅 Día: ${diaOk ? `${diaOk} ` : ''}${fechaTxt}`.trim(),
        `🕐 Hora: ${normalizeHourHM(base.hora) || base.hora || ''}`,
        '',
        `*PARA TERMINAR DE CONFIRMAR EL TURNO SE SOLICITA UNA SEÑA DE ${TURNOS_SENA_TXT}*`,
        '',
        '💳 Datos para la transferencia',
        '',
        'Alias:',
        TURNOS_ALIAS,
        '',
        'Titular:',
        TURNOS_ALIAS_TITULAR,
        '',
        'Cuando haga la transferencia, envíe por aquí el comprobante 📩',
      ];
      const msgPago = lines.join('\n').trim();
      pushHistory(waId, "assistant", msgPago);
      await sendWhatsAppText(phone, msgPago);
      scheduleInactivityFollowUp(waId, phone);
    }

    function fallbackLooksLikeBookingConfirmation(out) {
      const t = normalize(String(out || ''));
      return /(turno reservado|turno confirmado|queda confirmado|queda reservado|agendado|agendada|agendé|agende|reservado|reservada|seña recibida|sena recibida|comprobante recibido|pago confirmado)/i.test(t);
    }

    function buildSafeDraftReplyForFallback(base) {
      if (!base) return "Decime qué necesitás y lo vemos 😊";

      if (!base.servicio || !base.fecha || !base.hora) {
        return `Para seguir con el turno todavía me falta un dato 😊

Contame servicio, día y horario, y lo dejo listo.`;
      }

      if (!base.cliente_full && !base.telefono_contacto) {
        return `Para dejarlo listo necesito estos datos 

👤 Nombre y apellido de la persona que va a asistir
📱 Número de teléfono de contacto`;
      }

      if (!base.cliente_full) {
        return `Perfecto 😊

Ahora necesito este dato:

👤 Nombre y apellido de la persona que va a asistir`;
      }

      if (!base.telefono_contacto) {
        return `Perfecto 😊

Ahora necesito este dato 😊

📱 Número de teléfono de contacto`;
      }

      if (base.payment_status === 'payment_review') {
        return `Recibí el comprobante 😊

Lo estoy validando con los datos del turno. En cuanto quede confirmado, le aviso por aquí.`;
      }

      if (base.payment_status !== 'paid_verified') {
        return buildPaymentPendingMessage();
      }

      return `Ya tengo los datos del turno 😊 Si querés, te confirmo el detalle de fecha, hora y servicio.`;
    }

    async function respondFinalizeResult(base, result) {

      if (result.type === "busy") {
        const msgBusy = await buildBusyTurnoMessage({ base });
        pushHistory(waId, "assistant", msgBusy);
        await sendWhatsAppText(phone, msgBusy);
        scheduleInactivityFollowUp(waId, phone);
        return true;
      }
      if (result.type === "invalid_past_date") {
        const resetBase = {
          ...base,
          fecha: '',
          hora: '',
          flow_step: 'awaiting_date',
          last_intent: 'book_appointment',
          last_service_name: base.servicio || base.last_service_name || '',
        };
        await saveAppointmentDraft(waId, phone, resetBase);
        updateLastCloseContext(waId, { suppressInactivityPrompt: true });
        const msgPast = `Esa fecha ya pasó o ese horario ya quedó atrás 😊

${await buildWeeklyAvailabilityMessage({
          servicio: base?.servicio || base?.last_service_name || '',
          durationMin: Number(base?.duracion_min || 60) || 60,
          limitDays: 6,
        })}`;
        pushHistory(waId, "assistant", msgPast);
        await sendWhatsAppText(phone, msgPast);
        scheduleInactivityFollowUp(waId, phone);
        return true;
      }
      if (result.type === "need_name") {
        if (!base.telefono_contacto) {
          await askForContactData(base);
        } else {
          await askForName(base);
        }
        return true;
      }
      if (result.type === "need_phone") {
        if (!base.cliente_full) {
          await askForContactData(base);
        } else {
          await askForPhone(base);
        }
        return true;
      }
      if (result.type === "need_payment") {
        if (base.payment_status === 'payment_review' && (base.payment_proof_media_id || base.payment_proof_text)) {
          const msgRev = `Recibí el comprobante 😊

Lo estoy validando con los datos del turno. En cuanto quede confirmado, le aviso por aquí.`;
          await saveAppointmentDraft(waId, phone, { ...base, awaiting_contact: false, flow_step: 'payment_review', last_intent: 'book_appointment', last_service_name: base.servicio || base.last_service_name || '' });
          updateLastCloseContext(waId, { suppressInactivityPrompt: true });
          pushHistory(waId, 'assistant', msgRev);
          await sendWhatsAppText(phone, msgRev);
          scheduleInactivityFollowUp(waId, phone);
          return true;
        }
        await askForPayment(base);
        return true;
      }
      if (result.type === "missing_core") {
        await askForMissingTurnoData(base);
        return true;
      }
      if (result.type === "pending_stylist_confirmation") {
        clearProductMemory(waId);
        const diaOk = weekdayEsFromYMD(base.fecha);
        const msgPend = `✅ Solicitud recibida

Servicio: ${base.servicio}
📅 Día: ${diaOk} ${ymdToDMY(base.fecha)}
🕐 Hora: ${normalizeHourHM(base.hora) || base.hora}
👤 Cliente: ${base.cliente_full}
📱 Teléfono: ${normalizePhone(base.telefono_contacto || '')}

Seña recibida ✔

Ahora consulto con la estilista ${TURNOS_STYLIST_NAME} y le confirmo por aquí.`.trim();
        pushHistory(waId, "assistant", msgPend);
        await sendWhatsAppText(phone, msgPend);
        scheduleInactivityFollowUp(waId, phone);
        return true;
      }
      if (result.type === "booked") {
        clearProductMemory(waId);
        const diaOk = weekdayEsFromYMD(base.fecha);
        const msgOk = `*TURNO RESERVADO*✅

Servicio: ${base.servicio}
📅 Día: ${diaOk} ${ymdToDMY(base.fecha)}
🕐 Hora: ${normalizeHourHM(base.hora) || base.hora}
👤 Cliente: ${base.cliente_full}
📱 Teléfono: ${normalizePhone(base.telefono_contacto || '')}

Seña recibida ✔`.trim();
        pushHistory(waId, "assistant", msgOk);
        await sendWhatsAppText(phone, msgOk);
        scheduleInactivityFollowUp(waId, phone);
        return true;
      }
      return false;
    }

    if (pendingDraft && !(isYesNoShortReply(text) && lastAssistantWasQuestion(waId))) {
      const turno = await extractTurnoFromText({ text, customerName: name, context: pendingDraft });
      const relativeTurno = resolveRelativeTurnoReference(text, { pendingDraft, lastBooked: lastBookedTurno });
      const servicesForPendingTurn = await getServicesCatalog();
      const reliablePendingTurnService = resolveReliableTurnService({
        services: servicesForPendingTurn,
        text,
        pendingDraft,
        lastKnownService,
        aiService: turno?.servicio || '',
      });
      let merged = {
        ...pendingDraft,
        fecha: pendingDraft.fecha || relativeTurno?.fecha || "",
        hora: normalizeHourHM(pendingDraft.hora || relativeTurno?.hora || ""),
        servicio: reliablePendingTurnService || pendingDraft.servicio || pendingDraft.last_service_name || lastKnownService?.nombre || "",
        duracion_min: Number(pendingDraft.duracion_min || lastBookedTurno?.duracion_min || 60) || 60,
        notas: pendingDraft.notas || "",
      };

      if (turno?.ok) {
        merged = {
          ...merged,
          fecha: turno.fecha || merged.fecha || relativeTurno?.fecha || "",
          hora: normalizeHourHM(turno.hora || merged.hora || relativeTurno?.hora || ""),
          servicio: reliablePendingTurnService || turno.servicio || merged.servicio || "",
          duracion_min: Number(turno.duracion_min || merged.duracion_min || 60) || 60,
          notas: turno.notas || merged.notas || "",
        };
      }

      if (!merged.fecha && relativeTurno?.fecha) merged.fecha = relativeTurno.fecha;
      if (!merged.hora && relativeTurno?.hora) merged.hora = normalizeHourHM(relativeTurno.hora);

      if (!merged.servicio) {
        const rawTrim = String(text || "").trim();
        const nraw = normalize(rawTrim);
        const currentService = normalize(pendingDraft.servicio || pendingDraft.last_service_name || lastKnownService?.nombre || "");
        if (rawTrim && currentService && nraw === currentService) {
          merged.servicio = pendingDraft.servicio || pendingDraft.last_service_name || lastKnownService?.nombre || rawTrim;
        }
      }

      merged.fecha = toYMD(merged.fecha);
      Object.assign(merged, mergeContactIntoTurno({ turno: merged, text, waPhone: phone }));
      updateLastCloseContext(waId, { explicitName: merged.cliente_full || contactInfoFromText?.nombre || lastCloseContext.get(waId)?.explicitName || '' });
      merged = await tryApplyPaymentToDraft(merged, { text, mediaMeta });
      merged = await applyCatalogServiceDataToTurno(merged);

      if (!merged.servicio && lastKnownService?.nombre) {
        merged.servicio = lastKnownService.nombre;
        merged.last_service_name = lastKnownService.nombre;
      }

      if (!merged.servicio || !merged.fecha || !merged.hora) {
        merged.flow_step = inferDraftFlowStep(merged);
        merged.last_intent = 'book_appointment';
        merged.last_service_name = merged.servicio || merged.last_service_name || '';
        await saveAppointmentDraft(waId, phone, merged);
        updateLastCloseContext(waId, { suppressInactivityPrompt: true });
        await askForMissingTurnoData(merged);
        return;
      }

      const result = await finalizeAppointmentFlow({ waId, phone, name, merged });
      const handled = await respondFinalizeResult(merged, result);
      if (handled) return;
    }

    const looksLikeTurno = looksLikeAppointmentIntent(text, { pendingDraft, lastService: lastKnownService });
    if (looksLikeTurno && !(isYesNoShortReply(text) && lastAssistantWasQuestion(waId))) {
      const turno = await extractTurnoFromText({
        text,
        customerName: name,
        context: {
          servicio: pendingDraft?.servicio || lastKnownService?.nombre || "",
          fecha: pendingDraft?.fecha || "",
          hora: pendingDraft?.hora || "",
          duracion_min: pendingDraft?.duracion_min || 60,
          notas: pendingDraft?.notas || "",
        }
      });

      const relativeTurno = resolveRelativeTurnoReference(text, { pendingDraft, lastBooked: lastBookedTurno });

      if (turno?.ok || pendingDraft || lastKnownService || (isWarmAffirmativeReply(text) && lastKnownService) || relativeTurno?.fecha || relativeTurno?.hora) {
        const servicesForTurn = await getServicesCatalog();
        const reliableTurnService = resolveReliableTurnService({
          services: servicesForTurn,
          text,
          pendingDraft,
          lastKnownService,
          aiService: turno?.servicio || '',
        });

        const merged = {
          fecha: toYMD(turno?.fecha || pendingDraft?.fecha || relativeTurno?.fecha || ""),
          hora: normalizeHourHM(turno?.hora || pendingDraft?.hora || relativeTurno?.hora || ""),
          servicio: reliableTurnService || pendingDraft?.servicio || lastKnownService?.nombre || "",
          duracion_min: Number(turno?.duracion_min || pendingDraft?.duracion_min || lastBookedTurno?.duracion_min || 60) || 60,
          notas: turno?.notas || pendingDraft?.notas || "",
          cliente_full: pendingDraft?.cliente_full || "",
          telefono_contacto: normalizePhone(pendingDraft?.telefono_contacto || ""),
          payment_status: pendingDraft?.payment_status || "not_paid",
          payment_amount: pendingDraft?.payment_amount ?? null,
          payment_sender: pendingDraft?.payment_sender || "",
          payment_receiver: pendingDraft?.payment_receiver || "",
          payment_proof_text: pendingDraft?.payment_proof_text || "",
          payment_proof_media_id: pendingDraft?.payment_proof_media_id || "",
          payment_proof_filename: pendingDraft?.payment_proof_filename || "",
          awaiting_contact: !!pendingDraft?.awaiting_contact,
          flow_step: pendingDraft?.flow_step || "",
          last_intent: "book_appointment",
          last_service_name: pendingDraft?.last_service_name || lastKnownService?.nombre || "",
        };

        const falt = new Set(turno?.faltantes || []);
        if (!merged.fecha) falt.add("fecha");
        if (!merged.hora) falt.add("hora");
        if (!merged.servicio) falt.add("servicio");

        Object.assign(merged, mergeContactIntoTurno({ turno: merged, text, waPhone: phone }));
        updateLastCloseContext(waId, { explicitName: merged.cliente_full || contactInfoFromText?.nombre || lastCloseContext.get(waId)?.explicitName || '' });
        let mergedWithPayment = await tryApplyPaymentToDraft(merged, { text, mediaMeta });
        mergedWithPayment = await applyCatalogServiceDataToTurno(mergedWithPayment);
        mergedWithPayment.flow_step = inferDraftFlowStep(mergedWithPayment);
        mergedWithPayment.last_intent = "book_appointment";
        mergedWithPayment.last_service_name = mergedWithPayment.servicio || mergedWithPayment.last_service_name || "";

        if (falt.size) {
          mergedWithPayment.flow_step = inferDraftFlowStep(mergedWithPayment);
          mergedWithPayment.last_intent = 'book_appointment';
          mergedWithPayment.last_service_name = mergedWithPayment.servicio || mergedWithPayment.last_service_name || '';
          await saveAppointmentDraft(waId, phone, mergedWithPayment);
        updateLastCloseContext(waId, { suppressInactivityPrompt: true });
          await askForMissingTurnoData(mergedWithPayment);
          return;
        }

        const result = await finalizeAppointmentFlow({ waId, phone, name, merged: mergedWithPayment });
        const handled = await respondFinalizeResult(mergedWithPayment, result);
        if (handled) return;
      }
    }

    if (pendingDraft && !isExplicitProductIntent(text)) {
      const rawTrim = String(text || '').trim();
      const nraw = normalize(rawTrim);
      const currentService = normalize(pendingDraft?.servicio || pendingDraft?.last_service_name || lastKnownService?.nombre || '');
      if (rawTrim && currentService && nraw === currentService) {
        const repairedDraft = {
          ...pendingDraft,
          servicio: pendingDraft?.servicio || pendingDraft?.last_service_name || lastKnownService?.nombre || rawTrim,
          flow_step: inferDraftFlowStep({
            ...pendingDraft,
            servicio: pendingDraft?.servicio || pendingDraft?.last_service_name || lastKnownService?.nombre || rawTrim,
          }),
          last_intent: 'book_appointment',
          last_service_name: pendingDraft?.servicio || pendingDraft?.last_service_name || lastKnownService?.nombre || rawTrim,
        };
        await saveAppointmentDraft(waId, phone, repairedDraft);
        updateLastCloseContext(waId, { suppressInactivityPrompt: true });
        await askForMissingTurnoData(repairedDraft);
        return;
      }
    }

    if (!pendingDraft && isPoliteClosureAfterTurno(text) && lastAssistantLooksLikeTurnoMessage(waId)) {
      clearProductMemory(waId);
      const msgCierreTurno = `¡Gracias a vos! 😊

Tu turno ya quedó registrado. Cualquier cosa, estoy acá ✨`;
      pushHistory(waId, "assistant", msgCierreTurno);
      await sendWhatsAppText(phone, msgCierreTurno);
      updateLastCloseContext(waId, { suppressInactivityPrompt: true });
      return;
    }

    if (isPoliteCatalogDecline(text) && lastAssistantLooksLikeCatalogMessage(waId)) {
      clearProductMemory(waId);
      const msgNoCatalogo = `Perdón 😊 No le molesto más con eso.

Si después necesita algo, estoy acá ✨`;
      pushHistory(waId, "assistant", msgNoCatalogo);
      await sendWhatsAppText(phone, msgNoCatalogo);
      scheduleInactivityFollowUp(waId, phone);
      return;
    }

    if (textAsksForServicesList(text)) {
      const services = await getServicesCatalog();
      const parts = formatServicesListAll(services, 8);
      for (const part of parts.slice(0, 3)) {
        pushHistory(waId, "assistant", part);
        await sendWhatsAppText(phone, part);
      }
      scheduleInactivityFollowUp(waId, phone);
      return;
    }

    if (!pendingDraft && lastKnownService?.nombre && /^producto$/i.test(normalize(text))) {
      const stock = await getStockCatalog();
      const matches = findStock(stock, lastKnownService.nombre, 'LIST');
      if (matches.length) {
        const replyCatalog = formatStockReply(matches, 'LIST');
        pushHistory(waId, 'assistant', replyCatalog);
        await sendWhatsAppText(phone, replyCatalog);
        scheduleInactivityFollowUp(waId, phone);
        return;
      }
    }

    if (!pendingDraft && lastKnownService?.nombre && /^servicio$/i.test(normalize(text))) {
      const services = await getServicesCatalog();
      const matches = findServices(services, lastKnownService.nombre, 'DETAIL');
      const replyCatalog = formatServicesReply(matches, 'DETAIL');
      if (replyCatalog) {
        pushHistory(waId, 'assistant', replyCatalog);
        await sendWhatsAppText(phone, replyCatalog);
        scheduleInactivityFollowUp(waId, phone);
        return;
      }
    }

    if (textAsksForServiceDuration(text)) {
      const services = await getServicesCatalog();
      const ctxService = pendingDraft?.servicio || lastKnownService?.nombre || "";
      let durationMatches = findServiceByContext(services, text, ctxService);

      if (durationMatches.length) {
        const selectedService = durationMatches[0];
        if (selectedService?.nombre) {
          lastServiceByUser.set(waId, { nombre: selectedService.nombre, ts: Date.now() });
          if (pendingDraft) {
            await saveAppointmentDraft(waId, phone, {
              ...pendingDraft,
              servicio: pendingDraft.servicio || selectedService.nombre,
              duracion_min: parseServiceDurationToMinutes(selectedService.duracion) || Number(pendingDraft.duracion_min || 60) || 60,
              last_service_name: selectedService.nombre,
              last_intent: 'service_consultation',
              flow_step: pendingDraft.flow_step || inferDraftFlowStep({ ...pendingDraft, servicio: pendingDraft.servicio || selectedService.nombre }),
            });
          }
        }
        const replyDuration = formatServicesReply(durationMatches, "DETAIL", { showDuration: true, showDescription: true });
        if (replyDuration) {
          pushHistory(waId, "assistant", replyDuration);
          await sendWhatsAppText(phone, replyDuration);
          scheduleInactivityFollowUp(waId, phone);
          return;
        }
      }

      if (ctxService) {
        const msgNoDuration = `No encuentro la duración cargada para *${ctxService}* en este momento.`;
        pushHistory(waId, "assistant", msgNoDuration);
        await sendWhatsAppText(phone, msgNoDuration);
        scheduleInactivityFollowUp(waId, phone);
        return;
      }
    }

    if (textAsksForServicePrice(text)) {
      const services = await getServicesCatalog();
      const ctxService = pendingDraft?.servicio || lastKnownService?.nombre || "";
      let priceMatches = findServiceByContext(services, text, ctxService);

      if (priceMatches.length) {
        const selectedService = priceMatches[0];
        if (selectedService?.nombre) {
          lastServiceByUser.set(waId, { nombre: selectedService.nombre, ts: Date.now() });
          if (pendingDraft) {
            await saveAppointmentDraft(waId, phone, {
              ...pendingDraft,
              servicio: pendingDraft.servicio || selectedService.nombre,
              duracion_min: parseServiceDurationToMinutes(selectedService.duracion) || Number(pendingDraft.duracion_min || 60) || 60,
              last_service_name: selectedService.nombre,
              last_intent: 'service_consultation',
              flow_step: pendingDraft.flow_step || inferDraftFlowStep({ ...pendingDraft, servicio: pendingDraft.servicio || selectedService.nombre }),
            });
          }
        }
        const replyPrice = formatServicesReply(priceMatches, "DETAIL");
        if (replyPrice) {
          pushHistory(waId, "assistant", replyPrice);
          await sendWhatsAppText(phone, replyPrice);
          scheduleInactivityFollowUp(waId, phone);
          return;
        }
      }

      if (ctxService) {
        const msgNoPrice = `No encuentro el precio cargado para *${ctxService}* en este momento.`;
        pushHistory(waId, "assistant", msgNoPrice);
        await sendWhatsAppText(phone, msgNoPrice);
        scheduleInactivityFollowUp(waId, phone);
        return;
      }
    }

    const implicitServiceFollowupQuery = resolveImplicitServiceFollowupQuery(text, pendingDraft?.servicio || lastKnownService?.nombre || '');
    if (!pendingDraft && implicitServiceFollowupQuery && !isExplicitProductIntent(text)) {
      const services = await getServicesCatalog();
      const implicitMatches = findServices(services, implicitServiceFollowupQuery, 'DETAIL');
      if (implicitMatches.length) {
        const selectedService = implicitMatches[0];
        if (selectedService?.nombre) {
          lastServiceByUser.set(waId, { nombre: selectedService.nombre, ts: Date.now() });
        }
        const wantsDuration = textAsksForServiceDuration(text);
        const replyImplicit = formatServicesReply(implicitMatches, 'DETAIL', {
          showDuration: wantsDuration,
          showDescription: wantsDuration,
        });
        if (replyImplicit) {
          pushHistory(waId, 'assistant', replyImplicit);
          await sendWhatsAppText(phone, replyImplicit);
          scheduleInactivityFollowUp(waId, phone);
          return;
        }
      }
    }

    const lastCourseContext = getLastCourseContext(waId);

    let intent = await classifyAndExtract(text, {
      lastServiceName: lastKnownService?.nombre || '',
      lastCourseName: lastCourseContext?.selectedName || lastCourseContext?.query || '',
      hasCourseContext: !!lastCourseContext,
      courseOptions: Array.isArray(lastCourseContext?.lastOptions) ? lastCourseContext.lastOptions.slice(0, 10) : [],
      flowStep: pendingDraft?.flow_step || '',
      hasDraft: !!pendingDraft,
      historySnippet: convForAI.slice(-8).map((m) => `${m.role}: ${m.content}`).join(' | ').slice(0, 1600),
    });

    const explicitCourseIntent = detectCourseIntentFromContext(text, { lastCourseContext });
    if (explicitCourseIntent.isCourse) {
      intent = {
        ...intent,
        type: 'COURSE',
        query: explicitCourseIntent.query || intent.query || '',
        mode: explicitCourseIntent.mode || intent.mode || 'DETAIL',
      };
    }

    const lastProductCtx = getLastProductContext(waId);

    const productAI = (intent.type === 'PRODUCT' || intent.type === 'OTHER' || isExplicitProductIntent(text) || !!lastProductCtx)
      ? await extractProductIntentWithAI(text, {
          lastProductName: lastProductByUser.get(waId)?.nombre || '',
          lastServiceName: lastKnownService?.nombre || '',
          lastFamily: lastProductCtx?.family || '',
          lastHairType: lastProductCtx?.hairType || '',
          lastNeed: lastProductCtx?.need || '',
          lastUseType: lastProductCtx?.useType || '',
        })
      : null;

    const shouldTreatAsProduct = intent.type === 'PRODUCT' || (
      intent.type !== 'SERVICE' &&
      intent.type !== 'COURSE' &&
      (
        !!productAI?.is_product_query ||
        (!!lastProductCtx && looksLikeProductPreferenceReply(text))
      )
    );

    // ✅ actualizar tipo de intención para el seguimiento
    updateLastCloseContext(waId, {
      intentType: shouldTreatAsProduct ? 'PRODUCT' : (intent?.type || lastCloseContext.get(waId)?.intentType || 'OTHER'),
      explicitName: contactInfoFromText?.nombre || lastCloseContext.get(waId)?.explicitName || '',
      profileName: name || lastCloseContext.get(waId)?.profileName || '',
      lastUserText: text,
    });

    // Si el clasificador falla, igual intentamos buscar en stock con el texto del cliente
    // ✅ Evitar confusión: "SI/NO/OK/DALE" como respuesta a la última pregunta del bot NO debe disparar catálogo.
    if (!shouldTreatAsProduct && intent.type === 'OTHER' && !(isYesNoShortReply(text) && lastAssistantWasQuestion(waId))) {
      const stock = await getStockCatalog();
      const q = guessQueryFromText(text);
      const matches = findStock(stock, q, 'DETAIL');

      if (matches.length) {
        lastProductByUser.set(waId, matches[0]);

        if (userAsksForPhoto(text)) {
          const sent = await maybeSendProductPhoto(phone, matches[0], text);
          if (sent) {
            scheduleInactivityFollowUp(waId, phone);
            return;
          }
        }

        const replyCatalog = formatStockReply(matches, 'DETAIL');
        if (replyCatalog) {
          pushHistory(waId, 'assistant', replyCatalog);
          await sendWhatsAppText(phone, replyCatalog);
          scheduleInactivityFollowUp(waId, phone);
          return;
        }
      }
    }

    // PRODUCT
    if (shouldTreatAsProduct) {
      const stock = await getStockCatalog();
      const aiFamilyRaw = productAI?.family || '';
      const aiFamily = normalizeCatalogSearchText(aiFamilyRaw) === 'otro' ? '' : aiFamilyRaw;
      const aiSearchText = productAI?.specific_name || productAI?.search_text || '';
      const resolvedQuery = aiSearchText || intent.query || guessQueryFromText(text) || text;
      const resolvedFamily = aiFamily || detectProductFamily(resolvedQuery) || detectProductFamily(text) || lastProductCtx?.family || '';
      const resolvedFocusTerm = detectProductFocusTerm(aiSearchText || intent.query || text) || lastProductCtx?.focusTerm || '';
      const resolvedHairType = productAI?.hair_type || lastProductCtx?.hairType || '';
      const resolvedNeed = productAI?.need || lastProductCtx?.need || '';
      const resolvedUseType = normalizeUseType(productAI?.use_type || lastProductCtx?.useType || '');
      const productMode = (
        intent.mode === 'LIST' ||
        !!productAI?.wants_all_related ||
        (!productAI?.specific_name && !productAI?.wants_photo && isGenericProductQuery(resolvedQuery)) ||
        (resolvedFamily && !productAI?.specific_name && !intent.query)
      ) ? 'LIST' : intent.mode;

      const qCleanTokens = tokenize(intent.query || resolvedQuery || '', { expandSynonyms: true });
      const wantsAll = productMode === 'LIST' && (
        !resolvedQuery || !String(resolvedQuery).trim() || qCleanTokens.length === 0 ||
        /\b(catalogo|catálogo|lista|todo|toda|todos|todas|productos|stock)\b/i.test(resolvedQuery)
      );

      if (wantsAll) {
        const parts = formatStockListAll(stock, 12);
        setLastProductContext(waId, {
          family: resolvedFamily || '',
          focusTerm: resolvedFocusTerm || '',
          hairType: resolvedHairType || '',
          need: resolvedNeed || '',
          useType: resolvedUseType || '',
          mode: 'list_all',
        });
        for (const part of parts) {
          pushHistory(waId, 'assistant', part);
          await sendWhatsAppText(phone, part);
        }
        scheduleInactivityFollowUp(waId, phone);
        return;
      }

      let related = findStockRelated(stock, resolvedQuery, { family: resolvedFamily, focusTerm: resolvedFocusTerm, limit: 200 });
      let broader = related.length ? related : findStockRelated(stock, text, { family: resolvedFamily, focusTerm: resolvedFocusTerm, limit: 200 });
      const detailQuery = productAI?.specific_name || intent.query || guessQueryFromText(text);
      let matches = detailQuery ? findStock(stock, detailQuery, 'DETAIL') : [];

      if (resolvedFocusTerm && (isGenericProductOptionsFollowUp(text) || productMode === 'LIST' || !!productAI?.wants_all_related)) {
        related = filterRowsByProductFocus(related, resolvedFocusTerm);
        broader = filterRowsByProductFocus(broader, resolvedFocusTerm);
        matches = filterRowsByProductFocus(matches, resolvedFocusTerm);
      }
      const wantsRecommendation = !!(
        productAI?.wants_recommendation ||
        resolvedHairType ||
        resolvedNeed ||
        resolvedUseType ||
        (lastProductCtx && looksLikeProductPreferenceReply(text))
      );

      if (wantsRecommendation) {
        const pool = (matches.length > 1 ? matches : [])
          .concat(related)
          .concat(broader)
          .filter((row, idx, arr) =>
            arr.findIndex((x) =>
              normalizeCatalogSearchText(`${x?.nombre || ''} ${x?.marca || ''}`) ===
              normalizeCatalogSearchText(`${row?.nombre || ''} ${row?.marca || ''}`)
            ) === idx
          );

        const shortlist = shortlistProductsForRecommendation(pool.length ? pool : stock, {
          family: resolvedFamily,
          focusTerm: resolvedFocusTerm,
          hairType: resolvedHairType,
          need: resolvedNeed,
          useType: resolvedUseType,
          query: resolvedQuery || text,
          limit: 10,
        });

        if (shortlist.length) {
          const recoAI = await recommendProductsWithAI({
            text,
            familyLabel: resolvedFamily,
            hairType: resolvedHairType,
            need: resolvedNeed,
            useType: resolvedUseType,
            products: shortlist,
          });

          const picked = recoAI?.recommended_names?.length
            ? shortlist.filter((row) =>
                recoAI.recommended_names.some((name) => normalizeCatalogSearchText(name) === normalizeCatalogSearchText(row.nombre))
              ).slice(0, 4)
            : shortlist.slice(0, 4);

          const replyReco = formatRecommendedProductsReply(recoAI, picked.length ? picked : shortlist.slice(0, 4), {
            familyLabel: resolvedFamily,
            hairType: resolvedHairType,
            need: resolvedNeed,
            useType: resolvedUseType,
          });

          if (replyReco) {
            setLastProductContext(waId, {
              family: resolvedFamily || detectProductFamily(text) || '',
              focusTerm: resolvedFocusTerm || '',
              hairType: resolvedHairType || '',
              need: resolvedNeed || '',
              useType: resolvedUseType || '',
              mode: 'recommendation',
              lastOptions: (picked.length ? picked : shortlist.slice(0, 4)).map((x) => x.nombre),
            });
            if (picked.length === 1) lastProductByUser.set(waId, picked[0]);
            pushHistory(waId, 'assistant', replyReco);
            await sendWhatsAppText(phone, replyReco);
            scheduleInactivityFollowUp(waId, phone);
            return;
          }
        }
      }

      if (productMode === 'LIST') {
        if (related.length) {
          const relatedSlice = related.slice(0, 12);
          setLastProductContext(waId, {
            family: resolvedFamily || detectProductFamily(resolvedQuery) || '',
            focusTerm: resolvedFocusTerm || '',
            hairType: resolvedHairType || '',
            need: resolvedNeed || '',
            useType: resolvedUseType || '',
            mode: 'list',
            lastOptions: relatedSlice.map((x) => x.nombre),
          });

          if (userAsksForAllPhotos(text)) {
            const sentAll = await maybeSendMultipleProductPhotos(phone, relatedSlice, text);
            if (sentAll) {
              scheduleInactivityFollowUp(waId, phone);
              return;
            }
          }

          const parts = formatStockRelatedListAll(related, {
            familyLabel: resolvedFamily || detectProductFamily(resolvedQuery),
            chunkSize: 8,
          });
          for (const part of parts) {
            pushHistory(waId, 'assistant', part);
            await sendWhatsAppText(phone, part);
          }
          scheduleInactivityFollowUp(waId, phone);
          return;
        }
      }

      if (matches.length) {
        if (matches.length === 1) {
          lastProductByUser.set(waId, matches[0]);
          setLastProductContext(waId, {
            family: resolvedFamily || detectProductFamily(matches[0].nombre) || '',
            focusTerm: resolvedFocusTerm || detectProductFocusTerm(matches[0].nombre) || '',
            hairType: resolvedHairType || '',
            need: resolvedNeed || '',
            useType: resolvedUseType || '',
            mode: 'detail',
            lastOptions: [matches[0].nombre],
          });
        } else {
          setLastProductContext(waId, {
            family: resolvedFamily || detectProductFamily(resolvedQuery) || '',
            focusTerm: resolvedFocusTerm || '',
            hairType: resolvedHairType || '',
            need: resolvedNeed || '',
            useType: resolvedUseType || '',
            mode: 'list',
            lastOptions: matches.slice(0, 8).map((x) => x.nombre),
          });
        }

        if (userAsksForAllPhotos(text) && matches.length > 1) {
          const sentAll = await maybeSendMultipleProductPhotos(phone, matches, text);
          if (sentAll) {
            scheduleInactivityFollowUp(waId, phone);
            return;
          }
        }

        if ((productAI?.wants_photo || userAsksForPhoto(text)) && matches.length === 1) {
          const sent = await maybeSendProductPhoto(phone, matches[0], text);
          if (sent) {
            scheduleInactivityFollowUp(waId, phone);
            return;
          }
        }

        const replyCatalog = formatStockReply(matches, matches.length === 1 ? 'DETAIL' : 'LIST');
        if (replyCatalog) {
          pushHistory(waId, 'assistant', replyCatalog);
          await sendWhatsAppText(phone, replyCatalog);
          scheduleInactivityFollowUp(waId, phone);
          return;
        }
      }

      if (broader.length) {
        const broaderSlice = broader.slice(0, 12);
        setLastProductContext(waId, {
          family: resolvedFamily || detectProductFamily(resolvedQuery) || '',
          focusTerm: resolvedFocusTerm || '',
          hairType: resolvedHairType || '',
          need: resolvedNeed || '',
          useType: resolvedUseType || '',
          mode: 'list',
          lastOptions: broaderSlice.map((x) => x.nombre),
        });

        if (userAsksForAllPhotos(text)) {
          const sentAll = await maybeSendMultipleProductPhotos(phone, broaderSlice, text);
          if (sentAll) {
            scheduleInactivityFollowUp(waId, phone);
            return;
          }
        }

        const parts = formatStockRelatedListAll(broader, {
          familyLabel: resolvedFamily || detectProductFamily(resolvedQuery),
          chunkSize: 8,
        });
        for (const part of parts) {
          pushHistory(waId, 'assistant', part);
          await sendWhatsAppText(phone, part);
        }
        scheduleInactivityFollowUp(waId, phone);
        return;
      }

      setLastProductContext(waId, {
        family: resolvedFamily || '',
        focusTerm: resolvedFocusTerm || '',
        hairType: resolvedHairType || '',
        need: resolvedNeed || '',
        useType: resolvedUseType || '',
        mode: 'followup',
      });
      await sendWhatsAppText(phone, 'No lo encuentro así en el catálogo. Dígame la marca, para qué lo necesita o qué tipo de cabello tiene y le recomiendo mejor 😊');
      scheduleInactivityFollowUp(waId, phone);
      return;
    }
    // SERVICE
    if (intent.type === "SERVICE") {
      const services = await getServicesCatalog();
      const reliableServiceQuery = resolveReliableTurnService({
        services,
        text,
        pendingDraft,
        lastKnownService,
        aiService: intent.query || '',
      }) || intent.query;
      const matches = reliableServiceQuery ? findServices(services, reliableServiceQuery, intent.mode) : [];
      const replyCatalog = formatServicesReply(matches, intent.mode);

      if (replyCatalog) {
        clearLastCourseContext(waId);
        // ✅ Guardamos "último servicio" para continuidad de la charla y toma de turnos
        if (matches.length) {
          const selectedService = matches[0]?.nombre || '';
          if (selectedService) {
            lastServiceByUser.set(waId, { nombre: selectedService, ts: Date.now() });
            if (pendingDraft) {
              await saveAppointmentDraft(waId, phone, {
                ...pendingDraft,
                servicio: pendingDraft.servicio || selectedService,
                duracion_min: parseServiceDurationToMinutes(matches[0]?.duracion) || Number(pendingDraft.duracion_min || 60) || 60,
                last_service_name: selectedService,
                last_intent: 'service_consultation',
                flow_step: pendingDraft.flow_step || inferDraftFlowStep({ ...pendingDraft, servicio: pendingDraft.servicio || selectedService }),
              });
            }
          }
        }

        pushHistory(waId, "assistant", replyCatalog);
        await sendWhatsAppText(phone, replyCatalog);
        // ✅ INACTIVIDAD
        scheduleInactivityFollowUp(waId, phone);
        return;
      }

      // ✅ Evitar inventar servicios: si no está en el Excel, lo decimos y mostramos opciones reales
      const some = services.slice(0, 12).map(s => `${getCatalogItemEmoji(cleanServiceName(s.nombre), { kind: 'service' })} ${cleanServiceName(s.nombre)}`).join("\n");
      const msgNo = `No encuentro ese servicio en nuestra lista.

Servicios disponibles (algunos):
${some}

¿Con cuál desea sacar turno o consultar precio?`;
      clearLastCourseContext(waId);
      pushHistory(waId, "assistant", msgNo);
      await sendWhatsAppText(phone, msgNo);
      scheduleInactivityFollowUp(waId, phone);
      return;
    }
// COURSE
    if (intent.type === "COURSE") {
      const courses = await getCoursesCatalog();
      const courseQuery = resolveImplicitCourseFollowupQuery(text, lastCourseContext) || intent.query || '';
      const normalizedCourseQuery = normalize(courseQuery || '');
      const isGenericCourseQuery = !normalizedCourseQuery || /^(curso|cursos|taller|talleres|capacitacion|capacitaciones)$/.test(normalizedCourseQuery);

      if (!courses.length) {
        const msgEmpty = `La hoja CURSOS está vacía o no pude leer cursos disponibles en este momento.`;
        pushHistory(waId, "assistant", msgEmpty);
        await sendWhatsAppText(phone, msgEmpty);
        scheduleInactivityFollowUp(waId, phone);
        return;
      }

      const matches = isGenericCourseQuery ? courses : findCourses(courses, courseQuery, intent.mode);
      const replyCatalog = formatCoursesReply(matches, isGenericCourseQuery ? 'LIST' : intent.mode);
      if (replyCatalog) {
        setLastCourseContext(waId, {
          query: courseQuery || 'cursos',
          selectedName: matches.length === 1 ? (matches[0]?.nombre || '') : '',
          lastOptions: matches.slice(0, 10).map((c) => c.nombre).filter(Boolean),
        });
        pushHistory(waId, "assistant", replyCatalog);
        await sendWhatsAppText(phone, replyCatalog);
        await maybeSendCoursePhotos(phone, matches);
        // ✅ INACTIVIDAD
        scheduleInactivityFollowUp(waId, phone);
        return;
      }

      const someRows = courses.slice(0, 12);
      setLastCourseContext(waId, {
        query: courseQuery || 'cursos',
        selectedName: '',
        lastOptions: someRows.map((c) => c.nombre).filter(Boolean),
      });
      const some = someRows.map(c => `🎓 ${c.nombre}`).join("\n");
      const msgNo = `No encuentro ese curso en la hoja CURSOS.

Cursos disponibles (algunos):
${some}

Si quiere, le paso información de cualquiera de esos 😊`;
      pushHistory(waId, "assistant", msgNo);
      await sendWhatsAppText(phone, msgNo);
      scheduleInactivityFollowUp(waId, phone);
      return;
    }

    // fallback
    const model = pickModelForText(text);

    const messages = [
      { role: "system", content: SYSTEM_PROMPT },
      { role: "system", content: `Fecha y hora actual: ${nowARString()} (${TIMEZONE}).` },
      { role: "system", content: `Contexto del turno actual: ${JSON.stringify({
        servicio_actual: pendingDraft?.servicio || lastKnownService?.nombre || '',
        fecha_actual: pendingDraft?.fecha || '',
        hora_actual: pendingDraft?.hora || '',
        nombre_cliente: pendingDraft?.cliente_full || name || '',
        estado_pago: pendingDraft?.payment_status || 'not_paid',
        paso_actual: pendingDraft?.flow_step || '',
      })}` },
    ];
    if (name) messages.push({ role: "system", content: `Nombre del cliente: ${name}.` });

    for (const m of convForAI) messages.push(m);
    messages.push({ role: "user", content: text });

    const reply = await openai.chat.completions.create({ model, messages });
    let out = reply.choices?.[0]?.message?.content || "No pude responder.";

    if (pendingDraft && fallbackLooksLikeBookingConfirmation(out)) {
      const safeDraftState = {
        ...pendingDraft,
        servicio: pendingDraft?.servicio || lastKnownService?.nombre || '',
      };
      out = buildSafeDraftReplyForFallback(safeDraftState);
    }

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

// 23:59 hora Argentina (desactivado por defecto: ahora el seguimiento principal se sube a HubSpot al cerrar la charla)
if (ENABLE_END_OF_DAY_TRACKING) {
  setInterval(() => {
    const now = new Date().toLocaleTimeString("es-AR", { timeZone: TIMEZONE });
    if (now.startsWith("23:59")) endOfDayJob();
  }, 60000);
}

// ===================== PLANTILLAS DE TURNOS =====================
if (ENABLE_APPOINTMENT_TEMPLATES) {
  setInterval(() => {
    processAppointmentTemplateNotifications().catch((e) => {
      console.error('❌ Error en el scheduler de plantillas de turnos:', e?.response?.data || e?.message || e);
    });
  }, APPOINTMENT_TEMPLATE_SCAN_MS);
} else {
  console.log("ℹ️ Plantillas de turnos desactivadas temporalmente");
}

// ===================== START =====================
const PORT = process.env.PORT || 3000;

(async () => {
  await ensureDb();
  await ensureAppointmentTables();
  console.log(hasHubSpotEnabled()
    ? "✅ HubSpot CRM habilitado para seguimiento al cierre de charla"
    : "⚠️ HubSpot CRM no configurado: falta HUBSPOT_ACCESS_TOKEN / HUBSPOT_TOKEN");
  console.log(ENABLE_END_OF_DAY_TRACKING
    ? "ℹ️ Seguimiento de medianoche activado"
    : "ℹ️ Seguimiento de medianoche desactivado (se usa cierre por inactividad)");
  console.log(ENABLE_APPOINTMENT_TEMPLATES
    ? "ℹ️ Plantillas de turnos activadas"
    : "ℹ️ Plantillas de turnos desactivadas temporalmente");
  if (ENABLE_APPOINTMENT_TEMPLATES) {
    await processAppointmentTemplateNotifications().catch((e) => {
      console.error('❌ Error inicial procesando plantillas de turnos:', e?.response?.data || e?.message || e);
    });
  }

  app.listen(PORT, () => {
    console.log("🚀 Bot de estética activo");
    console.log(`Webhook: http://localhost:${PORT}/webhook`);
    console.log(`Health:  http://localhost:${PORT}/health`);
  });
})();
