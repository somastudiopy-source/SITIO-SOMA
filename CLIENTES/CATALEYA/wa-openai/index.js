
const express = require("express");
const axios = require("axios");
const dotenv = require("dotenv");
const OpenAI = require("openai");
const { google } = require("googleapis");
const fs = require("fs");
const path = require("path");
const os = require("os");
const { Pool } = require("pg");
let XLSX = null;
try { XLSX = require("xlsx"); } catch {}
let multer = null;
try { multer = require("multer"); } catch {}
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

// ===================== GOOGLE CONTACTS (People API) =====================
const GOOGLE_CONTACTS_CLIENT_ID = process.env.GOOGLE_CONTACTS_CLIENT_ID || "";
const GOOGLE_CONTACTS_CLIENT_SECRET = process.env.GOOGLE_CONTACTS_CLIENT_SECRET || "";
const GOOGLE_CONTACTS_REDIRECT_URI = process.env.GOOGLE_CONTACTS_REDIRECT_URI || "https://developers.google.com/oauthplayground";
const GOOGLE_CONTACTS_REFRESH_TOKEN_1 = process.env.GOOGLE_CONTACTS_REFRESH_TOKEN_1 || "";
const GOOGLE_CONTACTS_ACCOUNT_EMAIL_1 = String(process.env.GOOGLE_CONTACTS_ACCOUNT_EMAIL_1 || "").trim().toLowerCase();
const GOOGLE_CONTACTS_REFRESH_TOKEN_2 = process.env.GOOGLE_CONTACTS_REFRESH_TOKEN_2 || "";
const GOOGLE_CONTACTS_ACCOUNT_EMAIL_2 = String(process.env.GOOGLE_CONTACTS_ACCOUNT_EMAIL_2 || "").trim().toLowerCase();
const ENABLE_GOOGLE_CONTACTS_SYNC = String(process.env.ENABLE_GOOGLE_CONTACTS_SYNC || "true").toLowerCase() === "true";

const GOOGLE_CONTACTS_TARGETS = [
  { index: 1, email: GOOGLE_CONTACTS_ACCOUNT_EMAIL_1, refreshToken: GOOGLE_CONTACTS_REFRESH_TOKEN_1 },
  { index: 2, email: GOOGLE_CONTACTS_ACCOUNT_EMAIL_2, refreshToken: GOOGLE_CONTACTS_REFRESH_TOKEN_2 },
].filter((x) => x.email && x.refreshToken);

const googleContactsRuntimeState = new Map();

function getGoogleContactsTargetKey(target = {}) {
  return String(target?.email || target?.index || '').trim().toLowerCase();
}

function isGoogleInvalidGrantError(err) {
  const responseData = err?.response?.data || {};
  const raw = [
    err?.message || '',
    err?.code || '',
    responseData?.error || '',
    responseData?.error_description || '',
  ].join(' | ').toLowerCase();

  return raw.includes('invalid_grant') || raw.includes('expired or revoked');
}

function markGoogleContactsTargetInvalidGrant(target, err = null) {
  const key = getGoogleContactsTargetKey(target);
  if (!key) return;

  googleContactsRuntimeState.set(key, {
    disabled: true,
    reason: 'invalid_grant',
    disabledAt: Date.now(),
    error: String(err?.response?.data?.error_description || err?.message || 'invalid_grant').slice(0, 500),
  });

  console.error(`❌ Google Contacts deshabilitado para ${target.email}: refresh token inválido o vencido. Generá un refresh token nuevo y volvé a iniciar el servicio.`);
}

function isGoogleContactsTargetDisabled(target) {
  const key = getGoogleContactsTargetKey(target);
  if (!key) return false;
  return !!googleContactsRuntimeState.get(key)?.disabled;
}

function getGoogleContactsRuntimeSummary() {
  return GOOGLE_CONTACTS_TARGETS.map((target) => {
    const key = getGoogleContactsTargetKey(target);
    const state = googleContactsRuntimeState.get(key) || {};
    return {
      email: target.email,
      disabled: !!state.disabled,
      reason: state.reason || '',
      disabledAt: state.disabledAt || null,
      error: state.error || '',
    };
  });
}

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
      appointment_id BIGINT,
      client_name TEXT,
      contact_phone TEXT,
      service_name TEXT,
      service_notes TEXT,
      appointment_date DATE,
      appointment_time TIME,
      duration_min INTEGER NOT NULL DEFAULT 60,
      wants_color_confirmation BOOLEAN NOT NULL DEFAULT FALSE,
      availability_mode TEXT NOT NULL DEFAULT 'commercial',
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

  await db.query(`ALTER TABLE appointment_drafts ADD COLUMN IF NOT EXISTS appointment_id BIGINT`);
  await db.query(`ALTER TABLE appointment_drafts ADD COLUMN IF NOT EXISTS flow_step TEXT`);
  await db.query(`ALTER TABLE appointment_drafts ADD COLUMN IF NOT EXISTS last_intent TEXT`);
  await db.query(`ALTER TABLE appointment_drafts ADD COLUMN IF NOT EXISTS last_service_name TEXT`);
  await db.query(`ALTER TABLE appointment_drafts ADD COLUMN IF NOT EXISTS availability_mode TEXT`);
  await db.query(`UPDATE appointment_drafts SET availability_mode = 'commercial' WHERE availability_mode IS NULL OR TRIM(availability_mode) = ''`);

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
      availability_mode TEXT NOT NULL DEFAULT 'commercial',
      stylist_notified_at TIMESTAMPTZ,
      stylist_decision_at TIMESTAMPTZ,
      stylist_decision_note TEXT,
      reminder_client_24h_at TIMESTAMPTZ,
      reminder_client_2h_at TIMESTAMPTZ,
      reminder_stylist_24h_at TIMESTAMPTZ,
      reminder_stylist_2h_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  await db.query(`ALTER TABLE appointments ADD COLUMN IF NOT EXISTS availability_mode TEXT`);
  await db.query(`ALTER TABLE appointments ADD COLUMN IF NOT EXISTS stylist_decision_at TIMESTAMPTZ`);
  await db.query(`ALTER TABLE appointments ADD COLUMN IF NOT EXISTS stylist_decision_note TEXT`);
  await db.query(`UPDATE appointments SET availability_mode = 'commercial' WHERE availability_mode IS NULL OR TRIM(availability_mode) = ''`);

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


async function ensureCourseEnrollmentTables() {
  await db.query(`
    CREATE TABLE IF NOT EXISTS course_enrollment_drafts (
      wa_id TEXT PRIMARY KEY,
      wa_phone TEXT NOT NULL,
      course_name TEXT,
      course_category TEXT,
      student_name TEXT,
      student_dni TEXT,
      contact_phone TEXT,
      payment_status TEXT NOT NULL DEFAULT 'not_paid',
      payment_amount NUMERIC(10,2),
      payment_sender TEXT,
      payment_receiver TEXT,
      payment_proof_text TEXT,
      payment_proof_media_id TEXT,
      payment_proof_filename TEXT,
      flow_step TEXT,
      last_intent TEXT,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  await db.query(`ALTER TABLE course_enrollment_drafts ADD COLUMN IF NOT EXISTS course_category TEXT`);
  await db.query(`ALTER TABLE course_enrollment_drafts ADD COLUMN IF NOT EXISTS student_name TEXT`);
  await db.query(`ALTER TABLE course_enrollment_drafts ADD COLUMN IF NOT EXISTS student_dni TEXT`);
  await db.query(`ALTER TABLE course_enrollment_drafts ADD COLUMN IF NOT EXISTS contact_phone TEXT`);
  await db.query(`ALTER TABLE course_enrollment_drafts ADD COLUMN IF NOT EXISTS payment_status TEXT`);
  await db.query(`ALTER TABLE course_enrollment_drafts ADD COLUMN IF NOT EXISTS payment_amount NUMERIC(10,2)`);
  await db.query(`ALTER TABLE course_enrollment_drafts ADD COLUMN IF NOT EXISTS payment_sender TEXT`);
  await db.query(`ALTER TABLE course_enrollment_drafts ADD COLUMN IF NOT EXISTS payment_receiver TEXT`);
  await db.query(`ALTER TABLE course_enrollment_drafts ADD COLUMN IF NOT EXISTS payment_proof_text TEXT`);
  await db.query(`ALTER TABLE course_enrollment_drafts ADD COLUMN IF NOT EXISTS payment_proof_media_id TEXT`);
  await db.query(`ALTER TABLE course_enrollment_drafts ADD COLUMN IF NOT EXISTS payment_proof_filename TEXT`);
  await db.query(`ALTER TABLE course_enrollment_drafts ADD COLUMN IF NOT EXISTS flow_step TEXT`);
  await db.query(`ALTER TABLE course_enrollment_drafts ADD COLUMN IF NOT EXISTS last_intent TEXT`);

  await db.query(`
    CREATE TABLE IF NOT EXISTS course_enrollments (
      id BIGSERIAL PRIMARY KEY,
      wa_id TEXT NOT NULL,
      wa_phone TEXT NOT NULL,
      course_name TEXT NOT NULL,
      course_category TEXT,
      student_name TEXT,
      student_dni TEXT,
      contact_phone TEXT,
      status TEXT NOT NULL DEFAULT 'reserved',
      payment_status TEXT NOT NULL DEFAULT 'paid_verified',
      payment_amount NUMERIC(10,2),
      payment_sender TEXT,
      payment_receiver TEXT,
      payment_proof_text TEXT,
      payment_proof_media_id TEXT,
      payment_proof_filename TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  await db.query(`ALTER TABLE course_enrollments ADD COLUMN IF NOT EXISTS course_category TEXT`);
  await db.query(`ALTER TABLE course_enrollments ADD COLUMN IF NOT EXISTS student_name TEXT`);
  await db.query(`ALTER TABLE course_enrollments ADD COLUMN IF NOT EXISTS student_dni TEXT`);
  await db.query(`ALTER TABLE course_enrollments ADD COLUMN IF NOT EXISTS contact_phone TEXT`);
  await db.query(`ALTER TABLE course_enrollments ADD COLUMN IF NOT EXISTS status TEXT`);
  await db.query(`ALTER TABLE course_enrollments ADD COLUMN IF NOT EXISTS payment_status TEXT`);
  await db.query(`ALTER TABLE course_enrollments ADD COLUMN IF NOT EXISTS payment_amount NUMERIC(10,2)`);
  await db.query(`ALTER TABLE course_enrollments ADD COLUMN IF NOT EXISTS payment_sender TEXT`);
  await db.query(`ALTER TABLE course_enrollments ADD COLUMN IF NOT EXISTS payment_receiver TEXT`);
  await db.query(`ALTER TABLE course_enrollments ADD COLUMN IF NOT EXISTS payment_proof_text TEXT`);
  await db.query(`ALTER TABLE course_enrollments ADD COLUMN IF NOT EXISTS payment_proof_media_id TEXT`);
  await db.query(`ALTER TABLE course_enrollments ADD COLUMN IF NOT EXISTS payment_proof_filename TEXT`);

  await db.query(`
    CREATE TABLE IF NOT EXISTS course_enrollment_notifications (
      id BIGSERIAL PRIMARY KEY,
      enrollment_id BIGINT NOT NULL REFERENCES course_enrollments(id) ON DELETE CASCADE,
      notification_type TEXT NOT NULL,
      recipient_phone TEXT NOT NULL,
      template_name TEXT,
      wa_message_id TEXT,
      payload JSONB,
      sent_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      approved_at TIMESTAMPTZ,
      approved_by_phone TEXT,
      approval_text TEXT,
      expired_at TIMESTAMPTZ,
      expired_reason TEXT
    )
  `);

  await db.query(`ALTER TABLE course_enrollment_notifications ADD COLUMN IF NOT EXISTS approved_at TIMESTAMPTZ`);
  await db.query(`ALTER TABLE course_enrollment_notifications ADD COLUMN IF NOT EXISTS approved_by_phone TEXT`);
  await db.query(`ALTER TABLE course_enrollment_notifications ADD COLUMN IF NOT EXISTS approval_text TEXT`);
  await db.query(`ALTER TABLE course_enrollment_notifications ADD COLUMN IF NOT EXISTS approved_student_notified_at TIMESTAMPTZ`);
  await db.query(`ALTER TABLE course_enrollment_notifications ADD COLUMN IF NOT EXISTS expired_at TIMESTAMPTZ`);
  await db.query(`ALTER TABLE course_enrollment_notifications ADD COLUMN IF NOT EXISTS expired_reason TEXT`);
}


async function ensureCommercialFollowupTables() {
  await db.query(`
    CREATE TABLE IF NOT EXISTS commercial_followups (
      wa_id TEXT PRIMARY KEY,
      wa_phone TEXT NOT NULL,
      phone_raw TEXT,
      profile_name TEXT,
      explicit_name TEXT,
      followup_kind TEXT NOT NULL,
      context_json JSONB NOT NULL DEFAULT '{}'::jsonb,
      last_inbound_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      send_after_at TIMESTAMPTZ NOT NULL,
      status TEXT NOT NULL DEFAULT 'pending',
      sent_at TIMESTAMPTZ,
      last_error TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  await db.query(`
    CREATE TABLE IF NOT EXISTS commercial_followup_logs (
      id BIGSERIAL PRIMARY KEY,
      wa_id TEXT NOT NULL,
      wa_phone TEXT NOT NULL,
      followup_kind TEXT NOT NULL,
      message_text TEXT,
      status TEXT NOT NULL,
      error_text TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
}


async function ensureBirthdayMessageTables() {
  await db.query(`
    CREATE TABLE IF NOT EXISTS birthday_message_logs (
      id BIGSERIAL PRIMARY KEY,
      hubspot_contact_id TEXT NOT NULL,
      wa_phone TEXT NOT NULL,
      sent_date DATE NOT NULL,
      contact_name TEXT,
      birthday_value TEXT,
      message_text TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE (hubspot_contact_id, sent_date)
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
    'precio', 'info', 'informacion', 'gracias', 'te', 'me', 'mi', 'soy', 'que', 'por', 'para', 'necesito', 'busco',
    'botox', 'keratina', 'color', 'lavado', 'peinado', 'brushing', 'mascara', 'máscara', 'serum', 'sérum',
    'ampolla', 'ampollas', 'acondicionador', 'masterclass', 'tratamiento', 'alisar', 'matiz', 'matizador',
    'barberia', 'barbería', 'maquillaje', 'rubio', 'rubia', 'decolorante', 'mechas', 'secado'
  ]);

  const normalizedParts = parts.map((p) => normalize(p));
  if (normalizedParts.some((p) => banned.has(p))) return '';
  if (parts.some((p) => p.length < 2)) return '';

  const t = normalize(txt);
  if (/(alisado|tintura|corte|unas|uñas|depil|pestan|pestañ|ceja|curso|mueble|shampoo|matizador|nutricion|nutrición|bano de crema|baño de crema|camilla|silla|mesa|barber|consult|precio|gracias|hola|buen dia|buen día|necesito|busco|quisiera|ampolla|ampollas|botox|keratina|mascara|máscara|serum|sérum|lavado|peinado|brushing|color|masterclass|tratamiento|acondicionador|decolorante|rubio|mechas|secado)/i.test(t)) {
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
  if (/(interesad|estimad|servicio|producto|curso|turno|alisado|depilacion|depilación|mechita|mechas|tintura|corte|cliente salon|cliente salon de belleza|consult|pregunt|hola|buen dia|buen día|gracias|quisiera|quiero|ampolla|ampollas|botox|keratina|mascara|máscara|serum|sérum|lavado|peinado|brushing|color|masterclass|tratamiento|acondicionador|decolorante|rubio|secado)/i.test(t)) return true;
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
      HUBSPOT_PROPERTY.fechaNacimiento,
      HUBSPOT_STUDENT_PROPERTY,
      HUBSPOT_STUDENT_COURSES_PROPERTY,
      'mobilephone',
    ].filter(Boolean),
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

async function buildHubSpotContactProperties(ctx, existingContact, { includeConversationSummary = true } = {}) {
  const existing = existingContact?.properties || {};
  const conversationSnippet = includeConversationSummary ? getConversationSnippetForClose(ctx?.waId || '') : '';
  const focusText = includeConversationSummary ? [ctx.interest, ctx.lastUserText].filter(Boolean).join(' | ') : '';
  const combinedText = [conversationSnippet, focusText].filter(Boolean).join(' | ');
  const categoriasFound = includeConversationSummary
    ? pickCategorias({ intentType: ctx.intentType || 'OTHER', text: combinedText })
    : [];
  const primaryProduct = includeConversationSummary ? pickPrimaryProducto(focusText || conversationSnippet || '') : '';
  const productosFound = primaryProduct ? [primaryProduct] : [];
  const aiAnalysis = includeConversationSummary
    ? await analyzeCloseSummaryWithOpenAI({ ctx, conversationSnippet, productos: productosFound, categorias: categoriasFound })
    : null;
  const finalPrimaryProduct = cleanProductLabel(aiAnalysis?.productoPrincipal || '') || primaryProduct;
  const finalProductos = finalPrimaryProduct ? [finalPrimaryProduct] : [];
  const observacionLine = includeConversationSummary
    ? (aiAnalysis?.observacion || buildMiniObservacion({ text: combinedText, productos: finalProductos, categorias: categoriasFound }))
    : '';
  const observacion = includeConversationSummary
    ? mergeObservationHistory(existing?.[HUBSPOT_PROPERTY.observacion] || '', observacionLine)
    : (existing?.[HUBSPOT_PROPERTY.observacion] || '');

  const explicitFromSnippet = includeConversationSummary ? extractExplicitNameFromSnippet(conversationSnippet) : '';
  const forcedExplicitName = cleanNameCandidate(ctx.forceContactName || ctx.explicitName || '') || '';
  const chosenName = chooseBestContactName({
    existingName: existing?.[HUBSPOT_PROPERTY.firstname] || '',
    existingLastName: existing?.[HUBSPOT_PROPERTY.lastname] || '',
    explicitName: forcedExplicitName || (includeConversationSummary ? (aiAnalysis?.explicitName || explicitFromSnippet || '') : ''),
    profileName: ctx.profileName || ctx.name || '',
  });

  const normalizedPhone = normalizeHubSpotPhone(ctx.phoneRaw || ctx.phone || '');
  const rawPhone = String(ctx.phoneRaw || ctx.phone || '').trim();
  const now = new Date();

  const mergedCategoria = includeConversationSummary
    ? mergeHubSpotMulti(existing?.[HUBSPOT_PROPERTY.categoria] || '', categoriasFound)
    : (existing?.[HUBSPOT_PROPERTY.categoria] || '');
  const mergedProducto = includeConversationSummary
    ? mergeSlashText(existing?.[HUBSPOT_PROPERTY.producto] || '', finalProductos, 8)
    : (existing?.[HUBSPOT_PROPERTY.producto] || '');
  const existingFullName = [existing?.[HUBSPOT_PROPERTY.firstname], existing?.[HUBSPOT_PROPERTY.lastname]].filter(Boolean).join(' ').trim();
  const existingIsGeneric = isLikelyGenericContactName(existingFullName);
  const shouldForceContactName = !!forcedExplicitName && !!chosenName.fullName;
  const shouldOverwriteExistingName = !!chosenName.fullName && (existingIsGeneric || normalize(existingFullName) !== normalize(chosenName.fullName));

  const properties = {
    [HUBSPOT_PROPERTY.phone]: normalizedPhone || rawPhone || existing?.[HUBSPOT_PROPERTY.phone] || '',
    [HUBSPOT_PROPERTY.ultimoContacto]: hubspotDateValue(now),
    [HUBSPOT_PROPERTY.empresa]: existing?.[HUBSPOT_PROPERTY.empresa] || HUBSPOT_OPTION.empresaCataleya,
    [HUBSPOT_PROPERTY.whatsappContact]: 'true',
    [HUBSPOT_PROPERTY.whatsappWaId]: ctx.waId || existing?.[HUBSPOT_PROPERTY.whatsappWaId] || '',
    [HUBSPOT_PROPERTY.whatsappPhoneRaw]: rawPhone || existing?.[HUBSPOT_PROPERTY.whatsappPhoneRaw] || '',
    [HUBSPOT_PROPERTY.whatsappPhoneNormalized]: normalizedPhone || existing?.[HUBSPOT_PROPERTY.whatsappPhoneNormalized] || '',
    [HUBSPOT_PROPERTY.whatsappProfileName]: cleanNameCandidate(ctx.profileName || ctx.name || '') || existing?.[HUBSPOT_PROPERTY.whatsappProfileName] || '',
  };

  if (includeConversationSummary) {
    properties[HUBSPOT_PROPERTY.observacion] = observacion;
    properties[HUBSPOT_PROPERTY.producto] = mergedProducto || existing?.[HUBSPOT_PROPERTY.producto] || '';
    properties[HUBSPOT_PROPERTY.categoria] = mergedCategoria || existing?.[HUBSPOT_PROPERTY.categoria] || '';
  }

  if (chosenName.firstName && (shouldForceContactName || chosenName.source === 'chat_explicit' || chosenName.source === 'whatsapp_profile' || shouldOverwriteExistingName)) {
    properties[HUBSPOT_PROPERTY.firstname] = chosenName.firstName;
    if (HUBSPOT_PROPERTY.lastname) {
      properties[HUBSPOT_PROPERTY.lastname] = chosenName.lastName || '';
    }
    properties[HUBSPOT_PROPERTY.nameSource] = shouldForceContactName ? 'chat_explicit' : (chosenName.source || existing?.[HUBSPOT_PROPERTY.nameSource] || '');
    properties[HUBSPOT_PROPERTY.nameUpdatedAt] = hubspotDateTimeValue(now);
  }

  if (!existing?.[HUBSPOT_PROPERTY.fechaIngresoBase]) {
    properties[HUBSPOT_PROPERTY.fechaIngresoBase] = hubspotDateValue(now);
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

  const properties = await buildHubSpotContactProperties(ctx, contact, { includeConversationSummary: true });

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

async function upsertHubSpotContactIdentityOnly(ctx) {
  if (!hasHubSpotEnabled()) return null;

  const phoneRaw = String(ctx?.phoneRaw || ctx?.phone || '').trim();
  const waId = String(ctx?.waId || '').trim();
  if (!phoneRaw && !waId) return null;

  let contact = null;
  if (waId) contact = await findHubSpotContactByWaId(waId);
  if (!contact && phoneRaw) {
    const phoneMatches = await findHubSpotContactsByPhone(phoneRaw);
    contact = chooseBestHubSpotMatch(phoneMatches, phoneRaw);
  }

  const properties = await buildHubSpotContactProperties(ctx, contact, { includeConversationSummary: false });

  try {
    if (contact?.id) {
      await hubspotRequest('patch', `/crm/v3/objects/contacts/${contact.id}`, { properties });
      return { action: 'updated', id: contact.id };
    }

    const created = await hubspotRequest('post', '/crm/v3/objects/contacts', { properties });
    return { action: 'created', id: created?.id || '' };
  } catch (e) {
    console.error('❌ Error sincronizando identidad en HubSpot:', e?.response?.data || e?.message || e);
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


// ===================== DIFUSIÓN DIARIA (EXCEL -> COLA -> ENVÍO) =====================
const BROADCAST_MESSAGES = [
  '{saludo} {nombre} 😊 Desde Cataleya queríamos avisarte que seguimos con lugares disponibles en nuestros talleres. Si te interesa, te paso la info.',
  '{saludo} {nombre} 💛 Te escribimos desde Cataleya porque esta semana seguimos tomando inscripciones para nuestros talleres. Si querés, te cuento cuáles están disponibles.',
  '{saludo} {nombre} ✨ Estamos organizando nuevas inscripciones en Cataleya para talleres de peinados, trenzas africanas y pinta caritas. Si querés avanzar, te paso toda la info.',
  '{saludo} {nombre} 😊 Desde Cataleya queríamos contarte que seguimos con cupos para algunos talleres. Si te interesa, te digo cuáles quedan disponibles.',
  '{saludo} {nombre} 💛 Te escribimos desde Cataleya porque aún hay lugares en algunos de nuestros talleres. Si querés, te cuento opciones y cómo reservar.',
  '{saludo} {nombre} 😊 Queríamos avisarte que en Cataleya seguimos con inscripciones abiertas en talleres seleccionados. Si te interesa, te paso más información.',
  '{saludo} {nombre} ✨ Desde Cataleya queríamos acercarte esta info: todavía hay cupos en algunos talleres y si querés te orientamos según lo que más te interese.',
  '{saludo} {nombre} 😊 Seguimos sumando inscripciones en Cataleya para distintos talleres. Si querés, te paso la propuesta disponible en este momento.',
  '{saludo} {nombre} 💛 Te escribimos desde Cataleya porque quizá te interese saber que todavía hay algunos cupos en talleres. Si querés, te cuento.',
  '{saludo} {nombre} 😊 Desde Cataleya queríamos consultarte si seguís interesada en nuestros talleres. Si te interesa, te paso la info actualizada.',
  '{saludo} {nombre} ✨ En Cataleya seguimos con consultas e inscripciones para talleres de peinados, trenzas africanas y pinta caritas. Si querés, te explico cómo sería.',
  '{saludo} {nombre} 😊 Te escribimos desde Cataleya porque todavía tenemos lugares en algunos talleres. Si te interesa, te paso toda la información sin compromiso.',
  '{saludo} {nombre} 💛 Queríamos avisarte que en Cataleya siguen disponibles algunos talleres y si querés te puedo orientar con el que más te convenga.',
  '{saludo} {nombre} 😊 Desde Cataleya seguimos con cupos en talleres seleccionados. Si te interesa, te paso cuáles están disponibles y cómo reservar lugar.',
  '{saludo} {nombre} ✨ Te escribimos porque en Cataleya todavía quedan lugares en algunos talleres. Si querés, te paso info y opciones para avanzar.',
  '{saludo} {nombre} 😊 Queríamos comentarte que en Cataleya seguimos con inscripciones para talleres. Si querés, te paso lo disponible y te explico.',
  '{saludo} {nombre} 💛 Desde Cataleya queríamos acercarte esta propuesta porque aún hay cupos para algunos talleres. Si te interesa, te digo cuáles.',
  '{saludo} {nombre} 😊 Estamos organizando nuevas reservas en Cataleya para talleres seleccionados. Si te interesa, te paso la info actual.',
  '{saludo} {nombre} ✨ Te escribimos desde Cataleya porque todavía hay posibilidades de sumarte a algunos talleres. Si querés, te cuento.',
  '{saludo} {nombre} 😊 Seguimos con algunos lugares disponibles en talleres de Cataleya. Si te interesa, te explico cuáles están abiertos ahora.',
  '{saludo} {nombre} 💛 Desde Cataleya queríamos avisarte que todavía estás a tiempo de consultar por algunos talleres. Si querés, te paso información.',
  '{saludo} {nombre} 😊 Te escribimos desde Cataleya porque siguen abiertas algunas inscripciones. Si te interesa, te cuento cómo podés avanzar.',
  '{saludo} {nombre} ✨ En Cataleya seguimos con propuestas de talleres y todavía hay cupos en algunos casos. Si querés, te digo cuáles quedan.',
  '{saludo} {nombre} 😊 Queríamos acercarte esta info desde Cataleya: aún hay talleres con lugar disponible. Si te interesa, te paso el detalle.',
  '{saludo} {nombre} 💛 Desde Cataleya seguimos tomando consultas para talleres. Si te interesa, te puedo pasar la información ahora mismo.',
  '{saludo} {nombre} 😊 Te escribimos desde Cataleya porque quizás todavía te interese sumarte a alguno de nuestros talleres. Si querés, te paso la info.',
  '{saludo} {nombre} ✨ En Cataleya todavía tenemos cupos en algunos talleres. Si te interesa, te cuento opciones y cómo reservar.',
  '{saludo} {nombre} 😊 Seguimos con inscripciones abiertas en Cataleya para talleres seleccionados. Si te interesa, te paso todo por acá.',
  '{saludo} {nombre} 💛 Desde Cataleya queríamos contarte que aún hay algunos talleres con lugar disponible. Si querés, te paso la info actualizada.',
  '{saludo} {nombre} 😊 Te escribimos desde Cataleya porque seguimos con consultas e inscripciones para talleres. Si te interesa, te explico cómo sería.',
];
const broadcastAiNameCache = new Map();

function broadcastNormalizeHeader(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '');
}

function broadcastCleanText(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function broadcastFirstName(value) {
  const txt = broadcastCleanText(value);
  if (!txt) return '';
  return txt.split(' ')[0] || '';
}

function broadcastGreetingForDate(date = new Date()) {
  const parts = broadcastLocalDateParts(date, -180);
  if (parts.hour < 13) return 'Buen día';
  if (parts.hour < 20) return 'Buenas tardes';
  return 'Buenas noches';
}

function broadcastCleanupRenderedMessage(value = '', saludo = '') {
  let out = String(value || '');
  out = out.replace(/[ \t]+/g, ' ');
  out = out.replace(/\s+([,.;:!?])/g, '$1');
  out = out.replace(/([¡¿])\s+/g, '$1');
  out = out.replace(/,\s*,+/g, ', ');
  out = out.replace(/\n{3,}/g, '\n\n');
  if (saludo) {
    const escaped = saludo.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    out = out.replace(new RegExp(`(${escaped})\s*,`, 'gi'), '$1');
  }
  out = out.replace(/\b(Hola|Buenas|Buen día|Buenas tardes|Buenas noches)\s+(?=[,.;:!?]|$)/gi, '$1');
  out = out.replace(/\s{2,}/g, ' ').trim();
  out = out.replace(/^,\s*/, '').trim();
  return out;
}

async function broadcastDetectRealNameWithAI(rawCandidate = '') {
  const raw = broadcastCleanText(rawCandidate);
  if (!raw) return '';
  const cacheKey = normalize(raw);
  if (broadcastAiNameCache.has(cacheKey)) return broadcastAiNameCache.get(cacheKey) || '';

  let resolved = '';
  const cleaned = cleanNameCandidate(raw);
  if (cleaned && !isLikelyGenericContactName(cleaned)) {
    resolved = cleaned;
  } else {
    try {
      const completion = await openai.chat.completions.create({
        model: PRIMARY_MODEL || 'gpt-4.1-mini',
        temperature: 0,
        messages: [
          {
            role: 'system',
            content:
`Analizá un texto corto que podría ser el nombre de una persona en WhatsApp.
Devolvé SOLO JSON válido con estas claves:
- is_real_name: boolean
- normalized_name: string

Reglas:
- normalized_name debe contener solo un nombre real natural, con mayúsculas correctas.
- Si el texto es genérico, comercial, un saludo, una frase, un apodo no claro, o algo como "cliente", "beauty", "ventas", "info", "Cataleya", entonces is_real_name=false.
- No inventes nombres.
- No devuelvas explicación.`
          },
          {
            role: 'user',
            content: JSON.stringify({ candidate: raw.slice(0, 80) }),
          }
        ],
        response_format: { type: 'json_object' },
      });
      const parsed = JSON.parse(completion.choices?.[0]?.message?.content || '{}');
      const aiName = cleanNameCandidate(parsed?.normalized_name || '');
      if (parsed?.is_real_name && aiName && !isLikelyGenericContactName(aiName)) {
        resolved = aiName;
      }
    } catch {}
  }

  broadcastAiNameCache.set(cacheKey, resolved || '');
  return resolved || '';
}

async function broadcastResolveDisplayNameForRow(row = {}) {
  const directCandidates = [
    row.contact_name,
    row.profile_name,
  ].map((x) => broadcastCleanText(x)).filter(Boolean);

  for (const candidate of directCandidates) {
    const clean = cleanNameCandidate(candidate);
    if (clean && !isLikelyGenericContactName(clean)) {
      return broadcastFirstName(clean);
    }
  }

  try {
    const knownIdentity = await resolveKnownContactIdentity({
      waId: broadcastCleanText(row.wa_id || row.wa_phone || ''),
      phoneRaw: broadcastCleanText(row.wa_phone || ''),
      profileName: broadcastCleanText(row.profile_name || row.contact_name || ''),
    });
    const known = cleanNameCandidate(knownIdentity?.bestName || '');
    if (known && !isLikelyGenericContactName(known)) {
      return broadcastFirstName(known);
    }
  } catch {}

  const aiCandidate = directCandidates[0] || '';
  if (aiCandidate) {
    const aiName = await broadcastDetectRealNameWithAI(aiCandidate);
    if (aiName) return broadcastFirstName(aiName);
  }

  return '';
}
function broadcastSafeJson(value, fallback) {
  try {
    return JSON.stringify(value ?? fallback ?? []);
  } catch {
    return JSON.stringify(fallback ?? []);
  }
}

function broadcastParseJsonArray(value) {
  if (!value) return [];
  try {
    const parsed = typeof value === 'string' ? JSON.parse(value) : value;
    return Array.isArray(parsed) ? parsed.map((x) => broadcastCleanText(x)).filter(Boolean) : [];
  } catch {
    return [];
  }
}

function broadcastParseJsonAnyArray(value) {
  if (!value) return [];
  try {
    const parsed = typeof value === 'string' ? JSON.parse(value) : value;
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

function broadcastParseMessagesInput(value = '') {
  return String(value || '')
    .split(/\r?\n/)
    .map((x) => broadcastCleanText(x))
    .filter(Boolean);
}

function broadcastLocalDateParts(date = new Date(), offsetMinutes = -180) {
  const shifted = new Date(date.getTime() + offsetMinutes * 60000);
  return {
    year: shifted.getUTCFullYear(),
    month: shifted.getUTCMonth() + 1,
    day: shifted.getUTCDate(),
    hour: shifted.getUTCHours(),
    minute: shifted.getUTCMinutes(),
  };
}

function broadcastFormatYMD(parts) {
  const mm = String(parts.month).padStart(2, '0');
  const dd = String(parts.day).padStart(2, '0');
  return `${parts.year}-${mm}-${dd}`;
}

function broadcastTodayYMD() {
  return broadcastFormatYMD(broadcastLocalDateParts(new Date(), -180));
}

function broadcastFormatValidUntilDisplay(value = '') {
  const raw = String(value || '').trim();
  if (!raw) return '';
  const dt = new Date(raw);
  if (Number.isNaN(dt.getTime())) return raw;
  try {
    return new Intl.DateTimeFormat('es-AR', {
      timeZone: TIMEZONE,
      year: 'numeric', month: '2-digit', day: '2-digit',
      hour: '2-digit', minute: '2-digit', hour12: false,
    }).format(dt);
  } catch {
    const parts = broadcastLocalDateParts(dt, -180);
    return `${String(parts.day).padStart(2,'0')}/${String(parts.month).padStart(2,'0')}/${parts.year} ${String(parts.hour).padStart(2,'0')}:${String(parts.minute).padStart(2,'0')}`;
  }
}

function broadcastFormatValidUntilInput(value = '') {
  const raw = String(value || '').trim();
  if (!raw) return '';
  const dt = new Date(raw);
  if (Number.isNaN(dt.getTime())) return '';
  const parts = broadcastLocalDateParts(dt, -180);
  return `${parts.year}-${String(parts.month).padStart(2,'0')}-${String(parts.day).padStart(2,'0')}T${String(parts.hour).padStart(2,'0')}:${String(parts.minute).padStart(2,'0')}`;
}

function broadcastParseValidUntilInput(value = '') {
  const raw = String(value || '').trim();
  if (!raw) return null;
  if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$/.test(raw)) {
    const [datePart, timePart] = raw.split('T');
    const [year, month, day] = datePart.split('-').map(Number);
    const [hour, minute] = timePart.split(':').map(Number);
    const dt = broadcastUtcDateFromLocalYMDHM(`${year}-${String(month).padStart(2,'0')}-${String(day).padStart(2,'0')}`, hour, minute, -180);
    return Number.isNaN(dt.getTime()) ? null : dt.toISOString();
  }
  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    const dt = broadcastUtcDateFromLocalYMDHM(raw, 23, 59, -180);
    return Number.isNaN(dt.getTime()) ? null : dt.toISOString();
  }
  const dmy = raw.match(/^(\d{2})\/(\d{2})\/(\d{4})(?:\s+(\d{2}):(\d{2}))?$/);
  if (dmy) {
    const [day, month, year] = [Number(dmy[1]), Number(dmy[2]), Number(dmy[3])];
    const hour = dmy[4] != null ? Number(dmy[4]) : 23;
    const minute = dmy[5] != null ? Number(dmy[5]) : 59;
    const ymd = `${year}-${String(month).padStart(2,'0')}-${String(day).padStart(2,'0')}`;
    const dt = broadcastUtcDateFromLocalYMDHM(ymd, hour, minute, -180);
    return Number.isNaN(dt.getTime()) ? null : dt.toISOString();
  }
  const dt = new Date(raw);
  return Number.isNaN(dt.getTime()) ? null : dt.toISOString();
}

function broadcastParseYMD(ymd) {
  const m = String(ymd || '').match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!m) throw new Error(`Fecha inválida: ${ymd}`);
  return { year: Number(m[1]), month: Number(m[2]), day: Number(m[3]) };
}

function broadcastAddDaysYMD(ymd, days) {
  const base = broadcastParseYMD(ymd);
  const dt = new Date(Date.UTC(base.year, base.month - 1, base.day + Number(days || 0)));
  return broadcastFormatYMD({
    year: dt.getUTCFullYear(),
    month: dt.getUTCMonth() + 1,
    day: dt.getUTCDate(),
  });
}

function broadcastUtcDateFromLocalYMDHM(ymd, hour, minute, offsetMinutes = -180) {
  const parts = broadcastParseYMD(ymd);
  const utcMillis = Date.UTC(parts.year, parts.month - 1, parts.day, hour, minute, 0, 0) - (offsetMinutes * 60000);
  return new Date(utcMillis);
}

function broadcastParseHM(value) {
  const m = String(value || '').match(/^(\d{1,2}):(\d{2})$/);
  if (!m) throw new Error(`Hora inválida: ${value}`);
  return { hour: Number(m[1]), minute: Number(m[2]) };
}

function broadcastLocalMinutesOfDay(date = new Date()) {
  const parts = broadcastLocalDateParts(date, -180);
  return parts.hour * 60 + parts.minute;
}

function broadcastMaxWindowMinutes(windows = BROADCAST_WINDOWS) {
  return windows.reduce((max, row) => {
    const end = broadcastParseHM(row.end);
    return Math.max(max, end.hour * 60 + end.minute);
  }, 0);
}

function broadcastChooseStartYMD(startYMD = '') {
  if (startYMD) return startYMD;
  const today = broadcastTodayYMD();
  const nowLocalMinutes = broadcastLocalMinutesOfDay(new Date());
  return nowLocalMinutes > broadcastMaxWindowMinutes(BROADCAST_WINDOWS)
    ? broadcastAddDaysYMD(today, 1)
    : today;
}

function broadcastShuffle(array) {
  const out = array.slice();
  for (let i = out.length - 1; i > 0; i -= 1) {
    const j = Math.floor(Math.random() * (i + 1));
    [out[i], out[j]] = [out[j], out[i]];
  }
  return out;
}

function broadcastBuildCandidateTimesForDay(ymd, minDate = null) {
  const candidates = [];
  for (const row of BROADCAST_WINDOWS) {
    const start = broadcastParseHM(row.start);
    const end = broadcastParseHM(row.end);
    let m = start.hour * 60 + start.minute;
    const endMinutes = end.hour * 60 + end.minute;
    while (m <= endMinutes) {
      const hour = Math.floor(m / 60);
      const minute = m % 60;
      const dt = broadcastUtcDateFromLocalYMDHM(ymd, hour, minute, -180);
      if (!minDate || dt.getTime() > minDate.getTime()) candidates.push(dt);
      m += BROADCAST_SLOT_GRANULARITY_MINUTES;
    }
  }
  return candidates;
}

function broadcastPickRandomTimes(ymd, count, minDate = null) {
  if (count <= 0) return [];
  const candidates = broadcastBuildCandidateTimesForDay(ymd, minDate).sort((a, b) => a.getTime() - b.getTime());
  if (!candidates.length) return [];

  const target = Math.min(count, candidates.length);
  const minGapMs = BROADCAST_MIN_GAP_MINUTES * 60000;
  const picks = [];
  const usedIndexes = new Set();

  for (let i = 0; i < target; i += 1) {
    const startIdx = Math.floor((i * candidates.length) / target);
    let endIdx = Math.floor(((i + 1) * candidates.length) / target) - 1;
    if (endIdx < startIdx) endIdx = startIdx;

    const segmentIndexes = [];
    for (let idx = startIdx; idx <= endIdx; idx += 1) {
      if (usedIndexes.has(idx)) continue;
      const dt = candidates[idx];
      const tooClose = picks.some((picked) => Math.abs(dt.getTime() - picked.getTime()) < minGapMs);
      if (!tooClose) segmentIndexes.push(idx);
    }

    let chosenIdx = -1;
    if (segmentIndexes.length) {
      chosenIdx = segmentIndexes[Math.floor(Math.random() * segmentIndexes.length)];
    } else {
      const fallbackIndexes = [];
      for (let idx = 0; idx < candidates.length; idx += 1) {
        if (!usedIndexes.has(idx)) fallbackIndexes.push(idx);
      }
      if (!fallbackIndexes.length) break;
      chosenIdx = fallbackIndexes[Math.floor(Math.random() * fallbackIndexes.length)];
    }

    usedIndexes.add(chosenIdx);
    picks.push(candidates[chosenIdx]);
  }

  return picks.sort((a, b) => a.getTime() - b.getTime());
}

function broadcastRenderMessage(template, row = {}) {
  const saludo = broadcastGreetingForDate(new Date());
  const name = broadcastFirstName(cleanNameCandidate(row.contact_name || row.profile_name || ''));
  let out = String(template || '');
  out = out.replace(/\{saludo\}/gi, saludo);
  out = out.replace(/\{nombre\}/gi, name);
  return broadcastCleanupRenderedMessage(out, saludo);
}

async function broadcastRenderMessageForSend(template, row = {}) {
  const saludo = broadcastGreetingForDate(new Date());
  const name = await broadcastResolveDisplayNameForRow(row);
  let out = String(template || '');
  out = out.replace(/\{saludo\}/gi, saludo);
  out = out.replace(/\{nombre\}/gi, name);
  return broadcastCleanupRenderedMessage(out, saludo);
}

function broadcastDefaultOfferFromRow(row = {}) {
  const type = broadcastCleanText(row.offer_type || '').toUpperCase();
  const selected = broadcastCleanText(row.offer_selected_name || '');
  const items = broadcastParseJsonArray(row.offer_items_json || row.offer_items || []);
  if (!items.length && selected) items.push(selected);
  return { type, selectedName: selected, items };
}

function broadcastCanOperate() {
  if (!ENABLE_DAILY_BROADCAST) return false;
  return !!XLSX;
}

function broadcastExcelExists(excelPath = BROADCAST_EXCEL_PATH) {
  const target = String(excelPath || '').trim();
  if (!target) return false;
  try {
    return fs.existsSync(target);
  } catch {
    return false;
  }
}

async function ensureBroadcastTables() {
  await db.query(`
    CREATE TABLE IF NOT EXISTS broadcast_campaigns (
      id BIGSERIAL PRIMARY KEY,
      campaign_name TEXT NOT NULL UNIQUE,
      source_file TEXT,
      daily_pattern_json JSONB NOT NULL DEFAULT '[30,15,10]'::jsonb,
      timezone TEXT NOT NULL DEFAULT 'America/Argentina/Salta',
      windows_json JSONB NOT NULL DEFAULT '[]'::jsonb,
      messages_json JSONB NOT NULL DEFAULT '[]'::jsonb,
      campaign_type TEXT NOT NULL DEFAULT 'OTHER',
      ai_context TEXT,
      ai_response_style TEXT,
      ai_guardrails TEXT,
      ai_cta TEXT,
      valid_until TIMESTAMPTZ,
      assets_json JSONB NOT NULL DEFAULT '[]'::jsonb,
      is_active BOOLEAN NOT NULL DEFAULT TRUE,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  await db.query(`
    CREATE TABLE IF NOT EXISTS broadcast_queue (
      id BIGSERIAL PRIMARY KEY,
      campaign_name TEXT NOT NULL,
      source_file TEXT,
      source_row INTEGER,
      contact_name TEXT,
      profile_name TEXT,
      wa_phone TEXT NOT NULL,
      wa_id TEXT,
      custom_message TEXT,
      message_index INTEGER,
      message_text TEXT,
      wa_message_id TEXT,
      delivery_status TEXT,
      offer_type TEXT,
      offer_items_json JSONB NOT NULL DEFAULT '[]'::jsonb,
      offer_selected_name TEXT,
      schedule_day DATE,
      send_at TIMESTAMPTZ,
      status TEXT NOT NULL DEFAULT 'pending',
      attempts INTEGER NOT NULL DEFAULT 0,
      api_accepted_at TIMESTAMPTZ,
      sent_at TIMESTAMPTZ,
      delivered_at TIMESTAMPTZ,
      read_at TIMESTAMPTZ,
      last_error TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      CONSTRAINT broadcast_queue_status_chk CHECK (status IN ('pending','processing','sent','sent_api','delivered','read','error','skipped')),
      CONSTRAINT broadcast_queue_campaign_fk FOREIGN KEY (campaign_name) REFERENCES broadcast_campaigns(campaign_name) ON DELETE CASCADE,
      CONSTRAINT broadcast_queue_unique_contact UNIQUE (campaign_name, wa_phone)
    )
  `);

  await db.query(`ALTER TABLE broadcast_campaigns ADD COLUMN IF NOT EXISTS messages_json JSONB NOT NULL DEFAULT '[]'::jsonb`);
  await db.query(`ALTER TABLE broadcast_campaigns ADD COLUMN IF NOT EXISTS campaign_type TEXT NOT NULL DEFAULT 'OTHER'`);
  await db.query(`ALTER TABLE broadcast_campaigns ADD COLUMN IF NOT EXISTS ai_context TEXT`);
  await db.query(`ALTER TABLE broadcast_campaigns ADD COLUMN IF NOT EXISTS ai_response_style TEXT`);
  await db.query(`ALTER TABLE broadcast_campaigns ADD COLUMN IF NOT EXISTS ai_guardrails TEXT`);
  await db.query(`ALTER TABLE broadcast_campaigns ADD COLUMN IF NOT EXISTS ai_cta TEXT`);
  await db.query(`ALTER TABLE broadcast_campaigns ADD COLUMN IF NOT EXISTS valid_until TIMESTAMPTZ`);
  await db.query(`ALTER TABLE broadcast_campaigns ADD COLUMN IF NOT EXISTS assets_json JSONB NOT NULL DEFAULT '[]'::jsonb`);

  const validUntilType = await db.query(`
    SELECT data_type
      FROM information_schema.columns
     WHERE table_schema = 'public'
       AND table_name = 'broadcast_campaigns'
       AND column_name = 'valid_until'
     LIMIT 1`);
  const validUntilDataType = String(validUntilType.rows?.[0]?.data_type || '').toLowerCase();
  if (validUntilDataType && validUntilDataType !== 'timestamp with time zone') {
    await db.query(`ALTER TABLE broadcast_campaigns ADD COLUMN IF NOT EXISTS valid_until_tmp TIMESTAMPTZ`);
    await db.query(`
      UPDATE broadcast_campaigns
         SET valid_until_tmp = CASE
           WHEN valid_until IS NULL OR BTRIM(valid_until::text) = '' THEN NULL
           WHEN valid_until::text ~ '^\d{4}-\d{2}-\d{2}$' THEN (valid_until::text || ' 23:59:00-03')::timestamptz
           ELSE NULL
         END`);
    await db.query(`ALTER TABLE broadcast_campaigns DROP COLUMN valid_until`);
    await db.query(`ALTER TABLE broadcast_campaigns RENAME COLUMN valid_until_tmp TO valid_until`);
  }
  await db.query(`ALTER TABLE broadcast_queue ADD COLUMN IF NOT EXISTS wa_message_id TEXT`);
  await db.query(`ALTER TABLE broadcast_queue ADD COLUMN IF NOT EXISTS delivery_status TEXT`);
  await db.query(`ALTER TABLE broadcast_queue ADD COLUMN IF NOT EXISTS api_accepted_at TIMESTAMPTZ`);
  await db.query(`ALTER TABLE broadcast_queue ADD COLUMN IF NOT EXISTS delivered_at TIMESTAMPTZ`);
  await db.query(`ALTER TABLE broadcast_queue ADD COLUMN IF NOT EXISTS read_at TIMESTAMPTZ`);
  await db.query(`ALTER TABLE broadcast_queue DROP CONSTRAINT IF EXISTS broadcast_queue_status_chk`);
  await db.query(`
    ALTER TABLE broadcast_queue
    ADD CONSTRAINT broadcast_queue_status_chk
    CHECK (status IN ('pending','processing','sent','sent_api','delivered','read','error','skipped'))
  `);
  await db.query(`UPDATE broadcast_queue SET status = 'sent_api' WHERE status = 'sent'`);
  await db.query(`CREATE INDEX IF NOT EXISTS idx_broadcast_queue_pending_send_at ON broadcast_queue(status, send_at)`);
  await db.query(`CREATE INDEX IF NOT EXISTS idx_broadcast_queue_campaign_status ON broadcast_queue(campaign_name, status)`);
  await db.query(`CREATE INDEX IF NOT EXISTS idx_broadcast_queue_wa_message_id ON broadcast_queue(wa_message_id)`);
}

async function upsertBroadcastCampaign({
  campaignName = BROADCAST_CAMPAIGN_NAME,
  sourceFile = BROADCAST_EXCEL_PATH,
  pattern = BROADCAST_PATTERN_SAFE,
  messages = null,
  isActive = true,
  campaignType = null,
  aiContext = null,
  aiResponseStyle = null,
  aiGuardrails = null,
  aiCta = null,
  validUntil = null,
  assets = null,
} = {}) {
  const cleanCampaign = broadcastCleanText(campaignName) || BROADCAST_CAMPAIGN_NAME;
  const cleanMessages = Array.isArray(messages) ? messages.map((x) => broadcastCleanText(x)).filter(Boolean) : null;

  let finalIsActive = typeof isActive === 'boolean' ? isActive : true;
  if (typeof isActive !== 'boolean') {
    try {
      const existing = await db.query(
        `SELECT is_active
           FROM broadcast_campaigns
          WHERE campaign_name = $1
          LIMIT 1`,
        [cleanCampaign]
      );
      if (existing.rows?.length) finalIsActive = !!existing.rows[0].is_active;
    } catch {}
  }

  await db.query(
    `INSERT INTO broadcast_campaigns (
       campaign_name, source_file, daily_pattern_json, timezone, windows_json, messages_json,
       campaign_type, ai_context, ai_response_style, ai_guardrails, ai_cta, valid_until, assets_json,
       is_active, updated_at
     )
     VALUES (
       $1, $2, $3::jsonb, $4, $5::jsonb, COALESCE($6::jsonb, '[]'::jsonb),
       COALESCE($7, 'OTHER'), NULLIF($8, ''), NULLIF($9, ''), NULLIF($10, ''), NULLIF($11, ''), $12::timestamptz, COALESCE($13::jsonb, '[]'::jsonb),
       $14, NOW()
     )
     ON CONFLICT (campaign_name)
     DO UPDATE SET
       source_file = EXCLUDED.source_file,
       daily_pattern_json = EXCLUDED.daily_pattern_json,
       timezone = EXCLUDED.timezone,
       windows_json = EXCLUDED.windows_json,
       messages_json = COALESCE(EXCLUDED.messages_json, broadcast_campaigns.messages_json),
       campaign_type = COALESCE(EXCLUDED.campaign_type, broadcast_campaigns.campaign_type),
       ai_context = COALESCE(EXCLUDED.ai_context, broadcast_campaigns.ai_context),
       ai_response_style = COALESCE(EXCLUDED.ai_response_style, broadcast_campaigns.ai_response_style),
       ai_guardrails = COALESCE(EXCLUDED.ai_guardrails, broadcast_campaigns.ai_guardrails),
       ai_cta = COALESCE(EXCLUDED.ai_cta, broadcast_campaigns.ai_cta),
       valid_until = COALESCE(EXCLUDED.valid_until, broadcast_campaigns.valid_until),
       assets_json = COALESCE(EXCLUDED.assets_json, broadcast_campaigns.assets_json),
       is_active = EXCLUDED.is_active,
       updated_at = NOW()`,
    [
      cleanCampaign,
      broadcastCleanText(sourceFile),
      broadcastSafeJson(pattern, [30, 15, 10]),
      TIMEZONE,
      broadcastSafeJson(BROADCAST_WINDOWS, []),
      cleanMessages ? broadcastSafeJson(cleanMessages, []) : null,
      campaignType ? String(campaignType).trim().toUpperCase() : null,
      broadcastCleanText(aiContext),
      broadcastCleanText(aiResponseStyle),
      broadcastCleanText(aiGuardrails),
      broadcastCleanText(aiCta),
      broadcastParseValidUntilInput(validUntil),
      Array.isArray(assets) ? broadcastSafeJson(assets, []) : null,
      !!finalIsActive,
    ]
  );
  return cleanCampaign;
}

async function getBroadcastMessagesForCampaign(campaignName = BROADCAST_CAMPAIGN_NAME) {
  const cleanCampaign = broadcastCleanText(campaignName) || BROADCAST_CAMPAIGN_NAME;
  try {
    const result = await db.query(
      `SELECT messages_json
         FROM broadcast_campaigns
        WHERE campaign_name = $1
        LIMIT 1`,
      [cleanCampaign]
    );
    const rows = Array.isArray(result.rows) ? result.rows : [];
    const parsed = broadcastParseJsonArray(rows?.[0]?.messages_json || []);
    return parsed.length ? parsed : BROADCAST_MESSAGES;
  } catch {
    return BROADCAST_MESSAGES;
  }
}

async function getBroadcastCampaignConfig(campaignName = BROADCAST_CAMPAIGN_NAME) {
  const cleanCampaign = broadcastCleanText(campaignName) || BROADCAST_CAMPAIGN_NAME;
  try {
    const result = await db.query(
      `SELECT campaign_name, source_file, daily_pattern_json, messages_json, is_active, timezone,
              campaign_type, ai_context, ai_response_style, ai_guardrails, ai_cta, valid_until, assets_json
         FROM broadcast_campaigns
        WHERE campaign_name = $1
        LIMIT 1`,
      [cleanCampaign]
    );
    const row = result.rows?.[0] || null;
    const messages = broadcastParseJsonArray(row?.messages_json || []);
    const pattern = Array.isArray(row?.daily_pattern_json) ? row.daily_pattern_json.map((x) => Number(x || 0)).filter((n) => Number.isFinite(n) && n > 0) : BROADCAST_PATTERN_SAFE;
    return {
      exists: !!row,
      campaignName: cleanCampaign,
      sourceFile: broadcastCleanText(row?.source_file || ''),
      isActive: row ? !!row.is_active : true,
      messages: messages.length ? messages : BROADCAST_MESSAGES,
      pattern: pattern.length ? pattern : BROADCAST_PATTERN_SAFE,
      timezone: broadcastCleanText(row?.timezone || TIMEZONE),
      campaignType: String(row?.campaign_type || 'OTHER').trim().toUpperCase() || 'OTHER',
      aiContext: String(row?.ai_context || '').trim(),
      aiResponseStyle: String(row?.ai_response_style || '').trim(),
      aiGuardrails: String(row?.ai_guardrails || '').trim(),
      aiCta: String(row?.ai_cta || '').trim(),
      validUntil: broadcastFormatValidUntilDisplay(row?.valid_until || ''),
      validUntilRaw: String(row?.valid_until || '').trim(),
      assets: broadcastParseJsonAnyArray(row?.assets_json || []),
    };
  } catch {
    return {
      exists: false,
      campaignName: cleanCampaign,
      sourceFile: '',
      isActive: true,
      messages: BROADCAST_MESSAGES,
      pattern: BROADCAST_PATTERN_SAFE,
      timezone: TIMEZONE,
      campaignType: 'OTHER',
      aiContext: '',
      aiResponseStyle: '',
      aiGuardrails: '',
      aiCta: '',
      validUntil: '',
      validUntilRaw: '',
      assets: [],
    };
  }
}


async function getBroadcastAssetsForCampaign(campaignName = BROADCAST_CAMPAIGN_NAME) {
  const cfg = await getBroadcastCampaignConfig(campaignName);
  return Array.isArray(cfg.assets) ? cfg.assets : [];
}

async function sendBroadcastAssetsToRecipient(recipient = '', assets = []) {
  const rows = Array.isArray(assets) ? assets : [];
  let sent = 0;
  for (const asset of rows.slice(0, 5)) {
    const mediaId = String(asset?.mediaId || asset?.media_id || '').trim();
    const mimeType = String(asset?.mimeType || asset?.mime_type || '').trim();
    const filename = String(asset?.filename || '').trim();
    const caption = String(asset?.caption || '').trim();
    if (!mediaId) continue;
    if (isBroadcastAssetImage(mimeType, filename)) {
      await sendWhatsAppImageById(recipient, mediaId, caption);
    } else {
      await sendWhatsAppDocumentById(recipient, mediaId, filename || `archivo-${mediaId}`, caption);
    }
    sent += 1;
  }
  return sent;
}

async function saveBroadcastCampaignContext({
  campaignName = BROADCAST_CAMPAIGN_NAME,
  campaignType = 'OTHER',
  aiContext = '',
  aiResponseStyle = '',
  aiGuardrails = '',
  aiCta = '',
  validUntil = '',
} = {}) {
  const current = await getBroadcastCampaignConfig(campaignName);
  await upsertBroadcastCampaign({
    campaignName,
    sourceFile: current.sourceFile || BROADCAST_EXCEL_PATH,
    pattern: current.pattern || BROADCAST_PATTERN_SAFE,
    messages: current.messages || BROADCAST_MESSAGES,
    isActive: current.isActive,
    campaignType,
    aiContext,
    aiResponseStyle,
    aiGuardrails,
    aiCta,
    validUntil,
    assets: current.assets || [],
  });
}

function looksLikeDifferentTopicThanActiveBroadcast(text = '', activeOffer = null) {
  const activeType = String(activeOffer?.type || '').trim().toUpperCase();
  if (!activeType) return false;

  const courseIntent = detectCourseIntentFromContext(text, { lastCourseContext: getLastCourseContext(activeOffer?.waId || '') });
  const explicitProduct = isExplicitProductIntent(text);
  const explicitService = isExplicitServiceIntent(text);
  const explicitCourse = !!courseIntent?.isCourse;

  if (activeType === 'COURSE') return explicitProduct || explicitService;
  if (activeType === 'PRODUCT') return explicitCourse || explicitService;
  if (activeType === 'SERVICE') return explicitCourse || explicitProduct;
  return false;
}

function isAmbiguousBroadcastFollowup(text = '') {
  const t = normalizeShortReply(text || '');
  if (!t) return false;
  return /^(me interesa|interesante|pasame info|pasame mas info|pasa info|info|quiero info|quiero saber|dale|ok|oka|si|sí|bueno|genial|perfecto|quiero mas|quiero más|como hago|cómo hago|precio|horarios|cupos|requisitos|hay cuotas|cuotas|me interesa pasame info)$/.test(t);
}

async function buildAssistantMessagesForBroadcastCampaign(waId = '', activeOffer = null) {
  const campaignName = String(activeOffer?.campaignName || '').trim();
  if (!campaignName) return [];
  const cfg = await getBroadcastCampaignConfig(campaignName);
  const pieces = [];
  if (cfg.campaignType) pieces.push(`Tipo de difusión activa: ${cfg.campaignType}.`);
  if (cfg.aiContext) pieces.push(`Contexto de la difusión activa: ${cfg.aiContext}`);
  if (cfg.aiResponseStyle) pieces.push(`Cómo debe responder la IA: ${cfg.aiResponseStyle}`);
  if (cfg.aiGuardrails) pieces.push(`No debe inventar: ${cfg.aiGuardrails}`);
  if (cfg.aiCta) pieces.push(`Objetivo de respuesta: ${cfg.aiCta}`);
  if (cfg.validUntil) pieces.push(`Vigencia de la difusión: ${cfg.validUntil}`);
  if (Array.isArray(activeOffer?.items) && activeOffer.items.length) {
    pieces.push(`Opciones activas ofrecidas: ${activeOffer.items.slice(0, 10).join(' | ')}`);
  }
  return pieces.length ? [{ role: 'system', content: pieces.join('\n') }] : [];
}

async function updateBroadcastPendingMessages(campaignName = BROADCAST_CAMPAIGN_NAME, messages = []) {
  const cleanCampaign = broadcastCleanText(campaignName) || BROADCAST_CAMPAIGN_NAME;
  const messagePool = Array.isArray(messages) && messages.length ? messages.map((x) => broadcastCleanText(x)).filter(Boolean) : BROADCAST_MESSAGES;
  if (!messagePool.length) return { updated: 0 };

  const rows = await db.query(
    `SELECT id, custom_message, message_index
       FROM broadcast_queue
      WHERE campaign_name = $1
        AND status IN ('pending','error','skipped')
      ORDER BY COALESCE(send_at, created_at) ASC, id ASC`,
    [cleanCampaign]
  );

  let updated = 0;
  for (let i = 0; i < rows.rows.length; i += 1) {
    const row = rows.rows[i];
    if (broadcastCleanText(row.custom_message)) continue;
    const idx = row.message_index != null ? Number(row.message_index) : (i % messagePool.length);
    const template = messagePool[((idx % messagePool.length) + messagePool.length) % messagePool.length] || messagePool[0];
    const result = await db.query(
      `UPDATE broadcast_queue
          SET message_index = $2,
              message_text = $3,
              updated_at = NOW()
        WHERE id = $1`,
      [row.id, idx, template]
    );
    updated += Number(result.rowCount || 0);
  }

  return { updated };
}

function broadcastPickColumn(row, alternatives = []) {
  for (const key of alternatives) {
    if (Object.prototype.hasOwnProperty.call(row, key) && row[key] !== undefined && row[key] !== null && String(row[key]).trim() !== '') {
      return row[key];
    }
  }
  return '';
}

async function importBroadcastContactsFromExcel({
  campaignName = BROADCAST_CAMPAIGN_NAME,
  excelPath = BROADCAST_EXCEL_PATH,
  sourceFile = BROADCAST_EXCEL_PATH,
  offerType = BROADCAST_OFFER_TYPE,
  offerItems = BROADCAST_OFFER_ITEMS,
  offerSelectedName = BROADCAST_OFFER_SELECTED_NAME,
  messages = null,
} = {}) {
  if (!ENABLE_DAILY_BROADCAST) return { inserted: 0, skipped: 0, totalRows: 0, disabled: true, reason: 'feature_disabled' };
  if (!XLSX) return { inserted: 0, skipped: 0, totalRows: 0, disabled: true, reason: 'xlsx_missing' };
  if (!excelPath || !fs.existsSync(excelPath)) return { inserted: 0, skipped: 0, totalRows: 0, disabled: true, reason: 'excel_missing' };

  const cleanCampaign = await upsertBroadcastCampaign({ campaignName, sourceFile, messages });
  const wb = XLSX.readFile(excelPath, { cellDates: false });
  const firstSheet = wb.SheetNames[0];
  if (!firstSheet) return { inserted: 0, skipped: 0, totalRows: 0, campaignName: cleanCampaign };

  const rawRows = XLSX.utils.sheet_to_json(wb.Sheets[firstSheet], { defval: '' });
  if (!rawRows.length) return { inserted: 0, skipped: 0, totalRows: 0, campaignName: cleanCampaign };

  const rows = rawRows.map((row) => {
    const obj = {};
    for (const [key, value] of Object.entries(row)) obj[broadcastNormalizeHeader(key)] = value;
    return obj;
  });

  let inserted = 0;
  let skipped = 0;

  for (let i = 0; i < rows.length; i += 1) {
    const row = rows[i];
    const name = broadcastCleanText(broadcastPickColumn(row, ['nombre', 'name', 'contacto', 'cliente']));
    const phoneRaw = broadcastPickColumn(row, ['numero', 'número', 'telefono', 'telefono_whatsapp', 'tel', 'celular', 'whatsapp', 'phone']);
    const waPhone = normalizePhoneDigits(phoneRaw);
    const profileName = broadcastCleanText(broadcastPickColumn(row, ['perfil', 'profile_name', 'nombre_perfil'])) || name;
    const customMessage = broadcastCleanText(broadcastPickColumn(row, ['mensaje', 'message', 'custom_message']));
    const offerTypeRow = broadcastCleanText(broadcastPickColumn(row, ['offer_type', 'tipo_oferta'])) || offerType;
    const offerSelectedRow = broadcastCleanText(broadcastPickColumn(row, ['offer_selected_name', 'oferta_principal'])) || offerSelectedName;
    const waId = broadcastCleanText(broadcastPickColumn(row, ['wa_id', 'whatsapp_id'])) || waPhone;

    if (!waPhone) {
      skipped += 1;
      continue;
    }

    try {
      const result = await db.query(
        `INSERT INTO broadcast_queue (
           campaign_name, source_file, source_row, contact_name, profile_name, wa_phone, wa_id,
           custom_message, offer_type, offer_items_json, offer_selected_name, status, updated_at
         ) VALUES (
           $1, $2, $3, $4, $5, $6, $7,
           $8, $9, $10::jsonb, $11, 'pending', NOW()
         )
         ON CONFLICT (campaign_name, wa_phone) DO NOTHING`,
        [
          cleanCampaign,
          broadcastCleanText(sourceFile || excelPath),
          i + 2,
          name,
          profileName,
          waPhone,
          waId,
          customMessage,
          String(offerTypeRow || '').toUpperCase(),
          broadcastSafeJson(offerItems, []),
          offerSelectedRow,
        ]
      );
      inserted += Number(result.rowCount || 0);
      if (!result.rowCount) skipped += 1;
    } catch {
      skipped += 1;
    }
  }

  return { inserted, skipped, totalRows: rows.length, campaignName: cleanCampaign };
}

async function getBroadcastPlanningState(campaignName) {
  const existingDays = await db.query(
    `SELECT schedule_day::text AS schedule_day
       FROM broadcast_queue
      WHERE campaign_name = $1
        AND schedule_day IS NOT NULL
      GROUP BY schedule_day
      ORDER BY schedule_day ASC`,
    [campaignName]
  );

  const maxScheduled = await db.query(
    `SELECT MAX(schedule_day)::text AS max_day
       FROM broadcast_queue
      WHERE campaign_name = $1
        AND schedule_day IS NOT NULL`,
    [campaignName]
  );

  return {
    existingDaysCount: existingDays.rows.length,
    maxDay: broadcastCleanText(maxScheduled.rows?.[0]?.max_day || ''),
  };
}

async function planBroadcastQueue({
  campaignName = BROADCAST_CAMPAIGN_NAME,
  startYMD = '',
  pattern = BROADCAST_PATTERN_SAFE,
} = {}) {
  const cleanCampaign = broadcastCleanText(campaignName) || BROADCAST_CAMPAIGN_NAME;

  const pending = await db.query(
    `SELECT *
       FROM broadcast_queue
      WHERE campaign_name = $1
        AND status = 'pending'
        AND send_at IS NULL
      ORDER BY id ASC`,
    [cleanCampaign]
  );

  if (!pending.rows.length) {
    return { scheduled: 0, daysUsed: 0, campaignName: cleanCampaign };
  }

  const planningState = await getBroadcastPlanningState(cleanCampaign);
  const patternSafe = Array.isArray(pattern) && pattern.length ? pattern.map((n) => Math.max(0, Number(n || 0))).filter((n) => n > 0) : [30, 15, 10];

  const campaignMessages = await getBroadcastMessagesForCampaign(cleanCampaign);

  let dayCursor = planningState.maxDay ? broadcastAddDaysYMD(planningState.maxDay, 1) : broadcastChooseStartYMD(startYMD);
  if (startYMD && !planningState.maxDay) dayCursor = startYMD;

  let patternIndex = planningState.existingDaysCount % patternSafe.length;
  let queueIndex = 0;
  let daysUsed = 0;

  while (queueIndex < pending.rows.length) {
    const dayQuota = patternSafe[patternIndex % patternSafe.length];
    let currentMinDate = null;
    if (daysUsed === 0 && dayCursor === broadcastTodayYMD()) {
      currentMinDate = new Date(Date.now() + 2 * 60 * 1000);
    }

    const slots = broadcastPickRandomTimes(dayCursor, dayQuota, currentMinDate);
    if (!slots.length) {
      dayCursor = broadcastAddDaysYMD(dayCursor, 1);
      patternIndex += 1;
      daysUsed += 1;
      continue;
    }

    const batch = pending.rows.slice(queueIndex, queueIndex + slots.length);
    for (let i = 0; i < batch.length; i += 1) {
      const row = batch[i];
      const slot = slots[i];
      const messagePool = Array.isArray(campaignMessages) && campaignMessages.length ? campaignMessages : BROADCAST_MESSAGES;
      const messageIndex = row.message_index != null ? Number(row.message_index) : ((queueIndex + i) % messagePool.length);
      const baseMessage = broadcastCleanText(row.custom_message) || messagePool[messageIndex % messagePool.length] || '';
      const scheduledTemplate = broadcastCleanText(baseMessage);

      await db.query(
        `UPDATE broadcast_queue
            SET schedule_day = $2,
                send_at = $3,
                message_index = $4,
                message_text = $5,
                updated_at = NOW()
          WHERE id = $1`,
        [row.id, dayCursor, slot.toISOString(), messageIndex, scheduledTemplate]
      );
    }

    queueIndex += batch.length;
    dayCursor = broadcastAddDaysYMD(dayCursor, 1);
    patternIndex += 1;
    daysUsed += 1;
  }

  await upsertBroadcastCampaign({ campaignName: cleanCampaign, sourceFile: BROADCAST_EXCEL_PATH, pattern: patternSafe, isActive: undefined });
  return { scheduled: pending.rows.length, daysUsed, campaignName: cleanCampaign };
}

async function ensureBroadcastCampaignLoaded({
  campaignName = BROADCAST_CAMPAIGN_NAME,
  excelPath = BROADCAST_EXCEL_PATH,
  offerType = BROADCAST_OFFER_TYPE,
  offerItems = BROADCAST_OFFER_ITEMS,
  offerSelectedName = BROADCAST_OFFER_SELECTED_NAME,
  messages = null,
  startYMD = '',
} = {}) {
  if (!broadcastCanOperate()) {
    return { ready: false, inserted: 0, skipped: 0, totalRows: 0, scheduled: 0, daysUsed: 0, reason: !ENABLE_DAILY_BROADCAST ? 'feature_disabled' : (!XLSX ? 'xlsx_missing' : 'excel_missing') };
  }

  const imported = await importBroadcastContactsFromExcel({
    campaignName,
    excelPath,
    sourceFile: excelPath,
    offerType,
    offerItems,
    offerSelectedName,
    messages,
  });

  const planned = await planBroadcastQueue({ campaignName: imported.campaignName || campaignName, startYMD, pattern: BROADCAST_PATTERN_SAFE });
  return { ready: true, ...imported, ...planned };
}

async function getBroadcastSummary(campaignName = BROADCAST_CAMPAIGN_NAME) {
  const cleanCampaign = broadcastCleanText(campaignName) || BROADCAST_CAMPAIGN_NAME;
  const summary = await db.query(
    `SELECT
        COUNT(*) AS total,
        COUNT(*) FILTER (WHERE status = 'pending') AS pending,
        COUNT(*) FILTER (WHERE status = 'processing') AS processing,
        COUNT(*) FILTER (WHERE status IN ('sent','sent_api','delivered','read')) AS sent_api,
        COUNT(*) FILTER (WHERE status IN ('delivered','read')) AS delivered,
        COUNT(*) FILTER (WHERE status = 'read') AS read,
        COUNT(*) FILTER (WHERE status = 'error') AS error,
        COUNT(*) FILTER (WHERE status = 'skipped') AS skipped,
        COUNT(*) FILTER (WHERE status = 'pending' AND schedule_day = CURRENT_DATE) AS scheduled_today,
        COUNT(DISTINCT schedule_day) FILTER (WHERE status IN ('pending','processing','error','skipped') AND schedule_day IS NOT NULL) AS remaining_days,
        MIN(send_at) FILTER (WHERE status = 'pending') AS next_send_at,
        MAX(send_at) FILTER (WHERE status IN ('pending','processing','error','skipped')) AS last_send_at
       FROM broadcast_queue
      WHERE campaign_name = $1`,
    [cleanCampaign]
  );
  const base = summary.rows?.[0] || {};
  const campaign = await getBroadcastCampaignConfig(cleanCampaign);
  return {
    campaign_name: cleanCampaign,
    total: Number(base.total || 0),
    pending: Number(base.pending || 0),
    processing: Number(base.processing || 0),
    sent_api: Number(base.sent_api || 0),
    delivered: Number(base.delivered || 0),
    read: Number(base.read || 0),
    error: Number(base.error || 0),
    skipped: Number(base.skipped || 0),
    scheduled_today: Number(base.scheduled_today || 0),
    remaining_days: Number(base.remaining_days || 0),
    next_send_at: base.next_send_at || null,
    last_send_at: base.last_send_at || null,
    is_active: !!campaign.isActive,
    messages_count: Array.isArray(campaign.messages) ? campaign.messages.length : 0,
    source_file: campaign.sourceFile || BROADCAST_EXCEL_PATH,
  };
}


async function listBroadcastCampaignHistory(limit = 30) {
  try {
    const result = await db.query(
      `SELECT
          c.campaign_name,
          c.campaign_type,
          c.is_active,
          c.updated_at,
          COUNT(q.id) AS total,
          COUNT(q.id) FILTER (WHERE q.status = 'pending') AS pending,
          COUNT(q.id) FILTER (WHERE q.status IN ('sent_api','delivered','read')) AS sent_api,
          COUNT(q.id) FILTER (WHERE q.status = 'error') AS error
         FROM broadcast_campaigns c
         LEFT JOIN broadcast_queue q ON q.campaign_name = c.campaign_name
        GROUP BY c.campaign_name, c.campaign_type, c.is_active, c.updated_at
        ORDER BY c.updated_at DESC, c.campaign_name ASC
        LIMIT $1`,
      [Math.max(1, Number(limit || 30))]
    );
    return (result.rows || []).map((row) => ({
      campaignName: String(row.campaign_name || '').trim(),
      campaignType: String(row.campaign_type || 'OTHER').trim().toUpperCase() || 'OTHER',
      isActive: !!row.is_active,
      updatedAt: row.updated_at || null,
      updatedAtText: broadcastFormatValidUntilDisplay(row.updated_at || ''),
      total: Number(row.total || 0),
      pending: Number(row.pending || 0),
      sentApi: Number(row.sent_api || 0),
      error: Number(row.error || 0),
    }));
  } catch {
    return [];
  }
}

async function markBroadcastProcessing(id) {
  const result = await db.query(
    `UPDATE broadcast_queue
        SET status = 'processing', attempts = attempts + 1, updated_at = NOW()
      WHERE id = $1
        AND status = 'pending'
      RETURNING *`,
    [id]
  );
  return result.rows[0] || null;
}


function broadcastExtractWaMessageId(sendResponse = null) {
  if (!sendResponse || typeof sendResponse !== 'object') return '';
  return String(sendResponse?.wa_msg_id || sendResponse?.messages?.[0]?.id || '').trim();
}

function broadcastStatusTimestampToIso(timestampValue = '') {
  const raw = String(timestampValue || '').trim();
  if (!raw) return new Date().toISOString();
  if (/^\d+$/.test(raw)) {
    const num = Number(raw);
    if (Number.isFinite(num) && num > 0) {
      const ms = raw.length >= 13 ? num : num * 1000;
      return new Date(ms).toISOString();
    }
  }
  const dt = new Date(raw);
  return Number.isNaN(dt.getTime()) ? new Date().toISOString() : dt.toISOString();
}

async function updateBroadcastDeliveryFromWebhook(statusObj = {}) {
  const waMessageId = String(statusObj?.id || '').trim();
  if (!waMessageId) return { matched: 0, ignored: true, reason: 'missing_id' };

  const rawStatus = String(statusObj?.status || '').trim().toLowerCase();
  const statusIso = broadcastStatusTimestampToIso(statusObj?.timestamp);

  if (rawStatus === 'delivered') {
    const result = await db.query(
      `UPDATE broadcast_queue
          SET status = 'delivered',
              delivery_status = 'delivered',
              delivered_at = COALESCE(delivered_at, $2::timestamptz),
              updated_at = NOW(),
              last_error = NULL
        WHERE wa_message_id = $1`,
      [waMessageId, statusIso]
    );
    return { matched: Number(result.rowCount || 0), status: 'delivered' };
  }

  if (rawStatus === 'read') {
    const result = await db.query(
      `UPDATE broadcast_queue
          SET status = 'read',
              delivery_status = 'read',
              delivered_at = COALESCE(delivered_at, $2::timestamptz),
              read_at = COALESCE(read_at, $2::timestamptz),
              updated_at = NOW(),
              last_error = NULL
        WHERE wa_message_id = $1`,
      [waMessageId, statusIso]
    );
    return { matched: Number(result.rowCount || 0), status: 'read' };
  }

  if (rawStatus === 'failed') {
    const failureText = String(statusObj?.errors?.[0]?.message || statusObj?.errors?.[0]?.title || 'delivery_failed').slice(0, 500);
    const result = await db.query(
      `UPDATE broadcast_queue
          SET status = 'error',
              delivery_status = 'failed',
              last_error = $2,
              updated_at = NOW()
        WHERE wa_message_id = $1`,
      [waMessageId, failureText]
    );
    return { matched: Number(result.rowCount || 0), status: 'failed' };
  }

  if (rawStatus === 'sent') {
    const result = await db.query(
      `UPDATE broadcast_queue
          SET status = CASE WHEN status IN ('delivered','read') THEN status ELSE 'sent_api' END,
              delivery_status = 'sent',
              api_accepted_at = COALESCE(api_accepted_at, $2::timestamptz),
              sent_at = COALESCE(sent_at, $2::timestamptz),
              updated_at = NOW(),
              last_error = NULL
        WHERE wa_message_id = $1`,
      [waMessageId, statusIso]
    );
    return { matched: Number(result.rowCount || 0), status: 'sent' };
  }

  return { matched: 0, ignored: true, reason: 'unsupported_status', status: rawStatus };
}

async function processBroadcastQueue() {
  if (!broadcastCanOperate()) return { sent_api: 0, failed: 0, attempted: 0, ready: false };

  await ensureBroadcastCampaignLoaded({
    campaignName: BROADCAST_CAMPAIGN_NAME,
    excelPath: BROADCAST_EXCEL_PATH,
    offerType: BROADCAST_OFFER_TYPE,
    offerItems: BROADCAST_OFFER_ITEMS,
    offerSelectedName: BROADCAST_OFFER_SELECTED_NAME,
  }).catch(() => null);

  await db.query(
    `UPDATE broadcast_queue q
        SET status = 'pending',
            send_at = NOW() + ($1 || ' minutes')::interval,
            updated_at = NOW(),
            last_error = COALESCE(last_error, 'Sin confirmación de entrega. Reprogramado automáticamente.')
       FROM broadcast_campaigns c
      WHERE q.campaign_name = c.campaign_name
        AND c.is_active = TRUE
        AND q.status = 'sent_api'
        AND q.api_accepted_at IS NOT NULL
        AND q.api_accepted_at < NOW() - ($2 || ' minutes')::interval
        AND q.attempts < $3`,
    [Math.max(2, Math.floor(BROADCAST_SAME_RECIPIENT_GAP_MS / 60000)), Math.max(2, Math.floor(BROADCAST_SENT_API_STALE_MS / 60000)), BROADCAST_MAX_CONFIRM_RETRIES]
  );

  await db.query(
    `UPDATE broadcast_queue q
        SET status = 'error',
            delivery_status = 'failed',
            updated_at = NOW(),
            last_error = COALESCE(last_error, 'Sin confirmación de entrega luego de varios intentos.')
       FROM broadcast_campaigns c
      WHERE q.campaign_name = c.campaign_name
        AND c.is_active = TRUE
        AND q.status = 'sent_api'
        AND q.attempts >= $1
        AND q.api_accepted_at IS NOT NULL
        AND q.api_accepted_at < NOW() - ($2 || ' minutes')::interval`,
    [BROADCAST_MAX_CONFIRM_RETRIES, Math.max(2, Math.floor(BROADCAST_SENT_API_STALE_MS / 60000))]
  );

  const due = await db.query(
    `SELECT q.id
       FROM broadcast_queue q
       JOIN broadcast_campaigns c ON c.campaign_name = q.campaign_name
      WHERE c.is_active = TRUE
        AND q.status = 'pending'
        AND q.send_at IS NOT NULL
        AND q.send_at <= NOW()
      ORDER BY q.send_at ASC, q.id ASC
      LIMIT $1`,
    [BROADCAST_MAX_PER_RUN]
  );

  let sentApi = 0;
  let failed = 0;

  for (const row of due.rows || []) {
    const processing = await markBroadcastProcessing(row.id);
    if (!processing) continue;

    try {
      const currentCampaign = await getBroadcastCampaignConfig(processing.campaign_name || BROADCAST_CAMPAIGN_NAME);
      if (currentCampaign.exists && currentCampaign.isActive === false) {
        await db.query(
          `UPDATE broadcast_queue SET status = 'pending', updated_at = NOW() WHERE id = $1`,
          [processing.id]
        );
        continue;
      }

      const recipient = normalizeWhatsAppRecipient(processing.wa_phone);
      if (!recipient) throw new Error('Número inválido');

      const recentForRecipient = await db.query(
        `SELECT sent_at
           FROM broadcast_queue
          WHERE wa_phone = $1
            AND sent_at IS NOT NULL
            AND status IN ('sent_api','delivered','read')
            AND id <> $2
          ORDER BY sent_at DESC
          LIMIT 1`,
        [processing.wa_phone, processing.id]
      );
      const recentSentAt = recentForRecipient.rows?.[0]?.sent_at ? new Date(recentForRecipient.rows[0].sent_at) : null;
      if (recentSentAt && (Date.now() - recentSentAt.getTime()) < BROADCAST_SAME_RECIPIENT_GAP_MS) {
        await db.query(
          `UPDATE broadcast_queue
              SET status = 'pending',
                  send_at = $2,
                  updated_at = NOW(),
                  last_error = 'Reprogramado para no enviar mensajes pegados al mismo número.'
            WHERE id = $1`,
          [processing.id, new Date(recentSentAt.getTime() + BROADCAST_SAME_RECIPIENT_GAP_MS).toISOString()]
        );
        continue;
      }

      const waId = broadcastCleanText(processing.wa_id) || recipient;
      const messageTemplate = broadcastCleanText(processing.custom_message || processing.message_text || '');
      if (!messageTemplate) throw new Error('Mensaje vacío');
      const messageText = await broadcastRenderMessageForSend(messageTemplate, processing);
      if (!messageText) throw new Error('Mensaje vacío');

      const campaignAssets = Array.isArray(currentCampaign.assets) ? currentCampaign.assets : [];
      if (campaignAssets.length) {
        await sendBroadcastAssetsToRecipient(recipient, campaignAssets);
      }

      const sendResponse = await sendWhatsAppText(recipient, messageText, { disableDedup: true });
      const waMessageId = broadcastExtractWaMessageId(sendResponse);
      if (!waMessageId) {
        throw new Error('WhatsApp no devolvió ID de mensaje');
      }

      pushHistory(waId, 'assistant', messageText);
      updateLastCloseContext(waId, {
        waId,
        phone: recipient,
        phoneRaw: processing.wa_phone,
        name: processing.contact_name || processing.profile_name || '',
        profileName: processing.profile_name || processing.contact_name || '',
      });

      const offer = broadcastDefaultOfferFromRow(processing);
      const effectiveType = String((currentCampaign.campaignType || offer.type || '')).trim().toUpperCase();
      const effectiveItems = normalizeActiveOfferItems((offer.items && offer.items.length) ? offer.items : BROADCAST_OFFER_ITEMS);
      if (effectiveType && effectiveItems.length) {
        if (effectiveType === 'COURSE') clearProductMemory(waId);
        if (effectiveType === 'PRODUCT') clearLastCourseContext(waId);
        setActiveAssistantOffer(waId, {
          type: effectiveType,
          items: effectiveItems,
          selectedName: offer.selectedName || currentCampaign.aiCta || (effectiveItems.length === 1 ? effectiveItems[0] : ''),
          mode: 'DETAIL',
          questionKind: 'MANUAL_BROADCAST',
          lastAssistantText: messageText,
          campaignName: processing.campaign_name || currentCampaign.campaignName || BROADCAST_CAMPAIGN_NAME,
          campaignType: currentCampaign.campaignType || effectiveType,
        });
        if (effectiveType === 'COURSE') {
          const courseRows = effectiveItems.map((nombre) => ({ nombre }));
          setLastCourseContext(waId, {
            query: offer.selectedName || effectiveItems[0] || 'cursos',
            selectedName: offer.selectedName || effectiveItems[0] || '',
            currentCourseName: offer.selectedName || effectiveItems[0] || '',
            lastOptions: effectiveItems.slice(0, 10),
            recentCourses: mergeCourseContextRows(courseRows, getLastCourseContext(waId)?.recentCourses || []),
            requestedInterest: buildHubSpotCourseInterestLabel(offer.selectedName || effectiveItems[0] || 'cursos'),
          });
        }
      }

      await db.query(
        `UPDATE broadcast_queue
            SET status = 'sent_api',
                delivery_status = 'sent',
                wa_message_id = $2,
                api_accepted_at = NOW(),
                sent_at = NOW(),
                updated_at = NOW(),
                last_error = NULL,
                message_text = $3
          WHERE id = $1`,
        [processing.id, waMessageId, messageText]
      );
      sentApi += 1;
    } catch (e) {
      const errText = String(e?.response?.data?.error?.message || e?.message || e).slice(0, 500);
      await db.query(
        `UPDATE broadcast_queue
            SET status = CASE WHEN attempts < $3 THEN 'pending' ELSE 'error' END,
                send_at = CASE WHEN attempts < $3 THEN NOW() + ($4 || ' minutes')::interval ELSE send_at END,
                last_error = $2,
                updated_at = NOW()
          WHERE id = $1`,
        [processing.id, errText, BROADCAST_MAX_CONFIRM_RETRIES, Math.max(2, Math.floor(BROADCAST_SAME_RECIPIENT_GAP_MS / 60000))]
      );
      failed += 1;
    }
  }

  return { sent_api: sentApi, failed, attempted: due.rows.length, ready: true };
}

async function resetBroadcastErrorsToPending(campaignName = BROADCAST_CAMPAIGN_NAME) {
  const cleanCampaign = broadcastCleanText(campaignName) || BROADCAST_CAMPAIGN_NAME;
  const result = await db.query(
    `UPDATE broadcast_queue
        SET status = 'pending', last_error = NULL, updated_at = NOW()
      WHERE campaign_name = $1
        AND status = 'error'`,
    [cleanCampaign]
  );
  return { restored: Number(result.rowCount || 0) };
}

async function clearBroadcastQueueForUpload(campaignName = BROADCAST_CAMPAIGN_NAME, replaceMode = 'append') {
  const cleanCampaign = broadcastCleanText(campaignName) || BROADCAST_CAMPAIGN_NAME;
  const mode = String(replaceMode || 'append').trim().toLowerCase();

  if (mode === 'replace_all') {
    const result = await db.query(`DELETE FROM broadcast_queue WHERE campaign_name = $1`, [cleanCampaign]);
    return { deleted: Number(result.rowCount || 0), mode };
  }

  if (mode === 'replace_pending') {
    const result = await db.query(
      `DELETE FROM broadcast_queue
        WHERE campaign_name = $1
          AND status IN ('pending','processing','error','skipped')`,
      [cleanCampaign]
    );
    return { deleted: Number(result.rowCount || 0), mode };
  }

  return { deleted: 0, mode: 'append' };
}

const app = express();
app.use(express.json({ limit: "25mb" }));
app.use(express.urlencoded({ limit: "5mb", extended: true }));

const broadcastUpload = multer ? multer({ storage: multer.memoryStorage(), limits: { fileSize: 10 * 1024 * 1024 } }) : null;

function broadcastPanelRedirect(res, msg = '', isError = false, campaignName = '') {
  const query = new URLSearchParams();
  if (msg) query.set(isError ? 'error' : 'ok', msg);
  if (campaignName) query.set('campaign_name', String(campaignName || '').trim());
  const qs = query.toString();
  return res.redirect(`/broadcast/panel${qs ? `?${qs}` : ''}`);
}

app.get("/broadcast/panel", async (req, res) => {
  const campaignName = String(req.query?.campaign_name || BROADCAST_CAMPAIGN_NAME || '').trim() || BROADCAST_CAMPAIGN_NAME;
  const summary = await getBroadcastSummary(campaignName).catch(() => ({}));
  const campaign = await getBroadcastCampaignConfig(campaignName).catch(() => ({
    exists: false,
    campaignName,
    sourceFile: '',
    isActive: true,
    messages: BROADCAST_MESSAGES,
    pattern: BROADCAST_PATTERN_SAFE,
    timezone: TIMEZONE,
    campaignType: 'OTHER',
    aiContext: '',
    aiResponseStyle: '',
    aiGuardrails: '',
    aiCta: '',
    validUntil: '',
    validUntilRaw: '',
    assets: [],
  }));
  const history = await listBroadcastCampaignHistory(40).catch(() => []);

  const okMsg = broadcastCleanText(req.query?.ok || '');
  const errMsg = broadcastCleanText(req.query?.error || '');
  const messagesText = (Array.isArray(campaign.messages) && campaign.messages.length ? campaign.messages : BROADCAST_MESSAGES).join('\n');
  const campaignType = campaign.campaignType || 'OTHER';
  const campaignAssets = Array.isArray(campaign.assets) ? campaign.assets : [];
  const replaceDefault = String(req.query?.replace_mode || 'replace_pending').trim().toLowerCase();
  const pending = Number(summary?.pending || 0);
  const processing = Number(summary?.processing || 0);
  const sentApi = Number(summary?.sent_api || 0);
  const delivered = Number(summary?.delivered || 0);
  const readCount = Number(summary?.read || 0);
  const error = Number(summary?.error || 0);
  const skipped = Number(summary?.skipped || 0);
  const total = Number(summary?.total || 0);
  const remaining = pending + processing + error + skipped;
  const daysLeft = Number(summary?.remaining_days || 0);
  const currentFileLabel = campaign.sourceFile || BROADCAST_EXCEL_PATH || 'Sin archivo cargado todavía';

  const html = `<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Difusión Cataleya</title>
  <style>
    body { font-family: Arial, sans-serif; background:#f7f7f8; padding:24px; color:#111; }
    .wrap { max-width: 1280px; margin: 0 auto; display:grid; gap:18px; }
    .box { background:#fff; border-radius:16px; padding:24px; box-shadow:0 10px 30px rgba(0,0,0,.08); }
    h1, h2, h3 { margin-top:0; }
    label { display:block; margin:14px 0 6px; font-weight:600; }
    input, select, textarea, button { width:100%; padding:12px; border-radius:10px; border:1px solid #d0d7de; box-sizing:border-box; font:inherit; }
    button { background:#111827; color:#fff; border:none; cursor:pointer; margin-top:12px; }
    .muted { color:#555; font-size:14px; }
    .code { font-family: monospace; background:#f3f4f6; padding:3px 6px; border-radius:6px; }
    .grid { display:grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap:12px; }
    .stat { background:#f9fafb; border:1px solid #e5e7eb; border-radius:14px; padding:14px; }
    .stat strong { display:block; font-size:26px; margin-top:6px; }
    .main-layout { display:grid; grid-template-columns: minmax(0, 2fr) minmax(320px, 1fr); gap:18px; align-items:start; }
    .pill { display:inline-block; padding:6px 10px; border-radius:999px; background:#eef2ff; font-size:13px; }
    .ok { background:#ecfdf5; color:#065f46; padding:12px 14px; border-radius:12px; margin-bottom:12px; }
    .error { background:#fef2f2; color:#991b1b; padding:12px 14px; border-radius:12px; margin-bottom:12px; }
    .actions { display:grid; grid-template-columns: 1fr 1fr; gap:12px; }
    .section { border:1px solid #e5e7eb; border-radius:14px; padding:16px; margin-top:14px; background:#fafafa; }
    .row-2 { display:grid; grid-template-columns: 1fr 1fr; gap:14px; }
    .campaign-list { display:grid; gap:10px; }
    .campaign-item { display:block; text-decoration:none; color:inherit; border:1px solid #e5e7eb; border-radius:12px; padding:12px; background:#fff; }
    .campaign-item.active { border-color:#111827; box-shadow:0 0 0 1px #111827 inset; }
    .campaign-item small { color:#666; display:block; margin-top:4px; }
    ul.assets { margin:8px 0 0; padding-left:18px; }
    .footer-save { position:sticky; bottom:0; background:#fff; padding-top:12px; }
    @media (max-width: 980px) { .main-layout, .row-2, .actions { grid-template-columns: 1fr; } }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="box">
      <h1>Panel de difusión Cataleya</h1>
      <p class="muted">Bot: <span class="code">https://bot-cataleya.onrender.com</span></p>
      <p class="muted">Horarios de envío: <span class="code">09:00 a 22:00</span>. Los mensajes se distribuyen aislados dentro de esa franja.</p>
      <p class="muted">Patrón diario actual: <span class="code">${(campaign.pattern || BROADCAST_PATTERN_SAFE).join(' / ')}</span></p>
      <p class="muted">Estado: <span class="pill">${summary?.is_active === false ? 'Pausada' : 'Activa'}</span></p>
      ${okMsg ? `<div class="ok">${okMsg}</div>` : ''}
      ${errMsg ? `<div class="error">${errMsg}</div>` : ''}
      <div class="grid">
        <div class="stat"><span>Total cargados</span><strong>${total}</strong></div>
        <div class="stat"><span>Enviados a Meta</span><strong>${sentApi}</strong></div>
        <div class="stat"><span>Entregados</span><strong>${delivered}</strong></div>
        <div class="stat"><span>Leídos</span><strong>${readCount}</strong></div>
        <div class="stat"><span>Pendientes</span><strong>${pending}</strong></div>
        <div class="stat"><span>Errores</span><strong>${error}</strong></div>
        <div class="stat"><span>Faltan</span><strong>${remaining}</strong></div>
        <div class="stat"><span>Días restantes</span><strong>${daysLeft}</strong></div>
      </div>
      <p class="muted" style="margin-top:14px;">Programados para hoy: <span class="code">${Number(summary?.scheduled_today || 0)}</span> | Próximo envío: <span id="next-send-human" class="code">${summary?.next_send_at || '—'}</span></p>
      <p class="muted" style="margin-top:8px;">Faltan para el próximo envío: <span id="next-send-countdown" class="code">${summary?.next_send_at ? 'calculando...' : '—'}</span></p>
    </div>

    <div class="main-layout">
      <div class="box">
        <h2>Editar campaña</h2>
        <p class="muted">Completá todo y recién al final tocá <span class="code">Guardar y actualizar difusión</span>. Si no elegís un nuevo Excel o nuevos archivos, se conserva lo que ya tenía esta campaña.</p>

        <form action="/broadcast/save" method="post" enctype="multipart/form-data">
          <div class="row-2">
            <div>
              <label>Nombre de campaña</label>
              <input type="text" name="campaign_name" value="${campaignName}" />
            </div>
            <div>
              <label>Tipo de campaña</label>
              <select name="campaign_type">
                <option value="OTHER" ${campaignType === 'OTHER' ? 'selected' : ''}>OTHER</option>
                <option value="PRODUCT" ${campaignType === 'PRODUCT' ? 'selected' : ''}>PRODUCT</option>
                <option value="SERVICE" ${campaignType === 'SERVICE' ? 'selected' : ''}>SERVICE</option>
                <option value="COURSE" ${campaignType === 'COURSE' ? 'selected' : ''}>COURSE</option>
              </select>
            </div>
          </div>

          <div class="section">
            <h3 style="margin-bottom:6px;">Excel de contactos</h3>
            <p class="muted">Columnas mínimas: <span class="code">Nombre</span> y <span class="code">Número</span>. Si querés, podés agregar <span class="code">Mensaje</span> para un texto individual por fila.</p>
            <p class="muted">Archivo actual: <span class="code">${currentFileLabel}</span></p>
            <label>Nuevo Excel (opcional)</label>
            <input type="file" name="excel_file" accept=".xlsx,.xls" />
            <label>Modo de importación</label>
            <select name="replace_mode">
              <option value="append" ${replaceDefault === 'append' ? 'selected' : ''}>Agregar a la cola actual</option>
              <option value="replace_pending" ${replaceDefault === 'replace_pending' ? 'selected' : ''}>Reemplazar pendientes/errores y dejar enviados</option>
              <option value="replace_all" ${replaceDefault === 'replace_all' ? 'selected' : ''}>Borrar todo y empezar de cero</option>
            </select>
          </div>

          <div class="section">
            <h3 style="margin-bottom:6px;">Mensajes de difusión</h3>
            <p class="muted">Uno por línea. Podés usar <span class="code">{saludo}</span> y <span class="code">{nombre}</span>.</p>
            <textarea name="broadcast_messages" rows="12">${messagesText}</textarea>
          </div>

          <div class="section">
            <h3 style="margin-bottom:6px;">Contexto IA de la difusión</h3>
            <label>De qué trata la difusión</label>
            <textarea name="ai_context" rows="4">${campaign.aiContext || ''}</textarea>

            <label>Cómo debe responder la IA</label>
            <textarea name="ai_response_style" rows="4">${campaign.aiResponseStyle || ''}</textarea>

            <label>Qué no debe inventar</label>
            <textarea name="ai_guardrails" rows="4">${campaign.aiGuardrails || ''}</textarea>

            <label>Objetivo / CTA</label>
            <textarea name="ai_cta" rows="3">${campaign.aiCta || ''}</textarea>

            <label>Vigente hasta</label>
            <input type="datetime-local" name="valid_until" value="${broadcastFormatValidUntilInput(campaign.validUntilRaw || '')}" />
            <p class="muted">Si querés que la IA entienda “solo por hoy”, poné acá la fecha y hora de cierre de esa campaña.</p>
          </div>

          <div class="section">
            <h3 style="margin-bottom:6px;">Fotos y archivos de la campaña</h3>
            <p class="muted">Si subís imágenes o documentos, se conservan en esta campaña y se pueden enviar antes del texto.</p>
            <label>Adjuntar nuevos archivos (opcional)</label>
            <input type="file" name="asset_files" accept=".png,.jpg,.jpeg,.webp,.pdf" multiple />
            <p class="muted">Adjuntos actuales: <span class="code">${campaignAssets.length}</span></p>
            ${campaignAssets.length ? `<ul class="assets">${campaignAssets.map((a) => `<li>${a.filename || a.mediaId || 'archivo'}</li>`).join('')}</ul>` : '<p class="muted">No hay adjuntos cargados.</p>'}
          </div>

          <div class="footer-save">
            <button type="submit">Guardar y actualizar difusión</button>
          </div>
        </form>
      </div>

      <div style="display:grid; gap:18px;">
        <div class="box">
          <h2>Control rápido</h2>
          <div class="actions">
            <form action="/broadcast/toggle" method="post">
              <input type="hidden" name="campaign_name" value="${campaignName}" />
              <input type="hidden" name="action" value="${summary?.is_active === false ? 'resume' : 'pause'}" />
              <button type="submit">${summary?.is_active === false ? 'Prender difusión' : 'Pausar difusión'}</button>
            </form>
            <form action="/broadcast/retry-errors" method="post">
              <input type="hidden" name="campaign_name" value="${campaignName}" />
              <button type="submit">Reintentar errores</button>
            </form>
          </div>
          <p class="muted" style="margin-top:12px;">Si pausás la difusión, el bot no envía nada aunque haya pendientes. Al prenderla otra vez, sigue donde estaba.</p>
          <p class="muted">Importante: <span class="code">Enviados a Meta</span> significa que WhatsApp aceptó el mensaje. <span class="code">Entregados</span> y <span class="code">Leídos</span> se actualizan cuando llega el estado real por webhook.</p>
        </div>

        <div class="box">
          <h2>Registro de campañas</h2>
          <p class="muted">Acá ves las difusiones que fuiste creando.</p>
          <div class="campaign-list">
            ${history.length ? history.map((item) => `
              <a class="campaign-item ${item.campaignName === campaignName ? 'active' : ''}" href="/broadcast/panel?campaign_name=${encodeURIComponent(item.campaignName)}">
                <strong>${item.campaignName}</strong>
                <small>Tipo: ${item.campaignType} · ${item.isActive ? 'Activa' : 'Pausada'}</small>
                <small>Total: ${item.total} · Pendientes: ${item.pending} · Errores: ${item.error}</small>
                <small>Actualizada: ${item.updatedAtText || '—'}</small>
              </a>
            `).join('') : '<p class="muted">Todavía no hay campañas registradas.</p>'}
          </div>
        </div>
      </div>
    </div>
  </div>
  <script>
    (function(){
      const NEXT_SEND_AT_RAW = ${summary?.next_send_at ? JSON.stringify(summary.next_send_at) : 'null'};
      const TIMEZONE_LABEL = ${JSON.stringify(TIMEZONE)};
      const humanEl = document.getElementById('next-send-human');
      const countdownEl = document.getElementById('next-send-countdown');

      function two(n){ return String(n).padStart(2, '0'); }

      function formatNextSend(date){
        try {
          return new Intl.DateTimeFormat('es-AR', {
            timeZone: TIMEZONE_LABEL,
            weekday: 'long',
            day: '2-digit',
            month: '2-digit',
            hour: '2-digit',
            minute: '2-digit',
            hour12: false
          }).format(date);
        } catch (_) {
          const d = date;
          return two(d.getDate()) + '/' + two(d.getMonth()+1) + ' ' + two(d.getHours()) + ':' + two(d.getMinutes());
        }
      }

      function renderCountdown(){
        if (!humanEl || !countdownEl || !NEXT_SEND_AT_RAW) {
          if (countdownEl && !NEXT_SEND_AT_RAW) countdownEl.textContent = '—';
          return;
        }

        const target = new Date(NEXT_SEND_AT_RAW);
        if (Number.isNaN(target.getTime())) {
          humanEl.textContent = '—';
          countdownEl.textContent = '—';
          return;
        }

        humanEl.textContent = formatNextSend(target);

        const now = new Date();
        const diff = target.getTime() - now.getTime();
        if (diff <= 0) {
          countdownEl.textContent = '00:00:00';
          return;
        }

        const totalSeconds = Math.floor(diff / 1000);
        const hours = Math.floor(totalSeconds / 3600);
        const minutes = Math.floor((totalSeconds % 3600) / 60);
        const seconds = totalSeconds % 60;
        countdownEl.textContent = two(hours) + ':' + two(minutes) + ':' + two(seconds);
      }

      renderCountdown();
      setInterval(renderCountdown, 1000);
    })();
  </script>
</body>
</html>`;
  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  return res.status(200).send(html);
});

app.post("/broadcast/save", (req, res, next) => {
  if (!broadcastUpload) {
    return res.status(500).json({ ok: false, error: 'multer_missing', message: 'Falta instalar multer: npm i multer' });
  }
  return broadcastUpload.fields([
    { name: 'excel_file', maxCount: 1 },
    { name: 'asset_files', maxCount: 8 },
  ])(req, res, next);
}, async (req, res) => {
  try {
    const campaignName = String(req.body?.campaign_name || BROADCAST_CAMPAIGN_NAME || '').trim() || BROADCAST_CAMPAIGN_NAME;
    const replaceMode = String(req.body?.replace_mode || 'replace_pending').trim().toLowerCase();
    const campaignType = String(req.body?.campaign_type || 'OTHER').trim().toUpperCase() || 'OTHER';
    const aiContext = req.body?.ai_context || '';
    const aiResponseStyle = req.body?.ai_response_style || '';
    const aiGuardrails = req.body?.ai_guardrails || '';
    const aiCta = req.body?.ai_cta || '';
    const validUntilRaw = String(req.body?.valid_until || '').trim();
    const validUntilParsed = validUntilRaw ? broadcastParseValidUntilInput(validUntilRaw) : null;
    if (validUntilRaw && !validUntilParsed) {
      return broadcastPanelRedirect(res, 'No pude interpretar la vigencia. Usá fecha y hora válidas.', true, campaignName);
    }

    const current = await getBroadcastCampaignConfig(campaignName);
    const filesMap = req.files || {};
    const excelFile = Array.isArray(filesMap.excel_file) ? filesMap.excel_file[0] : null;
    const assetFiles = Array.isArray(filesMap.asset_files) ? filesMap.asset_files : [];
    const broadcastMessages = broadcastParseMessagesInput(req.body?.broadcast_messages || '');
    const finalMessages = broadcastMessages.length ? broadcastMessages : (current.messages?.length ? current.messages : BROADCAST_MESSAGES);

    let sourceFile = current.sourceFile || '';
    if (excelFile?.buffer && excelFile?.originalname) {
      if (!XLSX) {
        return broadcastPanelRedirect(res, 'Falta instalar xlsx: npm i xlsx', true, campaignName);
      }
      const tempPath = path.join(getTmpDir(), `broadcast-${Date.now()}-${String(excelFile.originalname).replace(/[^a-zA-Z0-9._-]/g, '_')}`);
      fs.writeFileSync(tempPath, excelFile.buffer);
      BROADCAST_EXCEL_PATH = tempPath;
      sourceFile = tempPath;
    }

    const mergedAssets = Array.isArray(current.assets) ? current.assets.slice() : [];
    for (const file of assetFiles) {
      const safeName = String(file.originalname || `archivo-${Date.now()}`).replace(/[^a-zA-Z0-9._-]/g, '_');
      const tmpPath = path.join(getTmpDir(), `broadcast-asset-${Date.now()}-${safeName}`);
      fs.writeFileSync(tmpPath, file.buffer);
      const mimeType = String(file.mimetype || guessMimeTypeFromFilename(safeName) || 'application/octet-stream');
      const mediaId = await uploadMediaToWhatsApp(tmpPath, mimeType);
      mergedAssets.push({
        filename: safeName,
        mimeType,
        mediaId,
        uploadedAt: new Date().toISOString(),
      });
    }

    await upsertBroadcastCampaign({
      campaignName,
      sourceFile: sourceFile || BROADCAST_EXCEL_PATH,
      pattern: current.pattern || BROADCAST_PATTERN_SAFE,
      messages: finalMessages,
      isActive: current.isActive,
      campaignType,
      aiContext,
      aiResponseStyle,
      aiGuardrails,
      aiCta,
      validUntil: validUntilParsed,
      assets: mergedAssets,
    });

    await updateBroadcastPendingMessages(campaignName, finalMessages);

    await db.query(
      `UPDATE broadcast_queue
          SET offer_type = $2,
              updated_at = NOW()
        WHERE campaign_name = $1
          AND status IN ('pending','processing','error','skipped')`,
      [campaignName, campaignType]
    );

    let summaryMsg = 'Difusión guardada correctamente.';
    if (excelFile?.buffer && excelFile?.originalname) {
      const cleared = await clearBroadcastQueueForUpload(campaignName, replaceMode);
      const data = await ensureBroadcastCampaignLoaded({
        campaignName,
        excelPath: sourceFile,
        offerType: campaignType === 'OTHER' ? BROADCAST_OFFER_TYPE : campaignType,
        offerItems: BROADCAST_OFFER_ITEMS,
        offerSelectedName: BROADCAST_OFFER_SELECTED_NAME,
        messages: finalMessages,
      });
      summaryMsg = `Difusión actualizada. Nuevos: ${Number(data.inserted || 0)} | Omitidos: ${Number(data.skipped || 0)} | Cola limpiada: ${Number(cleared.deleted || 0)}.`;
    } else if (sourceFile && fs.existsSync(sourceFile)) {
      await planBroadcastQueue({
        campaignName,
        pattern: current.pattern || BROADCAST_PATTERN_SAFE,
      }).catch(() => null);
    }

    return broadcastPanelRedirect(res, summaryMsg, false, campaignName);
  } catch (e) {
    return broadcastPanelRedirect(res, e?.message || 'No pude guardar la difusión.', true, String(req.body?.campaign_name || BROADCAST_CAMPAIGN_NAME || '').trim() || BROADCAST_CAMPAIGN_NAME);
  }
});

app.post("/broadcast/context", async (req, res) => {
  try {
    const campaignName = String(req.body?.campaign_name || BROADCAST_CAMPAIGN_NAME || '').trim() || BROADCAST_CAMPAIGN_NAME;
    await saveBroadcastCampaignContext({
      campaignName,
      campaignType: String(req.body?.campaign_type || 'OTHER').trim().toUpperCase() || 'OTHER',
      aiContext: req.body?.ai_context || '',
      aiResponseStyle: req.body?.ai_response_style || '',
      aiGuardrails: req.body?.ai_guardrails || '',
      aiCta: req.body?.ai_cta || '',
      validUntil: req.body?.valid_until || '',
    });
    return broadcastPanelRedirect(res, 'Contexto IA guardado correctamente.', false, campaignName);
  } catch (e) {
    const safeCampaignName = String(req.body?.campaign_name || BROADCAST_CAMPAIGN_NAME || '').trim() || BROADCAST_CAMPAIGN_NAME;
    return broadcastPanelRedirect(res, e?.message || 'No pude guardar el contexto IA.', true, safeCampaignName);
  }
});

app.post("/broadcast/assets", (req, res, next) => {
  if (!broadcastUpload) {
    return res.status(500).json({ ok: false, error: 'multer_missing', message: 'Falta instalar multer: npm i multer' });
  }
  return broadcastUpload.array('files', 6)(req, res, next);
}, async (req, res) => {
  try {
    const files = Array.isArray(req.files) ? req.files : [];
    const campaignName = String(req.body?.campaign_name || BROADCAST_CAMPAIGN_NAME || '').trim() || BROADCAST_CAMPAIGN_NAME;
    if (!files.length) return broadcastPanelRedirect(res, 'No recibí archivos para subir.', true, campaignName);

    const current = await getBroadcastCampaignConfig(campaignName);
    const assets = Array.isArray(current.assets) ? current.assets.slice() : [];
    for (const file of files) {
      const safeName = String(file.originalname || `archivo-${Date.now()}`).replace(/[^a-zA-Z0-9._-]/g, '_');
      const tmpPath = path.join(getTmpDir(), `broadcast-asset-${Date.now()}-${safeName}`);
      fs.writeFileSync(tmpPath, file.buffer);
      const mimeType = String(file.mimetype || guessMimeTypeFromFilename(safeName) || 'application/octet-stream');
      const mediaId = await uploadMediaToWhatsApp(tmpPath, mimeType);
      assets.push({
        filename: safeName,
        mimeType,
        mediaId,
        uploadedAt: new Date().toISOString(),
      });
    }

    await upsertBroadcastCampaign({
      campaignName,
      sourceFile: current.sourceFile || BROADCAST_EXCEL_PATH,
      pattern: current.pattern || BROADCAST_PATTERN_SAFE,
      messages: current.messages || BROADCAST_MESSAGES,
      isActive: current.isActive,
      campaignType: current.campaignType,
      aiContext: current.aiContext,
      aiResponseStyle: current.aiResponseStyle,
      aiGuardrails: current.aiGuardrails,
      aiCta: current.aiCta,
      validUntil: current.validUntilRaw || current.validUntil,
      assets,
    });

    return broadcastPanelRedirect(res, `Archivos cargados correctamente: ${files.length}.`, false, campaignName);
  } catch (e) {
    const safeCampaignName = String(req.body?.campaign_name || BROADCAST_CAMPAIGN_NAME || '').trim() || BROADCAST_CAMPAIGN_NAME;
    return broadcastPanelRedirect(res, e?.message || 'No pude subir los archivos de la campaña.', true, safeCampaignName);
  }
});

app.post("/broadcast/messages", async (req, res) => {
  try {
    const campaignName = String(req.body?.campaign_name || BROADCAST_CAMPAIGN_NAME || '').trim() || BROADCAST_CAMPAIGN_NAME;
    const broadcastMessages = broadcastParseMessagesInput(req.body?.broadcast_messages || '');
    if (!broadcastMessages.length) {
      return broadcastPanelRedirect(res, 'Tenés que dejar al menos un mensaje.', true, campaignName);
    }

    const current = await getBroadcastCampaignConfig(campaignName);
    await upsertBroadcastCampaign({
      campaignName,
      sourceFile: current.sourceFile || BROADCAST_EXCEL_PATH,
      pattern: current.pattern || BROADCAST_PATTERN_SAFE,
      messages: broadcastMessages,
      isActive: current.isActive,
    });
    await updateBroadcastPendingMessages(campaignName, broadcastMessages);
    return broadcastPanelRedirect(res, 'Mensajes guardados correctamente.', false, campaignName);
  } catch (e) {
    const safeCampaignName = String(req.body?.campaign_name || BROADCAST_CAMPAIGN_NAME || '').trim() || BROADCAST_CAMPAIGN_NAME;
    return broadcastPanelRedirect(res, e?.message || 'No pude guardar los mensajes.', true, safeCampaignName);
  }
});

app.post("/broadcast/toggle", async (req, res) => {
  try {
    const campaignName = String(req.body?.campaign_name || BROADCAST_CAMPAIGN_NAME || '').trim() || BROADCAST_CAMPAIGN_NAME;
    const action = String(req.body?.action || '').trim().toLowerCase();
    const enable = action === 'resume' || action === 'start' || action === 'on' || action === 'enable';
    await db.query(
      `UPDATE broadcast_campaigns
          SET is_active = $2, updated_at = NOW()
        WHERE campaign_name = $1`,
      [campaignName, enable]
    );
    return broadcastPanelRedirect(res, enable ? 'Difusión prendida.' : 'Difusión pausada.', false, campaignName);
  } catch (e) {
    const safeCampaignName = String(req.body?.campaign_name || BROADCAST_CAMPAIGN_NAME || '').trim() || BROADCAST_CAMPAIGN_NAME;
    return broadcastPanelRedirect(res, e?.message || 'No pude cambiar el estado de la difusión.', true, safeCampaignName);
  }
});

app.post("/broadcast/upload", (req, res, next) => {
  if (!broadcastUpload) {
    return res.status(500).json({ ok: false, error: 'multer_missing', message: 'Falta instalar multer: npm i multer' });
  }
  return broadcastUpload.single('file')(req, res, next);
}, async (req, res) => {
  try {
    if (!XLSX) {
      return res.status(500).json({ ok: false, error: 'xlsx_missing', message: 'Falta instalar xlsx: npm i xlsx' });
    }
    if (!req.file?.buffer || !req.file?.originalname) {
      return res.status(400).json({ ok: false, error: 'file_missing', message: 'No recibí ningún archivo Excel.' });
    }

    const campaignName = String(req.body?.campaign_name || BROADCAST_CAMPAIGN_NAME || '').trim() || BROADCAST_CAMPAIGN_NAME;
    const replaceMode = String(req.body?.replace_mode || 'append').trim().toLowerCase();
    const current = await getBroadcastCampaignConfig(campaignName);
    const tempPath = path.join(getTmpDir(), `broadcast-${Date.now()}-${req.file.originalname.replace(/[^a-zA-Z0-9._-]/g, '_')}`);

    fs.writeFileSync(tempPath, req.file.buffer);
    BROADCAST_EXCEL_PATH = tempPath;
    await upsertBroadcastCampaign({
      campaignName,
      sourceFile: tempPath,
      pattern: current.pattern || BROADCAST_PATTERN_SAFE,
      messages: current.messages || BROADCAST_MESSAGES,
      isActive: current.isActive,
      campaignType: current.campaignType,
      aiContext: current.aiContext,
      aiResponseStyle: current.aiResponseStyle,
      aiGuardrails: current.aiGuardrails,
      aiCta: current.aiCta,
      validUntil: current.validUntilRaw || current.validUntil,
      assets: current.assets || [],
    });

    const cleared = await clearBroadcastQueueForUpload(campaignName, replaceMode);
    const data = await ensureBroadcastCampaignLoaded({
      campaignName,
      excelPath: tempPath,
      offerType: BROADCAST_OFFER_TYPE,
      offerItems: BROADCAST_OFFER_ITEMS,
      offerSelectedName: BROADCAST_OFFER_SELECTED_NAME,
      messages: current.messages?.length ? current.messages : null,
    });

    if (req.headers.accept && String(req.headers.accept).includes('text/html')) {
      return broadcastPanelRedirect(res, `Excel importado. Nuevos: ${Number(data.inserted || 0)} | Omitidos: ${Number(data.skipped || 0)}.`, false, campaignName);
    }

    return res.json({
      ok: true,
      upload_url: 'https://bot-cataleya.onrender.com/broadcast/upload',
      panel_url: 'https://bot-cataleya.onrender.com/broadcast/panel',
      replace_mode: replaceMode,
      cleared,
      excel_path: tempPath,
      messages_count: current.messages?.length || BROADCAST_MESSAGES.length,
      ...data,
    });
  } catch (e) {
    if (req.headers.accept && String(req.headers.accept).includes('text/html')) {
      const safeCampaignName = String(req.body?.campaign_name || BROADCAST_CAMPAIGN_NAME || '').trim() || BROADCAST_CAMPAIGN_NAME;
      return broadcastPanelRedirect(res, e?.message || 'No pude importar el Excel.', true, safeCampaignName);
    }
    return res.status(500).json({ ok: false, error: e?.message || 'error_broadcast_upload' });
  }
});

app.get("/broadcast/status", async (req, res) => {
  try {
    const campaignName = String(req.query?.campaign_name || BROADCAST_CAMPAIGN_NAME || '').trim() || BROADCAST_CAMPAIGN_NAME;
    const summary = await getBroadcastSummary(campaignName);
    const campaign = await getBroadcastCampaignConfig(campaignName);
    return res.json({
      ok: true,
      enabled: ENABLE_DAILY_BROADCAST,
      ready: broadcastCanOperate(),
      excel_path: campaign.sourceFile || BROADCAST_EXCEL_PATH,
      excel_present: broadcastExcelExists(campaign.sourceFile || BROADCAST_EXCEL_PATH),
      panel_url: 'https://bot-cataleya.onrender.com/broadcast/panel',
      upload_url: 'https://bot-cataleya.onrender.com/broadcast/upload',
      campaign_name: campaignName,
      daily_pattern: campaign.pattern || BROADCAST_PATTERN_SAFE,
      messages_count: campaign.messages?.length || BROADCAST_MESSAGES.length,
      is_active: !!campaign.isActive,
      campaign_type: campaign.campaignType || 'OTHER',
      ai_context: campaign.aiContext || '',
      ai_response_style: campaign.aiResponseStyle || '',
      ai_guardrails: campaign.aiGuardrails || '',
      ai_cta: campaign.aiCta || '',
      valid_until: campaign.validUntil || '',
      assets_count: Array.isArray(campaign.assets) ? campaign.assets.length : 0,
      summary,
    });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e?.message || 'error_broadcast_status' });
  }
});

app.post("/broadcast/import", async (req, res) => {
  try {
    const body = req.body || {};
    const campaignName = String(body.campaign_name || BROADCAST_CAMPAIGN_NAME || '').trim() || BROADCAST_CAMPAIGN_NAME;
    const campaign = await getBroadcastCampaignConfig(campaignName);
    const excelPath = String(body.excel_path || campaign.sourceFile || BROADCAST_EXCEL_PATH || '').trim();
    if (!excelPath || !fs.existsSync(excelPath)) {
      return res.status(400).json({ ok: false, error: 'excel_missing', message: 'No hay ninguna lista de Excel cargada todavía. Subila en https://bot-cataleya.onrender.com/broadcast/panel o por POST a /broadcast/upload.' });
    }

    const data = await ensureBroadcastCampaignLoaded({
      campaignName,
      excelPath,
      offerType: String(body.offer_type || BROADCAST_OFFER_TYPE || '').trim().toUpperCase() || BROADCAST_OFFER_TYPE,
      offerItems: Array.isArray(body.offer_items) && body.offer_items.length ? body.offer_items : BROADCAST_OFFER_ITEMS,
      offerSelectedName: String(body.offer_selected_name || BROADCAST_OFFER_SELECTED_NAME || '').trim() || BROADCAST_OFFER_SELECTED_NAME,
      startYMD: String(body.start_ymd || '').trim(),
      messages: campaign.messages?.length ? campaign.messages : null,
    });

    return res.json({ ok: true, ...data });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e?.message || 'error_broadcast_import' });
  }
});

app.post("/broadcast/retry-errors", async (req, res) => {
  try {
    const campaignName = String(req.body?.campaign_name || BROADCAST_CAMPAIGN_NAME || '').trim() || BROADCAST_CAMPAIGN_NAME;
    const data = await resetBroadcastErrorsToPending(campaignName);
    if (req.headers.accept && String(req.headers.accept).includes('text/html')) {
      return broadcastPanelRedirect(res, `Errores restaurados a pendientes: ${Number(data.restored || 0)}.`, false, campaignName);
    }
    return res.json({ ok: true, ...data });
  } catch (e) {
    if (req.headers.accept && String(req.headers.accept).includes('text/html')) {
      const safeCampaignName = String(req.body?.campaign_name || BROADCAST_CAMPAIGN_NAME || '').trim() || BROADCAST_CAMPAIGN_NAME;
      return broadcastPanelRedirect(res, e?.message || 'No pude restaurar los errores.', true, safeCampaignName);
    }
    return res.status(500).json({ ok: false, error: e?.message || 'error_broadcast_retry' });
  }
});

// ===================== CONFIG =====================
const DRIVE_FOLDER_ID = "1pKCqh1HEvQaI6XQ85ST8yvzxYWRXpxM1";
const TIMEZONE = "America/Argentina/Salta";

const WHATSAPP_TEMPLATE_LANGUAGE = process.env.WHATSAPP_TEMPLATE_LANGUAGE || "es_AR";
const TEMPLATE_NUEVO_TURNO_PELUQUERA = process.env.TEMPLATE_NUEVO_TURNO_PELUQUERA || "consulta_disponibilidad_peluquera";
const TEMPLATE_TURNO_CONFIRMADO_PELUQUERA = process.env.TEMPLATE_TURNO_CONFIRMADO_PELUQUERA || "turno_confirmado_peluquera";
const STYLIST_NOTIFY_PHONE_RAW = process.env.STYLIST_NOTIFY_PHONE || "3868 466370";
const COURSE_NOTIFY_PHONE_RAW = process.env.COURSE_NOTIFY_PHONE || STYLIST_NOTIFY_PHONE_RAW;
const APPOINTMENT_ACTIVE_CONFLICT_STATUSES = ['pending_stylist_confirmation', 'awaiting_payment'];
const TEMPLATE_CURSO_SENA_RECIBIDA = process.env.TEMPLATE_CURSO_SENA_RECIBIDA || "inscripcion_curso_sena_recibida";
const APPOINTMENT_TEMPLATE_SCAN_MS = Number(process.env.APPOINTMENT_TEMPLATE_SCAN_MS || 60000);
const STYLIST_CONFIRMATION_TIMEOUT_MS = Number(process.env.STYLIST_CONFIRMATION_TIMEOUT_MS || 2 * 60 * 60 * 1000);
const COURSE_MANAGER_CONFIRMATION_TIMEOUT_MS = Number(process.env.COURSE_MANAGER_CONFIRMATION_TIMEOUT_MS || 12 * 60 * 60 * 1000);

const ENABLE_COMMERCIAL_FOLLOWUPS = String(process.env.ENABLE_COMMERCIAL_FOLLOWUPS || "true").toLowerCase() === "true";
const COMMERCIAL_FOLLOWUP_SCAN_MS = Number(process.env.COMMERCIAL_FOLLOWUP_SCAN_MS || 60000);
const COMMERCIAL_FOLLOWUP_DELAY_MS = Number(process.env.COMMERCIAL_FOLLOWUP_DELAY_MS || 23 * 60 * 60 * 1000);
const COMMERCIAL_FOLLOWUP_MAX_PER_RUN = Number(process.env.COMMERCIAL_FOLLOWUP_MAX_PER_RUN || 20);
const COMMERCIAL_PROMO_WINDOW_HOURS = Number(process.env.COMMERCIAL_PROMO_WINDOW_HOURS || 48);
const COMMERCIAL_PROMO_DISCOUNT_PERCENT = Number(process.env.COMMERCIAL_PROMO_DISCOUNT_PERCENT || 50);

const ENABLE_BIRTHDAY_MESSAGES = String(process.env.ENABLE_BIRTHDAY_MESSAGES || "true").toLowerCase() === "true";
const BIRTHDAY_SCAN_MS = Number(process.env.BIRTHDAY_SCAN_MS || 60 * 60 * 1000);

// ===================== DIFUSIÓN DIARIA DESDE EXCEL =====================
const ENABLE_DAILY_BROADCAST = String(process.env.ENABLE_DAILY_BROADCAST || "true").toLowerCase() === "true";
const BROADCAST_SCAN_MS = Number(process.env.BROADCAST_SCAN_MS || 60000);
const BROADCAST_MAX_PER_RUN = Number(process.env.BROADCAST_MAX_PER_RUN || 6);
let BROADCAST_EXCEL_PATH = String(process.env.BROADCAST_EXCEL_PATH || path.join(__dirname, "broadcast_clientes.xlsx")).trim();
const BROADCAST_CAMPAIGN_NAME = String(process.env.BROADCAST_CAMPAIGN_NAME || "difusion_diaria").trim() || "difusion_diaria";
const BROADCAST_AUTO_IMPORT_ON_START = String(process.env.BROADCAST_AUTO_IMPORT_ON_START || "true").toLowerCase() === "true";
const BROADCAST_OFFER_TYPE = String(process.env.BROADCAST_OFFER_TYPE || "COURSE").trim().toUpperCase() || "COURSE";
const BROADCAST_OFFER_ITEMS = String(process.env.BROADCAST_OFFER_ITEMS || "Taller de Peinados|Taller de Trenzas Africanas|Taller de Pinta Caritas")
  .split("|")
  .map((x) => String(x || "").trim())
  .filter(Boolean);
const BROADCAST_OFFER_SELECTED_NAME = String(process.env.BROADCAST_OFFER_SELECTED_NAME || "Talleres Cataleya").trim();
const BROADCAST_WINDOWS = [
  { start: '09:00', end: '22:00' },
];
const BROADCAST_DAILY_PATTERN = String(process.env.BROADCAST_DAILY_PATTERN || '30,15,10')
  .split(',')
  .map((x) => Number(String(x || '').trim()))
  .filter((n) => Number.isFinite(n) && n > 0);
const BROADCAST_PATTERN_SAFE = BROADCAST_DAILY_PATTERN.length ? BROADCAST_DAILY_PATTERN : [30, 15, 10];
const BROADCAST_SLOT_GRANULARITY_MINUTES = 5;
const BROADCAST_MIN_GAP_MINUTES = 20;

const BROADCAST_SAME_RECIPIENT_GAP_MS = Number(process.env.BROADCAST_SAME_RECIPIENT_GAP_MS || 5 * 60 * 1000);
const BROADCAST_SENT_API_STALE_MS = Number(process.env.BROADCAST_SENT_API_STALE_MS || 8 * 60 * 1000);
const BROADCAST_MAX_CONFIRM_RETRIES = Number(process.env.BROADCAST_MAX_CONFIRM_RETRIES || 3);

function isBroadcastAssetImage(mimeType = '', filename = '') {
  const mime = String(mimeType || '').toLowerCase();
  if (mime.startsWith('image/')) return true;
  const lower = String(filename || '').toLowerCase();
  return /\.(png|jpe?g|webp)$/i.test(lower);
}


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
  fechaNacimiento: "fecha_de_nacimiento",
};

const HUBSPOT_OPTION = {
  clienteSi: "true",
  empresaCataleya: "CATALEYA Salón de Belleza",
};

const HUBSPOT_STUDENT_PROPERTY = String(process.env.HUBSPOT_STUDENT_PROPERTY || '').trim();
const HUBSPOT_STUDENT_COURSES_PROPERTY = String(process.env.HUBSPOT_STUDENT_COURSES_PROPERTY || '').trim();

function hasHubSpotEnabled() {
  return !!HUBSPOT_ACCESS_TOKEN;
}

function hasGoogleContactsSyncEnabled() {
  return ENABLE_GOOGLE_CONTACTS_SYNC
    && !!GOOGLE_CONTACTS_CLIENT_ID
    && !!GOOGLE_CONTACTS_CLIENT_SECRET
    && GOOGLE_CONTACTS_TARGETS.some((target) => !isGoogleContactsTargetDisabled(target));
}

const pendingContactNameRequests = new Map();
const oneShotReplyPrefixByPhone = new Map();
const contactIdentityCache = new Map();
const CONTACT_IDENTITY_CACHE_MS = Number(process.env.CONTACT_IDENTITY_CACHE_MS || 10 * 60 * 1000);

function getCachedIdentityByWaId(waId) {
  const row = contactIdentityCache.get(waId);
  if (!row) return null;
  if ((Date.now() - Number(row.ts || 0)) > CONTACT_IDENTITY_CACHE_MS) {
    contactIdentityCache.delete(waId);
    return null;
  }
  return row.value || null;
}

function setCachedIdentityByWaId(waId, value) {
  if (!waId) return value || null;
  contactIdentityCache.set(waId, { ts: Date.now(), value: value || null });
  return value || null;
}

function clearCachedIdentityByWaId(waId) {
  if (waId) contactIdentityCache.delete(waId);
}

function getPendingContactNameRequest(waId) {
  return pendingContactNameRequests.get(waId) || null;
}

function setPendingContactNameRequest(waId, patch = {}) {
  if (!waId) return null;
  const prev = getPendingContactNameRequest(waId) || {};
  const next = { ...prev, ...patch, updatedAt: Date.now() };
  pendingContactNameRequests.set(waId, next);
  return next;
}

function clearPendingContactNameRequest(waId) {
  if (waId) pendingContactNameRequests.delete(waId);
}

function getReplyPrefixPhoneKey(phone = '') {
  const raw = String(phone || '').trim();
  if (!raw) return '';
  return normalizePhone(raw) || normalizeWhatsAppRecipient(raw) || raw.replace(/[^\d]/g, '');
}

function setOneShotReplyPrefix(phone = '', prefix = '') {
  const key = getReplyPrefixPhoneKey(phone);
  const value = String(prefix || '').trim();
  if (!key || !value) return;
  oneShotReplyPrefixByPhone.set(key, value);
}

function consumeOneShotReplyPrefix(phone = '') {
  const key = getReplyPrefixPhoneKey(phone);
  if (!key) return '';
  const value = oneShotReplyPrefixByPhone.get(key) || '';
  if (value) oneShotReplyPrefixByPhone.delete(key);
  return value;
}

function extractNameAnswer(text = '') {
  const explicit = extractExplicitNameFromSnippet(text);
  if (explicit) return explicit;
  return cleanNameCandidate(text);
}

function looksLikeStandaloneNameAnswer(text = '') {
  const cleaned = extractNameAnswer(text);
  if (!cleaned) return false;
  const compact = String(text || '').replace(/\s+/g, ' ').trim();
  if (!compact) return false;
  return compact.length <= 60 && !/[?.!,;:]/.test(compact.replace(/[ÁÉÍÓÚÑáéíóúñ' -]/g, ''));
}

function cleanDeferredNameRequestText(text = '') {
  return String(text || '')
    .replace(/\s+/g, ' ')
    .replace(/^(hola|holi|holis|buen dia|buen día|buenas|buenas tardes|buenas noches|que tal|qué tal|como va|cómo va)\b[\s,.:;!-]*/i, '')
    .trim();
}

function mergeDeferredNameRequestText(base = '', extra = '') {
  const parts = [];
  const seen = new Set();

  for (const raw of [base, extra]) {
    const clean = cleanDeferredNameRequestText(raw);
    if (!clean) continue;
    const key = normalize(clean);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    parts.push(clean);
  }

  return parts.join('\n').trim();
}

function buildContactPendingTopicLabel(text = '') {
  let clean = cleanDeferredNameRequestText(text);
  if (!clean) return '';
  clean = clean.replace(/[¿?]+/g, '').trim();
  if (clean.length > 80) clean = `${clean.slice(0, 77).trim()}...`;
  return clean ? `*${clean}*` : '';
}

function buildContactAskNameMessage(contextText = '') {
  const topic = buildContactPendingTopicLabel(contextText);
  return topic
    ? `Buen día 😊 No te tengo agendada todavía.

¿Te podría pedir por favor tu nombre así te registro bien y seguimos con ${topic}? Así también podés ver nuestras historias y publicaciones.`
    : `Buen día 😊 No te tengo agendada todavía.

¿Te podría pedir por favor tu nombre así te registro bien? Así también podés ver nuestras historias y publicaciones.`;
}

function buildContactAskNameReminderMessage(contextText = '') {
  const topic = buildContactPendingTopicLabel(contextText);
  return topic
    ? `Antes de seguir con ${topic}, ¿me pasás por favor tu nombre así te registro bien? 😊`
    : `Antes de seguir, ¿me pasás por favor tu nombre así te registro bien? 😊`;
}

function buildContactNameUpdatedMessage(firstName = '', options = {}) {
  const nice = titleCaseName(firstName || '');
  if (options?.resumeOriginalRequest) {
    return nice ? `Gracias ${nice} 😊` : `Gracias 😊`;
  }
  return nice
    ? `Gracias ${nice} 😊 Ya te registré correctamente. ¿En qué puedo ayudarte?`
    : `Gracias 😊 Ya te registré correctamente. ¿En qué puedo ayudarte?`;
}

const NAME_REPLY_AI_MAX_CHARS = Number(process.env.NAME_REPLY_AI_MAX_CHARS || 220);

function extractNameFromMixedTextFast(text = '') {
  const raw = String(text || '').replace(/\s+/g, ' ').trim();
  if (!raw) return '';

  const explicit = extractExplicitNameFromSnippet(raw);
  if (explicit) return explicit;

  const candidates = [];
  const beforeComma = raw.split(/[\n,;:-]/).map((x) => cleanNameCandidate(x)).find(Boolean);
  if (beforeComma) candidates.push(beforeComma);

  const leadingWords = raw.match(/^([A-Za-zÁÉÍÓÚÑáéíóúñ' ]{2,60})(?:\s+\d|[,;:-]|$)/);
  if (leadingWords) {
    const cleaned = cleanNameCandidate(leadingWords[1]);
    if (cleaned) candidates.push(cleaned);
  }

  return candidates.find((value) => value && !isLikelyGenericContactName(value)) || '';
}

function removeDetectedNameFromTextFast(text = '', explicitName = '') {
  const raw = String(text || '').replace(/\s+/g, ' ').trim();
  const name = String(explicitName || '').trim();
  if (!raw || !name) return '';

  const escaped = name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  let out = raw
    .replace(new RegExp(`^(?:me llamo|mi nombre es|soy|habla)\\s+${escaped}\\b[\\s,.:;-]*`, 'i'), '')
    .replace(new RegExp(`^${escaped}\\b[\\s,.:;-]*`, 'i'), '')
    .trim();

  return out;
}

function looksLikeNameCollectionRefusal(text = '') {
  const t = normalize(text || '');
  if (!t) return false;
  return /(no quiero dar(?:te|le|lo)? mi nombre|prefiero no dar(?:te|le|lo)? mi nombre|prefiero no decir(?:te|le|lo)? mi nombre|no te lo quiero pasar|no quiero pasar(?:te|le)? mi nombre|sin nombre|dejalo asi|dejalo así|despues te lo paso|después te lo paso)/i.test(t);
}

async function detectInboundNameWithAI(text = '', context = {}) {
  const raw = String(text || '').replace(/\s+/g, ' ').trim();
  if (!raw) return { hasName: false, explicitName: '', remainingText: '', isRefusal: false, source: 'empty' };

  const explicit = extractExplicitNameFromSnippet(raw);
  if (explicit) {
    return {
      hasName: true,
      explicitName: explicit,
      remainingText: removeDetectedNameFromTextFast(raw, explicit),
      isRefusal: false,
      source: 'pattern',
    };
  }

  const fastMixed = extractNameFromMixedTextFast(raw);
  if (fastMixed && !looksLikeNameCollectionRefusal(raw)) {
    const remainingFast = removeDetectedNameFromTextFast(raw, fastMixed);
    const compact = raw.replace(/\s+/g, ' ').trim();
    const compactWithoutName = remainingFast.replace(/\s+/g, ' ').trim();
    const safeStandalone = looksLikeStandaloneNameAnswer(raw) || (!compactWithoutName && compact.length <= 80);
    if (safeStandalone) {
      return { hasName: true, explicitName: fastMixed, remainingText: compactWithoutName, isRefusal: false, source: 'fast' };
    }
  }

  try {
    const completion = await openai.chat.completions.create({
      model: PRIMARY_MODEL,
      temperature: 0,
      messages: [
        {
          role: 'system',
          content:
`Analizá la respuesta de una persona cuando un salón le pidió su nombre por WhatsApp. Devolvé SOLO JSON válido con estas claves:
- has_name: boolean
- explicit_name: string
- remaining_text: string
- is_refusal: boolean

Reglas:
- explicit_name debe contener solo el nombre real de la persona, en formato natural.
- Si el texto contiene nombre y además otros datos o preguntas, separalos en remaining_text.
- Si el texto NO aporta un nombre real, has_name debe ser false.
- Rechazá como nombre palabras de belleza/comercial como botox, keratina, alisado, color, máscara, serum, ampollas, masterclass, producto, servicio, curso, turno y similares.
- Si la persona se niega a dar el nombre o posterga pasarlo, marcá is_refusal=true.
- No inventes nombres.`
        },
        {
          role: 'user',
          content: JSON.stringify({
            mensaje: raw.slice(0, NAME_REPLY_AI_MAX_CHARS),
            contexto_pendiente: String(context.pendingTopic || '').slice(0, 140),
            nombre_perfil_whatsapp: String(context.profileName || '').slice(0, 80),
          })
        }
      ],
      response_format: { type: 'json_object' },
    });

    const parsed = JSON.parse(completion.choices?.[0]?.message?.content || '{}');
    const explicitName = cleanNameCandidate(parsed?.explicit_name || '');
    const remainingText = String(parsed?.remaining_text || '').replace(/\s+/g, ' ').trim();
    return {
      hasName: !!parsed?.has_name && !!explicitName && !isLikelyGenericContactName(explicitName),
      explicitName: explicitName && !isLikelyGenericContactName(explicitName) ? explicitName : '',
      remainingText,
      isRefusal: !!parsed?.is_refusal,
      source: 'ai',
    };
  } catch {
    return { hasName: false, explicitName: '', remainingText: '', isRefusal: looksLikeNameCollectionRefusal(raw), source: 'fallback' };
  }
}

async function resolveInboundExplicitName(text = '', context = {}) {
  const raw = String(text || '').replace(/\s+/g, ' ').trim();
  if (!raw) return { hasName: false, explicitName: '', remainingText: '', isRefusal: false, source: 'empty' };

  const explicit = extractExplicitNameFromSnippet(raw);
  if (explicit) {
    return { hasName: true, explicitName: explicit, remainingText: removeDetectedNameFromTextFast(raw, explicit), isRefusal: false, source: 'pattern' };
  }

  if (context?.pendingNameRequest) {
    return detectInboundNameWithAI(raw, {
      pendingTopic: context?.pendingTopic || context?.pendingNameRequest?.deferredText || '',
      profileName: context?.profileName || '',
    });
  }

  const cleanStandalone = cleanNameCandidate(raw);
  if (cleanStandalone && looksLikeStandaloneNameAnswer(raw) && !isLikelyGenericContactName(cleanStandalone)) {
    return { hasName: true, explicitName: cleanStandalone, remainingText: '', isRefusal: false, source: 'fast' };
  }

  return { hasName: false, explicitName: '', remainingText: '', isRefusal: false, source: 'none' };
}

function shouldAskContactNameFirst({ waId = '', text = '', explicitName = '', pendingNameRequest = null } = {}) {
  if (pendingNameRequest?.awaiting) return false;
  if (explicitName) return false;

  const conv = ensureConv(waId);
  const messageCount = Array.isArray(conv?.messages) ? conv.messages.length : 0;
  if (messageCount !== 0) return false;

  const raw = String(text || '').trim();
  return !!raw;
}

function buildGoogleContactsNote(ctx = {}) {
  const pieces = [];
  if (ctx?.clientId) pieces.push(`Cliente: ${ctx.clientId}`);
  if (ctx?.phone) pieces.push(`WhatsApp: +${normalizePhoneDigits(ctx.phone)}`);
  pieces.push('Origen: WhatsApp bot');
  return pieces.join(' | ');
}

async function getGoogleContactsOAuthClient(target) {
  if (!target?.refreshToken) throw new Error('GOOGLE_CONTACTS_REFRESH_TOKEN faltante');
  if (isGoogleContactsTargetDisabled(target)) {
    const err = new Error(`Google Contacts deshabilitado para ${target.email}`);
    err.googleContactsDisabled = true;
    throw err;
  }

  const oauth2 = new google.auth.OAuth2(
    GOOGLE_CONTACTS_CLIENT_ID,
    GOOGLE_CONTACTS_CLIENT_SECRET,
    GOOGLE_CONTACTS_REDIRECT_URI
  );
  oauth2.setCredentials({ refresh_token: target.refreshToken });

  try {
    await oauth2.getAccessToken();
    return oauth2;
  } catch (e) {
    if (isGoogleInvalidGrantError(e)) {
      markGoogleContactsTargetInvalidGrant(target, e);
      const err = new Error(`invalid_grant Google Contacts (${target.email})`);
      err.code = 'invalid_grant';
      err.googleContactsDisabled = true;
      err.original = e;
      throw err;
    }
    throw e;
  }
}

async function getGooglePeopleClient(target) {
  const auth = await getGoogleContactsOAuthClient(target);
  return google.people({ version: 'v1', auth });
}

async function warmupGoogleContactSearch(people) {
  try {
    await people.people.searchContacts({ query: '', readMask: 'names,phoneNumbers,metadata' });
  } catch {}
}

function getGoogleContactDisplayName(person) {
  const names = Array.isArray(person?.names) ? person.names : [];
  const primary = names.find((n) => n?.metadata?.primary) || names[0] || {};
  return titleCaseName(primary.displayName || [primary.givenName, primary.familyName].filter(Boolean).join(' '));
}

function getGoogleContactEtag(person) {
  const sources = Array.isArray(person?.metadata?.sources) ? person.metadata.sources : [];
  const src = sources.find((s) => s?.etag) || {};
  return src.etag || '';
}

function getGoogleContactBiographyText(person) {
  const bios = Array.isArray(person?.biographies) ? person.biographies : [];
  const primary = bios.find((b) => b?.metadata?.primary) || bios[0] || {};
  return String(primary?.value || '').replace(/\s+/g, ' ').trim();
}

function googleContactHasPhone(person, phoneRaw) {
  const target = normalizeComparablePhone(phoneRaw || '');
  if (!target) return false;
  const phones = Array.isArray(person?.phoneNumbers) ? person.phoneNumbers : [];
  return phones.some((p) => normalizeComparablePhone(p?.value || '') === target);
}

function isGoogleFailedPreconditionEtagError(err) {
  const responseData = err?.response?.data || {};
  const nestedError = responseData?.error || {};
  const message = String(nestedError?.message || responseData?.message || err?.message || '').toLowerCase();
  const status = String(nestedError?.status || responseData?.status || '').toUpperCase();
  return status === 'FAILED_PRECONDITION' || message.includes('person.etag') || message.includes('clear local cache');
}

function isGoogleNotFoundError(err) {
  const responseData = err?.response?.data || {};
  const nestedError = responseData?.error || {};
  const message = String(nestedError?.message || responseData?.message || err?.message || '').toLowerCase();
  const status = String(nestedError?.status || responseData?.status || '').toUpperCase();
  const code = Number(nestedError?.code || responseData?.code || err?.code || 0);
  return code === 404 || status === 'NOT_FOUND' || message.includes('not found');
}

function buildGoogleNamePayload(fullName) {
  const parts = splitNameParts(fullName);
  return [{
    displayName: parts.fullName || fullName,
    givenName: parts.firstName || '',
    familyName: parts.lastName || '',
  }];
}

function buildGoogleContactCreateBody({ normalizedPhone = '', finalName = '', note = '' } = {}) {
  const body = {};
  if (normalizedPhone) body.phoneNumbers = [{ value: normalizedPhone }];
  if (finalName) body.names = buildGoogleNamePayload(finalName);
  if (note) body.biographies = [{ value: note }];
  return body;
}

function buildGoogleContactUpdatePlan(person, { normalizedPhone = '', finalName = '', note = '' } = {}) {
  const currentName = getGoogleContactDisplayName(person);
  const currentNote = getGoogleContactBiographyText(person);
  const shouldUpdateName = !!finalName && (isLikelyGenericContactName(currentName) || normalize(currentName) !== normalize(finalName));
  const shouldUpdateNote = !!note && normalize(currentNote) !== normalize(note);
  const shouldUpdatePhone = !!normalizedPhone && !googleContactHasPhone(person, normalizedPhone);

  if (!shouldUpdateName && !shouldUpdateNote && !shouldUpdatePhone) {
    return {
      shouldUpdate: false,
      displayName: currentName,
      requestBody: null,
      updateFields: '',
    };
  }

  const requestBody = { etag: getGoogleContactEtag(person) };
  const fields = [];

  if (shouldUpdatePhone) {
    requestBody.phoneNumbers = [{ value: normalizedPhone }];
    fields.push('phoneNumbers');
  }
  if (shouldUpdateName) {
    requestBody.names = buildGoogleNamePayload(finalName);
    fields.push('names');
  }
  if (shouldUpdateNote) {
    requestBody.biographies = [{ value: note }];
    fields.push('biographies');
  }

  return {
    shouldUpdate: true,
    displayName: finalName || currentName,
    requestBody,
    updateFields: Array.from(new Set(fields)).join(','),
  };
}

async function searchGoogleContactsByPhone(target, phoneRaw) {
  if (!target?.refreshToken || isGoogleContactsTargetDisabled(target)) return [];

  let people = null;
  try {
    people = await getGooglePeopleClient(target);
  } catch (e) {
    if (e?.googleContactsDisabled || isGoogleInvalidGrantError(e)) return [];
    throw e;
  }

  await warmupGoogleContactSearch(people);

  const candidates = Array.from(new Set([
    ...buildPhoneCandidates(phoneRaw),
    normalizePhoneDigits(phoneRaw || ''),
    normalizeComparablePhone(phoneRaw || ''),
  ].filter(Boolean)));

  const found = new Map();
  for (const q of candidates.slice(0, 6)) {
    try {
      const resp = await people.people.searchContacts({
        query: q,
        readMask: 'names,phoneNumbers,metadata,biographies',
        pageSize: 10,
      });
      const rows = Array.isArray(resp?.data?.results) ? resp.data.results : [];
      for (const item of rows) {
        const person = item?.person || {};
        const phones = Array.isArray(person?.phoneNumbers) ? person.phoneNumbers : [];
        const exact = phones.some((p) => normalizeComparablePhone(p?.value || '') === normalizeComparablePhone(phoneRaw || ''));
        if (exact && person?.resourceName && !found.has(person.resourceName)) found.set(person.resourceName, person);
      }
    } catch (e) {
      console.error(`❌ Error buscando Google Contacts en ${target.email}:`, e?.response?.data || e?.message || e);
    }
    if (found.size) break;
  }

  return Array.from(found.values());
}

async function upsertGoogleContactInTarget(target, { phoneRaw, explicitName = '', profileName = '', note = '' } = {}) {
  if (!target?.refreshToken || !phoneRaw) return { action: 'skipped' };
  if (isGoogleContactsTargetDisabled(target)) return { action: 'skipped_disabled', account: target.email };

  let people = null;
  try {
    people = await getGooglePeopleClient(target);
  } catch (e) {
    if (e?.googleContactsDisabled || isGoogleInvalidGrantError(e)) {
      return { action: 'skipped_disabled', account: target.email, error: 'invalid_grant' };
    }
    throw e;
  }

  const normalizedPhone = normalizeHubSpotPhone(phoneRaw) || `+${normalizePhoneDigits(phoneRaw)}`;
  const finalName = cleanNameCandidate(explicitName) || cleanNameCandidate(profileName) || '';
  const createBody = buildGoogleContactCreateBody({ normalizedPhone, finalName, note });

  const refreshPersonByResourceName = async (resourceName) => {
    if (!resourceName) return null;
    try {
      const resp = await people.people.get({
        resourceName,
        personFields: 'names,phoneNumbers,metadata,biographies',
      });
      return resp?.data || null;
    } catch (getErr) {
      if (isGoogleNotFoundError(getErr)) return null;
      throw getErr;
    }
  };

  const tryUpdatePerson = async (person) => {
    const plan = buildGoogleContactUpdatePlan(person, { normalizedPhone, finalName, note });
    if (!plan.shouldUpdate) {
      return { action: 'unchanged', resourceName: person.resourceName, displayName: plan.displayName, account: target.email };
    }

    await people.people.updateContact({
      resourceName: person.resourceName,
      updatePersonFields: plan.updateFields,
      requestBody: plan.requestBody,
    });

    return { action: 'updated', resourceName: person.resourceName, displayName: plan.displayName, account: target.email };
  };

  const updateExistingPersonWithRetry = async (existing) => {
    try {
      return await tryUpdatePerson(existing);
    } catch (e) {
      if (e?.googleContactsDisabled || isGoogleInvalidGrantError(e)) {
        return { action: 'skipped_disabled', account: target.email, error: 'invalid_grant' };
      }

      if (!isGoogleFailedPreconditionEtagError(e) && !isGoogleNotFoundError(e)) {
        throw e;
      }

      let freshPerson = await refreshPersonByResourceName(existing?.resourceName || '');
      if (!freshPerson) {
        await sleep(700);
        freshPerson = await refreshPersonByResourceName(existing?.resourceName || '');
      }

      if (!freshPerson) {
        const created = await people.people.createContact({ requestBody: createBody });
        return { action: isGoogleNotFoundError(e) ? 'recreated' : 'created_after_refresh', resourceName: created?.data?.resourceName || '', displayName: finalName, account: target.email };
      }

      try {
        const retried = await tryUpdatePerson(freshPerson);
        return { ...retried, action: retried.action === 'updated' ? 'updated_after_refresh' : retried.action };
      } catch (retryErr) {
        if (retryErr?.googleContactsDisabled || isGoogleInvalidGrantError(retryErr)) {
          return { action: 'skipped_disabled', account: target.email, error: 'invalid_grant' };
        }

        if (isGoogleFailedPreconditionEtagError(retryErr)) {
          await sleep(700);
          const latestPerson = await refreshPersonByResourceName(freshPerson?.resourceName || existing?.resourceName || '');
          if (!latestPerson) {
            const created = await people.people.createContact({ requestBody: createBody });
            return { action: 'recreated', resourceName: created?.data?.resourceName || '', displayName: finalName, account: target.email };
          }
          const thirdTry = await tryUpdatePerson(latestPerson);
          return { ...thirdTry, action: thirdTry.action === 'updated' ? 'updated_after_refresh' : thirdTry.action };
        }

        if (isGoogleNotFoundError(retryErr)) {
          const created = await people.people.createContact({ requestBody: createBody });
          return { action: 'recreated', resourceName: created?.data?.resourceName || '', displayName: finalName, account: target.email };
        }
        throw retryErr;
      }
    }
  };

  const existingRows = await searchGoogleContactsByPhone(target, phoneRaw);
  const uniqueExistingRows = Array.from(new Map((existingRows || []).filter(Boolean).map((person) => [person.resourceName || `${Math.random()}`, person])).values())
    .filter((person) => !!person?.resourceName);

  if (!uniqueExistingRows.length) {
    const created = await people.people.createContact({ requestBody: createBody });
    return { action: 'created', resourceName: created?.data?.resourceName || '', displayName: finalName, account: target.email };
  }

  const results = [];
  for (const existing of uniqueExistingRows) {
    try {
      const row = await updateExistingPersonWithRetry(existing);
      results.push(row);
    } catch (e) {
      if (e?.googleContactsDisabled || isGoogleInvalidGrantError(e)) {
        return { action: 'skipped_disabled', account: target.email, error: 'invalid_grant' };
      }
      console.error(`❌ Error actualizando contacto exacto en Google Contacts (${target.email}):`, e?.response?.data || e?.message || e);
      results.push({ action: 'error', account: target.email, resourceName: existing?.resourceName || '', error: e?.message || 'error' });
    }
  }

  const preferred = results.find((row) => /^updated|^recreated|^created/.test(String(row?.action || '')))
    || results.find((row) => row?.action === 'unchanged')
    || results[0];

  if (preferred) {
    return {
      ...preferred,
      account: target.email,
      matches: results,
    };
  }

  const created = await people.people.createContact({ requestBody: createBody });
  return { action: 'created_fallback', resourceName: created?.data?.resourceName || '', displayName: finalName, account: target.email, matches: results };
}

async function syncGoogleContactsForPhone({ phoneRaw, explicitName = '', profileName = '', note = '' } = {}) {
  if (!hasGoogleContactsSyncEnabled() || !phoneRaw) return [];
  const results = [];
  for (const target of GOOGLE_CONTACTS_TARGETS) {
    try {
      const row = await upsertGoogleContactInTarget(target, { phoneRaw, explicitName, profileName, note });
      results.push(row);
    } catch (e) {
      console.error(`❌ Error sincronizando Google Contacts (${target.email}):`, e?.response?.data || e?.message || e);
      results.push({ action: 'error', account: target.email, error: e?.message || 'error' });
    }
  }
  return results;
}

async function resolveKnownContactIdentity({ waId, phoneRaw, profileName = '' } = {}) {
  const cached = getCachedIdentityByWaId(waId);
  if (cached) return cached;

  const out = {
    shouldAskName: false,
    bestName: cleanNameCandidate(profileName) || '',
    bestSource: profileName ? 'whatsapp_profile' : '',
    hubspotContact: null,
    googleContacts: [],
  };

  if (hasHubSpotEnabled() && phoneRaw) {
    const hsMatches = await findHubSpotContactsByPhone(phoneRaw);
    const hs = chooseBestHubSpotMatch(hsMatches, phoneRaw);
    if (hs) {
      out.hubspotContact = hs;
      const props = hs.properties || {};
      const full = [props[HUBSPOT_PROPERTY.firstname] || '', props[HUBSPOT_PROPERTY.lastname] || ''].filter(Boolean).join(' ').trim();
      if (full && !isLikelyGenericContactName(full)) {
        out.bestName = cleanNameCandidate(full) || out.bestName;
        out.bestSource = 'hubspot';
      }
    }
  }

  if (hasGoogleContactsSyncEnabled() && phoneRaw) {
    for (const target of GOOGLE_CONTACTS_TARGETS) {
      try {
        const matches = await searchGoogleContactsByPhone(target, phoneRaw);
        const person = matches[0] || null;
        if (person) {
          const displayName = getGoogleContactDisplayName(person);
          out.googleContacts.push({ account: target.email, resourceName: person.resourceName || '', displayName });
          if (displayName && !isLikelyGenericContactName(displayName) && !out.bestName) {
            out.bestName = displayName;
            out.bestSource = `google:${target.email}`;
          }
        }
      } catch (e) {
        console.error(`❌ Error resolviendo identidad en Google Contacts (${target.email}):`, e?.response?.data || e?.message || e);
      }
    }
  }

  const cleanedProfile = cleanNameCandidate(profileName);
  if (!out.bestName && cleanedProfile && !isLikelyGenericContactName(cleanedProfile)) {
    out.bestName = cleanedProfile;
    out.bestSource = 'whatsapp_profile';
  }

  out.shouldAskName = !out.bestName || isLikelyGenericContactName(out.bestName);
  return setCachedIdentityByWaId(waId, out);
}

async function syncIdentityEverywhere({ waId, phoneRaw, profileName = '', explicitName = '' } = {}) {
  const cleanExplicit = cleanNameCandidate(explicitName);
  if (!phoneRaw || !cleanExplicit) return { ok: false };

  const googleResults = await syncGoogleContactsForPhone({
    phoneRaw,
    explicitName: cleanExplicit,
    profileName,
    note: buildGoogleContactsNote({ clientId: CLIENT_ID, phone: phoneRaw }),
  });

  let hubspotResult = null;
  try {
    hubspotResult = await upsertHubSpotContactIdentityOnly({
      waId,
      phone: normalizePhone(phoneRaw),
      phoneRaw,
      profileName,
      name: profileName,
      explicitName: cleanExplicit,
      forceContactName: cleanExplicit,
      lastUserText: '',
      intentType: 'OTHER',
      interest: null,
      suppressInactivityPrompt: false,
    });
  } catch (e) {
    console.error('❌ Error sincronizando nombre en HubSpot:', e?.response?.data || e?.message || e);
  }

  clearCachedIdentityByWaId(waId);
  setCachedIdentityByWaId(waId, {
    shouldAskName: false,
    bestName: cleanExplicit,
    bestSource: 'chat_explicit',
    hubspotContact: null,
    googleContacts: googleResults,
  });

  return { ok: true, googleResults, hubspotResult };
}


function detectCommercialFollowupKindFromContext(ctx = {}) {
  const fullText = [
    ctx?.interest || "",
    ctx?.lastUserText || "",
    ctx?.observacion || "",
    ctx?.producto || "",
    ctx?.categoria || "",
  ].join(" | ");

  const t = normalize(fullText);

  if (/\bcurso\b|\bcursos\b|\btaller\b|\bcapacitacion\b|\bcapacitación\b/.test(t)) {
    return "COURSE";
  }

  const hasProductSignal =
    !!detectProductFamily(fullText) ||
    !!pickPrimaryProducto(fullText) ||
    /\bshampoo\b|\bchampu\b|\bchampú\b|\bmatizador\b|\bnutricion\b|\bnutrición\b|\bserum\b|\bsérum\b|\bampolla\b|\btratamiento\b|\btintura\b|\bdecolorante\b|\boxidante\b|\bkeratina\b|\bproducto\b|\bstock\b|\binsumo\b/.test(t);

  if (hasProductSignal) return "PRODUCT";

  return "";
}

async function upsertCommercialFollowupCandidate(waId) {
  if (!ENABLE_COMMERCIAL_FOLLOWUPS || !waId) return;

  const ctx = lastCloseContext.get(waId) || {};
  const followupKind = detectCommercialFollowupKindFromContext(ctx);
  if (!followupKind) return;

  const phone = normalizePhone(ctx.phone || ctx.phoneRaw || "");
  if (!phone) return;

  const sendAfter = new Date(Date.now() + COMMERCIAL_FOLLOWUP_DELAY_MS);

  await db.query(
    `
    INSERT INTO commercial_followups (
      wa_id, wa_phone, phone_raw, profile_name, explicit_name,
      followup_kind, context_json, last_inbound_at, send_after_at, status, sent_at, last_error, created_at, updated_at
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,NOW(),$8,'pending',NULL,NULL,NOW(),NOW())
    ON CONFLICT (wa_id) DO UPDATE SET
      wa_phone = EXCLUDED.wa_phone,
      phone_raw = EXCLUDED.phone_raw,
      profile_name = EXCLUDED.profile_name,
      explicit_name = EXCLUDED.explicit_name,
      followup_kind = EXCLUDED.followup_kind,
      context_json = EXCLUDED.context_json,
      last_inbound_at = NOW(),
      send_after_at = EXCLUDED.send_after_at,
      status = 'pending',
      sent_at = NULL,
      last_error = NULL,
      updated_at = NOW()
    `,
    [
      waId,
      phone,
      String(ctx.phoneRaw || "").trim(),
      String(ctx.profileName || ctx.name || "").trim(),
      String(ctx.explicitName || "").trim(),
      followupKind,
      ctx || {},
      sendAfter.toISOString(),
    ]
  );
}

async function logCommercialFollowup({ waId, waPhone, followupKind, messageText, status, errorText = "" }) {
  await db.query(
    `
    INSERT INTO commercial_followup_logs (wa_id, wa_phone, followup_kind, message_text, status, error_text)
    VALUES ($1,$2,$3,$4,$5,$6)
    `,
    [
      waId,
      normalizePhone(waPhone || ""),
      followupKind || "",
      String(messageText || "").trim() || null,
      status,
      String(errorText || "").trim() || null,
    ]
  );
}

function getFriendlyFirstName(ctx = {}, contact = null) {
  const props = contact?.properties || {};
  return titleCaseName(
    props?.[HUBSPOT_PROPERTY.firstname] ||
    ctx?.explicitName ||
    ctx?.profileName ||
    ctx?.name ||
    ""
  );
}

function compactCommercialText(value = '', maxLen = 90) {
  const txt = String(value || '').replace(/\s+/g, ' ').trim();
  if (!txt) return '';
  if (txt.length <= maxLen) return txt;
  return `${txt.slice(0, Math.max(0, maxLen - 3)).trim()}...`;
}

function stableCommercialHash(value = '') {
  const raw = String(value || '');
  let hash = 0;
  for (let i = 0; i < raw.length; i += 1) {
    hash = ((hash << 5) - hash) + raw.charCodeAt(i);
    hash |= 0;
  }
  return Math.abs(hash);
}

function pickCommercialVariant(seed = '', options = []) {
  const rows = Array.isArray(options) ? options.filter(Boolean) : [];
  if (!rows.length) return '';
  return rows[stableCommercialHash(seed) % rows.length] || rows[0] || '';
}

function parseCommercialMoneyNumber(value = '') {
  const raw = String(value || '').trim();
  if (!raw) return null;
  const cleaned = raw.replace(/[^\d,.-]/g, '');
  if (!cleaned) return null;
  if (cleaned.includes(',') && cleaned.includes('.')) {
    const normalized = cleaned.replace(/\./g, '').replace(',', '.');
    const num = Number(normalized);
    return Number.isFinite(num) ? num : null;
  }
  if (cleaned.includes(',') && !cleaned.includes('.')) {
    const normalized = cleaned.replace(',', '.');
    const num = Number(normalized);
    return Number.isFinite(num) ? num : null;
  }
  const num = Number(cleaned);
  return Number.isFinite(num) ? num : null;
}

function resolveCommercialPromoPrices(row = {}) {
  const beforeRaw = String(row?.precioSinPromocion || '').trim();
  const nowRaw = String(row?.precio || '').trim();
  if (!beforeRaw || !nowRaw) return null;

  const beforeNum = parseCommercialMoneyNumber(beforeRaw);
  const nowNum = parseCommercialMoneyNumber(nowRaw);
  if (beforeNum != null && nowNum != null && beforeNum <= nowNum) return null;

  return {
    beforeRaw,
    nowRaw,
    beforeNum,
    nowNum,
    beforeText: moneyOrConsult(beforeRaw),
    nowText: moneyOrConsult(nowRaw),
  };
}

function hasCommercialPromoPrices(row = {}) {
  return !!resolveCommercialPromoPrices(row);
}

function normalizeTruthyHubSpotValue(value = '') {
  const t = normalize(String(value || '').trim());
  if (!t) return false;
  return /^(si|sí|true|1|yes|x|ok|alumno|alumna|activo|activa)$/i.test(t)
    || t.includes('si✅')
    || t.includes('si')
    || t.includes('alumno')
    || t.includes('alumna');
}

function splitCommercialCourseValues(value = '') {
  const raw = String(value || '').replace(/[•·]/g, '\n');
  return raw
    .split(/[\n,;|/]+/g)
    .map((x) => String(x || '').replace(/\s+/g, ' ').trim())
    .filter((x) => x && x.length >= 3)
    .slice(0, 30);
}

function getHubSpotStudentSignals(contact = null) {
  const props = contact?.properties || {};
  const studentValue = HUBSPOT_STUDENT_PROPERTY ? props?.[HUBSPOT_STUDENT_PROPERTY] : '';
  const coursesValue = HUBSPOT_STUDENT_COURSES_PROPERTY ? props?.[HUBSPOT_STUDENT_COURSES_PROPERTY] : '';
  return {
    isStudent: normalizeTruthyHubSpotValue(studentValue),
    studentValue: String(studentValue || '').trim(),
    studiedCourses: splitCommercialCourseValues(coursesValue),
  };
}

async function getStudentProfileForCommercialFollowup({ waId = '', phone = '', contact = null } = {}) {
  const phoneNorm = normalizePhone(phone || '');
  const dbCourses = [];
  const seen = new Set();
  const addCourse = (value) => {
    const clean = String(value || '').replace(/\s+/g, ' ').trim();
    const key = normalize(clean);
    if (!clean || !key || seen.has(key)) return;
    seen.add(key);
    dbCourses.push(clean);
  };

  const params = [];
  const clauses = [];
  let idx = 1;
  if (waId) {
    clauses.push(`wa_id = $${idx++}`);
    params.push(String(waId));
  }
  if (phoneNorm) {
    clauses.push(`wa_phone = $${idx}`);
    clauses.push(`contact_phone = $${idx}`);
    params.push(phoneNorm);
    idx += 1;
  }

  if (clauses.length) {
    const sql = `
      SELECT course_name
        FROM course_enrollments
       WHERE (${clauses.join(' OR ')})
       ORDER BY updated_at DESC, created_at DESC
       LIMIT 50`;
    try {
      const r = await db.query(sql, params);
      for (const row of (r.rows || [])) addCourse(row?.course_name || '');
    } catch (e) {
      console.error('❌ Error leyendo historial de cursos para follow-up comercial:', e?.message || e);
    }
  }

  const hubspotSignals = getHubSpotStudentSignals(contact);
  for (const value of (hubspotSignals.studiedCourses || [])) addCourse(value);

  return {
    isStudent: !!(hubspotSignals.isStudent || dbCourses.length),
    studiedCourses: dbCourses,
    hubspotStudentValue: hubspotSignals.studentValue || '',
  };
}

function doesCourseMatchStudy(courseName = '', studiedCourses = []) {
  const base = normalize(String(courseName || '').trim());
  if (!base) return false;
  return (Array.isArray(studiedCourses) ? studiedCourses : []).some((value) => {
    const key = normalize(String(value || '').trim());
    return !!key && (key === base || key.includes(base) || base.includes(key));
  });
}

function filterCommercialAvailableCourses(rows = []) {
  return (Array.isArray(rows) ? rows : []).filter((row) => {
    const estado = normalize(String(row?.estado || '').trim());
    const cupos = normalize(String(row?.cupos || '').trim());
    if (/(cerrad|complet|agotad|sin cupo|no disponible|finalizad)/i.test(estado)) return false;
    if (/^0+(?:[.,]0+)?$/.test(cupos)) return false;
    return true;
  });
}

function buildCommercialProductActionLine() {
  return `Si le interesa, avíseme y se lo dejo preparado. Pasadas estas *${COMMERCIAL_PROMO_WINDOW_HOURS} hs*, la promoción deja de estar vigente.`;
}

function buildCommercialCourseActionLine() {
  return 'Si le interesa, avíseme y le paso lo que está disponible o avanzamos con la inscripción sin perder la oportunidad.';
}

function buildCommercialProductIntro({ firstName = '', isStudent = false, studiedCourses = [], domain = '' } = {}) {
  const saludo = firstName ? `Buen día ${firstName} 😊` : 'Buen día 😊';
  const studyHint = studiedCourses.length
    ? ` por lo que viene relacionado con ${studiedCourses.length === 1 ? `*${studiedCourses[0]}*` : 'lo que estudió'}`
    : '';

  const variants = isStudent
    ? [
        `${saludo}

Por ser *alumno/a*, le quedó por estas *${COMMERCIAL_PROMO_WINDOW_HOURS} hs* una promoción especial del *${COMMERCIAL_PROMO_DISCOUNT_PERCENT}% OFF* en opciones que le pueden servir para trabajar o para uso personal${studyHint} ✨`,
        `${saludo}

Le aviso porque como *alumno/a* tiene un beneficio especial del *${COMMERCIAL_PROMO_DISCOUNT_PERCENT}% OFF* durante estas *${COMMERCIAL_PROMO_WINDOW_HOURS} hs* en productos${studyHint}.`,
        `${saludo}

Como *alumno/a* le quedó una oportunidad especial por estas *${COMMERCIAL_PROMO_WINDOW_HOURS} hs*: *${COMMERCIAL_PROMO_DISCOUNT_PERCENT}% OFF* en productos que le pueden complementar muy bien${studyHint}.`,
      ]
    : [
        `${saludo}

Le escribo porque por estas *${COMMERCIAL_PROMO_WINDOW_HOURS} hs* le quedó una promoción exclusiva del *${COMMERCIAL_PROMO_DISCOUNT_PERCENT}% OFF* en opciones que van muy bien con lo que venía consultando ✨`,
        `${saludo}

Le aviso antes de que se cierre: durante estas *${COMMERCIAL_PROMO_WINDOW_HOURS} hs* tiene una promoción exclusiva del *${COMMERCIAL_PROMO_DISCOUNT_PERCENT}% OFF* en productos relacionados con lo que le interesó.`,
        `${saludo}

Le dejé seleccionadas algunas opciones que pueden complementar justo lo que estaba buscando, con *${COMMERCIAL_PROMO_DISCOUNT_PERCENT}% OFF* por estas *${COMMERCIAL_PROMO_WINDOW_HOURS} hs*.`,
      ];

  return pickCommercialVariant(`${firstName}|${isStudent}|${domain}|${studiedCourses.join('|')}`, variants);
}

function buildCommercialCourseIntro({ firstName = '', isStudent = false, studiedCourses = [], topCourseName = '' } = {}) {
  const saludo = firstName ? `Buen día ${firstName} 😊` : 'Buen día 😊';
  const studyHint = studiedCourses.length
    ? (studiedCourses.length === 1 ? ` por lo que ya estudió en *${studiedCourses[0]}*` : ' por lo que ya estuvo estudiando con nosotros')
    : '';

  const variants = isStudent
    ? [
        `${saludo}

Le aviso antes que se cierre la oportunidad: hay un curso que puede complementarle muy bien${studyHint}, y todavía está a tiempo de aprovechar los cupos.`,
        `${saludo}

Como *alumno/a*, quise avisarle antes que a otros porque quedó una opción de curso que puede servirle mucho${studyHint}.`,
        `${saludo}

Por ser *alumno/a*, le escribo para avisarle que todavía hay cupos en otra propuesta que puede sumarle muy bien${studyHint}.`,
      ]
    : [
        `${saludo}

Le escribo porque el curso que había consultado está moviendo los cupos y todavía está a tiempo de aprovechar la oportunidad ✨`,
        `${saludo}

Le aviso antes de que se cierren lugares: hay cursos con cupos en movimiento y puede reservar antes de perder la oportunidad.`,
        `${saludo}

Quise avisarle porque todavía está a tiempo de avanzar con un curso y no quedarse sin lugar.`,
      ];

  return pickCommercialVariant(`${firstName}|${isStudent}|${topCourseName}|${studiedCourses.join('|')}`, variants);
}

function buildCommercialPromoLines(rows = []) {
  return (Array.isArray(rows) ? rows : []).filter(Boolean).slice(0, 3).map((row) => {
    const promo = resolveCommercialPromoPrices(row);
    if (!promo) return '';
    const desc = compactCommercialText(row?.descripcion || '', 78);
    return [
      `• *${row.nombre}*`,
      `Antes: *${promo.beforeText}*`,
      `Ahora por estas *${COMMERCIAL_PROMO_WINDOW_HOURS} hs*: *${promo.nowText}*`,
      desc ? `• ${desc}` : '',
    ].filter(Boolean).join('\n');
  }).filter(Boolean);
}

async function getHubSpotContactForFollowup(ctx = {}) {
  let contact = null;
  if (ctx?.waId) contact = await findHubSpotContactByWaId(ctx.waId);
  if (!contact && (ctx?.phoneRaw || ctx?.phone)) {
    const matches = await findHubSpotContactsByPhone(ctx.phoneRaw || ctx.phone);
    contact = chooseBestHubSpotMatch(matches, ctx.phoneRaw || ctx.phone);
  }
  return contact || null;
}

async function buildCourseFollowupMessage({ ctx, contact }) {
  const props = contact?.properties || {};
  const studentProfile = await getStudentProfileForCommercialFollowup({
    waId: ctx?.waId || '',
    phone: ctx?.phoneRaw || ctx?.phone || '',
    contact,
  });

  const fullText = [
    props?.[HUBSPOT_PROPERTY.observacion] || '',
    props?.[HUBSPOT_PROPERTY.producto] || '',
    props?.[HUBSPOT_PROPERTY.categoria] || '',
    studentProfile.studiedCourses.join(' | '),
    ctx?.interest || '',
    ctx?.lastUserText || '',
    getConversationSnippetForClose(ctx?.waId || ''),
  ].join(' | ');

  const courses = filterCommercialAvailableCourses(await getCoursesCatalog());
  if (!courses.length) return '';

  let matches = findCourses(courses, fullText, 'DETAIL');
  if (!matches.length && ctx?.interest) matches = findCourses(courses, ctx.interest, 'DETAIL');
  if (!matches.length) matches = findCourses(courses, 'curso', 'LIST');

  let selected = matches.filter(Boolean);
  if (studentProfile.isStudent) {
    const notStudied = selected.filter((row) => !doesCourseMatchStudy(row?.nombre || '', studentProfile.studiedCourses));
    if (notStudied.length) selected = notStudied;
  }
  if (!selected.length && studentProfile.isStudent) {
    selected = courses.filter((row) => !doesCourseMatchStudy(row?.nombre || '', studentProfile.studiedCourses));
  }
  if (!selected.length) selected = courses.slice(0, 3);

  const top = selected[0];
  if (!top?.nombre) return '';

  const firstName = getFriendlyFirstName(ctx, contact);
  const intro = buildCommercialCourseIntro({
    firstName,
    isStudent: studentProfile.isStudent,
    studiedCourses: studentProfile.studiedCourses,
    topCourseName: top.nombre,
  });

  const detailLines = [
    `• *${top.nombre}*`,
    top.fechaInicio ? `• Inicio: ${top.fechaInicio}` : '',
    top.diasHorarios ? `• Días y horarios: ${top.diasHorarios}` : '',
    top.modalidad ? `• Modalidad: ${top.modalidad}` : '',
    top.precio ? `• Precio: *${moneyOrConsult(top.precio)}*` : '',
    top.sena ? `• Seña / inscripción: ${top.sena}` : '',
    top.cupos ? `• Cupos: ${top.cupos}` : '',
  ].filter(Boolean).join('\n');

  const secondaryCourse = selected.slice(1).find((row) => row?.nombre);
  let bridge = '';
  if (studentProfile.isStudent) {
    bridge = secondaryCourse
      ? `También le puede servir *${secondaryCourse.nombre}* como complemento.`
      : 'Si quiere, también puedo recomendarle otra opción relacionada o dejarle un cupo para un conocido cercano.';
  } else {
    bridge = secondaryCourse
      ? `Si quiere, también le puedo pasar otra opción disponible como *${secondaryCourse.nombre}*.`
      : 'Si quiere, también le puedo pasar otras opciones que hoy estén disponibles.';
  }

  return [
    intro,
    '',
    detailLines,
    '',
    bridge,
    buildCommercialCourseActionLine(),
  ].filter(Boolean).join('\n');
}

async function buildProductFollowupMessage({ ctx, contact }) {
  const props = contact?.properties || {};
  const studentProfile = await getStudentProfileForCommercialFollowup({
    waId: ctx?.waId || '',
    phone: ctx?.phoneRaw || ctx?.phone || '',
    contact,
  });

  const fullText = [
    props?.[HUBSPOT_PROPERTY.observacion] || '',
    props?.[HUBSPOT_PROPERTY.producto] || '',
    props?.[HUBSPOT_PROPERTY.categoria] || '',
    studentProfile.studiedCourses.join(' | '),
    ctx?.interest || '',
    ctx?.lastUserText || '',
    getConversationSnippetForClose(ctx?.waId || ''),
  ].join(' | ');

  const stock = await getStockCatalog();
  if (!stock.length) return '';

  const resolvedDomain = detectProductDomain(fullText) || 'hair';
  const resolvedFamily = resolvedDomain === 'furniture'
    ? (detectFurnitureFamily(fullText) || '')
    : (detectProductFamily(fullText) || '');
  const resolvedFocusTerm = detectProductFocusTerm(fullText) || '';

  const related = findStockRelated(stock, fullText, {
    domain: resolvedDomain,
    family: resolvedFamily,
    focusTerm: resolvedFocusTerm,
    limit: 40,
  }).filter((row) => hasCommercialPromoPrices(row));

  if (!related.length) return '';

  const treatmentKnowledge = resolvedDomain === 'hair'
    ? detectHairTreatmentKnowledge({ text: fullText, family: resolvedFamily, need: fullText })
    : null;

  const shortlist = shortlistProductsForRecommendation(related, {
    domain: resolvedDomain,
    query: fullText,
    family: resolvedFamily,
    focusTerm: resolvedFocusTerm,
    need: fullText,
    useType: normalizeUseType(fullText),
    limit: 6,
  }).filter((row) => hasCommercialPromoPrices(row));

  if (!shortlist.length) return '';

  const recoAI = await recommendProductsWithAI({
    text: fullText,
    domain: resolvedDomain,
    familyLabel: resolvedFamily,
    need: fullText,
    useType: normalizeUseType(fullText),
    treatmentKnowledge,
    historySnippet: buildConversationHistorySnippet(ensureConv(ctx?.waId || '').messages || [], 18, 2200),
    products: shortlist.slice(0, 6),
  });

  const pickedNames = new Set(
    Array.isArray(recoAI?.recommended_names) ? recoAI.recommended_names : []
  );

  let picked = shortlist.filter((x) => pickedNames.has(x.nombre)).slice(0, 3);
  if (!picked.length) picked = shortlist.slice(0, 2);
  if (!picked.length) return '';

  const firstName = getFriendlyFirstName(ctx, contact);
  const intro = buildCommercialProductIntro({
    firstName,
    isStudent: studentProfile.isStudent,
    studiedCourses: studentProfile.studiedCourses,
    domain: resolvedDomain,
  });

  const bodyLines = buildCommercialPromoLines(picked);
  if (!bodyLines.length) return '';

  const supportLine = compactCommercialText(recoAI?.sales_angle || recoAI?.rationale || recoAI?.follow_up || '', 110);

  return [
    intro,
    supportLine || '',
    '',
    bodyLines.join('\n\n'),
    '',
    buildCommercialProductActionLine(),
  ].filter(Boolean).join('\n');
}

async function processCommercialFollowups() {
  if (!ENABLE_COMMERCIAL_FOLLOWUPS) return;

  const r = await db.query(
    `
    SELECT *
    FROM commercial_followups
    WHERE status = 'pending'
      AND send_after_at <= NOW()
    ORDER BY send_after_at ASC
    LIMIT $1
    `,
    [COMMERCIAL_FOLLOWUP_MAX_PER_RUN]
  );

  for (const row of (r.rows || [])) {
    const waId = String(row.wa_id || "").trim();
    const waPhone = normalizePhone(row.wa_phone || "");
    const ctx = row.context_json || {};
    const followupKind = String(row.followup_kind || "").trim();

    try {
      if (!waId || !waPhone || !followupKind) continue;

      const claim = await db.query(
        `
        UPDATE commercial_followups
        SET status = 'processing', updated_at = NOW()
        WHERE wa_id = $1 AND status = 'pending'
        RETURNING wa_id
        `,
        [waId]
      );
      if (!claim.rows?.length) continue;

      const draft = await getAppointmentDraft(waId);
      if (draft) {
        await db.query(
          `UPDATE commercial_followups
           SET status = 'skipped', last_error = $2, updated_at = NOW()
           WHERE wa_id = $1`,
          [waId, "draft_abierto"]
        );
        await logCommercialFollowup({
          waId,
          waPhone,
          followupKind,
          messageText: "",
          status: "skipped",
          errorText: "draft_abierto",
        });
        continue;
      }

      const contact = await getHubSpotContactForFollowup({
        ...ctx,
        waId,
        phone: waPhone,
        phoneRaw: row.phone_raw || ctx.phoneRaw || waPhone,
      });

      let messageText = "";
      if (followupKind === "COURSE") {
        messageText = await buildCourseFollowupMessage({ ctx: { ...ctx, waId }, contact });
      } else if (followupKind === "PRODUCT") {
        messageText = await buildProductFollowupMessage({ ctx: { ...ctx, waId }, contact });
      }

      if (!messageText) {
        await db.query(
          `UPDATE commercial_followups
           SET status = 'skipped', last_error = $2, updated_at = NOW()
           WHERE wa_id = $1`,
          [waId, "sin_mensaje"]
        );
        await logCommercialFollowup({
          waId,
          waPhone,
          followupKind,
          messageText: "",
          status: "skipped",
          errorText: "sin_mensaje",
        });
        continue;
      }

      pushHistory(waId, "assistant", messageText);
      await sendWhatsAppText(waPhone, messageText);

      await db.query(
        `UPDATE commercial_followups
         SET status = 'sent', sent_at = NOW(), last_error = NULL, updated_at = NOW()
         WHERE wa_id = $1`,
        [waId]
      );

      await logCommercialFollowup({
        waId,
        waPhone,
        followupKind,
        messageText,
        status: "sent",
      });
    } catch (e) {
      const errTxt = e?.response?.data ? JSON.stringify(e.response.data) : (e?.message || "error");
      console.error("❌ Error procesando follow-up comercial:", errTxt);

      await db.query(
        `UPDATE commercial_followups
         SET status = 'error', last_error = $2, updated_at = NOW()
         WHERE wa_id = $1`,
        [waId, String(errTxt).slice(0, 1000)]
      );

      await logCommercialFollowup({
        waId,
        waPhone,
        followupKind,
        messageText: "",
        status: "error",
        errorText: errTxt,
      });
    }
  }
}


async function searchHubSpotContactsPaged(filterGroups = [], limit = 200, extraProperties = []) {
  if (!HUBSPOT_ACCESS_TOKEN || !Array.isArray(filterGroups) || !filterGroups.length) return [];

  const properties = Array.from(new Set([
    HUBSPOT_PROPERTY.firstname,
    HUBSPOT_PROPERTY.lastname,
    HUBSPOT_PROPERTY.phone,
    'mobilephone',
    HUBSPOT_PROPERTY.whatsappPhoneRaw,
    HUBSPOT_PROPERTY.whatsappPhoneNormalized,
    HUBSPOT_PROPERTY.whatsappProfileName,
    HUBSPOT_PROPERTY.fechaNacimiento,
    HUBSPOT_STUDENT_PROPERTY,
    HUBSPOT_STUDENT_COURSES_PROPERTY,
    ...extraProperties,
  ].filter(Boolean)));

  let after = undefined;
  const all = [];

  while (true) {
    const body = {
      filterGroups,
      limit,
      properties,
      ...(after ? { after } : {}),
    };

    const data = await hubspotRequest('post', '/crm/v3/objects/contacts/search', body);
    const rows = Array.isArray(data?.results) ? data.results : [];
    all.push(...rows);

    const nextAfter = data?.paging?.next?.after;
    if (!nextAfter) break;
    after = nextAfter;
  }

  return all;
}

function getTodayPartsInSalonTZ() {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: TIMEZONE,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).formatToParts(new Date());

  const get = (t) => parts.find((p) => p.type === t)?.value || '';
  return {
    year: get('year'),
    month: get('month'),
    day: get('day'),
    ymd: `${get('year')}-${get('month')}-${get('day')}`,
  };
}

function parseHubSpotBirthdayMonthDay(rawValue) {
  const raw = String(rawValue || '').trim();
  if (!raw) return null;

  if (/^\d{11,15}$/.test(raw)) {
    const n = Number(raw);
    if (Number.isFinite(n)) {
      const d = new Date(n);
      if (!Number.isNaN(d.getTime())) {
        return {
          month: String(d.getUTCMonth() + 1).padStart(2, '0'),
          day: String(d.getUTCDate()).padStart(2, '0'),
          iso: d.toISOString(),
        };
      }
    }
  }

  const iso = raw.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (iso) {
    return {
      month: iso[2],
      day: iso[3],
      iso: raw,
    };
  }

  const d = new Date(raw);
  if (!Number.isNaN(d.getTime())) {
    return {
      month: String(d.getUTCMonth() + 1).padStart(2, '0'),
      day: String(d.getUTCDate()).padStart(2, '0'),
      iso: d.toISOString(),
    };
  }

  return null;
}

function getBestContactPhoneForBirthday(props = {}) {
  return (
    props[HUBSPOT_PROPERTY.whatsappPhoneNormalized] ||
    props[HUBSPOT_PROPERTY.whatsappPhoneRaw] ||
    props.mobilephone ||
    props[HUBSPOT_PROPERTY.phone] ||
    ''
  );
}

function getBestContactNameForBirthday(props = {}) {
  const full = [props.firstname || '', props.lastname || ''].filter(Boolean).join(' ').trim();
  if (full) return titleCaseName(full);
  if (props[HUBSPOT_PROPERTY.whatsappProfileName]) return titleCaseName(props[HUBSPOT_PROPERTY.whatsappProfileName]);
  return '';
}

function getBestContactFirstNameForBirthday(props = {}) {
  const first = titleCaseName(props.firstname || '');
  if (first) return first;
  const fallback = getBestContactNameForBirthday(props);
  return titleCaseName((fallback || '').split(' ')[0] || '');
}

function buildBirthdayMessage(contactProps = {}) {
  const firstName = getBestContactFirstNameForBirthday(contactProps);
  return `${firstName ? `¡Feliz cumpleaños, ${firstName}!` : '¡Feliz cumpleaños!'} 🎉💖

Hoy queremos saludarte con mucho cariño de parte de todo el salón de Cataleya.
Gracias por ser parte de esta familia tan linda. Esperamos que tengas un día hermoso, lleno de alegría y cosas lindas ✨

Por ser alumno/a especial, hoy tenés un beneficio exclusivo para productos de Cataleya.
Si quiere, le cuento cuál es la promo disponible 😊`;
}

async function wasBirthdayMessageSentToday(hubspotContactId, ymd) {
  const r = await db.query(
    `SELECT 1
       FROM birthday_message_logs
      WHERE hubspot_contact_id = $1
        AND sent_date = $2::date
      LIMIT 1`,
    [String(hubspotContactId), String(ymd)]
  );
  return !!r.rows?.length;
}

async function logBirthdayMessage({ hubspotContactId, waPhone, sentDate, contactName, birthdayValue, messageText }) {
  await db.query(
    `INSERT INTO birthday_message_logs (
      hubspot_contact_id, wa_phone, sent_date, contact_name, birthday_value, message_text
    ) VALUES ($1,$2,$3::date,$4,$5,$6)
    ON CONFLICT (hubspot_contact_id, sent_date) DO NOTHING`,
    [
      String(hubspotContactId),
      normalizePhone(waPhone || ''),
      String(sentDate),
      String(contactName || ''),
      String(birthdayValue || ''),
      String(messageText || ''),
    ]
  );
}

async function processBirthdayMessages() {
  if (!ENABLE_BIRTHDAY_MESSAGES || !hasHubSpotEnabled()) return;

  const today = getTodayPartsInSalonTZ();

  let contacts = [];
  try {
    contacts = await searchHubSpotContactsPaged([
      {
        filters: [
          {
            propertyName: HUBSPOT_PROPERTY.fechaNacimiento,
            operator: 'HAS_PROPERTY',
          }
        ]
      }
    ], 200, [HUBSPOT_PROPERTY.fechaNacimiento]);
  } catch (e) {
    console.error('❌ Error buscando cumpleaños en HubSpot:', e?.response?.data || e?.message || e);
    return;
  }

  for (const contact of (contacts || [])) {
    try {
      const props = contact?.properties || {};
      const rawBirthday = props[HUBSPOT_PROPERTY.fechaNacimiento];
      const parsed = parseHubSpotBirthdayMonthDay(rawBirthday);
      if (!parsed) continue;
      if (parsed.month !== today.month || parsed.day !== today.day) continue;

      const recipient = normalizeWhatsAppRecipient(getBestContactPhoneForBirthday(props));
      if (!recipient) continue;

      const alreadySent = await wasBirthdayMessageSentToday(contact.id, today.ymd);
      if (alreadySent) continue;

      const messageText = buildBirthdayMessage(props);
      await sendWhatsAppText(recipient, messageText);

      await logBirthdayMessage({
        hubspotContactId: contact.id,
        waPhone: recipient,
        sentDate: today.ymd,
        contactName: getBestContactNameForBirthday(props),
        birthdayValue: rawBirthday,
        messageText,
      });

      console.log(`🎂 Feliz cumpleaños enviado a ${recipient} (${contact.id})`);
    } catch (e) {
      console.error('❌ Error enviando cumpleaños:', e?.response?.data || e?.message || e);
    }
  }
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


async function extractStrictCoursePaymentProofFromImage(filename) {
  const safeName = String(filename || "").trim();
  if (!safeName) {
    return { ok: false, es_comprobante: false, titular: "", monto: "", texto_visible: "" };
  }

  const fullPath = path.join(MEDIA_DIR, safeName);
  if (!fs.existsSync(fullPath)) {
    return { ok: false, es_comprobante: false, titular: "", monto: "", texto_visible: "" };
  }

  const ext = path.extname(fullPath).toLowerCase();
  const mime =
    ext === ".png" ? "image/png" :
    ext === ".webp" ? "image/webp" :
    "image/jpeg";

  try {
    const dataUrl = fileToDataUrl(fullPath, mime);

    const resp = await openai.chat.completions.create({
      model: COMPLEX_MODEL,
      messages: [
        {
          role: "system",
          content:
`Mirá esta imagen SOLO como comprobante de pago o transferencia.

Devolvé SOLO JSON válido con estas claves:
- es_comprobante: boolean
- titular: string
- monto: string
- texto_visible: string

Reglas:
- "titular" debe contener únicamente el nombre del titular visible.
- "monto" debe contener únicamente el monto visible.
- No pongas CVU, alias, banco, número de operación, motivo ni otros datos dentro de "titular" o "monto".
- Si se ve "Monica Pacheco", devolvé exactamente "Monica Pacheco".
- Si se ve "$ 10.000", devolvé exactamente "$10.000".
- En "texto_visible" transcribí solo lo mínimo útil para validar titular y monto.
- No inventes nada.`
        },
        {
          role: "user",
          content: [
            { type: "text", text: "Extraé únicamente el titular y el monto visibles del comprobante." },
            { type: "image_url", image_url: { url: dataUrl } },
          ],
        },
      ],
      response_format: { type: "json_object" },
    });

    const obj = safeJsonParseFromText(resp.choices?.[0]?.message?.content || "") || {};

    return {
      ok: true,
      es_comprobante: !!obj.es_comprobante,
      titular: String(obj.titular || "").trim(),
      monto: String(obj.monto || "").trim(),
      texto_visible: String(obj.texto_visible || "").trim(),
    };
  } catch {
    return { ok: false, es_comprobante: false, titular: "", monto: "", texto_visible: "" };
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
- Si el cliente manda solo un saludo y NO hay un tema activo reciente, respondé con saludo breve + “¿en qué puedo ayudarte?”.
- Si ya venían hablando de algo, un “hola”, “holaa”, “chau”, “gracias”, “ok” o saludo corto NO reinicia la conversación: seguí el tema activo.
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
- Si preguntan por cursos, respondé SOLO con lo que exista en la hoja CURSOS. Nunca enumeres ni inventes cursos por tu cuenta. Si no encontrás coincidencias, decí claramente que por el momento no hay uno similar y que le vamos a avisar cuando salga algo relacionado.

TURNOS:
- Información de turnos (siempre):
  - Estilista: Flavia Rueda.
  - Primero ofrecer SOLO horarios comerciales de lunes a sábados: 10:00, 11:00, 12:00, 17:00, 18:00, 19:00 y 20:00.
  - Si el cliente dice que ninguno de esos le sirve, recién ahí abrir la franja especial de siesta: 14:00, 15:00 y 16:00.
  - El orden correcto es: elegir servicio + día + horario + datos de contacto -> consultar con la estilista -> si la estilista acepta, recién ahí pedir la seña obligatoria de $10.000 -> cuando la seña esté validada, el turno queda confirmado.
  - Alias para transferir: Cataleya178
  sale a nombre Monica Pacheco. Luego debe enviar foto/captura del comprobante.
  - No digas que el turno está reservado ni confirmado antes de que la estilista acepte y la seña quede validada.
  - Al registrar un turno, solicitar nombre completo y teléfono de contacto.
- No inventes precios ni servicios: solo los que figuran en el Excel de servicios. 
- NO se ofrece lifting de pestañas, cejas, perfilado, uñas, limpiezas faciales ni otros servicios fuera del Excel.

- si busca Corte masculino / varón / hombre: es SOLO por orden de llegada, no se toma turno. Horario: Lunes a Sábados 10 a 13 hs y 17 a 22 hs. Precio final: $10.000 PESOS

- Horario del salón comercial:
Solo los Lunes:
08:00 a 13:00 hs
15:00 a 22:00 hs

Martes:
15:00 a 22:00 hs

Miércoles a Sábados:
17:00 a 22:00 hs

Si preguntan por precios, stock u opciones, usá los catálogos cuando sea posible.
Para productos capilares, razoná como una profesional del rubro: entendé el paso del tratamiento, el objetivo (hidratación, reparación, color, alisado, barbería, finalización) y sugerí complementos reales SOLO si existen en catálogo.
Si detectás uso personal, recomendá en tono de cuidado y mantenimiento. Si detectás uso profesional, recomendá en tono de trabajo, rendimiento, terminación y venta complementaria para el salón.
Cuando respondas con productos/servicios/cursos, NO reescribas los nombres: copiá el "Nombre" tal cual figura en el Excel.
`.trim();

// ===================== MEMORIA DIARIA (leads) =====================
const dailyLeads = new Map();

// ===================== HISTORIAL CORTO =====================
const conversations = new Map();
// ✅ MEMORIA: mantener contexto por al menos 10 horas
const CONV_TTL_MS = 30 * 24 * 60 * 60 * 1000; // 30 días


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

const INACTIVITY_MS = Number(process.env.INACTIVITY_FOLLOWUP_MS || 15 * 60 * 1000); // 15 minutos por defecto (mensaje de cierre)
const CLOSE_LOG_MS = Number(process.env.CLOSE_LOG_MS || 0);  // 0 por defecto: se registra al cumplirse la inactividad

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

  try {
    const courseDraft = await getCourseEnrollmentDraft(waId);
    if (courseDraft) return true;
  } catch (e) {
    console.error("Error verificando borrador de inscripción para inactividad:", e?.response?.data || e?.message || e);
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

    const runCloseLog = async () => {
      try {
        const suppress = await shouldSuppressInactivityFollowUp(waId);
        if (suppress) return;
        await logConversationClose(waId);
      } catch (e) {
        console.error("Error guardando seguimiento por cierre:", e?.response?.data || e?.message || e);
      }
    };

    if (CLOSE_LOG_MS > 0) {
      const timer2 = setTimeout(runCloseLog, CLOSE_LOG_MS);
      closeTimers.set(waId, timer2);
    } else {
      await runCloseLog();
    }
  }, INACTIVITY_MS);

  inactivityTimers.set(waId, timer);
}

// Último producto por usuario (para “mandame foto” sin repetir nombre)
const lastProductByUser = new Map();
// ✅ Contexto de producto por usuario (para continuar la charla de forma fluida)
const lastProductContextByUser = new Map();
const PRODUCT_CONTEXT_TTL_MS = Number(process.env.PRODUCT_CONTEXT_TTL_MS || 7 * 24 * 60 * 60 * 1000);

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
const COURSE_CONTEXT_TTL_MS = Number(process.env.COURSE_CONTEXT_TTL_MS || 7 * 24 * 60 * 60 * 1000);

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


// ✅ Última oferta/respuesta activa del asistente (para interpretar mejor respuestas como
// "pasá fotos", "quiero ese", "más info", "quiero avanzar", etc.)
const activeAssistantOfferByUser = new Map();
const ACTIVE_ASSISTANT_OFFER_TTL_MS = Number(process.env.ACTIVE_ASSISTANT_OFFER_TTL_MS || 7 * 24 * 60 * 60 * 1000);

function normalizeActiveOfferItems(items = []) {
  const out = [];
  const seen = new Set();
  for (const raw of (Array.isArray(items) ? items : [])) {
    const clean = String(raw || '').trim();
    const key = normalize(clean);
    if (!clean || !key || seen.has(key)) continue;
    seen.add(key);
    out.push(clean);
  }
  return out.slice(0, 12);
}

function getActiveAssistantOffer(waId) {
  const row = activeAssistantOfferByUser.get(waId);
  if (!row) return null;
  if ((Date.now() - Number(row.ts || 0)) > ACTIVE_ASSISTANT_OFFER_TTL_MS) {
    activeAssistantOfferByUser.delete(waId);
    return null;
  }
  return row;
}

function setActiveAssistantOffer(waId, patch = {}) {
  if (!waId) return null;
  const prev = getActiveAssistantOffer(waId) || {};
  const items = normalizeActiveOfferItems(
    patch.items !== undefined ? patch.items : (prev.items || [])
  );
  const selectedNameRaw = String(
    patch.selectedName !== undefined ? patch.selectedName : (prev.selectedName || '')
  ).trim();
  const selectedName = selectedNameRaw || (items.length === 1 ? items[0] : '');
  const next = {
    ...prev,
    ...patch,
    items,
    selectedName,
    type: String(patch.type || prev.type || '').trim().toUpperCase(),
    ts: Date.now(),
  };
  activeAssistantOfferByUser.set(waId, next);
  return next;
}

function clearActiveAssistantOffer(waId) {
  if (waId) activeAssistantOfferByUser.delete(waId);
}

function rememberAssistantProductOffer(waId, rows = [], extra = {}) {
  const items = normalizeActiveOfferItems((Array.isArray(rows) ? rows : []).map((x) => x?.nombre || x).filter(Boolean));
  return setActiveAssistantOffer(waId, {
    type: 'PRODUCT',
    items,
    selectedName: extra.selectedName || (items.length === 1 ? items[0] : ''),
    mode: extra.mode || '',
    domain: extra.domain || '',
    family: extra.family || '',
    focusTerm: extra.focusTerm || '',
    questionKind: extra.questionKind || '',
    lastAssistantText: extra.lastAssistantText || '',
  });
}

function rememberAssistantCourseOffer(waId, rows = [], extra = {}) {
  const items = normalizeActiveOfferItems((Array.isArray(rows) ? rows : []).map((x) => x?.nombre || x).filter(Boolean));
  return setActiveAssistantOffer(waId, {
    type: 'COURSE',
    items,
    selectedName: extra.selectedName || (items.length === 1 ? items[0] : ''),
    mode: extra.mode || '',
    questionKind: extra.questionKind || '',
    lastAssistantText: extra.lastAssistantText || '',
  });
}

function rememberAssistantServiceOffer(waId, rows = [], extra = {}) {
  const items = normalizeActiveOfferItems((Array.isArray(rows) ? rows : []).map((x) => cleanServiceName(x?.nombre || x)).filter(Boolean));
  return setActiveAssistantOffer(waId, {
    type: 'SERVICE',
    items,
    selectedName: extra.selectedName || (items.length === 1 ? items[0] : ''),
    mode: extra.mode || '',
    questionKind: extra.questionKind || '',
    lastAssistantText: extra.lastAssistantText || '',
  });
}

function looksLikeActiveOfferFollowup(text = '') {
  const t = normalize(text || '');
  if (!t) return false;
  if (/^(ese|esa|esos|esas|de ese|de esa|de esos|de esas|ese curso|ese servicio|ese producto|el primero|la primera|el segundo|la segunda|el tercero|la tercera|quiero ese|quiero esa|mas info|más info|info|precio|cuanto sale|cuánto sale|cuanto cuesta|cuánto cuesta|cuanto dura|cuánto dura|cuando empieza|cuándo empieza|horario|horarios|cupos|requisitos|quiero avanzar|quiero seguir|quiero continuar|quiero reservar|quiero inscribirme|quiero anotarme|quiero turno|pasame foto|pásame foto|pasame fotos|pásame fotos|mandame foto|mandame fotos|mostrame|mostrame fotos|ver|ok|oka|dale|bien|perfecto)$/.test(t)) return true;
  if (userAsksForPhoto(text)) return true;
  if (/(quiero (ese|esa|seguir|continuar|avanzar|reservar|inscribirme|anotarme|turno)|de ese|de esa|precio del primero|precio del segundo|foto del primero|foto del segundo|material del curso|pasame el material|pasame material|me interesa ese)/i.test(t)) return true;
  return false;
}

async function resolveReplyToActiveAssistantOfferWithAI(text, context = {}) {
  const raw = String(text || '').trim();
  const activeOffer = context.activeOffer || null;
  if (!raw || !activeOffer?.type) {
    return { action: 'NONE', target_type: '', target_name: '', goal: 'NONE', wants_all_items: false };
  }

  const fallback = (() => {
    const activeType = String(activeOffer?.type || '').toUpperCase();
    const targetName = String(activeOffer?.selectedName || activeOffer?.items?.[0] || '').trim();

    if (activeType === 'PRODUCT' && userAsksForPhoto(raw)) {
      return {
        action: 'CONTINUE_ACTIVE_OFFER',
        target_type: 'PRODUCT',
        target_name: targetName,
        goal: 'PHOTO',
        wants_all_items: !!(userAsksForAllPhotos(raw) || (!targetName && Array.isArray(activeOffer?.items) && activeOffer.items.length > 1)),
      };
    }

    if (activeType === 'COURSE' && /(inscrib|inscripción|inscripcion|anot|reserv(ar|o)? lugar|quiero seguir|quiero avanzar|quiero ese|quiero ese curso|seña|sena)/i.test(normalize(raw))) {
      return { action: 'CONTINUE_ACTIVE_OFFER', target_type: 'COURSE', target_name: targetName, goal: 'SIGNUP', wants_all_items: false };
    }

    if (activeType === 'SERVICE' && /(turno|reserv(ar|a)?|agend(ar|a)?|cita|quiero seguir|quiero avanzar|quiero ese servicio)/i.test(normalize(raw))) {
      return { action: 'CONTINUE_ACTIVE_OFFER', target_type: 'SERVICE', target_name: targetName, goal: 'BOOK', wants_all_items: false };
    }

    return { action: 'NONE', target_type: '', target_name: '', goal: 'NONE', wants_all_items: false };
  })();

  if (!looksLikeActiveOfferFollowup(raw) && !context.force) return fallback;

  try {
    const completion = await openai.chat.completions.create({
      model: PRIMARY_MODEL,
      temperature: 0,
      messages: [
        {
          role: 'system',
          content:
`Analizá si el mensaje del cliente responde DIRECTAMENTE a la última oferta/respuesta comercial del asistente por WhatsApp.

Devolvé SOLO JSON válido con estas claves:
- action: CONTINUE_ACTIVE_OFFER | SWITCH_TOPIC | NONE
- target_type: PRODUCT | COURSE | SERVICE | OTHER
- target_name: string
- goal: PHOTO | PRICE | DETAIL | LIST_MORE | MATERIAL | SIGNUP | BOOK | DURATION | SCHEDULE | REQUIREMENTS | CUPS | NONE
- wants_all_items: boolean

Reglas:
- Priorizá entender si la persona responde a lo ÚLTIMO que el asistente le mostró.
- Si dice cosas como "pasame fotos", "precio del primero", "quiero ese", "de ese", "más info", "quiero avanzar", "quiero seguir", normalmente es CONTINUE_ACTIVE_OFFER.
- PRODUCT: fotos, precio, detalle, elegir una opción, seguir con la recomendación.
- COURSE: más info, precio, horarios, requisitos, material, o inscribirse.
- SERVICE: precio, duración, detalle o sacar turno.
- Si cambia claramente a otro tema distinto, devolvé SWITCH_TOPIC.
- target_name debe ser el ítem más probable dentro de las opciones activas. Si no se puede saber y sigue siendo una respuesta a la oferta activa, puede ir vacío.
- wants_all_items=true solo si pide ver todas las fotos/opciones o habla de todas las opciones activas.
- No inventes productos, cursos ni servicios.`
        },
        {
          role: 'user',
          content: JSON.stringify({
            mensaje: raw,
            oferta_activa_tipo: activeOffer?.type || '',
            oferta_activa_items: Array.isArray(activeOffer?.items) ? activeOffer.items.slice(0, 12) : [],
            oferta_activa_item_seleccionado: activeOffer?.selectedName || '',
            oferta_activa_modo: activeOffer?.mode || '',
            oferta_activa_texto: activeOffer?.lastAssistantText || '',
            ultimo_servicio: context.lastServiceName || '',
            ultimo_curso: context.lastCourseName || '',
            historial_reciente: context.historySnippet || '',
          })
        }
      ],
      response_format: { type: 'json_object' },
    });

    const obj = JSON.parse(completion.choices?.[0]?.message?.content || '{}');
    return {
      action: String(obj.action || fallback.action || 'NONE').trim().toUpperCase(),
      target_type: String(obj.target_type || activeOffer?.type || '').trim().toUpperCase(),
      target_name: String(obj.target_name || '').trim(),
      goal: String(obj.goal || fallback.goal || 'NONE').trim().toUpperCase(),
      wants_all_items: !!obj.wants_all_items,
    };
  } catch {
    return fallback;
  }
}

function buildSyntheticTextFromActiveOfferResolution(resolution, activeOffer = null) {
  const type = String(resolution?.target_type || activeOffer?.type || '').trim().toUpperCase();
  const target = String(resolution?.target_name || activeOffer?.selectedName || activeOffer?.items?.[0] || '').trim();
  const goal = String(resolution?.goal || 'NONE').trim().toUpperCase();
  const allItems = !!resolution?.wants_all_items;

  if (!type) return '';

  if (type === 'PRODUCT') {
    if (goal === 'PHOTO') return allItems ? 'pasame fotos de todo eso' : `pasame foto de ${target}`.trim();
    if (goal === 'PRICE') return `precio de ${target}`.trim();
    if (goal === 'LIST_MORE') return target ? `mostrame más opciones de ${target}` : 'mostrame más opciones';
    return target || 'quiero ese producto';
  }

  if (type === 'COURSE') {
    if (goal === 'SIGNUP') return `quiero inscribirme al curso ${target}`.trim();
    if (goal === 'MATERIAL') return `pasame el material del curso ${target}`.trim();
    if (goal === 'PRICE') return `precio del curso ${target}`.trim();
    if (goal === 'SCHEDULE') return `horarios del curso ${target}`.trim();
    if (goal === 'DURATION') return `duración del curso ${target}`.trim();
    if (goal === 'REQUIREMENTS') return `requisitos del curso ${target}`.trim();
    if (goal === 'CUPS') return `cupos del curso ${target}`.trim();
    return target ? `info del curso ${target}` : 'más info del curso';
  }

  if (type === 'SERVICE') {
    if (goal === 'BOOK') return `quiero turno para ${target}`.trim();
    if (goal === 'PRICE') return `precio del servicio ${target}`.trim();
    if (goal === 'DURATION') return `cuánto dura ${target}`.trim();
    return target ? `info del servicio ${target}` : 'más info del servicio';
  }

  return '';
}

async function reviewInboundMessageFirstWithAI(text, context = {}) {
  const raw = String(text || '').trim();
  if (!raw) {
    return {
      type: 'OTHER',
      mode: 'DETAIL',
      query: '',
      product_domain: '',
      product_family: '',
      follows_active_offer: false,
      topic_changed: false,
      keep_appointment_flow: false,
      should_clear_active_offer: false,
    };
  }

  const fallback = (() => {
    const fastIntent = detectFastCatalogIntent(raw, {
      hasCourseContext: !!context.hasCourseContext,
      hasDraft: !!context.hasDraft,
      flowStep: context.flowStep || '',
    });

    const explicitCourse = detectCourseIntentFromContext(raw, {
      lastCourseContext: context.lastCourseContext || null,
    });

    const activeType = String(context.activeAssistantOfferType || '').trim().toUpperCase();
    const activeDomain = String(context.activeAssistantOfferDomain || '').trim();
    const activeFamily = String(context.activeAssistantOfferFamily || '').trim();
    const productDomain = detectProductDomain(raw, detectFurnitureFamily(raw) || detectProductFamily(raw));
    const productFamily = productDomain === 'furniture' ? detectFurnitureFamily(raw) : detectProductFamily(raw);

    const inferredType = explicitCourse.isCourse
      ? 'COURSE'
      : (fastIntent?.type || (isExplicitProductIntent(raw) ? 'PRODUCT' : (isExplicitServiceIntent(raw) ? 'SERVICE' : 'OTHER')));

    const followsActiveOffer = !!activeType
      && looksLikeActiveOfferFollowup(raw)
      && (
        inferredType === 'OTHER'
        || inferredType === activeType
        || (activeType === 'PRODUCT' && inferredType === 'PRODUCT' && !productFamily)
      );

    const topicChanged = !!(
      activeType
      && !followsActiveOffer
      && inferredType !== 'OTHER'
      && (
        inferredType !== activeType
        || (inferredType === 'PRODUCT' && (
          (!!activeDomain && !!productDomain && activeDomain !== productDomain)
          || (!!activeFamily && !!productFamily && normalizeCatalogSearchText(activeFamily) !== normalizeCatalogSearchText(productFamily))
        ))
      )
    );

    return {
      type: inferredType,
      mode: explicitCourse.isCourse ? (explicitCourse.mode || 'DETAIL') : (fastIntent?.mode || 'DETAIL'),
      query: explicitCourse.isCourse
        ? (explicitCourse.query || raw)
        : (fastIntent?.query || raw),
      product_domain: productDomain || '',
      product_family: productFamily || '',
      follows_active_offer: followsActiveOffer,
      topic_changed: topicChanged,
      keep_appointment_flow: !!(context.hasDraft && looksLikeAppointmentIntent(raw, {
        pendingDraft: context.pendingDraft || null,
        lastService: context.lastService || null,
      })),
      should_clear_active_offer: topicChanged,
    };
  })();

  try {
    const completion = await openai.chat.completions.create({
      model: PRIMARY_MODEL,
      temperature: 0,
      messages: [
        {
          role: 'system',
          content:
`Analizá el mensaje del cliente ANTES de activar cualquier flujo del bot.

Devolvé SOLO JSON válido con estas claves:
- type: PRODUCT | SERVICE | COURSE | OTHER
- mode: LIST | DETAIL
- query: string
- product_domain: hair | furniture | ""
- product_family: string
- follows_active_offer: boolean
- topic_changed: boolean
- keep_appointment_flow: boolean

Reglas:
- PRODUCT incluye stock, productos, insumos, muebles, sillones, camillas, espejos, planchas, secadores y equipamiento.
- SERVICE incluye servicios del salón y continuidad real de turnos.
- COURSE incluye cursos, talleres, capacitaciones e inscripción.
- Si el mensaje introduce un tema nuevo y concreto, marcá topic_changed=true.
- Si viene una oferta activa pero el cliente cambia a otro producto/servicio/curso distinto, follows_active_offer=false y topic_changed=true.
- “necesito una plancha”, “necesito un secador”, “tienen sillones de barbería” son PRODUCT y normalmente topic_changed=true si antes venían con otra cosa.
- “sí, quiero esa”, “pasame foto”, “precio del primero”, “más info” suelen follows_active_offer=true solo si realmente responden a la última oferta.
- “barbería” dentro de “sillones de barbería” sigue siendo PRODUCT/furniture, no SERVICE.
- keep_appointment_flow=true solo si realmente sigue un turno activo con fecha, hora, nombre, teléfono, comprobante o confirmación clara del turno.
- No inventes productos, servicios ni cursos.`,
        },
        {
          role: 'user',
          content: JSON.stringify({
            mensaje: raw,
            oferta_activa_tipo: context.activeAssistantOfferType || '',
            oferta_activa_dominio: context.activeAssistantOfferDomain || '',
            oferta_activa_familia: context.activeAssistantOfferFamily || '',
            oferta_activa_items: Array.isArray(context.activeAssistantOfferItems) ? context.activeAssistantOfferItems.slice(0, 12) : [],
            oferta_activa_seleccionado: context.activeAssistantOfferSelectedName || '',
            tiene_borrador_turno: !!context.hasDraft,
            flujo_actual: context.flowStep || '',
            ultimo_servicio: context.lastServiceName || '',
            ultimo_producto: context.lastProductName || '',
            ultimo_curso: context.lastCourseName || '',
            historial_reciente: context.historySnippet || '',
          }),
        },
      ],
      response_format: { type: 'json_object' },
    });

    const obj = JSON.parse(completion.choices?.[0]?.message?.content || '{}');
    const type = String(obj.type || fallback.type || 'OTHER').trim().toUpperCase();
    const mode = String(obj.mode || fallback.mode || 'DETAIL').trim().toUpperCase() === 'LIST' ? 'LIST' : 'DETAIL';
    const productDomainRaw = String(obj.product_domain || fallback.product_domain || '').trim().toLowerCase();
    const productDomain = ['hair', 'furniture'].includes(productDomainRaw)
      ? productDomainRaw
      : (fallback.product_domain || '');
    const productFamily = String(
      obj.product_family
      || fallback.product_family
      || (type === 'PRODUCT' ? (productDomain === 'furniture' ? detectFurnitureFamily(raw) : detectProductFamily(raw)) : '')
    ).trim();

    const activeType = String(context.activeAssistantOfferType || '').trim().toUpperCase();
    const activeDomain = String(context.activeAssistantOfferDomain || '').trim();
    const activeFamily = String(context.activeAssistantOfferFamily || '').trim();

    const followsActiveOffer = !!obj.follows_active_offer
      && !(
        type === 'PRODUCT'
        && (
          (!!activeDomain && !!productDomain && activeDomain !== productDomain)
          || (!!activeFamily && !!productFamily && normalizeCatalogSearchText(activeFamily) !== normalizeCatalogSearchText(productFamily))
        )
      );

    const topicChanged = !!(
      obj.topic_changed
      || (
        activeType
        && !followsActiveOffer
        && type !== 'OTHER'
        && (
          type !== activeType
          || (type === 'PRODUCT' && (
            (!!activeDomain && !!productDomain && activeDomain !== productDomain)
            || (!!activeFamily && !!productFamily && normalizeCatalogSearchText(activeFamily) !== normalizeCatalogSearchText(productFamily))
          ))
        )
      )
    );

    return {
      type,
      mode,
      query: String(obj.query || fallback.query || raw).trim(),
      product_domain: productDomain || '',
      product_family: productFamily || '',
      follows_active_offer: followsActiveOffer,
      topic_changed: topicChanged,
      keep_appointment_flow: !!obj.keep_appointment_flow,
      should_clear_active_offer: topicChanged,
    };
  } catch {
    return fallback;
  }
}


function toCourseContextRow(course = {}) {
  const nombre = String(course?.nombre || '').trim();
  if (!nombre) return null;
  return {
    nombre,
    categoria: String(course?.categoria || '').trim(),
    modalidad: String(course?.modalidad || '').trim(),
    duracionTotal: String(course?.duracionTotal || '').trim(),
    fechaInicio: String(course?.fechaInicio || '').trim(),
    fechaFin: String(course?.fechaFin || '').trim(),
    diasHorarios: String(course?.diasHorarios || '').trim(),
    requisitos: String(course?.requisitos || '').trim(),
    info: String(course?.info || '').trim(),
    cupos: String(course?.cupos || '').trim(),
    sena: String(course?.sena || '').trim(),
    precio: String(course?.precio || '').trim(),
    estado: String(course?.estado || '').trim(),
  };
}

function mergeCourseContextRows(freshRows = [], previousRows = [], limit = 12) {
  const seen = new Set();
  const out = [];
  const pushRow = (row) => {
    const normalizedName = normalize(row?.nombre || '');
    if (!normalizedName || seen.has(normalizedName)) return;
    seen.add(normalizedName);
    const compact = toCourseContextRow(row);
    if (compact) out.push(compact);
  };

  for (const row of (Array.isArray(freshRows) ? freshRows : [])) pushRow(row);
  for (const row of (Array.isArray(previousRows) ? previousRows : [])) pushRow(row);
  return out.slice(0, limit);
}

const pendingAmbiguousBeautyByUser = new Map();
const lastResolvedBeautyByUser = new Map();
const BEAUTY_CONTEXT_TTL_MS = Number(process.env.BEAUTY_CONTEXT_TTL_MS || 7 * 24 * 60 * 60 * 1000);

function getPendingAmbiguousBeauty(waId) {
  const row = pendingAmbiguousBeautyByUser.get(waId);
  if (!row) return null;
  if ((Date.now() - Number(row.ts || 0)) > BEAUTY_CONTEXT_TTL_MS) {
    pendingAmbiguousBeautyByUser.delete(waId);
    return null;
  }
  return row;
}

function setPendingAmbiguousBeauty(waId, patch = {}) {
  if (!waId) return null;
  const prev = getPendingAmbiguousBeauty(waId) || {};
  const next = { ...prev, ...patch, ts: Date.now() };
  pendingAmbiguousBeautyByUser.set(waId, next);
  return next;
}

function clearPendingAmbiguousBeauty(waId) {
  if (waId) pendingAmbiguousBeautyByUser.delete(waId);
}

function getLastResolvedBeauty(waId) {
  const row = lastResolvedBeautyByUser.get(waId);
  if (!row) return null;
  if ((Date.now() - Number(row.ts || 0)) > BEAUTY_CONTEXT_TTL_MS) {
    lastResolvedBeautyByUser.delete(waId);
    return null;
  }
  return row;
}

function setLastResolvedBeauty(waId, patch = {}) {
  if (!waId) return null;
  const prev = getLastResolvedBeauty(waId) || {};
  const next = { ...prev, ...patch, ts: Date.now() };
  lastResolvedBeautyByUser.set(waId, next);
  return next;
}

function clearLastResolvedBeauty(waId) {
  if (waId) lastResolvedBeautyByUser.delete(waId);
}

function buildBeautyCanonicalLabel(term = '') {
  const clean = normalize(String(term || '').trim());
  if (!clean) return '';
  if (clean == 'nutricion') return 'Nutrición';
  return clean.charAt(0).toUpperCase() + clean.slice(1);
}

function looksLikeBeautyContextFollowup(text = '') {
  const t = normalize(text || '');
  if (!t) return false;
  if (textAsksForServicePrice(text) || textAsksForServiceDuration(text)) return true;
  return /^(servicio|producto|el servicio|un servicio|el producto|un producto|para hacerme|para hacermelo|para hacermela|para comprar|para usar|para vender|para trabajar|quiero comprar|y el precio|y cuanto dura|y cuánto dura|y turno|turno|quiero turno|quiero sacar turno|quiero reservar|reservar|agendar|sacar turno)$/i.test(t);
}

function shouldRunBeautyResolver(text, { pendingTerm = '', lastResolved = null } = {}) {
  const raw = String(text || '').trim();
  if (!raw) return false;
  if (shouldRunConversationWrapUpAI(raw)) return false;
  if (extractAmbiguousBeautyTerm(raw)) return true;
  if (pendingTerm) return true;
  if (lastResolved?.term && looksLikeBeautyContextFollowup(raw)) return true;
  return false;
}

async function resolveAmbiguousBeautyIntentWithAI(text, context = {}) {
  const raw = String(text || '').trim();
  if (!raw) {
    return { kind: 'UNKNOWN', canonicalQuery: '', goal: 'UNKNOWN' };
  }

  try {
    const completion = await openai.chat.completions.create({
      model: PRIMARY_MODEL,
      temperature: 0,
      messages: [
        {
          role: 'system',
          content:
`Analizá consultas ambiguas de peluquería/belleza y devolvé SOLO JSON.
Campos:
- kind: SERVICE | PRODUCT | UNKNOWN
- canonical_query: nombre base corto y limpio del tema (ej: Keratina, Botox, Nutrición, Alisado)
- goal: PRICE | DURATION | BOOK_APPOINTMENT | DETAIL | LIST | UNKNOWN

Reglas:
- SERVICE si la persona habla del servicio del salón, su precio, su duración, o quiere turno.
- PRODUCT si la persona habla de comprar, stock, insumos o producto.
- UNKNOWN si todavía no se puede saber.
- Si el mensaje es solo “servicio” o “producto”, usá pending_term.
- Si pregunta “cuánto dura”, “cuánto demora”, “precio”, “turno”, “quiero reservar”, usá pending_term o last_resolved_term.
- canonical_query debe ser solo el tema base, sin palabras extra.
- Si no hay suficiente base para canonical_query, devolvé cadena vacía.

Ejemplos:
- pending_term=Keratina y mensaje=servicio => SERVICE + Keratina + DETAIL
- pending_term=Botox y mensaje=producto => PRODUCT + Botox + DETAIL
- mensaje=cuánto cuesta el servicio de botox => SERVICE + Botox + PRICE
- mensaje=cuánto dura la keratina => SERVICE + Keratina + DURATION
- last_resolved_term=Nutrición, last_resolved_kind=SERVICE y mensaje=y turno => SERVICE + Nutrición + BOOK_APPOINTMENT`
        },
        {
          role: 'user',
          content: JSON.stringify({
            mensaje: raw,
            pending_term: context.pendingTerm || '',
            last_resolved_term: context.lastResolvedTerm || '',
            last_resolved_kind: context.lastResolvedKind || '',
            last_service_name: context.lastServiceName || '',
            historial_reciente: context.historySnippet || '',
          })
        }
      ],
      response_format: { type: 'json_object' },
    });

    const parsed = JSON.parse(completion.choices[0].message.content || '{}');
    return {
      kind: String(parsed.kind || 'UNKNOWN').trim().toUpperCase(),
      canonicalQuery: buildBeautyCanonicalLabel(parsed.canonical_query || ''),
      goal: String(parsed.goal || 'UNKNOWN').trim().toUpperCase(),
    };
  } catch {
    return { kind: 'UNKNOWN', canonicalQuery: '', goal: 'UNKNOWN' };
  }
}


function shouldRunConversationWrapUpAI(text = '', waId = '') {
  const raw = String(text || '').trim();
  if (!raw) return false;
  const t = normalize(raw);
  if (!t) return false;

  if (/\b(gracias|muchas gracias|mil gracias|gracias igual|no gracias|por ahora no|despues|después|lo aviso|lo veo|mas tarde|más tarde|otro momento|dejalo ahi|dejalo así|dejala ahi|mejor después|mejor despues|cualquier cosa te aviso|cualquier cosa le aviso)\b/i.test(t)) {
    return true;
  }

  if (isPoliteCatalogDecline(raw) || isPoliteClosureAfterTurno(raw)) return true;

  if (t === 'no' && (lastAssistantWasQuestion(waId) || lastAssistantLooksLikeCatalogMessage(waId))) {
    return true;
  }

  return false;
}

async function classifyConversationWrapUpWithAI(text, context = {}) {
  const raw = String(text || '').trim();
  if (!raw) return { action: 'CONTINUE', tone: 'neutral' };

  try {
    const completion = await openai.chat.completions.create({
      model: PRIMARY_MODEL,
      temperature: 0,
      messages: [
        {
          role: 'system',
          content:
`Analizá el último mensaje del cliente dentro de una conversación comercial de WhatsApp y devolvé SOLO JSON.
Campos:
- action: CONTINUE | CLOSE_POLITELY
- tone: neutral | grateful | postpone | decline
- reason: string breve

Usá el historial y la última pregunta/oferta del bot.

Elegí CLOSE_POLITELY si el cliente:
- agradece y corta por ahora
- posterga ("después te aviso", "lo veo y te digo", "en otro momento")
- rechaza una sugerencia/oferta ("no gracias", "no", "por ahora no")
- indica que no quiere seguir con ese tema ahora

Elegí CONTINUE si todavía está consultando, respondiendo datos útiles o quiere seguir la charla.

No confundas cierres amables con consultas.
Ejemplos:
- "muchas gracias, después lo aviso" => CLOSE_POLITELY
- "no gracias" después de una sugerencia => CLOSE_POLITELY
- "no" después de "¿es para uso personal o para trabajar?" => CLOSE_POLITELY
- "¿y cuánto dura?" => CONTINUE
- "servicio" => CONTINUE`
        },
        {
          role: 'user',
          content: JSON.stringify({
            mensaje: raw,
            last_assistant_message: context.lastAssistantMessage || '',
            pending_appointment: !!context.pendingAppointment,
            has_product_context: !!context.hasProductContext,
            has_beauty_context: !!context.hasBeautyContext,
            has_course_context: !!context.hasCourseContext,
            history_snippet: context.historySnippet || '',
          })
        }
      ],
      response_format: { type: 'json_object' },
    });

    const parsed = JSON.parse(completion.choices?.[0]?.message?.content || '{}');
    const action = String(parsed.action || 'CONTINUE').trim().toUpperCase();
    return {
      action: action === 'CLOSE_POLITELY' ? 'CLOSE_POLITELY' : 'CONTINUE',
      tone: String(parsed.tone || 'neutral').trim().toLowerCase(),
      reason: String(parsed.reason || '').trim(),
    };
  } catch {
    return { action: 'CONTINUE', tone: 'neutral', reason: '' };
  }
}

function buildPoliteConversationCloseMessage(result = {}) {
  const tone = String(result?.tone || '').toLowerCase();

  if (tone === 'postpone') {
    return `Perfecto 😊

No hay problema. Cuando quiera retomarlo, me escribe y seguimos por acá ✨`;
  }

  return `Perfecto, cualquier cosa estoy acá para ayudarte 😊

¡Que tengas un lindo día! ✨`;
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
const pendingStylistSuggestions = new Map();
const STYLIST_SUGGESTION_TTL_MS = Number(process.env.STYLIST_SUGGESTION_TTL_MS || 12 * 60 * 60 * 1000);

function getPendingStylistSuggestion(waId) {
  const row = pendingStylistSuggestions.get(waId);
  if (!row) return null;
  if ((Date.now() - Number(row.ts || 0)) > STYLIST_SUGGESTION_TTL_MS) {
    pendingStylistSuggestions.delete(waId);
    return null;
  }
  return row;
}

function setPendingStylistSuggestion(waId, patch = {}) {
  if (!waId) return null;
  const next = { ...(patch || {}), ts: Date.now() };
  pendingStylistSuggestions.set(waId, next);
  return next;
}

function clearPendingStylistSuggestion(waId) {
  if (waId) pendingStylistSuggestions.delete(waId);
}

// ===================== ✅ INFO FIJA DE TURNOS (NO CAMBIAR ARQUITECTURA) =====================
const TURNOS_STYLIST_NAME = "Flavia Rueda";
const TURNOS_HORARIOS_TXT = "Lunes a Sábados en horarios comerciales de 10, 11, 12, 17, 18, 19 y 20 hs";
const TURNOS_SENA_TXT = "$10.000";
const TURNOS_ALIAS = "Cataleya178";
const TURNOS_ALIAS_TITULAR = "Monica Pacheco";
const COURSE_SENA_AMOUNT = 10000;
const COURSE_SENA_TXT = "$10.000";
const TURNOS_ALLOWED_BLOCKS_COMMERCIAL = [
  { label: "mañana", start: "10:00", end: "13:00" },
  { label: "tarde", start: "17:00", end: "21:00" },
];
const TURNOS_ALLOWED_BLOCKS_SIESTA = [
  { label: "siesta", start: "14:00", end: "17:00" },
];
const TURNOS_ALLOWED_START_TIMES_COMMERCIAL = ["10:00", "11:00", "12:00", "17:00", "18:00", "19:00", "20:00"];
const TURNOS_ALLOWED_START_TIMES_SIESTA = ["14:00", "15:00", "16:00"];

function normalizeAvailabilityMode(mode) {
  return String(mode || '').trim().toLowerCase() === 'siesta' ? 'siesta' : 'commercial';
}

function getTurnoAllowedBlocks(mode = 'commercial') {
  const normalized = normalizeAvailabilityMode(mode);
  return normalized === 'siesta' ? TURNOS_ALLOWED_BLOCKS_SIESTA : TURNOS_ALLOWED_BLOCKS_COMMERCIAL;
}

function getTurnoAllowedStartTimes(mode = 'commercial') {
  const normalized = normalizeAvailabilityMode(mode);
  return normalized === 'siesta'
    ? TURNOS_ALLOWED_START_TIMES_SIESTA
    : TURNOS_ALLOWED_START_TIMES_COMMERCIAL;
}

function isHourAllowedForAvailabilityMode(timeHM, mode = 'commercial') {
  const safeHM = normalizeHourHM(timeHM);
  if (!safeHM) return false;
  return getTurnoAllowedStartTimes(mode).includes(safeHM);
}

function textRequestsSiestaAvailability(text = '') {
  const t = normalize(text || '');
  if (!t) return false;
  return /(ninguno de esos horarios|ninguno me sirve|no me sirve ninguno|no me sirven esos horarios|no puedo en esos horarios|no puedo ni a la manana ni a la tarde|no puedo ni a la mañana ni a la tarde|tenes a la siesta|tenes horario de siesta|tienen horario de siesta|siesta|14 hs|15 hs|16 hs|a las 14|a las 15|a las 16)/i.test(t);
}

function buildCommercialHoursBridge() {
  return 'Primero trabajo con los horarios comerciales de 10, 11, 12, 17, 18, 19 y 20 hs. Si ninguno de esos le sirve, también puedo revisar una franja especial de siesta a las 14, 15 o 16 hs 😊';
}

function turnoInfoBlock() {
  return (
`✨ Turnos en Cataleya

Profesional: ${TURNOS_STYLIST_NAME}
📅 Horarios comerciales para turnos:
• 10:00
• 11:00
• 12:00
• 17:00
• 18:00
• 19:00
• 20:00

Si ninguno de esos horarios le sirve, también puedo revisar una franja especial de siesta: 14:00, 15:00 o 16:00.

La seña se solicita recién después de confirmar disponibilidad con la estilista.`
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

  m = compact.match(/^(\d{1,2})(?:[:\.h])(\d{1,2})(?:[:\.h](\d{1,2}))?$/);
  if (m) {
    const hh = Number(m[1]);
    const mm = Number(m[2]);
    const ss = Number(m[3] || 0);
    if (hh >= 0 && hh <= 23 && mm >= 0 && mm <= 59 && ss >= 0 && ss <= 59) {
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
      const nameBeforePhone = raw.match(/^\s*([A-Za-zÁÉÍÓÚÑáéíóúñ' ]{5,80}?)(?:\s*,\s*|\s+y\s+)?(?:(?:y\s+)?(?:su\s+)?(?:numero|número|telefono|tel|cel|celular|whatsapp|wsp))/i);
      if (nameBeforePhone) {
        const cand = String(nameBeforePhone[1] || '').replace(/\s+/g, ' ').trim().replace(/[,:;.-]+$/, '');
        if (!/\d/.test(cand) && cand.split(' ').length >= 2) nombre = cand;
      }
    }

    if (!nombre && telefono) {
      const compact = raw.replace(/\s+/g, ' ').trim();
      const plainPhone = String(telefono || '').replace(/^\+/, '');
      const withoutPhone = compact.replace(plainPhone, '').replace(/[\s,;:()\-.]+/g, ' ').trim();
      const firstLine = String(raw.split(/\n+/)[0] || '').replace(/\s+/g, ' ').trim();
      const candidateBase = (firstLine && !/\d/.test(firstLine)) ? firstLine : withoutPhone;
      const cleanedCandidate = String(candidateBase || '')
        .replace(/(?:telefono|tel|cel|celular|whatsapp|wsp|numero|número)\s*:?/gi, '')
        .replace(/\s+/g, ' ')
        .trim()
        .replace(/[,:;.-]+$/, '');
      if (cleanedCandidate && !/\d/.test(cleanedCandidate) && cleanedCandidate.split(' ').length >= 2) {
        nombre = cleanedCandidate;
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
  const info = extractContactInfo(raw);
  if (info.nombre || info.telefono) return true;
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
    appointment_id: row.appointment_id == null ? null : Number(row.appointment_id),
    fecha: resolveAppointmentDateYMD(row.appointment_date || ''),
    hora: formatAppointmentTimeForTemplate(row.appointment_time || ''),
    servicio: row.service_name || "",
    duracion_min: Number(row.duration_min || 60) || 60,
    notas: row.service_notes || "",
    cliente_full: row.client_name || "",
    telefono_contacto: row.contact_phone || row.wa_phone || "",
    availability_mode: normalizeAvailabilityMode(row.availability_mode || 'commercial'),
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
      wa_id, wa_phone, appointment_id, client_name, contact_phone, service_name, service_notes,
      appointment_date, appointment_time, duration_min, wants_color_confirmation,
      availability_mode, payment_status, payment_amount, payment_sender, payment_receiver,
      payment_proof_text, payment_proof_media_id, payment_proof_filename,
      awaiting_contact, flow_step, last_intent, last_service_name, updated_at
    ) VALUES (
      $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,NOW()
    )
    ON CONFLICT (wa_id) DO UPDATE SET
      wa_phone = EXCLUDED.wa_phone,
      appointment_id = EXCLUDED.appointment_id,
      client_name = EXCLUDED.client_name,
      contact_phone = EXCLUDED.contact_phone,
      service_name = EXCLUDED.service_name,
      service_notes = EXCLUDED.service_notes,
      appointment_date = EXCLUDED.appointment_date,
      appointment_time = EXCLUDED.appointment_time,
      duration_min = EXCLUDED.duration_min,
      wants_color_confirmation = EXCLUDED.wants_color_confirmation,
      availability_mode = EXCLUDED.availability_mode,
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
      d.appointment_id == null ? null : Number(d.appointment_id),
      d.cliente_full || null,
      normalizePhone(d.telefono_contacto || waPhone || "") || null,
      d.servicio || null,
      d.notas || null,
      toYMD(d.fecha) || null,
      d.hora || null,
      Number(d.duracion_min || 60) || 60,
      !!d.wants_color_confirmation,
      normalizeAvailabilityMode(d.availability_mode || 'commercial'),
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

function mapCourseEnrollmentDraftRow(row) {
  if (!row) return null;
  return {
    curso_nombre: row.course_name || '',
    curso_categoria: row.course_category || '',
    alumno_nombre: row.student_name || '',
    alumno_dni: row.student_dni || '',
    telefono_contacto: row.contact_phone || row.wa_phone || '',
    payment_status: row.payment_status || 'not_paid',
    payment_amount: row.payment_amount == null ? null : Number(row.payment_amount),
    payment_sender: row.payment_sender || '',
    payment_receiver: row.payment_receiver || '',
    payment_proof_text: row.payment_proof_text || '',
    payment_proof_media_id: row.payment_proof_media_id || '',
    payment_proof_filename: row.payment_proof_filename || '',
    flow_step: row.flow_step || '',
    last_intent: row.last_intent || '',
    wa_phone: row.wa_phone || '',
  };
}

async function getCourseEnrollmentDraft(waId) {
  const r = await db.query(`SELECT * FROM course_enrollment_drafts WHERE wa_id = $1 LIMIT 1`, [waId]);
  return mapCourseEnrollmentDraftRow(r.rows[0]);
}

async function saveCourseEnrollmentDraft(waId, waPhone, draft) {
  const d = draft || {};
  await db.query(
    `INSERT INTO course_enrollment_drafts (
      wa_id, wa_phone, course_name, course_category, student_name, student_dni, contact_phone,
      payment_status, payment_amount, payment_sender, payment_receiver,
      payment_proof_text, payment_proof_media_id, payment_proof_filename,
      flow_step, last_intent, updated_at
    ) VALUES (
      $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,NOW()
    )
    ON CONFLICT (wa_id) DO UPDATE SET
      wa_phone = EXCLUDED.wa_phone,
      course_name = EXCLUDED.course_name,
      course_category = EXCLUDED.course_category,
      student_name = EXCLUDED.student_name,
      student_dni = EXCLUDED.student_dni,
      contact_phone = EXCLUDED.contact_phone,
      payment_status = EXCLUDED.payment_status,
      payment_amount = EXCLUDED.payment_amount,
      payment_sender = EXCLUDED.payment_sender,
      payment_receiver = EXCLUDED.payment_receiver,
      payment_proof_text = EXCLUDED.payment_proof_text,
      payment_proof_media_id = EXCLUDED.payment_proof_media_id,
      payment_proof_filename = EXCLUDED.payment_proof_filename,
      flow_step = EXCLUDED.flow_step,
      last_intent = EXCLUDED.last_intent,
      updated_at = NOW()`,
    [
      waId,
      normalizePhone(waPhone || d.telefono_contacto || ''),
      d.curso_nombre || null,
      d.curso_categoria || null,
      d.alumno_nombre || null,
      d.alumno_dni || null,
      normalizePhone(d.telefono_contacto || waPhone || '') || null,
      d.payment_status || 'not_paid',
      d.payment_amount == null ? null : Number(d.payment_amount),
      d.payment_sender || null,
      d.payment_receiver || null,
      d.payment_proof_text || null,
      d.payment_proof_media_id || null,
      d.payment_proof_filename || null,
      d.flow_step || null,
      d.last_intent || null,
    ]
  );
}

async function deleteCourseEnrollmentDraft(waId) {
  await db.query(`DELETE FROM course_enrollment_drafts WHERE wa_id = $1`, [waId]);
}

function inferCourseEnrollmentFlowStep(base = {}) {
  if (!String(base?.curso_nombre || '').trim()) return 'awaiting_course';
  if (!String(base?.alumno_nombre || '').trim()) return 'awaiting_name';
  if (!String(base?.alumno_dni || '').trim()) return 'awaiting_dni';
  if (base?.payment_status === 'payment_review') return 'payment_review';
  if (base?.payment_status === 'awaiting_salon_payment') return 'awaiting_salon_payment';
  if (base?.payment_status !== 'paid_verified') return 'awaiting_payment';
  return 'reserved';
}

function normalizeStudentDni(raw = '') {
  const digits = String(raw || '').replace(/\D/g, '');
  if (digits.length < 7 || digits.length > 8) return '';
  return digits;
}

function extractStudentDni(text = '') {
  const raw = String(text || '').trim();
  if (!raw) return '';
  const m = raw.match(/(?:dni|documento)?\s*[:#-]?\s*(\d{1,2}(?:\.\d{3}){2}|\d{7,8})\b/i);
  return normalizeStudentDni(m?.[1] || '');
}

function stripStudentDni(text = '') {
  const raw = String(text || '').trim();
  if (!raw) return '';
  return raw
    .replace(/(?:dni|documento)?\s*[:#-]?\s*(\d{1,2}(?:\.\d{3}){2}|\d{7,8})\b/ig, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function extractStudentFullName(text = '') {
  const cleaned = cleanNameCandidate(stripStudentDni(text));
  const parts = splitNameParts(cleaned);
  return parts.fullName && parts.lastName ? parts.fullName : '';
}

function mergeStudentIntoCourseEnrollment({ draft, text, waPhone }) {
  const out = { ...(draft || {}) };
  const raw = String(text || '').trim();
  if (!raw) {
    out.telefono_contacto = normalizePhone(out.telefono_contacto || waPhone || '');
    return out;
  }

  const maybeName = extractStudentFullName(raw);
  const maybeDni = extractStudentDni(raw);

  if (!out.alumno_nombre && maybeName) out.alumno_nombre = maybeName;
  if (!out.alumno_dni && maybeDni) out.alumno_dni = maybeDni;
  out.telefono_contacto = normalizePhone(out.telefono_contacto || waPhone || '');
  return out;
}

function buildCourseEnrollmentMissingCourseMessage(courses = []) {
  const rows = Array.isArray(courses) ? courses.filter((x) => x?.nombre).slice(0, 8) : [];
  if (!rows.length) {
    return `Para inscribirle necesito que me diga a qué curso quiere anotarse 😊`;
  }
  const opts = rows.map((c) => `🎓 ${c.nombre}`).join('\n');
  return `Para reservarle el lugar necesito que me diga cuál es el curso 😊

Opciones cargadas:
${opts}`;
}

function buildCourseEnrollmentNeedNameMessage(courseName = '') {
  const courseTxt = courseName ? ` en *${courseName}*` : '';
  return `Perfecto 😊 Para reservarle el lugar${courseTxt}, necesito estos datos del *alumno o alumna que va a asistir*:

• *Nombre y apellido*
• *DNI*`;
}

function buildCourseEnrollmentNeedDniMessage(courseName = '') {
  const courseTxt = courseName ? ` para *${courseName}*` : '';
  return `Perfecto 😊 Ya me quedó el nombre${courseTxt}.

Ahora necesito el *DNI del alumno o alumna* para completar los datos.`;
}

function buildCourseEnrollmentPaymentMessage(base = {}) {
  const courseName = String(base?.curso_nombre || '').trim();
  return [
    `Perfecto 😊 Ya registré los datos del alumno${courseName ? ` para *${courseName}*` : ''}.`,
    '',
    '*PARA TERMINAR DE CONFIRMAR LOS DATOS DEL ALUMNO TIENE 2 OPCIONES:*',
    '',
    `1) *Señar la inscripción* con ${COURSE_SENA_TXT} por transferencia`,
    '2) *Acercarse a pagar directamente al salón en su horario comercial* y traer una *fotocopia del documento del alumno o alumna*',
    '',
    '💳 Datos para la transferencia',
    '',
    'Alias:',
    TURNOS_ALIAS,
    '',
    'Titular:',
    TURNOS_ALIAS_TITULAR,
    '',
    'Si hace la transferencia, envíe por aquí el comprobante 📩',
    'Si prefiere acercarse al salón, avíseme por este medio y lo dejo asentado 😊',
  ].join('\n').trim();
}

function buildCourseEnrollmentReviewMessage(base = {}) {
  const courseName = String(base?.curso_nombre || '').trim();
  return courseName
    ? `Ya me quedó cargado el comprobante de *${courseName}* 😊

Si quiere, puede enviarme una captura donde se vea bien el monto de ${COURSE_SENA_TXT} y el titular *${TURNOS_ALIAS_TITULAR}* para terminar de validarlo.`
    : `Ya me quedó cargado el comprobante 😊

Si quiere, puede enviarme una captura donde se vea bien el monto de ${COURSE_SENA_TXT} y el titular *${TURNOS_ALIAS_TITULAR}* para terminar de validarlo.`;
}

async function tryApplyPaymentToCourseEnrollmentDraft(base, { text, mediaMeta } = {}) {
  const next = { ...(base || {}) };
  const rawText = String(text || '').trim();
  const previousProofText = String(next.payment_proof_text || '').trim();
  const previousProofExists = !!(next.payment_proof_media_id || previousProofText);
  const userSaysProofWasSent = looksLikeProofAlreadySent(rawText);
  const maybeProof =
    !!mediaMeta ||
    detectSenaPaid({ text: rawText }) ||
    looksLikePaymentProofText(rawText) ||
    userSaysProofWasSent;

  if (!maybeProof) return next;

  next.payment_proof_text = rawText || next.payment_proof_text || '';
  next.payment_proof_media_id = mediaMeta?.id || next.payment_proof_media_id || '';
  next.payment_proof_filename =
    mediaMeta?.filename || mediaMeta?.file_name || next.payment_proof_filename || '';

  let strictImage = {
    ok: false,
    es_comprobante: false,
    titular: '',
    monto: '',
    texto_visible: '',
  };

  if (mediaMeta?.filename) {
    strictImage = await extractStrictCoursePaymentProofFromImage(mediaMeta.filename);
  }

  const evidenceText = [
    rawText,
    previousProofText,
    strictImage.texto_visible,
    strictImage.titular,
    strictImage.monto,
  ]
    .filter(Boolean)
    .join('\n')
    .slice(0, 7000);

  const aiPago = await extractPagoInfoWithAI(evidenceText);
  const strictMonto = extractMoneyAmountFromText(strictImage?.monto || '') || null;
  const aiMonto = extractMoneyAmountFromText(aiPago?.monto || '') || null;
  const heuristicAmount = extractMoneyAmountFromText(evidenceText) || null;

  const receiverDetected =
    detectTitularMonicaPacheco(evidenceText) ||
    detectTitularMonicaPacheco(strictImage?.titular || '') ||
    isExpectedReceiver(strictImage?.titular || '') ||
    isExpectedReceiver(aiPago?.receptor || '');

  const amountLooksRight =
    strictMonto === COURSE_SENA_AMOUNT ||
    aiMonto === COURSE_SENA_AMOUNT ||
    heuristicAmount === COURSE_SENA_AMOUNT ||
    detectMonto10000(evidenceText);

  const canVerifyWithConfidence =
    receiverDetected &&
    amountLooksRight &&
    (
      !!mediaMeta ||
      !!strictImage.es_comprobante ||
      !!strictImage.texto_visible ||
      !!rawText
    );

  if (canVerifyWithConfidence) {
    next.payment_status = 'paid_verified';
    next.payment_amount = COURSE_SENA_AMOUNT;
    next.payment_receiver = TURNOS_ALIAS_TITULAR;
    if (!next.payment_sender && aiPago?.pagador) next.payment_sender = aiPago.pagador;
    next.payment_proof_text = evidenceText || next.payment_proof_text || '';
    return next;
  }

  if ((mediaMeta || userSaysProofWasSent || previousProofExists) && !paymentMessageIsTooWeakToVerify(rawText)) {
    next.payment_status = next.payment_status === 'paid_verified' ? 'paid_verified' : 'payment_review';

    if (next.payment_status !== 'paid_verified') {
      next.payment_amount = strictMonto || aiMonto || heuristicAmount || next.payment_amount || null;
      next.payment_receiver = receiverDetected ? TURNOS_ALIAS_TITULAR : (next.payment_receiver || '');
      if (!next.payment_sender && aiPago?.pagador) next.payment_sender = aiPago.pagador;
      next.payment_proof_text = evidenceText || next.payment_proof_text || '';
    }

    return next;
  }

  return next;
}

async function createCourseEnrollmentRecord({ waId, waPhone, draft }) {
  const d = draft || {};
  if (d.payment_proof_media_id) {
    const existing = await db.query(
      `SELECT *
         FROM course_enrollments
        WHERE wa_id = $1
          AND course_name = $2
          AND payment_proof_media_id = $3
        LIMIT 1`,
      [waId, d.curso_nombre || '', d.payment_proof_media_id]
    );
    if (existing.rows?.[0]) return existing.rows[0];
  }

  const r = await db.query(
    `INSERT INTO course_enrollments (
      wa_id, wa_phone, course_name, course_category, student_name, student_dni, contact_phone,
      status, payment_status, payment_amount, payment_sender, payment_receiver,
      payment_proof_text, payment_proof_media_id, payment_proof_filename,
      created_at, updated_at
    ) VALUES (
      $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,NOW(),NOW()
    ) RETURNING *`,
    [
      waId,
      normalizePhone(waPhone || d.telefono_contacto || ''),
      d.curso_nombre || '',
      d.curso_categoria || null,
      d.alumno_nombre || null,
      d.alumno_dni || null,
      normalizePhone(d.telefono_contacto || waPhone || '') || null,
      d.status || 'reserved',
      d.payment_status || 'paid_verified',
      d.payment_amount == null ? (d.payment_status === 'paid_verified' ? COURSE_SENA_AMOUNT : null) : Number(d.payment_amount),
      d.payment_sender || null,
      d.payment_receiver || TURNOS_ALIAS_TITULAR,
      d.payment_proof_text || null,
      d.payment_proof_media_id || null,
      d.payment_proof_filename || null,
    ]
  );
  return r.rows?.[0] || null;
}

async function finalizeCourseEnrollmentFlow({ waId, phone, draft }) {
  const base = { ...(draft || {}) };
  base.flow_step = inferCourseEnrollmentFlowStep(base);
  if (!base.curso_nombre) return { type: 'need_course' };
  if (!base.alumno_nombre) return { type: 'need_name' };
  if (!base.alumno_dni) return { type: 'need_dni' };
  if (base.payment_status === 'payment_review') return { type: 'payment_review' };
  if (base.payment_status !== 'paid_verified') return { type: 'need_payment' };
  const created = await createCourseEnrollmentRecord({ waId, waPhone: phone, draft: { ...base, status: 'reserved', payment_status: 'paid_verified' } });

  let managerNotification = null;
  try {
    managerNotification = await notifyCourseManagerEnrollmentPaid(created || {
      id: null,
      student_name: base.alumno_nombre || '',
      student_dni: base.alumno_dni || '',
      course_name: base.curso_nombre || '',
      wa_phone: normalizePhone(phone || ''),
      contact_phone: normalizePhone(base.telefono_contacto || phone || ''),
      payment_proof_filename: base.payment_proof_filename || '',
    });
  } catch (e) {
    console.error('❌ Error notificando inscripción de curso a responsable:', e?.response?.data || e?.message || e);
  }

  await deleteCourseEnrollmentDraft(waId);
  return { type: 'reserved', enrollmentId: created?.id || null, enrollmentRow: created || null, managerNotification };
}

async function finalizeCourseEnrollmentOnsiteFlow({ waId, phone, draft }) {
  const base = { ...(draft || {}) };
  base.flow_step = inferCourseEnrollmentFlowStep(base);
  if (!base.curso_nombre) return { type: 'need_course' };
  if (!base.alumno_nombre) return { type: 'need_name' };
  if (!base.alumno_dni) return { type: 'need_dni' };

  await deleteCourseEnrollmentDraft(waId);
  return { type: 'onsite_cancelled' };
}

function buildCourseEnrollmentOnsitePendingMessage(base = {}) {
  const courseName = String(base?.curso_nombre || '').trim();
  const studentName = String(base?.alumno_nombre || '').trim();
  const saludo = studentName ? `${studentName}, ` : '';
  return [
    `Perfecto 😊 ${saludo}dejé *cerrado el flujo de inscripción por chat*${courseName ? ` para *${courseName}*` : ''}.`,
    '',
    'Puede *acercarse directamente al salón en su horario comercial* para completar todo de forma presencial.',
    'Recuerde llevar una *fotocopia del documento del alumno o alumna*.',
    '',
    `Si más adelante prefiere retomar por WhatsApp, me escribe y seguimos con la reserva por transferencia de ${COURSE_SENA_TXT}.`,
  ].join('\n').trim();
}

function buildCourseEnrollmentReservedMessage(base = {}) {
  const courseName = String(base?.curso_nombre || '').trim();
  const studentName = String(base?.alumno_nombre || '').trim();
  const saludo = studentName ? `${studentName}, ` : '';
  return [
    `Perfecto 😊 ${saludo}ya quedó *reservado el lugar*${courseName ? ` en *${courseName}*` : ''}.`,
    '',
    `Se registró la seña de ${COURSE_SENA_TXT}.`,
    '',
    `Si necesita más información del curso, me escribe por aquí ✨`,
  ].join('\n').trim();
}

function looksLikeCourseEnrollmentPause(text = '') {
  const t = normalizeShortReply(text || '');
  if (!t) return false;
  return /^(no quiero seguir|no quiero inscribirme|ya no quiero|despues sigo|después sigo|lo dejo ahi|lo dejo ahí|frenemos|frenalo|pause|pausa|cancelar|cancelalo|cancelalo por ahora|dejalo por ahora|quiero cancelar eso|quiero cancelar esto|cancelar eso|cancelar esto|quiero frenar eso|quiero frenar esto|olvidate|olvídate)$/.test(t);
}

function looksLikeCourseOnsitePaymentIntent(text = '') {
  const t = normalize(text || '');
  if (!t) return false;
  return /(me acerco|voy al salon|voy al salón|paso por el salon|paso por el salón|lo pago en el salon|lo pago en el salón|pago en el salon|pago en el salón|abono en el salon|abono en el salón|voy a pagar al salon|voy a pagar al salón|prefiero pagar en el salon|prefiero pagar en el salón|quiero pagar en el salon|quiero pagar en el salón|pago directo en el salon|pago directo en el salón|voy directamente al salon|voy directamente al salón)/i.test(t);
}

async function extractCourseEnrollmentIntentWithAI(text, context = {}) {
  const raw = String(text || '').trim();
  const t = normalize(raw);
  if (!raw) return { action: 'NONE', course_query: '' };

  if (looksLikeCourseEnrollmentPause(raw)) return { action: 'PAUSE', course_query: '' };
  if (detectSenaPaid({ text: raw }) || looksLikePaymentProofText(raw) || looksLikeProofAlreadySent(raw)) {
    return { action: 'PAYMENT', course_query: '' };
  }

  let forcedAction = '';
  if (/(inscrib|inscripción|inscripcion|anot|reserv(ar|o)? lugar|quiero ese curso|quiero ese|me quiero sumar|quiero entrar|quiero reservar mi lugar|seña|sena)/i.test(t)) {
    forcedAction = 'START_SIGNUP';
  } else if (context?.hasDraft) {
    forcedAction = 'CONTINUE_SIGNUP';
  }

  try {
    const completion = await openai.chat.completions.create({
      model: PRIMARY_MODEL,
      temperature: 0,
      messages: [
        {
          role: 'system',
          content:
`Analizá mensajes sobre cursos de un salón y devolvé SOLO JSON válido con:
- action: START_SIGNUP | CONTINUE_SIGNUP | PAYMENT | PAUSE | NONE
- course_query: string

Reglas:
- START_SIGNUP si la persona quiere inscribirse, anotarse, reservar lugar o señar.
- CONTINUE_SIGNUP si ya hay flujo activo y el mensaje sigue aportando datos.
- PAYMENT si manda o menciona comprobante, transferencia, seña o pago.
- PAUSE si quiere frenar o cancelar por ahora.
- NONE si solo consulta info.
- course_query debe intentar extraer el nombre o tema del curso cuando la persona lo insinúa, incluso en frases como “quiero inscribirme al de celulares”, “el segundo”, “el de reparación”, “ese curso” o “al técnico de tablets”.
- Si no se puede inferir con claridad, devolvé cadena vacía.`
        },
        {
          role: 'user',
          content: JSON.stringify({
            mensaje: raw,
            curso_actual: context.currentCourseName || '',
            opciones_recientes: Array.isArray(context.recentCourseNames) ? context.recentCourseNames.slice(0, 12) : [],
            hay_borrador_activo: !!context.hasDraft,
            historial: context.historySnippet || '',
          })
        }
      ],
      response_format: { type: 'json_object' },
    });
    const obj = JSON.parse(completion.choices?.[0]?.message?.content || '{}');
    return {
      action: String(forcedAction || obj.action || 'NONE').trim().toUpperCase(),
      course_query: String(obj.course_query || '').trim(),
    };
  } catch {
    return { action: forcedAction || 'NONE', course_query: '' };
  }
}

async function askForMissingCourseEnrollmentData({ waId, phone, base, courseOptions = [] } = {}) {
  const draft = { ...(base || {}) };
  draft.flow_step = inferCourseEnrollmentFlowStep(draft);
  draft.last_intent = draft.last_intent || 'course_signup';
  await saveCourseEnrollmentDraft(waId, phone, draft);

  if (!draft.curso_nombre) {
    const msg = buildCourseEnrollmentMissingCourseMessage(courseOptions);
    pushHistory(waId, 'assistant', msg);
    await sendWhatsAppText(phone, msg);
    return { asked: 'course', draft };
  }

  if (!draft.alumno_nombre) {
    const msg = buildCourseEnrollmentNeedNameMessage(draft.curso_nombre || '');
    pushHistory(waId, 'assistant', msg);
    await sendWhatsAppText(phone, msg);
    return { asked: 'name', draft };
  }

  if (!draft.alumno_dni) {
    const msg = buildCourseEnrollmentNeedDniMessage(draft.curso_nombre || '');
    pushHistory(waId, 'assistant', msg);
    await sendWhatsAppText(phone, msg);
    return { asked: 'dni', draft };
  }

  if (draft.payment_status === 'payment_review') {
    const msg = buildCourseEnrollmentReviewMessage(draft);
    pushHistory(waId, 'assistant', msg);
    await sendWhatsAppText(phone, msg);
    return { asked: 'payment_review', draft };
  }

  const msg = buildCourseEnrollmentPaymentMessage(draft);
  pushHistory(waId, 'assistant', msg);
  await sendWhatsAppText(phone, msg);
  return { asked: 'payment', draft };
}

function buildPaymentPendingMessage() {
  return `La estilista ya confirmó el horario 😊

Para terminar de confirmar el turno se solicita una seña de ${TURNOS_SENA_TXT}.

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
  return /(^|[\s:\-])monica pacheco($|[\s:\-])/.test(t)
    || /titular[\s:\-]*monica pacheco/.test(t)
    || /transferencia recibida[\s:\-]*monica pacheco/.test(t);
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
      availability_mode, created_at, updated_at
    ) VALUES (
      $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,
      $12,$13,$14,$15,$16,$17,$18,$19,$20,NOW(),NOW()
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
      normalizeAvailabilityMode(merged.availability_mode || 'commercial'),
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

function mapAppointmentRowToDraft(row) {
  if (!row) return null;
  return {
    appointment_id: row.id == null ? null : Number(row.id),
    fecha: resolveAppointmentDateYMD(row.appointment_date || ''),
    hora: formatAppointmentTimeForTemplate(row.appointment_time || ''),
    servicio: row.service_name || '',
    duracion_min: Number(row.duration_min || 60) || 60,
    notas: row.service_notes || '',
    cliente_full: row.client_name || '',
    telefono_contacto: row.contact_phone || row.wa_phone || '',
    availability_mode: normalizeAvailabilityMode(row.availability_mode || 'commercial'),
    payment_status: row.payment_status || 'not_paid',
    payment_amount: row.payment_amount == null ? null : Number(row.payment_amount),
    payment_sender: row.payment_sender || '',
    payment_receiver: row.payment_receiver || '',
    payment_proof_text: row.payment_proof_text || '',
    payment_proof_media_id: row.payment_proof_media_id || '',
    payment_proof_filename: row.payment_proof_filename || '',
    awaiting_contact: false,
    flow_step: 'awaiting_payment',
    last_intent: 'book_appointment',
    last_service_name: row.service_name || '',
    wa_phone: row.wa_phone || '',
  };
}

async function getAppointmentById(appointmentId) {
  if (!appointmentId) return null;
  const r = await db.query(`SELECT * FROM appointments WHERE id = $1 LIMIT 1`, [Number(appointmentId)]);
  return r.rows?.[0] || null;
}

async function getLatestAppointmentForClient({ waId, phone, statuses = [] } = {}) {
  const filters = [];
  const values = [];
  let idx = 1;

  if (waId) {
    filters.push(`wa_id = $${idx++}`);
    values.push(String(waId));
  }

  const phoneNorm = normalizePhone(phone || '');
  if (phoneNorm) {
    filters.push(`(wa_phone = $${idx} OR contact_phone = $${idx})`);
    values.push(phoneNorm);
    idx += 1;
  }

  if (!filters.length) return null;

  let sql = `SELECT * FROM appointments WHERE (${filters.join(' OR ')})`;
  if (Array.isArray(statuses) && statuses.length) {
    const placeholders = statuses.map(() => `$${idx++}`);
    sql += ` AND status IN (${placeholders.join(', ')})`;
    values.push(...statuses.map((x) => String(x)));
  }
  sql += ` ORDER BY updated_at DESC, created_at DESC LIMIT 1`;

  const r = await db.query(sql, values);
  return r.rows?.[0] || null;
}

async function listAppointmentsForClient({ waId, phone, statuses = [] } = {}) {
  const filters = [];
  const values = [];
  let idx = 1;

  if (waId) {
    filters.push(`wa_id = $${idx++}`);
    values.push(String(waId));
  }

  const phoneNorm = normalizePhone(phone || '');
  if (phoneNorm) {
    filters.push(`(wa_phone = $${idx} OR contact_phone = $${idx})`);
    values.push(phoneNorm);
    idx += 1;
  }

  if (!filters.length) return [];

  let sql = `SELECT * FROM appointments WHERE (${filters.join(' OR ')})`;
  if (Array.isArray(statuses) && statuses.length) {
    const placeholders = statuses.map(() => `$${idx++}`);
    sql += ` AND status IN (${placeholders.join(', ')})`;
    values.push(...statuses.map((x) => String(x)));
  }
  sql += ` ORDER BY updated_at DESC, created_at DESC`;

  const r = await db.query(sql, values);
  return Array.isArray(r.rows) ? r.rows : [];
}

async function archiveConflictingAppointmentsForClient({ waId, phone, reason = '', keepAppointmentId = null } = {}) {
  const rows = await listAppointmentsForClient({
    waId,
    phone,
    statuses: APPOINTMENT_ACTIVE_CONFLICT_STATUSES,
  });

  const keepId = keepAppointmentId == null ? null : Number(keepAppointmentId);
  const affected = [];
  for (const row of rows) {
    if (keepId && Number(row?.id || 0) === keepId) continue;
    await updateAppointmentStatus(row.id, {
      status: 'cancelled_replaced',
      stylist_decision_note: String(reason || 'Solicitud previa cerrada automáticamente para evitar cruces de flujo.').slice(0, 500),
    });
    affected.push(row.id);
  }
  return affected;
}

async function clearAppointmentStateForCourseFlow({ waId, phone, reason = '' } = {}) {
  const draft = waId ? await getAppointmentDraft(waId) : null;
  if (draft) await deleteAppointmentDraft(waId);
  clearPendingStylistSuggestion(waId);
  const archivedIds = await archiveConflictingAppointmentsForClient({
    waId,
    phone,
    reason: reason || 'Flujo de turno cancelado automáticamente por inicio de inscripción a curso.',
  });
  return {
    clearedDraft: !!draft,
    clearedSuggestion: true,
    archivedAppointments: archivedIds,
  };
}

function looksLikeCourseFlowSignal(text = '', { lastCourseContext = null, pendingCourseDraft = null } = {}) {
  if (pendingCourseDraft) return true;
  const t = normalize(text || '');
  if (!t) return false;
  if (/(masterclass|capacitacion|capacitación|seminario|workshop|taller|curso|cursos)/i.test(t)) return true;
  if (lastCourseContext?.selectedName && /(inscrib|inscripción|inscripcion|anot|reserv(ar|o)? lugar|seña|sena|quiero ese|quiero inscribirme|quiero reservar mi lugar|ya te transferi|ya te transferí|te mande el comprobante|te mandé el comprobante|comprobante|pago|transferencia)/i.test(t)) return true;
  return false;
}

async function findAppointmentByNotificationMessageId(messageId) {
  if (!messageId) return null;
  const r = await db.query(
    `SELECT a.*
       FROM appointment_notifications n
       JOIN appointments a ON a.id = n.appointment_id
      WHERE n.wa_message_id = $1
      ORDER BY n.sent_at DESC
      LIMIT 1`,
    [String(messageId)]
  );
  return r.rows?.[0] || null;
}

async function updateAppointmentStatus(appointmentId, patch = {}) {
  if (!appointmentId) return null;
  const allowed = {
    status: patch.status,
    payment_status: patch.payment_status,
    payment_amount: patch.payment_amount,
    payment_sender: patch.payment_sender,
    payment_receiver: patch.payment_receiver,
    payment_proof_text: patch.payment_proof_text,
    payment_proof_media_id: patch.payment_proof_media_id,
    payment_proof_filename: patch.payment_proof_filename,
    calendar_event_id: patch.calendar_event_id,
    client_name: patch.client_name,
    contact_phone: patch.contact_phone ? normalizePhone(patch.contact_phone) : patch.contact_phone,
    service_name: patch.service_name,
    service_notes: patch.service_notes,
    appointment_date: patch.appointment_date ? toYMD(patch.appointment_date) : patch.appointment_date,
    appointment_time: patch.appointment_time ? normalizeHourHM(patch.appointment_time) : patch.appointment_time,
    duration_min: patch.duration_min == null ? undefined : Number(patch.duration_min || 60) || 60,
    availability_mode: patch.availability_mode ? normalizeAvailabilityMode(patch.availability_mode) : patch.availability_mode,
    stylist_decision_note: patch.stylist_decision_note,
  };

  const sets = [];
  const values = [];
  let idx = 1;
  for (const [key, value] of Object.entries(allowed)) {
    if (value === undefined) continue;
    sets.push(`${key} = $${idx++}`);
    values.push(value);
  }
  if (patch.mark_stylist_decision_at) sets.push(`stylist_decision_at = NOW()`);
  if (!sets.length) return getAppointmentById(appointmentId);
  sets.push(`updated_at = NOW()`);
  values.push(Number(appointmentId));

  const r = await db.query(
    `UPDATE appointments SET ${sets.join(', ')} WHERE id = $${idx} RETURNING *`,
    values
  );
  return r.rows?.[0] || null;
}

function buildAppointmentPaymentMessageFromRow(row) {
  const appt = row || {};
  const dateYMD = resolveAppointmentDateYMD(appt.appointment_date || '');
  const diaOk = dateYMD ? weekdayEsFromYMD(dateYMD) : '';
  const fechaTxt = dateYMD ? ymdToDMY(dateYMD) : '';
  const horaTxt = normalizeHourHM(appt.appointment_time || '') || String(appt.appointment_time || '').slice(0, 5);
  return [
    `Perfecto 😊 ${TURNOS_STYLIST_NAME} ya confirmó que puede tomar ese turno.`,
    '',
    `Servicio: ${appt.service_name || ''}`,
    `📅 Día: ${diaOk ? `${diaOk} ` : ''}${fechaTxt}`.trim(),
    `🕐 Hora: ${horaTxt}`,
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
  ].join('\n').trim();
}

function parseStylistDecisionAction(msg = {}, text = '') {
  const payload = normalize(msg?.button?.payload || msg?.interactive?.button_reply?.id || '');
  const title = normalize(msg?.button?.text || msg?.interactive?.button_reply?.title || text || '');
  const hay = `${payload} | ${title}`.trim();

  if (/(?:^|\b)(decline|reject|rechaz|rechazar|no_puede|no puede|no_disponible|ocupada|ocupado|imposible|no me da|no llego)(?:\b|$)/i.test(hay)) {
    return 'decline';
  }

  if (/(?:^|\b)(suggest|suger|sugerir|proponer|propongo|otro_horario|otro horario|te ofrezco|ofrezco|podria|podría)(?:\b|$)/i.test(hay)) {
    return 'suggest';
  }

  if (/(?:^|\b)(accept|accepted|acept|aceptar|confirm|confirmar|disponible|puedo|si puedo|sí puedo)(?:\b|$)/i.test(hay)) {
    return 'accept';
  }

  return '';
}

function stylistSuggestionNeedsDetails(msg = {}, text = '') {
  const buttonText = normalize(msg?.button?.text || msg?.interactive?.button_reply?.title || '');
  const raw = normalize(text || '');
  if (!raw) return true;
  if (/^(sugerir otro|sugerir otro horario|otro horario|suggest)$/.test(raw)) return true;
  if (buttonText && raw === buttonText) return true;
  return false;
}

async function notifyStylistTurnConfirmed(apptRow) {
  const recipient = normalizeWhatsAppRecipient(STYLIST_NOTIFY_PHONE_RAW);
  if (!recipient || !apptRow?.id) return false;

  const appt = buildAppointmentData({
    id: apptRow.id,
    wa_id: apptRow.wa_id || '',
    wa_phone: apptRow.wa_phone || '',
    client_name: apptRow.client_name || '',
    contact_phone: apptRow.contact_phone || '',
    service_name: apptRow.service_name || '',
    appointment_date: apptRow.appointment_date || '',
    appointment_time: apptRow.appointment_time || '',
    status: apptRow.status || 'booked',
    stylist_notified_at: apptRow.stylist_notified_at || null,
  });

  await sendAppointmentTemplateAndLog({
    appointmentId: appt.id,
    recipientPhone: recipient,
    templateName: TEMPLATE_TURNO_CONFIRMADO_PELUQUERA,
    notificationType: 'stylist_turn_confirmed',
    vars: [
      appt.client_name || 'Cliente',
      appt.service_name || 'Servicio',
      formatAppointmentDateForTemplate(appt.appointment_date),
      formatAppointmentTimeForTemplate(appt.appointment_time),
      normalizePhone(appt.contact_phone || appt.wa_phone || ''),
    ],
  });

  return true;
}

async function handleStylistWorkflowInbound({ msg, text, phone, phoneRaw }) {
  const stylistPhone = normalizePhone(STYLIST_NOTIFY_PHONE_RAW);
  const inboundPhone = normalizePhone(phoneRaw || phone || '');
  if (!stylistPhone || inboundPhone !== stylistPhone) return false;
  if (parseCourseManagerApprovalAction(msg, text) === 'approve') return false;

  const contextMsgId = msg?.context?.id || '';
  let appt = await findAppointmentByNotificationMessageId(contextMsgId);
  if (!appt) {
    const r = await db.query(
      `SELECT * FROM appointments
        WHERE status = 'pending_stylist_confirmation'
        ORDER BY updated_at DESC, created_at DESC
        LIMIT 2`
    );
    const pendingRows = Array.isArray(r.rows) ? r.rows : [];
    if (pendingRows.length === 1) {
      appt = pendingRows[0];
    } else if (pendingRows.length > 1) {
      await sendWhatsAppText(phone, 'Veo más de un turno pendiente. Respondeme tocando o contestando sobre el mensaje puntual del turno para no equivocarme.');
      return true;
    }
  }

  if (!appt) {
    await sendWhatsAppText(phone, 'No encontré un turno pendiente asociado a esa respuesta.');
    return true;
  }

  if (String(appt.status || '').trim() !== 'pending_stylist_confirmation') {
    await sendWhatsAppText(phone, 'Ese turno ya no está pendiente de confirmación. Si querés responder otro, hacelo sobre el mensaje correcto del turno pendiente.');
    return true;
  }

  const action = parseStylistDecisionAction(msg, text);
  const suggestionDate = extractLikelyDateFromText(text);
  const suggestionTime = extractLikelyHourFromText(text);
  const suggestionBits = [];
  if (suggestionDate) suggestionBits.push(ymdToDMY(suggestionDate));
  if (suggestionTime) suggestionBits.push(normalizeHourHM(suggestionTime));
  const suggestionText = suggestionBits.length ? suggestionBits.join(' a las ') : String(text || '').trim();

  if (action === 'accept') {
    const updated = await updateAppointmentStatus(appt.id, {
      status: 'awaiting_payment',
      payment_status: 'not_paid',
      stylist_decision_note: String(text || '').trim() || 'Aceptado por estilista',
      mark_stylist_decision_at: true,
    });
    await saveAppointmentDraft(appt.wa_id, appt.wa_phone, mapAppointmentRowToDraft(updated || appt));
    const msgClient = buildAppointmentPaymentMessageFromRow(updated || appt);
    pushHistory(appt.wa_id, 'assistant', msgClient);
    await sendWhatsAppText(appt.contact_phone || appt.wa_phone, msgClient);
    await sendWhatsAppText(phone, 'Perfecto. Ya le pedí la seña a la clienta. Cuando la envíe, te aviso por aquí.');
    updateLastCloseContext(appt.wa_id, { suppressInactivityPrompt: true });
    scheduleInactivityFollowUp(appt.wa_id, appt.contact_phone || appt.wa_phone);
    return true;
  }

  if (action === 'decline' || action === 'suggest') {
    if (action === 'suggest' && stylistSuggestionNeedsDetails(msg, text)) {
      await sendWhatsAppText(phone, 'Decime el nuevo día y horario en el mismo mensaje, por ejemplo: *martes 17 de abril a las 18 hs*.');
      return true;
    }

    await deleteAppointmentDraft(appt.wa_id);
    clearPendingStylistSuggestion(appt.wa_id);

    const suggestedDateYMD = suggestionDate || resolveAppointmentDateYMD(appt.appointment_date || '');
    const suggestedTimeHM = normalizeHourHM(suggestionTime || '');
    const suggestedMode = suggestedTimeHM && isHourAllowedForAvailabilityMode(suggestedTimeHM, 'siesta') ? 'siesta' : 'commercial';

    if (action === 'suggest' && (!suggestedDateYMD || !suggestedTimeHM || !isHourAllowedForAvailabilityMode(suggestedTimeHM, suggestedMode) || isSundayYMD(suggestedDateYMD) || isPastAppointmentDateTime(suggestedDateYMD, suggestedTimeHM))) {
      await sendWhatsAppText(phone, 'Necesito que me sugieras un día y horario válido dentro de los permitidos. Ejemplo: *jueves 17 de abril a las 18 hs* o *mañana 15 hs*.');
      return true;
    }

    await updateAppointmentStatus(appt.id, {
      status: action === 'suggest' && suggestionText ? 'stylist_suggested' : 'stylist_rejected',
      stylist_decision_note: String(text || '').trim() || (action === 'suggest' ? 'La estilista propuso otro horario' : 'La estilista no puede en ese horario'),
      mark_stylist_decision_at: true,
    });

    if (action === 'suggest' && suggestionText) {
      setPendingStylistSuggestion(appt.wa_id, {
        appointment_id: Number(appt.id),
        fecha: suggestedDateYMD,
        hora: suggestedTimeHM,
        servicio: appt.service_name || '',
        duracion_min: Number(appt.duration_min || 60) || 60,
        cliente_full: appt.client_name || '',
        telefono_contacto: normalizePhone(appt.contact_phone || appt.wa_phone || ''),
        wa_phone: normalizePhone(appt.wa_phone || ''),
        availability_mode: normalizeAvailabilityMode(suggestedMode),
        stylist_note: String(text || '').trim(),
      });
    }

    const msgClient = action === 'suggest' && suggestionText
      ? `La estilista no puede en ese horario, pero me ofrece *${suggestedDateYMD ? `${capitalizeEs(weekdayEsFromYMD(suggestedDateYMD))} ${ymdToDM(suggestedDateYMD)} ` : ''}${suggestedTimeHM}*.

Si le sirve, respóndame *sí* y le paso la seña. Si no, dígame otro día u horario y le paso lo disponible 😊`
      : `La estilista no puede en ese horario.

Si ninguno de los horarios comerciales le sirve, puedo revisar otros horarios 😊`;
    pushHistory(appt.wa_id, 'assistant', msgClient);
    await sendWhatsAppText(appt.contact_phone || appt.wa_phone, msgClient);
    await sendWhatsAppText(phone, action === 'suggest' && suggestionText
      ? 'Perfecto. Ya le propuse el horario alternativo a la clienta.'
      : 'Perfecto. Ya le avisé a la clienta para que elija otro horario.');
    updateLastCloseContext(appt.wa_id, { suppressInactivityPrompt: true });
    scheduleInactivityFollowUp(appt.wa_id, appt.contact_phone || appt.wa_phone);
    return true;
  }

  await sendWhatsAppText(phone, 'Respondeme con aceptar o no puede, o sugerime otro horario, y yo sigo el flujo con la clienta.');
  return true;
}

function buildStylistTimeoutClientMessage(appt = {}) {
  const fechaYMD = resolveAppointmentDateYMD(appt?.appointment_date || '');
  const fechaTxt = fechaYMD ? ymdToDMY(fechaYMD) : '';
  const horaTxt = formatAppointmentTimeForTemplate(appt?.appointment_time || '');
  const servicioTxt = String(appt?.service_name || '').trim();
  return [
    'Todavía no recibimos confirmación de la estilista para ese turno 😊',
    '',
    servicioTxt ? `Servicio: ${servicioTxt}` : '',
    fechaTxt ? `📅 Fecha solicitada: ${fechaTxt}` : '',
    horaTxt ? `🕐 Hora solicitada: ${horaTxt}` : '',
    '',
    'Si quiere, puedo volver a revisar otro horario disponible y seguimos por acá ✨',
  ].filter(Boolean).join('\n').trim();
}

function buildCourseManagerTimeoutStudentMessage(enrollment = {}) {
  const studentName = String(enrollment?.student_name || '').trim();
  const courseName = String(enrollment?.course_name || '').trim();
  const saludo = studentName ? `${studentName}, ` : '';
  return [
    `Perfecto 😊 ${saludo}ya tenemos registrada la seña${courseName ? ` para *${courseName}*` : ''}.`,
    '',
    'La responsable todavía no terminó de revisarlo, así que por ahora quedó *pendiente de confirmación manual*.',
    'Apenas lo valide, le avisamos por este mismo medio ✨',
  ].join('\n').trim();
}

async function processAppointmentResponseTimeouts() {
  const r = await db.query(
    `SELECT id, wa_id, wa_phone, client_name, contact_phone, service_name,
            appointment_date::text AS appointment_date,
            to_char(appointment_time, 'HH24:MI') AS appointment_time,
            status,
            stylist_notified_at
       FROM appointments
      WHERE status = 'pending_stylist_confirmation'
        AND stylist_notified_at IS NOT NULL`
  );

  for (const raw of (r.rows || [])) {
    const appt = buildAppointmentData(raw);
    const notifiedAt = new Date(appt.stylist_notified_at || 0).getTime();
    if (!notifiedAt || Number.isNaN(notifiedAt)) continue;
    if ((Date.now() - notifiedAt) < STYLIST_CONFIRMATION_TIMEOUT_MS) continue;

    try {
      const updated = await updateAppointmentStatus(appt.id, {
        status: 'stylist_timeout',
        stylist_decision_note: `Sin respuesta de la estilista dentro de ${Math.round(STYLIST_CONFIRMATION_TIMEOUT_MS / 3600000)} horas.`,
        mark_stylist_decision_at: true,
      });
      await deleteAppointmentDraft(appt.wa_id);
      clearPendingStylistSuggestion(appt.wa_id);

      const clientPhone = normalizeWhatsAppRecipient(appt.contact_phone || appt.wa_phone || '');
      const clientMsg = buildStylistTimeoutClientMessage(updated || appt);
      if (clientPhone && clientMsg) {
        pushHistory(appt.wa_id, 'assistant', clientMsg);
        await sendWhatsAppText(clientPhone, clientMsg);
      }
    } catch (e) {
      console.error(`❌ Error venciendo espera de estilista para appointment ${appt.id}:`, e?.response?.data || e?.message || e);
    }
  }
}

async function processCourseManagerApprovalTimeouts() {
  const r = await db.query(
    `SELECT n.*, e.wa_phone, e.contact_phone, e.student_name, e.course_name
       FROM course_enrollment_notifications n
       JOIN course_enrollments e ON e.id = n.enrollment_id
      WHERE n.notification_type = 'course_payment_received'
        AND n.approved_at IS NULL
        AND n.expired_at IS NULL`
  );

  for (const row of (r.rows || [])) {
    const sentAt = new Date(row.sent_at || 0).getTime();
    if (!sentAt || Number.isNaN(sentAt)) continue;
    if ((Date.now() - sentAt) < COURSE_MANAGER_CONFIRMATION_TIMEOUT_MS) continue;

    try {
      const expired = await db.query(
        `UPDATE course_enrollment_notifications
            SET expired_at = COALESCE(expired_at, NOW()),
                expired_reason = COALESCE(expired_reason, $2)
          WHERE id = $1
            AND approved_at IS NULL
            AND expired_at IS NULL
          RETURNING *`,
        [Number(row.id || 0), `Sin respuesta de la responsable dentro de ${Math.round(COURSE_MANAGER_CONFIRMATION_TIMEOUT_MS / 3600000)} horas.`]
      );
      const expiredRow = expired.rows?.[0];
      if (!expiredRow) continue;

      const studentPhone = normalizeWhatsAppRecipient(row.wa_phone || row.contact_phone || '');
      const studentMsg = buildCourseManagerTimeoutStudentMessage(row);
      if (studentPhone && studentMsg) {
        await sendWhatsAppText(studentPhone, studentMsg);
      }
    } catch (e) {
      console.error(`❌ Error venciendo aprobación de curso ${row.enrollment_id}:`, e?.response?.data || e?.message || e);
    }
  }
}

async function processAppointmentTemplateNotifications() {
  const stylistRecipient = normalizeWhatsAppRecipient(STYLIST_NOTIFY_PHONE_RAW);
  if (!stylistRecipient) return;

  const r = await db.query(
    `SELECT id, wa_id, wa_phone, client_name, contact_phone, service_name,
            appointment_date::text AS appointment_date,
            to_char(appointment_time, 'HH24:MI') AS appointment_time,
            status,
            stylist_notified_at
       FROM appointments
      WHERE appointment_date IS NOT NULL
        AND appointment_time IS NOT NULL
        AND status = 'pending_stylist_confirmation'
        AND appointment_date >= CURRENT_DATE - INTERVAL '1 day'
      ORDER BY appointment_date ASC, appointment_time ASC`
  );

  for (const raw of (r.rows || [])) {
    const appt = buildAppointmentData(raw);
    const startAt = getAppointmentStartDate(appt);
    if (!startAt) continue;
    if (startAt.getTime() <= Date.now()) continue;

    try {
      if (!appt.stylist_notified_at) {
        await sendNewAppointmentTemplateToStylist(appt);
      }
    } catch (e) {
      console.error(`❌ Error enviando plantilla inicial de turno para appointment ${appt.id}:`, e?.response?.data || e?.message || e);
    }
  }
}

async function finalizeAppointmentFlow({ waId, phone, merged }) {
  merged.fecha = toYMD(merged.fecha);
  merged.hora = normalizeHourHM(merged.hora);
  merged.availability_mode = normalizeAvailabilityMode(merged.availability_mode || 'commercial');
  if (!merged?.servicio || !merged?.fecha || !merged?.hora) return { type: "missing_core" };

  if (!isHourAllowedForAvailabilityMode(merged.hora, merged.availability_mode)) {
    return { type: 'invalid_hour' };
  }

  if (isPastAppointmentDateTime(merged.fecha, merged.hora)) return { type: "invalid_past_date" };

  const busy = await calendarHasConflict({
    dateYMD: merged.fecha,
    startHM: merged.hora,
    durationMin: Number(merged.duracion_min || 60) || 60,
  });
  if (busy) return { type: "busy" };

  if (!merged?.cliente_full) return { type: "need_name" };
  if (!merged?.telefono_contacto) return { type: "need_phone" };

  if (merged?.appointment_id) {
    if (merged.payment_status !== "paid_verified") return { type: "need_payment" };

    const currentAppt = await getAppointmentById(merged.appointment_id);
    if (currentAppt?.status === 'pending_stylist_confirmation') return { type: 'still_waiting_stylist' };

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

    const bookedRow = await updateAppointmentStatus(merged.appointment_id, {
      status: 'booked',
      payment_status: 'paid_verified',
      payment_amount: merged.payment_amount == null ? 10000 : Number(merged.payment_amount),
      payment_sender: merged.payment_sender || '',
      payment_receiver: merged.payment_receiver || TURNOS_ALIAS_TITULAR,
      payment_proof_text: merged.payment_proof_text || '',
      payment_proof_media_id: merged.payment_proof_media_id || '',
      payment_proof_filename: merged.payment_proof_filename || '',
      calendar_event_id: ev?.eventId || null,
      client_name: merged.cliente_full || '',
      contact_phone: normalizePhone(merged.telefono_contacto || ''),
      service_name: merged.servicio || '',
      service_notes: merged.notas || '',
      appointment_date: merged.fecha,
      appointment_time: merged.hora,
      duration_min: Number(merged.duracion_min || 60) || 60,
      availability_mode: merged.availability_mode,
    });

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

    try {
      const bookedRowForStylist = await getAppointmentRowById(merged.appointment_id);
      await notifyStylistTurnConfirmed(bookedRowForStylist || bookedRow || currentAppt || {});
    } catch (e) {
      console.error('❌ Error avisando a la peluquera que el turno ya quedó señado:', e?.response?.data || e?.message || e);
    }

    await deleteAppointmentDraft(waId);
    updateLastCloseContext(waId, { suppressInactivityPrompt: false });
    return { type: "booked", eventId: ev?.eventId || null };
  }

  const toCreate = {
    ...merged,
    payment_status: 'not_paid',
    payment_amount: null,
    payment_sender: '',
    payment_receiver: '',
    payment_proof_text: '',
    payment_proof_media_id: '',
    payment_proof_filename: '',
  };

  const apptRow = await createAppointmentRecord({
    waId,
    waPhone: phone,
    merged: toCreate,
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

function isColorOrTinturaService(text) {
  const t = normalize(text);
  return /(\bcolor\b|coloraci|tintur|tinte|mecha|balayage|decolor|reflej|raic|raiz|ilumin|tonaliz)/i.test(t);
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
- Interpretá fechas relativas ("mañana", "el viernes", "el lunes") usando ${nowTxt} y zona ${TIMEZONE}.
- Si el texto NO es un pedido de turno NI una continuación de reserva, ok=false.
- Si te paso un contexto con datos ya conocidos, mantenelos y completá lo faltante.
- Si el contexto ya trae servicio y el cliente responde solo con fecha, hora o ambas cosas, eso sigue siendo continuación de turno y ok=true.
- Ejemplos de continuación válida con contexto previo:
  - servicio=Alisado + mensaje="el lunes a las 17" => ok=true, fecha y hora completas.
  - servicio=Alisado + mensaje="lunes 17 hs" => ok=true.
  - servicio=Alisado + mensaje="a las 17:30" => ok=true, conservar fecha del contexto si ya existía.
  - servicio=Alisado + mensaje="el lunes" => ok=true, completar fecha y dejar hora faltante.
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

function getAvailableSlotsForDate({ dateYMD, durationMin, events = [], availabilityMode = 'commercial' }) {
  const safeDate = toYMD(dateYMD);
  const safeDuration = Math.max(30, Number(durationMin) || 60);
  if (!safeDate || isSundayYMD(safeDate) || safeDate < todayYMDInTZ()) return [];

  const nowLocal = formatYMDHMInTZ(new Date());
  const nowMinutes = safeDate === nowLocal.ymd ? hmToMinutes(nowLocal.hm) : -1;
  const slots = [];

  for (const hm of getTurnoAllowedStartTimes(availabilityMode)) {
    const slotMinutes = hmToMinutes(hm);
    if (Number.isNaN(slotMinutes)) continue;
    if (safeDate === nowLocal.ymd && nowMinutes >= 0 && slotMinutes <= nowMinutes) continue;

    const block = getTurnoAllowedBlocks(availabilityMode).find((item) => {
      const start = hmToMinutes(item.start);
      const end = hmToMinutes(item.end);
      return !Number.isNaN(start) && !Number.isNaN(end) && slotMinutes >= start && slotMinutes < end;
    });
    if (!block) continue;

    if ((slotMinutes + safeDuration) > hmToMinutes(block.end)) continue;

    const hasConflict = events.some((ev) => eventOverlapsSlot(ev, {
      dateYMD: safeDate,
      startHM: hm,
      durationMin: safeDuration,
    }));

    if (!hasConflict) {
      slots.push({ hm, label: block.label });
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

function formatSlotsByBlock(slots = [], availabilityMode = 'commercial') {
  if (!Array.isArray(slots) || !slots.length) return '';

  const grouped = getTurnoAllowedBlocks(availabilityMode)
    .map((block) => {
      const values = slots.filter((slot) => slot.label === block.label).map((slot) => slot.hm);
      return values.length ? `${block.label}: ${values.join(', ')}` : '';
    })
    .filter(Boolean);

  return grouped.join(' | ');
}

function formatSlotsByBlockMultiline(slots = [], availabilityMode = 'commercial') {
  if (!Array.isArray(slots) || !slots.length) return '• Sin horarios disponibles';

  return getTurnoAllowedBlocks(availabilityMode)
    .map((block) => {
      const values = slots.filter((slot) => slot.label === block.label).map((slot) => slot.hm);
      return values.length ? `• ${capitalizeEs(block.label)}: ${values.join(', ')}` : '';
    })
    .filter(Boolean)
    .join('\n');
}

function buildCalendarUnavailableAvailabilityMessage(availabilityMode = 'commercial') {
  const mode = normalizeAvailabilityMode(availabilityMode);
  return mode === 'siesta'
    ? `En este momento no puedo revisar los horarios especiales de siesta porque no estoy leyendo Google Calendar. Apenas se restablezca, le paso solo los horarios realmente libres 😊`
    : `En este momento no puedo revisar los turnos porque no estoy leyendo Google Calendar. Para no ofrecer horarios ocupados, prefiero no pasarle opciones hasta reconectar el calendario 😊`;
}

async function getAvailabilitySummaries({ daysYMD, durationMin, availabilityMode = 'commercial' }) {
  const safeDays = (Array.isArray(daysYMD) ? daysYMD : []).map((d) => toYMD(d)).filter(Boolean);
  if (!safeDays.length) return [];

  if (!CALENDAR_ID) {
    return safeDays.map((dateYMD) => ({
      dateYMD,
      weekday: weekdayEsFromYMD(dateYMD),
      slots: [],
      calendarUnavailable: true,
    }));
  }

  const first = safeDays[0];
  const lastExclusive = addDaysToYMD(safeDays[safeDays.length - 1], 1);

  try {
    const items = await listCalendarEventsInRange({ fromYMD: first, toYMDExclusive: lastExclusive });
    return safeDays.map((dateYMD) => ({
      dateYMD,
      weekday: weekdayEsFromYMD(dateYMD),
      slots: getAvailableSlotsForDate({ dateYMD, durationMin, events: items, availabilityMode }),
      calendarUnavailable: false,
    }));
  } catch (err) {
    console.error('❌ Error leyendo Google Calendar para disponibilidad:', err?.response?.data || err?.message || err);
    return safeDays.map((dateYMD) => ({
      dateYMD,
      weekday: weekdayEsFromYMD(dateYMD),
      slots: [],
      calendarUnavailable: true,
    }));
  }
}

async function buildWeeklyAvailabilityMessage({ servicio, durationMin, limitDays = 6, availabilityMode = 'commercial' }) {
  const mode = normalizeAvailabilityMode(availabilityMode);
  const days = getUpcomingTurnoDays(limitDays);
  const summaries = await getAvailabilitySummaries({ daysYMD: days, durationMin, availabilityMode: mode });

  if (summaries.some((item) => item.calendarUnavailable)) {
    return buildCalendarUnavailableAvailabilityMessage(mode);
  }

  const available = summaries.filter((item) => item.slots.length > 0);

  if (!available.length) {
    return mode === 'siesta'
      ? `En este momento no me quedan turnos disponibles en horario especial de siesta. Si quiere, le reviso otras opciones comerciales 😊`
      : `En este momento no me quedan turnos disponibles en los próximos días dentro de los horarios comerciales. Si ninguno de esos horarios le sirve, también puedo revisar la franja especial de siesta de 14, 15 o 16 hs 😊`;
  }

  const daysToShow = available.slice(0, 3);
  const lines = daysToShow.map((item) => {
    const dayHeader = `*${capitalizeEs(item.weekday)} ${ymdToDM(item.dateYMD)}*`;
    const slotLines = formatSlotsByBlockMultiline(item.slots, mode);
    return `${dayHeader}
${slotLines}`;
  });

  const footer = mode === 'siesta'
    ? `Decime qué día y horario especial de siesta le queda mejor y lo dejo presentado a la estilista.`
    : `Decime qué día y horario le queda mejor y lo presento primero a la estilista. Si ninguno de estos horarios le sirve, también puedo revisar 14, 15 o 16 hs 😊`;

  return `Perfecto 😊

${servicio ? `Servicio: ${servicio}

` : ''}Te paso los próximos turnos disponibles:

${lines.join('\n\n')}

${footer}`;
}

async function buildDateAvailabilityMessage({ dateYMD, servicio, durationMin, availabilityMode = 'commercial' }) {
  const safeDate = toYMD(dateYMD);
  if (!safeDate) return '';

  if (isSundayYMD(safeDate)) {
    return `Los domingos no trabajamos con turnos 😊

Si quiere, le paso las opciones disponibles de lunes a sábado.`;
  }

  const mode = normalizeAvailabilityMode(availabilityMode);
  const [summary] = await getAvailabilitySummaries({ daysYMD: [safeDate], durationMin, availabilityMode: mode });
  if (summary?.calendarUnavailable) {
    return buildCalendarUnavailableAvailabilityMessage(mode);
  }

  const dayLabel = `${capitalizeEs(summary?.weekday || weekdayEsFromYMD(safeDate))} ${ymdToDM(safeDate)}`;

  if (summary?.slots?.length) {
    const footer = mode === 'siesta'
      ? 'Decime cuál le queda mejor dentro del horario especial de siesta y lo presento a la estilista.'
      : 'Decime cuál le queda mejor y lo presento a la estilista. Si ninguno de estos horarios le sirve, también puedo revisar 14, 15 o 16 hs 😊';
    return `Te digo lo que nos queda disponible:

${servicio ? `Servicio: ${servicio}

` : ''}*${dayLabel}*
${formatSlotsByBlockMultiline(summary.slots, mode)}

${footer}`;
  }

  const weekly = await buildWeeklyAvailabilityMessage({ servicio, durationMin, limitDays: 6, availabilityMode: mode });
  return `Ese día no me queda lugar disponible dentro de ${mode === 'siesta' ? 'ese horario especial de siesta' : 'los horarios comerciales'}.

${weekly}`;
}

async function buildBusyTurnoMessage({ base }) {
  const safeDate = toYMD(base?.fecha || '');
  const safeDuration = Math.max(30, Number(base?.duracion_min || 60) || 60);
  const servicio = base?.servicio || base?.last_service_name || '';
  const availabilityMode = normalizeAvailabilityMode(base?.availability_mode || 'commercial');

  if (safeDate) {
    const sameDayMsg = await buildDateAvailabilityMessage({ dateYMD: safeDate, servicio, durationMin: safeDuration, availabilityMode });
    const diaC = capitalizeEs(weekdayEsFromYMD(safeDate));
    return `Ese horario ya está ocupado (${diaC} ${ymdToDM(safeDate)} ${normalizeHourHM(base?.hora) || base?.hora}).

${sameDayMsg}`;
  }

  return buildWeeklyAvailabilityMessage({ servicio, durationMin: safeDuration, limitDays: 6, availabilityMode });
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

  m = s.match(/^(\d{4})-(\d{2})-(\d{2})[T\s]/);
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

  const courseInterest = buildHubSpotCourseInterestLabel(source);
  if (courseInterest && courseInterest !== 'CURSO') return courseInterest;

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
      precioSinPromocion: header.findIndex(h => {
        const v = normalize(h);
        return v === "precio sin promocion" || v.includes("precio sin promocion");
      }),
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
        precioSinPromocion: idx.precioSinPromocion >= 0 ? (r[idx.precioSinPromocion] || "").trim() : "",
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

function isCatalogAdministrativeRow(row) {
  const bag = normalizeCatalogSearchText(`${row?.tab || ''} ${row?.nombre || ''} ${row?.categoria || ''} ${row?.descripcion || ''}`);
  if (!bag) return false;
  if (/(\bseña\b|\bsena\b|\bseñas\b|\bsenas\b|\bturno\b|\bturnos\b|\bcorte masculino\b|\borden de llegada\b|\bhorario\b|\bhorarios\b|\binscrip|\binscripción|\bcurso\b|\bcursos\b|\bclase\b|\bclases\b|\bcapacit)/i.test(bag)) {
    return true;
  }
  if (/\bservicio\b|\bservicios\b/i.test(bag) && !/(shampoo|champu|champú|acondicionador|mascara|mascarilla|serum|sérum|aceite|oleo|óleo|tintura|oxidante|decolorante|matizador|ampolla|keratina|alisado|botox|protector|spray|gel|cera|plancha|secador|camilla|sillon|sillón|espejo|mueble|mesa|puff)/i.test(bag)) {
    return true;
  }
  return false;
}

function isCatalogRowAvailable(row) {
  const stockTxt = normalizeCatalogSearchText(String(row?.stock || '').trim());
  if (!stockTxt) return true;
  if (/(sin stock|agotad|no disponible|no hay|sin unidades)/i.test(stockTxt)) return false;
  if (/^0+(?:[.,]0+)?$/.test(stockTxt)) return false;
  const m = stockTxt.match(/\d+(?:[.,]\d+)?/);
  if (m) {
    const qty = Number(String(m[0]).replace(',', '.'));
    if (!Number.isNaN(qty)) return qty > 0;
  }
  return true;
}

function filterSellableCatalogRows(rows, { includeOutOfStock = true } = {}) {
  const base = Array.isArray(rows) ? rows.filter((row) => row?.nombre && !isCatalogAdministrativeRow(row)) : [];
  return includeOutOfStock ? base : base.filter((row) => isCatalogRowAvailable(row));
}

function buildOutOfStockCatalogMessage({ domain = '', family = '', query = '' } = {}) {
  const normalizedQuery = normalizeCatalogSearchText(query || '');
  if (family === 'plancha') return 'Por el momento no tenemos una plancha para el pelo disponible en stock.';
  if (family === 'secador') return 'Por el momento no tenemos un secador para el pelo disponible en stock.';
  if (family === 'sillon') return 'Por el momento no tenemos sillones de barbería disponibles en stock.';
  if (family === 'camilla') return 'Por el momento no tenemos camillas disponibles en stock.';
  if (/\bampolla\b/.test(normalizedQuery)) return 'Por el momento no tenemos ampollas disponibles en stock.';
  return domain === 'furniture'
    ? 'Por el momento ese mueble o equipamiento no está disponible en stock.'
    : 'Por el momento ese producto no está disponible en stock.';
}

const AUTO_PHOTO_SPECIAL_ALIASES = ['capa', 'capas', 'tijera', 'tijeras', 'rociador', 'rociadores'];
const AUTO_PHOTO_TAB_NAMES = ['equipamiento', 'muebles'];

function isAutoPhotoTabName(tab = '') {
  const t = normalizeCatalogSearchText(tab || '');
  return AUTO_PHOTO_TAB_NAMES.includes(t);
}

function rowMatchesAutoPhotoSpecial(row) {
  const hay = buildStockHaystack(row);
  return AUTO_PHOTO_SPECIAL_ALIASES.some((alias) => containsCatalogPhrase(hay, alias));
}

function isAutoPhotoExceptionalRow(row) {
  if (!row?.nombre) return false;
  if (isAutoPhotoTabName(row?.tab || '')) return true;
  return rowMatchesAutoPhotoSpecial(row);
}

function selectAutoPhotoExceptionalRows(rows, { domain = '', family = '', query = '' } = {}) {
  const list = Array.isArray(rows) ? rows.filter((row) => !!row?.nombre) : [];
  if (!list.length) return [];

  const normalizedFamily = normalizeCatalogSearchText(family || '');
  const normalizedQuery = normalizeCatalogSearchText(query || '');
  const forcesException = (
    domain === 'furniture'
    || ['plancha', 'secador'].includes(normalizedFamily)
    || /(\bplancha\b|\bplanchita\b|\bplanchitas\b|\bsecador\b|\bsecadores\b|\bcapa\b|\bcapas\b|\btijera\b|\btijeras\b|\brociador\b|\brociadores\b|\bmueble\b|\bmuebles\b|\bequipamiento\b|\bsillon\b|\bsillón\b|\bsillones\b|\bcamilla\b|\bcamillas\b|\bespejo\b|\bespejos\b)/i.test(normalizedQuery)
  );

  const preferred = list.filter((row) => isAutoPhotoExceptionalRow(row) || (domain === 'furniture' && detectRowProductDomain(row) === 'furniture'));
  if (preferred.length) return preferred;
  return forcesException ? list : [];
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

const FURNITURE_FAMILY_DEFS = [
  { id: 'sillon', label: 'sillón', aliases: ['sillon', 'sillón', 'sillones'] },
  { id: 'camilla', label: 'camilla', aliases: ['camilla', 'camillas'] },
  { id: 'espejo', label: 'espejo', aliases: ['espejo', 'espejos', 'espejo led', 'espejos led'] },
  { id: 'mesa', label: 'mesa', aliases: ['mesa', 'mesas', 'mesa manicura', 'mesa de manicura'] },
  { id: 'puff', label: 'puff', aliases: ['puff', 'puffs'] },
  { id: 'respaldo', label: 'respaldo', aliases: ['respaldo', 'respaldos'] },
  { id: 'combo', label: 'combo', aliases: ['combo', 'combos'] },
  { id: 'tocador', label: 'tocador', aliases: ['tocador', 'tocadores'] },
  { id: 'recepcion', label: 'recepción', aliases: ['recepcion', 'recepción', 'mostrador'] },
  { id: 'equipamiento', label: 'equipamiento', aliases: ['equipamiento', 'mobiliario', 'mueble', 'muebles'] },
];

const HAIR_TREATMENT_KNOWLEDGE = [
  {
    id: 'hidratacion_reparacion',
    label: 'hidratación y nutrición capilar',
    triggerFamilies: ['bano_de_crema', 'tratamiento', 'shampoo', 'acondicionador', 'serum', 'aceite', 'protector'],
    triggerKeywords: ['baño de crema', 'bano de crema', 'nutricion', 'nutrición', 'hidratacion', 'hidratación', 'reparacion', 'reparación', 'cabello seco', 'pelo seco', 'frizz', 'brillo', 'suavidad'],
    primaryFamilies: ['bano_de_crema', 'tratamiento', 'shampoo', 'acondicionador'],
    complementFamiliesPersonal: ['serum', 'aceite', 'protector'],
    complementFamiliesProfessional: ['serum', 'aceite', 'protector', 'tratamiento', 'shampoo'],
    stepsPersonal: [
      'lavado según la necesidad del cabello',
      'aplicación del baño de crema o tratamiento',
      'enjuague y orden del lavado',
      'finalización con sérum, aceite o protector'
    ],
    stepsProfessional: [
      'diagnóstico del cabello y lavado técnico',
      'aplicación del tratamiento o baño de crema',
      'tiempo de pose y enjuague',
      'terminación con sérum, aceite o protector para sellar'
    ],
    followup: '¿Lo necesita para uso personal o para trabajar? ¿Busca hidratación, reparación, bajar frizz o más brillo final?'
  },
  {
    id: 'coloracion_mantenimiento',
    label: 'coloración y mantenimiento del color',
    triggerFamilies: ['tintura', 'oxidante', 'decolorante', 'matizador', 'shampoo', 'tratamiento', 'serum'],
    triggerKeywords: ['tintura', 'coloracion', 'coloración', 'decolorante', 'oxidante', 'matizador', 'rubio', 'canas', 'mechas', 'balayage', 'tono', 'reflejos'],
    primaryFamilies: ['tintura', 'oxidante', 'decolorante', 'matizador'],
    complementFamiliesPersonal: ['shampoo', 'tratamiento', 'serum'],
    complementFamiliesProfessional: ['oxidante', 'matizador', 'tratamiento', 'serum', 'shampoo'],
    stepsPersonal: [
      'servicio o coloración según el objetivo',
      'lavado de mantenimiento del color',
      'tratamiento post color',
      'finalización con sérum o cuidado de brillo'
    ],
    stepsProfessional: [
      'diagnóstico del tono y técnica a realizar',
      'mezcla con oxidante / decoloración / coloración',
      'matización o corrección si hace falta',
      'tratamiento post color y finalización'
    ],
    followup: '¿Es para uso personal o profesional? ¿Busca cubrir canas, mantener rubios, matizar o trabajar coloración completa?'
  },
  {
    id: 'alisado_reconstruccion',
    label: 'alisado, keratina y reconstrucción',
    triggerFamilies: ['alisado', 'keratina', 'botox', 'shampoo', 'tratamiento', 'serum', 'protector'],
    triggerKeywords: ['alisado', 'keratina', 'keratin', 'botox capilar', 'reconstruccion', 'reconstrucción', 'sellado', 'lacio', 'anti frizz', 'frizz', 'plastificado'],
    primaryFamilies: ['alisado', 'keratina', 'botox', 'tratamiento'],
    complementFamiliesPersonal: ['shampoo', 'serum', 'protector'],
    complementFamiliesProfessional: ['shampoo', 'tratamiento', 'serum', 'protector'],
    stepsPersonal: [
      'lavado y preparación del cabello',
      'aplicación del producto de alisado o reconstrucción',
      'sellado y planchado si corresponde',
      'mantenimiento con shampoo y finalizador'
    ],
    stepsProfessional: [
      'diagnóstico y lavado técnico de arrastre',
      'aplicación del activo de alisado o keratina',
      'secado, sellado y planchado según técnica',
      'mantenimiento recomendado para prolongar el resultado'
    ],
    followup: '¿Lo necesita para uso personal o para trabajar? ¿Busca bajar frizz, alisar, reconstruir o dejar mantenimiento post alisado?'
  },
  {
    id: 'barberia_styling',
    label: 'barbería y terminación masculina',
    triggerFamilies: ['cera', 'gel', 'aceite', 'shampoo'],
    triggerKeywords: ['barberia', 'barbería', 'barba', 'corte masculino', 'peinado masculino', 'styling', 'fijacion', 'fijación'],
    primaryFamilies: ['cera', 'gel', 'aceite', 'shampoo'],
    complementFamiliesPersonal: ['aceite', 'shampoo'],
    complementFamiliesProfessional: ['cera', 'gel', 'aceite', 'shampoo'],
    stepsPersonal: [
      'lavado o preparación',
      'corte o peinado',
      'terminación con cera o gel',
      'cuidado de barba o finalización con aceite'
    ],
    stepsProfessional: [
      'preparación del cabello o barba',
      'corte, barba o styling',
      'terminación con cera o gel según fijación',
      'cuidado final con aceite o producto de mantenimiento'
    ],
    followup: '¿Es para uso personal o para barbería? ¿Busca fijación, brillo, textura o terminación para barba?'
  },
];

function getHairTreatmentKnowledgeById(id) {
  const key = normalizeCatalogSearchText(id || '');
  if (!key) return null;
  return HAIR_TREATMENT_KNOWLEDGE.find((item) => normalizeCatalogSearchText(item.id) === key) || null;
}

function getFamilyAliasesSafe(family) {
  const aliases = getProductFamilyAliases(family);
  if (aliases.length) return aliases;
  const label = getProductFamilyLabel(family);
  return label ? [label] : [];
}

function rowMatchesHairFamily(row, family) {
  const aliases = getFamilyAliasesSafe(family);
  if (!aliases.length) return false;
  const hay = buildStockHaystack(row);
  return aliases.some((alias) => containsCatalogPhrase(hay, alias));
}

function detectHairTreatmentKnowledge({ text = '', family = '', need = '' } = {}) {
  const bag = normalizeCatalogSearchText([text, family, need].filter(Boolean).join(' | '));
  if (!bag) return null;

  let best = null;
  for (const item of HAIR_TREATMENT_KNOWLEDGE) {
    let score = 0;
    const familyKey = normalizeCatalogSearchText(family);
    if (familyKey && item.triggerFamilies.some((x) => normalizeCatalogSearchText(x) === familyKey)) {
      score += 7;
    }
    for (const triggerFamily of item.triggerFamilies) {
      const aliases = getFamilyAliasesSafe(triggerFamily);
      for (const alias of aliases) {
        if (containsCatalogPhrase(bag, alias)) score += normalizeCatalogSearchText(alias).split(' ').length >= 2 ? 3 : 2;
      }
    }
    for (const kw of (item.triggerKeywords || [])) {
      if (containsCatalogPhrase(bag, kw)) score += normalizeCatalogSearchText(kw).split(' ').length >= 2 ? 2 : 1;
    }
    if (!score) continue;
    if (!best || score > best.score) best = { item, score };
  }

  return best?.item || null;
}

function getTreatmentComplementFamilies(knowledge, useType = '') {
  const info = knowledge || null;
  if (!info) return [];
  return normalizeUseType(useType) === 'profesional'
    ? (info.complementFamiliesProfessional || [])
    : (info.complementFamiliesPersonal || []);
}

function buildTreatmentStepsSummary(knowledge, useType = '') {
  const info = knowledge || null;
  if (!info) return '';
  const steps = normalizeUseType(useType) === 'profesional'
    ? (info.stepsProfessional || info.stepsPersonal || [])
    : (info.stepsPersonal || info.stepsProfessional || []);
  const short = steps.slice(0, 4).map((step, idx) => `${idx + 1}) ${step}`);
  return short.length ? `Normalmente este trabajo se completa así: ${short.join(' · ')}` : '';
}

function buildTreatmentQuestion(knowledge, useType = '') {
  const info = knowledge || null;
  if (!info) return '';
  const use = normalizeUseType(useType);
  if (use === 'profesional') {
    return `¿Lo necesita para trabajar en el salón? Así le recomiendo una combinación más completa y rendidora.`;
  }
  return info.followup || '';
}

function buildTreatmentRecommendationPool(stockRows, { knowledge = null, family = '', query = '', useType = '', need = '', hairType = '' } = {}) {
  const info = knowledge || null;
  const rows = Array.isArray(stockRows) ? stockRows.filter((row) => detectRowProductDomain(row) === 'hair') : [];
  if (!info || !rows.length) return [];

  const primaryFamilies = Array.from(new Set([family, ...(info.primaryFamilies || [])].filter(Boolean)));
  const complementFamilies = Array.from(new Set(getTreatmentComplementFamilies(info, useType).filter(Boolean)));
  const relevantFamilies = Array.from(new Set([...primaryFamilies, ...complementFamilies]));

  const scored = rows.map((row) => {
    let score = scoreProductCandidate(row, {
      query,
      family,
      domain: 'hair',
      hairType,
      need,
      useType,
    });

    if (primaryFamilies.some((fam) => rowMatchesHairFamily(row, fam))) score += 3.4;
    if (complementFamilies.some((fam) => rowMatchesHairFamily(row, fam))) score += 1.6;

    const bag = buildStockHaystack(row);
    if (normalizeUseType(useType) === 'profesional' && /(profesional|salon|salón|barber)/i.test(bag)) score += 1.1;
    if (normalizeUseType(useType) === 'personal' && /(personal|hogar|casa)/i.test(bag)) score += 0.6;

    return { row, score };
  }).filter((item) => item.score > 0.2);

  scored.sort((a, b) => b.score - a.score);

  const out = [];
  const seen = new Set();
  for (const item of scored) {
    const key = normalizeCatalogSearchText(`${item.row?.nombre || ''} ${item.row?.marca || ''}`);
    if (!key || seen.has(key)) continue;

    if (relevantFamilies.length) {
      const matchesRelevant = relevantFamilies.some((fam) => rowMatchesHairFamily(item.row, fam));
      if (!matchesRelevant && item.score < 1.4) continue;
    }

    seen.add(key);
    out.push(item.row);
    if (out.length >= 20) break;
  }

  return out;
}

const PRODUCT_TYPE_KEYWORDS = PRODUCT_FAMILY_DEFS.flatMap((x) => x.aliases).concat(FURNITURE_FAMILY_DEFS.flatMap((x) => x.aliases));
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

function getFurnitureFamilyDef(family) {
  const f = normalizeCatalogSearchText(family);
  if (!f) return null;
  return FURNITURE_FAMILY_DEFS.find((item) =>
    normalizeCatalogSearchText(item.id) === f ||
    normalizeCatalogSearchText(item.label) === f ||
    item.aliases.some((alias) => normalizeCatalogSearchText(alias) === f)
  ) || null;
}

function getFurnitureFamilyAliases(family) {
  const def = getFurnitureFamilyDef(family);
  return def?.aliases || [];
}

function detectFurnitureFamily(query) {
  const q = normalizeCatalogSearchText(query);
  if (!q) return '';

  let best = null;
  for (const def of FURNITURE_FAMILY_DEFS) {
    let score = 0;
    for (const alias of def.aliases) {
      if (containsCatalogPhrase(q, alias)) score += normalizeCatalogSearchText(alias).split(' ').length >= 2 ? 4 : 2;
    }
    if (!score) continue;
    if (!best || score > best.score) best = { id: def.id, score };
  }
  return best?.id || '';
}

function detectProductDomain(query, family = '') {
  const q = normalizeCatalogSearchText(`${query || ''} ${family || ''}`);
  if (!q) return '';
  if (detectFurnitureFamily(q)) return 'furniture';
  if (detectProductFamily(q)) return 'hair';
  if (/(mueble|muebles|camilla|camillas|sillon|sillón|sillones|espejo|espejos|mesa|mesas|puff|respaldo|tocador|mostrador|recepcion|recepción|sala de espera|salon|salón|negocio|local)/i.test(q)) return 'furniture';
  if (/(shampoo|champu|champú|acondicionador|mascara|mascarilla|serum|sérum|aceite|oleo|óleo|tintura|oxidante|decolorante|matizador|ampolla|keratina|alisado|botox|protector|spray|gel|cera)/i.test(q)) return 'hair';
  return '';
}

function detectRowProductDomain(row) {
  const bag = normalizeCatalogSearchText(`${row?.tab || ''} ${row?.nombre || ''} ${row?.categoria || ''} ${row?.descripcion || ''}`);
  if (/(muebles|equipamiento|camilla|sillon|sillón|espejo|mesa|puff|respaldo|tocador|mostrador|recepcion|recepción)/i.test(bag)) return 'furniture';
  return 'hair';
}

function isGenericProductQuery(query) {
  const q = normalizeCatalogSearchText(query || '');
  if (!q) return false;

  const activeDomain = detectProductDomain(q);
  const family = activeDomain === 'furniture'
    ? detectFurnitureFamily(q)
    : detectProductFamily(q);

  const tokens = tokenize(q, { expandSynonyms: false }).filter(Boolean);
  const asksForList = /(\b(lista|opciones|catalogo|catálogo|stock|productos|todo|todas|todos|tenes|tenés|hay|disponible|disponibles|mostrame|mostrarme|mandame|pasame)\b)/i.test(q);

  if (asksForList) return true;
  if (!family) return false;
  return tokens.length <= 4;
}

function buildProductFollowupQuestion({ domain = '', familyLabel = '', useType = '' } = {}) {
  const readableFamily = familyLabel ? (domain === 'furniture' ? (getFurnitureFamilyDef(familyLabel)?.label || familyLabel) : getProductFamilyLabel(familyLabel)) : '';
  if (domain === 'furniture') {
    return `✨ Si quiere, le ayudo a elegir. ¿Es para uso personal o para salón?`;
  }

  const treatmentKnowledge = detectHairTreatmentKnowledge({ family: familyLabel, text: readableFamily });
  const treatmentQuestion = buildTreatmentQuestion(treatmentKnowledge, useType);
  if (treatmentQuestion) {
    return `✨ Si quiere, le ayudo a elegir. ${treatmentQuestion}`;
  }

  return `✨ Si quiere, le ayudo a elegir. ¿Es para uso personal o para trabajar?`;
}

function looksLikeFurniturePreferenceReply(text) {
  const t = normalize(text || '');
  if (!t) return false;
  return /(uso personal|para mi casa|para mi hogar|para casa|para un salon|para un salón|para negocio|para trabajar|para local|para peluqueria|para peluquería|barberia|barbería|moderno|clasico|clásico|elegante|infantil|barbie|chesterfield|ambar|ámbar|petalo|pétalo|star gema|recepcion|recepción|espera|liviano|grande|chico|chicos|uno|dos|tres|cuatro|puestos|puesto|espacio|medida|medidas|ancho|alto|comodidad|funcional|diseño|diseno|llamativo)/i.test(t);
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

function isAccessoryOnlyMatch(row, family = '') {
  const f = normalizeCatalogSearchText(family || '');
  const hay = buildStockHaystack(row);
  if (!f || !hay) return false;
  if (f === 'plancha') return /(cepillo para plancha|peine para plancha|para plancha)/i.test(hay);
  if (f === 'secador') return /(difusor|boquilla|pico|para secador)/i.test(hay);
  return false;
}

function filterRowsForRequestedFamily(rows, { family = '', domain = '', query = '' } = {}) {
  let out = Array.isArray(rows) ? rows.slice() : [];
  if (!out.length) return out;
  const activeDomain = domain || detectProductDomain(query, family);
  if (activeDomain) out = out.filter((row) => detectRowProductDomain(row) === activeDomain);
  const q = normalizeCatalogSearchText(query || '');
  const wantsAmpolla = /ampolla/.test(q);
  if (wantsAmpolla) {
    const onlyAmpolla = out.filter((row) => /ampolla/i.test(buildStockHaystack(row)));
    if (onlyAmpolla.length) out = onlyAmpolla;
  }
  if (family) {
    const aliases = activeDomain === 'furniture' ? getFurnitureFamilyAliases(family) : getProductFamilyAliases(family);
    if (aliases.length) {
      const strict = out.filter((row) => aliases.some((alias) => containsCatalogPhrase(buildStockHaystack(row), alias)) && !isAccessoryOnlyMatch(row, family));
      if (strict.length) out = strict;
    }
  }
  return out;
}

function wantsStrictNoApproximation(query = '', family = '', domain = '') {
  const q = normalizeCatalogSearchText(query || '');
  if (!q) return false;
  if (/foto|fotos|imagen|imágenes|imagenes/.test(q)) return true;
  if (/ampolla|ampollas|secador|secadores|plancha|planchas|camilla|camillas|sillon|sillón|sillones/.test(q)) return true;
  if ((domain === 'furniture' || family === 'camilla' || family === 'sillon') && !/mueble|muebles|equipamiento/.test(q)) return true;
  return false;
}

function buildNoExactCatalogMessage({ domain = '', family = '', query = '', wantsPhoto = false } = {}) {
  const familyLabel = family
    ? (domain === 'furniture' ? (getFurnitureFamilyDef(family)?.label || family) : getProductFamilyLabel(family))
    : '';
  if (wantsPhoto) {
    return familyLabel
      ? `No tengo una foto vinculada para ${familyLabel} en este momento.`
      : 'No tengo una foto vinculada para ese producto o mueble en este momento.';
  }
  if (family === 'plancha') return 'Por el momento no tenemos una plancha para el pelo cargada en catálogo. Si quiere, cuando la incorporemos le avisamos.';
  if (family === 'secador') return 'Por el momento no tenemos un secador para el pelo cargado en catálogo.';
  if (/ampolla/.test(normalizeCatalogSearchText(query || ''))) return 'Por el momento no tenemos ampollas cargadas en catálogo. Si quiere, le recomiendo tratamientos o baños de crema según lo que necesite.';
  if (family === 'sillon') return 'Por el momento no tenemos sillones de barbería cargados en catálogo.';
  if (family === 'camilla') return 'Por el momento no tenemos camillas para spa cargadas en catálogo.';
  return domain === 'furniture'
    ? 'Por el momento no encuentro ese mueble o equipamiento en catálogo.'
    : 'Por el momento no encuentro ese producto en catálogo.';
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
  const furnitureFamily = detectFurnitureFamily(query);
  if (furnitureFamily) {
    const aliases = getFurnitureFamilyAliases(furnitureFamily);
    return aliases.length ? aliases : [furnitureFamily];
  }
  const family = detectProductFamily(query);
  if (!family) return [];
  const aliases = getProductFamilyAliases(family);
  return aliases.length ? aliases : [family];
}

function isFurnitureOnlyQuery(query) {
  return detectProductDomain(query) === 'furniture';
}

function applyProductTypeGuard(rows, query) {
  const activeDomain = detectProductDomain(query);
  const family = activeDomain === 'furniture' ? detectFurnitureFamily(query) : detectProductFamily(query);
  const aliases = activeDomain === 'furniture' ? getFurnitureFamilyAliases(family) : getProductFamilyAliases(family);
  if (!aliases.length) return rows;

  const guarded = rows.filter((r) => {
    const haystack = `${r.nombre} ${r.categoria} ${r.marca} ${r.descripcion} ${r.tab}`;
    if (activeDomain && detectRowProductDomain(r) !== activeDomain) return false;
    return aliases.some((alias) => containsCatalogPhrase(haystack, alias));
  });

  return guarded.length ? guarded : rows.filter((r) => !activeDomain || detectRowProductDomain(r) === activeDomain);
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

function findStockRelated(rows, rawQuery, { family = '', focusTerm = '', limit = 200, domain = '' } = {}) {
  const q = normalizeCatalogSearchText(rawQuery || '');
  const resolvedDomain = domain || detectProductDomain(q, family);
  const resolvedFamily = family || (resolvedDomain === 'furniture' ? detectFurnitureFamily(q) : detectProductFamily(q));
  const aliases = resolvedDomain === 'furniture' ? getFurnitureFamilyAliases(resolvedFamily) : getProductFamilyAliases(resolvedFamily);
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
    if (resolvedDomain && detectRowProductDomain(row) !== resolvedDomain) continue;
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

  if (focusTerm) {
    out = filterRowsByProductFocus(out, focusTerm);
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
Hay dos dominios principales:
- hair: insumos/productos para cabello o barbería
- furniture: muebles, sillones, camillas, espejos, mesas, puff, recepciones y equipamiento

Devolvé SOLO JSON con estas claves:
- is_product_query: boolean
- domain: string (hair, furniture o vacío)
- family: string (familia puntual o vacío)
- search_text: string
- specific_name: string
- wants_all_related: boolean
- wants_photo: boolean
- wants_price: boolean
- wants_recommendation: boolean
- hair_type: string
- need: string
- use_type: string (personal, profesional o vacío)
- business_type: string
- style: string
- seats_needed: string
- treatment_context: string
- work_type: string

Reglas:
- Priorizá interpretar el mensaje junto con historial_reciente si existe.
- Si el cliente primero pidió opciones y después agrega variables como uso personal, cabello dañado, lo mejor, económico, para trabajar, rubio, reparación o nutrición, seguí ese mismo hilo y marcá wants_recommendation=true.
- Si en el historial ya quedó claro que están hablando de productos, no lo saques de PRODUCT salvo que el cambio a servicio o turno sea clarísimo.
- Si pregunta por muebles o equipamiento, domain=furniture.
- Si pregunta por productos para el cabello, domain=hair.
- Si pregunta genéricamente por una familia, wants_all_related=true.
- Si pide stock, precios, lista, catálogo, opciones o qué hay de una familia, is_product_query=true.
- Si pide foto o precio de un producto puntual, specific_name debe contener ese producto.
- Si trae detalles para elegir, wants_recommendation=true.
- En hair, los detalles suelen ser tipo de cabello o resultado buscado.
- En hair, si consulta por una etapa técnica (por ejemplo baño de crema, nutrición, matizador, tintura, keratina, barbería, cera, gel), tratá de detectar el contexto del trabajo en treatment_context.
- En furniture, los detalles suelen ser si es para uso personal o negocio, estilo, cantidad de puestos, espacio o prioridad de diseño/funcionalidad.
- Si existe familia_actual y el cliente responde solo con datos de preferencia, seguí la continuidad y marcá is_product_query=true.
- Si parece servicio/turno y no producto, is_product_query=false.
- Consultas por ampollas, planchas, secadores, camillas, sillones, espejos o muebles siempre cuentan como PRODUCT.
- “Cuánto dura/demora” + nombre de tratamiento (keratina, shock de keratina, botox, shock de botox, nutrición) debe ir a SERVICE, no a PRODUCT.
- No inventes nombres de productos.`,
        },
        {
          role: 'user',
          content: JSON.stringify({
            mensaje: raw,
            ultimo_producto: context.lastProductName || '',
            ultimo_servicio: context.lastServiceName || '',
            familia_actual: context.lastFamily || '',
            dominio_actual: context.lastDomain || '',
            cabello_actual: context.lastHairType || '',
            necesidad_actual: context.lastNeed || '',
            uso_actual: context.lastUseType || '',
            historial_reciente: context.historySnippet || '',
          }),
        },
      ],
      response_format: { type: 'json_object' },
    });

    const obj = JSON.parse(completion.choices?.[0]?.message?.content || '{}');
    return {
      is_product_query: !!obj.is_product_query,
      domain: String(obj.domain || '').trim(),
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
      business_type: String(obj.business_type || '').trim(),
      style: String(obj.style || '').trim(),
      seats_needed: String(obj.seats_needed || '').trim(),
      treatment_context: String(obj.treatment_context || '').trim(),
      work_type: String(obj.work_type || '').trim(),
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
  if (/(profes|trabaj|peluquer|salon|salón|barber|negocio|local|para clientas|para clientes|para el salon|para el salón)/.test(t)) return 'profesional';
  if (/(personal|casa|hogar|para mi|para mí|uso propio)/.test(t)) return 'personal';
  return '';
}

function deriveProductTags(row) {
  const bag = normalizeCatalogSearchText(`${row?.nombre || ''} ${row?.categoria || ''} ${row?.marca || ''} ${row?.descripcion || ''} ${row?.tab || ''}`);
  const domain = detectRowProductDomain(row);
  const tags = [];

  if (domain === 'furniture') {
    const checks = [
      ['salón', /(salon|salón|peluquer|barber|negocio|local)/],
      ['personal', /(personal|hogar|casa)/],
      ['infantil', /(infantil|niña|niño|barbie)/],
      ['espera', /(espera|recepcion|recepción|living)/],
      ['moderno', /(moderno|minimalista|led|gema)/],
      ['clásico', /(chesterfield|clasico|clásico|vintage)/],
      ['combo', /(combo|mesa|puff|camastro)/],
    ];
    for (const [label, rx] of checks) {
      if (rx.test(bag)) tags.push(label);
    }
    return tags;
  }

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

function buildProductAICandidate(row, opts = {}) {
  const treatmentKnowledge = opts?.treatmentKnowledge || null;
  const productFamilies = PRODUCT_FAMILY_DEFS
    .filter((fam) => rowMatchesHairFamily(row, fam.id))
    .map((fam) => fam.label);

  let technicalRole = '';
  if (treatmentKnowledge) {
    if ((treatmentKnowledge.primaryFamilies || []).some((fam) => rowMatchesHairFamily(row, fam))) technicalRole = 'base';
    else if (getTreatmentComplementFamilies(treatmentKnowledge, opts?.useType || '').some((fam) => rowMatchesHairFamily(row, fam))) technicalRole = 'complemento';
  }

  return {
    nombre: row.nombre,
    categoria: row.categoria || '',
    marca: row.marca || '',
    precio: row.precio || '',
    descripcion: compactProductDescription(row.descripcion || ''),
    dominio: detectRowProductDomain(row),
    tags: deriveProductTags(row),
    familias: productFamilies,
    rol_tecnico: technicalRole,
  };
}

function scoreProductCandidate(row, { query = '', family = '', domain = '', hairType = '', need = '', useType = '', businessType = '', style = '', seatsNeeded = '' } = {}) {
  const hay = buildStockHaystack(row);
  const rowDomain = detectRowProductDomain(row);
  const activeDomain = domain || rowDomain;
  let score = 0;

  if (domain && rowDomain === domain) score += 1.4;

  if (query) {
    score += Math.max(
      scoreField(query, row.nombre) * 1.2,
      scoreField(query, row.categoria) * 0.92,
      scoreField(query, row.descripcion) * 0.78,
      scoreField(query, row.marca) * 0.55
    );
  }

  const aliases = activeDomain === 'furniture' ? getFurnitureFamilyAliases(family) : getProductFamilyAliases(family);
  for (const alias of aliases) {
    if (containsCatalogPhrase(hay, alias)) score += 3.2;
  }

  if (activeDomain === 'furniture') {
    const businessNorm = normalizeCatalogSearchText(businessType || '');
    const styleNorm = normalizeCatalogSearchText(style || '');
    const needNorm = normalizeCatalogSearchText(need || '');
    const useNorm = normalizeUseType(useType);
    for (const tok of tokenize(`${businessNorm} ${styleNorm} ${needNorm}`, { expandSynonyms: false })) {
      if (tok.length >= 3 && hay.includes(tok)) score += 1.2;
    }
    if (useNorm === 'profesional' && /(salon|salón|barber|negocio|local|recepcion|recepción|espera)/i.test(hay)) score += 1.15;
    if (useNorm === 'personal' && /(personal|hogar|casa|living)/i.test(hay)) score += 0.95;
    if (String(seatsNeeded || '').trim() && /(2|dos|3|tres|4|cuatro|cuerpos|puestos|puesto|puff|camastro)/i.test(hay)) score += 0.55;
    return score;
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

function isHairCareRecommendationCandidate(row, { family = '' } = {}) {
  const fam = normalizeCatalogSearchText(family || '');
  if (fam === 'plancha' || fam === 'secador') return true;
  const hay = buildStockHaystack(row);
  if (!hay) return false;
  if (/(pinza|pinzas|peine|peines|cepillo|cepillos|gorra|brocha|brochas|bowl|espatula|espátula|papel aluminio|papel|guante|guantes|difusor|boquilla|pico|repuesto|accesorio|accesorios)/i.test(hay)) {
    return false;
  }
  return true;
}

function shortlistProductsForRecommendation(rows, criteria = {}) {
  const items = Array.isArray(rows) ? rows.filter((r) => r?.nombre) : [];
  if (!items.length) return [];

  const needsHairCareFilter = (
    criteria?.domain === 'hair'
    && (!!criteria?.need || !!criteria?.hairType)
    && !['plancha', 'secador'].includes(normalizeCatalogSearchText(criteria?.family || ''))
  );

  const filteredItems = needsHairCareFilter
    ? items.filter((row) => isHairCareRecommendationCandidate(row, { family: criteria?.family || '' }))
    : items;

  const scopedBase = filteredItems.length ? filteredItems : items;
  const scopedItems = criteria?.focusTerm ? filterRowsByProductFocus(scopedBase, criteria.focusTerm) : scopedBase;

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

async function recommendProductsWithAI({ text, domain = '', familyLabel = '', hairType = '', need = '', useType = '', businessType = '', style = '', seatsNeeded = '', treatmentKnowledge = null, historySnippet = '', products = [] } = {}) {
  const candidates = Array.isArray(products)
    ? products.filter((p) => p?.nombre).slice(0, 10).map((row) => buildProductAICandidate(row, { treatmentKnowledge, useType }))
    : [];
  if (!candidates.length) return null;

  const treatmentSummary = treatmentKnowledge ? buildTreatmentStepsSummary(treatmentKnowledge, useType) : '';

  try {
    const completion = await openai.chat.completions.create({
      model: PRIMARY_MODEL,
      messages: [
        {
          role: 'system',
          content:
`Sos un asistente comercial de salón que recomienda SOLO entre las opciones enviadas.
Hay dos dominios:
- hair: productos/insumos para cabello o barbería
- furniture: muebles y equipamiento

Tu trabajo:
- Elegir hasta 4 opciones que mejor encajen.
- No inventar productos ni cambiar nombres.
- Respondé breve, cálido y vendedor.
- No des explicaciones largas ni técnicas.
- Si domain=furniture, priorizá uso personal o negocio, estilo y funcionalidad.
- Si domain=hair, pensá como una profesional, pero explicalo en pocas palabras.
- Si uso=profesional, hablá en lógica de trabajo y terminación.
- Si uso=personal, hablá en lógica de cuidado y mantenimiento.
- Si hay "resumen_tratamiento", usalo solo para orientar la selección.
- Priorizá el contexto completo de historial_reciente y no solo el último mensaje.
- Si el historial ya marca una necesidad concreta, no la contradigas después.
- Cuando el cliente pide "la mejor" opción, elegí primero la más alineada al problema, no la más genérica.
- No agregues productos fuera de las opciones enviadas.
- Usá un tono lindo y simple, con como mucho un emoji suave como ✨.

Respondé SOLO JSON con:
- intro: string (máximo 1 oración corta)
- recommended_names: string[] (hasta 4 nombres exactos)
- follow_up: string (máximo 1 oración corta)
- rationale: string (muy breve, opcional, máximo 10 palabras)
- step_summary: string (muy breve, opcional, máximo 10 palabras)
- sales_angle: string (muy breve, opcional, máximo 10 palabras)`,
        },
        {
          role: 'user',
          content: JSON.stringify({
            mensaje_cliente: text || '',
            dominio: domain || '',
            familia: familyLabel || '',
            tipo_cabello: hairType || '',
            necesidad: need || '',
            uso: useType || '',
            tipo_negocio: businessType || '',
            estilo: style || '',
            puestos: seatsNeeded || '',
            historial_reciente: historySnippet || '',
            tratamiento: treatmentKnowledge ? {
              id: treatmentKnowledge.id,
              label: treatmentKnowledge.label,
              resumen_tratamiento: treatmentSummary,
              familias_base: treatmentKnowledge.primaryFamilies || [],
              familias_complementarias: getTreatmentComplementFamilies(treatmentKnowledge, useType),
            } : null,
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
      step_summary: String(obj.step_summary || '').trim(),
      sales_angle: String(obj.sales_angle || '').trim(),
    };
  } catch {
    return null;
  }
}

function formatRecommendedProductsReply(aiPayload, rows, { domain = '', familyLabel = '', hairType = '', need = '', useType = '', businessType = '', style = '', seatsNeeded = '', treatmentKnowledge = null } = {}) {
  const items = Array.isArray(rows) ? rows.filter((r) => r?.nombre).slice(0, 4) : [];
  if (!items.length) return null;

  const readableFamily = familyLabel
    ? (domain === 'furniture' ? (getFurnitureFamilyDef(familyLabel)?.label || familyLabel) : getProductFamilyLabel(familyLabel))
    : '';
  const intro = aiPayload?.intro
    ? aiPayload.intro
    : readableFamily
      ? `✨ Estas opciones de *${readableFamily}* le pueden servir:`
      : `✨ Estas opciones le pueden servir:`;

  const supportLine = [
    aiPayload?.sales_angle,
    aiPayload?.rationale,
    domain === 'hair' ? aiPayload?.step_summary : '',
  ].map((x) => String(x || '').trim()).find(Boolean) || '';

  const bodyParts = [intro];
  if (supportLine) bodyParts.push(supportLine);

  const lines = items.map((p) => {
    const precio = moneyOrConsult(p.precio);
    const desc = compactProductDescription(p.descripcion || '', 95);
    return `${getCatalogItemEmoji(p.nombre, { kind: 'product' })} *${p.nombre}*\n• Precio: *${precio}*${desc ? `\n• ${desc}` : ''}`;
  });

  const followUp = aiPayload?.follow_up || buildProductFollowupQuestion({ domain, familyLabel, useType });

  return `${bodyParts.filter(Boolean).join('\n')}\n\n${lines.join('\n\n')}\n\n${followUp}`.trim();
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

function sanitizeCourseSearchQuery(query) {
  let q = normalize(String(query || '').trim());
  if (!q) return '';

  q = q
    .replace(/[!?¡¿.,;:()]/g, ' ')
    .replace(/\b(hola|buenas|buenos dias|buen dia|buen día|buenas tardes|buenas noches)\b/g, ' ')
    .replace(/\b(quiero info|quisiera info|quiero saber|quisiera saber|queria saber|quería saber|me pasas info|me pasás info|mandame info|mandáme info|pasame info|pasáme info|consulto por|consulta por|consulta sobre|informacion|información)\b/g, ' ')
    .replace(/\b(curso|cursos|clase|clases|capacitacion|capacitaciones|capacitación|taller|talleres|masterclass|seminario|seminarios|workshop|formacion|formación|certificacion|certificación)\b/g, ' ')
    .replace(/\b(algun|alguno|alguna|de ese|ese curso|de ese curso|mas info|más info|precio|cuanto sale|cuánto sale|cuanto cuesta|cuánto cuesta|cuando empieza|cuándo empieza|cuando arranca|cuándo arranca|inicio|duracion|duración|horario|horarios|dias|días|cupo|cupos|inscripcion|inscripción|requisitos|modalidad|presencial|online|virtual|hay|tenes|tenés|tienen|ofrecen|dictan|dan|brindan|disponibles|abiertos|abiertas|busco|ando buscando)\b/g, ' ')
    .replace(/\b(de|del|de la|de las|de los|para|sobre)\b/g, ' ')
    .replace(/\b(y|e|o|u|ni)\b/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

  if (q === 'barber') q = 'barberia';
  return q;
}

function humanizeCourseSearchQuery(query) {
  const q = sanitizeCourseSearchQuery(query);
  if (!q) return '';
  return sentenceCase(
    q
      .replace(/\bbarberia\b/g, 'barbería')
      .replace(/\bcolorimetria\b/g, 'colorimetría')
      .replace(/\bestetica\b/g, 'estética')
      .replace(/\bpeluqueria\b/g, 'peluquería')
      .replace(/\bninos\b/g, 'niños')
  );
}


const COURSE_BROAD_CATEGORY_TOKENS = new Set([
  'barber', 'barberia', 'maquillaje', 'colorimetria', 'peluqueria', 'estetica',
  'auxiliar', 'peinados', 'recogidos', 'ninos', 'niñas', 'ninas'
]);

const COURSE_STRICT_MODIFIER_TOKENS = new Set([
  'avanzada', 'avanzado', 'basica', 'basico', 'inicial', 'intermedio', 'intermedia',
  'profesional', 'perfeccionamiento', 'intensivo', 'intensiva', 'experto', 'experta',
  'especializacion', 'especialización'
]);

function buildHubSpotCourseInterestLabel(text) {
  const raw = String(text || '').trim();
  if (!raw) return '';
  const hasCourseSignal =
    COURSE_SIGNAL_RE.test(raw)
    || /(dictan clases|dan clases|masterclass|seminario|workshop)/i.test(raw)
    || /^curso\s+/i.test(raw);

  const topic = sanitizeCourseSearchQuery(raw);
  if (!topic) return hasCourseSignal ? 'CURSO' : '';

  return `CURSO ${normalize(topic).toUpperCase().replace(/\s+/g, ' ').trim()}`.trim();
}

function getStrictCourseTokens(queryRaw) {
  const cleaned = sanitizeCourseSearchQuery(queryRaw);
  if (!cleaned) return [];

  const tokens = Array.from(new Set(
    normalize(cleaned)
      .split(' ')
      .map((x) => x.trim())
      .filter(Boolean)
  ));

  if (!tokens.length) return [];

  const strictModifiers = tokens.filter((tok) => COURSE_STRICT_MODIFIER_TOKENS.has(tok));
  if (strictModifiers.length) return strictModifiers;

  const specific = tokens.filter((tok) => tok.length >= 4 && !COURSE_BROAD_CATEGORY_TOKENS.has(tok));
  if (specific.length >= 2) return specific;

  return [];
}

function courseCandidateSatisfiesStrictQuery(row, queryRaw) {
  const required = getStrictCourseTokens(queryRaw);
  if (!required.length) return true;

  const hay = normalize([
    row?.nombre,
    row?.categoria,
    row?.modalidad,
    row?.duracionTotal,
    row?.fechaInicio,
    row?.fechaFin,
    row?.diasHorarios,
    row?.info,
    row?.estado,
    row?.requisitos,
  ].filter(Boolean).join(' | '));

  return required.every((tok) => hay.includes(tok));
}

function scoreCourseCandidate(row, queryRaw) {
  const query = sanitizeCourseSearchQuery(queryRaw) || normalize(queryRaw || '');
  if (!query) return 0;

  const hay = [
    row?.nombre,
    row?.categoria,
    row?.modalidad,
    row?.duracionTotal,
    row?.fechaInicio,
    row?.fechaFin,
    row?.diasHorarios,
    row?.info,
    row?.estado,
    row?.requisitos,
  ].filter(Boolean).join(' | ');

  const hayNorm = normalize(hay);
  let score = Math.max(
    scoreField(query, row?.nombre || '') * 1.25,
    scoreField(query, row?.categoria || '') * 1.05,
    scoreField(query, row?.info || '') * 0.78,
    scoreField(query, row?.requisitos || '') * 0.52,
    scoreField(query, row?.modalidad || '') * 0.36,
  );

  if (containsCatalogPhrase(hayNorm, query)) score += 0.6;

  const qTokens = tokenize(query, { expandSynonyms: true });
  const hayTokens = tokenize(hayNorm, { expandSynonyms: false });
  if (qTokens.length && hayTokens.length) {
    score += jaccard(qTokens, hayTokens) * 0.85;
    if (qTokens.some((tok) => tok.length >= 4 && hayNorm.includes(tok))) score += 0.25;
  }

  return score;
}

function findCourses(rows, query, mode) {
  const rawQuery = String(query || '').trim();
  const cleanQuery = sanitizeCourseSearchQuery(rawQuery);
  const q = normalize(cleanQuery || rawQuery);
  if (!q) return [];

  const scopedRows = (Array.isArray(rows) ? rows : []).filter((row) => courseCandidateSatisfiesStrictQuery(row, rawQuery));
  if (!scopedRows.length) return [];

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

  if (mode === "LIST") {
    const listed = scopedRows.filter(match);
    if (listed.length) return listed;
  }

  const exact = scopedRows.filter((r) => normalize(r.nombre) === q || normalize(r.categoria) === q);
  if (exact.length) return exact;

  const contains = scopedRows.filter((r) => normalize(r.nombre).includes(q) || normalize(r.categoria).includes(q));
  if (contains.length) return contains;

  const scored = scopedRows
    .map((row) => ({ row, score: scoreCourseCandidate(row, cleanQuery || rawQuery) }))
    .filter((item) => item.score >= 0.72)
    .sort((a, b) => b.score - a.score)
    .map((item) => item.row);
  if (scored.length) return scored;

  return scopedRows.filter(match);
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

function detectCourseFollowupGoal(text) {
  const t = normalize(text || '');
  if (!t) return '';

  if (/^(ese|ese curso|de ese|de ese curso|mas info|más info|info)$/.test(t)) return 'DETAIL';
  if (/(precio|precios|valor|valores|costo|costos|cuanto sale|cuánto sale|cuanto cuesta|cuánto cuesta)/i.test(t)) return 'PRICE';
  if (/(cuando empieza|cuándo empieza|cuando arranca|cuándo arranca|inicio|fecha de inicio|cuando es|cuándo es)/i.test(t)) return 'START';
  if (/(requisitos|requisito)/i.test(t)) return 'REQUIREMENTS';
  if (/(modalidad|presencial|online|virtual)/i.test(t)) return 'MODALITY';
  if (/(duracion|duración|cuanto dura|cuánto dura)/i.test(t)) return 'DURATION';
  if (/(horario|horarios|dias|días)/i.test(t)) return 'SCHEDULE';
  if (/(cupo|cupos)/i.test(t)) return 'CUPS';
  if (/(seña|sena|inscripcion|inscripción|reservar lugar|reserva de lugar)/i.test(t)) return 'SIGNUP';
  return '';
}

function findCourseByContextName(rows, courseName) {
  const wanted = normalize(String(courseName || '').trim());
  if (!wanted) return null;
  const list = Array.isArray(rows) ? rows : [];
  return list.find((row) => normalize(row?.nombre || '') === wanted)
    || list.find((row) => normalize(row?.nombre || '').includes(wanted))
    || list.find((row) => wanted.includes(normalize(row?.nombre || '')))
    || null;
}


const COURSE_REFERENCE_STOPWORDS = new Set([
  'curso', 'cursos', 'clase', 'clases', 'capacitacion', 'capacitaciones', 'taller', 'talleres',
  'masterclass', 'seminario', 'seminarios', 'workshop', 'de', 'del', 'la', 'las', 'el', 'los',
  'ese', 'esa', 'este', 'esta', 'cuanto', 'sale', 'cuesta', 'precio', 'precios', 'valor', 'valores',
  'cuando', 'empieza', 'arranca', 'inicio', 'requisitos', 'modalidad', 'duracion', 'horario', 'horarios',
  'dias', 'cupos', 'cupo', 'inscripcion', 'info', 'mas', 'quiero', 'saber', 'sobre', 'para', 'y'
]);

function courseReferenceHaystack(row) {
  return normalize([
    row?.nombre,
    row?.categoria,
    row?.modalidad,
    row?.duracionTotal,
    row?.fechaInicio,
    row?.diasHorarios,
    row?.requisitos,
    row?.info,
    row?.estado,
  ].filter(Boolean).join(' | '));
}

function extractRawCourseReferenceTokens(text = '') {
  const raw = normalize(String(text || '').trim());
  if (!raw) return [];
  return Array.from(new Set(
    raw
      .replace(/[!?¡¿.,;:()]/g, ' ')
      .split(/\s+/)
      .map((token) => token.trim())
      .filter((token) => token && token.length >= 4 && !COURSE_REFERENCE_STOPWORDS.has(token))
  ));
}

function findCourseByReferenceHint(rows, text) {
  const list = Array.isArray(rows) ? rows : [];
  if (!list.length) return null;

  const rawNorm = normalize(String(text || '').trim());
  if (!rawNorm) return null;

  const tokens = extractRawCourseReferenceTokens(text);
  if (!tokens.length) return null;

  const scored = list
    .map((row) => {
      const hay = courseReferenceHaystack(row);
      if (!hay) return null;

      let score = 0;
      const name = normalize(row?.nombre || '');
      const category = normalize(row?.categoria || '');

      if (name && rawNorm.includes(name)) score += 6;
      if (category && rawNorm.includes(category)) score += 4;

      for (const token of tokens) {
        if (name.includes(token)) score += 2.1;
        else if (category.includes(token)) score += 1.6;
        else if (hay.includes(token)) score += 0.7;
      }

      return score >= 2.2 ? { row, score } : null;
    })
    .filter(Boolean)
    .sort((a, b) => b.score - a.score);

  if (!scored.length) return null;
  if (scored.length > 1 && (scored[0].score - scored[1].score) < 0.85) return null;
  return scored[0].row || null;
}

function resolveCourseFromConversationContext(rows, text, lastCourseContext = null) {
  const catalogRows = Array.isArray(rows) ? rows : [];
  const recentRows = Array.isArray(lastCourseContext?.recentCourses) ? lastCourseContext.recentCourses : [];
  const currentCourseName = lastCourseContext?.currentCourseName || lastCourseContext?.selectedName || '';
  const currentCourse = findCourseByContextName(catalogRows, currentCourseName) || findCourseByContextName(recentRows, currentCourseName);

  const cleanedQuery = sanitizeCourseSearchQuery(text);
  const followupGoal = detectCourseFollowupGoal(text);

  if (!cleanedQuery && followupGoal) {
    if (currentCourse) return currentCourse;
    if (recentRows.length === 1) return recentRows[0];
  }

  if (cleanedQuery) {
    const recentMatches = findCourses(recentRows, cleanedQuery, 'DETAIL');
    if (recentMatches.length === 1) return recentMatches[0];

    const catalogMatches = findCourses(catalogRows, cleanedQuery, 'DETAIL');
    if (catalogMatches.length === 1) return catalogMatches[0];

    if (!getStrictCourseTokens(cleanedQuery).length) {
      const hintedRecent = findCourseByReferenceHint(recentRows, text);
      if (hintedRecent) return hintedRecent;
      const hintedCatalog = findCourseByReferenceHint(catalogRows, text);
      if (hintedCatalog) return hintedCatalog;
    }

    return null;
  }

  const hintedRecent = findCourseByReferenceHint(recentRows, text);
  if (hintedRecent) return hintedRecent;
  const hintedCatalog = findCourseByReferenceHint(catalogRows, text);
  if (hintedCatalog) return hintedCatalog;

  return currentCourse || null;
}

function resolveCourseByOrdinalChoice(rows, text = '') {
  const list = Array.isArray(rows) ? rows.filter((x) => x?.nombre) : [];
  if (!list.length) return null;
  const t = normalize(text || '');
  if (!t) return null;

  if (/(primer|primero|1ro|uno|el primero|la primera)/.test(t)) return list[0] || null;
  if (/(segundo|segunda|2do|dos|el segundo|la segunda)/.test(t)) return list[1] || null;
  if (/(tercer|tercero|tercera|3ro|tres|el tercero|la tercera)/.test(t)) return list[2] || null;
  if (/(ultimo|último|ultima|última|el ultimo|el último|la ultima|la última)/.test(t)) return list[list.length - 1] || null;
  return null;
}

function stripCourseSignupNoise(text = '') {
  return sanitizeCourseSearchQuery(
    String(text || '')
      .replace(/(quiero|me quiero|quisiera|para|anotarme|anotarme al|anotarme a|inscribirme|inscribirme al|inscribirme a|sumarme|reservar|reservar lugar|quiero reservar mi lugar|me quiero inscribir|para curso de|curso de)/gi, ' ')
      .replace(/\s+/g, ' ')
      .trim()
  );
}

async function resolveCourseEnrollmentSelectionWithAI(rows, text, context = {}) {
  const catalogRows = Array.isArray(rows) ? rows.filter((x) => x?.nombre) : [];
  const recentRows = Array.isArray(context?.recentCourses) ? context.recentCourses.filter((x) => x?.nombre) : [];
  const activeRows = recentRows.length ? recentRows : catalogRows;
  const raw = String(text || '').trim();
  if (!raw) return { course: null, course_query: '' };

  const ordinal = resolveCourseByOrdinalChoice(activeRows, raw);
  if (ordinal) return { course: ordinal, course_query: ordinal.nombre || '' };

  const cleanedHint = stripCourseSignupNoise(raw);
  if (cleanedHint) {
    const hintedRecent = findCourseByReferenceHint(activeRows, cleanedHint) || findCourseByReferenceHint(activeRows, raw);
    if (hintedRecent) return { course: hintedRecent, course_query: hintedRecent.nombre || cleanedHint };

    const hintedMatches = findCourses(activeRows, cleanedHint, 'DETAIL');
    if (hintedMatches.length === 1) return { course: hintedMatches[0], course_query: hintedMatches[0]?.nombre || cleanedHint };

    const catalogHintedMatches = findCourses(catalogRows, cleanedHint, 'DETAIL');
    if (catalogHintedMatches.length === 1) return { course: catalogHintedMatches[0], course_query: catalogHintedMatches[0]?.nombre || cleanedHint };
  }

  try {
    const completion = await openai.chat.completions.create({
      model: PRIMARY_MODEL,
      temperature: 0,
      messages: [
        {
          role: 'system',
          content:
`Elegí cuál es el curso correcto mencionado por la persona. Devolvé SOLO JSON válido con:
- selected_name: string
- course_query: string

Reglas:
- Tenés que resolver referencias naturales como “el segundo”, “el de celulares”, “el técnico”, “ese”, “ese de reparación”.
- selected_name debe ser exactamente uno de los nombres disponibles, o cadena vacía si no está claro.
- course_query puede contener una pista corta útil si no lográs elegir con total claridad.
- No inventes nombres.`
        },
        {
          role: 'user',
          content: JSON.stringify({
            mensaje: raw,
            curso_actual: context?.currentCourseName || '',
            opciones_recientes: activeRows.map((x) => x.nombre).slice(0, 12),
            cursos_disponibles: catalogRows.map((x) => x.nombre).slice(0, 20),
            historial: context?.historySnippet || '',
          })
        }
      ],
      response_format: { type: 'json_object' },
    });

    const obj = JSON.parse(completion.choices?.[0]?.message?.content || '{}');
    const selectedName = String(obj.selected_name || '').trim();
    const courseQuery = String(obj.course_query || '').trim();
    const selected = findCourseByContextName(activeRows, selectedName) || findCourseByContextName(catalogRows, selectedName);
    if (selected) return { course: selected, course_query: selected.nombre || courseQuery };
    return { course: null, course_query: courseQuery };
  } catch {
    return { course: null, course_query: cleanedHint || '' };
  }
}

function formatNaturalCourseFollowupReply(course, goal = 'DETAIL') {
  if (!course) return '';

  const nombre = course.nombre || 'el curso';
  const precio = course.precio ? moneyOrConsult(course.precio) : '';
  const inicio = course.fechaInicio || '';
  const modalidad = course.modalidad || '';
  const duracion = course.duracionTotal || '';
  const horarios = course.diasHorarios || '';
  const requisitos = course.requisitos || '';
  const cupos = course.cupos || '';
  const sena = course.sena || '';

  if (goal === 'PRICE') {
    const lines = [
      precio ? `El curso *${nombre}* sale *${precio}*.` : `No tengo el precio cargado de *${nombre}* en este momento.`,
      sena ? `La seña / inscripción es: ${sena}.` : '',
    ].filter(Boolean);
    return lines.join('\n');
  }

  if (goal === 'START') {
    return inicio
      ? `El curso *${nombre}* comienza *${inicio}*.`
      : `Todavía no tengo cargada la fecha de inicio de *${nombre}*.`;
  }

  if (goal === 'REQUIREMENTS') {
    return requisitos
      ? `Para *${nombre}*, los requisitos son: ${requisitos}.`
      : `Por el momento no tengo requisitos cargados para *${nombre}*.`;
  }

  if (goal === 'MODALITY') {
    return modalidad
      ? `*${nombre}* es modalidad *${modalidad}*.`
      : `Por el momento no tengo cargada la modalidad de *${nombre}*.`;
  }

  if (goal === 'DURATION') {
    return duracion
      ? `La duración de *${nombre}* es *${duracion}*.`
      : `Por el momento no tengo cargada la duración de *${nombre}*.`;
  }

  if (goal === 'SCHEDULE') {
    if (horarios) return `Los días y horarios de *${nombre}* son: ${horarios}.`;
    if (inicio) return `Por ahora tengo cargado que *${nombre}* comienza *${inicio}*.`;
    return `Por el momento no tengo cargados los días y horarios de *${nombre}*.`;
  }

  if (goal === 'CUPS') {
    return cupos
      ? `Los cupos de *${nombre}* son: ${cupos}.`
      : `Por el momento no tengo un número de cupos cargado para *${nombre}*.`;
  }

  if (goal === 'SIGNUP') {
    const lines = [
      sena ? `Para reservar lugar en *${nombre}*, la seña / inscripción es: ${sena}.` : `Puedo pasarle la info de inscripción de *${nombre}*.`,
      precio ? `Precio total: *${precio}*.` : '',
    ].filter(Boolean);
    return lines.join('\n');
  }

  const detailLines = [
    `Le paso la info de *${nombre}* 😊`,
    precio ? `• Precio: *${precio}*` : '',
    inicio ? `• Inicio: ${inicio}` : '',
    modalidad ? `• Modalidad: ${modalidad}` : '',
    duracion ? `• Duración: ${duracion}` : '',
    horarios ? `• Días y horarios: ${horarios}` : '',
    requisitos ? `• Requisitos: ${requisitos}` : '',
    sena ? `• Seña / inscripción: ${sena}` : '',
  ].filter(Boolean);

  return detailLines.join('\n');
}

const COURSE_SIGNAL_RE = /(curso|cursos|clase|clases|capacitacion|capacitaciones|capacitación|capacitaciones|taller|talleres|masterclass|seminario|seminarios|workshop|formacion|formación|certificacion|certificación)/i;
const COURSE_GENERIC_LIST_RE = /(hay|tenes|tenés|tienen|ofrecen|ofreces|dictan|dan|brindan|hacen|disponibles|abiertos|abiertas|cupos|inscripciones|inscripcion|inscripción|empiezan|arrancan|se dicta|se dictan|se esta dictando|se está dictando|se estan dando|se están dando|busco|ando buscando|quiero info|quisiera info|me pasas info|me pasás info|mostrame|mostrar|mandame|pasame|lista|opciones|catalogo|catálogo|informacion|información)/i;
const COURSE_FOLLOWUP_RE = /(mas info|más info|info|precio|cuanto sale|cuánto sale|cuanto cuesta|cuánto cuesta|cuando empieza|cuándo empieza|cuando arranca|cuándo arranca|inicio|duracion|duración|horario|horarios|dias|días|cupo|cupos|inscripcion|inscripción|requisitos|modalidad|presencial|online|virtual|de ese|de ese curso|ese curso|de barberia|de barbería|de maquillaje|de colorimetria|de colorimetría|de peinados|de auxiliar|de estetica|de estética|para aprender)/i;
const PRODUCT_SIGNAL_RE = /(producto|productos|stock|insumo|insumos|shampoo|acondicionador|mascara|mascarilla|serum|aceite|oleo|tintura|oxidante|decolorante|matizador|ampolla|protector|spray|crema|gel|cera|mueble|muebles|espejo|espejos|camilla|camillas|sillon|sillones|silla|sillas|mesa|mesas|respaldo|puff|equipamiento|maquina|máquina|maquinas|máquinas|plancha|planchas|secador|secadores)/i;
const PRODUCT_LIST_SIGNAL_RE = /(hay|tenes|tenés|tienen|venden|disponible|disponibles|stock|lista|opciones|catalogo|catálogo|mostrar|mostrame|mandame|pasame|busco|ando buscando|quiero ver|foto|fotos|imagen|imagenes)/i;
const SERVICE_SIGNAL_RE = /(turno|turnos|servicio|servicios|reservar|reserva|agendar|agenda|cita|me quiero hacer|quiero hacerme|para hacerme|hacerme|me hago|realizan|trabajan con|atienden|precio del servicio|valor del servicio)/i;
const SERVICE_LIST_SIGNAL_RE = /(que servicios|qué servicios|servicios tienen|lista de servicios|todos los servicios|mostrar servicios|mostrame servicios|mandame servicios|pasame servicios)/i;

function isLikelyGenericCourseListQuery(text) {
  const t = normalize(text || '');
  if (!t) return false;
  if (/^(curso|cursos|clase|clases|capacitacion|capacitaciones|capacitación|taller|talleres|masterclass|seminario|seminarios|workshop)$/.test(t)) return true;
  if (COURSE_SIGNAL_RE.test(t) && COURSE_GENERIC_LIST_RE.test(t)) return true;
  if (/(busco|ando buscando|quiero info|quisiera info)/i.test(t) && COURSE_SIGNAL_RE.test(t)) return true;
  if (/(dictan clases|dan clases|estan dando clases|están dando clases|se dicta|se dictan|se esta dictando|se está dictando|se estan dando|se están dando)/i.test(t)) return true;
  return false;
}

function detectFastCatalogIntent(text, context = {}) {
  const raw = String(text || '').trim();
  const t = normalize(raw);
  if (!t) return null;

  const hasCourseSignal = COURSE_SIGNAL_RE.test(t)
    || /(dictan clases|dan clases|estan dando clases|están dando clases|se dicta|se dictan|se esta dictando|se está dictando|se estan dando|se están dando)/i.test(t);
  const hasCourseFollowup = !!context.hasCourseContext && (COURSE_FOLLOWUP_RE.test(t) || /^(ese|ese curso|de ese|de ese curso|barberia|barbería|maquillaje|colorimetria|colorimetría|auxiliar|peinados|cupos?|precio|info|horario|horarios|duracion|duración|modalidad|inicio|requisitos)$/.test(t));

  if (hasCourseSignal || hasCourseFollowup) {
    const generic = isLikelyGenericCourseListQuery(raw);
    return {
      type: 'COURSE',
      query: generic ? 'cursos' : raw,
      mode: generic ? 'LIST' : 'DETAIL',
      confidence: generic ? 0.99 : 0.96,
    };
  }

  const hasProductSignal = PRODUCT_SIGNAL_RE.test(t);
  const hasServiceSignal = SERVICE_SIGNAL_RE.test(t);

  if (hasProductSignal && !hasServiceSignal) {
    return {
      type: 'PRODUCT',
      query: raw,
      mode: PRODUCT_LIST_SIGNAL_RE.test(t) ? 'LIST' : 'DETAIL',
      confidence: 0.92,
    };
  }

  if (hasServiceSignal && !hasProductSignal) {
    return {
      type: 'SERVICE',
      query: raw,
      mode: SERVICE_LIST_SIGNAL_RE.test(t) ? 'LIST' : 'DETAIL',
      confidence: 0.9,
    };
  }

  return null;
}

function detectCourseIntentFromContext(text, { lastCourseContext = null } = {}) {
  const raw = String(text || '').trim();
  const t = normalize(raw);
  if (!t) return { isCourse: false, query: '', mode: 'DETAIL' };

  const explicit = COURSE_SIGNAL_RE.test(t)
    || isExplicitCourseKeyword(raw)
    || /(dictan clases|dan clases|estan dando clases|están dando clases|se dicta|se dictan|se esta dictando|se está dictando|se estan dando|se están dando)/i.test(t);

  const genericList = explicit && isLikelyGenericCourseListQuery(raw);

  if (explicit) {
    return {
      isCourse: true,
      query: genericList ? 'cursos' : raw,
      mode: genericList ? 'LIST' : 'DETAIL',
    };
  }

  if (lastCourseContext && COURSE_FOLLOWUP_RE.test(t) && !/(\bturno\b|\breserv\w*\b|\bagend\w*\b|\bcita\b)/i.test(t)) {
    return {
      isCourse: true,
      query: resolveImplicitCourseFollowupQuery(raw, lastCourseContext) || raw,
      mode: 'DETAIL',
    };
  }

  return { isCourse: false, query: '', mode: 'DETAIL' };
}

// ===================== RESPUESTAS =====================
function formatStockReply(matches, mode, opts = {}) {
  if (!matches.length) return null;

  const items = mode === "LIST" ? matches.slice(0, 10) : matches.slice(0, 1);
  const blocks = items.map((p) => {
    const precio = moneyOrConsult(p.precio);
    return [
      `${getCatalogItemEmoji(p.nombre, { kind: 'product' })} *${p.nombre}*`,
      `• Precio: *${precio}*`,
    ].join("\n");
  });

  const inferredDomain = opts.domain || detectProductDomain(opts.familyLabel || matches.map((x) => `${x?.tab || ''} ${x?.nombre || ''} ${x?.categoria || ''}`).join(' | '));
  const header = mode === "LIST" ? `Encontré estas opciones:` : `Está en catálogo:`;
  const footer = `\n\n${buildProductFollowupQuestion({ domain: inferredDomain, familyLabel: opts.familyLabel || '' })}`;
  return `${header}\n\n${blocks.join("\n\n— — —\n\n")}${footer}`.trim();
}

// LISTA COMPLETA (sin filtrar por stock): se manda en varios mensajes para no cortar WhatsApp.
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

function formatStockRelatedListAll(rows, { domain = '', familyLabel = '', chunkSize = 10 } = {}) {
  const items = Array.isArray(rows) ? rows.filter(r => r?.nombre) : [];
  if (!items.length) return [];

  const activeDomain = domain || detectProductDomain(familyLabel || rows.map((x) => `${x?.tab || ''} ${x?.nombre || ''} ${x?.categoria || ''}`).join(' | '));
  const readableFamily = familyLabel ? (activeDomain === 'furniture' ? (getFurnitureFamilyDef(familyLabel)?.label || familyLabel) : getProductFamilyLabel(familyLabel)) : '';
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

    const header = i === 0 ? `✨ Tenemos estas opciones${intro}:` : `✨ Más opciones${intro}:`;
    const footer = (i + chunkSize) >= items.length
      ? `\n\n${buildProductFollowupQuestion({ domain: activeDomain, familyLabel })}`
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

function formatCourseReplyBlock(course = {}) {
  if (!course?.nombre) return "";

  const lines = [
    `🎓 *${course.nombre}*`,
    course.categoria ? `• Categoría: ${course.categoria}` : "",
    course.modalidad ? `• Modalidad: ${course.modalidad}` : "",
    course.duracionTotal ? `• Duración: ${course.duracionTotal}` : "",
    course.fechaInicio ? `• Inicio: ${course.fechaInicio}` : "",
    course.fechaFin ? `• Finalización: ${course.fechaFin}` : "",
    course.diasHorarios ? `• Días y horarios: ${course.diasHorarios}` : "",
    course.precio ? `• Precio: *${moneyOrConsult(course.precio)}*` : "",
    course.sena ? `• Seña / inscripción: ${course.sena}` : "",
    course.cupos ? `• Cupos: ${course.cupos}` : "",
    course.requisitos ? `• Requisitos: ${course.requisitos}` : "",
    course.estado ? `• Estado: ${course.estado}` : "",
    course.info ? `• Info: ${course.info}` : "",
  ].filter(Boolean);

  return lines.join("\n");
}

function buildCourseMediaCaption(course = {}) {
  const caption = formatCourseReplyBlock(course).trim();
  if (!caption) return '';
  if (caption.length <= 900) return caption;
  return `${caption.slice(0, 897).trimEnd()}...`;
}

function buildCourseCatalogClosingPrompt(mode = 'LIST') {
  return mode === 'LIST'
    ? 'Si quiere, le paso requisitos, horarios o inscripción del curso que le interese 😊'
    : 'Si quiere, también le paso requisitos, horarios o inscripción 😊';
}

function formatCoursesReplySequence(matches, mode) {
  if (!matches.length) return [];

  const limited = mode === "LIST" ? matches.slice(0, 10) : matches.slice(0, 3);
  const blocks = limited.map((c) => formatCourseReplyBlock(c)).filter(Boolean);
  const header = mode === "LIST"
    ? "🎓 Estos son los cursos disponibles:"
    : "🎓 Este es el curso que encontré:";

  if (mode === "LIST" && blocks.length > 1) {
    return [
      header,
      ...blocks,
      'Si quiere, también le paso el material del curso que le interese 😊',
    ].filter(Boolean);
  }

  return [`${header}\n\n${blocks.join("\n\n— — —\n\n")}\n\nSi quiere, también le paso el material del curso 😊`.trim()];
}

async function sendCourseCatalogResponses(phone, waId, matches, mode) {
  const limited = Array.isArray(matches)
    ? (mode === 'LIST' ? matches.slice(0, 10) : matches.slice(0, 3))
    : [];
  if (!limited.length) return false;

  const renderedParts = [];

  rememberAssistantCourseOffer(waId, limited, {
    mode,
    selectedName: limited.length === 1 ? (limited[0]?.nombre || '') : '',
    questionKind: mode === 'LIST' ? 'LIST' : 'DETAIL',
    lastAssistantText: '',
  });

  for (const course of limited) {
    const caption = buildCourseMediaCaption(course);
    const hasLinkedMedia = !!String(course?.link || '').trim();

    if (hasLinkedMedia) {
      const mediaResult = await sendCourseMediaDirect(phone, course, { caption });
      if (mediaResult.ok) {
        renderedParts.push(caption);
        pushHistory(waId, "assistant", caption);
      } else {
        const fallbackText = caption || formatNaturalCourseFollowupReply(course, 'DETAIL');
        if (fallbackText) {
          renderedParts.push(fallbackText);
          pushHistory(waId, "assistant", fallbackText);
          await sendWhatsAppText(phone, fallbackText);
        }
      }
    } else {
      const fallbackText = caption || formatNaturalCourseFollowupReply(course, 'DETAIL');
      if (fallbackText) {
        renderedParts.push(fallbackText);
        pushHistory(waId, "assistant", fallbackText);
        await sendWhatsAppText(phone, fallbackText);
      }
    }

    await sleep(250);
  }

  const closingPrompt = buildCourseCatalogClosingPrompt(mode);
  if (closingPrompt) {
    renderedParts.push(closingPrompt);
    pushHistory(waId, "assistant", closingPrompt);
    await sendWhatsAppText(phone, closingPrompt);
  }

  rememberAssistantCourseOffer(waId, limited, {
    mode,
    selectedName: limited.length === 1 ? (limited[0]?.nombre || '') : '',
    questionKind: mode === 'LIST' ? 'LIST' : 'DETAIL',
    lastAssistantText: renderedParts.join('\n\n'),
  });

  return true;
}

function formatCoursesReply(matches, mode) {
  const parts = formatCoursesReplySequence(matches, mode);
  return parts.length ? parts[0] : null;
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
  return merged.slice(-32);
}

function buildConversationHistorySnippet(messages = [], maxTurns = 16, maxChars = 2400) {
  const rows = Array.isArray(messages) ? messages.slice(-Math.max(1, maxTurns)) : [];
  return rows
    .map((m) => `${m.role === 'assistant' ? 'assistant' : 'user'}: ${String(m?.content || '').replace(/\s+/g, ' ').trim()}`)
    .filter(Boolean)
    .join(' | ')
    .slice(0, Math.max(200, maxChars));
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
    fecha: resolveAppointmentDateYMD(row.appointment_date || ''),
    hora: formatAppointmentTimeForTemplate(row.appointment_time || ''),
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
  return /(?:\blunes\b|\bmartes\b|\bmiercoles\b|\bmiércoles\b|\bjueves\b|\bviernes\b|\bsabado\b|\bsábado\b|\bdomingo\b|\bhoy\b|\bmañana\b|\bpasado\s+mañana\b|\bproximo\b|\bpróximo\b|\bel\s+dia\b|\bel\s+día\b|\by\s+el\b|\bque\s+horarios\b|\bqué\s+horarios\b|\bque\s+disponibilidad\b|\bqué\s+disponibilidad\b|\btenes\s+lugar\b|\btenés\s+lugar\b|\bdisponible\b|\bdisponibilidad\b|\ba\s+la\s+mañana\b|\bpor\s+la\s+mañana\b|\ba\s+la\s+tarde\b|\bpor\s+la\s+tarde\b|\b\d{1,2}[:.]\d{2}\b|\b\d{1,2}\s*(?:hs|horas?)\b|\b\d{1,2}[\/-]\d{1,2}(?:[\/-]\d{2,4})?\b)/i.test(t);
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

async function classifyAppointmentDraftControl(text, context = {}) {
  const raw = String(text || '').trim();
  if (!raw) return { action: 'UNCLEAR', reason: '', source: 'empty' };

  const flowStep = String(context.flowStep || '').trim();
  const contactInfo = extractContactInfo(raw);
  const cleanFullName = cleanNameCandidate(raw);
  const rawPhone = sanitizePossiblePhone(raw);
  const hasName = !!(contactInfo.nombre || cleanFullName);
  const hasPhone = !!(contactInfo.telefono || rawPhone);

  if (flowStep === 'awaiting_name' && hasName) {
    return { action: 'CONTINUE_APPOINTMENT', reason: 'provided_name', source: 'deterministic' };
  }

  if (flowStep === 'awaiting_phone' && hasPhone) {
    return { action: 'CONTINUE_APPOINTMENT', reason: 'provided_phone', source: 'deterministic' };
  }

  if (flowStep === 'awaiting_contact' && (hasName || hasPhone)) {
    return { action: 'CONTINUE_APPOINTMENT', reason: 'provided_contact', source: 'deterministic' };
  }

  if (flowStep === 'ready_to_book' && (hasName || hasPhone || isLikelyPaymentText(raw))) {
    return { action: 'CONTINUE_APPOINTMENT', reason: 'provided_completion_data', source: 'deterministic' };
  }

  try {
    const completion = await openai.chat.completions.create({
      model: PRIMARY_MODEL,
      temperature: 0,
      messages: [
        {
          role: 'system',
          content:
`Analizá SOLO el control del flujo de turnos y devolvé JSON estricto.

Campos:
- action: CONTINUE_APPOINTMENT | PAUSE_APPOINTMENT | SWITCH_TOPIC | UNCLEAR
- reason: string breve

Reglas:
- CONTINUE_APPOINTMENT si el cliente sigue con el turno, confirma, aporta datos del turno, responde algo útil para reservar o manda comprobante.
- PAUSE_APPOINTMENT si posterga, frena, cancela, deja para más adelante, dice que responde otro día o que después confirma.
- SWITCH_TOPIC si deja el turno y cambia a otro tema claro, por ejemplo producto, cursos, fotos, muebles, horarios del salón, otra consulta distinta.
- UNCLEAR si no alcanza para decidir.

Importante:
- "te respondo en otro momento", "después te confirmo", "más tarde te aviso", "por ahora no", "dejémoslo ahí" => PAUSE_APPOINTMENT
- "quiero comprar un shampoo", "tenés fotos de las camillas", "qué cursos hay", "cancelar" => SWITCH_TOPIC si además cambia a otra consulta, o PAUSE_APPOINTMENT si solo corta el turno.
- "no gracias" puede ser PAUSE_APPOINTMENT si solo cierra el tema del turno.
- Si manda fecha, hora, nombre, teléfono o comprobante, casi siempre es CONTINUE_APPOINTMENT.
- Si el flujo está esperando nombre o teléfono, respuestas como "María Tolaba", "Juan Pérez" o un número de celular son CONTINUE_APPOINTMENT.
- Respuestas como "el lunes a las 17", "lunes 17 hs", "a las 17", "el martes" o similares son CONTINUE_APPOINTMENT.

Respondé SOLO JSON.`
        },
        {
          role: 'user',
          content: JSON.stringify({
            mensaje: raw,
            servicio_actual: context.serviceName || '',
            fecha_actual: context.date || '',
            hora_actual: context.time || '',
            flujo_actual: flowStep,
            historial_reciente: context.historySnippet || '',
          })
        }
      ],
      response_format: { type: 'json_object' },
    });

    const obj = JSON.parse(completion.choices?.[0]?.message?.content || '{}');
    const action = String(obj?.action || 'UNCLEAR').trim().toUpperCase();
    if (!['CONTINUE_APPOINTMENT', 'PAUSE_APPOINTMENT', 'SWITCH_TOPIC', 'UNCLEAR'].includes(action)) {
      return { action: 'UNCLEAR', reason: '', source: 'ai_invalid' };
    }
    return {
      action,
      reason: String(obj?.reason || '').trim(),
      source: 'ai',
    };
  } catch {
    const fallback = extractTurnoPauseIntent(raw);
    if (fallback.matched) {
      return {
        action: fallback.remainder ? 'SWITCH_TOPIC' : 'PAUSE_APPOINTMENT',
        reason: 'fallback_regex',
        remainder: fallback.remainder || '',
        source: 'regex',
      };
    }
    return { action: 'UNCLEAR', reason: '', source: 'fallback' };
  }
}

function isExplicitProductIntent(text) {
  const t = normalize(text || '');
  return /(producto|productos|stock|insumo|insumos|shampoo|acondicionador|mascara|mascarilla|serum|aceite|oleo|tintura|oxidante|decolorante|matizador|ampolla|protector|spray|crema|gel|cera|comprar|venden|tenes|tenés|hay disponible|te queda|les queda|mueble|muebles|espejo|espejos|camilla|camillas|sillon|sillones|silla|sillas|mesa|mesas|respaldo|puff|equipamiento|maquina|máquina|maquinas|máquinas|plancha|planchas|secador|secadores)/i.test(t);
}

function isExplicitServiceIntent(text) {
  const t = normalize(text || '');
  return /(turno|otro turno|servicio|servicios|reservar|agendar|cita|hac(en|en)|realizan|trabajan con|me quiero hacer|quiero hacerme|para hacerme|hacerme|me hago|sesion|sesión|aplicacion|aplicación|corte femenino|femenino|femenina|mi hija|otra hija|mi tia|mi señora|ella)/i.test(t);
}

function extractAmbiguousBeautyTerm(text) {
  const t = normalize(text || '');
  const terms = ['alisado', 'botox', 'keratina', 'nutricion', 'tratamiento'];
  return terms.find(term => new RegExp(`\\b${escapeRegex(term)}\\b`, 'i').test(t)) || '';
}

const AMBIGUOUS_BRIDGE_MESSAGE = `Hola!😊 En este momento no podemos identificar sobre qué nos consultás. Si nos decís qué producto, servicio, promoción o curso te interesa, te pasamos toda la información.`;

function stripGreetingPrefix(text) {
  const raw = String(text || '').trim();
  if (!raw) return '';
  return raw
    .replace(/^(hola+|buenas+|buen dia|buen día|buenos dias|buenos días|buenas tardes|buenas noches|buen diaa+|holaa+)[,!.\s-]*/i, '')
    .trim();
}

function isGreetingOnly(text) {
  const t = normalize(stripGreetingPrefix(text || ''));
  return !t;
}

function stripSoftConversationPrefix(text) {
  const original = String(text || '').trim();
  if (!original) return '';

  let out = original;
  const patterns = [
    /^(?:hola+|holaa+|holi+|holis+|buenas+|buen dia|buen día|buenos dias|buenos días|buenas tardes|buenas noches|hey|ey)\b[,!\s.:-]*/i,
    /^(?:chau+|chao+|gracias+|oka+y?|ok+|dale+|perfecto+|bueno+)\b[,!\s.:-]*/i,
  ];

  let changed = true;
  while (changed) {
    changed = false;
    for (const rx of patterns) {
      const candidate = out.replace(rx, '').trim();
      if (candidate && candidate !== out) {
        out = candidate;
        changed = true;
      }
    }
  }

  return out;
}

function hasRecentConversationContext(waId) {
  if (!waId) return false;
  return !!(
    getActiveAssistantOffer(waId)
    || getLastProductContext(waId)
    || getLastCourseContext(waId)
    || getPendingAmbiguousBeauty(waId)
    || getLastResolvedBeauty(waId)
    || getLastKnownService(waId, null)
  );
}

function buildContextualGreetingReply(waId) {
  const activeOffer = getActiveAssistantOffer(waId);
  if (activeOffer?.type === 'PRODUCT') {
    return 'Hola 😊 Seguimos con lo que veníamos viendo. Decime qué necesitás sobre esos productos y te ayudo.';
  }
  if (activeOffer?.type === 'COURSE') {
    return 'Hola 😊 Seguimos con lo que veníamos viendo del curso. Decime qué querés saber y te ayudo.';
  }
  if (activeOffer?.type === 'SERVICE' || getLastKnownService(waId, null)) {
    return 'Hola 😊 Seguimos con lo que veníamos viendo. Decime qué necesitás y te ayudo.';
  }
  if (getLastCourseContext(waId)) {
    return 'Hola 😊 Seguimos con lo que veníamos viendo del curso. Decime qué necesitás y te ayudo.';
  }
  if (getLastProductContext(waId)) {
    return 'Hola 😊 Seguimos con lo que veníamos viendo. Contame qué buscás y te ayudo a elegir.';
  }
  return '¡Hola! 😊 ¿En qué puedo ayudarte?';
}

function looksLikeHairCareConsultation(text = '') {
  const t = normalize(text || '');
  if (!t) return false;
  if (/(\bturno\b|\breserv\w*\b|\bagend\w*\b|\bcita\b|cuanto dura|cuánto dura|cuanto demora|cuánto demora)/i.test(t)) return false;
  return /(pelo|cabello|seco|reseco|dañado|danado|quebrado|frizz|rulos|graso|caida|caída|hidrat|repar|nutric|acondicionador|bano de crema|baño de crema|mascara|máscara|mascarilla|ampolla|serum|sérum|shampoo|matizador|qué es mejor|que es mejor|como es el tema|cómo es el tema|me recomendas|me recomendás|cual me conviene|cuál me conviene)/i.test(t);
}

function hasConcreteCommercialContext(text) {
  const t = normalize(text || '');
  if (!t) return false;
  return /(curso|cursos|capacitacion|capacitación|taller|talleres|servicio|servicios|turno|turnos|producto|productos|promo|promocion|promoción|alisado|botox|keratina|nutricion|nutrición|tratamiento|corte|color|tintura|mechas|balayage|barber|barberia|barbería|shampoo|champu|champú|acondicionador|serum|sérum|ampolla|matizador|decolorante|camilla|camillas|espejo|espejos|sillon|sillón|sillones|mesa|mesas|mueble|muebles|equipamiento|maquina|máquina|maquinas|máquinas)/i.test(t);
}

function isAmbiguousBridgeCandidate(text) {
  const raw = stripGreetingPrefix(text || '');
  const t = normalize(raw);
  if (!t) return false;
  if (hasConcreteCommercialContext(t)) return false;
  if (t.length > 40) return false;

  const exactMatches = new Set([
    'info', 'informacion', 'información', 'mas info', 'más info',
    'precio', 'precios', 'valor', 'valores', 'costo', 'costos',
    'cuanto', 'cuánto', 'cuanto sale', 'cuánto sale', 'cuanto cuesta', 'cuánto cuesta',
    'requisitos', 'que requisitos', 'qué requisitos', 'requisito',
    'promo', 'promocion', 'promoción', 'promo?', 'promocion?', 'promoción?'
  ]);
  if (exactMatches.has(t)) return true;

  return /^(info|informacion|información|precio|precios|valor|valores|costo|costos|promo|promocion|promoción|requisitos|requisito|cuanto|cuánto)(\s|$|[?!.,])/.test(t);
}

function shouldUseAmbiguousBridgeMessage({ waId, text }) {
  if (!waId) return false;
  if (isGreetingOnly(text)) return false;
  if (!isAmbiguousBridgeCandidate(text)) return false;

  // Si ya existe contexto reciente, nunca tratamos el mensaje como ambiguo.
  if (
    getLastCourseContext(waId) ||
    getLastProductContext(waId) ||
    getPendingAmbiguousBeauty(waId) ||
    getLastResolvedBeauty(waId) ||
    getLastKnownService(waId, null)
  ) {
    return false;
  }

  const conv = ensureConv(waId);
  const history = Array.isArray(conv?.messages) ? conv.messages : [];
  const assistantCount = history.filter((m) => m?.role === 'assistant').length;
  const userCount = history.filter((m) => m?.role === 'user').length;

  return assistantCount === 0 && userCount <= 1;
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
  if (base?.appointment_id && base?.payment_status !== 'paid_verified') return 'awaiting_payment';
  return 'ready_to_book';
}

// ===================== INTENCIÓN =====================
async function classifyAndExtract(text, context = {}) {
  const completion = await openai.chat.completions.create({
    model: PRIMARY_MODEL,
    temperature: 0,
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

Reglas de negocio:
- PRODUCT incluye productos, insumos, stock, muebles, espejos, camillas, sillones, equipamiento y máquinas. Si menciona muebles/equipamiento, mantené PRODUCT pero pensalo como submundo MUEBLES.
- SERVICE incluye servicios del salón y consultas para reservar/agendar turno.
- COURSE incluye cursos, clases, capacitaciones, talleres, masterclass, workshop, seminarios y preguntas sobre cupos, inscripción, modalidad, horarios, inicio, precio o requisitos de cursos.
- Si el cliente quiere anotarse, inscribirse, reservar lugar o señar para un curso, también sigue siendo COURSE.
- Si una palabra puede ser producto o servicio y el cliente no lo aclaró, devolvé OTHER.
- Si el mensaje es genérico y pide opciones, lista, qué tienen o si hay disponible, mode=LIST.
- Si nombra algo puntual o sigue una conversación ya abierta sobre ese tema, mode=DETAIL.
- Si hay borrador de turno activo o servicio_actual cargado y el cliente manda fecha, hora, nombre, teléfono, comprobante o una continuación tipo "el lunes a las 17", priorizá SERVICE.

Tené en cuenta el contexto previo:
- servicio_actual: si existe, mensajes como "quiero el turno", "dale", "quiero ese", "bien" suelen referirse a ese servicio.
- curso_actual y curso_contexto_activo: si venían hablando de cursos, mensajes como "alguno de barbería", "más info", "de ese", "cuándo empieza", "precio", "cupos", "modalidad" deben clasificarse como COURSE.
- flujo_actual: si el cliente ya estaba hablando de reservar, priorizá continuidad y no lo mandes a catálogo de nuevo.
- Si el mensaje es solo 'si', 'dale', 'ok' o similar y venían hablando de un servicio, priorizá la continuidad de ese tema.
- producto_actual, producto_contexto_activo y productos_recientes: si venían hablando de productos o el asistente acaba de recomendar opciones, mensajes como "pasame fotos", "precio del primero", "quiero ese", "más info" o "de ese" deben clasificarse como PRODUCT.
- oferta_asistente_activa: si existe, priorizá interpretar el mensaje como respuesta a esa última oferta del bot, salvo que el cambio de tema sea claro.

Ejemplos:
- "hola busco cursos" => COURSE + query "cursos" + LIST
- "están dictando clases?" => COURSE + query "cursos" + LIST
- "tienen capacitaciones?" => COURSE + query "cursos" + LIST
- "alguno de barbería" con contexto de curso => COURSE + DETAIL
- "cursos de barbería" => COURSE + query "barbería" + DETAIL
- "hay espejos?" => PRODUCT + LIST
- "qué stock hay de camillas" => PRODUCT + LIST
- "precio del espejo led" => PRODUCT + DETAIL
- "qué servicios hacen" => SERVICE + LIST
- "quiero turno para corte" => SERVICE + DETAIL
- "precio del alisado" => OTHER
- "tienen ampollas para el pelo" => PRODUCT + query "ampollas" + LIST
- "cual es la mejor ampolla para reparacion profunda" => PRODUCT + query "ampolla reparacion" + DETAIL
- "necesito una plancha para el pelo" => PRODUCT + query "plancha" + DETAIL
- "necesito un secador para el pelo" => PRODUCT + query "secador" + DETAIL
- "tienen sillones de barbería" => PRODUCT + query "sillones de barbería" + DETAIL
- "podés enviarme una foto de la camilla camaleón" => PRODUCT + query "Camilla Camaleón" + DETAIL
- "cuanto dura el shock de keratina" => SERVICE + query "shock de keratina" + DETAIL

Respondé SOLO JSON.`
      },
      {
        role: "user",
        content: JSON.stringify({
          mensaje: text,
          servicio_actual: context.lastServiceName || "",
          producto_actual: context.lastProductName || "",
          producto_contexto_activo: !!context.hasProductContext,
          productos_recientes: context.productOptions || [],
          curso_actual: context.lastCourseName || "",
          curso_contexto_activo: !!context.hasCourseContext,
          cursos_recientes: context.courseOptions || [],
          oferta_asistente_activa: context.activeAssistantOfferType || "",
          oferta_asistente_items: context.activeAssistantOfferItems || [],
          oferta_asistente_seleccionado: context.activeAssistantOfferSelectedName || "",
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
    const rawType = obj.type || "OTHER";
    const rawQuery = (obj.query || "").trim();
    const rawMode = obj.mode || "DETAIL";
    const t = normalize(text || '');

    if (/(curso|cursos|inscrib|capacitacion|capacitación|masterclass|taller)/i.test(t)) {
      return { type: 'COURSE', query: rawQuery || text.trim(), mode: rawMode || 'DETAIL' };
    }

    if (/(ampolla|ampollas|secador|secadores|plancha|planchas|camilla|camillas|sillon|sillón|sillones|espejo|espejos|mueble|muebles|equipamiento)/i.test(t) && rawType !== 'COURSE') {
      return { type: 'PRODUCT', query: rawQuery || text.trim(), mode: rawMode || 'DETAIL' };
    }

    if (/(cuanto dura|cuánto dura|cuanto demora|cuánto demora|duracion|duración)/i.test(t) && /(shock de keratina|keratina|shock de botox|botox|nutricion|nutrición|alisado)/i.test(t)) {
      return { type: 'SERVICE', query: rawQuery || text.trim(), mode: 'DETAIL' };
    }

    return {
      type: rawType,
      query: rawQuery,
      mode: rawMode,
    };
  } catch {
    const t = normalize(text || '');
    if (/(curso|cursos|inscrib|capacitacion|capacitación|masterclass|taller)/i.test(t)) return { type: 'COURSE', query: text.trim(), mode: 'DETAIL' };
    if (/(ampolla|ampollas|secador|secadores|plancha|planchas|camilla|camillas|sillon|sillón|sillones|espejo|espejos|mueble|muebles|equipamiento)/i.test(t)) return { type: 'PRODUCT', query: text.trim(), mode: 'DETAIL' };
    if (/(cuanto dura|cuánto dura|cuanto demora|cuánto demora|duracion|duración)/i.test(t) && /(shock de keratina|keratina|shock de botox|botox|nutricion|nutrición|alisado)/i.test(t)) return { type: 'SERVICE', query: text.trim(), mode: 'DETAIL' };
    return { type: "OTHER", query: "", mode: "DETAIL" };
  }
}

// ===================== WHATSAPP SEND =====================
async function sendWhatsAppText(to, text, options = {}) {
  let body = String(text || "");
  const oneShotPrefix = consumeOneShotReplyPrefix(to);
  if (oneShotPrefix) {
    body = body ? `${oneShotPrefix}

${body}`.trim() : String(oneShotPrefix || '').trim();
  }

  const disableDedup = !!options?.disableDedup;
  const dedupKey = `${to}::${body}`;
  const now = Date.now();
  const prevTs = lastSentOutByPeer.get(dedupKey) || 0;
  if (!disableDedup && body && (now - prevTs) < OUT_DEDUP_MS) {
    return { deduped: true };
  }

  const resp = await axios.post(
    `https://graph.facebook.com/v19.0/${process.env.PHONE_NUMBER_ID}/messages`,
    { messaging_product: "whatsapp", to, text: { body } },
    {
      headers: {
        Authorization: `Bearer ${process.env.WHATSAPP_TOKEN}`,
        "Content-Type": "application/json",
      },
    }
  );

  if (!disableDedup && body) {
    lastSentOutByPeer.set(dedupKey, now);
  }

  const wa_msg_id = resp?.data?.messages?.[0]?.id || null;

  await dbInsertMessage({
    direction: "out",
    wa_peer: to,
    name: null,
    text: body,
    msg_type: "text",
    wa_msg_id,
    raw: resp?.data || {},
  });

  return { ...(resp?.data || {}), wa_msg_id };
}


async function sendWhatsAppTemplate(to, templateName, bodyVars = [], meta = {}, options = {}) {
  const recipient = normalizeWhatsAppRecipient(to);
  if (!recipient) throw new Error(`Número inválido para plantilla: ${to || '(vacío)'}`);
  if (!templateName) throw new Error('Falta templateName');

  const body = (Array.isArray(bodyVars) ? bodyVars : []).map((v) => String(v ?? '').trim());
  const disableDedup = !!options?.disableDedup;
  const dedupKey = `${recipient}::template::${templateName}::${body.join('|')}`;
  const now = Date.now();
  const prevTs = lastSentOutByPeer.get(dedupKey) || 0;
  if (!disableDedup && (now - prevTs) < OUT_DEDUP_MS) return { deduped: true };

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

  if (!disableDedup) {
    lastSentOutByPeer.set(dedupKey, now);
  }

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

function resolveAppointmentDateYMD(value) {
  if (!value) return '';

  if (value instanceof Date) {
    const time = value.getTime();
    if (Number.isNaN(time)) return '';
    const yyyy = String(value.getUTCFullYear()).padStart(4, '0');
    const mm = String(value.getUTCMonth() + 1).padStart(2, '0');
    const dd = String(value.getUTCDate()).padStart(2, '0');
    return buildValidYMD(yyyy, mm, dd);
  }

  const raw = String(value || '').trim();
  if (!raw) return '';

  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    return buildValidYMD(raw.slice(0, 4), raw.slice(5, 7), raw.slice(8, 10));
  }

  const shortEnglish = raw.match(/^(?:Sun|Mon|Tue|Wed|Thu|Fri|Sat)\s+([A-Za-z]{3})\s+(\d{1,2})(?:\s+(\d{4}))?$/i)
    || raw.match(/^([A-Za-z]{3})\s+(\d{1,2})(?:\s+(\d{4}))?$/i);
  if (shortEnglish) {
    const monthMap = {
      jan: '01', feb: '02', mar: '03', apr: '04', may: '05', jun: '06',
      jul: '07', aug: '08', sep: '09', oct: '10', nov: '11', dec: '12',
    };
    const mon = monthMap[String(shortEnglish[1]).slice(0, 3).toLowerCase()];
    const dd = String(shortEnglish[2]).padStart(2, '0');
    const yyyy = String(shortEnglish[3] || todayYMDInTZ().slice(0, 4));
    if (mon) return buildValidYMD(yyyy, mon, dd);
  }

  return toYMD(raw);
}

function formatAppointmentDateForTemplate(dateYMD) {
  const ymd = resolveAppointmentDateYMD(dateYMD);
  if (!ymd) return '';

  try {
    const d = new Date(`${ymd}T12:00:00-03:00`);
    if (Number.isNaN(d.getTime())) return ymdToDMY(ymd);

    const weekday = new Intl.DateTimeFormat('es-AR', {
      weekday: 'long',
      timeZone: TIMEZONE,
    }).format(d);

    const day = new Intl.DateTimeFormat('es-AR', {
      day: 'numeric',
      timeZone: TIMEZONE,
    }).format(d);

    const month = new Intl.DateTimeFormat('es-AR', {
      month: 'long',
      timeZone: TIMEZONE,
    }).format(d);

    const weekdayCap = weekday ? weekday.charAt(0).toUpperCase() + weekday.slice(1) : '';
    return `${weekdayCap} ${day} de ${month}`.trim();
  } catch {
    return ymdToDMY(ymd);
  }
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
    appointment_date: resolveAppointmentDateYMD(row.appointment_date || ''),
    appointment_time: formatAppointmentTimeForTemplate(row.appointment_time || ''),
    status: String(row.status || '').trim(),
    stylist_notified_at: row.stylist_notified_at || null,
  };
}

function buildAppointmentTemplateVarsForStylist(appt) {
  return [
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
  const allowed = new Set(['stylist_notified_at']);
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

async function insertCourseEnrollmentNotificationLog({ enrollmentId, notificationType, recipientPhone, templateName, waMessageId, payload }) {
  await db.query(
    `INSERT INTO course_enrollment_notifications (enrollment_id, notification_type, recipient_phone, template_name, wa_message_id, payload)
     VALUES ($1, $2, $3, $4, $5, $6)`,
    [
      enrollmentId,
      notificationType,
      normalizePhone(recipientPhone || ''),
      templateName || null,
      waMessageId || null,
      payload || {},
    ]
  );
}

async function hasCourseEnrollmentNotification({ enrollmentId, notificationType }) {
  const r = await db.query(
    `SELECT id FROM course_enrollment_notifications WHERE enrollment_id = $1 AND notification_type = $2 LIMIT 1`,
    [enrollmentId, notificationType]
  );
  return !!r.rows?.length;
}

async function sendCourseEnrollmentTemplateAndLog({ enrollmentId, recipientPhone, templateName, notificationType, vars }) {
  const response = await sendWhatsAppTemplate(recipientPhone, templateName, vars, {
    enrollment_id: enrollmentId,
    notification_type: notificationType,
  });

  await insertCourseEnrollmentNotificationLog({
    enrollmentId,
    notificationType,
    recipientPhone,
    templateName,
    waMessageId: response?.wa_msg_id || response?.messages?.[0]?.id || null,
    payload: { vars },
  });

  return response;
}

function buildCourseEnrollmentTemplateVars(enrollment = {}) {
  return [
    String(enrollment.student_name || '').trim() || 'Alumno/a',
    String(enrollment.course_name || '').trim() || 'Curso',
    normalizePhone(enrollment.contact_phone || enrollment.wa_phone || '') || '-',
    COURSE_SENA_TXT,
    'comprobante recibido',
  ];
}

function buildCourseProofCaption(enrollment = {}) {
  return 'Comprobante enviado';
}

function guessMimeTypeFromFilename(filename = '') {
  const lower = String(filename || '').trim().toLowerCase();
  if (lower.endsWith('.png')) return 'image/png';
  if (lower.endsWith('.jpg') || lower.endsWith('.jpeg')) return 'image/jpeg';
  if (lower.endsWith('.webp')) return 'image/webp';
  if (lower.endsWith('.pdf')) return 'application/pdf';
  return 'application/octet-stream';
}

async function sendWhatsAppDocumentById(to, mediaId, filename, caption) {
  await axios.post(
    `https://graph.facebook.com/v19.0/${process.env.PHONE_NUMBER_ID}/messages`,
    {
      messaging_product: 'whatsapp',
      to,
      type: 'document',
      document: { id: mediaId, ...(filename ? { filename } : {}), ...(caption ? { caption } : {}) },
    },
    {
      headers: {
        Authorization: `Bearer ${process.env.WHATSAPP_TOKEN}`,
        'Content-Type': 'application/json',
      },
    }
  );
  await dbInsertMessage({
    direction: 'out',
    wa_peer: to,
    name: null,
    text: caption || '',
    msg_type: 'document',
    wa_msg_id: mediaId || null,
    raw: { sent: true, media: { id: mediaId, filename: filename || `out-${mediaId}` } },
  });
}

async function forwardCourseProofToManager(enrollment = {}) {
  const recipient = normalizeWhatsAppRecipient(COURSE_NOTIFY_PHONE_RAW);
  const savedName = String(enrollment.payment_proof_filename || '').trim();
  if (!recipient || !savedName) return false;

  const filePath = path.join(MEDIA_DIR, savedName);
  if (!fs.existsSync(filePath)) return false;

  const mimeType = guessMimeTypeFromFilename(savedName);
  const mediaId = await uploadMediaToWhatsApp(filePath, mimeType);
  const caption = buildCourseProofCaption(enrollment);
  if (mimeType.startsWith('image/')) {
    await sendWhatsAppImageById(recipient, mediaId, caption);
  } else {
    await sendWhatsAppDocumentById(recipient, mediaId, savedName, caption);
  }
  return true;
}

function parseCourseManagerApprovalAction(msg = {}, text = '') {
  const payload = normalize(msg?.button?.payload || msg?.interactive?.button_reply?.id || '');
  const title = normalize(msg?.button?.text || msg?.interactive?.button_reply?.title || text || '');
  const hay = `${payload} | ${title}`.trim();
  if (/(?:^|\b)(aprobar|aprobado|approve|approved|ok visto|visto|revisado|confirmado)(?:\b|$)/i.test(hay)) return 'approve';
  return '';
}

async function findCourseEnrollmentNotificationByMessageId(waMessageId = '') {
  const msgId = String(waMessageId || '').trim();
  if (!msgId) return null;
  const r = await db.query(`SELECT * FROM course_enrollment_notifications WHERE wa_message_id = $1 LIMIT 1`, [msgId]);
  return r.rows?.[0] || null;
}

async function markCourseEnrollmentNotificationApproved(notificationId, { approvedByPhone = '', approvalText = '' } = {}) {
  const r = await db.query(
    `UPDATE course_enrollment_notifications
        SET approved_at = COALESCE(approved_at, NOW()),
            approved_by_phone = COALESCE(NULLIF(approved_by_phone, ''), $2),
            approval_text = CASE WHEN approved_at IS NULL THEN $3 ELSE COALESCE(approval_text, $3) END
      WHERE id = $1
      RETURNING *`,
    [Number(notificationId || 0), normalizePhone(approvedByPhone || ''), String(approvalText || '').trim() || null]
  );
  return r.rows?.[0] || null;
}

async function markCourseEnrollmentNotificationStudentNotified(notificationId) {
  const r = await db.query(
    `UPDATE course_enrollment_notifications
        SET approved_student_notified_at = COALESCE(approved_student_notified_at, NOW())
      WHERE id = $1
      RETURNING *`,
    [Number(notificationId || 0)]
  );
  return r.rows?.[0] || null;
}

async function findCourseEnrollmentById(enrollmentId) {
  const r = await db.query(`SELECT * FROM course_enrollments WHERE id = $1 LIMIT 1`, [Number(enrollmentId || 0)]);
  return r.rows?.[0] || null;
}

function buildCourseEnrollmentApprovedStudentMessage(enrollment = {}) {
  const studentName = String(enrollment?.student_name || '').trim();
  const courseName = String(enrollment?.course_name || '').trim();
  const saludo = studentName ? `${studentName}, ` : '';
  return [
    `Perfecto 😊 ${saludo}ya quedó *todo registrado correctamente*${courseName ? ` para *${courseName}*` : ''}.`,
    '',
    'El resto de la inscripción puede abonarlo *el primer día / al comienzo de la primera clase*.',
    'Si lo prefiere, también puede *acercarse al local* para completar ese pago.',
    '',
    'Cualquier cosa, me escribe por aquí ✨',
  ].join("\n").trim();
}

async function findLatestPendingCourseEnrollmentNotification(recipientPhone = '') {
  const r = await db.query(
    `SELECT *
       FROM course_enrollment_notifications
      WHERE recipient_phone = $1
        AND notification_type = 'course_payment_received'
        AND approved_at IS NULL
        AND expired_at IS NULL
      ORDER BY sent_at DESC, id DESC
      LIMIT 1`,
    [normalizePhone(recipientPhone || '')]
  );
  return r.rows?.[0] || null;
}

async function handleCourseManagerApprovalInbound({ msg, text, phone, phoneRaw }) {
  const managerPhone = normalizePhone(COURSE_NOTIFY_PHONE_RAW);
  const inboundPhone = normalizePhone(phoneRaw || phone || '');
  if (!managerPhone || inboundPhone !== managerPhone) return false;

  const action = parseCourseManagerApprovalAction(msg, text);
  if (action !== 'approve') return false;

  const contextMsgId = msg?.context?.id || '';
  let notif = await findCourseEnrollmentNotificationByMessageId(contextMsgId);
  if (!notif) notif = await findLatestPendingCourseEnrollmentNotification(inboundPhone);

  if (!notif) {
    await sendWhatsAppText(phone, 'No encontré una inscripción pendiente para marcar como aprobada.');
    return true;
  }

  if (notif.approved_at) {
    await sendWhatsAppText(phone, 'Esa inscripción ya estaba marcada como aprobada.');
    return true;
  }

  if (notif.expired_at) {
    await sendWhatsAppText(phone, 'Esa inscripción ya venció por falta de respuesta dentro del plazo configurado.');
    return true;
  }

  const approvedNotif = await markCourseEnrollmentNotificationApproved(notif.id, {
    approvedByPhone: inboundPhone,
    approvalText: String(text || '').trim() || 'Aprobado por responsable',
  });

  await sendWhatsAppText(phone, 'Perfecto 😊 Ya quedó marcada como revisada.');

  const enrollment = await findCourseEnrollmentById(notif.enrollment_id);
  if (approvedNotif && enrollment && !approvedNotif.approved_student_notified_at) {
    const studentPhone = normalizeWhatsAppRecipient(enrollment.wa_phone || enrollment.contact_phone || '');
    const studentMessage = buildCourseEnrollmentApprovedStudentMessage(enrollment);
    if (studentPhone && studentMessage) {
      try {
        await sendWhatsAppText(studentPhone, studentMessage);
        await markCourseEnrollmentNotificationStudentNotified(notif.id);
      } catch (e) {
        console.error('❌ Error avisando al alumno aprobación de inscripción:', e?.response?.data || e?.message || e);
      }
    }
  }
  return true;
}

async function notifyCourseManagerEnrollmentPaid(enrollment = {}) {
  const enrollmentId = Number(enrollment?.id || 0);
  const recipient = normalizeWhatsAppRecipient(COURSE_NOTIFY_PHONE_RAW);
  if (!enrollmentId || !recipient || !TEMPLATE_CURSO_SENA_RECIBIDA) return { ok: false, skipped: true };

  const alreadySent = await hasCourseEnrollmentNotification({
    enrollmentId,
    notificationType: 'course_payment_received',
  });
  if (alreadySent) return { ok: true, deduped: true };

  await sendCourseEnrollmentTemplateAndLog({
    enrollmentId,
    recipientPhone: recipient,
    templateName: TEMPLATE_CURSO_SENA_RECIBIDA,
    notificationType: 'course_payment_received',
    vars: buildCourseEnrollmentTemplateVars(enrollment),
  });

  await sleep(700);

  const proofForwarded = await forwardCourseProofToManager(enrollment).catch((e) => {
    console.error('❌ Error reenviando comprobante de curso a responsable:', e?.response?.data || e?.message || e);
    return false;
  });

  return { ok: true, proofForwarded };
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

  if (msg.type === "button") {
    return { text: msg.button?.text || msg.button?.payload || "", kind: "button" };
  }

  if (msg.type === "interactive") {
    return {
      text: msg.interactive?.button_reply?.title || msg.interactive?.button_reply?.id || msg.interactive?.list_reply?.title || msg.interactive?.list_reply?.id || "",
      kind: "interactive"
    };
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

async function trySendExceptionalProductPhotos(phone, products, { domain = '', family = '', query = '' } = {}) {
  const selected = selectAutoPhotoExceptionalRows(products, { domain, family, query });
  if (!selected.length) return { attempted: false, handled: false, sentCount: 0, missing: [], failed: [] };

  const unique = [];
  const seen = new Set();
  for (const product of selected) {
    const key = normalizeCatalogSearchText(`${product?.nombre || ''} ${product?.marca || ''}`);
    if (!product?.nombre || seen.has(key)) continue;
    seen.add(key);
    unique.push(product);
  }

  const limited = unique.slice(0, 8);
  if (!limited.length) return { attempted: false, handled: false, sentCount: 0, missing: [], failed: [] };

  if (limited.length > 1) {
    await sendWhatsAppText(phone, 'Le paso las opciones con foto 😊');
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

  if (!sentCount) {
    if (missing.length) {
      const msg = missing.length === 1
        ? `No tengo foto vinculada para *${missing[0]}*, pero le paso la info igual 😊`
        : `No tengo foto vinculada para estas opciones: ${missing.join(', ')}, pero le paso la info igual 😊`;
      await sendWhatsAppText(phone, msg);
    }
    if (failed.length) {
      await sendWhatsAppText(phone, 'No pude enviar las fotos en este momento. Le paso la info igual 😊');
    }
    return { attempted: true, handled: false, sentCount, missing, failed, selected: limited };
  }

  if (missing.length) {
    await sendWhatsAppText(phone, `No tengo foto vinculada de: ${missing.join(', ')}.`);
  }

  if (failed.length) {
    await sendWhatsAppText(phone, `No pude enviar algunas fotos ahora mismo: ${failed.join(', ')}.`);
  }

  return { attempted: true, handled: true, sentCount, missing, failed, selected: limited };
}

async function maybeSendProductPhoto(phone, product, userText) {
  if (!product) return false;
  if (!userAsksForPhoto(userText)) return false;

  const result = await sendProductPhotoDirect(phone, product);
  if (result.ok) return true;

  if (result.reason === 'missing_link') {
    await sendWhatsAppText(
      phone,
      'No tengo una foto vinculada para ese producto o mueble en este momento.'
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
  if (/\.mp4($|\?)/.test(v)) return 'video/mp4';
  if (/\.mov($|\?)/.test(v)) return 'video/quicktime';
  if (/\.3gp($|\?)/.test(v)) return 'video/3gpp';
  if (/\.m4v($|\?)/.test(v)) return 'video/mp4';
  return 'image/jpeg';
}

function mediaExtFromMime(mimeType = '') {
  const m = String(mimeType || '').toLowerCase();
  if (m === 'image/png') return '.png';
  if (m === 'image/webp') return '.webp';
  if (m === 'image/jpeg' || m === 'image/jpg') return '.jpg';
  if (m === 'video/mp4') return '.mp4';
  if (m === 'video/quicktime') return '.mov';
  if (m === 'video/3gpp') return '.3gp';
  if (m === 'application/pdf') return '.pdf';
  return imageExtFromMime(mimeType);
}

function normalizeSharedMediaMimeType(mimeType = '', fallbackValue = '') {
  const clean = String(mimeType || '').split(';')[0].trim().toLowerCase();
  if (clean) return clean;
  return imageMimeFromPathish(fallbackValue || '');
}

async function downloadSharedMediaFromLink(link, tmpPrefix = 'media') {
  const value = String(link || '').trim();
  if (!value) throw new Error('missing_link');

  const driveFileId = extractDriveFileId(value);
  if (driveFileId) {
    let mimeType = '';
    try {
      const drive = await getDriveClient();
      const meta = await drive.files.get({ fileId: driveFileId, fields: 'mimeType,name' });
      mimeType = meta?.data?.mimeType || '';
    } catch {}

    mimeType = normalizeSharedMediaMimeType(mimeType, value);
    const ext = mediaExtFromMime(mimeType);
    const tmpPath = path.join(getTmpDir(), `${tmpPrefix}-${driveFileId}${ext}`);
    await downloadDriveFileToPath(driveFileId, tmpPath);
    return { tmpPath, mimeType };
  }

  const resp = await axios.get(value, { responseType: 'arraybuffer' });
  const mimeType = normalizeSharedMediaMimeType(resp?.headers?.['content-type'] || '', value);
  const ext = mediaExtFromMime(mimeType);
  const tmpPath = path.join(getTmpDir(), `${tmpPrefix}-${Date.now()}${ext}`);
  fs.writeFileSync(tmpPath, Buffer.from(resp.data));
  return { tmpPath, mimeType };
}

async function sendWhatsAppVideoById(to, mediaId, caption) {
  await axios.post(
    `https://graph.facebook.com/v19.0/${process.env.PHONE_NUMBER_ID}/messages`,
    {
      messaging_product: 'whatsapp',
      to,
      type: 'video',
      video: { id: mediaId, ...(caption ? { caption } : {}) },
    },
    {
      headers: {
        Authorization: `Bearer ${process.env.WHATSAPP_TOKEN}`,
        'Content-Type': 'application/json',
      },
    }
  );
  await dbInsertMessage({
    direction: 'out',
    wa_peer: to,
    name: null,
    text: caption || '',
    msg_type: 'video',
    wa_msg_id: mediaId || null,
    raw: { sent: true, media: { id: mediaId, filename: `out-${mediaId}.mp4` } },
  });
}

async function sendCourseMediaDirect(phone, course, opts = {}) {
  if (!course) return { ok: false, reason: 'no_course' };

  const mediaLink = String(course.link || '').trim();
  if (!mediaLink) {
    return { ok: false, reason: 'missing_link' };
  }

  let tmpPath = '';
  try {
    const downloaded = await downloadSharedMediaFromLink(mediaLink, 'course');
    tmpPath = downloaded.tmpPath;
    const mimeType = normalizeSharedMediaMimeType(downloaded.mimeType || '', mediaLink);
    const mediaId = await uploadMediaToWhatsApp(tmpPath, mimeType);

    try {
      const savedName = `out-${mediaId}${mediaExtFromMime(mimeType)}`;
      fs.copyFileSync(tmpPath, path.join(MEDIA_DIR, savedName));
    } catch {}

    const caption = String(opts?.caption || '').trim() || [
      course.nombre || 'Curso',
      course.precio ? `Precio: ${moneyOrConsult(course.precio)}` : '',
      course.fechaInicio ? `Inicio: ${course.fechaInicio}` : '',
    ].filter(Boolean).join(' | ');

    if (mimeType.startsWith('image/')) {
      await sendWhatsAppImageById(phone, mediaId, caption);
    } else if (mimeType.startsWith('video/')) {
      await sendWhatsAppVideoById(phone, mediaId, caption);
    } else {
      await sendWhatsAppDocumentById(phone, mediaId, path.basename(tmpPath), caption);
    }

    return { ok: true, mediaType: mimeType };
  } catch (e) {
    console.error('❌ Error enviando material del curso:', e?.response?.data || e?.message || e);
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
    await sendWhatsAppText(phone, 'Le paso también el material de los cursos 😊');
  }

  for (const course of limited) {
    const result = await sendCourseMediaDirect(phone, course);
    if (result.ok) {
      sentCount += 1;
    } else if (result.reason === 'missing_link') {
      missing.push(course.nombre || 'Curso');
    } else {
      failed.push(course.nombre || 'Curso');
    }
  }

  if (missing.length) {
    await sendWhatsAppText(phone, `No tengo el link del material cargado correctamente en la columna “Link” para: ${missing.join(', ')}.`);
  }

  if (failed.length && !sentCount) {
    await sendWhatsAppText(phone, 'No pude enviar el material de los cursos en este momento. Revise que el link de imagen o video esté accesible o compartido correctamente.');
  } else if (failed.length) {
    await sendWhatsAppText(phone, `No pude enviar algunos archivos de cursos ahora mismo: ${failed.join(', ')}.`);
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
    googleContacts: {
      enabled: hasGoogleContactsSyncEnabled(),
      targets: getGoogleContactsRuntimeSummary(),
    },
  });
});

// ===================== WEBHOOK =====================
app.post("/webhook", async (req, res) => {
  res.sendStatus(200);

  try {
    const value = req.body.entry?.[0]?.changes?.[0]?.value;
    const statuses = Array.isArray(value?.statuses) ? value.statuses : [];
    if (statuses.length) {
      for (const statusObj of statuses) {
        try {
          await updateBroadcastDeliveryFromWebhook(statusObj);
        } catch (statusErr) {
          console.error('❌ Error actualizando estado de difusión por webhook:', statusErr?.response?.data || statusErr?.message || statusErr);
        }
      }
    }

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

    const mergedTextRaw = String(mergedInbound.text || '').trim();
    const mergedIntentRaw = String(mergedInbound.userIntentText || userIntentText || mergedTextRaw).trim();
    text = stripSoftConversationPrefix(mergedTextRaw) || mergedTextRaw;
    userIntentText = stripSoftConversationPrefix(mergedIntentRaw) || mergedIntentRaw || text;
    mediaMeta = mergedInbound.mediaMeta || mediaMeta || null;
    contactInfoFromText = extractContactInfo(text);

    updateLastCloseContext(waId, {
      explicitName: contactInfoFromText?.nombre || lastCloseContext.get(waId)?.explicitName || '',
      lastUserText: text,
      profileName: name || lastCloseContext.get(waId)?.profileName || '',
    });


    const courseManagerHandled = await handleCourseManagerApprovalInbound({ msg, text, phone, phoneRaw });
    if (courseManagerHandled) return;

    const stylistHandled = await handleStylistWorkflowInbound({ msg, text, phone, phoneRaw });
    if (stylistHandled) return;

    const pendingNameReq = getPendingContactNameRequest(waId);
    const inboundName = await resolveInboundExplicitName(text, {
      pendingNameRequest: pendingNameReq,
      pendingTopic: pendingNameReq?.deferredText || '',
      profileName: name,
    });
    const explicitNameAnswer = inboundName?.explicitName || '';

    if (pendingNameReq?.awaiting) {
      if (!inboundName?.hasName) {
        if (inboundName?.isRefusal) {
          const refusalMsg = buildContactAskNameReminderMessage(pendingNameReq?.deferredText || text);
          pushHistory(waId, "assistant", refusalMsg);
          await sendWhatsAppText(phone, refusalMsg);
          scheduleInactivityFollowUp(waId, phone);
          return;
        }

        const mergedDeferredText = mergeDeferredNameRequestText(
          pendingNameReq?.deferredText || '',
          text
        );
        const mergedDeferredUserIntentText = mergeDeferredNameRequestText(
          pendingNameReq?.deferredUserIntentText || pendingNameReq?.deferredText || '',
          userIntentText || text
        );

        setPendingContactNameRequest(waId, {
          awaiting: true,
          phoneRaw,
          profileName: name,
          deferredText: mergedDeferredText || pendingNameReq?.deferredText || '',
          deferredUserIntentText: mergedDeferredUserIntentText || pendingNameReq?.deferredUserIntentText || '',
          deferredMediaMeta: pendingNameReq?.deferredMediaMeta || mediaMeta || null,
          reminderCount: Number(pendingNameReq?.reminderCount || 0) + 1,
        });

        const askAgain = buildContactAskNameReminderMessage(mergedDeferredText || pendingNameReq?.deferredText || text);
        pushHistory(waId, "assistant", askAgain);
        await sendWhatsAppText(phone, askAgain);
        scheduleInactivityFollowUp(waId, phone);
        return;
      }

      const deferredText = String(pendingNameReq?.deferredText || '').trim();
      const deferredUserIntentText = String(pendingNameReq?.deferredUserIntentText || deferredText || '').trim();
      const deferredMediaMeta = pendingNameReq?.deferredMediaMeta || null;
      const remainingAfterName = String(inboundName?.remainingText || '').trim();
      const resumedText = mergeDeferredNameRequestText(deferredText, remainingAfterName);
      const resumedUserIntentText = mergeDeferredNameRequestText(deferredUserIntentText, remainingAfterName);

      await syncIdentityEverywhere({ waId, phoneRaw, profileName: name, explicitName: explicitNameAnswer });
      updateLastCloseContext(waId, { explicitName: explicitNameAnswer, profileName: name || explicitNameAnswer });
      clearPendingContactNameRequest(waId);

      if (resumedText) {
        setOneShotReplyPrefix(phone, buildContactNameUpdatedMessage(explicitNameAnswer, { resumeOriginalRequest: true }));
        text = resumedText;
        userIntentText = resumedUserIntentText || resumedText;
        mediaMeta = deferredMediaMeta || mediaMeta || null;
        contactInfoFromText = extractContactInfo(text);
        updateLastCloseContext(waId, {
          explicitName: explicitNameAnswer,
          lastUserText: text,
          profileName: name || explicitNameAnswer,
        });
      } else {
        const msgNameOk = buildContactNameUpdatedMessage(explicitNameAnswer);
        pushHistory(waId, "assistant", msgNameOk);
        await sendWhatsAppText(phone, msgNameOk);
        scheduleInactivityFollowUp(waId, phone);
        return;
      }
    } else if (explicitNameAnswer && !isLikelyGenericContactName(explicitNameAnswer)) {
      await syncIdentityEverywhere({ waId, phoneRaw, profileName: name, explicitName: explicitNameAnswer });
      updateLastCloseContext(waId, { explicitName: explicitNameAnswer, profileName: name || explicitNameAnswer });
    }

    pushHistory(waId, "user", text);

    const activeOfferForReview = getActiveAssistantOffer(waId);
    if (activeOfferForReview && !looksLikeDifferentTopicThanActiveBroadcast(text, activeOfferForReview) && (looksLikeActiveOfferFollowup(text) || isAmbiguousBroadcastFollowup(text))) {
      const forcedResolution = await resolveReplyToActiveAssistantOfferWithAI(text, {
        activeOffer: activeOfferForReview,
        force: true,
        lastServiceName: getLastKnownService(waId, await getAppointmentDraft(waId))?.nombre || '',
        lastCourseName: getLastCourseContext(waId)?.selectedName || getLastCourseContext(waId)?.query || '',
        historySnippet: buildConversationHistorySnippet(ensureConv(waId).messages || [], 12, 1400),
      });
      if (forcedResolution?.action === 'CONTINUE_ACTIVE_OFFER') {
        const syntheticEarly = buildSyntheticTextFromActiveOfferResolution(forcedResolution, activeOfferForReview);
        if (syntheticEarly) {
          text = syntheticEarly;
          userIntentText = syntheticEarly;
          updateLastCloseContext(waId, { lastUserText: syntheticEarly });
          if (String(activeOfferForReview?.type || '').toUpperCase() === 'COURSE') {
            const seededCourses = (activeOfferForReview.items || []).map((nombre) => ({ nombre }));
            setLastCourseContext(waId, {
              query: activeOfferForReview.selectedName || activeOfferForReview.items?.[0] || 'cursos',
              selectedName: activeOfferForReview.selectedName || activeOfferForReview.items?.[0] || '',
              currentCourseName: activeOfferForReview.selectedName || activeOfferForReview.items?.[0] || '',
              lastOptions: normalizeActiveOfferItems(activeOfferForReview.items || []).slice(0, 10),
              recentCourses: mergeCourseContextRows(seededCourses, getLastCourseContext(waId)?.recentCourses || []),
              requestedInterest: buildHubSpotCourseInterestLabel(activeOfferForReview.selectedName || activeOfferForReview.items?.[0] || 'cursos'),
            });
            clearProductMemory(waId);
          }
        }
      }
    }

    const firstAiReview = await reviewInboundMessageFirstWithAI(text, {
      activeAssistantOfferType: activeOfferForReview?.type || '',
      activeAssistantOfferDomain: activeOfferForReview?.domain || '',
      activeAssistantOfferFamily: activeOfferForReview?.family || '',
      activeAssistantOfferItems: Array.isArray(activeOfferForReview?.items) ? activeOfferForReview.items.slice(0, 12) : [],
      activeAssistantOfferSelectedName: activeOfferForReview?.selectedName || '',
      hasDraft: !!(await getAppointmentDraft(waId)),
      pendingDraft: await getAppointmentDraft(waId),
      flowStep: (await getAppointmentDraft(waId))?.flow_step || '',
      lastService: getLastKnownService(waId, await getAppointmentDraft(waId)),
      lastServiceName: getLastKnownService(waId, await getAppointmentDraft(waId))?.nombre || '',
      lastProductName: lastProductByUser.get(waId)?.nombre || '',
      lastCourseContext: getLastCourseContext(waId),
      lastCourseName: getLastCourseContext(waId)?.selectedName || getLastCourseContext(waId)?.query || '',
      hasCourseContext: !!getLastCourseContext(waId),
      historySnippet: buildConversationHistorySnippet(ensureConv(waId).messages || [], 12, 1600),
    });

    if (firstAiReview?.topic_changed) {
      if (firstAiReview?.should_clear_active_offer) clearActiveAssistantOffer(waId);
      if (firstAiReview?.type !== 'PRODUCT') clearProductMemory(waId);
      if (firstAiReview?.type !== 'COURSE') clearLastCourseContext(waId);
      if (firstAiReview?.type !== 'SERVICE') lastServiceByUser.delete(waId);
    }

    if (!pendingNameReq?.awaiting && shouldRunConversationWrapUpAI(text, waId)) {
      const wrapUpControl = await classifyConversationWrapUpWithAI(text, {
        lastAssistantMessage: getLastAssistantMessage(waId)?.content || '',
        pendingAppointment: !!(await getAppointmentDraft(waId)),
        pendingCourseEnrollment: !!(await getCourseEnrollmentDraft(waId)),
        hasProductContext: !!getLastProductContext(waId),
        hasBeautyContext: !!getLastResolvedBeauty(waId) || !!getPendingAmbiguousBeauty(waId) || !!getLastKnownService(waId, null),
        hasCourseContext: !!getLastCourseContext(waId),
        historySnippet: buildConversationHistorySnippet(ensureConv(waId).messages || [], 14, 1800),
      });

      if (wrapUpControl.action === 'CLOSE_POLITELY') {
        clearProductMemory(waId);
        clearActiveAssistantOffer(waId);
        clearPendingAmbiguousBeauty(waId);
        clearLastResolvedBeauty(waId);
        clearLastCourseContext(waId);
        lastServiceByUser.delete(waId);

        try { await deleteAppointmentDraft(waId); } catch {}
        try { await deleteCourseEnrollmentDraft(waId); } catch {}
        if (inactivityTimers.has(waId)) {
          clearTimeout(inactivityTimers.get(waId));
          inactivityTimers.delete(waId);
        }
        if (closeTimers.has(waId)) {
          clearTimeout(closeTimers.get(waId));
          closeTimers.delete(waId);
        }

        const msgWrapUp = buildPoliteConversationCloseMessage(wrapUpControl);
        pushHistory(waId, "assistant", msgWrapUp);
        await sendWhatsAppText(phone, msgWrapUp);
        updateLastCloseContext(waId, { lastUserText: text, suppressInactivityPrompt: true });
        await logConversationClose(waId);
        return;
      }
    }

    const knownIdentity = await resolveKnownContactIdentity({ waId, phoneRaw, profileName: name });
    if (shouldAskContactNameFirst({ waId, text, explicitName: explicitNameAnswer, pendingNameRequest: pendingNameReq })) {
      const deferredText = mergeDeferredNameRequestText(text, '');
      setPendingContactNameRequest(waId, {
        awaiting: true,
        phoneRaw,
        profileName: name,
        deferredText,
        deferredUserIntentText: mergeDeferredNameRequestText(userIntentText || text, ''),
        deferredMediaMeta: mediaMeta || null,
        reminderCount: 0,
      });
      const askNameMsg = buildContactAskNameMessage(deferredText || text);
      pushHistory(waId, "assistant", askNameMsg);
      await sendWhatsAppText(phone, askNameMsg);
      scheduleInactivityFollowUp(waId, phone);
      return;
    }

    if (!pendingNameReq?.awaiting && knownIdentity?.shouldAskName && !explicitNameAnswer) {
      const deferredText = mergeDeferredNameRequestText(text, '');
      setPendingContactNameRequest(waId, {
        awaiting: true,
        phoneRaw,
        profileName: name,
        deferredText,
        deferredUserIntentText: mergeDeferredNameRequestText(userIntentText || text, ''),
        deferredMediaMeta: mediaMeta || null,
        reminderCount: 0,
      });
      const askNameMsg = buildContactAskNameMessage(deferredText || text);
      pushHistory(waId, "assistant", askNameMsg);
      await sendWhatsAppText(phone, askNameMsg);
      scheduleInactivityFollowUp(waId, phone);
      return;
    }

    if (isGreetingOnly(text)) {
      const greetingReply = hasRecentConversationContext(waId)
        ? buildContextualGreetingReply(waId)
        : '¡Hola! 😊 ¿En qué puedo ayudarte?';
      pushHistory(waId, 'assistant', greetingReply);
      await sendWhatsAppText(phone, greetingReply);
      scheduleInactivityFollowUp(waId, phone);
      return;
    }

    if (shouldUseAmbiguousBridgeMessage({ waId, text })) {
      pushHistory(waId, "assistant", AMBIGUOUS_BRIDGE_MESSAGE);
      await sendWhatsAppText(phone, AMBIGUOUS_BRIDGE_MESSAGE);
      scheduleInactivityFollowUp(waId, phone);
      return;
    }

    // ✅ Si el cliente frena, posterga o cambia de tema en medio del flujo, resolvemos eso antes de seguir.
    // Precompute pending appointment draft and course draft.  We need these
    // variables early because looksLikeCourseFlowSignal uses pendingCourseDraft.
    let pendingDraft = await getAppointmentDraft(waId);
    let pendingCourseDraft = await getCourseEnrollmentDraft(waId);
    // Detect whether the current message likely belongs to a course flow.  We call this
    // before referencing quickCourseFlow to avoid a temporal dead zone.
    const quickCourseFlowEarly = looksLikeCourseFlowSignal(text, {
      lastCourseContext: getLastCourseContext(waId),
      pendingCourseDraft,
    });
    if (pendingDraft && !quickCourseFlowEarly) {
      const draftControlEarly = await classifyAppointmentDraftControl(text, {
        serviceName: pendingDraft?.servicio || pendingDraft?.last_service_name || '',
        date: pendingDraft?.fecha || '',
        time: pendingDraft?.hora || '',
        flowStep: pendingDraft?.flow_step || '',
        historySnippet: buildConversationHistorySnippet(ensureConv(waId).messages || [], 10, 1200),
      });

      if (draftControlEarly.action === 'PAUSE_APPOINTMENT' || draftControlEarly.action === 'SWITCH_TOPIC') {
        await deleteAppointmentDraft(waId);

        const pauseIntentEarly = extractTurnoPauseIntent(text);
        const shouldPauseOnly = draftControlEarly.action === 'PAUSE_APPOINTMENT';
        const nextTextEarly = pauseIntentEarly.matched ? pauseIntentEarly.remainder : text;

        if (shouldPauseOnly) {
          const msgPauseTurno = `Perfecto 😊

No hay problema. Frené la gestión del turno por ahora.

Cuando quiera retomarlo, me escribe y le paso nuevamente los horarios disponibles.`;
          pushHistory(waId, "assistant", msgPauseTurno);
          await sendWhatsAppText(phone, msgPauseTurno);
          scheduleInactivityFollowUp(waId, phone);
          return;
        }

        text = String(nextTextEarly || '').trim() || String(text || '').trim();
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

    await upsertCommercialFollowupCandidate(waId);

    const activeAssistantOffer = getActiveAssistantOffer(waId);
    const shouldReviewActiveOffer = !!(
      activeAssistantOffer
      && (
        firstAiReview?.follows_active_offer
        || (!firstAiReview?.topic_changed && (firstAiReview?.type === 'OTHER' || !firstAiReview?.type))
      )
    );

    const activeOfferResolution = shouldReviewActiveOffer
      ? await resolveReplyToActiveAssistantOfferWithAI(text, {
          activeOffer: activeAssistantOffer,
          lastServiceName: getLastKnownService(waId, await getAppointmentDraft(waId))?.nombre || '',
          lastCourseName: getLastCourseContext(waId)?.selectedName || getLastCourseContext(waId)?.query || '',
          historySnippet: buildConversationHistorySnippet(ensureConv(waId).messages || [], 12, 1400),
        })
      : (firstAiReview?.topic_changed ? { action: 'SWITCH_TOPIC' } : null);

    if (activeOfferResolution?.action === 'CONTINUE_ACTIVE_OFFER') {
      if (activeOfferResolution.target_type === 'PRODUCT') {
        const stockForActiveOffer = filterSellableCatalogRows(await getStockCatalog(), { includeOutOfStock: false });
        const activeProductNames = normalizeActiveOfferItems(
          activeOfferResolution.wants_all_items
            ? (activeAssistantOffer?.items || [])
            : [
                activeOfferResolution.target_name || '',
                activeAssistantOffer?.selectedName || '',
                activeAssistantOffer?.items?.[0] || '',
              ]
        );
        const activeProducts = resolveProductsByNames(stockForActiveOffer, activeProductNames);
        if (activeProducts.length) {
          setLastProductContext(waId, {
            domain: activeAssistantOffer?.domain || '',
            family: activeAssistantOffer?.family || '',
            focusTerm: activeAssistantOffer?.focusTerm || '',
            mode: activeProducts.length > 1 ? 'list' : 'detail',
            lastOptions: activeProducts.map((x) => x.nombre).slice(0, 10),
          });
          if (activeProducts.length === 1) lastProductByUser.set(waId, activeProducts[0]);

          if (activeOfferResolution.goal === 'PHOTO') {
            const sent = activeOfferResolution.wants_all_items || activeProducts.length > 1
              ? await maybeSendMultipleProductPhotos(phone, activeProducts, userIntentText || text)
              : await maybeSendProductPhoto(phone, activeProducts[0], userIntentText || text);
            if (sent) {
              scheduleInactivityFollowUp(waId, phone);
              return;
            }
          }
        }
      }

      const syntheticTextFromActiveOffer = buildSyntheticTextFromActiveOfferResolution(activeOfferResolution, activeAssistantOffer);
      if (syntheticTextFromActiveOffer) {
        text = syntheticTextFromActiveOffer;
        userIntentText = syntheticTextFromActiveOffer;
        updateLastCloseContext(waId, { lastUserText: syntheticTextFromActiveOffer });
      }
    } else if (activeOfferResolution?.action === 'SWITCH_TOPIC') {
      clearActiveAssistantOffer(waId);
    }

    // Si piden foto sin decir cuál: priorizar la última oferta activa, luego el contexto de producto y por último el último producto puntual.
    if (userAsksForPhoto(userIntentText)) {
      const stockForPhotos = filterSellableCatalogRows(await getStockCatalog(), { includeOutOfStock: false });
      const activeOfferForPhotos = getActiveAssistantOffer(waId);
      const lastCtxForPhotos = getLastProductContext(waId);

      if (activeOfferForPhotos?.type === 'PRODUCT' && Array.isArray(activeOfferForPhotos.items) && activeOfferForPhotos.items.length) {
        const activeOfferProducts = resolveProductsByNames(stockForPhotos, activeOfferForPhotos.items);
        if (activeOfferProducts.length) {
          const sentFromActiveOffer = (userAsksForAllPhotos(userIntentText) || activeOfferProducts.length > 1)
            ? await maybeSendMultipleProductPhotos(phone, activeOfferProducts, userIntentText)
            : await maybeSendProductPhoto(phone, activeOfferProducts[0], userIntentText);
          if (sentFromActiveOffer) {
            scheduleInactivityFollowUp(waId, phone);
            return;
          }
        }
      }

      if (lastCtxForPhotos?.lastOptions?.length) {
        const optionProducts = resolveProductsByNames(stockForPhotos, lastCtxForPhotos.lastOptions);
        if (optionProducts.length) {
          const sentFromContext = (userAsksForAllPhotos(userIntentText) || optionProducts.length > 1)
            ? await maybeSendMultipleProductPhotos(phone, optionProducts, userIntentText)
            : await maybeSendProductPhoto(phone, optionProducts[0], userIntentText);
          if (sentFromContext) {
            scheduleInactivityFollowUp(waId, phone);
            return;
          }
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
    const explicitCourseIntentEarly = detectCourseIntentFromContext(text, { lastCourseContext: activeCourseContextEarly });
    const shouldSkipBarberWalkInRule = (
      !!explicitCourseIntentEarly?.isCourse
      || (!!activeCourseContextEarly && looksLikeCourseFollowUp(text))
      || firstAiReview?.type === 'PRODUCT'
    );

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
    // Reuse previously-computed pendingDraft and pendingCourseDraft if available.  If
    // they are undefined (e.g. because the early pause logic didn't run), compute
    // them now.  Avoid re-declaring with `let` to prevent shadowing.
    pendingDraft = pendingDraft || await getAppointmentDraft(waId);
    pendingCourseDraft = pendingCourseDraft || await getCourseEnrollmentDraft(waId);
    let pendingStylistSuggestion = getPendingStylistSuggestion(waId);
    // Reuse the early quickCourseFlow detection if defined; otherwise compute it here.
    const quickCourseFlow = (typeof quickCourseFlowEarly !== 'undefined') ? quickCourseFlowEarly : looksLikeCourseFlowSignal(text, {
      lastCourseContext: getLastCourseContext(waId),
      pendingCourseDraft,
    });

    if (!pendingDraft && pendingStylistSuggestion && !quickCourseFlow) {
      if (isWarmAffirmativeReply(text)) {
        const updatedSuggestion = await updateAppointmentStatus(pendingStylistSuggestion.appointment_id, {
          status: 'awaiting_payment',
          payment_status: 'not_paid',
          appointment_date: pendingStylistSuggestion.fecha || undefined,
          appointment_time: pendingStylistSuggestion.hora || undefined,
          duration_min: Number(pendingStylistSuggestion.duracion_min || 60) || 60,
          client_name: pendingStylistSuggestion.cliente_full || '',
          contact_phone: normalizePhone(pendingStylistSuggestion.telefono_contacto || phone || ''),
          service_name: pendingStylistSuggestion.servicio || '',
          availability_mode: normalizeAvailabilityMode(pendingStylistSuggestion.availability_mode || 'commercial'),
          stylist_decision_note: pendingStylistSuggestion.stylist_note || 'Horario alternativo aceptado por cliente',
        });
        const suggestionRow = updatedSuggestion || await getAppointmentById(pendingStylistSuggestion.appointment_id);
        if (suggestionRow) {
          const suggestionDraft = mapAppointmentRowToDraft(suggestionRow);
          await saveAppointmentDraft(waId, phone, suggestionDraft);
          clearPendingStylistSuggestion(waId);
          pendingStylistSuggestion = null;
          pendingDraft = suggestionDraft;
          const msgPaySuggestion = buildAppointmentPaymentMessageFromRow(suggestionRow);
          pushHistory(waId, 'assistant', msgPaySuggestion);
          await sendWhatsAppText(phone, msgPaySuggestion);
          updateLastCloseContext(waId, { suppressInactivityPrompt: true });
          scheduleInactivityFollowUp(waId, phone);
          return;
        }
      }

      if (/^(no|no me sirve|no puedo|prefiero otro|otro horario|busquemos otro|busco otro)$/i.test(normalizeShortReply(text || ''))) {
        clearPendingStylistSuggestion(waId);
        pendingStylistSuggestion = null;
      }
    }

    if (!pendingDraft && !quickCourseFlow) {
      const latestPendingStylist = await getLatestAppointmentForClient({
        waId,
        phone,
        statuses: ['pending_stylist_confirmation', 'awaiting_payment'],
      });
      if (latestPendingStylist && (looksLikePaymentIntentOnly(text) || looksLikeProofAlreadySent(text) || mediaMeta)) {
        const waitingMsg = latestPendingStylist.status === 'awaiting_payment'
          ? buildAppointmentPaymentMessageFromRow(latestPendingStylist)
          : `Todavía no le pedí la seña porque primero estoy esperando la confirmación de la estilista 😊

Apenas ella me diga que puede, le paso por aquí los datos para la transferencia.`;
        pushHistory(waId, 'assistant', waitingMsg);
        await sendWhatsAppText(phone, waitingMsg);
        updateLastCloseContext(waId, { suppressInactivityPrompt: true });
        scheduleInactivityFollowUp(waId, phone);
        return;
      }
    }

    if (pendingDraft && textRequestsSiestaAvailability(text) && normalizeAvailabilityMode(pendingDraft.availability_mode || 'commercial') !== 'siesta') {
      const siestaDraft = {
        ...pendingDraft,
        availability_mode: 'siesta',
        flow_step: pendingDraft?.fecha ? (pendingDraft?.hora ? inferDraftFlowStep({ ...pendingDraft, availability_mode: 'siesta' }) : 'awaiting_time') : 'awaiting_date',
        last_intent: 'book_appointment',
        last_service_name: pendingDraft?.servicio || pendingDraft?.last_service_name || '',
      };
      await saveAppointmentDraft(waId, phone, siestaDraft);
      pendingDraft = siestaDraft;
      const siestaMsg = pendingDraft?.fecha
        ? await buildDateAvailabilityMessage({
            dateYMD: pendingDraft.fecha,
            servicio: pendingDraft.servicio || pendingDraft.last_service_name || '',
            durationMin: Number(pendingDraft.duracion_min || 60) || 60,
            availabilityMode: 'siesta',
          })
        : await buildWeeklyAvailabilityMessage({
            servicio: pendingDraft.servicio || pendingDraft.last_service_name || '',
            durationMin: Number(pendingDraft.duracion_min || 60) || 60,
            limitDays: 6,
            availabilityMode: 'siesta',
          });
      pushHistory(waId, 'assistant', siestaMsg);
      await sendWhatsAppText(phone, siestaMsg);
      updateLastCloseContext(waId, { suppressInactivityPrompt: true });
      scheduleInactivityFollowUp(waId, phone);
      return;
    }

    const recentDbMessages = await getRecentDbMessages(phone, 12);
    const convForAI = mergeConversationForAI(recentDbMessages, ensureConv(waId).messages || []);

    if (pendingCourseDraft) {
      await clearAppointmentStateForCourseFlow({
        waId,
        phone,
        reason: 'Se mantuvo activo el flujo de inscripción a curso y se limpiaron estados de turnos pendientes.',
      });
      pendingDraft = null;
      pendingStylistSuggestion = null;
      const courseDraftIntent = await extractCourseEnrollmentIntentWithAI(text, {
        hasDraft: true,
        currentCourseName: pendingCourseDraft?.curso_nombre || '',
        historySnippet: buildConversationHistorySnippet(convForAI, 14, 1200),
      });

      if (courseDraftIntent.action === 'PAUSE') {
        await deleteCourseEnrollmentDraft(waId);
        pendingCourseDraft = null;
        const msgPauseCourse = `Perfecto 😊

Frené la inscripción al curso por ahora.

Cuando quiera retomarla, me escribe y seguimos desde ahí.`;
        pushHistory(waId, 'assistant', msgPauseCourse);
        await sendWhatsAppText(phone, msgPauseCourse);
        scheduleInactivityFollowUp(waId, phone);
        return;
      }

      if (!looksLikeAppointmentIntent(text, { pendingDraft, lastService: getLastKnownService(waId, pendingDraft) }) && !isExplicitProductIntent(text) && !isExplicitServiceIntent(text)) {
        if (isLikelyGenericCourseListQuery(text) || /(otros cursos|que otros cursos|qué otros cursos|que cursos hay|qué cursos hay|que cursos estan dictando|qué cursos están dictando|que cursos tenes|qué cursos tenés|que cursos tienen|qué cursos tienen)/i.test(text || '')) {
          await deleteCourseEnrollmentDraft(waId);
          pendingCourseDraft = null;
        } else {
        const courses = await getCoursesCatalog();
        const courseCtxForDraft = {
          ...(getLastCourseContext(waId) || {}),
          selectedName: pendingCourseDraft?.curso_nombre || getLastCourseContext(waId)?.selectedName || '',
          currentCourseName: pendingCourseDraft?.curso_nombre || getLastCourseContext(waId)?.currentCourseName || '',
        };
        const referencedCourse = resolveCourseFromConversationContext(courses, text, courseCtxForDraft)
          || findCourseByContextName(courses, pendingCourseDraft?.curso_nombre || '');
        const draftFollowupGoal = detectCourseFollowupGoal(text);
        const draftLooksLikeNormalCourseQuestion = !!referencedCourse
          && !!draftFollowupGoal
          && !['START_SIGNUP', 'CONTINUE_SIGNUP', 'PAYMENT'].includes(courseDraftIntent.action || '');
        if (draftLooksLikeNormalCourseQuestion) {
          const naturalCourseReply = formatNaturalCourseFollowupReply(referencedCourse, draftFollowupGoal || 'DETAIL');
          if (naturalCourseReply) {
            const rememberedCourses = mergeCourseContextRows([referencedCourse], Array.isArray(courseCtxForDraft?.recentCourses) ? courseCtxForDraft.recentCourses : []);
            setLastCourseContext(waId, {
              query: courseCtxForDraft?.query || referencedCourse?.nombre || 'cursos',
              selectedName: referencedCourse?.nombre || '',
              currentCourseName: referencedCourse?.nombre || '',
              lastOptions: rememberedCourses.map((c) => c.nombre).filter(Boolean).slice(0, 10),
              recentCourses: rememberedCourses,
              requestedInterest: buildHubSpotCourseInterestLabel(referencedCourse?.nombre || referencedCourse?.categoria || ''),
            });
            updateLastCloseContext(waId, {
              intentType: 'COURSE',
              interest: buildHubSpotCourseInterestLabel(referencedCourse?.nombre || referencedCourse?.categoria || ''),
              lastUserText: text,
              suppressInactivityPrompt: true,
            });
            rememberAssistantCourseOffer(waId, [referencedCourse], { mode: 'DETAIL', selectedName: referencedCourse?.nombre || '', questionKind: draftFollowupGoal || 'DETAIL', lastAssistantText: naturalCourseReply });
            pushHistory(waId, 'assistant', naturalCourseReply);
            await sendWhatsAppText(phone, naturalCourseReply);
            scheduleInactivityFollowUp(waId, phone);
            return;
          }
        }

        let mergedCourseDraft = {
          ...pendingCourseDraft,
          curso_nombre: pendingCourseDraft?.curso_nombre || referencedCourse?.nombre || '',
          curso_categoria: pendingCourseDraft?.curso_categoria || referencedCourse?.categoria || '',
          telefono_contacto: normalizePhone(pendingCourseDraft?.telefono_contacto || phone || ''),
          last_intent: 'course_signup',
        };

        mergedCourseDraft = mergeStudentIntoCourseEnrollment({
          draft: mergedCourseDraft,
          text,
          waPhone: phone,
        });
        mergedCourseDraft = await tryApplyPaymentToCourseEnrollmentDraft(mergedCourseDraft, { text, mediaMeta });
        mergedCourseDraft.flow_step = inferCourseEnrollmentFlowStep(mergedCourseDraft);

        if (looksLikeCourseOnsitePaymentIntent(text) && mergedCourseDraft.alumno_nombre && mergedCourseDraft.alumno_dni) {
          const resultOnsiteCourse = await finalizeCourseEnrollmentOnsiteFlow({ waId, phone, draft: mergedCourseDraft });
          if (resultOnsiteCourse.type === 'onsite_cancelled') {
            const msgOnsite = buildCourseEnrollmentOnsitePendingMessage(mergedCourseDraft);
            pushHistory(waId, 'assistant', msgOnsite);
            await sendWhatsAppText(phone, msgOnsite);
            updateLastCloseContext(waId, {
              intentType: 'COURSE',
              interest: buildHubSpotCourseInterestLabel(mergedCourseDraft.curso_nombre || ''),
              suppressInactivityPrompt: false,
            });
            scheduleInactivityFollowUp(waId, phone);
            return;
          }
        }

        if (mergedCourseDraft.payment_status === 'paid_verified') {
          const resultCourse = await finalizeCourseEnrollmentFlow({ waId, phone, draft: mergedCourseDraft });
          if (resultCourse.type === 'reserved') {
            const msgReserved = buildCourseEnrollmentReservedMessage(mergedCourseDraft);
            const rememberedReservedCourses = mergeCourseContextRows([{ nombre: mergedCourseDraft.curso_nombre || '', categoria: mergedCourseDraft.curso_categoria || '' }], previousRecentCourses);
            setLastCourseContext(waId, {
              query: mergedCourseDraft.curso_nombre || lastCourseContext?.query || 'cursos',
              selectedName: mergedCourseDraft.curso_nombre || '',
              currentCourseName: mergedCourseDraft.curso_nombre || '',
              lastOptions: rememberedReservedCourses.map((c) => c.nombre).filter(Boolean).slice(0, 10),
              recentCourses: rememberedReservedCourses,
              requestedInterest: buildHubSpotCourseInterestLabel(mergedCourseDraft.curso_nombre || ''),
            });
            pushHistory(waId, 'assistant', msgReserved);
            await sendWhatsAppText(phone, msgReserved);
            updateLastCloseContext(waId, {
              intentType: 'COURSE',
              interest: buildHubSpotCourseInterestLabel(mergedCourseDraft.curso_nombre || ''),
              suppressInactivityPrompt: false,
            });
            scheduleInactivityFollowUp(waId, phone);
            return;
          }
        }

        await askForMissingCourseEnrollmentData({
          waId,
          phone,
          base: mergedCourseDraft,
          courseOptions: courses,
        });
        updateLastCloseContext(waId, {
          intentType: 'COURSE',
          interest: buildHubSpotCourseInterestLabel(mergedCourseDraft.curso_nombre || ''),
          suppressInactivityPrompt: true,
        });
        scheduleInactivityFollowUp(waId, phone);
        return;
        }
      }
    }

    if (pendingDraft) {
      const draftControl = await classifyAppointmentDraftControl(text, {
        serviceName: pendingDraft?.servicio || pendingDraft?.last_service_name || '',
        date: pendingDraft?.fecha || '',
        time: pendingDraft?.hora || '',
        flowStep: pendingDraft?.flow_step || '',
        historySnippet: buildConversationHistorySnippet(convForAI, 14, 1200),
      });

      if (draftControl.action === 'PAUSE_APPOINTMENT' || draftControl.action === 'SWITCH_TOPIC') {
        await deleteAppointmentDraft(waId);
        pendingDraft = null;

        const pauseIntent = extractTurnoPauseIntent(text);
        const shouldPauseOnly = draftControl.action === 'PAUSE_APPOINTMENT';
        const nextText = pauseIntent.matched ? pauseIntent.remainder : text;

        if (shouldPauseOnly) {
          const msgPauseTurno = `Perfecto 😊

No hay problema. Frené la gestión del turno por ahora.

Cuando quiera retomarlo, me escribe y le paso nuevamente los horarios disponibles.`;
          pushHistory(waId, "assistant", msgPauseTurno);
          await sendWhatsAppText(phone, msgPauseTurno);
          scheduleInactivityFollowUp(waId, phone);
          return;
        }

        text = String(nextText || '').trim() || String(text || '').trim();
        normTxt = normalize(text);
      }
    }

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
      setPendingAmbiguousBeauty(waId, { term: buildBeautyCanonicalLabel(ambiguousBeautyTerm) || ambiguousBeautyTerm });
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

    let beautyIntentOverride = null;
    const pendingAmbiguousBeauty = getPendingAmbiguousBeauty(waId);
    const lastResolvedBeauty = getLastResolvedBeauty(waId);

    if (shouldRunBeautyResolver(text, {
      pendingTerm: pendingAmbiguousBeauty?.term || '',
      lastResolved: lastResolvedBeauty,
    })) {
      const beautyResolution = await resolveAmbiguousBeautyIntentWithAI(text, {
        pendingTerm: pendingAmbiguousBeauty?.term || '',
        lastResolvedTerm: lastResolvedBeauty?.term || '',
        lastResolvedKind: lastResolvedBeauty?.kind || '',
        lastServiceName: lastKnownService?.nombre || '',
        historySnippet: buildConversationHistorySnippet(convForAI, 18, 1600),
      });

      const canonicalBeautyQuery = buildBeautyCanonicalLabel(
        beautyResolution.canonicalQuery ||
        pendingAmbiguousBeauty?.term ||
        extractAmbiguousBeautyTerm(text) ||
        lastResolvedBeauty?.term ||
        ''
      );

      if (beautyResolution.kind === 'SERVICE' && canonicalBeautyQuery) {
        const services = await getServicesCatalog();
        const resolvedService = resolveServiceCatalogMatch(services, canonicalBeautyQuery);
        const resolvedServiceName = resolvedService?.nombre || canonicalBeautyQuery;

        lastServiceByUser.set(waId, { nombre: resolvedServiceName, ts: Date.now() });
        clearPendingAmbiguousBeauty(waId);
        setLastResolvedBeauty(waId, { term: resolvedServiceName, kind: 'SERVICE' });

        if (beautyResolution.goal === 'BOOK_APPOINTMENT') {
          const baseTurno = await applyCatalogServiceDataToTurno({
            servicio: resolvedServiceName,
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
            last_service_name: resolvedServiceName,
          });
          await saveAppointmentDraft(waId, phone, baseTurno);
          await askForMissingTurnoData(baseTurno);
          return;
        }

        if (resolvedService) {
          if (beautyResolution.goal === 'DURATION') {
            const replyDuration = formatServicesReply([resolvedService], 'DETAIL', { showDuration: true, showDescription: true });
            if (replyDuration) {
              pushHistory(waId, 'assistant', replyDuration);
              await sendWhatsAppText(phone, replyDuration);
              scheduleInactivityFollowUp(waId, phone);
              return;
            }
          }

          if (beautyResolution.goal === 'PRICE' || beautyResolution.goal === 'DETAIL' || /^servicio$/i.test(normalize(text))) {
            const replyService = formatServicesReply([resolvedService], 'DETAIL');
            if (replyService) {
              pushHistory(waId, 'assistant', replyService);
              await sendWhatsAppText(phone, replyService);
              scheduleInactivityFollowUp(waId, phone);
              return;
            }
          }
        }

        beautyIntentOverride = { type: 'SERVICE', query: resolvedServiceName, mode: 'DETAIL' };
      } else if (beautyResolution.kind === 'PRODUCT' && canonicalBeautyQuery) {
        clearPendingAmbiguousBeauty(waId);
        setLastResolvedBeauty(waId, { term: canonicalBeautyQuery, kind: 'PRODUCT' });
        beautyIntentOverride = { type: 'PRODUCT', query: canonicalBeautyQuery, mode: beautyResolution.goal === 'LIST' ? 'LIST' : 'DETAIL' };
      }
    }

    async function askForMissingTurnoData(base) {
      const servicioTxt = base?.servicio || base?.last_service_name || "";
      const availabilityMode = normalizeAvailabilityMode(base?.availability_mode || 'commercial');

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
          availabilityMode,
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
          availabilityMode,
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
          availabilityMode,
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

      if (base?.appointment_id && base?.payment_status !== 'paid_verified') {
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
      if (!base?.appointment_id) {
        const msgBeforePayment = `Primero tengo que confirmar ese horario con la estilista. Apenas ella me diga que puede, ahí le paso los datos para la seña 😊`;
        pushHistory(waId, "assistant", msgBeforePayment);
        await sendWhatsAppText(phone, msgBeforePayment);
        updateLastCloseContext(waId, { suppressInactivityPrompt: true });
        scheduleInactivityFollowUp(waId, phone);
        return;
      }

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

      if (base.appointment_id && base.payment_status !== 'paid_verified') {
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
      if (result.type === "invalid_hour") {
        const modeTxt = normalizeAvailabilityMode(base?.availability_mode || 'commercial') === 'siesta'
          ? 'el horario especial de siesta'
          : 'los horarios comerciales';
        const resetHourBase = {
          ...base,
          hora: '',
          flow_step: 'awaiting_time',
          last_intent: 'book_appointment',
          last_service_name: base.servicio || base.last_service_name || '',
        };
        await saveAppointmentDraft(waId, phone, resetHourBase);
        updateLastCloseContext(waId, { suppressInactivityPrompt: true });
        const msgInvalidHour = `Ese horario no entra dentro de ${modeTxt} 😊

${await buildDateAvailabilityMessage({
          dateYMD: base.fecha,
          servicio: base?.servicio || base?.last_service_name || '',
          durationMin: Number(base?.duracion_min || 60) || 60,
          availabilityMode: normalizeAvailabilityMode(base?.availability_mode || 'commercial'),
        })}`;
        pushHistory(waId, 'assistant', msgInvalidHour);
        await sendWhatsAppText(phone, msgInvalidHour);
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
          availabilityMode: normalizeAvailabilityMode(base?.availability_mode || 'commercial'),
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
      if (result.type === "still_waiting_stylist") {
        const msgWait = `Todavía estoy esperando la confirmación de la estilista 😊

Apenas ella me diga que puede, ahí le paso los datos para la seña.`;
        pushHistory(waId, 'assistant', msgWait);
        await sendWhatsAppText(phone, msgWait);
        updateLastCloseContext(waId, { suppressInactivityPrompt: true });
        scheduleInactivityFollowUp(waId, phone);
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

Ahora consulto con la estilista ${TURNOS_STYLIST_NAME}. Si ella puede en ese horario, recién ahí le paso los datos para la seña.`.trim();
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
        appointment_id: pendingDraft?.appointment_id || null,
        availability_mode: textRequestsSiestaAvailability(text)
          ? 'siesta'
          : normalizeAvailabilityMode(pendingDraft?.availability_mode || 'commercial'),
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
      if (!pendingDraft) {
        await archiveConflictingAppointmentsForClient({
          waId,
          phone,
          reason: 'La clienta inició una nueva solicitud de turno y se cerró la anterior para evitar superposiciones.',
        });
      }
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
          appointment_id: pendingDraft?.appointment_id || null,
          availability_mode: textRequestsSiestaAvailability(text)
            ? 'siesta'
            : normalizeAvailabilityMode(pendingDraft?.availability_mode || 'commercial'),
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
      clearActiveAssistantOffer(waId);
      const msgCierreTurno = `¡Gracias a vos! 😊

Tu turno ya quedó registrado. Cualquier cosa, estoy acá ✨`;
      pushHistory(waId, "assistant", msgCierreTurno);
      await sendWhatsAppText(phone, msgCierreTurno);
      updateLastCloseContext(waId, { suppressInactivityPrompt: true });
      return;
    }

    if (isPoliteCatalogDecline(text) && lastAssistantLooksLikeCatalogMessage(waId)) {
      clearProductMemory(waId);
      clearActiveAssistantOffer(waId);
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
      rememberAssistantServiceOffer(waId, services.slice(0, 12), { mode: 'LIST', questionKind: 'LIST', lastAssistantText: parts.join('\n\n') });
      for (const part of parts) {
        pushHistory(waId, "assistant", part);
        await sendWhatsAppText(phone, part);
      }
      scheduleInactivityFollowUp(waId, phone);
      return;
    }

    if (!pendingDraft && !beautyIntentOverride && lastKnownService?.nombre && /^producto$/i.test(normalize(text))) {
      const stock = filterSellableCatalogRows(await getStockCatalog(), { includeOutOfStock: false });
      const matches = findStock(stock, lastKnownService.nombre, 'LIST');
      if (matches.length) {
        const replyCatalog = formatStockReply(matches, 'LIST', { domain: detectProductDomain(text), familyLabel: detectFurnitureFamily(text) || detectProductFamily(text) });
        rememberAssistantProductOffer(waId, matches, { mode: 'LIST', selectedName: matches.length === 1 ? (matches[0]?.nombre || '') : '', lastAssistantText: replyCatalog });
        pushHistory(waId, 'assistant', replyCatalog);
        await sendWhatsAppText(phone, replyCatalog);
        scheduleInactivityFollowUp(waId, phone);
        return;
      }
    }

    if (!pendingDraft && !beautyIntentOverride && lastKnownService?.nombre && /^servicio$/i.test(normalize(text))) {
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
        rememberAssistantServiceOffer(waId, durationMatches, { mode: 'DETAIL', selectedName: durationMatches[0]?.nombre || '', questionKind: 'DURATION', lastAssistantText: replyDuration });
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
        rememberAssistantServiceOffer(waId, priceMatches, { mode: 'DETAIL', selectedName: priceMatches[0]?.nombre || '', questionKind: 'PRICE', lastAssistantText: replyPrice });
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
          rememberAssistantServiceOffer(waId, implicitMatches, { mode: 'DETAIL', selectedName: implicitMatches[0]?.nombre || '', questionKind: wantsDuration ? 'DURATION' : 'DETAIL', lastAssistantText: replyImplicit });
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
      lastProductName: lastProductByUser.get(waId)?.nombre || '',
      hasProductContext: !!getLastProductContext(waId),
      productOptions: Array.isArray(getLastProductContext(waId)?.lastOptions) ? getLastProductContext(waId).lastOptions.slice(0, 10) : [],
      lastCourseName: lastCourseContext?.selectedName || lastCourseContext?.query || '',
      hasCourseContext: !!lastCourseContext,
      courseOptions: Array.isArray(lastCourseContext?.lastOptions) ? lastCourseContext.lastOptions.slice(0, 10) : [],
      activeAssistantOfferType: getActiveAssistantOffer(waId)?.type || '',
      activeAssistantOfferItems: Array.isArray(getActiveAssistantOffer(waId)?.items) ? getActiveAssistantOffer(waId).items.slice(0, 12) : [],
      activeAssistantOfferSelectedName: getActiveAssistantOffer(waId)?.selectedName || '',
      flowStep: pendingDraft?.flow_step || '',
      hasDraft: !!pendingDraft,
      historySnippet: buildConversationHistorySnippet(convForAI, 18, 1600),
    });


    if (beautyIntentOverride) {
      intent = {
        ...intent,
        type: beautyIntentOverride.type || intent.type,
        query: beautyIntentOverride.query || intent.query || '',
        mode: beautyIntentOverride.mode || intent.mode || 'DETAIL',
      };
    }

    if (firstAiReview?.type && firstAiReview.type !== 'OTHER') {
      intent = {
        ...intent,
        type: firstAiReview.type,
        query: firstAiReview.query || intent.query || '',
        mode: firstAiReview.mode || intent.mode || 'DETAIL',
      };
    }

    const explicitCourseIntent = detectCourseIntentFromContext(text, { lastCourseContext });
    if (explicitCourseIntent.isCourse && (!firstAiReview?.type || firstAiReview.type === 'COURSE' || firstAiReview.type === 'OTHER')) {
      intent = {
        ...intent,
        type: 'COURSE',
        query: explicitCourseIntent.query || intent.query || '',
        mode: explicitCourseIntent.mode || intent.mode || 'DETAIL',
      };
    }

    const fastIntent = detectFastCatalogIntent(text, {
      lastServiceName: lastKnownService?.nombre || '',
      lastCourseName: lastCourseContext?.selectedName || lastCourseContext?.query || '',
      hasCourseContext: !!lastCourseContext,
      hasDraft: !!pendingDraft,
      flowStep: pendingDraft?.flow_step || '',
    });

    if (fastIntent && (!firstAiReview?.type || firstAiReview.type === 'OTHER')) {
      const shouldOverride = (
        intent.type === 'OTHER'
        || (fastIntent.type === 'COURSE' && intent.type !== 'COURSE')
        || (fastIntent.type === 'PRODUCT' && intent.type === 'SERVICE' && !looksLikeAppointmentIntent(text, { pendingDraft, lastService: lastKnownService }))
      );

      if (shouldOverride) {
        intent = {
          ...intent,
          type: fastIntent.type,
          query: fastIntent.query || intent.query || '',
          mode: fastIntent.mode || intent.mode || 'DETAIL',
        };
      }
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
          lastDomain: lastProductCtx?.domain || '',
          historySnippet: buildConversationHistorySnippet(convForAI, 18, 2200),
        })
      : null;

    const shouldTreatAsProduct = (
      intent.type === 'PRODUCT'
      || (
        intent.type !== 'COURSE'
        && !looksLikeAppointmentIntent(text, {
          pendingDraft: pendingDraft || null,
          lastService: lastKnownService || null,
        })
        && (
          !!productAI?.is_product_query
          || looksLikeHairCareConsultation(text)
          || (!!lastProductCtx && ((lastProductCtx?.domain === 'furniture' && looksLikeFurniturePreferenceReply(text)) || (lastProductCtx?.domain !== 'furniture' && looksLikeProductPreferenceReply(text))))
        )
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
      const stock = filterSellableCatalogRows(await getStockCatalog(), { includeOutOfStock: false });
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

        const replyCatalog = formatStockReply(matches, 'DETAIL', { domain: detectProductDomain(text), familyLabel: detectFurnitureFamily(text) || detectProductFamily(text) });
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
      const rawStock = await getStockCatalog();
      const stockAll = filterSellableCatalogRows(rawStock, { includeOutOfStock: true });
      const stock = filterSellableCatalogRows(rawStock, { includeOutOfStock: false });
      const aiFamilyRaw = productAI?.family || '';
      const aiFamily = normalizeCatalogSearchText(aiFamilyRaw) === 'otro' ? '' : aiFamilyRaw;
      const aiSearchText = productAI?.specific_name || productAI?.search_text || '';
      const forcedDomainFromFirstAI = firstAiReview?.type === 'PRODUCT' ? (firstAiReview.product_domain || '') : '';
      const forcedFamilyFromFirstAI = firstAiReview?.type === 'PRODUCT' ? (firstAiReview.product_family || '') : '';
      const resolvedQuery = aiSearchText || firstAiReview?.query || intent.query || guessQueryFromText(text) || text;
      const resolvedDomain = forcedDomainFromFirstAI || productAI?.domain || detectProductDomain(aiSearchText || resolvedQuery || text, aiFamily || forcedFamilyFromFirstAI || lastProductCtx?.family || '') || lastProductCtx?.domain || '';
      const resolvedFamily = forcedFamilyFromFirstAI || aiFamily || (resolvedDomain === 'furniture' ? detectFurnitureFamily(resolvedQuery) || detectFurnitureFamily(text) : detectProductFamily(resolvedQuery) || detectProductFamily(text)) || lastProductCtx?.family || '';
      const resolvedFocusTerm = detectProductFocusTerm(aiSearchText || intent.query || text) || lastProductCtx?.focusTerm || '';
      const resolvedHairType = productAI?.hair_type || lastProductCtx?.hairType || '';
      const resolvedNeed = productAI?.need || productAI?.treatment_context || lastProductCtx?.need || '';
      const resolvedUseType = normalizeUseType(productAI?.use_type || productAI?.work_type || lastProductCtx?.useType || '');
      const resolvedBusinessType = productAI?.business_type || lastProductCtx?.businessType || '';
      const resolvedStyle = productAI?.style || lastProductCtx?.style || '';
      const resolvedSeatsNeeded = productAI?.seats_needed || lastProductCtx?.seatsNeeded || '';
      const treatmentKnowledge = resolvedDomain === 'hair'
        ? (getHairTreatmentKnowledgeById(lastProductCtx?.treatmentId || '') || detectHairTreatmentKnowledge({
            text: `${text} ${resolvedQuery} ${productAI?.treatment_context || ''}`,
            family: resolvedFamily,
            need: `${resolvedNeed} ${resolvedHairType}`.trim(),
          }))
        : null;
      const shouldAutoRecommendTreatment = !!(
        treatmentKnowledge &&
        resolvedDomain === 'hair' &&
        !productAI?.wants_photo
      );
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
        const stockBase = resolvedDomain === 'furniture' ? stock.filter((x) => detectRowProductDomain(x) === 'furniture') : stock.filter((x) => detectRowProductDomain(x) !== 'furniture');
        const parts = resolvedDomain === 'furniture'
          ? formatStockRelatedListAll(stockBase, { domain: resolvedDomain, familyLabel: resolvedFamily || 'equipamiento', chunkSize: 8 })
          : formatStockListAll(stockBase, 12);
        setLastProductContext(waId, {
          domain: resolvedDomain || '',
          family: resolvedFamily || '',
          focusTerm: resolvedFocusTerm || '',
          hairType: resolvedHairType || '',
          need: resolvedNeed || '',
          useType: resolvedUseType || '',
          businessType: resolvedBusinessType || '',
          style: resolvedStyle || '',
          seatsNeeded: resolvedSeatsNeeded || '',
          mode: 'list_all',
        });
        rememberAssistantProductOffer(waId, stockBase.slice(0, 12), { mode: 'LIST', domain: resolvedDomain || '', family: resolvedFamily || '', focusTerm: resolvedFocusTerm || '', questionKind: 'LIST', lastAssistantText: parts.join('\n\n') });
        for (const part of parts) {
          pushHistory(waId, 'assistant', part);
          await sendWhatsAppText(phone, part);
        }
        scheduleInactivityFollowUp(waId, phone);
        return;
      }

      let related = findStockRelated(stock, resolvedQuery, { domain: resolvedDomain, family: resolvedFamily, focusTerm: resolvedFocusTerm, limit: 200 });
      let broader = related.length ? related : findStockRelated(stock, text, { domain: resolvedDomain, family: resolvedFamily, focusTerm: resolvedFocusTerm, limit: 200 });
      const detailQuery = productAI?.specific_name || intent.query || guessQueryFromText(text);
      let matches = detailQuery ? findStock(stock, detailQuery, 'DETAIL') : [];

      if (resolvedFocusTerm && (isGenericProductOptionsFollowUp(text) || productMode === 'LIST' || !!productAI?.wants_all_related)) {
        related = filterRowsByProductFocus(related, resolvedFocusTerm);
        broader = filterRowsByProductFocus(broader, resolvedFocusTerm);
        matches = filterRowsByProductFocus(matches, resolvedFocusTerm);
      }

      matches = filterRowsForRequestedFamily(matches, { family: resolvedFamily, domain: resolvedDomain, query: `${detailQuery || ''} ${text || ''}` });
      related = filterRowsForRequestedFamily(related, { family: resolvedFamily, domain: resolvedDomain, query: `${resolvedQuery || ''} ${text || ''}` });
      broader = filterRowsForRequestedFamily(broader, { family: resolvedFamily, domain: resolvedDomain, query: `${resolvedQuery || ''} ${text || ''}` });

      const unavailableMatchesBase = detailQuery ? findStock(stockAll, detailQuery, 'DETAIL') : [];
      const unavailableMatches = filterRowsForRequestedFamily(unavailableMatchesBase, {
        family: resolvedFamily,
        domain: resolvedDomain,
        query: `${detailQuery || ''} ${text || ''}`,
      }).filter((row) => !isCatalogRowAvailable(row));

      const strictNoApprox = wantsStrictNoApproximation(`${resolvedQuery || ''} ${text || ''}`, resolvedFamily, resolvedDomain);
      const wantsPhotoOnly = !!productAI?.wants_photo || userAsksForPhoto(text);
      if (!matches.length && unavailableMatches.length && strictNoApprox) {
        const msgNoStock = buildOutOfStockCatalogMessage({
          domain: resolvedDomain,
          family: resolvedFamily,
          query: `${resolvedQuery || ''} ${text || ''}`,
        });
        pushHistory(waId, 'assistant', msgNoStock);
        await sendWhatsAppText(phone, msgNoStock);
        scheduleInactivityFollowUp(waId, phone);
        return;
      }

      if (!matches.length && !related.length && !broader.length && strictNoApprox) {
        const msgNoExact = buildNoExactCatalogMessage({
          domain: resolvedDomain,
          family: resolvedFamily,
          query: `${resolvedQuery || ''} ${text || ''}`,
          wantsPhoto: wantsPhotoOnly,
        });
        pushHistory(waId, 'assistant', msgNoExact);
        await sendWhatsAppText(phone, msgNoExact);
        scheduleInactivityFollowUp(waId, phone);
        return;
      }
      const wantsRecommendation = !!(
        productAI?.wants_recommendation ||
        resolvedHairType ||
        resolvedNeed ||
        resolvedUseType ||
        resolvedBusinessType ||
        resolvedStyle ||
        resolvedSeatsNeeded ||
        shouldAutoRecommendTreatment ||
        (lastProductCtx && ((resolvedDomain === 'furniture' && looksLikeFurniturePreferenceReply(text)) || (resolvedDomain !== 'furniture' && looksLikeProductPreferenceReply(text))))
      );

      const tryExceptionalPhotoReply = async (rows, { mode = 'LIST', selectedName = '', questionKind = 'LIST' } = {}) => {
        const summaryMode = mode === 'DETAIL' ? 'DETAIL' : 'LIST';
        const summaryText = formatStockReply(rows, summaryMode, { domain: resolvedDomain, familyLabel: resolvedFamily }) || '';
        rememberAssistantProductOffer(waId, rows, {
          mode,
          domain: resolvedDomain || '',
          family: resolvedFamily || '',
          focusTerm: resolvedFocusTerm || '',
          selectedName: selectedName || '',
          questionKind,
          lastAssistantText: summaryText,
        });
        if (summaryText) pushHistory(waId, 'assistant', summaryText);
        const photoResult = await trySendExceptionalProductPhotos(phone, rows, {
          domain: resolvedDomain,
          family: resolvedFamily,
          query: `${resolvedQuery || ''} ${text || ''}`,
        });
        if (photoResult.handled) {
          scheduleInactivityFollowUp(waId, phone);
          return true;
        }
        return false;
      };

      if (wantsRecommendation) {
        const treatmentPool = treatmentKnowledge
          ? buildTreatmentRecommendationPool(stock, {
              knowledge: treatmentKnowledge,
              family: resolvedFamily,
              query: resolvedQuery || text,
              useType: resolvedUseType,
              need: resolvedNeed,
              hairType: resolvedHairType,
            })
          : [];

        const treatingNeed = !!(
          resolvedDomain === 'hair'
          && (treatmentKnowledge || resolvedNeed || resolvedHairType)
          && !['plancha', 'secador'].includes(normalizeCatalogSearchText(resolvedFamily || ''))
        );

        const pool = (matches.length > 1 ? matches : [])
          .concat(treatmentPool)
          .concat(related)
          .concat(broader)
          .filter((row, idx, arr) =>
            arr.findIndex((x) =>
              normalizeCatalogSearchText(`${x?.nombre || ''} ${x?.marca || ''}`) ===
              normalizeCatalogSearchText(`${row?.nombre || ''} ${row?.marca || ''}`)
            ) === idx
          )
          .filter((row) => !treatingNeed || isHairCareRecommendationCandidate(row, { family: resolvedFamily }));

        const shortlist = shortlistProductsForRecommendation(pool.length ? pool : stock, {
          domain: resolvedDomain,
          family: resolvedFamily,
          focusTerm: resolvedFocusTerm,
          hairType: resolvedHairType,
          need: resolvedNeed,
          useType: resolvedUseType,
          businessType: resolvedBusinessType,
          style: resolvedStyle,
          seatsNeeded: resolvedSeatsNeeded,
          query: resolvedQuery || text,
          limit: 10,
        });

        if (shortlist.length) {
          const recoAI = await recommendProductsWithAI({
            text,
            domain: resolvedDomain,
            familyLabel: resolvedFamily,
            hairType: resolvedHairType,
            need: resolvedNeed,
            useType: resolvedUseType,
            businessType: resolvedBusinessType,
            style: resolvedStyle,
            seatsNeeded: resolvedSeatsNeeded,
            treatmentKnowledge,
            historySnippet: buildConversationHistorySnippet(convForAI, 18, 2200),
            products: shortlist,
          });

          const picked = recoAI?.recommended_names?.length
            ? shortlist.filter((row) =>
                recoAI.recommended_names.some((name) => normalizeCatalogSearchText(name) === normalizeCatalogSearchText(row.nombre))
              ).slice(0, 4)
            : shortlist.slice(0, 4);

          const replyReco = formatRecommendedProductsReply(recoAI, picked.length ? picked : shortlist.slice(0, 4), {
            domain: resolvedDomain,
            familyLabel: resolvedFamily,
            hairType: resolvedHairType,
            need: resolvedNeed,
            useType: resolvedUseType,
            businessType: resolvedBusinessType,
            style: resolvedStyle,
            seatsNeeded: resolvedSeatsNeeded,
            treatmentKnowledge,
          });

          if (replyReco) {
            setLastProductContext(waId, {
              domain: resolvedDomain || detectProductDomain(text, resolvedFamily) || '',
              family: resolvedFamily || (resolvedDomain === 'furniture' ? detectFurnitureFamily(text) : detectProductFamily(text)) || '',
              focusTerm: resolvedFocusTerm || '',
              hairType: resolvedHairType || '',
              need: resolvedNeed || '',
              useType: resolvedUseType || '',
              businessType: resolvedBusinessType || '',
              style: resolvedStyle || '',
              seatsNeeded: resolvedSeatsNeeded || '',
              treatmentId: treatmentKnowledge?.id || '',
              mode: 'recommendation',
              lastOptions: (picked.length ? picked : shortlist.slice(0, 4)).map((x) => x.nombre),
            });
            const pickedRows = picked.length ? picked : shortlist.slice(0, 4);
            rememberAssistantProductOffer(waId, pickedRows, { mode: 'recommendation', domain: resolvedDomain || detectProductDomain(text, resolvedFamily) || '', family: resolvedFamily || '', focusTerm: resolvedFocusTerm || '', questionKind: 'RECOMMENDATION', lastAssistantText: replyReco });
            if (pickedRows.length === 1) lastProductByUser.set(waId, pickedRows[0]);
            const exceptionalRecoHandled = await tryExceptionalPhotoReply(pickedRows, { mode: 'recommendation', selectedName: pickedRows.length === 1 ? (pickedRows[0]?.nombre || '') : '', questionKind: 'RECOMMENDATION' });
            if (exceptionalRecoHandled) {
              return;
            }
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
            domain: resolvedDomain || detectProductDomain(resolvedQuery, resolvedFamily) || '',
            family: resolvedFamily || (resolvedDomain === 'furniture' ? detectFurnitureFamily(resolvedQuery) : detectProductFamily(resolvedQuery)) || '',
            focusTerm: resolvedFocusTerm || '',
            hairType: resolvedHairType || '',
            need: resolvedNeed || '',
            useType: resolvedUseType || '',
            businessType: resolvedBusinessType || '',
            style: resolvedStyle || '',
            seatsNeeded: resolvedSeatsNeeded || '',
            treatmentId: treatmentKnowledge?.id || '',
            mode: 'list',
            lastOptions: relatedSlice.map((x) => x.nombre),
          });

          const exceptionalListHandled = await tryExceptionalPhotoReply(relatedSlice, { mode: 'LIST', questionKind: 'LIST' });
          if (exceptionalListHandled) {
            return;
          }

          if (userAsksForAllPhotos(text)) {
            const sentAll = await maybeSendMultipleProductPhotos(phone, relatedSlice, text);
            if (sentAll) {
              scheduleInactivityFollowUp(waId, phone);
              return;
            }
          }

          const parts = formatStockRelatedListAll(related, {
            domain: resolvedDomain,
            familyLabel: resolvedFamily || (resolvedDomain === 'furniture' ? detectFurnitureFamily(resolvedQuery) : detectProductFamily(resolvedQuery)),
            chunkSize: 8,
          });
          rememberAssistantProductOffer(waId, relatedSlice, { mode: 'LIST', domain: resolvedDomain || '', family: resolvedFamily || '', focusTerm: resolvedFocusTerm || '', questionKind: 'LIST', lastAssistantText: parts.join('\n\n') });
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
            domain: resolvedDomain || detectRowProductDomain(matches[0]) || '',
            family: resolvedFamily || ((resolvedDomain || detectRowProductDomain(matches[0])) === 'furniture' ? detectFurnitureFamily(matches[0].nombre) : detectProductFamily(matches[0].nombre)) || '',
            focusTerm: resolvedFocusTerm || detectProductFocusTerm(matches[0].nombre) || '',
            hairType: resolvedHairType || '',
            need: resolvedNeed || '',
            useType: resolvedUseType || '',
            businessType: resolvedBusinessType || '',
            style: resolvedStyle || '',
            seatsNeeded: resolvedSeatsNeeded || '',
            treatmentId: treatmentKnowledge?.id || '',
            mode: 'detail',
            lastOptions: [matches[0].nombre],
          });
        } else {
          setLastProductContext(waId, {
            domain: resolvedDomain || detectProductDomain(resolvedQuery, resolvedFamily) || '',
            family: resolvedFamily || (resolvedDomain === 'furniture' ? detectFurnitureFamily(resolvedQuery) : detectProductFamily(resolvedQuery)) || '',
            focusTerm: resolvedFocusTerm || '',
            hairType: resolvedHairType || '',
            need: resolvedNeed || '',
            useType: resolvedUseType || '',
            businessType: resolvedBusinessType || '',
            style: resolvedStyle || '',
            seatsNeeded: resolvedSeatsNeeded || '',
            treatmentId: treatmentKnowledge?.id || '',
            mode: 'list',
            lastOptions: matches.slice(0, 8).map((x) => x.nombre),
          });
        }

        const exceptionalMatchHandled = await tryExceptionalPhotoReply(matches.length === 1 ? [matches[0]] : matches.slice(0, 8), {
          mode: matches.length === 1 ? 'DETAIL' : 'LIST',
          selectedName: matches.length === 1 ? (matches[0]?.nombre || '') : '',
          questionKind: matches.length === 1 ? 'DETAIL' : 'LIST',
        });
        if (exceptionalMatchHandled) {
          return;
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

        const replyCatalog = formatStockReply(matches, matches.length === 1 ? 'DETAIL' : 'LIST', { domain: resolvedDomain, familyLabel: resolvedFamily });
        if (replyCatalog) {
          rememberAssistantProductOffer(waId, matches, { mode: matches.length === 1 ? 'DETAIL' : 'LIST', domain: resolvedDomain || '', family: resolvedFamily || '', focusTerm: resolvedFocusTerm || '', selectedName: matches.length === 1 ? (matches[0]?.nombre || '') : '', questionKind: matches.length === 1 ? 'DETAIL' : 'LIST', lastAssistantText: replyCatalog });
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
          treatmentId: treatmentKnowledge?.id || '',
          mode: 'list',
          lastOptions: broaderSlice.map((x) => x.nombre),
        });

        const exceptionalBroaderHandled = await tryExceptionalPhotoReply(broaderSlice, { mode: 'LIST', questionKind: 'LIST' });
        if (exceptionalBroaderHandled) {
          return;
        }

        if (userAsksForAllPhotos(text)) {
          const sentAll = await maybeSendMultipleProductPhotos(phone, broaderSlice, text);
          if (sentAll) {
            scheduleInactivityFollowUp(waId, phone);
            return;
          }
        }

        const parts = formatStockRelatedListAll(broader, {
          domain: resolvedDomain,
          familyLabel: resolvedFamily || (resolvedDomain === 'furniture' ? detectFurnitureFamily(resolvedQuery) : detectProductFamily(resolvedQuery)),
          chunkSize: 8,
        });
        rememberAssistantProductOffer(waId, broaderSlice, { mode: 'LIST', domain: resolvedDomain || '', family: resolvedFamily || '', focusTerm: resolvedFocusTerm || '', questionKind: 'LIST', lastAssistantText: parts.join('\n\n') });
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
        treatmentId: treatmentKnowledge?.id || '',
        mode: 'followup',
      });
      const strictNoFindMsg = wantsStrictNoApproximation(`${resolvedQuery || ''} ${text || ''}`, resolvedFamily, resolvedDomain)
        ? buildNoExactCatalogMessage({ domain: resolvedDomain, family: resolvedFamily, query: `${resolvedQuery || ''} ${text || ''}`, wantsPhoto: !!(productAI?.wants_photo || userAsksForPhoto(text)) })
        : `No lo encuentro así en el catálogo. ${resolvedDomain === 'furniture' ? 'Dígame qué mueble busca, si es para uso personal o para un salón/negocio y qué estilo le gustaría 😊' : 'Dígame la marca, para qué lo necesita o qué tipo de cabello tiene y le recomiendo mejor 😊'}`;
      await sendWhatsAppText(phone, strictNoFindMsg);
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

Si su consulta era por *cuidado capilar o productos*, también la puedo orientar por ahí 😊

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
      const previousRecentCourses = Array.isArray(lastCourseContext?.recentCourses) ? lastCourseContext.recentCourses : [];
      const referencedCourse = resolveCourseFromConversationContext(courses, text, lastCourseContext);
      const activeCourse = referencedCourse || findCourseByContextName(courses, lastCourseContext?.currentCourseName || lastCourseContext?.selectedName || '');
      const followupGoal = detectCourseFollowupGoal(text);
      const cleanedDirectFollowupText = sanitizeCourseSearchQuery(text);
      const isOnlyFollowupGoal = !cleanedDirectFollowupText && !!followupGoal;
      const isDirectCurrentCourseFollowup = !!activeCourse && isOnlyFollowupGoal;
      const isExplicitReferencedCourseFollowup = !!referencedCourse && !!followupGoal;
      const courseEnrollmentIntent = await extractCourseEnrollmentIntentWithAI(text, {
        hasDraft: !!pendingCourseDraft,
        currentCourseName: activeCourse?.nombre || referencedCourse?.nombre || lastCourseContext?.selectedName || '',
        recentCourseNames: Array.isArray(lastCourseContext?.recentCourses) ? lastCourseContext.recentCourses.map((x) => x?.nombre).filter(Boolean).slice(0, 12) : [],
        historySnippet: buildConversationHistorySnippet(convForAI, 14, 1200),
      });
      const isGenericCourseCatalogAsk = isLikelyGenericCourseListQuery(text) || /(otros cursos|que otros cursos|qué otros cursos|que cursos hay|qué cursos hay|que cursos estan dictando|qué cursos están dictando|que cursos tenes|qué cursos tenés|que cursos tienen|qué cursos tienen)/i.test(text || '');
      const hasExplicitCourseSignupSignal = followupGoal === 'SIGNUP' || courseEnrollmentIntent.action === 'START_SIGNUP';
      const continuesExistingCourseSignup = !!pendingCourseDraft && ['CONTINUE_SIGNUP', 'PAYMENT'].includes(courseEnrollmentIntent.action || '');
      const wantsCourseSignup = !isGenericCourseCatalogAsk && (hasExplicitCourseSignupSignal || continuesExistingCourseSignup);

      if (wantsCourseSignup) {
        await clearAppointmentStateForCourseFlow({
          waId,
          phone,
          reason: 'Se inició una inscripción a curso y se cerraron estados de turnos pendientes para evitar cruces.',
        });
        pendingDraft = null;
        pendingStylistSuggestion = null;

        const aiSignupSelection = await resolveCourseEnrollmentSelectionWithAI(courses, text, {
          currentCourseName: activeCourse?.nombre || referencedCourse?.nombre || lastCourseContext?.selectedName || '',
          recentCourses: Array.isArray(lastCourseContext?.recentCourses) ? lastCourseContext.recentCourses : [],
          historySnippet: buildConversationHistorySnippet(convForAI, 14, 1200),
        });
        const signupQuery = aiSignupSelection.course_query || courseEnrollmentIntent.course_query || activeCourse?.nombre || referencedCourse?.nombre || lastCourseContext?.selectedName || stripCourseSignupNoise(text) || text;
        const signupMatches = signupQuery ? findCourses(courses, signupQuery, 'DETAIL') : [];
        const signupCourse = aiSignupSelection.course || activeCourse || referencedCourse || signupMatches[0] || findCourseByContextName(courses, courseEnrollmentIntent.course_query || '') || null;
        const isFreshCourseSignup = courseEnrollmentIntent.action === 'START_SIGNUP';
        let baseCourseDraft = {
          ...(pendingCourseDraft || {}),
          curso_nombre: signupCourse?.nombre || pendingCourseDraft?.curso_nombre || '',
          curso_categoria: signupCourse?.categoria || pendingCourseDraft?.curso_categoria || '',
          alumno_nombre: isFreshCourseSignup ? '' : (pendingCourseDraft?.alumno_nombre || ''),
          alumno_dni: isFreshCourseSignup ? '' : (pendingCourseDraft?.alumno_dni || ''),
          telefono_contacto: normalizePhone(phone || pendingCourseDraft?.telefono_contacto || ''),
          payment_status: isFreshCourseSignup ? 'not_paid' : (pendingCourseDraft?.payment_status || 'not_paid'),
          payment_amount: isFreshCourseSignup ? null : (pendingCourseDraft?.payment_amount ?? null),
          payment_sender: isFreshCourseSignup ? '' : (pendingCourseDraft?.payment_sender || ''),
          payment_receiver: isFreshCourseSignup ? '' : (pendingCourseDraft?.payment_receiver || ''),
          payment_proof_text: isFreshCourseSignup ? '' : (pendingCourseDraft?.payment_proof_text || ''),
          payment_proof_media_id: isFreshCourseSignup ? '' : (pendingCourseDraft?.payment_proof_media_id || ''),
          payment_proof_filename: isFreshCourseSignup ? '' : (pendingCourseDraft?.payment_proof_filename || ''),
          last_intent: 'course_signup',
        };
        baseCourseDraft = mergeStudentIntoCourseEnrollment({
          draft: baseCourseDraft,
          text,
          waPhone: phone,
        });
        baseCourseDraft = await tryApplyPaymentToCourseEnrollmentDraft(baseCourseDraft, { text, mediaMeta });
        baseCourseDraft.flow_step = inferCourseEnrollmentFlowStep(baseCourseDraft);

        if (!baseCourseDraft.curso_nombre) {
          const suggestedCourses = activeCourse ? [activeCourse] : (signupMatches.length ? signupMatches : courses);
          await askForMissingCourseEnrollmentData({
            waId,
            phone,
            base: baseCourseDraft,
            courseOptions: suggestedCourses,
          });
          updateLastCloseContext(waId, {
            intentType: 'COURSE',
            interest: buildHubSpotCourseInterestLabel(courseEnrollmentIntent.course_query || text),
            suppressInactivityPrompt: true,
          });
          scheduleInactivityFollowUp(waId, phone);
          return;
        }

        if (looksLikeCourseOnsitePaymentIntent(text) && baseCourseDraft.alumno_nombre && baseCourseDraft.alumno_dni) {
          const resultOnsiteCourse = await finalizeCourseEnrollmentOnsiteFlow({ waId, phone, draft: baseCourseDraft });
          if (resultOnsiteCourse.type === 'onsite_cancelled') {
            const msgOnsite = buildCourseEnrollmentOnsitePendingMessage(baseCourseDraft);
            pushHistory(waId, 'assistant', msgOnsite);
            await sendWhatsAppText(phone, msgOnsite);
            updateLastCloseContext(waId, {
              intentType: 'COURSE',
              interest: buildHubSpotCourseInterestLabel(baseCourseDraft.curso_nombre || ''),
              suppressInactivityPrompt: false,
            });
            scheduleInactivityFollowUp(waId, phone);
            return;
          }
        }

        if (baseCourseDraft.payment_status === 'paid_verified') {
          const resultCourse = await finalizeCourseEnrollmentFlow({ waId, phone, draft: baseCourseDraft });
          if (resultCourse.type === 'reserved') {
            const msgReserved = buildCourseEnrollmentReservedMessage(baseCourseDraft);
            const rememberedReservedCourses = mergeCourseContextRows([{ nombre: baseCourseDraft.curso_nombre || '', categoria: baseCourseDraft.curso_categoria || '' }], previousRecentCourses);
            setLastCourseContext(waId, {
              query: baseCourseDraft.curso_nombre || lastCourseContext?.query || 'cursos',
              selectedName: baseCourseDraft.curso_nombre || '',
              currentCourseName: baseCourseDraft.curso_nombre || '',
              lastOptions: rememberedReservedCourses.map((c) => c.nombre).filter(Boolean).slice(0, 10),
              recentCourses: rememberedReservedCourses,
              requestedInterest: buildHubSpotCourseInterestLabel(baseCourseDraft.curso_nombre || ''),
            });
            pushHistory(waId, 'assistant', msgReserved);
            await sendWhatsAppText(phone, msgReserved);
            updateLastCloseContext(waId, {
              intentType: 'COURSE',
              interest: buildHubSpotCourseInterestLabel(baseCourseDraft.curso_nombre || ''),
              suppressInactivityPrompt: false,
            });
            scheduleInactivityFollowUp(waId, phone);
            return;
          }
        }

        await askForMissingCourseEnrollmentData({
          waId,
          phone,
          base: baseCourseDraft,
          courseOptions: signupCourse ? [signupCourse] : signupMatches,
        });
        setLastCourseContext(waId, {
          query: signupCourse?.nombre || lastCourseContext?.query || 'cursos',
          selectedName: signupCourse?.nombre || '',
          currentCourseName: signupCourse?.nombre || '',
          lastOptions: mergeCourseContextRows(signupCourse ? [signupCourse] : signupMatches, previousRecentCourses).map((c) => c.nombre).filter(Boolean).slice(0, 10),
          recentCourses: mergeCourseContextRows(signupCourse ? [signupCourse] : signupMatches, previousRecentCourses),
          requestedInterest: buildHubSpotCourseInterestLabel(signupCourse?.nombre || courseEnrollmentIntent.course_query || text),
        });
        updateLastCloseContext(waId, {
          intentType: 'COURSE',
          interest: buildHubSpotCourseInterestLabel(baseCourseDraft.curso_nombre || courseEnrollmentIntent.course_query || text),
          suppressInactivityPrompt: true,
        });
        scheduleInactivityFollowUp(waId, phone);
        return;
      }

      if (isDirectCurrentCourseFollowup || isExplicitReferencedCourseFollowup) {
        const rememberedCourses = mergeCourseContextRows([activeCourse], previousRecentCourses);
        const currentInterest = buildHubSpotCourseInterestLabel(activeCourse?.nombre || activeCourse?.categoria || '') || lastCourseContext?.requestedInterest || '';

        setLastCourseContext(waId, {
          query: lastCourseContext?.query || activeCourse?.nombre || 'cursos',
          selectedName: activeCourse?.nombre || '',
          currentCourseName: activeCourse?.nombre || '',
          lastOptions: rememberedCourses.map((c) => c.nombre).filter(Boolean).slice(0, 10),
          recentCourses: rememberedCourses,
          requestedInterest: currentInterest,
        });

        updateLastCloseContext(waId, {
          intentType: 'COURSE',
          interest: currentInterest,
          lastUserText: text,
        });

        if (followupGoal === 'MATERIAL') {
          const caption = buildCourseMediaCaption(activeCourse);
          const mediaResult = await sendCourseMediaDirect(phone, activeCourse, { caption });
          if (mediaResult.ok) {
            rememberAssistantCourseOffer(waId, [activeCourse], { mode: 'DETAIL', selectedName: activeCourse?.nombre || '', questionKind: 'MATERIAL', lastAssistantText: caption });
            pushHistory(waId, 'assistant', caption);
          } else {
            const fallbackMaterialText = caption || formatNaturalCourseFollowupReply(activeCourse, 'DETAIL');
            rememberAssistantCourseOffer(waId, [activeCourse], { mode: 'DETAIL', selectedName: activeCourse?.nombre || '', questionKind: 'MATERIAL', lastAssistantText: fallbackMaterialText });
            pushHistory(waId, 'assistant', fallbackMaterialText);
            await sendWhatsAppText(phone, fallbackMaterialText);
          }
          scheduleInactivityFollowUp(waId, phone);
          return;
        }

        const naturalCourseReply = formatNaturalCourseFollowupReply(activeCourse, followupGoal || 'DETAIL');
        if (naturalCourseReply) {
          rememberAssistantCourseOffer(waId, [activeCourse], { mode: 'DETAIL', selectedName: activeCourse?.nombre || '', questionKind: followupGoal || 'DETAIL', lastAssistantText: naturalCourseReply });
          pushHistory(waId, 'assistant', naturalCourseReply);
          await sendWhatsAppText(phone, naturalCourseReply);
          scheduleInactivityFollowUp(waId, phone);
          return;
        }
      }

      const courseQuery = resolveImplicitCourseFollowupQuery(text, lastCourseContext) || intent.query || '';
      const cleanedCourseQuery = sanitizeCourseSearchQuery(courseQuery);
      const normalizedCourseQuery = normalize(cleanedCourseQuery || courseQuery || '');
      const isGenericCourseQuery = !normalizedCourseQuery || isLikelyGenericCourseListQuery(courseQuery || '') || /^(curso|cursos|clase|clases|taller|talleres|capacitacion|capacitaciones|capacitación|masterclass|seminario|seminarios|workshop)$/.test(normalizedCourseQuery);

      if (!courses.length) {
        const msgEmpty = `La hoja CURSOS está vacía o no pude leer cursos disponibles en este momento.`;
        pushHistory(waId, "assistant", msgEmpty);
        await sendWhatsAppText(phone, msgEmpty);
        scheduleInactivityFollowUp(waId, phone);
        return;
      }

      if (referencedCourse && !cleanedCourseQuery && !followupGoal) {
        const detailReply = formatNaturalCourseFollowupReply(referencedCourse, 'DETAIL');
        if (detailReply) {
          const rememberedCourses = mergeCourseContextRows([referencedCourse], previousRecentCourses);
          const currentInterest = buildHubSpotCourseInterestLabel(referencedCourse?.nombre || referencedCourse?.categoria || '') || lastCourseContext?.requestedInterest || '';

          setLastCourseContext(waId, {
            query: referencedCourse?.nombre || lastCourseContext?.query || 'cursos',
            selectedName: referencedCourse?.nombre || '',
            currentCourseName: referencedCourse?.nombre || '',
            lastOptions: rememberedCourses.map((c) => c.nombre).filter(Boolean).slice(0, 10),
            recentCourses: rememberedCourses,
            requestedInterest: currentInterest,
          });

          updateLastCloseContext(waId, {
            intentType: 'COURSE',
            interest: currentInterest,
            lastUserText: text,
          });

          rememberAssistantCourseOffer(waId, [referencedCourse], { mode: 'DETAIL', selectedName: referencedCourse?.nombre || '', questionKind: 'DETAIL', lastAssistantText: detailReply });
          pushHistory(waId, "assistant", detailReply);
          await sendWhatsAppText(phone, detailReply);
          scheduleInactivityFollowUp(waId, phone);
          return;
        }
      }

      const matches = isGenericCourseQuery ? courses : findCourses(courses, cleanedCourseQuery || courseQuery, intent.mode);
      const replyMode = isGenericCourseQuery ? 'LIST' : intent.mode;
      const replyCatalog = formatCoursesReply(matches, replyMode);
      if (replyCatalog) {
        const selectedCourseName = matches.length === 1 ? (matches[0]?.nombre || '') : '';
        const rememberedCourses = mergeCourseContextRows(matches, previousRecentCourses);
        const courseInterestLabel = buildHubSpotCourseInterestLabel(selectedCourseName || cleanedCourseQuery || courseQuery || text);

        setLastCourseContext(waId, {
          query: cleanedCourseQuery || courseQuery || 'cursos',
          selectedName: selectedCourseName,
          currentCourseName: selectedCourseName || '',
          lastOptions: rememberedCourses.map((c) => c.nombre).filter(Boolean).slice(0, 10),
          recentCourses: rememberedCourses,
          requestedInterest: courseInterestLabel || '',
        });

        updateLastCloseContext(waId, {
          intentType: 'COURSE',
          interest: courseInterestLabel || (selectedCourseName || cleanedCourseQuery || courseQuery || lastCloseContext.get(waId)?.interest || ''),
          lastUserText: text,
        });

        await sendCourseCatalogResponses(phone, waId, matches, replyMode);
        scheduleInactivityFollowUp(waId, phone);
        return;
      }

      const someRows = courses.slice(0, 12);
      const wantedCourseInterest = buildHubSpotCourseInterestLabel(cleanedCourseQuery || courseQuery || text);
      const rememberedCourses = mergeCourseContextRows(previousRecentCourses, someRows);

      setLastCourseContext(waId, {
        query: cleanedCourseQuery || courseQuery || 'cursos',
        selectedName: '',
        currentCourseName: '',
        lastOptions: rememberedCourses.map((c) => c.nombre).filter(Boolean).slice(0, 10),
        recentCourses: rememberedCourses,
        requestedInterest: wantedCourseInterest || '',
      });

      updateLastCloseContext(waId, {
        intentType: 'COURSE',
        interest: wantedCourseInterest || (cleanedCourseQuery || courseQuery || text),
        lastUserText: text,
      });

      if (!isGenericCourseQuery) {
        const topic = humanizeCourseSearchQuery(cleanedCourseQuery || courseQuery);
        const msgNo = topic
          ? `Por el momento no tenemos un curso de *${topic}* disponible.

Apenas salga algo relacionado, le vamos a estar avisando 😊`
          : `Por el momento no tenemos un curso similar disponible.

Apenas salga algo relacionado, le vamos a estar avisando 😊`;
        pushHistory(waId, "assistant", msgNo);
        await sendWhatsAppText(phone, msgNo);
        scheduleInactivityFollowUp(waId, phone);
        return;
      }

      const some = someRows.map(c => `🎓 ${c.nombre}`).join("\n");
      const msgNo = `No encontré una coincidencia exacta en la hoja CURSOS.

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

    const activeCampaignMessages = await buildAssistantMessagesForBroadcastCampaign(waId, getActiveAssistantOffer(waId));

    const messages = [
      { role: "system", content: SYSTEM_PROMPT },
      ...activeCampaignMessages,
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
    processAppointmentResponseTimeouts().catch((e) => {
      console.error('❌ Error en el scheduler de vencimiento de turnos:', e?.response?.data || e?.message || e);
    });
    processCourseManagerApprovalTimeouts().catch((e) => {
      console.error('❌ Error en el scheduler de vencimiento de cursos:', e?.response?.data || e?.message || e);
    });
  }, APPOINTMENT_TEMPLATE_SCAN_MS);
} else {
  console.log("ℹ️ Plantillas de turnos desactivadas temporalmente");
}

// ===================== FOLLOW-UP COMERCIAL =====================
if (ENABLE_COMMERCIAL_FOLLOWUPS) {
  setInterval(() => {
    processCommercialFollowups().catch((e) => {
      console.error("❌ Error en el scheduler de follow-up comercial:", e?.response?.data || e?.message || e);
    });
  }, COMMERCIAL_FOLLOWUP_SCAN_MS);
} else {
  console.log("ℹ️ Follow-up comercial desactivado");
}

// ===================== DIFUSIÓN DIARIA =====================
if (ENABLE_DAILY_BROADCAST) {
  setInterval(() => {
    processBroadcastQueue().catch((e) => {
      console.error("❌ Error en el scheduler de difusión diaria:", e?.response?.data || e?.message || e);
    });
  }, BROADCAST_SCAN_MS);
} else {
  console.log("ℹ️ Difusión diaria desactivada");
}

// ===================== CUMPLEAÑOS =====================
if (ENABLE_BIRTHDAY_MESSAGES) {
  setInterval(() => {
    processBirthdayMessages().catch((e) => {
      console.error("❌ Error en el scheduler de cumpleaños:", e?.response?.data || e?.message || e);
    });
  }, BIRTHDAY_SCAN_MS);
} else {
  console.log("ℹ️ Mensajes de cumpleaños desactivados");
}

// ===================== START =====================
const PORT = process.env.PORT || 3000;

(async () => {
  await ensureDb();
  await ensureAppointmentTables();
  await ensureCourseEnrollmentTables();
  await ensureCommercialFollowupTables();
  await ensureBirthdayMessageTables();
  await ensureBroadcastTables();
  console.log(hasHubSpotEnabled()
    ? "✅ HubSpot CRM habilitado para seguimiento al cierre de charla"
    : "⚠️ HubSpot CRM no configurado: falta HUBSPOT_ACCESS_TOKEN / HUBSPOT_TOKEN");
  console.log(ENABLE_END_OF_DAY_TRACKING
    ? "ℹ️ Seguimiento de medianoche activado"
    : "ℹ️ Seguimiento de medianoche desactivado (se usa cierre por inactividad)");
  console.log(ENABLE_APPOINTMENT_TEMPLATES
    ? "ℹ️ Plantillas de turnos activadas"
    : "ℹ️ Plantillas de turnos desactivadas temporalmente");
  console.log(`ℹ️ Vencimiento de respuesta de peluquera: ${Math.round(STYLIST_CONFIRMATION_TIMEOUT_MS / 3600000)} hs`);
  console.log(`ℹ️ Vencimiento de respuesta de responsable de cursos: ${Math.round(COURSE_MANAGER_CONFIRMATION_TIMEOUT_MS / 3600000)} hs`);
  console.log(ENABLE_COMMERCIAL_FOLLOWUPS
    ? "ℹ️ Follow-up comercial activado"
    : "ℹ️ Follow-up comercial desactivado");
  console.log(ENABLE_BIRTHDAY_MESSAGES
    ? "ℹ️ Mensajes de cumpleaños activados"
    : "ℹ️ Mensajes de cumpleaños desactivados");
  console.log(ENABLE_DAILY_BROADCAST
    ? (broadcastExcelExists()
        ? `ℹ️ Difusión diaria activada con Excel cargado en: ${BROADCAST_EXCEL_PATH} | horario 09:00 a 22:00`
        : 'ℹ️ Difusión diaria lista pero sin Excel cargado todavía. Subilo en https://bot-cataleya.onrender.com/broadcast/panel')
    : "ℹ️ Difusión diaria desactivada");
  console.log(hasGoogleContactsSyncEnabled()
    ? `ℹ️ Google Contacts sincronizado en ${GOOGLE_CONTACTS_TARGETS.map((x) => x.email).join(' y ')}`
    : "ℹ️ Google Contacts no configurado para sincronización automática");
  if (ENABLE_APPOINTMENT_TEMPLATES) {
    await processAppointmentTemplateNotifications().catch((e) => {
      console.error('❌ Error inicial procesando plantillas de turnos:', e?.response?.data || e?.message || e);
    });
    await processAppointmentResponseTimeouts().catch((e) => {
      console.error('❌ Error inicial procesando vencimientos de turnos:', e?.response?.data || e?.message || e);
    });
    await processCourseManagerApprovalTimeouts().catch((e) => {
      console.error('❌ Error inicial procesando vencimientos de cursos:', e?.response?.data || e?.message || e);
    });
  }
  if (ENABLE_COMMERCIAL_FOLLOWUPS) {
    await processCommercialFollowups().catch((e) => {
      console.error('❌ Error inicial procesando follow-up comercial:', e?.response?.data || e?.message || e);
    });
  }
  if (ENABLE_BIRTHDAY_MESSAGES) {
    await processBirthdayMessages().catch((e) => {
      console.error('❌ Error inicial procesando cumpleaños:', e?.response?.data || e?.message || e);
    });
  }
  if (ENABLE_DAILY_BROADCAST && BROADCAST_AUTO_IMPORT_ON_START) {
    await ensureBroadcastCampaignLoaded({
      campaignName: BROADCAST_CAMPAIGN_NAME,
      excelPath: BROADCAST_EXCEL_PATH,
      offerType: BROADCAST_OFFER_TYPE,
      offerItems: BROADCAST_OFFER_ITEMS,
      offerSelectedName: BROADCAST_OFFER_SELECTED_NAME,
    }).catch((e) => {
      console.error('❌ Error inicial preparando difusión diaria:', e?.response?.data || e?.message || e);
    });
    await processBroadcastQueue().catch((e) => {
      console.error('❌ Error inicial procesando difusión diaria:', e?.response?.data || e?.message || e);
    });
  }

  app.listen(PORT, () => {
    console.log("🚀 Bot de estética activo");
    console.log(`Webhook: http://localhost:${PORT}/webhook`);
    console.log(`Health:  http://localhost:${PORT}/health`);
  });
})();
