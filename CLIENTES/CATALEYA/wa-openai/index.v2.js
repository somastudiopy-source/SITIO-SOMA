const express = require("express");
const dotenv = require("dotenv");
const { Pool } = require("pg");
const axios = require("axios");
const fs = require("fs");
const path = require("path");

dotenv.config();

const CLIENT_ID = "CATALEYA";
const PORT = Number(process.env.PORT || 3000);
const VERIFY_TOKEN = process.env.VERIFY_TOKEN || "";
const DB_URL = process.env.DB_URL || process.env.DATABASE_URL || "";
const INBOUND_MERGE_MS = Number(process.env.INBOUND_MERGE_MS || 1200);
const INBOUND_MERGE_EXTENDED_MS = Number(process.env.INBOUND_MERGE_EXTENDED_MS || 2200);
const INBOUND_MERGE_MAX_WAIT_MS = Number(process.env.INBOUND_MERGE_MAX_WAIT_MS || 5000);
const MEDIA_DIR = process.env.MEDIA_DIR || path.join(__dirname, "media");

if (!DB_URL) {
  throw new Error("Falta variable DB_URL o DATABASE_URL para conectar a PostgreSQL");
}

const db = new Pool({
  connectionString: DB_URL,
  ssl: DB_URL.includes("render.com") || DB_URL.includes("railway")
    ? { rejectUnauthorized: false }
    : false,
});
try { fs.mkdirSync(MEDIA_DIR, { recursive: true }); } catch {}

let pdfParse = null;
try { pdfParse = require("pdf-parse"); } catch {}
const processedMsgIds = new Set();
const inboundMergeState = new Map();
const convByWaId = new Map();
const activeOfferByWaId = new Map();
const lastCourseContextByWaId = new Map();
const lastProductContextByWaId = new Map();
const lastServiceByWaId = new Map();

function getMetaGraphVersion() {
  return process.env.META_GRAPH_VERSION || "v19.0";
}

function getWhatsAppGraphBaseUrl() {
  return `https://graph.facebook.com/${getMetaGraphVersion()}`;
}

function getWhatsAppAuthHeaders() {
  const token = process.env.WHATSAPP_TOKEN || "";
  if (!token) {
    throw new Error("Falta variable WHATSAPP_TOKEN para usar cliente WhatsApp/Meta");
  }
  return { Authorization: `Bearer ${token}` };
}

function getRequiredPhoneNumberId() {
  const phoneNumberId = process.env.PHONE_NUMBER_ID || "";
  if (!phoneNumberId) {
    throw new Error("Falta variable PHONE_NUMBER_ID para usar cliente WhatsApp/Meta");
  }
  return phoneNumberId;
}

async function sendWhatsAppText(phone, text) {
  const phoneNumberId = getRequiredPhoneNumberId();
  const url = `${getWhatsAppGraphBaseUrl()}/${phoneNumberId}/messages`;
  try {
    const resp = await axios.post(
      url,
      {
        messaging_product: "whatsapp",
        to: String(phone || ""),
        text: { body: String(text || "") },
      },
      { headers: { ...getWhatsAppAuthHeaders() } }
    );
    const wa_msg_id = resp?.data?.messages?.[0]?.id || null;
    return { ok: true, wa_msg_id, raw: resp.data };
  } catch (e) {
    console.error("❌ META sendWhatsAppText error:", e?.response?.data || e?.message || e);
    throw e;
  }
}

async function getWhatsAppMediaUrl(mediaId) {
  if (!mediaId) throw new Error("Falta mediaId para obtener URL de media");
  const url = `${getWhatsAppGraphBaseUrl()}/${mediaId}`;
  try {
    const resp = await axios.get(url, {
      headers: { ...getWhatsAppAuthHeaders() },
    });
    return {
      id: mediaId,
      url: resp?.data?.url || "",
      mime_type: resp?.data?.mime_type || "",
      sha256: resp?.data?.sha256 || "",
      file_size: resp?.data?.file_size || null,
    };
  } catch (e) {
    console.error("❌ META getWhatsAppMediaUrl error:", e?.response?.data || e?.message || e);
    throw e;
  }
}

async function downloadWhatsAppMediaToFile(mediaUrl, outputPath) {
  if (!mediaUrl) throw new Error("Falta mediaUrl para descargar media");
  if (!outputPath) throw new Error("Falta outputPath para guardar media");
  try {
    const response = await axios.get(mediaUrl, {
      responseType: "stream",
      headers: { ...getWhatsAppAuthHeaders() },
    });
    await new Promise((resolve, reject) => {
      const writer = fs.createWriteStream(outputPath);
      response.data.pipe(writer);
      writer.on("finish", resolve);
      writer.on("error", reject);
    });
    return outputPath;
  } catch (e) {
    console.error("❌ META downloadWhatsAppMediaToFile error:", e?.response?.data || e?.message || e);
    throw e;
  }
}

async function tryParsePdfBuffer(buf) {
  if (!pdfParse) return "";
  try {
    const data = await pdfParse(buf);
    return String(data?.text || "").trim();
  } catch {
    return "";
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function inboundTextLooksLikeContinuation(text = "") {
  const raw = String(text || "").trim();
  const t = raw.toLowerCase();
  const wordCount = t ? t.split(/\s+/).filter(Boolean).length : 0;

  if (raw.length <= 18) return true;
  if (wordCount <= 3) return true;
  if (/^(si|sí|no|ok|dale|perfecto|bueno|bien|otra cosa|para mi|para ella|quiero|necesito)$/i.test(t)) return true;
  return false;
}

function compactMergedInboundText(text = "") {
  const lines = String(text || "")
    .split(/\r?\n+/)
    .map((x) => String(x || "").replace(/\s+/g, " ").trim())
    .filter(Boolean);

  if (!lines.length) return "";
  if (lines.length === 1) return lines[0];

  let out = lines[0];
  for (const line of lines.slice(1)) {
    const compact = String(line || "").trim();
    if (!compact) continue;
    const words = compact.split(/\s+/).filter(Boolean).length;
    const joinInline = compact.length <= 36 || words <= 6 || inboundTextLooksLikeContinuation(compact);
    out += joinInline ? ` ${compact}` : `\n${compact}`;
  }
  return out.replace(/\s+\n/g, "\n").replace(/\n\s+/g, "\n").trim();
}

function getInboundMergeTargetMs(state = {}) {
  const items = Array.isArray(state?.items) ? state.items : [];
  const latest = items[items.length - 1] || {};
  const latestText = String(latest?.userIntentText || latest?.text || "").trim();
  const mergedText = compactMergedInboundText(items.map((it) => String(it?.userIntentText || it?.text || "").trim()).filter(Boolean).join("\n"));
  if (items.length >= 2) return INBOUND_MERGE_EXTENDED_MS;
  if (inboundTextLooksLikeContinuation(latestText) || inboundTextLooksLikeContinuation(mergedText)) return INBOUND_MERGE_EXTENDED_MS;
  return INBOUND_MERGE_MS;
}

function appendInboundMergeChunk(waId, chunk) {
  const now = Date.now();
  const prev = inboundMergeState.get(waId) || { version: 0, items: [], firstTs: now, lastTs: now };
  const next = {
    version: Number(prev.version || 0) + 1,
    firstTs: Number(prev.firstTs || now),
    lastTs: now,
    items: [...(Array.isArray(prev.items) ? prev.items : []), { ...chunk, ts: now }].slice(-12),
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
  const mergedText = compactMergedInboundText(items.map((it) => String(it?.text || "").trim()).filter(Boolean).join("\n"));
  const mergedUserIntentText = compactMergedInboundText(items.map((it) => String(it?.userIntentText || it?.text || "").trim()).filter(Boolean).join("\n"));
  const latestMedia = [...items].reverse().find((it) => !!it?.mediaMeta)?.mediaMeta || null;
  return { ...latest, itemCount: items.length, items, text: mergedText, userIntentText: mergedUserIntentText || mergedText, mediaMeta: latestMedia };
}

async function waitForInboundMergeSilence(waId, version) {
  const startedAt = Date.now();
  while (true) {
    const state = inboundMergeState.get(waId);
    if (!state || state.version !== version) return null;
    const targetMs = getInboundMergeTargetMs(state);
    const lastTs = Number(state.lastTs || startedAt);
    const elapsedSinceLastChunk = Date.now() - lastTs;
    const totalElapsed = Date.now() - startedAt;
    if (elapsedSinceLastChunk >= targetMs || totalElapsed >= INBOUND_MERGE_MAX_WAIT_MS) return consumeInboundMergeChunk(waId, version);
    const missing = Math.max(120, Math.min(450, targetMs - elapsedSinceLastChunk));
    await sleep(missing);
  }
}

async function dbInsertMessage({ direction, wa_peer, name, text, msg_type, wa_msg_id, raw }) {
  await db.query(
    `INSERT INTO messages(client_id, direction, wa_peer, name, text, msg_type, wa_msg_id, raw_json)
     VALUES($1,$2,$3,$4,$5,$6,$7,$8)`,
    [
      CLIENT_ID,
      String(direction || ""),
      String(wa_peer || ""),
      name ? String(name) : null,
      text ? String(text) : null,
      msg_type ? String(msg_type) : null,
      wa_msg_id ? String(wa_msg_id) : null,
      raw ? JSON.stringify(raw) : null,
    ]
  );
}

function ensureConv(waId) {
  const now = Date.now();
  let c = convByWaId.get(waId);
  if (!c) {
    c = { messages: [], updatedAt: now };
    convByWaId.set(waId, c);
  }
  if (!Array.isArray(c.messages)) c.messages = [];
  c.updatedAt = now;
  return c;
}

function pushHistory(waId, role, content) {
  const conv = ensureConv(waId);
  const text = String(content || "").trim();
  if (!text) return;
  conv.messages.push({ role, content: text });
  conv.messages = conv.messages.slice(-30);
  conv.updatedAt = Date.now();
  console.log("🧠 HISTORY_PUSHED", JSON.stringify({ wa_id: waId, role, content_len: text.length, total_messages: conv.messages.length }));
}

async function getRecentDbMessages(waPeer, limit = 12) {
  const peerNorm = String(waPeer || "").replace(/[^\d]/g, "");
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
  return merged.slice(-32);
}

function clearLocalConversationState({ waId = "", phone = "", phoneRaw = "" } = {}) {
  if (waId) convByWaId.delete(waId);
  if (waId) activeOfferByWaId.delete(waId);
  if (waId) lastCourseContextByWaId.delete(waId);
  if (waId) lastProductContextByWaId.delete(waId);
  if (waId) lastServiceByWaId.delete(waId);
  console.log("🧹 CONTEXT_CLEAR", JSON.stringify({ scope: "local", wa_id: waId || "", phone: phone || "", phone_raw: phoneRaw || "" }));
}

async function clearPersistedConversationState({ waId = "", phone = "", phoneRaw = "" } = {}) {
  const peer = String(phone || phoneRaw || "").replace(/[^\d]/g, "");
  if (!peer) return;
  await db.query(
    `DELETE FROM messages
      WHERE client_id = $1
        AND wa_peer = $2`,
    [CLIENT_ID, peer]
  );
  console.log("🧹 CONTEXT_CLEAR", JSON.stringify({ scope: "persisted", wa_id: waId || "", phone: peer }));
}

function getActiveAssistantOffer(waId) {
  return activeOfferByWaId.get(waId) || null;
}
function setActiveAssistantOffer(waId, patch = {}) {
  const prev = activeOfferByWaId.get(waId) || {};
  const next = { ...prev, ...patch, updatedAt: Date.now() };
  activeOfferByWaId.set(waId, next);
  return next;
}
function clearActiveAssistantOffer(waId) {
  activeOfferByWaId.delete(waId);
}

function getLastCourseContext(waId) {
  return lastCourseContextByWaId.get(waId) || null;
}
function setLastCourseContext(waId, patch = {}) {
  const prev = lastCourseContextByWaId.get(waId) || {};
  const next = { ...prev, ...patch, updatedAt: Date.now() };
  lastCourseContextByWaId.set(waId, next);
  return next;
}
function clearLastCourseContext(waId) {
  lastCourseContextByWaId.delete(waId);
}

function getLastProductContext(waId) {
  return lastProductContextByWaId.get(waId) || null;
}
function setLastProductContext(waId, patch = {}) {
  const prev = lastProductContextByWaId.get(waId) || {};
  const next = { ...prev, ...patch, updatedAt: Date.now() };
  lastProductContextByWaId.set(waId, next);
  return next;
}
function clearLastProductContext(waId) {
  lastProductContextByWaId.delete(waId);
}

function getLastServiceContext(waId) {
  return lastServiceByWaId.get(waId) || null;
}
function setLastServiceContext(waId, patch = {}) {
  const prev = lastServiceByWaId.get(waId) || {};
  const next = { ...prev, ...patch, updatedAt: Date.now() };
  lastServiceByWaId.set(waId, next);
  return next;
}
function clearLastServiceContext(waId) {
  lastServiceByWaId.delete(waId);
}

async function extractTextFromIncomingMessage(msg = {}) {
  if (msg.type === "text") return { text: msg.text?.body || "", kind: "text" };
  if (msg.type === "button") return { text: msg.button?.text || msg.button?.payload || "", kind: "button" };
  if (msg.type === "interactive") {
    return {
      text: msg.interactive?.button_reply?.title || msg.interactive?.button_reply?.id || msg.interactive?.list_reply?.title || msg.interactive?.list_reply?.id || "",
      kind: "interactive",
    };
  }
  if (msg.type === "image") {
    const caption = msg.image?.caption || "";
    return { text: caption, kind: "image_caption", media: { id: msg.image?.id || "", mime_type: "image/*", filename: "" } };
  }
  if (msg.type === "document") {
    const mediaId = msg.document?.id;
    const caption = msg.document?.caption || "";
    const filename = msg.document?.filename || "";
    if (!mediaId) return { text: caption, kind: "document_no_id" };
    const mediaInfo = await getWhatsAppMediaUrl(mediaId);
    const mime = mediaInfo.mime_type || "";
    const ext = mime.includes("pdf") ? ".pdf" : ".bin";
    const tmpFile = path.join(MEDIA_DIR, `wa-doc-${mediaId}${ext}`);
    await downloadWhatsAppMediaToFile(mediaInfo.url, tmpFile);
    let extractedTxt = "";
    if (mime.includes("pdf")) {
      const buf = fs.readFileSync(tmpFile);
      extractedTxt = await tryParsePdfBuffer(buf);
    }
    const combined = [
      caption ? `Texto adjunto del cliente: "${caption}"` : "",
      filename ? `Documento: ${filename}` : "",
      extractedTxt ? `Texto del documento: ${String(extractedTxt).slice(0, 6000)}` : "",
      (!extractedTxt && mime.includes("pdf")) ? "No pude extraer texto del PDF. Si querés, enviame una captura o el texto importante." : "",
    ].filter(Boolean).join("\n");
    return {
      text: combined,
      kind: extractedTxt ? "document" : "document_pdf_no_text",
      media: { id: mediaId, mime_type: mime, filename: filename || path.basename(tmpFile) },
    };
  }
  return { text: "", kind: `ignored_${msg.type || "unknown"}` };
}

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
      template_name TEXT,
      template_language TEXT,
      template_var_mapping_json JSONB NOT NULL DEFAULT '[]'::jsonb,
      template_components_json JSONB NOT NULL DEFAULT '[]'::jsonb,
      template_header_format TEXT,
      template_body_var_count INTEGER NOT NULL DEFAULT 0,
      template_header_var_count INTEGER NOT NULL DEFAULT 0,
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
      source_row_json JSONB NOT NULL DEFAULT '{}'::jsonb,
      contact_name TEXT,
      profile_name TEXT,
      wa_phone TEXT NOT NULL,
      wa_id TEXT,
      custom_message TEXT,
      message_index INTEGER,
      message_text TEXT,
      template_name TEXT,
      template_language TEXT,
      template_vars_json JSONB NOT NULL DEFAULT '[]'::jsonb,
      template_header_vars_json JSONB NOT NULL DEFAULT '[]'::jsonb,
      template_header_format TEXT,
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
  await db.query(`ALTER TABLE broadcast_campaigns ADD COLUMN IF NOT EXISTS template_name TEXT`);
  await db.query(`ALTER TABLE broadcast_campaigns ADD COLUMN IF NOT EXISTS template_language TEXT`);
  await db.query(`ALTER TABLE broadcast_campaigns ADD COLUMN IF NOT EXISTS template_var_mapping_json JSONB NOT NULL DEFAULT '[]'::jsonb`);
  await db.query(`ALTER TABLE broadcast_campaigns ADD COLUMN IF NOT EXISTS template_components_json JSONB NOT NULL DEFAULT '[]'::jsonb`);
  await db.query(`ALTER TABLE broadcast_campaigns ADD COLUMN IF NOT EXISTS template_header_format TEXT`);
  await db.query(`ALTER TABLE broadcast_campaigns ADD COLUMN IF NOT EXISTS template_body_var_count INTEGER NOT NULL DEFAULT 0`);
  await db.query(`ALTER TABLE broadcast_campaigns ADD COLUMN IF NOT EXISTS template_header_var_count INTEGER NOT NULL DEFAULT 0`);

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
           WHEN valid_until::text ~ '^\\d{4}-\\d{2}-\\d{2}$' THEN (valid_until::text || ' 23:59:00-03')::timestamptz
           ELSE NULL
         END`);
    await db.query(`ALTER TABLE broadcast_campaigns DROP COLUMN valid_until`);
    await db.query(`ALTER TABLE broadcast_campaigns RENAME COLUMN valid_until_tmp TO valid_until`);
  }

  await db.query(`ALTER TABLE broadcast_queue ADD COLUMN IF NOT EXISTS source_row_json JSONB NOT NULL DEFAULT '{}'::jsonb`);
  await db.query(`ALTER TABLE broadcast_queue ADD COLUMN IF NOT EXISTS template_name TEXT`);
  await db.query(`ALTER TABLE broadcast_queue ADD COLUMN IF NOT EXISTS template_language TEXT`);
  await db.query(`ALTER TABLE broadcast_queue ADD COLUMN IF NOT EXISTS template_vars_json JSONB NOT NULL DEFAULT '[]'::jsonb`);
  await db.query(`ALTER TABLE broadcast_queue ADD COLUMN IF NOT EXISTS template_header_vars_json JSONB NOT NULL DEFAULT '[]'::jsonb`);
  await db.query(`ALTER TABLE broadcast_queue ADD COLUMN IF NOT EXISTS template_header_format TEXT`);
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
}

async function initDb() {
  await ensureDb();
  await ensureAppointmentTables();
  await ensureCourseEnrollmentTables();
  await ensureCommercialFollowupTables();
  await ensureBirthdayMessageTables();
  await ensureBroadcastTables();
}

const app = express();
app.use(express.json({ limit: "2mb" }));

app.get("/ping", (req, res) => {
  res.status(200).send("pong");
});

app.get("/health", async (req, res) => {
  let dbOk = false;
  let dbError = "";

  try {
    await db.query("SELECT 1");
    dbOk = true;
  } catch (e) {
    dbError = e?.message || "db_error";
  }

  res.json({
    ok: true,
    clientId: CLIENT_ID,
    db: {
      configured: !!DB_URL,
      ok: dbOk,
      error: dbError || null,
    },
  });
});

app.get("/webhook", (req, res) => {
  const mode = req.query["hub.mode"];
  const token = req.query["hub.verify_token"];
  const challenge = req.query["hub.challenge"];

  if (mode === "subscribe" && token === VERIFY_TOKEN) {
    return res.status(200).send(challenge);
  }
  return res.sendStatus(403);
});

app.post("/webhook", async (req, res) => {
  res.sendStatus(200);
  try {
    const entries = Array.isArray(req.body?.entry) ? req.body.entry : [];
    const entryCount = entries.length;
    const changesCount = entries.reduce((acc, entry) => acc + (Array.isArray(entry?.changes) ? entry.changes.length : 0), 0);
    const messagesCount = entries.reduce((acc, entry) => acc + (Array.isArray(entry?.changes) ? entry.changes.reduce((inAcc, c) => inAcc + (Array.isArray(c?.value?.messages) ? c.value.messages.length : 0), 0) : 0), 0);
    console.log("📩 WEBHOOK_INBOUND_COUNTS", JSON.stringify({ entry_count: entryCount, changes_count: changesCount, messages_count: messagesCount }));

    for (const entry of entries) {
      const changes = Array.isArray(entry?.changes) ? entry.changes : [];
      for (const change of changes) {
        const value = change?.value || {};
        const statuses = Array.isArray(value?.statuses) ? value.statuses : [];
        for (const statusObj of statuses) {
          console.log("ℹ️ WEBHOOK_STATUS", JSON.stringify({
            id: statusObj?.id || "",
            status: statusObj?.status || "",
            recipient_id: statusObj?.recipient_id || "",
            timestamp: statusObj?.timestamp || "",
          }));
        }

        const inboundMessages = Array.isArray(value?.messages) ? value.messages : [];
        const contacts = Array.isArray(value?.contacts) ? value.contacts : [];
        for (let msgIndex = 0; msgIndex < inboundMessages.length; msgIndex += 1) {
          const msg = inboundMessages[msgIndex];
          const contact = contacts[msgIndex] || contacts[0] || {};
          if (!msg) continue;

          if (msg.id && processedMsgIds.has(msg.id)) {
            console.log("⚠️ WEBHOOK_DEDUPE_SKIPPED", JSON.stringify({ msg_id: msg.id, msg_type: msg?.type || "", msg_timestamp: msg?.timestamp || "" }));
            continue;
          }
          if (msg.id) {
            processedMsgIds.add(msg.id);
            if (processedMsgIds.size > 5000) processedMsgIds.clear();
          }

          const phoneRaw = msg.from;
          const phone = String(contact?.wa_id || phoneRaw || "").replace(/[^\d]/g, "");
          const waId = contact?.wa_id || phoneRaw || "";
          const name = contact?.profile?.name || "";
          const msgContextId = String(msg?.context?.id || "").trim();
          const hasCaption = !!((msg.type === "image" && String(msg.image?.caption || "").trim()) || (msg.type === "document" && String(msg.document?.caption || "").trim()));
          const isStoryReply = !!(msg?.context?.from || msg?.context?.id || msg?.context?.referred_product || msg?.context?.forwarded);

          console.log("📨 WEBHOOK_INBOUND_MSG", JSON.stringify({
            msg_id: msg?.id || "",
            msg_type: msg?.type || "",
            msg_timestamp: msg?.timestamp || "",
            contact_wa_id: contact?.wa_id || "",
            msg_from: phoneRaw || "",
            profile_name: name || "",
            has_caption: hasCaption,
            context_id: msgContextId || "",
            looks_like_story_reply: isStoryReply,
          }));

          const convBefore = ensureConv(waId);
          console.log("🧠 CONTEXT_BEFORE", JSON.stringify({
            wa_id: waId,
            history_count: Array.isArray(convBefore?.messages) ? convBefore.messages.length : 0,
            has_active_offer: !!getActiveAssistantOffer(waId),
            has_last_course_context: !!getLastCourseContext(waId),
            has_last_product_context: !!getLastProductContext(waId),
            has_last_service_context: !!getLastServiceContext(waId),
          }));

          const extracted = await extractTextFromIncomingMessage(msg);
          let text = String(extracted?.text || "").trim();
          let mediaMeta = extracted?.media || null;
          let userIntentText = (
            msg.type === "image" ? (msg.image?.caption || "") :
            msg.type === "document" ? (msg.document?.caption || "") :
            text
          ).trim();
          if (!userIntentText && text) userIntentText = text;

          if (/^\/reset-context$/i.test(text) || /^\/reset-context$/i.test(userIntentText)) {
            clearLocalConversationState({ waId, phone, phoneRaw });
            await clearPersistedConversationState({ waId, phone, phoneRaw });
            continue;
          }

          console.log("🧾 WEBHOOK_EXTRACTED_TEXT", JSON.stringify({
            msg_id: msg?.id || "",
            kind: extracted?.kind || "",
            extracted_text_len: text.length,
            user_intent_len: userIntentText.length,
          }));

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
              inbound_meta: {
                context_id: msgContextId || "",
                has_caption: hasCaption,
                looks_like_story_reply: isStoryReply,
                wa_id: waId || "",
              },
            },
          });

          const inboundVersion = appendInboundMergeChunk(waId || phone || msg.id || `unknown-${Date.now()}`, {
            phone,
            phoneRaw,
            name,
            text,
            userIntentText,
            mediaMeta,
            msgType: msg.type,
            msgId: msg.id,
          });
          const mergedInbound = await waitForInboundMergeSilence(waId || phone || msg.id || "unknown", inboundVersion);
          if (!mergedInbound) continue;
          const mergedTextRaw = String(mergedInbound.text || "").trim();
          const mergedIntentRaw = String(mergedInbound.userIntentText || userIntentText || mergedTextRaw).trim();
          text = compactMergedInboundText(mergedTextRaw) || mergedTextRaw;
          userIntentText = compactMergedInboundText(mergedIntentRaw) || mergedIntentRaw || text;
          mediaMeta = mergedInbound.mediaMeta || mediaMeta || null;

          console.log("🧩 WEBHOOK_INBOUND_MERGE", JSON.stringify({
            wa_id: waId || "",
            msg_id: msg?.id || "",
            merged_item_count: Number(mergedInbound?.itemCount || 1),
            merged_text_len: text.length,
            merged_intent_len: userIntentText.length,
            merged_text: String(text || "").slice(0, 500),
            has_media: !!mediaMeta,
          }));

          pushHistory(waId, "user", userIntentText || text || "");
          const recentDbMessages = await getRecentDbMessages(phone, 12);
          const convAfter = ensureConv(waId);
          const convForAI = mergeConversationForAI(recentDbMessages, convAfter.messages || []);
          console.log("🧠 CONTEXT_AFTER", JSON.stringify({
            wa_id: waId,
            history_count: Array.isArray(convAfter?.messages) ? convAfter.messages.length : 0,
            merged_for_ai_count: Array.isArray(convForAI) ? convForAI.length : 0,
            has_active_offer: !!getActiveAssistantOffer(waId),
            has_last_course_context: !!getLastCourseContext(waId),
            has_last_product_context: !!getLastProductContext(waId),
            has_last_service_context: !!getLastServiceContext(waId),
          }));
        }
      }
    }
  } catch (e) {
    console.error("❌ ERROR webhook v2.0-d:", e?.response?.data || e?.message || e);
  }
});

initDb()
  .then(() => {
    console.log("✅ DB init v2.0-b completado");
    app.listen(PORT, () => {
      console.log("✅ index.v2 base server iniciado");
      console.log(`- Puerto: ${PORT}`);
      console.log(`- DB_URL/DATABASE_URL configurado: ${DB_URL ? "sí" : "no"}`);
      console.log(`- Ping: http://localhost:${PORT}/ping`);
      console.log(`- Health: http://localhost:${PORT}/health`);
      console.log(`- Webhook verify: http://localhost:${PORT}/webhook`);
    });
  })
  .catch((err) => {
    console.error("❌ Error inicializando DB en v2.0-b:", err?.message || err);
    process.exit(1);
  });
