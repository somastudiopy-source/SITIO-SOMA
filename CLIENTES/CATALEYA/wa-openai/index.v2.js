const express = require("express");
const dotenv = require("dotenv");
const { Pool } = require("pg");
const axios = require("axios");
const fs = require("fs");

dotenv.config();

const CLIENT_ID = "CATALEYA";
const PORT = Number(process.env.PORT || 3000);
const VERIFY_TOKEN = process.env.VERIFY_TOKEN || "";
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
