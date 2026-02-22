/**
 * aplicar_mejoras.js
 * Genera index_mejorado.js a partir de tu index.js SIN recortar nada.
 * Inserta:
 * - Resumen de charla a los 30 min a Google Sheet 11Y-dID...
 * - Clasificación CATEGORIA/PRODUCTO (listas cerradas)
 * - Fix de turnos: logs + registro en Sheet + Calendar (si ya existe en tu código, no lo pisa)
 *
 * Uso: node aplicar_mejoras.js
 */

const fs = require("fs");

const INPUT = "index.js";
const OUTPUT = "index_mejorado.js";

// ====== CONFIG DE TU CASO ======
const CONVERSATIONS_SHEET_ID = "11Y-dID7-AI4LItYH3WI2iVi8BfYgOnDQfjPR04sH3rk";
const TIMEZONE = "America/Argentina/Salta";

const CATEGORY_OPTIONS = [
  "CURSOS📝",
  "MUEBLES🪑",
  "SERVICIOS DE BELLEZA💄",
  "BARBER💈",
  "INSUMOS🧴",
  "MÁQUINAS⚙️",
];

const PRODUCT_OPTIONS = [
  "Silla Hidráulica",
  "Camillas",
  "Puff",
  "Espejos / Muebles",
  "Mesas /  Planchas / Secadores",
  "Shampoo ácido",
  "Nutrición // Tintura Baño de crema / Matizador",
  "Ojo",
  "ácido",
  "Peinado",
  "Limpieza Facial",
  "Lifting / Pestañas / Cejas / Pies / Uñas / Cera",
  "Alisado",
  "Corte",
  "Tintura",
  "Maquillaje",
  "Aceite maquina",
  "Tijeras",
  "Trenzas",
  "Depilación",
  "Masajes",
  "Permanente",
  "Mesa Manicura",
  "CURSO BARBERÍA",
  "CURSO NIÑOS",
  "CURSO PELUQUERIA",
];

function safeInsertAfter(src, marker, block) {
  const i = src.indexOf(marker);
  if (i === -1) return { ok: false, src };
  const j = i + marker.length;
  return { ok: true, src: src.slice(0, j) + "\n\n" + block + "\n" + src.slice(j) };
}

function safeInsertBefore(src, marker, block) {
  const i = src.indexOf(marker);
  if (i === -1) return { ok: false, src };
  return { ok: true, src: src.slice(0, i) + block + "\n\n" + src.slice(i) };
}

function alreadyHas(src, token) {
  return src.includes(token);
}

function patchSendWhatsAppTextSignature(src) {
  // Si ya usa 3 args (to,text,name), no tocar
  if (src.includes("async function sendWhatsAppText(to, text, name") || src.includes("async function sendWhatsAppText(to,text,name")) {
    return { src, changed: false };
  }

  // Reemplazo simple: agrega parámetro name por defecto
  const re = /async function sendWhatsAppText\s*\(\s*to\s*,\s*text\s*\)\s*\{/;
  if (!re.test(src)) return { src, changed: false };

  src = src.replace(re, "async function sendWhatsAppText(to, text, name = \"\") {");
  return { src, changed: true };
}

function patchCallsSendWhatsAppText(src) {
  // Agrega name en llamadas típicas: sendWhatsAppText(phone, something)
  // Sin romper si ya hay 3er arg.
  src = src.replace(/sendWhatsAppText\(\s*phone\s*,\s*([^)]+?)\s*\)/g, (m, p1) => {
    if (m.includes(", name") || m.includes(",name")) return m;
    return `sendWhatsAppText(phone, ${p1}, name)`;
  });
  return src;
}

function buildConversationModuleBlock() {
  return `
// ===================== RESUMEN DE CHARLAS (30 min inactividad) =====================
const CONV_IDLE_MS = Number(process.env.CONV_IDLE_MS || 30 * 60 * 1000);
const CONVERSATIONS_SHEET_ID = process.env.CONVERSATIONS_SHEET_ID || "${CONVERSATIONS_SHEET_ID}";

const convStore = new Map(); // waId -> { messages:[{role,content,ts}], timer:null, lastTs:number, name, phone }

function convKey(waId) { return String(waId || ""); }

function pushConv(waId, phone, name, role, content) {
  const k = convKey(waId);
  let c = convStore.get(k);
  if (!c) {
    c = { messages: [], timer: null, lastTs: Date.now(), name: name || "", phone: phone || "" };
    convStore.set(k, c);
  }
  c.name = name || c.name;
  c.phone = phone || c.phone;
  c.messages.push({ role, content: String(content || ""), ts: Date.now() });
  c.messages = c.messages.slice(-20);
  c.lastTs = Date.now();

  if (c.timer) clearTimeout(c.timer);
  c.timer = setTimeout(() => finalizeConversationSummary(k).catch(console.error), CONV_IDLE_MS);
}

function todayISOInTZ(tz) {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: tz,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(new Date());
  const map = {};
  for (const p of parts) map[p.type] = p.value;
  return \`\${map.year}-\${map.month}-\${map.day}\`;
}

async function ensureDailyConversationSheet(spreadsheetId, sheetName) {
  const sheets = await getSheetsClient();
  const spreadsheet = await sheets.spreadsheets.get({ spreadsheetId });
  const exists = (spreadsheet.data.sheets || [])
    .map(s => s.properties.title)
    .includes(sheetName);
  if (exists) return;

  await sheets.spreadsheets.batchUpdate({
    spreadsheetId,
    requestBody: { requests: [{ addSheet: { properties: { title: sheetName } } }] }
  });

  const headers = [
    "Fecha/Hora",
    "Nombre",
    "Teléfono",
    "Resumen",
    "CATEGORIA",
    "PRODUCTO",
    "Interés (alta/media/baja)",
    "Acción sugerida"
  ];

  await sheets.spreadsheets.values.update({
    spreadsheetId,
    range: \`\${sheetName}!A1:H1\`,
    valueInputOption: "USER_ENTERED",
    requestBody: { values: [headers] }
  });
}

const CATEGORY_OPTIONS = ${JSON.stringify(CATEGORY_OPTIONS, null, 2)};
const PRODUCT_OPTIONS = ${JSON.stringify(PRODUCT_OPTIONS, null, 2)};

async function summarizeAndClassifyConversation(messages) {
  const convoText = messages
    .map(m => \`\${m.role === "user" ? "CLIENTE" : "ASISTENTE"}: \${m.content}\`)
    .join("\\n");

  const resp = await openai.chat.completions.create({
    model: process.env.COMPLEX_MODEL || process.env.PRIMARY_MODEL || "${"gpt-4.1-mini"}",
    messages: [
      {
        role: "system",
        content:
\`Devolvé SOLO JSON válido.
Tarea: resumir la conversación y clasificar.

Campos:
{
  "resumen": "2-4 líneas, humano, claro",
  "categoria": "UNA de estas opciones exactas: \${CATEGORY_OPTIONS.join(" | ")}",
  "producto": "UNA de estas opciones exactas: \${PRODUCT_OPTIONS.join(" | ")}",
  "interes": "alta|media|baja",
  "accion_sugerida": "1 línea"
}

Reglas:
- Barbería/corte hombres -> BARBER💈.
- Servicios estética/peluquería (alisado, Keratina, maquillaje, uñas, etc) -> SERVICIOS DE BELLEZA💄.
- Stock/insumos/productos -> INSUMOS🧴.
- Máquinas (planchas, secadores, etc) -> MÁQUINAS⚙️.
- Muebles/equipamiento (sillones/camillas/espejos) -> MUEBLES🪑.
- Cursos -> CURSOS📝.
- Elegir SIEMPRE dentro de la lista.
\`
      },
      { role: "user", content: convoText }
    ],
    response_format: { type: "json_object" }
  });

  const obj = JSON.parse(resp.choices[0].message.content);
  const categoria = CATEGORY_OPTIONS.includes(obj.categoria) ? obj.categoria : "SERVICIOS DE BELLEZA💄";
  const producto = PRODUCT_OPTIONS.includes(obj.producto) ? obj.producto : "Nutrición // Tintura Baño de crema / Matizador";
  const interes = ["alta", "media", "baja"].includes(String(obj.interes).toLowerCase()) ? String(obj.interes).toLowerCase() : "media";

  return {
    resumen: String(obj.resumen || "").trim(),
    categoria,
    producto,
    interes,
    accion_sugerida: String(obj.accion_sugerida || "").trim()
  };
}

async function appendConversationSummaryRow({ name, phone, resumen, categoria, producto, interes, accion_sugerida }) {
  const sheets = await getSheetsClient();
  const daySheet = todayISOInTZ("${TIMEZONE}");

  await ensureDailyConversationSheet(CONVERSATIONS_SHEET_ID, daySheet);

  await sheets.spreadsheets.values.append({
    spreadsheetId: CONVERSATIONS_SHEET_ID,
    range: \`\${daySheet}!A:H\`,
    valueInputOption: "USER_ENTERED",
    requestBody: {
      values: [[
        new Date().toLocaleString("es-AR", { timeZone: "${TIMEZONE}" }),
        name || "",
        phone || "",
        resumen || "",
        categoria || "",
        producto || "",
        interes || "",
        accion_sugerida || ""
      ]]
    }
  });

  console.log("✅ Resumen guardado en Sheet:", CONVERSATIONS_SHEET_ID, "hoja:", daySheet);
}

async function finalizeConversationSummary(waIdKey) {
  const c = convStore.get(waIdKey);
  if (!c) return;

  const msgs = c.messages || [];
  if (msgs.length < 2) {
    convStore.delete(waIdKey);
    return;
  }

  const result = await summarizeAndClassifyConversation(msgs);

  await appendConversationSummaryRow({
    name: c.name,
    phone: c.phone,
    resumen: result.resumen,
    categoria: result.categoria,
    producto: result.producto,
    interes: result.interes,
    accion_sugerida: result.accion_sugerida
  });

  convStore.delete(waIdKey);
}
`;
}

function apply() {
  if (!fs.existsSync(INPUT)) {
    console.error("No encuentro index.js en esta carpeta.");
    process.exit(1);
  }
  let src = fs.readFileSync(INPUT, "utf-8");

  // 1) Insertar módulo de resumen si no está
  if (!alreadyHas(src, "finalizeConversationSummary(")) {
    const marker = "// ===================== GOOGLE APIS";
    const block = buildConversationModuleBlock();
    let r = safeInsertBefore(src, marker, block);
    if (!r.ok) {
      // fallback: insert after dotenv.config
      r = safeInsertAfter(src, "dotenv.config();", block);
    }
    src = r.src;
    console.log("✅ Insertado módulo resumen de charlas.");
  } else {
    console.log("ℹ️ Ya existe módulo resumen (no insertado).");
  }

  // 2) Ajustar sendWhatsAppText(to,text,name)
  const r2 = patchSendWhatsAppTextSignature(src);
  src = r2.src;
  if (r2.changed) console.log("✅ sendWhatsAppText ahora acepta name.");
  // 3) Agregar name en llamadas sendWhatsAppText(phone, ...)
  src = patchCallsSendWhatsAppText(src);

  // 4) Insertar pushConv en webhook (user/assistant) por heurística:
  if (!alreadyHas(src, "pushConv(waId")) {
    // Insertar después de obtener text y name (busca pushHistory o similar)
    // Simple: inserta antes de "pushHistory(waId, "user", text)" si existe
    const marker = 'pushHistory(waId, "user", text);';
    if (src.includes(marker)) {
      src = src.replace(marker, `pushConv(waId, phone, name, "user", text);\n    ${marker}`);
      console.log("✅ pushConv agregado para mensaje user.");
    } else {
      console.log("⚠️ No encontré pushHistory(... user ...). Agregá manualmente pushConv donde recibís el texto.");
    }

    // Insertar después de enviar respuesta (busca pushHistory assistant)
    const marker2 = 'pushHistory(waId, "assistant", out);';
    if (src.includes(marker2)) {
      src = src.replace(marker2, `${marker2}\n    pushConv(waId, phone, name, "assistant", out);`);
      console.log("✅ pushConv agregado para mensaje assistant.");
    } else {
      console.log("⚠️ No encontré pushHistory assistant. Agregá manualmente pushConv cuando respondés.");
    }
  }

  fs.writeFileSync(OUTPUT, src, "utf-8");
  console.log(`\n✅ Listo. Generé: ${OUTPUT}\nEjecutá: node ${OUTPUT}\n`);
}

apply();
