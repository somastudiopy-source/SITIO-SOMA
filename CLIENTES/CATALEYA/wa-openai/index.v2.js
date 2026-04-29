const express = require("express");
const dotenv = require("dotenv");
const { Pool } = require("pg");

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

app.listen(PORT, () => {
  console.log("✅ index.v2 base server iniciado");
  console.log(`- Puerto: ${PORT}`);
  console.log(`- DB_URL/DATABASE_URL configurado: ${DB_URL ? "sí" : "no"}`);
  console.log(`- Ping: http://localhost:${PORT}/ping`);
  console.log(`- Health: http://localhost:${PORT}/health`);
  console.log(`- Webhook verify: http://localhost:${PORT}/webhook`);
});
