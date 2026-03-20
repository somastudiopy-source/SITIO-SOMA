(function () {
  "use strict";

  const body = document.body;

  // =========================
  // Theme
  // =========================
  const themeToggle = document.getElementById("themeToggle");
  const themeIcon = document.getElementById("themeIcon");
  const logo = document.getElementById("brandLogo");

  function syncIcon() {
    const isLight = body.classList.contains("theme-light");
    if (themeIcon) themeIcon.textContent = isLight ? "☀" : "☾";
  }

  function syncLogo() {
    if (!logo) return;
    const isLight = body.classList.contains("theme-light");
    const nextSrc = isLight ? logo.dataset.light : logo.dataset.dark;
    if (nextSrc) logo.src = nextSrc;
  }

  const saved = localStorage.getItem("soma_theme");
  if (saved === "light") {
    body.classList.remove("theme-dark");
    body.classList.add("theme-light");
  } else {
    body.classList.remove("theme-light");
    body.classList.add("theme-dark");
  }
  syncIcon();
  syncLogo();

  if (themeToggle) {
    themeToggle.addEventListener("click", () => {
      const isLight = body.classList.contains("theme-light");
      body.classList.toggle("theme-light", !isLight);
      body.classList.toggle("theme-dark", isLight);
      localStorage.setItem("soma_theme", !isLight ? "light" : "dark");
      syncIcon();
      syncLogo();
    });
  }

  // =========================
  // Google Calendar
  // =========================
  let CALENDAR_ID = "";
  let CAL_TZ = "America/Argentina/Buenos_Aires";
  const CAL_HL = "es";
  let currentCalMode = "WEEK";

  const elOpenCal = document.getElementById("openCalendarBtn");
  const elCalModal = document.getElementById("calModal");
  const elCalClose = document.getElementById("calCloseBtn");
  const elCalFrame = document.getElementById("calFrame");
  const elCalWeek = document.getElementById("calWeekBtn");
  const elCalMonth = document.getElementById("calMonthBtn");

  const elCalAdd = document.getElementById("calAdd");
  const elCalAddToggle = document.getElementById("calAddToggleBtn");
  const elCalCreate = document.getElementById("calCreateBtn");
  const elCalTitle = document.getElementById("calTitle");
  const elCalDate = document.getElementById("calDate");
  const elCalStartTime = document.getElementById("calStartTime");
  const elCalEndTime = document.getElementById("calEndTime");
  const elCalDetails = document.getElementById("calDetails");

  async function ensureCalendarConfig() {
    if (CALENDAR_ID) return true;
    try {
      const r = await fetch("/api/me", { credentials: "same-origin" });
      if (!r.ok) return false;
      const j = await r.json();
      CALENDAR_ID = j.calendar_id || "";
      CAL_TZ = j.calendar_tz || CAL_TZ;
    } catch (_) {}
    return Boolean(CALENDAR_ID);
  }

  function calEmbedSrc(mode) {
    const src = encodeURIComponent(CALENDAR_ID);
    const ctz = encodeURIComponent(CAL_TZ);
    const m = encodeURIComponent(mode || "WEEK");
    return `https://calendar.google.com/calendar/embed?src=${src}&ctz=${ctz}&hl=${encodeURIComponent(CAL_HL)}&mode=${m}&showTitle=0&showPrint=0&showTabs=0&showCalendars=0&showTz=0`;
  }

  async function openCal(mode) {
    if (!elCalModal || !elCalFrame) return;
    const ok = await ensureCalendarConfig();
    if (!ok) {
      alert("No hay un calendario configurado para este usuario.");
      return;
    }
    currentCalMode = mode || currentCalMode || "WEEK";
    elCalFrame.src = calEmbedSrc(currentCalMode);
    elCalModal.classList.add("show");
    elCalModal.setAttribute("aria-hidden", "false");
  }

  function closeCal() {
    if (!elCalModal || !elCalFrame) return;
    elCalModal.classList.remove("show");
    elCalModal.setAttribute("aria-hidden", "true");
    if (elCalAdd) elCalAdd.hidden = true;
    elCalFrame.src = "about:blank";
  }

  function pad2(n) {
    return String(n).padStart(2, "0");
  }

  function toGCalDateFromParts(dateValue, timeValue) {
    if (!dateValue || !timeValue) return "";
    const [yyyy, mm, dd] = String(dateValue).split("-");
    const [hh, mi] = String(timeValue).split(":");
    if (!yyyy || !mm || !dd || !hh || !mi) return "";
    return `${yyyy}${pad2(mm)}${pad2(dd)}T${pad2(hh)}${pad2(mi)}00`;
  }

  function localDateFromParts(dateValue, timeValue) {
    if (!dateValue || !timeValue) return null;
    const d = new Date(`${dateValue}T${timeValue}:00`);
    return Number.isNaN(d.getTime()) ? null : d;
  }

  async function openCreateEvent() {
    const ok = await ensureCalendarConfig();
    if (!ok) {
      alert("No hay un calendario configurado para este usuario.");
      return;
    }

    const title = (elCalTitle?.value || "").trim();
    const date = (elCalDate?.value || "").trim();
    const startTime = (elCalStartTime?.value || "").trim();
    const endTime = (elCalEndTime?.value || "").trim();
    const details = (elCalDetails?.value || "").trim();

    const start = toGCalDateFromParts(date, startTime);
    const end = toGCalDateFromParts(date, endTime);

    if (!title || !start || !end) {
      alert("Completá: Título, Fecha, Hora inicio y Hora fin.");
      return;
    }

    const startLocal = localDateFromParts(date, startTime);
    const endLocal = localDateFromParts(date, endTime);

    if (!startLocal || !endLocal) {
      alert("Fecha u hora inválida.");
      return;
    }

    if (endLocal <= startLocal) {
      alert("La hora de fin debe ser mayor a la hora de inicio.");
      return;
    }

    const base = "https://calendar.google.com/calendar/u/0/r/eventedit";
    const params = new URLSearchParams();
    params.set("text", title);
    params.set("dates", `${start}/${end}`);
    if (details) params.set("details", details);
    params.set("ctz", CAL_TZ);
    params.set("src", CALENDAR_ID);
    params.set("sf", "true");
    params.set("output", "xml");

    window.open(`${base}?${params.toString()}`, "_blank", "noopener,noreferrer");
  }

  elOpenCal?.addEventListener("click", () => openCal("WEEK"));
  elCalClose?.addEventListener("click", closeCal);

  elCalWeek?.addEventListener("click", async () => {
    const ok = await ensureCalendarConfig();
    if (!ok) return;
    currentCalMode = "WEEK";
    if (elCalFrame) elCalFrame.src = calEmbedSrc(currentCalMode);
  });

  elCalMonth?.addEventListener("click", async () => {
    const ok = await ensureCalendarConfig();
    if (!ok) return;
    currentCalMode = "MONTH";
    if (elCalFrame) elCalFrame.src = calEmbedSrc(currentCalMode);
  });

  elCalAddToggle?.addEventListener("click", () => {
    if (!elCalAdd) return;
    elCalAdd.hidden = !elCalAdd.hidden;
  });

  elCalCreate?.addEventListener("click", openCreateEvent);

  elCalModal?.addEventListener("click", (e) => {
    if (e.target === elCalModal) closeCal();
  });

  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape" && elCalModal?.classList.contains("show")) {
      closeCal();
    }
    if (e.key === "Escape" && elWaModal?.classList.contains("show")) {
      closeWaConnectModal();
    }
  });

  // =========================
  // WhatsApp Connect / Coexistence
  // =========================
  const elOpenWaConnect = document.getElementById("openWaConnectBtn");
  const elWaModal = document.getElementById("waConnectModal");
  const elWaClose = document.getElementById("waConnectCloseBtn");
  const elWaRefresh = document.getElementById("waRefreshBtn");
  const elWaLaunch = document.getElementById("waLaunchBtn");

  const elWaStatusPill = document.getElementById("waStatusPill");
  const elWaStatusText = document.getElementById("waStatusText");
  const elWaModeValue = document.getElementById("waModeValue");
  const elWaFeatureValue = document.getElementById("waFeatureValue");
  const elWaWabaValue = document.getElementById("waWabaValue");
  const elWaPhoneIdValue = document.getElementById("waPhoneIdValue");
  const elWaDisplayNumberValue = document.getElementById("waDisplayNumberValue");
  const elWaLastEventValue = document.getElementById("waLastEventValue");
  const elWaEvents = document.getElementById("waEvents");

  let waStatusPoll = null;

  function truncateMiddle(value, keep = 8) {
    const s = String(value || "");
    if (!s) return "—";
    if (s.length <= keep * 2 + 3) return s;
    return `${s.slice(0, keep)}…${s.slice(-keep)}`;
  }

  function setWaValue(el, value, opts = {}) {
    if (!el) return;
    const raw = String(value || "").trim();
    el.textContent = raw ? (opts.short ? truncateMiddle(raw) : raw) : "—";
    if (raw) el.title = raw;
    else el.removeAttribute("title");
  }

  function setWaPill(status, configured) {
    if (!elWaStatusPill) return;
    const normalized = String(status || "").toLowerCase();

    elWaStatusPill.classList.remove("is-ok", "is-pending", "is-error");

    if (!configured) {
      elWaStatusPill.textContent = "Falta configuración";
      elWaStatusPill.classList.add("is-error");
      return;
    }

    if (normalized === "connected") {
      elWaStatusPill.textContent = "Conectado";
      elWaStatusPill.classList.add("is-ok");
      return;
    }

    if (normalized === "pending" || normalized === "started") {
      elWaStatusPill.textContent = "Pendiente";
      elWaStatusPill.classList.add("is-pending");
      return;
    }

    elWaStatusPill.textContent = "Sin conexión";
    elWaStatusPill.classList.add("is-error");
  }

  function renderWaEvents(events) {
    if (!elWaEvents) return;
    const list = Array.isArray(events) ? events : [];

    if (!list.length) {
      elWaEvents.innerHTML = `<div class="wa-event-item">Todavía no hay eventos registrados.</div>`;
      return;
    }

    elWaEvents.innerHTML = list.map((ev) => {
      const created = formatConversationTime(ev.created_utc || "");
      const name = esc(ev.event_name || "evento");
      return `
        <div class="wa-event-item">
          <strong>${name}</strong>
          <span>${esc(created || "—")}</span>
        </div>
      `;
    }).join("");
  }

  function renderWaStatus(data) {
    const configured = !!data?.configured;
    const conn = data?.connection || {};
    const status = conn.status || (configured ? "pending" : "missing");

    setWaPill(status, configured);
    setWaValue(elWaModeValue, data?.mode || "hosted");
    setWaValue(elWaFeatureValue, data?.feature || "coexistence");
    setWaValue(elWaWabaValue, conn.waba_id, { short: true });
    setWaValue(elWaPhoneIdValue, conn.phone_number_id, { short: true });
    setWaValue(elWaDisplayNumberValue, conn.display_phone_number);
    setWaValue(elWaLastEventValue, conn.last_event || (data?.events?.[0]?.event_name || ""));
    renderWaEvents(data?.events || []);

    if (!elWaStatusText) return;

    if (!configured) {
      elWaStatusText.textContent = "Para usar esta función tenés que cargar en Render la URL oficial del Embedded Signup / Hosted Signup de Meta y, si corresponde, el App ID y Config ID.";
      return;
    }

    if (status === "connected") {
      const number = conn.display_phone_number || "el número del cliente";
      elWaStatusText.textContent = `El panel ya tiene una conexión registrada para ${number}. Si el cliente vuelve a entrar al flujo oficial de Meta, puede revalidar o ajustar la coexistencia.`;
      return;
    }

    elWaStatusText.textContent = "Abrí el flujo oficial de Meta. Si la cuenta está configurada para coexistencia, Meta mostrará el QR o el paso de vinculación dentro de ese proceso.";
  }

  async function fetchWaConnectStatus() {
    const res = await fetch("/api/wa/connect-status", { credentials: "same-origin" });
    const data = await res.json().catch(() => ({}));
    renderWaStatus(data || {});
    return data || {};
  }

  async function openWaConnectModal() {
    if (!elWaModal) return;
    elWaModal.classList.add("show");
    elWaModal.setAttribute("aria-hidden", "false");
    await fetchWaConnectStatus();
    stopWaStatusPolling();
    waStatusPoll = setInterval(() => {
      fetchWaConnectStatus().catch(() => {});
    }, 8000);
  }

  function stopWaStatusPolling() {
    if (waStatusPoll) {
      clearInterval(waStatusPoll);
      waStatusPoll = null;
    }
  }

  function closeWaConnectModal() {
    if (!elWaModal) return;
    elWaModal.classList.remove("show");
    elWaModal.setAttribute("aria-hidden", "true");
    stopWaStatusPolling();
  }

  async function launchWaConnect() {
    const data = await fetchWaConnectStatus();

    if (!data?.configured || !data?.launch_url) {
      alert("Falta configurar la URL oficial de Embedded Signup / Hosted Signup en el backend.");
      return;
    }

    const popup = window.open(data.launch_url, "_blank", "noopener,noreferrer,width=520,height=780");
    if (!popup) {
      window.location.href = data.launch_url;
    }
  }

  elOpenWaConnect?.addEventListener("click", openWaConnectModal);
  elWaClose?.addEventListener("click", closeWaConnectModal);
  elWaRefresh?.addEventListener("click", () => fetchWaConnectStatus());
  elWaLaunch?.addEventListener("click", launchWaConnect);

  elWaModal?.addEventListener("click", (e) => {
    if (e.target === elWaModal) closeWaConnectModal();
  });

  window.addEventListener("message", async (event) => {
    try {
      let payload = event.data;
      if (!payload) return;

      if (typeof payload === "string") {
        try {
          payload = JSON.parse(payload);
        } catch (_) {
          return;
        }
      }

      const likelyMetaPayload =
        payload?.type === "WA_EMBEDDED_SIGNUP" ||
        payload?.waba_id ||
        payload?.phone_number_id ||
        payload?.whatsapp_business_account_id ||
        payload?.event === "FINISH" ||
        payload?.event === "PARTNER_ADDED";

      if (!likelyMetaPayload) return;

      await fetch("/api/wa/connect-session", {
        method: "POST",
        credentials: "same-origin",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ status: "pending", raw: payload }),
      });

      await fetchWaConnectStatus();
    } catch (e) {
      console.error("WA connect postMessage:", e);
    }
  });

  // =========================
  // Elements / state
  // =========================
  let selectedPeer = null;
  let convCache = [];
  let refreshBusy = false;
  let lastRenderedSignature = "";
  let autoScrollLockUntil = 0;

  const elConvs = document.getElementById("convs");
  const elQ = document.getElementById("q");
  const elSync = document.getElementById("sync");

  const elMsgs = document.getElementById("msgs");
  const elChatTitle = document.getElementById("chatTitle");
  const elChatSubtitle = document.getElementById("chatSubtitle");

  const elChatSearch = document.getElementById("chatSearch");
  const elFindUp = document.getElementById("findUp");
  const elFindDown = document.getElementById("findDown");
  const elFindCount = document.getElementById("findCount");

  const elDeleteChat = document.getElementById("deleteChatBtn");

  const elEmojiBtn = document.getElementById("emojiBtn");
  const elEmojiPanel = document.getElementById("emojiPanel");

  const elAttachBtn = document.getElementById("attachBtn");
  const elFile = document.getElementById("file");

  const elFileChip = document.getElementById("fileChip");
  const elFileChipName = document.getElementById("fileChipName");
  const elFileChipClear = document.getElementById("fileChipClear");

  const elText = document.getElementById("text");
  const elSend = document.getElementById("send");
  const elSendResult = document.getElementById("sendResult");

  // Paging
  let hasMore = false;
  let oldestId = null;
  let loadingMore = false;

  // Find state
  let findQuery = "";
  let hits = [];
  let hitIndex = -1;

  function esc(s) {
    return String(s || "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#039;");
  }

  function nl2br(str) {
    return String(str || "").replace(/\n/g, "<br>");
  }

  function parseServerDate(ts) {
    if (!ts) return null;

    if (ts instanceof Date && Number.isFinite(ts.getTime())) return ts;

    if (typeof ts === "number") {
      const n = ts < 1000000000000 ? ts * 1000 : ts;
      const dNum = new Date(n);
      if (Number.isFinite(dNum.getTime())) return dNum;
    }

    if (typeof ts === "string") {
      const s = ts.trim();

      if (/^\d+$/.test(s)) {
        const n = Number(s);
        const normalized = s.length <= 10 ? n * 1000 : n;
        const dUnix = new Date(normalized);
        if (Number.isFinite(dUnix.getTime())) return dUnix;
      }

      const direct = new Date(s);
      if (Number.isFinite(direct.getTime())) return direct;

      const fixed1 = s.replace(" ", "T");
      const d1 = new Date(fixed1);
      if (Number.isFinite(d1.getTime())) return d1;

      const d2 = new Date(fixed1 + "Z");
      if (Number.isFinite(d2.getTime())) return d2;
    }

    return null;
  }

  function formatMessageHour(ts) {
    const d = parseServerDate(ts);
    if (!d) return "";

    return d.toLocaleTimeString("es-AR", {
      hour: "2-digit",
      minute: "2-digit",
      hour12: false,
      timeZone: "America/Argentina/Buenos_Aires",
    });
  }

  function formatConversationTime(ts) {
    const d = parseServerDate(ts);
    if (!d) return "";

    const tz = "America/Argentina/Buenos_Aires";
    const now = new Date();
    const dArg = new Date(d.toLocaleString("en-US", { timeZone: tz }));
    const nowArg = new Date(now.toLocaleString("en-US", { timeZone: tz }));

    const sameDay =
      dArg.getDate() === nowArg.getDate() &&
      dArg.getMonth() === nowArg.getMonth() &&
      dArg.getFullYear() === nowArg.getFullYear();

    const yesterday = new Date(nowArg);
    yesterday.setDate(nowArg.getDate() - 1);

    const isYesterday =
      dArg.getDate() === yesterday.getDate() &&
      dArg.getMonth() === yesterday.getMonth() &&
      dArg.getFullYear() === yesterday.getFullYear();

    if (sameDay) {
      return new Intl.DateTimeFormat("es-AR", {
        hour: "2-digit",
        minute: "2-digit",
        hour12: false,
        timeZone: tz,
      }).format(d);
    }

    if (isYesterday) return "AYER";

    return new Intl.DateTimeFormat("es-AR", {
      day: "2-digit",
      month: "2-digit",
      year: "2-digit",
      timeZone: tz,
    }).format(d);
  }

  function getConversationTime(c) {
    return (
      c.day_label ||
      formatConversationTime(
        c.last_ts || c.last_message_ts || c.updated_at || c.created_at || c.ts_utc
      )
    );
  }

  function getMessageTimestamp(m) {
    return (
      m.ts_utc ||
      m.timestamp ||
      m.created_at ||
      m.sent_at ||
      m.ts ||
      m.date ||
      m.datetime ||
      m.raw_ts ||
      m.raw_json?.ts_utc ||
      m.raw_json?.timestamp ||
      m.raw_json?.created_at ||
      m.raw_json?.sent_at ||
      m.raw_json?.ts ||
      m.raw_json?.date ||
      null
    );
  }

  function getMediaUrl(m) {
    const candidates = [
      m.media_url,
      m.file_url,
      m.url,
      m.image_url,
      m.photo_url,
      m.media?.url,
      m.media?.path,
      m.raw_json?.media_url,
      m.raw_json?.file_url,
      m.raw_json?.url,
      m.raw_json?.image_url,
      m.raw_json?.photo_url,
      m.raw_json?.media?.url,
      m.raw_json?.media?.path,
      m.path,
    ].filter(Boolean);

    if (!candidates.length) return "";

    const raw = String(candidates[0]);

    if (/^https?:\/\//i.test(raw) || raw.startsWith("blob:") || raw.startsWith("data:")) {
      return raw;
    }
    if (raw.startsWith("/")) return raw;

    return `/${raw.replace(/^\/+/, "")}`;
  }

  function showResult(msg, isError) {
    if (!elSendResult) return;
    elSendResult.textContent = msg || "";
    elSendResult.style.color = isError ? "#d94b4b" : "";
  }

  function setEnabledForChat(enabled) {
    if (elSend) elSend.disabled = !enabled;
    if (elEmojiBtn) elEmojiBtn.disabled = !enabled;
    if (elAttachBtn) elAttachBtn.disabled = !enabled;
    if (elDeleteChat) elDeleteChat.disabled = !enabled;
  }

  function updateComposerState() {
    const hasChat = !!selectedPeer;
    const hasText = !!(elText?.value || "").trim();
    const hasFile = !!(elFile?.files && elFile.files.length > 0);

    if (elSend) elSend.disabled = !(hasChat && (hasText || hasFile));
    if (elEmojiBtn) elEmojiBtn.disabled = !hasChat;
    if (elAttachBtn) elAttachBtn.disabled = !hasChat;
    if (elDeleteChat) elDeleteChat.disabled = !hasChat;
  }

  function setSyncOk() {
    if (elSync) {
      elSync.textContent =
        "OK • " +
        new Date().toLocaleTimeString("es-AR", {
          hour: "2-digit",
          minute: "2-digit",
        });
    }
  }

  function scrollToBottom() {
    if (!elMsgs) return;
    elMsgs.scrollTop = elMsgs.scrollHeight;
  }

  function scrollToBottomForce() {
    if (!elMsgs) return;

    autoScrollLockUntil = Date.now() + 700;

    const go = () => {
      if (!elMsgs) return;
      elMsgs.scrollTop = elMsgs.scrollHeight;
    };

    go();
    requestAnimationFrame(go);
    setTimeout(go, 50);
    setTimeout(go, 140);
    setTimeout(go, 260);
  }

  function isNearBottom(el) {
    if (!el) return true;
    return el.scrollHeight - el.scrollTop - el.clientHeight < 120;
  }

  function buildMessagesSignature(msgs) {
    if (!Array.isArray(msgs) || !msgs.length) return "empty";

    const first = msgs[0];
    const last = msgs[msgs.length - 1];

    return JSON.stringify({
      count: msgs.length,
      firstId: first?.id || null,
      lastId: last?.id || null,
      lastTs: getMessageTimestamp(last) || "",
      lastText: last?.text || "",
      lastMedia: getMediaUrl(last) || "",
      lastType: last?.msg_type || last?.type || "",
    });
  }

  // =========================
  // Conversations
  // =========================
  async function loadConversations() {
    const res = await fetch("/api/conversations?limit=400", { credentials: "same-origin" });
    const data = await res.json();
    convCache = Array.isArray(data.conversations) ? data.conversations : [];

    if (selectedPeer && !convCache.some((c) => c.wa_peer === selectedPeer)) {
      selectedPeer = null;
      setEnabledForChat(false);
      if (elChatTitle) elChatTitle.textContent = "Seleccioná un chat";
      if (elChatSubtitle) elChatSubtitle.textContent = "—";
      if (elMsgs) {
        elMsgs.innerHTML = `<div class="center-hint">Elegí una conversación para ver los mensajes.</div>`;
      }
    }

    renderConversations();
    setSyncOk();
  }

  function renderConversations() {
    const q = (elQ?.value || "").toLowerCase().trim();
    if (!elConvs) return;
    elConvs.innerHTML = "";

    const filtered = convCache.filter((c) => {
      const name = (c.name || "").toLowerCase();
      const peer = (c.wa_peer || "").toLowerCase();
      const last = (c.last_text || "").toLowerCase();
      return !q || name.includes(q) || peer.includes(q) || last.includes(q);
    });

    if (filtered.length === 0) {
      elConvs.innerHTML = `<div style="padding:14px;opacity:.75;">Sin conversaciones.</div>`;
      return;
    }

    filtered.forEach((c) => {
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "conv" + (c.wa_peer === selectedPeer ? " active" : "");

      btn.innerHTML = `
        <div class="avatar">${esc((c.name || c.wa_peer || "?").slice(0, 1).toUpperCase())}</div>
        <div class="conv-main">
          <div class="conv-top">
            <div class="conv-name">${esc(c.name || c.wa_peer || "Sin nombre")}</div>
            <div class="conv-time">${esc(getConversationTime(c))}</div>
          </div>
          <div class="conv-last">${esc(c.last_text || "Sin mensajes")}</div>
        </div>
        ${Number(c.unread || 0) > 0 ? `<div class="unread">${Number(c.unread || 0)}</div>` : ``}
      `;

      btn.onclick = async () => {
        selectedPeer = c.wa_peer;
        if (elChatTitle) elChatTitle.textContent = c.name || c.wa_peer;
        if (elChatSubtitle) elChatSubtitle.textContent = c.wa_peer;
        setEnabledForChat(true);
        closeEmoji();
        clearFileChip();
        await openConversation(selectedPeer);
        renderConversations();
      };

      elConvs.appendChild(btn);
    });
  }

  async function openConversation(peer) {
    await fetch("/api/mark_read", {
      method: "POST",
      credentials: "same-origin",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ wa_peer: peer }),
    }).catch(() => {});

    resetFind();
    if (elMsgs) elMsgs.innerHTML = "";
    oldestId = null;
    lastRenderedSignature = "";
    await loadLatest(peer);
    await loadConversations();
    scrollToBottomForce();
  }

  async function loadLatest(peer) {
    const url = new URL(location.origin + "/api/chat");
    url.searchParams.set("wa_peer", peer);
    url.searchParams.set("limit", "50");

    const res = await fetch(url.toString(), { credentials: "same-origin" });
    const data = await res.json();
    hasMore = !!data.has_more;

    const msgs = Array.isArray(data.messages) ? data.messages : [];
    renderMessagesReplace(msgs);
    lastRenderedSignature = buildMessagesSignature(msgs);

    if (msgs.length) oldestId = msgs[0].id;
    scrollToBottomForce();
    applyFind(findQuery);
  }

  async function loadMoreOlder(peer) {
    if (!hasMore || loadingMore || !oldestId) return;
    loadingMore = true;

    const prevScrollHeight = elMsgs ? elMsgs.scrollHeight : 0;

    try {
      const url = new URL(location.origin + "/api/chat");
      url.searchParams.set("wa_peer", peer);
      url.searchParams.set("limit", "50");
      url.searchParams.set("before_id", String(oldestId));

      const res = await fetch(url.toString(), { credentials: "same-origin" });
      const data = await res.json();
      hasMore = !!data.has_more;

      const msgs = Array.isArray(data.messages) ? data.messages : [];
      if (msgs.length) oldestId = msgs[0].id;

      renderMessagesPrepend(msgs);

      if (elMsgs) {
        const newScrollHeight = elMsgs.scrollHeight;
        elMsgs.scrollTop = newScrollHeight - prevScrollHeight;
      }
    } finally {
      loadingMore = false;
    }

    applyFind(findQuery);
  }

  function renderMessagesReplace(msgs) {
    if (!elMsgs) return;

    if (!msgs.length) {
      elMsgs.innerHTML = `<div class="center-hint">Sin mensajes.</div>`;
      return;
    }

    elMsgs.innerHTML = "";
    msgs.forEach((m) => elMsgs.appendChild(messageNode(m)));
  }

  function renderMessagesPrepend(msgs) {
    if (!elMsgs || !msgs.length) return;
    const frag = document.createDocumentFragment();
    msgs.forEach((m) => frag.appendChild(messageNode(m)));
    elMsgs.prepend(frag);
  }

  function isImageMessage(m) {
    const msgType = String(
      m.msg_type ||
      m.type ||
      m.media_kind ||
      m.raw_json?.msg_type ||
      m.raw_json?.type ||
      m.raw_json?.media_kind ||
      ""
    ).toLowerCase();

    const mediaKind = String(
      m.media_kind ||
      m.raw_json?.media_kind ||
      m.media?.kind ||
      m.raw_json?.media?.kind ||
      ""
    ).toLowerCase();

    const contentType = String(
      m.content_type ||
      m.raw_json?.content_type ||
      m.media?.content_type ||
      m.raw_json?.media?.content_type ||
      ""
    ).toLowerCase();

    const mediaUrl = String(getMediaUrl(m) || "").toLowerCase();

    return (
      msgType === "image" ||
      mediaKind === "image" ||
      contentType.startsWith("image/") ||
      mediaUrl.endsWith(".png") ||
      mediaUrl.endsWith(".jpg") ||
      mediaUrl.endsWith(".jpeg") ||
      mediaUrl.endsWith(".webp") ||
      mediaUrl.endsWith(".gif")
    );
  }

  function messageNode(m) {
    const row = document.createElement("div");
    const direction = m.direction === "in" ? "in" : "out";
    row.className = "bubble-row " + direction;

    const b = document.createElement("div");
    b.className = "bubble " + direction;

    const mediaUrl = getMediaUrl(m);
    let mediaHtml = "";

    if (mediaUrl) {
      if (isImageMessage(m)) {
        mediaHtml = `<img class="media-preview" src="${esc(mediaUrl)}" alt="imagen" loading="lazy">`;
      } else {
        mediaHtml = `<div style="margin-bottom:8px;"><a href="${esc(mediaUrl)}" target="_blank" rel="noopener noreferrer" style="color:inherit;opacity:.9;text-decoration:none;">📎 Abrir archivo</a></div>`;
      }
    }

    const rawText = String(m.text || "");
    const hour = formatMessageHour(getMessageTimestamp(m));

    b.innerHTML = `
      ${mediaHtml}
      <div class="txt" data-msgtext="1" data-rawtext="${esc(rawText)}">${nl2br(esc(rawText))}</div>
      <div class="meta">${esc(hour || "--:--")}</div>
    `;

    row.appendChild(b);

    const img = row.querySelector("img.media-preview");
    if (img) {
      img.addEventListener("load", () => {
        if (Date.now() < autoScrollLockUntil || isNearBottom(elMsgs)) {
          scrollToBottomForce();
        }
      });
    }

    return row;
  }

  elMsgs?.addEventListener("scroll", async () => {
    if (!selectedPeer) return;
    if (Date.now() < autoScrollLockUntil) return;
    if (elMsgs.scrollTop <= 40) await loadMoreOlder(selectedPeer);
  });

  async function refreshSelectedConversation() {
    if (!selectedPeer || refreshBusy) return;
    refreshBusy = true;

    try {
      const keepBottom = isNearBottom(elMsgs);

      const url = new URL(location.origin + "/api/chat");
      url.searchParams.set("wa_peer", selectedPeer);
      url.searchParams.set("limit", "50");

      const res = await fetch(url.toString(), { credentials: "same-origin" });
      const data = await res.json();
      const msgs = Array.isArray(data.messages) ? data.messages : [];

      const nextSignature = buildMessagesSignature(msgs);

      if (nextSignature !== lastRenderedSignature) {
        renderMessagesReplace(msgs);
        lastRenderedSignature = nextSignature;

        if (msgs.length) oldestId = msgs[0].id;
        hasMore = !!data.has_more;

        if (keepBottom) {
          scrollToBottomForce();
        }

        applyFind(findQuery);
      }
    } catch (e) {
      console.error("refreshSelectedConversation:", e);
    } finally {
      refreshBusy = false;
    }
  }

  // =========================
  // Emojis
  // =========================
  const EMOJIS = [
    "😀", "😁", "😂", "🤣", "😊", "😍", "😘", "😎",
    "🙂", "😉", "😅", "😇", "🤩", "🥳", "😴", "🤔",
    "👍", "👎", "👏", "🙏", "💪", "🔥", "✨", "💯",
    "❤️", "💙", "💜", "🩷", "🧡", "💛", "💚", "🤍",
    "🎉", "✅", "❌", "⚠️", "📎", "📷", "🧾", "📄"
  ];

  function buildEmojiPanel() {
    if (!elEmojiPanel) return;
    elEmojiPanel.innerHTML = `
      <div class="emoji-grid">
        ${EMOJIS.map((e) => `<button class="emoji" type="button" data-e="${esc(e)}">${esc(e)}</button>`).join("")}
      </div>
    `;

    elEmojiPanel.querySelectorAll(".emoji").forEach((btn) => {
      btn.addEventListener("click", () => {
        const e = btn.getAttribute("data-e") || "";
        insertAtCursor(elText, e);
        elText?.focus();
      });
    });
  }

  function openEmoji() {
    if (elEmojiPanel) elEmojiPanel.style.display = "block";
  }

  function closeEmoji() {
    if (elEmojiPanel) elEmojiPanel.style.display = "none";
  }

  function toggleEmoji() {
    if (!elEmojiPanel) return;
    const open = getComputedStyle(elEmojiPanel).display !== "none";
    open ? closeEmoji() : openEmoji();
  }

  function insertAtCursor(textarea, text) {
    if (!textarea) return;
    const start = textarea.selectionStart ?? textarea.value.length;
    const end = textarea.selectionEnd ?? textarea.value.length;
    const before = textarea.value.slice(0, start);
    const after = textarea.value.slice(end);
    textarea.value = before + text + after;
    const pos = start + text.length;
    textarea.setSelectionRange(pos, pos);
    updateComposerState();
  }

  elEmojiBtn?.addEventListener("click", toggleEmoji);

  document.addEventListener("click", (e) => {
    if (!elEmojiPanel || !elEmojiBtn) return;
    const t = e.target;
    if (elEmojiPanel.contains(t) || elEmojiBtn.contains(t)) return;
    closeEmoji();
  });

  buildEmojiPanel();
  closeEmoji();

  // =========================
  // Attach
  // =========================
  function setFileChip(name) {
    if (!elFileChip || !elFileChipName) return;
    elFileChipName.textContent = name || "";
    elFileChip.style.display = name ? "flex" : "none";
    updateComposerState();
  }

  function clearFileChip() {
    setFileChip("");
    if (elFile) elFile.value = "";
  }

  elAttachBtn?.addEventListener("click", () => {
    if (!selectedPeer) return;
    elFile?.click();
  });

  elFile?.addEventListener("change", () => {
    const f = elFile.files && elFile.files[0];
    if (f) setFileChip(f.name);
    else clearFileChip();
  });

  elFileChipClear?.addEventListener("click", clearFileChip);

  // =========================
  // Send
  // =========================
  async function uploadIfNeeded() {
    if (!elFile || !elFile.files || elFile.files.length === 0) return null;

    const file = elFile.files[0];
    const fd = new FormData();
    fd.append("file", file);

    showResult("Subiendo archivo…", false);

    const res = await fetch("/api/upload", {
      method: "POST",
      credentials: "same-origin",
      body: fd,
    });

    const data = await res.json().catch(() => ({}));

    if (!res.ok || !data.ok) {
      throw new Error(data.error || "No se pudo subir el archivo");
    }

    return {
      filename: data.filename || file.name,
      content_type: data.content_type || file.type || "application/octet-stream",
      media_url: data.media_url || data.file_url || data.url || data.path || null,
      media_id: data.media_id || data.id || null,
      raw: data,
    };
  }

  async function sendMessage() {
    if (!selectedPeer || !elSend) return;

    const text = (elText?.value || "").trim();
    const hasFile = !!(elFile?.files?.length);
    if (!text && !hasFile) return;

    elSend.disabled = true;

    try {
      const upload = hasFile ? await uploadIfNeeded() : null;

      showResult("Enviando…", false);

      const payload = {
        to: selectedPeer,
        text,
        filename: upload ? upload.filename : null,
        content_type: upload ? upload.content_type : null,
        media_url: upload ? upload.media_url : null,
        media_id: upload ? upload.media_id : null,
      };

      const res = await fetch("/api/send", {
        method: "POST",
        credentials: "same-origin",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });

      const data = await res.json().catch(() => ({}));

      if (!res.ok || !data.ok) {
        throw new Error(data.error || "No se pudo enviar");
      }

      showResult("Enviado ✅", false);

      if (elText) elText.value = "";
      clearFileChip();

      oldestId = null;
      lastRenderedSignature = "";
      await loadLatest(selectedPeer);
      await loadConversations();
      scrollToBottomForce();
    } catch (e) {
      showResult("Error: " + (e?.message || "Error inesperado"), true);
    } finally {
      updateComposerState();
    }
  }

  elSend?.addEventListener("click", sendMessage);

  elText?.addEventListener("input", updateComposerState);

  elText?.addEventListener("keydown", (e) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      sendMessage();
    }
  });

  // =========================
  // Delete conversation
  // =========================
  elDeleteChat?.addEventListener("click", async () => {
    if (!selectedPeer) return;

    const ok = confirm("¿Eliminar este chat completo? (Se borra del panel)");
    if (!ok) return;

    const res = await fetch("/api/delete_conversation", {
      method: "POST",
      credentials: "same-origin",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ wa_peer: selectedPeer }),
    });

    const data = await res.json().catch(() => ({}));

    if (!res.ok || !data.ok) {
      alert("No se pudo eliminar.");
      return;
    }

    selectedPeer = null;
    lastRenderedSignature = "";
    setEnabledForChat(false);
    closeEmoji();
    clearFileChip();
    resetFind();

    if (elChatTitle) elChatTitle.textContent = "Seleccioná un chat";
    if (elChatSubtitle) elChatSubtitle.textContent = "—";
    if (elMsgs) {
      elMsgs.innerHTML = `<div class="center-hint">Elegí una conversación para ver los mensajes.</div>`;
    }

    await loadConversations();
  });

  // =========================
  // Find in chat
  // =========================
  function resetFind() {
    findQuery = "";
    hits = [];
    hitIndex = -1;
    if (elFindCount) elFindCount.textContent = "0/0";
    if (elChatSearch) elChatSearch.value = "";
    clearHighlights();
  }

  function clearHighlights() {
    if (!elMsgs) return;

    elMsgs.querySelectorAll(".txt[data-msgtext='1']").forEach((txtEl) => {
      const raw = txtEl.getAttribute("data-rawtext") || "";
      txtEl.innerHTML = nl2br(esc(raw));
    });
  }

  function highlightTextInElement(el, query) {
    const raw = el.getAttribute("data-rawtext") || el.textContent || "";
    if (!query) {
      el.innerHTML = nl2br(esc(raw));
      return;
    }

    const lower = raw.toLowerCase();
    const ql = query.toLowerCase();

    let idx = lower.indexOf(ql);
    if (idx === -1) {
      el.innerHTML = nl2br(esc(raw));
      return;
    }

    let html = "";
    let last = 0;

    while (idx !== -1) {
      html += nl2br(esc(raw.slice(last, idx)));
      html += `<mark class="find-hit">${esc(raw.slice(idx, idx + query.length))}</mark>`;
      last = idx + query.length;
      idx = lower.indexOf(ql, last);
    }

    html += nl2br(esc(raw.slice(last)));
    el.innerHTML = html;
  }

  function applyFind(query) {
    findQuery = (query || "").trim();
    clearHighlights();
    hits = [];
    hitIndex = -1;

    if (!findQuery || !elMsgs) {
      if (elFindCount) elFindCount.textContent = "0/0";
      return;
    }

    elMsgs.querySelectorAll(".txt[data-msgtext='1']").forEach((txtEl) => {
      highlightTextInElement(txtEl, findQuery);
    });

    hits = Array.from(elMsgs.querySelectorAll("mark.find-hit"));

    if (!hits.length) {
      if (elFindCount) elFindCount.textContent = "0/0";
      return;
    }

    hitIndex = 0;
    focusHit(hitIndex);
  }

  function focusHit(i) {
    if (!hits.length) return;
    hits.forEach((h) => h.classList.remove("active"));
    const h = hits[i];
    if (!h) return;
    h.classList.add("active");
    h.scrollIntoView({ block: "center", behavior: "smooth" });
    if (elFindCount) elFindCount.textContent = `${i + 1}/${hits.length}`;
  }

  async function findNext() {
    if (!selectedPeer || !findQuery) return;
    if (!hits.length) applyFind(findQuery);
    if (!hits.length) return;

    if (hitIndex < hits.length - 1) {
      hitIndex += 1;
      focusHit(hitIndex);
      return;
    }

    if (hasMore) {
      await loadMoreOlder(selectedPeer);
      applyFind(findQuery);
    }
  }

  async function findPrev() {
    if (!selectedPeer || !findQuery) return;
    if (!hits.length) applyFind(findQuery);
    if (!hits.length) return;

    if (hitIndex > 0) {
      hitIndex -= 1;
      focusHit(hitIndex);
      return;
    }

    if (hasMore) {
      await loadMoreOlder(selectedPeer);
      applyFind(findQuery);
      if (hits.length) {
        hitIndex = hits.length - 1;
        focusHit(hitIndex);
      }
    }
  }

  let findTimer = null;

  elChatSearch?.addEventListener("input", () => {
    clearTimeout(findTimer);
    findTimer = setTimeout(() => {
      applyFind(elChatSearch.value || "");
    }, 200);
  });

  elFindDown?.addEventListener("click", findNext);
  elFindUp?.addEventListener("click", findPrev);

  elChatSearch?.addEventListener("keydown", (e) => {
    if (e.key === "Enter") {
      e.preventDefault();
      findNext();
    }
  });

  // =========================
  // Sidebar search
  // =========================
  elQ?.addEventListener("input", renderConversations);

  // =========================
  // Notifications
  // =========================
  function requestBrowserNotifications() {
    if (!("Notification" in window)) return;
    if (Notification.permission === "default") {
      Notification.requestPermission().catch(() => {});
    }
  }

  requestBrowserNotifications();

  // =========================
  // Start
  // =========================
  setEnabledForChat(false);
  updateComposerState();

  if (elMsgs && !elMsgs.innerHTML.trim()) {
    elMsgs.innerHTML = `<div class="center-hint">Elegí una conversación para ver los mensajes.</div>`;
  }

  loadConversations().catch(console.error);

  setInterval(async () => {
    try {
      await loadConversations();
      await refreshSelectedConversation();
    } catch (e) {
      console.error(e);
    }
  }, 1500);
})();
