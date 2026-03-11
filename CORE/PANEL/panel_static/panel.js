(function () {
  "use strict";

  const body = document.body;

  // Theme
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
    logo.src = isLight ? logo.dataset.light : logo.dataset.dark;
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

    const direct = new Date(ts);
    if (Number.isFinite(direct.getTime())) return direct;

    if (typeof ts === "string") {
      const fixed = ts.replace(" ", "T");
      const d2 = new Date(fixed);
      if (Number.isFinite(d2.getTime())) return d2;

      const d3 = new Date(fixed + "Z");
      if (Number.isFinite(d3.getTime())) return d3;
    }

    return null;
  }

function formatMessageHour(ts) {

  if (!ts) return "";

  const d = new Date(ts);

  if (!Number.isFinite(d.getTime())) return "";

  return d.toLocaleTimeString("es-AR", {
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
    timeZone: "America/Argentina/Buenos_Aires"
  });

}

  function fmtDayLabel(ts) {
    const d = parseServerDate(ts);
    if (!d) return "";

    const now = new Date();

    const dArg = new Date(
      d.toLocaleString("en-US", { timeZone: "America/Argentina/Buenos_Aires" })
    );
    const nowArg = new Date(
      now.toLocaleString("en-US", { timeZone: "America/Argentina/Buenos_Aires" })
    );

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

    if (sameDay) return fmtHour(ts);
    if (isYesterday) return "AYER";

    return dArg.toLocaleDateString("es-AR", {
      day: "2-digit",
      month: "2-digit",
      year: "2-digit",
      timeZone: "America/Argentina/Buenos_Aires",
    });
  }

  function getConversationTime(c) {
    const candidate =
      c?.last_ts ||
      c?.last_message_ts ||
      c?.ts_utc ||
      c?.updated_at ||
      c?.created_at ||
      null;

    return fmtDayLabel(candidate);
  }

  function setSync(text) {
    const elSync = document.getElementById("sync");
    if (elSync) elSync.textContent = text || "";
  }

  function setSyncOk() {
    setSync(
      "OK • " +
        new Date().toLocaleTimeString("es-AR", {
          hour: "2-digit",
          minute: "2-digit",
        })
    );
  }

  function showResult(msg, isError) {
    const elSendResult = document.getElementById("sendResult");
    if (!elSendResult) return;
    elSendResult.textContent = msg || "";
    elSendResult.style.color = isError ? "#d94b4b" : "";
  }

  function scrollToBottom() {
    const elMsgs = document.getElementById("msgs");
    if (!elMsgs) return;
    elMsgs.scrollTop = elMsgs.scrollHeight;
  }

  function isNearBottom(el) {
    if (!el) return true;
    return el.scrollHeight - el.scrollTop - el.clientHeight < 120;
  }

  let selectedPeer = null;
  let convCache = [];
  let currentMessages = [];
  let refreshBusy = false;

  const elConvs = document.getElementById("convs");
  const elQ = document.getElementById("q");
  const elMsgs = document.getElementById("msgs");
  const elChatTitle = document.getElementById("chatTitle");
  const elChatSubtitle = document.getElementById("chatSubtitle");
  const elDeleteChat = document.getElementById("deleteChatBtn");

  const elChatSearch = document.getElementById("chatSearch");
  const elFindUp = document.getElementById("findUp");
  const elFindDown = document.getElementById("findDown");
  const elFindCount = document.getElementById("findCount");

  const elEmojiBtn = document.getElementById("emojiBtn");
  const elEmojiPanel = document.getElementById("emojiPanel");

  const elAttachBtn = document.getElementById("attachBtn");
  const elFile = document.getElementById("file");
  const elFileChip = document.getElementById("fileChip");
  const elFileChipName = document.getElementById("fileChipName");
  const elFileChipClear = document.getElementById("fileChipClear");

  const elText = document.getElementById("text");
  const elSend = document.getElementById("send");

  let CALENDAR_ID = "";
  let CAL_TZ = "America/Argentina/Buenos_Aires";
  const CAL_HL = "es";

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

  let findQuery = "";
  let hits = [];
  let hitIndex = -1;

  async function ensureCalendarConfig() {
    if (CALENDAR_ID) return true;
    try {
      const r = await fetch("/api/me", { credentials: "same-origin" });
      const j = await r.json();
      CALENDAR_ID = j.calendar_id || "";
      CAL_TZ = j.calendar_tz || CAL_TZ;
    } catch (_) {}
    return Boolean(CALENDAR_ID);
  }

  function calEmbedSrc(mode) {
    const src = encodeURIComponent(CALENDAR_ID);
    const ctz = encodeURIComponent(CAL_TZ);
    return `https://calendar.google.com/calendar/embed?src=${src}&ctz=${ctz}&hl=${encodeURIComponent(CAL_HL)}&mode=${mode}&showTitle=0&showPrint=0&showTabs=0&showCalendars=0&showTz=0`;
  }

  async function openCal(mode) {
    if (!elCalModal || !elCalFrame) return;
    const ok = await ensureCalendarConfig();
    if (!ok) {
      alert("No hay un calendario configurado para este usuario.");
      return;
    }
    elCalFrame.src = calEmbedSrc(mode || "WEEK");
    elCalModal.classList.add("show");
    elCalModal.classList.add("open");
    elCalModal.setAttribute("aria-hidden", "false");
  }

  function closeCal() {
    if (!elCalModal || !elCalFrame) return;
    elCalModal.classList.remove("show");
    elCalModal.classList.remove("open");
    elCalModal.setAttribute("aria-hidden", "true");
    if (elCalAdd) elCalAdd.hidden = true;
    elCalFrame.src = "about:blank";
  }

  function toGCalDateFromParts(dateValue, timeValue) {
    if (!dateValue || !timeValue) return "";
    const d = new Date(`${dateValue}T${timeValue}`);
    if (Number.isNaN(d.getTime())) return "";
    const yyyy = d.getUTCFullYear();
    const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
    const dd = String(d.getUTCDate()).padStart(2, "0");
    const hh = String(d.getUTCHours()).padStart(2, "0");
    const mi = String(d.getUTCMinutes()).padStart(2, "0");
    const ss = "00";
    return `${yyyy}${mm}${dd}T${hh}${mi}${ss}Z`;
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

    const base = "https://calendar.google.com/calendar/u/0/r/eventedit";
    const params = new URLSearchParams();
    params.set("text", title);
    params.set("dates", `${start}/${end}`);
    if (details) params.set("details", details);
    params.set("ctz", CAL_TZ);
    params.set("src", CALENDAR_ID);
    params.set("sf", "true");
    params.set("output", "xml");

    window.open(`${base}?${params.toString()}`, "_blank", "noopener");
  }

  elOpenCal?.addEventListener("click", () => openCal("WEEK"));
  elCalClose?.addEventListener("click", closeCal);

  elCalWeek?.addEventListener("click", async () => {
    const ok = await ensureCalendarConfig();
    if (!ok) return;
    if (elCalFrame) elCalFrame.src = calEmbedSrc("WEEK");
  });

  elCalMonth?.addEventListener("click", async () => {
    const ok = await ensureCalendarConfig();
    if (!ok) return;
    if (elCalFrame) elCalFrame.src = calEmbedSrc("MONTH");
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
    if (e.key === "Escape") closeCal();
  });

  function setEnabledForChat(enabled) {
    if (elSend) elSend.disabled = !enabled;
    if (elEmojiBtn) elEmojiBtn.disabled = !enabled;
    if (elAttachBtn) elAttachBtn.disabled = !enabled;
    if (elDeleteChat) elDeleteChat.disabled = !enabled;
    if (elText) elText.disabled = !enabled;
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

  elText?.addEventListener("input", updateComposerState);

  async function loadConversations() {
    const res = await fetch("/api/conversations?limit=400", {
      credentials: "same-origin",
    });
    const data = await res.json();
    convCache = Array.isArray(data.conversations) ? data.conversations : [];
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
      btn.className = "conv-row" + (c.wa_peer === selectedPeer ? " active" : "");

      const timeText = getConversationTime(c);

      btn.innerHTML = `
        <div class="conv-avatar">${esc((c.name || c.wa_peer || "?").slice(0, 1).toUpperCase())}</div>
        <div class="conv-main">
          <div class="conv-top">
            <div class="conv-name">${esc(c.name || c.wa_peer || "Sin nombre")}</div>
            <div class="conv-time">${esc(timeText)}</div>
          </div>
          <div class="conv-bottom">
            <div class="conv-preview">${esc(c.last_text || "Sin mensajes")}</div>
            ${Number(c.unread || 0) > 0 ? `<div class="conv-badge">${Number(c.unread || 0)}</div>` : ``}
          </div>
        </div>
      `;

      btn.addEventListener("click", async () => {
        selectedPeer = c.wa_peer;
        if (elChatTitle) elChatTitle.textContent = c.name || c.wa_peer;
        if (elChatSubtitle) elChatSubtitle.textContent = c.wa_peer;
        setEnabledForChat(true);
        closeEmoji();
        clearFileChip();
        renderConversations();
        await openConversation(selectedPeer, true);
      });

      elConvs.appendChild(btn);
    });
  }

  elQ?.addEventListener("input", renderConversations);

function isImageMessage(m) {
  const type = String(m.msg_type || "").toLowerCase();
  const kind = String(m.media_kind || "").toLowerCase();
  const content = String(m.content_type || "").toLowerCase();
  const url = String(m.media_url || "").toLowerCase();

  if (type === "image") return true;
  if (kind === "image") return true;
  if (content.startsWith("image/")) return true;

  if (
    url.endsWith(".png") ||
    url.endsWith(".jpg") ||
    url.endsWith(".jpeg") ||
    url.endsWith(".webp") ||
    url.endsWith(".gif")
  ) {
    return true;
  }

  return false;
}

    if (text.includes("<imagen>") || text === "imagen") return true;

    return false;
  }

  function messageNode(m) {
    const row = document.createElement("div");
    row.className = "bubble-row " + (m.direction === "in" ? "in" : "out");

    const b = document.createElement("div");
    b.className = "bubble " + (m.direction === "out" ? "out" : "");

    let mediaHtml = "";
if (m.media_url) {
  if (isImageMessage(m)) {

    mediaHtml = `
      <img 
        src="${esc(m.media_url)}"
        class="msg-image"
        loading="lazy"
        style="max-width:260px;border-radius:12px;margin-bottom:6px"
      />
    `;

  } else {

    mediaHtml = `
      <div style="margin-bottom:6px">
        <a href="${esc(m.media_url)}" target="_blank">
          📎 Abrir archivo
        </a>
      </div>
    `;

  }
}

    const hour = fmtHour(m.ts_utc || m.created_at || m.timestamp);

    b.innerHTML = `
      ${mediaHtml}
      <div class="txt" data-msgtext="1">${nl2br(esc(m.text || ""))}</div>
      <div class="meta">${hour ? esc(hour) : ""}</div>
    `;

    row.appendChild(b);
    return row;
  }

  function renderMessagesReplace(msgs) {
    if (!elMsgs) return;

    currentMessages = Array.isArray(msgs) ? msgs : [];

    if (!currentMessages.length) {
      elMsgs.innerHTML = `<div class="center-hint">Sin mensajes.</div>`;
      return;
    }

    elMsgs.innerHTML = "";
    currentMessages.forEach((m) => elMsgs.appendChild(messageNode(m)));
    applyFind(findQuery);
  }

  async function openConversation(peer, markRead) {
    if (!peer) return;

    if (markRead) {
      await fetch("/api/mark_read", {
        method: "POST",
        credentials: "same-origin",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ wa_peer: peer }),
      }).catch(() => {});
    }

    const url = new URL(location.origin + "/api/chat");
    url.searchParams.set("wa_peer", peer);
    url.searchParams.set("limit", "100");

    const res = await fetch(url.toString(), { credentials: "same-origin" });
    const data = await res.json();
    renderMessagesReplace(Array.isArray(data.messages) ? data.messages : []);
    scrollToBottom();

    if (markRead) {
      await loadConversations();
    }
  }

  async function refreshCurrentConversationSilently() {
    if (!selectedPeer || refreshBusy) return;
    refreshBusy = true;

    try {
      const keepBottom = isNearBottom(elMsgs);
      const prevLastId = currentMessages.length ? currentMessages[currentMessages.length - 1]?.id : null;

      const url = new URL(location.origin + "/api/chat");
      url.searchParams.set("wa_peer", selectedPeer);
      url.searchParams.set("limit", "100");

      const res = await fetch(url.toString(), { credentials: "same-origin" });
      const data = await res.json();
      const nextMessages = Array.isArray(data.messages) ? data.messages : [];

      const nextLastId = nextMessages.length ? nextMessages[nextMessages.length - 1]?.id : null;
      const changed =
        nextMessages.length !== currentMessages.length ||
        String(prevLastId || "") !== String(nextLastId || "");

      if (changed) {
        renderMessagesReplace(nextMessages);
        if (keepBottom) scrollToBottom();
      }
    } catch (e) {
      console.error("refreshCurrentConversationSilently:", e);
    } finally {
      refreshBusy = false;
    }
  }

  async function refreshAllSilently() {
    if (refreshBusy) return;
    try {
      await loadConversations();
      if (selectedPeer) {
        await refreshCurrentConversationSilently();
      }
    } catch (e) {
      console.error("refreshAllSilently:", e);
      setSync("Error");
    }
  }

  setInterval(refreshAllSilently, 2500);

  document.addEventListener("visibilitychange", () => {
    if (!document.hidden) {
      refreshAllSilently();
    }
  });

  const EMOJIS = [
    "😀","😁","😂","🤣","😊","😍","😘","😎",
    "🙂","😉","😅","😇","🤩","🥳","😴","🤔",
    "👍","👎","👏","🙏","💪","🔥","✨","💯",
    "❤️","💙","💜","🩷","🧡","💛","💚","🤍",
    "🎉","✅","❌","⚠️","📎","📷","🧾","📄"
  ];

  function insertAtCursor(textarea, text) {
    if (!textarea) return;
    const start = textarea.selectionStart ?? textarea.value.length;
    const end = textarea.selectionEnd ?? textarea.value.length;
    const before = textarea.value.slice(0, start);
    const after = textarea.value.slice(end);
    textarea.value = before + text + after;
    const pos = start + text.length;
    textarea.setSelectionRange(pos, pos);
    textarea.focus();
    updateComposerState();
  }

  function buildEmojiPanel() {
    if (!elEmojiPanel) return;
    elEmojiPanel.innerHTML = `
      <div class="emoji-grid">
        ${EMOJIS.map(e => `<button class="emoji" type="button" data-e="${esc(e)}">${esc(e)}</button>`).join("")}
      </div>
    `;
    elEmojiPanel.querySelectorAll(".emoji").forEach((btn) => {
      btn.addEventListener("click", () => {
        const e = btn.getAttribute("data-e") || "";
        insertAtCursor(elText, e);
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
    const visible = getComputedStyle(elEmojiPanel).display !== "none";
    if (visible) closeEmoji();
    else openEmoji();
  }

  buildEmojiPanel();
  closeEmoji();

  elEmojiBtn?.addEventListener("click", toggleEmoji);

  document.addEventListener("click", (e) => {
    if (!elEmojiPanel || !elEmojiBtn) return;
    const t = e.target;
    if (elEmojiPanel.contains(t) || elEmojiBtn.contains(t)) return;
    closeEmoji();
  });

  function setFileChip(name) {
    if (!elFileChip || !elFileChipName) return;
    elFileChipName.textContent = name || "";
    elFileChip.style.display = name ? "flex" : "none";
    updateComposerState();
  }

  function clearFileChip() {
    if (elFile) elFile.value = "";
    setFileChip("");
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

    const data = await res.json();
    if (!res.ok || !data.ok) {
      throw new Error(data.error || "No se pudo subir el archivo");
    }
    return data;
  }

  async function sendMessage() {
    if (!selectedPeer || !elSend) return;

    const text = (elText?.value || "").trim();
    const hasFile = !!(elFile?.files && elFile.files.length > 0);

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
      };

      const res = await fetch("/api/send", {
        method: "POST",
        credentials: "same-origin",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });

      const data = await res.json();

      if (!res.ok || !data.ok) {
        throw new Error(data.detail || data.error || "No se pudo enviar");
      }

      showResult("Enviado ✅", false);

      if (elText) elText.value = "";
      clearFileChip();

      await openConversation(selectedPeer, true);
      scrollToBottom();
    } catch (e) {
      console.error(e);
      showResult("Error: " + e.message, true);
    } finally {
      updateComposerState();
    }
  }

  elSend?.addEventListener("click", sendMessage);

  elText?.addEventListener("keydown", (e) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      sendMessage();
    }
  });

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

    const data = await res.json();

    if (!res.ok || !data.ok) {
      alert("No se pudo eliminar.");
      return;
    }

    selectedPeer = null;
    currentMessages = [];
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

  function resetFind() {
    findQuery = "";
    hits = [];
    hitIndex = -1;
    if (elFindCount) elFindCount.textContent = "0/0";
    clearHighlights();
  }

  function clearHighlights() {
    if (!elMsgs) return;
    elMsgs.querySelectorAll("mark.find-hit").forEach((mark) => {
      const textNode = document.createTextNode(mark.textContent || "");
      mark.replaceWith(textNode);
    });
  }

  function highlightTextInElement(el, query) {
    const text = el.textContent || "";
    if (!query) return;

    const lower = text.toLowerCase();
    const ql = query.toLowerCase();
    let idx = lower.indexOf(ql);
    if (idx === -1) return;

    const parts = [];
    let last = 0;

    while (idx !== -1) {
      parts.push(document.createTextNode(text.slice(last, idx)));
      const m = document.createElement("mark");
      m.className = "find-hit";
      m.textContent = text.slice(idx, idx + query.length);
      parts.push(m);
      last = idx + query.length;
      idx = lower.indexOf(ql, last);
    }

    parts.push(document.createTextNode(text.slice(last)));
    el.replaceChildren(...parts);
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

  function findNext() {
    if (!hits.length) return;
    hitIndex = hitIndex < hits.length - 1 ? hitIndex + 1 : 0;
    focusHit(hitIndex);
  }

  function findPrev() {
    if (!hits.length) return;
    hitIndex = hitIndex > 0 ? hitIndex - 1 : hits.length - 1;
    focusHit(hitIndex);
  }

  let findTimer = null;

  elChatSearch?.addEventListener("input", () => {
    clearTimeout(findTimer);
    findTimer = setTimeout(() => {
      applyFind(elChatSearch.value || "");
    }, 200);
  });

  elChatSearch?.addEventListener("keydown", (e) => {
    if (e.key === "Enter") {
      e.preventDefault();
      findNext();
    }
  });

  elFindDown?.addEventListener("click", findNext);
  elFindUp?.addEventListener("click", findPrev);

  function requestBrowserNotifications() {
    if (!("Notification" in window)) return;
    if (Notification.permission === "default") {
      Notification.requestPermission().catch(() => {});
    }
  }

  requestBrowserNotifications();

  function injectRuntimeStyles() {
    const style = document.createElement("style");
    style.textContent = `
      .conv-row{
        width:100%;
        border:none;
        background:transparent;
        display:flex;
        gap:12px;
        padding:12px;
        border-radius:16px;
        text-align:left;
        cursor:pointer;
        transition:background .18s ease, transform .12s ease;
      }
      .conv-row:hover{background:rgba(0,0,0,.04)}
      .conv-row.active{background:rgba(120,120,120,.10)}
      .conv-avatar{
        min-width:42px;
        height:42px;
        border-radius:999px;
        display:flex;
        align-items:center;
        justify-content:center;
        font-weight:700;
        background:linear-gradient(135deg,#8b5cf6,#3b82f6);
        color:#fff;
      }
      .conv-main{flex:1; min-width:0}
      .conv-top,.conv-bottom{
        display:flex;
        align-items:center;
        justify-content:space-between;
        gap:10px;
      }
      .conv-name{
        font-weight:700;
        overflow:hidden;
        text-overflow:ellipsis;
        white-space:nowrap;
      }
      .conv-time{
        font-size:.85rem;
        opacity:.75;
        min-width:max-content;
      }
      .conv-preview{
        overflow:hidden;
        text-overflow:ellipsis;
        white-space:nowrap;
        opacity:.8;
        font-size:.92rem;
        max-width:100%;
      }
      .conv-badge{
        min-width:24px;
        height:24px;
        border-radius:999px;
        padding:0 8px;
        display:flex;
        align-items:center;
        justify-content:center;
        color:#fff;
        font-size:.82rem;
        font-weight:700;
        background:linear-gradient(135deg,#3b82f6,#d946ef);
      }
      .bubble-row{
        display:flex;
        width:100%;
        margin:8px 0;
      }
      .bubble-row.in{justify-content:flex-start}
      .bubble-row.out{justify-content:flex-end}
      .bubble{
        max-width:min(78%,560px);
        border-radius:20px;
        padding:12px 14px 8px;
        box-shadow:0 8px 20px rgba(0,0,0,.06);
        background:#fff;
        word-break:break-word;
      }
      .bubble.out{
        background:#e9f7ff;
      }
      .theme-dark .bubble{
        background:#1c1f26;
      }
      .theme-dark .bubble.out{
        background:#183446;
      }
      .txt{
        white-space:pre-wrap;
        line-height:1.35;
      }
      .meta{
        margin-top:6px;
        font-size:.78rem;
        opacity:.65;
        text-align:right;
      }
      .media-preview{
        max-width:100%;
        border-radius:14px;
        display:block;
        margin-bottom:8px;
      }
      .emoji-panel,
      #emojiPanel{
        padding:10px;
      }
      .emoji-grid{
        display:grid;
        grid-template-columns:repeat(8,1fr);
        gap:8px;
      }
      .emoji{
        border:none;
        background:transparent;
        cursor:pointer;
        font-size:20px;
        border-radius:10px;
        padding:6px;
      }
      .emoji:hover{
        background:rgba(120,120,120,.10);
      }
      .file-chip{
        display:flex;
        align-items:center;
        gap:10px;
      }
      mark.find-hit{
        background:#fde68a;
        color:inherit;
        border-radius:4px;
        padding:0 2px;
      }
      mark.find-hit.active{
        background:#f59e0b;
      }
      .modal.open,
      .modal.show{
        display:flex !important;
      }
    `;
    document.head.appendChild(style);
  }

  injectRuntimeStyles();
  setEnabledForChat(false);
  closeEmoji();
  loadConversations().catch(console.error);
})();
