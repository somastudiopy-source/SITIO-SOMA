(function () {
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
  syncIcon(); syncLogo();

  if (themeToggle) {
    themeToggle.addEventListener("click", () => {
      const isLight = body.classList.contains("theme-light");
      body.classList.toggle("theme-light", !isLight);
      body.classList.toggle("theme-dark", isLight);
      localStorage.setItem("soma_theme", !isLight ? "light" : "dark");
      syncIcon(); syncLogo();
    });
  }

  
  // -------- Google Calendar (Modal + Week/Month + Crear Evento) --------
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

  async function ensureCalendarConfig() {
    // Fetch per-user calendar config from backend (users.json)
    if (CALENDAR_ID) return true;
    try {
      const r = await fetch("/api/me");
      const j = await r.json();
      if (j && j.ok) {
        CALENDAR_ID = j.calendar_id || "";
        CAL_TZ = j.calendar_tz || CAL_TZ;
      }
    } catch (e) {}
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
    elCalModal.setAttribute("aria-hidden", "false");
  }

  function closeCal() {
    if (!elCalModal || !elCalFrame) return;
    elCalModal.classList.remove("show");
    elCalModal.setAttribute("aria-hidden", "true");
    if (elCalAdd) elCalAdd.hidden = true;
    elCalFrame.src = "about:blank";
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

  function toGCalDateFromParts(dateValue, timeValue) {
    // dateValue: "YYYY-MM-DD", timeValue: "HH:MM"
    if (!dateValue || !timeValue) return "";
    const dtLocalValue = `${dateValue}T${timeValue}`;
    const d = new Date(dtLocalValue);
    if (Number.isNaN(d.getTime())) return "";
    // Google expects UTC: YYYYMMDDTHHMMSSZ
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
    // intentar fijar calendario destino (si el usuario tiene acceso)
    params.set("src", CALENDAR_ID);
    params.set("sf", "true");
    params.set("output", "xml");

    window.open(`${base}?${params.toString()}`, "_blank", "noopener");
  }

  elCalCreate?.addEventListener("click", () => { openCreateEvent(); });

  // cerrar si clickeás fuera
  elCalModal?.addEventListener("click", (e) => {
    if (e.target === elCalModal) closeCal();
  });

  // cerrar con ESC
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") closeCal();
  });


  // Elements / state
  let selectedPeer = null;
  let convCache = [];

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
    return (s || "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;");
  }

  // sin segundos
  function fmtMsg(ts) {
    try {
      return new Date(ts).toLocaleString([], {
        year: "numeric",
        month: "2-digit",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit"
      });
    } catch {
      return ts || "";
    }
  }

  function showResult(msg) {
    if (elSendResult) elSendResult.textContent = msg || "";
  }

  function setEnabledForChat(enabled) {
    if (elSend) elSend.disabled = !enabled;
    if (elEmojiBtn) elEmojiBtn.disabled = !enabled;
    if (elAttachBtn) elAttachBtn.disabled = !enabled;
    if (elDeleteChat) elDeleteChat.disabled = !enabled;
  }

  // -------- Conversations --------
  async function loadConversations() {
    const res = await fetch("/api/conversations?limit=400");
    const data = await res.json();
    convCache = data.conversations || [];
    renderConversations();
    if (elSync) elSync.textContent = "OK • " + new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
  }

  function renderConversations() {
    const q = (elQ?.value || "").toLowerCase();
    if (!elConvs) return;
    elConvs.innerHTML = "";

    const filtered = convCache.filter((c) => {
      const name = (c.name || "").toLowerCase();
      const peer = (c.wa_peer || "").toLowerCase();
      return !q || name.includes(q) || peer.includes(q);
    });

    if (filtered.length === 0) {
      elConvs.innerHTML = `<div style="padding:14px;opacity:.75;">Sin conversaciones.</div>`;
      return;
    }

    filtered.forEach((c) => {
      const btn = document.createElement("button");
      btn.className = "conv" + (c.wa_peer === selectedPeer ? " active" : "");

      btn.innerHTML = `
        <div class="avatar">${esc((c.name || c.wa_peer).slice(0, 1).toUpperCase())}</div>
        <div class="conv-main">
          <div class="conv-top">
            <div class="conv-name">${esc(c.name || c.wa_peer)}</div>
            <div class="conv-time">${esc(c.day_label || "")}</div>
          </div>
          <div class="conv-last">${esc(c.last_text || "")}</div>
        </div>
        ${c.unread > 0 ? `<div class="unread">${c.unread}</div>` : ``}
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

  // -------- Chat load / paging --------
  async function openConversation(peer) {
    await fetch("/api/mark_read", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ wa_peer: peer })
    });

    resetFind();

    elMsgs.innerHTML = "";
    oldestId = null;
    await loadLatest(peer);
    await loadConversations();
  }

  async function loadLatest(peer) {
    const url = new URL(location.origin + "/api/chat");
    url.searchParams.set("wa_peer", peer);
    url.searchParams.set("limit", "50");

    const res = await fetch(url.toString());
    const data = await res.json();
    hasMore = !!data.has_more;

    const msgs = data.messages || [];
    renderMessagesReplace(msgs);

    if (msgs.length) oldestId = msgs[0].id;
    scrollToBottom();

    applyFind(findQuery);
  }

  async function loadMoreOlder(peer) {
    if (!hasMore || loadingMore || !oldestId) return;
    loadingMore = true;

    const prevScrollHeight = elMsgs.scrollHeight;

    const url = new URL(location.origin + "/api/chat");
    url.searchParams.set("wa_peer", peer);
    url.searchParams.set("limit", "50");
    url.searchParams.set("before_id", String(oldestId));

    const res = await fetch(url.toString());
    const data = await res.json();
    hasMore = !!data.has_more;

    const msgs = data.messages || [];
    if (msgs.length) oldestId = msgs[0].id;

    renderMessagesPrepend(msgs);

    const newScrollHeight = elMsgs.scrollHeight;
    elMsgs.scrollTop = newScrollHeight - prevScrollHeight;

    loadingMore = false;

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
    if ((m.msg_type || "") === "image") return true;
    if ((m.media_kind || "") === "image") return true;
    if ((m.content_type || "").startsWith("image/")) return true;
    const u = (m.media_url || "").toLowerCase();
    return u.endsWith(".png") || u.endsWith(".jpg") || u.endsWith(".jpeg") || u.endsWith(".webp") || u.endsWith(".gif");
  }

  function messageNode(m) {
    const row = document.createElement("div");
    row.className = "bubble-row " + (m.direction === "in" ? "in" : "out");

    const b = document.createElement("div");
    b.className = "bubble " + (m.direction === "out" ? "out" : "");

    let mediaHtml = "";
    if (m.media_url) {
      if (isImageMessage(m)) {
        mediaHtml = `<img class="media-preview" src="${esc(m.media_url)}" alt="imagen" />`;
      } else {
        mediaHtml = `<div style="margin-bottom:8px;">
          <a href="${esc(m.media_url)}" target="_blank" style="color:inherit;opacity:.9;text-decoration:none;">
            📎 Abrir archivo
          </a>
        </div>`;
      }
    }

    b.innerHTML = `
      ${mediaHtml}
      <div class="txt" data-msgtext="1">${esc(m.text || "")}</div>
      <div class="meta">${esc(m.msg_type || "text")} • ${esc(fmtMsg(m.ts_utc))}</div>
    `;
    row.appendChild(b);
    return row;
  }

  function scrollToBottom() {
    if (!elMsgs) return;
    elMsgs.scrollTop = elMsgs.scrollHeight;
  }

  elMsgs?.addEventListener("scroll", async () => {
    if (!selectedPeer) return;
    if (elMsgs.scrollTop <= 40) await loadMoreOlder(selectedPeer);
  });

  // -------- Emojis --------
  const EMOJIS = [
    "😀","😁","😂","🤣","😊","😍","😘","😎",
    "🙂","😉","😅","😇","🤩","🥳","😴","🤔",
    "👍","👎","👏","🙏","💪","🔥","✨","💯",
    "❤️","💙","💜","🩷","🧡","💛","💚","🤍",
    "🎉","✅","❌","⚠️","📎","📷","🧾","📄"
  ];

  function buildEmojiPanel() {
    if (!elEmojiPanel) return;
    elEmojiPanel.innerHTML = `
      <div class="emoji-grid">
        ${EMOJIS.map(e => `<button class="emoji" type="button" data-e="${esc(e)}">${esc(e)}</button>`).join("")}
      </div>
    `;
    elEmojiPanel.querySelectorAll(".emoji").forEach(btn => {
      btn.addEventListener("click", () => {
        const e = btn.getAttribute("data-e") || "";
        insertAtCursor(elText, e);
        elText.focus();
      });
    });
  }

  function openEmoji() { if (elEmojiPanel) elEmojiPanel.style.display = "block"; }
  function closeEmoji() { if (elEmojiPanel) elEmojiPanel.style.display = "none"; }
  function toggleEmoji() {
    if (!elEmojiPanel) return;
    const open = elEmojiPanel.style.display !== "none";
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

  // -------- Attach --------
  function setFileChip(name) {
    if (!elFileChip || !elFileChipName) return;
    elFileChipName.textContent = name || "";
    elFileChip.style.display = name ? "flex" : "none";
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

  // -------- Send --------
  async function uploadIfNeeded() {
    if (!elFile || !elFile.files || elFile.files.length === 0) return null;
    const file = elFile.files[0];
    const fd = new FormData();
    fd.append("file", file);

    showResult("Subiendo archivo…");
    const res = await fetch("/api/upload", { method: "POST", body: fd });
    const data = await res.json();
    if (!res.ok || !data.ok) throw new Error(data.error || "Upload failed");
    return data;
  }

  async function sendMessage() {
    if (!selectedPeer) return;

    const text = (elText?.value || "").trim();
    const hasFile = elFile?.files?.length > 0;

    if (!text && !hasFile) return;

    elSend.disabled = true;

    try {
      const upload = hasFile ? await uploadIfNeeded() : null;

      showResult("Enviando…");
      const payload = {
        to: selectedPeer,
        text,
        filename: upload ? upload.filename : null,
        content_type: upload ? upload.content_type : null
      };

      const res = await fetch("/api/send", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
      const data = await res.json();

      if (!res.ok) {
        showResult("Error: " + (data.error || "no se pudo enviar"));
        return;
      }

      showResult("Enviado ✅");
      elText.value = "";
      clearFileChip();

      oldestId = null;
      await loadLatest(selectedPeer);
      await loadConversations();

    } catch (e) {
      showResult("Error: " + e.message);
    } finally {
      elSend.disabled = false;
    }
  }

  elSend?.addEventListener("click", sendMessage);
  elText?.addEventListener("keydown", (e) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      sendMessage();
    }
  });

  // -------- Delete conversation --------
  elDeleteChat?.addEventListener("click", async () => {
    if (!selectedPeer) return;
    const ok = confirm("¿Eliminar este chat completo? (Se borra del panel)");
    if (!ok) return;

    const res = await fetch("/api/delete_conversation", {
      method: "POST",
      headers: {"Content-Type":"application/json"},
      body: JSON.stringify({ wa_peer: selectedPeer })
    });
    const data = await res.json();
    if (!res.ok || !data.ok) {
      alert("No se pudo eliminar.");
      return;
    }

    selectedPeer = null;
    setEnabledForChat(false);
    closeEmoji();
    clearFileChip();
    resetFind();

    if (elChatTitle) elChatTitle.textContent = "Seleccioná un chat";
    if (elChatSubtitle) elChatSubtitle.textContent = "—";
    if (elMsgs) elMsgs.innerHTML = `<div class="center-hint">Elegí una conversación para ver los mensajes.</div>`;

    await loadConversations();
  });

  // -------- Find in chat (▲ ▼) --------
  function resetFind() {
    findQuery = "";
    hits = [];
    hitIndex = -1;
    if (elFindCount) elFindCount.textContent = "0/0";
    clearHighlights();
  }

  function clearHighlights() {
    if (!elMsgs) return;
    elMsgs.querySelectorAll("mark.find-hit").forEach(mark => {
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

    elMsgs.querySelectorAll(".txt[data-msgtext='1']").forEach(txtEl => {
      highlightTextInElement(txtEl, findQuery);
    });

    hits = Array.from(elMsgs.querySelectorAll("mark.find-hit"));
    if (elFindCount) elFindCount.textContent = hits.length ? `1/${hits.length}` : "0/0";

    if (hits.length) {
      hitIndex = 0;
      focusHit(hitIndex);
    }
  }

  function focusHit(i) {
    if (!hits.length) return;
    hits.forEach(h => h.classList.remove("active"));
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
      hitIndex++;
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
      hitIndex--;
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

  // sidebar search
  elQ?.addEventListener("input", renderConversations);

  // start
  setEnabledForChat(false);
  loadConversations();
  setInterval(async () => {
    await loadConversations();
  }, 2500);
})();