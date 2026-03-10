(() => {
  "use strict";

  const $ = (id) => document.getElementById(id);

  const els = {
    brandLogo: $("brandLogo"),
    themeToggle: $("themeToggle"),
    themeIcon: $("themeIcon"),

    convs: $("convs"),
    q: $("q"),
    sync: $("sync"),

    chatTitle: $("chatTitle"),
    chatSubtitle: $("chatSubtitle"),
    msgs: $("msgs"),

    text: $("text"),
    send: $("send"),
    sendResult: $("sendResult"),

    file: $("file"),
    attachBtn: $("attachBtn"),
    fileChip: $("fileChip"),
    fileChipName: $("fileChipName"),
    fileChipClear: $("fileChipClear"),

    emojiBtn: $("emojiBtn"),
    emojiPanel: $("emojiPanel"),

    chatSearch: $("chatSearch"),
    findUp: $("findUp"),
    findDown: $("findDown"),
    findCount: $("findCount"),

    deleteChatBtn: $("deleteChatBtn"),

    openCalendarBtn: $("openCalendarBtn"),
    calModal: $("calModal"),
    calFrame: $("calFrame"),
    calCloseBtn: $("calCloseBtn"),
    calWeekBtn: $("calWeekBtn"),
    calMonthBtn: $("calMonthBtn"),
    calAddToggleBtn: $("calAddToggleBtn"),
    calAdd: $("calAdd"),
    calTitle: $("calTitle"),
    calDate: $("calDate"),
    calStartTime: $("calStartTime"),
    calEndTime: $("calEndTime"),
    calDetails: $("calDetails"),
    calCreateBtn: $("calCreateBtn"),
  };

  const state = {
    me: null,
    conversations: [],
    filteredConversations: [],
    currentPeer: null,
    currentChatMessages: [],
    currentName: "",
    currentUnread: 0,
    refreshTimer: null,
    refreshMs: 2500,
    selectedFile: null,
    chatSearchMatches: [],
    chatSearchIndex: -1,
    typingTimer: null,
    newestInboundByPeer: new Map(),
    loadedOnce: false,
    currentCalendarMode: "WEEK",
    theme: localStorage.getItem("soma_theme") || "light",
  };

  injectRuntimeStyles();
  bindEvents();
  boot();

  async function boot() {
    applyTheme(state.theme);

    try {
      await loadMe();
      setupCalendar();
      await loadConversations(true);
      startAutoRefresh();
      requestBrowserNotifications();
    } catch (err) {
      console.error(err);
      setSync("Error");
      setResult("No se pudo cargar el panel.", true);
    }
  }

  function bindEvents() {
    if (els.themeToggle) {
      els.themeToggle.addEventListener("click", toggleTheme);
    }

    if (els.q) {
      els.q.addEventListener("input", renderConversationList);
    }

    if (els.send) {
      els.send.addEventListener("click", onSend);
    }

    if (els.text) {
      els.text.addEventListener("input", updateComposerState);
      els.text.addEventListener("keydown", (e) => {
        if (e.key === "Enter" && !e.shiftKey) {
          e.preventDefault();
          onSend();
        }
      });
    }

    if (els.attachBtn && els.file) {
      els.attachBtn.addEventListener("click", () => els.file.click());
      els.file.addEventListener("change", onPickFile);
    }

    if (els.fileChipClear) {
      els.fileChipClear.addEventListener("click", clearSelectedFile);
    }

    if (els.deleteChatBtn) {
      els.deleteChatBtn.addEventListener("click", onDeleteConversation);
    }

    if (els.chatSearch) {
      els.chatSearch.addEventListener("input", handleChatSearch);
    }

    if (els.findUp) {
      els.findUp.addEventListener("click", () => stepChatSearch(-1));
    }

    if (els.findDown) {
      els.findDown.addEventListener("click", () => stepChatSearch(1));
    }

    if (els.emojiBtn) {
      els.emojiBtn.addEventListener("click", toggleEmojiPanel);
    }

    document.addEventListener("click", (e) => {
      if (
        els.emojiPanel &&
        els.emojiBtn &&
        !els.emojiPanel.contains(e.target) &&
        !els.emojiBtn.contains(e.target)
      ) {
        els.emojiPanel.style.display = "none";
      }
    });

    document.addEventListener("visibilitychange", () => {
      if (!document.hidden) {
        loadConversations(false).catch(console.error);
        if (state.currentPeer) {
          openConversation(state.currentPeer, false).catch(console.error);
        }
      }
    });
  }

  async function loadMe() {
    const res = await fetch("/api/me", { credentials: "same-origin" });
    if (!res.ok) throw new Error("No se pudo cargar /api/me");
    state.me = await res.json();
  }

  async function loadConversations(initial = false) {
    const res = await fetch("/api/conversations?limit=400", { credentials: "same-origin" });
    if (!res.ok) throw new Error("No se pudo cargar conversaciones");
    const data = await res.json();

    const previousUnread = new Map();
    for (const c of state.conversations) previousUnread.set(c.wa_peer, Number(c.unread || 0));

    state.conversations = Array.isArray(data.conversations) ? data.conversations : [];
    state.filteredConversations = state.conversations;

    renderConversationList();

    for (const conv of state.conversations) {
      const prev = previousUnread.get(conv.wa_peer) || 0;
      const nowUnread = Number(conv.unread || 0);

      if (!initial && nowUnread > prev) {
        maybeNotifyNewMessage(conv);
      }
    }

    if (!state.loadedOnce) {
      state.loadedOnce = true;
    }

    if (!state.currentPeer && state.conversations.length > 0) {
      await openConversation(state.conversations[0].wa_peer, true);
    }

    setSyncOk();
  }

  function renderConversationList() {
    if (!els.convs) return;

    const q = (els.q?.value || "").trim().toLowerCase();

    const list = state.conversations.filter((c) => {
      const name = (c.name || "").toLowerCase();
      const peer = (c.wa_peer || "").toLowerCase();
      const text = (c.last_text || "").toLowerCase();
      return !q || name.includes(q) || peer.includes(q) || text.includes(q);
    });

    state.filteredConversations = list;
    els.convs.innerHTML = "";

    if (list.length === 0) {
      els.convs.innerHTML = `<div class="center-hint">No hay conversaciones.</div>`;
      return;
    }

    list.forEach((conv) => {
      const row = document.createElement("button");
      row.type = "button";
      row.className = `conv-row ${conv.wa_peer === state.currentPeer ? "active" : ""}`;
      row.addEventListener("click", () => openConversation(conv.wa_peer, true));

      const avatarText = escapeHtml((conv.name || conv.wa_peer || "?").trim().charAt(0).toUpperCase());
      const dayText = conv.day_label || formatConversationDay(conv.last_ts);
      const preview = conv.last_text || "Sin mensajes";
      const unread = Number(conv.unread || 0);

      row.innerHTML = `
        <div class="conv-avatar">${avatarText}</div>
        <div class="conv-main">
          <div class="conv-top">
            <div class="conv-name">${escapeHtml(conv.name || conv.wa_peer || "Sin nombre")}</div>
            <div class="conv-time">${escapeHtml(dayText)}</div>
          </div>
          <div class="conv-bottom">
            <div class="conv-preview">${escapeHtml(preview)}</div>
            ${unread > 0 ? `<div class="conv-badge">${unread}</div>` : ""}
          </div>
        </div>
      `;

      els.convs.appendChild(row);
    });
  }

  async function openConversation(waPeer, markRead = true) {
    state.currentPeer = waPeer;

    const conv = state.conversations.find((c) => c.wa_peer === waPeer);
    state.currentName = conv?.name || waPeer || "Sin nombre";
    state.currentUnread = Number(conv?.unread || 0);

    renderConversationList();
    updateHeader();
    updateComposerState();
    showTypingIndicator("Actualizando…");

    const url = `/api/chat?wa_peer=${encodeURIComponent(waPeer)}&limit=100`;
    const res = await fetch(url, { credentials: "same-origin" });
    if (!res.ok) {
      hideTypingIndicator();
      throw new Error("No se pudo cargar chat");
    }

    const data = await res.json();
    state.currentChatMessages = Array.isArray(data.messages) ? data.messages : [];
    renderMessages();
    handleChatSearch();
    scrollMessagesToBottom();
    hideTypingIndicator();

    if (markRead) {
      await fetch("/api/mark_read", {
        method: "POST",
        credentials: "same-origin",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ wa_peer: waPeer }),
      }).catch(console.error);

      const local = state.conversations.find((c) => c.wa_peer === waPeer);
      if (local) local.unread = 0;
      renderConversationList();
    }
  }

  function updateHeader() {
    if (els.chatTitle) els.chatTitle.textContent = state.currentName || "Seleccioná un chat";
    if (els.chatSubtitle) els.chatSubtitle.textContent = state.currentPeer || "—";
    if (els.deleteChatBtn) els.deleteChatBtn.disabled = !state.currentPeer;
  }

  function renderMessages() {
    if (!els.msgs) return;

    if (!state.currentPeer) {
      els.msgs.innerHTML = `<div class="center-hint">Elegí una conversación para ver los mensajes.</div>`;
      return;
    }

    els.msgs.innerHTML = "";

    if (!state.currentChatMessages.length) {
      els.msgs.innerHTML = `<div class="center-hint">Todavía no hay mensajes en este chat.</div>`;
      return;
    }

    const frag = document.createDocumentFragment();

    state.currentChatMessages.forEach((msg, index) => {
      const wrap = document.createElement("div");
      wrap.className = `msg-wrap ${msg.direction === "out" ? "out" : "in"}`;
      wrap.dataset.msgIndex = String(index);

      const bubble = document.createElement("div");
      bubble.className = `msg-bubble ${msg.direction === "out" ? "out" : "in"}`;

      const body = document.createElement("div");
      body.className = "msg-body";

      if (msg.media_url) {
        body.appendChild(renderMedia(msg));
      }

      if (msg.text) {
        const text = document.createElement("div");
        text.className = "msg-text";
        text.innerHTML = nl2br(escapeHtml(msg.text));
        body.appendChild(text);
      }

      const meta = document.createElement("div");
      meta.className = "msg-meta";
      const msgType = msg.msg_type || "text";
      const hour = formatMessageHour(msg.ts_utc);
      meta.textContent = hour ? `${msgType} · ${hour}` : msgType;

      bubble.appendChild(body);
      bubble.appendChild(meta);
      wrap.appendChild(bubble);
      frag.appendChild(wrap);
    });

    els.msgs.appendChild(frag);
  }

  function renderMedia(msg) {
    const box = document.createElement("div");
    box.className = "msg-media";

    if (msg.media_kind === "image") {
      const img = document.createElement("img");
      img.src = msg.media_url;
      img.alt = "Imagen";
      img.className = "msg-image";
      box.appendChild(img);
      return box;
    }

    const link = document.createElement("a");
    link.href = msg.media_url;
    link.target = "_blank";
    link.rel = "noopener noreferrer";
    link.className = "msg-file";
    link.textContent = "Abrir archivo";
    box.appendChild(link);
    return box;
  }

  async function onSend() {
    if (!state.currentPeer || !els.send || els.send.disabled) return;

    const text = (els.text?.value || "").trim();

    if (!text && !state.selectedFile) return;

    els.send.disabled = true;
    setResult("Enviando…", false);

    try {
      let uploaded = null;

      if (state.selectedFile) {
        uploaded = await uploadCurrentFile();
      }

      const payload = {
        to: state.currentPeer,
        text,
        filename: uploaded?.filename || null,
        content_type: uploaded?.content_type || null,
      };

      const res = await fetch("/api/send", {
        method: "POST",
        credentials: "same-origin",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });

      const data = await res.json();

      if (!res.ok || data.ok === FalseBoolean()) {
        throw new Error(data.detail || data.error || "No se pudo enviar");
      }

      if (els.text) els.text.value = "";
      clearSelectedFile();
      setResult("Enviado.", false);

      await openConversation(state.currentPeer, true);
      await loadConversations(false);
    } catch (err) {
      console.error(err);
      setResult(err.message || "Error al enviar.", true);
    } finally {
      updateComposerState();
    }
  }

  async function uploadCurrentFile() {
    if (!state.selectedFile) return null;

    const fd = new FormData();
    fd.append("file", state.selectedFile);

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

  function onPickFile(e) {
    const file = e.target.files?.[0];
    if (!file) return;

    state.selectedFile = file;
    if (els.fileChip) els.fileChip.style.display = "flex";
    if (els.fileChipName) els.fileChipName.textContent = file.name;
    updateComposerState();
  }

  function clearSelectedFile() {
    state.selectedFile = null;
    if (els.file) els.file.value = "";
    if (els.fileChip) els.fileChip.style.display = "none";
    if (els.fileChipName) els.fileChipName.textContent = "";
    updateComposerState();
  }

  async function onDeleteConversation() {
    if (!state.currentPeer) return;
    const ok = confirm("¿Eliminar esta conversación del panel?");
    if (!ok) return;

    try {
      const res = await fetch("/api/delete_conversation", {
        method: "POST",
        credentials: "same-origin",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ wa_peer: state.currentPeer }),
      });

      const data = await res.json();
      if (!res.ok || !data.ok) throw new Error(data.error || "No se pudo eliminar");

      state.currentPeer = null;
      state.currentChatMessages = [];
      renderMessages();
      updateHeader();
      await loadConversations(false);
    } catch (err) {
      console.error(err);
      setResult(err.message || "No se pudo eliminar.", true);
    }
  }

  function updateComposerState() {
    const hasPeer = !!state.currentPeer;
    const hasText = !!(els.text?.value || "").trim();
    const hasFile = !!state.selectedFile;
    const canSend = hasPeer && (hasText || hasFile);

    if (els.send) els.send.disabled = !canSend;
    if (els.attachBtn) els.attachBtn.disabled = !hasPeer;
    if (els.emojiBtn) els.emojiBtn.disabled = !hasPeer;
  }

  function startAutoRefresh() {
    stopAutoRefresh();

    state.refreshTimer = setInterval(async () => {
      try {
        showTypingIndicator("Actualizando…");
        const prevMessagesLen = state.currentChatMessages.length;
        const prevLastInbound = getLastInboundId(state.currentChatMessages);

        await loadConversations(false);

        if (state.currentPeer) {
          await openConversation(state.currentPeer, false);

          const newLastInbound = getLastInboundId(state.currentChatMessages);
          const hasNewInbound =
            prevLastInbound &&
            newLastInbound &&
            String(newLastInbound) !== String(prevLastInbound);

          const hasNewMessages = state.currentChatMessages.length > prevMessagesLen;

          if (hasNewMessages) {
            scrollMessagesToBottom();
          }

          if (hasNewInbound) {
            flashWindowTitle();
          }
        }

        setSyncOk();
      } catch (err) {
        console.error("refresh error:", err);
        setSync("Error");
      } finally {
        hideTypingIndicator();
      }
    }, state.refreshMs);
  }

  function stopAutoRefresh() {
    if (state.refreshTimer) {
      clearInterval(state.refreshTimer);
      state.refreshTimer = null;
    }
  }

  function getLastInboundId(messages) {
    for (let i = messages.length - 1; i >= 0; i--) {
      if (messages[i]?.direction === "in") return messages[i]?.id;
    }
    return null;
  }

  function setSyncOk() {
    const now = new Date();
    setSync(
      `OK · ${now.toLocaleTimeString("es-AR", {
        hour: "2-digit",
        minute: "2-digit",
      })}`
    );
  }

  function setSync(text) {
    if (els.sync) els.sync.textContent = text;
  }

  function setResult(text, isError) {
    if (!els.sendResult) return;
    els.sendResult.textContent = text || "";
    els.sendResult.style.color = isError ? "#d94b4b" : "";
  }

  function scrollMessagesToBottom() {
    if (!els.msgs) return;
    els.msgs.scrollTop = els.msgs.scrollHeight;
  }

  function parseServerDate(ts) {
    if (!ts) return null;

    const d = new Date(ts);
    if (!Number.isFinite(d.getTime())) return null;

    return d;
  }

  function formatMessageHour(ts) {
    const d = parseServerDate(ts);
    if (!d) return "";
    return d.toLocaleTimeString("es-AR", {
      hour: "2-digit",
      minute: "2-digit",
      timeZone: "America/Argentina/Buenos_Aires",
    });
  }

  function formatConversationDay(ts) {
    const d = parseServerDate(ts);
    if (!d) return "";

    const now = new Date();
    const dArg = new Date(d.toLocaleString("en-US", { timeZone: "America/Argentina/Buenos_Aires" }));
    const nowArg = new Date(now.toLocaleString("en-US", { timeZone: "America/Argentina/Buenos_Aires" }));

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
      return formatMessageHour(ts);
    }

    if (isYesterday) {
      return "AYER";
    }

    return dArg.toLocaleDateString("es-AR", {
      timeZone: "America/Argentina/Buenos_Aires",
    });
  }

  function handleChatSearch() {
    const q = (els.chatSearch?.value || "").trim().toLowerCase();
    state.chatSearchMatches = [];
    state.chatSearchIndex = -1;

    const bubbles = Array.from(document.querySelectorAll(".msg-text"));
    bubbles.forEach((node) => {
      node.innerHTML = nl2br(escapeHtml(node.textContent || ""));
    });

    if (!q) {
      updateFindCount();
      return;
    }

    bubbles.forEach((node, index) => {
      const text = node.textContent || "";
      const lower = text.toLowerCase();
      if (lower.includes(q)) {
        state.chatSearchMatches.push(index);
        node.innerHTML = highlightText(text, q);
      }
    });

    if (state.chatSearchMatches.length) {
      state.chatSearchIndex = 0;
      focusChatMatch();
    }

    updateFindCount();
  }

  function stepChatSearch(dir) {
    if (!state.chatSearchMatches.length) return;

    state.chatSearchIndex += dir;

    if (state.chatSearchIndex < 0) {
      state.chatSearchIndex = state.chatSearchMatches.length - 1;
    }
    if (state.chatSearchIndex >= state.chatSearchMatches.length) {
      state.chatSearchIndex = 0;
    }

    focusChatMatch();
    updateFindCount();
  }

  function focusChatMatch() {
    document.querySelectorAll(".msg-text mark").forEach((m) => m.classList.remove("active"));
    const marks = Array.from(document.querySelectorAll(".msg-text mark"));
    if (!marks.length) return;

    const currentMark = marks[state.chatSearchIndex];
    if (!currentMark) return;

    currentMark.classList.add("active");
    currentMark.scrollIntoView({ block: "center", behavior: "smooth" });
  }

  function updateFindCount() {
    if (!els.findCount) return;
    if (!state.chatSearchMatches.length) {
      els.findCount.textContent = "0/0";
      return;
    }
    els.findCount.textContent = `${state.chatSearchIndex + 1}/${state.chatSearchMatches.length}`;
  }

  function toggleEmojiPanel() {
    if (!els.emojiPanel) return;
    if (!els.emojiPanel.innerHTML.trim()) {
      buildEmojiPanel();
    }
    els.emojiPanel.style.display = els.emojiPanel.style.display === "none" ? "grid" : "none";
  }

  function buildEmojiPanel() {
    if (!els.emojiPanel) return;
    const emojis = [
      "😀","😁","😂","🤣","😊","😍","😘","😎","🤩","🤔",
      "🙌","👏","👍","👋","🙏","🔥","✨","🎉","❤️","💬",
      "📅","📍","📞","💇","💅","🧴","🛍️","✅","❗","😉"
    ];

    els.emojiPanel.innerHTML = "";
    emojis.forEach((emoji) => {
      const b = document.createElement("button");
      b.type = "button";
      b.className = "emoji-item";
      b.textContent = emoji;
      b.addEventListener("click", () => {
        insertAtCursor(els.text, emoji);
        updateComposerState();
      });
      els.emojiPanel.appendChild(b);
    });
  }

  function insertAtCursor(input, text) {
    if (!input) return;
    const start = input.selectionStart ?? input.value.length;
    const end = input.selectionEnd ?? input.value.length;
    const before = input.value.slice(0, start);
    const after = input.value.slice(end);

    input.value = before + text + after;
    const pos = start + text.length;
    input.setSelectionRange(pos, pos);
    input.focus();
  }

  function setupCalendar() {
    if (!els.openCalendarBtn || !els.calModal || !els.calFrame) return;
    setCalendarMode(state.currentCalendarMode);

    els.openCalendarBtn.addEventListener("click", () => {
      els.calModal.setAttribute("aria-hidden", "false");
      els.calModal.classList.add("open");
    });

    if (els.calCloseBtn) {
      els.calCloseBtn.addEventListener("click", closeCalendar);
    }

    els.calModal.addEventListener("click", (e) => {
      if (e.target === els.calModal) closeCalendar();
    });

    if (els.calWeekBtn) {
      els.calWeekBtn.addEventListener("click", () => setCalendarMode("WEEK"));
    }

    if (els.calMonthBtn) {
      els.calMonthBtn.addEventListener("click", () => setCalendarMode("MONTH"));
    }

    if (els.calAddToggleBtn && els.calAdd) {
      els.calAddToggleBtn.addEventListener("click", () => {
        els.calAdd.hidden = !els.calAdd.hidden;
      });
    }

    if (els.calCreateBtn) {
      els.calCreateBtn.addEventListener("click", createCalendarEventLink);
    }
  }

  function setCalendarMode(mode) {
    state.currentCalendarMode = mode;
    if (els.calWeekBtn) els.calWeekBtn.classList.toggle("active", mode === "WEEK");
    if (els.calMonthBtn) els.calMonthBtn.classList.toggle("active", mode === "MONTH");

    const calendarId = state.me?.calendar_id || "";
    const calendarTz = state.me?.calendar_tz || "America/Argentina/Buenos_Aires";

    if (!calendarId || !els.calFrame) return;

    const src = new URL("https://calendar.google.com/calendar/embed");
    src.searchParams.set("src", calendarId);
    src.searchParams.set("ctz", calendarTz);
    src.searchParams.set("mode", mode);
    src.searchParams.set("showTitle", "0");
    src.searchParams.set("showTabs", "0");
    src.searchParams.set("showCalendars", "0");
    src.searchParams.set("showPrint", "0");
    src.searchParams.set("showNav", "1");
    els.calFrame.src = src.toString();
  }

  function createCalendarEventLink() {
    const title = (els.calTitle?.value || "").trim();
    const date = els.calDate?.value || "";
    const startTime = els.calStartTime?.value || "";
    const endTime = els.calEndTime?.value || "";
    const details = (els.calDetails?.value || "").trim();

    if (!title || !date || !startTime || !endTime) {
      alert("Completá título, fecha y horario.");
      return;
    }

    const start = buildCalendarDate(date, startTime);
    const end = buildCalendarDate(date, endTime);
    if (!start || !end) {
      alert("Fecha u horario inválidos.");
      return;
    }

    const url = new URL("https://calendar.google.com/calendar/render");
    url.searchParams.set("action", "TEMPLATE");
    url.searchParams.set("text", title);
    url.searchParams.set("details", details);
    url.searchParams.set("dates", `${start}/${end}`);
    if (state.me?.calendar_id) url.searchParams.set("src", state.me.calendar_id);

    window.open(url.toString(), "_blank", "noopener,noreferrer");
  }

  function buildCalendarDate(date, time) {
    try {
      const d = new Date(`${date}T${time}:00`);
      if (!Number.isFinite(d.getTime())) return "";
      const yyyy = d.getUTCFullYear();
      const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
      const dd = String(d.getUTCDate()).padStart(2, "0");
      const hh = String(d.getUTCHours()).padStart(2, "0");
      const mi = String(d.getUTCMinutes()).padStart(2, "0");
      const ss = String(d.getUTCSeconds()).padStart(2, "0");
      return `${yyyy}${mm}${dd}T${hh}${mi}${ss}Z`;
    } catch {
      return "";
    }
  }

  function closeCalendar() {
    if (!els.calModal) return;
    els.calModal.classList.remove("open");
    els.calModal.setAttribute("aria-hidden", "true");
  }

  function showTypingIndicator(text = "Escribiendo…") {
    hideTypingIndicator();

    if (!els.msgs || !state.currentPeer) return;

    const wrap = document.createElement("div");
    wrap.className = "msg-wrap in typing-wrap";
    wrap.id = "typingIndicator";
    wrap.innerHTML = `
      <div class="msg-bubble in typing-bubble">
        <div class="typing-row">
          <span class="typing-label">${escapeHtml(text)}</span>
          <span class="typing-dots"><i></i><i></i><i></i></span>
        </div>
      </div>
    `;
    els.msgs.appendChild(wrap);
    scrollMessagesToBottom();
  }

  function hideTypingIndicator() {
    const t = document.getElementById("typingIndicator");
    if (t) t.remove();
  }

  function requestBrowserNotifications() {
    if (!("Notification" in window)) return;
    if (Notification.permission === "default") {
      Notification.requestPermission().catch(() => {});
    }
  }

  function maybeNotifyNewMessage(conv) {
    if (!("Notification" in window)) return;
    if (Notification.permission !== "granted") return;
    if (!document.hidden && state.currentPeer === conv.wa_peer) return;

    const body = conv.last_text || "Nuevo mensaje";
    const title = conv.name || conv.wa_peer || "Nuevo mensaje";

    try {
      const n = new Notification(title, { body });
      setTimeout(() => n.close(), 5000);
    } catch (_) {}
  }

  function flashWindowTitle() {
    const original = document.title;
    let count = 0;
    const timer = setInterval(() => {
      document.title = document.title === "Nuevo mensaje · SOMA." ? original : "Nuevo mensaje · SOMA.";
      count += 1;
      if (count >= 6 || !document.hidden) {
        clearInterval(timer);
        document.title = original;
      }
    }, 700);
  }

  function toggleTheme() {
    const next = state.theme === "dark" ? "light" : "dark";
    applyTheme(next);
  }

  function applyTheme(theme) {
    state.theme = theme;
    localStorage.setItem("soma_theme", theme);

    document.body.classList.remove("theme-dark", "theme-light");
    document.body.classList.add(theme === "dark" ? "theme-dark" : "theme-light");

    if (els.themeIcon) {
      els.themeIcon.textContent = theme === "dark" ? "☀" : "☾";
    }

    if (els.brandLogo) {
      els.brandLogo.src = theme === "dark"
        ? els.brandLogo.dataset.dark
        : els.brandLogo.dataset.light;
    }
  }

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
      .msg-wrap{
        display:flex;
        margin:8px 0;
        width:100%;
        animation:fadeInUp .18s ease;
      }
      .msg-wrap.in{justify-content:flex-start}
      .msg-wrap.out{justify-content:flex-end}
      .msg-bubble{
        max-width:min(78%,560px);
        border-radius:20px;
        padding:12px 14px 8px;
        box-shadow:0 8px 20px rgba(0,0,0,.06);
      }
      .msg-bubble.in{
        background:#fff;
      }
      .msg-bubble.out{
        background:#e9f7ff;
      }
      .theme-dark .msg-bubble.in{
        background:#1c1f26;
      }
      .theme-dark .msg-bubble.out{
        background:#183446;
      }
      .msg-body{
        word-break:break-word;
      }
      .msg-text{
        white-space:pre-wrap;
        line-height:1.35;
      }
      .msg-meta{
        margin-top:6px;
        font-size:.78rem;
        opacity:.65;
        text-align:right;
      }
      .msg-image{
        max-width:100%;
        border-radius:14px;
        display:block;
        margin-bottom:8px;
      }
      .msg-file{
        display:inline-flex;
        padding:8px 12px;
        border-radius:12px;
        text-decoration:none;
      }
      .typing-row{
        display:flex;
        align-items:center;
        gap:10px;
      }
      .typing-label{
        font-size:.92rem;
        opacity:.8;
      }
      .typing-dots{
        display:inline-flex;
        gap:4px;
        align-items:center;
      }
      .typing-dots i{
        width:6px;
        height:6px;
        border-radius:999px;
        background:currentColor;
        opacity:.45;
        animation:typingBounce 1.1s infinite ease-in-out;
      }
      .typing-dots i:nth-child(2){animation-delay:.15s}
      .typing-dots i:nth-child(3){animation-delay:.30s}
      .emoji-panel{
        grid-template-columns:repeat(10,1fr);
        gap:8px;
        padding:10px;
      }
      .emoji-item{
        border:none;
        background:transparent;
        cursor:pointer;
        font-size:20px;
        border-radius:10px;
        padding:6px;
      }
      .emoji-item:hover{
        background:rgba(120,120,120,.10);
      }
      .file-chip{
        display:flex;
        align-items:center;
        gap:10px;
      }
      .chat-search mark{
        background:#fde68a;
      }
      .msg-text mark{
        background:#fde68a;
        color:inherit;
        border-radius:4px;
        padding:0 2px;
      }
      .msg-text mark.active{
        background:#f59e0b;
      }
      .modal.open{
        display:flex !important;
      }
      .cal-tab-btn.active{
        outline:2px solid rgba(59,130,246,.35);
      }
      @keyframes typingBounce{
        0%,80%,100%{transform:translateY(0);opacity:.4}
        40%{transform:translateY(-4px);opacity:1}
      }
      @keyframes fadeInUp{
        from{opacity:0; transform:translateY(6px)}
        to{opacity:1; transform:translateY(0)}
      }
    `;
    document.head.appendChild(style);
  }

  function highlightText(text, query) {
    if (!query) return nl2br(escapeHtml(text));
    const escaped = escapeRegExp(query);
    const re = new RegExp(`(${escaped})`, "gi");
    return nl2br(escapeHtml(text).replace(re, "<mark>$1</mark>"));
  }

  function nl2br(str) {
    return String(str).replace(/\n/g, "<br>");
  }

  function escapeHtml(value) {
    return String(value ?? "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#039;");
  }

  function escapeRegExp(str) {
    return String(str).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  }

  function FalseBoolean() {
    return false;
  }
})();
