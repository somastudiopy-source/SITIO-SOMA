(function () {
  const body = document.body;

  const toggleBtn = document.getElementById("themeToggle");
  const themeIcon = document.getElementById("themeIcon");

  const yearEl = document.getElementById("year");
  if (yearEl) yearEl.textContent = new Date().getFullYear();

  function syncIcon() {
    const isLight = body.classList.contains("theme-light");
    if (themeIcon) themeIcon.textContent = isLight ? "☀" : "☾";
  }

  function syncLogo() {
    const logo = document.getElementById("brandLogo");
    if (!logo) return;
    const isLight = body.classList.contains("theme-light");
    logo.src = isLight ? logo.dataset.light : logo.dataset.dark;
  }

  // Load theme from localStorage
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

  if (toggleBtn) {
    toggleBtn.addEventListener("click", () => {
      const isLight = body.classList.contains("theme-light");
      body.classList.toggle("theme-light", !isLight);
      body.classList.toggle("theme-dark", isLight);
      localStorage.setItem("soma_theme", !isLight ? "light" : "dark");
      syncIcon();
      syncLogo();
    });
  }

  // Demo button loader
  const demoBtn = document.getElementById("demoBtn");
  const loaderWrap = document.getElementById("loaderWrap");
  if (demoBtn && loaderWrap) {
    demoBtn.addEventListener("click", () => {
      loaderWrap.classList.add("show");
      demoBtn.disabled = true;

      setTimeout(() => {
        loaderWrap.classList.remove("show");
        demoBtn.disabled = false;
        alert("¡Listo! Después conectamos esto a tu WhatsApp / formulario.");
      }, 1100);
    });
  }

  // Contact button (placeholder)
  const contactBtn = document.getElementById("contactBtn");
  if (contactBtn) {
    contactBtn.addEventListener("click", () => {
      alert("Acá vamos a abrir WhatsApp o un formulario de contacto.");
    });
  }

  // ===============================
  // LOGIN REAL CONECTADO AL PANEL
  // ===============================

  const loginForm = document.getElementById("loginForm");
  const formMsg = document.getElementById("formMsg");

  if (loginForm) {
    loginForm.addEventListener("submit", async (e) => {
      e.preventDefault();

      const email = document.getElementById("email")?.value?.trim() || "";
      const password = document.getElementById("password")?.value?.trim() || "";

      if (!email || !password) {
        showMsg("Completá email y contraseña.", true);
        return;
      }

      const PANEL_BASE = "https://produccion-de-paneles-8c0u1.up.railway.app";

      try {
        showMsg("Ingresando…", false);

        const formData = new FormData();
        formData.append("email", email);
        formData.append("password", password);

        const response = await fetch(${PANEL_BASE}/login, {
          method: "POST",
          body: formData,
          credentials: "include",
        });

        if (response.status === 401) {
          showMsg("Email o contraseña incorrectos.", true);
          return;
        }

        // Si login OK, redirige al panel
        window.location.href = ${PANEL_BASE}/;

      } catch (error) {
        console.error(error);
        showMsg("Error conectando al panel. Intentá nuevamente.", true);
      }
    });
  }

  const forgot = document.getElementById("forgot");
  if (forgot) {
    forgot.addEventListener("click", (e) => {
      e.preventDefault();
      alert("Después conectamos recuperación de contraseña.");
    });
  }

  function showMsg(text, isError) {
    if (!formMsg) return;
    formMsg.classList.add("show");
    formMsg.textContent = text;
    formMsg.style.borderColor = isError
      ? "rgba(255,27,107,0.55)"
      : "rgba(0,97,255,0.55)";
  }
})();
