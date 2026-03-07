document.addEventListener("DOMContentLoaded", () => {
  const body = document.body;

  const toggleBtn = document.getElementById("themeToggle");
  const themeIcon = document.getElementById("themeIcon");
  const brandLogo = document.getElementById("brandLogo");
  const yearEl = document.getElementById("year");

  const demoBtn = document.getElementById("demoBtn");
  const loaderWrap = document.getElementById("loaderWrap");

  const THEME_KEY = "soma_theme";

  if (yearEl) {
    yearEl.textContent = new Date().getFullYear();
  }

  function isLightTheme() {
    return body.classList.contains("theme-light");
  }

  function applyTheme(theme) {
    const light = theme === "light";

    body.classList.toggle("theme-light", light);
    body.classList.toggle("theme-dark", !light);

    if (themeIcon) {
      themeIcon.textContent = light ? "☀" : "☾";
    }

    if (brandLogo) {
      const darkLogo = brandLogo.dataset.dark;
      const lightLogo = brandLogo.dataset.light;
      brandLogo.src = light ? lightLogo : darkLogo;
    }

    localStorage.setItem(THEME_KEY, theme);
  }

  function loadSavedTheme() {
    const savedTheme = localStorage.getItem(THEME_KEY);

    if (savedTheme === "light") {
      applyTheme("light");
    } else {
      applyTheme("dark");
    }
  }

  loadSavedTheme();

  if (toggleBtn) {
    toggleBtn.addEventListener("click", () => {
      applyTheme(isLightTheme() ? "dark" : "light");
    });
  }

  if (demoBtn && loaderWrap) {
    demoBtn.addEventListener("click", () => {
      if (demoBtn.disabled) return;

      demoBtn.disabled = true;
      loaderWrap.classList.add("show");

      setTimeout(() => {
        loaderWrap.classList.remove("show");
        demoBtn.disabled = false;

        window.location.href = "contacto.html";
      }, 900);
    });
  }
});
