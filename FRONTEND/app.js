// ====== CONFIG ======
console.log("🔍 [DEBUG] app.js script loaded!");
const BACKEND_URL = window.location.origin;
// ako backend nije isti origin/port, otkomentariši sledeće:
// const BACKEND_URL = "http://127.0.0.1:8000";

// Global loader state
let globalLoaderActive = false;
let loaderCheckInterval = null;
let globalLoaderCheckCount = 0;
const MAX_GLOBAL_LOADER_CHECKS = 100; // Maksimalno 100 provera (10 sekundi sa 100ms intervalom)

// ====== SMALL UTILS ======
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const fmt = (v, suffix = "") =>
  v === null || v === undefined || Number.isNaN(v) ? "No Data" : `${v}${suffix}`;

async function parseJsonSafe(resp) {
  const ct = resp.headers.get("content-type") || "";
  if (ct.includes("application/json")) return await resp.json();
  const txt = await resp.text();
  throw new Error(`HTTP ${resp.status}: ${txt.slice(0, 200)}`);
}

// === Loader helpers ===
window.sleep = window.sleep || (ms => new Promise(r => setTimeout(r, ms)));

function disableAllButtons(disable) {
  const buttons = document.querySelectorAll('button, .btn, #savePdf, .primary-ghost');
  buttons.forEach(btn => {
    if (disable) {
      btn.disabled = true;
      btn.style.pointerEvents = 'none';
      btn.style.opacity = '0.5';
    } else {
      btn.disabled = false;
      btn.style.pointerEvents = 'auto';
      btn.style.opacity = '1';
    }
  });
}

function ensureLoaderUI() {
  console.log("🔍 [DEBUG] ensureLoaderUI called");
  if (document.getElementById("loaderOverlay")) {
    console.log("🔍 [DEBUG] loaderOverlay already exists");
    return;
  }
  
  console.log("🔍 [DEBUG] creating loaderOverlay");
  const overlay = document.createElement("div");
  overlay.id = "loaderOverlay";
  overlay.innerHTML = `
    <div id="loaderBox" role="dialog" aria-live="polite" aria-label="Preparing">
      <div id="loaderTitle">🚀 Preparing Analysis...</div>
      <div id="loaderSpinner"></div>
      <div id="loaderDetail">Initializing system...</div>
    </div>`;
  
  document.body.appendChild(overlay);
  console.log("🔍 [DEBUG] loaderOverlay created and appended to body");
}
function showLoader(title = "🚀 Preparing Analysis...") {
  console.log("🔍 [DEBUG] showLoader called with title:", title);
  ensureLoaderUI();
  
  const overlay = document.getElementById("loaderOverlay");
  if (!overlay) {
    console.error("❌ [ERROR] loaderOverlay not found!");
    return;
  }
  
  // Set content
  const titleEl = document.getElementById("loaderTitle");
  const detailEl = document.getElementById("loaderDetail");
  
  if (titleEl) {
    titleEl.textContent = title;
  }
  
  if (detailEl) {
    detailEl.textContent = "Initializing system...";
  }
  
  // Make loader visible with high z-index
  console.log("🔍 [DEBUG] Setting loader styles");
  overlay.style.zIndex = "99999";
  overlay.style.display = "flex";
  overlay.style.visibility = "visible";
  overlay.style.opacity = "1";
  overlay.style.position = "fixed";
  overlay.style.top = "0";
  overlay.style.left = "0";
  overlay.style.width = "100vw";
  overlay.style.height = "100vh";
  
  // Force a reflow to ensure styles are applied
  overlay.offsetHeight;
  console.log("🔍 [DEBUG] Loader styles applied, display:", overlay.style.display, "visibility:", overlay.style.visibility);
  
  // Disable all buttons during loading
  disableAllButtons(true);
}

// Custom Modal System
function ensureCustomModalUI() {
  if (document.getElementById("customModalOverlay")) return;
  const overlay = document.createElement("div");
  overlay.id = "customModalOverlay";
  overlay.innerHTML = `
    <div id="customModal">
      <div id="customModalTitle">Title</div>
      <div id="customModalMessage">Message</div>
      <div id="customModalButtons"></div>
    </div>`;
  document.body.appendChild(overlay);
}

function showCustomModal(title, message, buttons = []) {
  ensureCustomModalUI();
  
  const overlay = document.getElementById("customModalOverlay");
  const modalTitle = document.getElementById("customModalTitle");
  const modalMessage = document.getElementById("customModalMessage");
  const modalButtons = document.getElementById("customModalButtons");
  
  modalTitle.textContent = title;
  modalMessage.textContent = message;
  
  // Clear existing buttons
  modalButtons.innerHTML = '';
  
  // Add buttons
  buttons.forEach(button => {
    const btn = document.createElement("button");
    btn.className = `customModalBtn ${button.type || 'primary'}`;
    btn.textContent = button.text;
    btn.onclick = () => {
      if (button.onClick) button.onClick();
      hideCustomModal();
    };
    modalButtons.appendChild(btn);
  });
  
  // Show modal
  overlay.style.display = "flex";
  overlay.style.zIndex = "100000";
  
  // Close on overlay click
  overlay.onclick = (e) => {
    if (e.target === overlay) {
      hideCustomModal();
    }
  };
}

function hideCustomModal() {
  const overlay = document.getElementById("customModalOverlay");
  if (overlay) {
    overlay.style.display = "none";
  }
}

// Replace browser alerts with custom modals
function showNotification(title, message) {
  showCustomModal(title, message, [
    { text: "OK", type: "primary", onClick: () => {} }
  ]);
}

function showError(title, message) {
  showCustomModal(title, message, [
    { text: "OK", type: "danger", onClick: () => {} }
  ]);
}

function showSuccess(title, message) {
  showCustomModal(title, message, [
    { text: "OK", type: "success", onClick: () => {} }
  ]);
}

function showConfirm(title, message, onConfirm, onCancel = null) {
  showCustomModal(title, message, [
    { text: "Cancel", type: "secondary", onClick: onCancel },
    { text: "Confirm", type: "primary", onClick: onConfirm }
  ]);
}
function updateLoader(detail) {
  ensureLoaderUI();
  if (detail !== undefined && detail !== null) {
    document.getElementById("loaderDetail").textContent = String(detail);
  }
}
function hideLoader() {
  console.log("🔍 [DEBUG] hideLoader called");
  const el = document.getElementById("loaderOverlay");
  if (el) {
    console.log("🔍 [DEBUG] Hiding loader, current display:", el.style.display);
    el.style.display = "none";
    console.log("🔍 [DEBUG] Loader hidden, new display:", el.style.display);
  } else {
    console.log("🔍 [DEBUG] loaderOverlay not found in hideLoader");
  }
  
  // Re-enable all buttons after loading
  disableAllButtons(false);
  
  // Stop global loader checking
  if (loaderCheckInterval) {
    clearInterval(loaderCheckInterval);
    loaderCheckInterval = null;
  }
  globalLoaderActive = false;
}

// Global loader functions
async function checkGlobalLoaderStatus() {
  try {
    globalLoaderCheckCount++;
    
    // Zaustavi provere ako je prekoračio limit
    if (globalLoaderCheckCount > MAX_GLOBAL_LOADER_CHECKS) {
      console.log("🛑 [GLOBAL LOADER] Max checks reached, stopping polling");
      stopGlobalLoaderPolling();
      hideGlobalLoader();
      return;
    }
    
    const response = await fetch('/api/global-loader-status');
    const data = await response.json();
    
    // Proveri da li je job zastareo (stariji od 5 minuta)
    if (data.active && data.started_at) {
      const startTime = new Date(data.started_at);
      const now = new Date();
      const diffMinutes = (now - startTime) / (1000 * 60);
      
      if (diffMinutes > 5) {
        console.log("⚠️ [GLOBAL LOADER] Job is stale (older than 5 minutes), stopping polling");
        stopGlobalLoaderPolling();
        hideGlobalLoader();
        return;
      }
    }
    
    if (data.active && !globalLoaderActive) {
      console.log("🌍 [GLOBAL LOADER] Showing global loader:", data);
      showGlobalLoader(data.detail || "Preparing analysis...", data.progress || 0);
    } else if (!data.active && globalLoaderActive) {
      console.log("🌍 [GLOBAL LOADER] Hiding global loader");
      hideGlobalLoader();
      stopGlobalLoaderPolling();
    } else if (data.active && globalLoaderActive) {
      // Ažuriraj postojeći loader
      updateGlobalLoader(data.detail || "Preparing analysis...", data.progress || 0);
    }
    
  } catch (error) {
    console.error("❌ [GLOBAL LOADER] Error checking status:", error);
    // Ako ima grešku, zaustavi polling
    stopGlobalLoaderPolling();
    hideGlobalLoader();
  }
}

// Funkcija za proveru globalnog loader statusa BEZ automatskog prikazivanja
async function checkGlobalLoaderStatusSilent() {
  try {
    const response = await fetch('/api/global-loader-status');
    const data = await response.json();
    
    if (data.active && !globalLoaderActive) {
      console.log("🌍 [GLOBAL LOADER] Found active job, starting polling:", data);
      // Pokreni polling samo ako postoji aktivan job
      startGlobalLoaderPolling();
    }
    
    return data;
  } catch (error) {
    console.error("❌ [GLOBAL LOADER] Error checking status silently:", error);
    return { active: false };
  }
}

// Funkcija za proveru da li se radi Prepare Day
async function isPrepareDayRunning() {
  try {
    const response = await fetch('/api/global-loader-status');
    const data = await response.json();
    console.log("🔍 [PREPARE CHECK] API Response:", data);
    console.log("🔍 [PREPARE CHECK] Is active:", data.active);
    console.log("🔍 [PREPARE CHECK] Status:", data.status);
    
    // Ako je job stariji od 5 minuta, smatraj ga "zastarelim" i ne čekaj ga
    if (data.active && data.started_at) {
      const startTime = new Date(data.started_at);
      const now = new Date();
      const diffMinutes = (now - startTime) / (1000 * 60);
      
      if (diffMinutes > 5) {
        console.log("⚠️ [PREPARE CHECK] Job is older than 5 minutes, considering it stale");
        return false;
      }
    }
    
    return data.active === true;
  } catch (error) {
    console.error("❌ [PREPARE CHECK] Error checking prepare status:", error);
    return false;
  }
}

// Funkcija za čekanje da se Prepare Day završi
async function waitForPrepareDayToComplete() {
  console.log("⏳ [PREPARE WAIT] Waiting for Prepare Day to complete...");
  
  while (true) {
    const isRunning = await isPrepareDayRunning();
    
    if (!isRunning) {
      console.log("✅ [PREPARE WAIT] Prepare Day completed, proceeding with analysis");
      return true;
    }
    
    console.log("⏳ [PREPARE WAIT] Prepare Day still running, waiting...");
    await sleep(2000); // Čekaj 2 sekunde pre sledeće provere
  }
}

function showGlobalLoader(title, progress = 0, detail = "Please wait...") {
  globalLoaderActive = true;
  showLoader(title);
  updateLoader(detail);
  
  // Pokreni polling ako nije već pokrenut
  if (!loaderCheckInterval) {
    startGlobalLoaderPolling();
  }
}

function updateGlobalLoader(detail, progress = 0) {
  updateLoader(detail);
}

function hideGlobalLoader() {
  globalLoaderActive = false;
  hideLoader();
  stopGlobalLoaderPolling();
}

function startGlobalLoaderPolling() {
  if (loaderCheckInterval) {
    clearInterval(loaderCheckInterval);
  }
  
  // Resetuj brojač
  globalLoaderCheckCount = 0;
  
  // Pokreni polling svakih 100ms
  loaderCheckInterval = setInterval(checkGlobalLoaderStatus, 100);
  
  // Dodaj timeout kao sigurnosnu mrežu - uvek sakrij loader nakon 30 sekundi
  setTimeout(() => {
    if (globalLoaderActive) {
      console.log("⏰ [GLOBAL LOADER] Timeout reached, force hiding loader");
      stopGlobalLoaderPolling();
      hideGlobalLoader();
    }
  }, 30000); // 30 sekundi
  
  console.log("🌍 [GLOBAL LOADER] Started polling");
}

function stopGlobalLoaderPolling() {
  if (loaderCheckInterval) {
    clearInterval(loaderCheckInterval);
    loaderCheckInterval = null;
  }
  
  console.log("🌍 [GLOBAL LOADER] Stopped polling");
}

// (ako nemaš već) bezbedno JSON parsiranje
async function parseJsonSafe(resp) {
  const ct = resp.headers.get("content-type") || "";
  if (ct.includes("application/json")) return await resp.json();
  const txt = await resp.text();
  throw new Error(`HTTP ${resp.status}: ${txt.slice(0, 200)}`);
}

// ====== THEME ======
function initTheme() {
  const root = document.documentElement;
  const saved = localStorage.getItem("ui-theme"); // 'light' | 'dark' | 'auto'
  if (!root.getAttribute("data-theme")) {
    root.setAttribute("data-theme", saved || "auto");
  }
  const btn = document.getElementById("themeToggle");
  if (btn) {
    btn.addEventListener("click", () => {
      const cur = root.getAttribute("data-theme") || "auto";
      const next = cur === "light" ? "dark" : cur === "dark" ? "auto" : "light";
      root.setAttribute("data-theme", next);
      localStorage.setItem("ui-theme", next);
      showToast(`Theme: ${next.toUpperCase()}`);
    });
  }
}

// ====== TOASTS ======
function ensureToastHost() {
  if (!document.getElementById("toastHost")) {
    const host = document.createElement("div");
    host.id = "toastHost";
    host.style.position = "fixed";
    host.style.right = "16px";
    host.style.bottom = "16px";
    host.style.zIndex = "9999";
    document.body.appendChild(host);
  }
}
function showToast(msg, kind = "info") {
  // Notifikacije su isključene
  return;
  
  ensureToastHost();
  const t = document.createElement("div");
  t.textContent = msg;
  t.style.marginTop = "10px";
  t.style.padding = "10px 12px";
  t.style.borderRadius = "12px";
  t.style.border = "1px solid var(--border)";
  t.style.background =
    kind === "error"
      ? "linear-gradient(180deg, rgba(239,68,68,.12), var(--surface))"
      : "linear-gradient(180deg, color-mix(in oklab, var(--surface) 92%, transparent), var(--surface-2))";
  t.style.boxShadow = "var(--shadow)";
  t.style.color = "var(--text)";
  t.style.fontSize = "14px";
  t.style.opacity = "0";
  t.style.transform = "translateY(6px)";
  t.style.transition = "240ms ease";
  document.getElementById("toastHost").appendChild(t);
  requestAnimationFrame(() => {
    t.style.opacity = "1";
    t.style.transform = "translateY(0)";
  });
  setTimeout(() => {
    t.style.opacity = "0";
    t.style.transform = "translateY(6px)";
    setTimeout(() => t.remove(), 240);
  }, 3000);
}

// ====== MODERN SHELL (radi i sa starim HTML-om) ======
function ensureModernShell() {
  const container = document.querySelector(".container") || document.body;

  // Header
  if (!document.querySelector(".app-header")) {
    const header = document.createElement("header");
    header.className = "app-header";
    header.innerHTML = `
      <div class="app-header__inner">
        <div class="brand">
          <svg width="26" height="26" viewBox="0 0 24 24"><path d="M12 2l3 7 7 1-5 5 1 7-6-3-6 3 1-7-5-5 7-1z"/></svg>
          <div class="brand__text">
            <strong>Sports Analysis</strong>
            <div class="sub">1H & FT markets</div>
          </div>
        </div>
        <div class="header-actions">
          <button id="savePdf" class="primary-ghost" title="Save PDF">
            <span>Save PDF</span>
          </button>
          <button id="themeToggle" class="icon-btn" title="Theme">
            <svg class="icon-sun" width="18" height="18" viewBox="0 0 24 24"><path d="M6.76 4.84l-1.8-1.79L3.17 4.84l1.79 1.8 1.8-1.8zM1 13h3v-2H1v2zm10-9h2V1h-2v3zm7.07 1.21l-1.79-1.8-1.8 1.8 1.8 1.79 1.79-1.79zM17 13h3v-2h-3v2zM7.05 18.36l-1.8 1.79 1.42 1.42 1.79-1.8-1.41-1.41zM13 23h-2v-3h2v3zm6.95-3.85l-1.79-1.79-1.41 1.41 1.79 1.8 1.41-1.42zM12 6a6 6 0 100 12 6 6 0 000-12z"/></svg>
            <svg class="icon-moon" width="18" height="18" viewBox="0 0 24 24"><path d="M21 12.79A9 9 0 1111.21 3 7 7 0 0021 12.79z"/></svg>
          </button>
        </div>
      </div>
    `;
    document.body.insertBefore(header, document.body.firstChild);
  }

  // Kontrole: probaj prebaciti stare inpute/dugmad u modern panel
  if (!document.querySelector(".controls__row")) {
    const panel = document.createElement("section");
    panel.className = "panel";
    panel.innerHTML = `
      <div class="controls__row">
        <div class="field">
          <label for="fromDate">From</label>
          <div class="field__input">
            <svg width="18" height="18" viewBox="0 0 24 24"><path d="M7 11h5v5H7z" opacity=".3"/><path d="M19 4h-1V2h-2v2H8V2H6v2H5c-1.11 0-2 .89-2 2v13c0 1.1.89 2 2 2h14c1.11 0 2-.9 2-2V6c0-1.11-.89-2-2-2zm0 15H5V9h14v10z"/></svg>
            <!-- fromDate lives here -->
          </div>
        </div>
        <div class="field">
          <label for="toDate">To</label>
          <div class="field__input">
            <svg width="18" height="18" viewBox="0 0 24 24"><path d="M7 11h5v5H7z" opacity=".3"/><path d="M19 4h-1V2h-2v2H8V2H6v2H5c-1.11 0-2 .89-2 2v13c0 1.1.89 2 2 2h14c1.11 0 2-.9 2-2V6c0-1.11-.89-2-2-2zm0 15H5V9h14v10z"/></svg>
            <!-- toDate lives here -->
          </div>
        </div>
        <div class="actions">
          <button id="analyze1p" class="btn primary">
            <svg width="20" height="20" viewBox="0 0 24 24" fill="currentColor">
              <path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-2 15l-5-5 1.41-1.41L10 14.17l7.59-7.59L19 8l-9 9z"/>
            </svg>
            <span>Analyze 1+ 1H</span>
          </button>
          <button id="analyzeGG" class="btn subtle">
            <svg width="20" height="20" viewBox="0 0 24 24" fill="currentColor">
              <path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-1 17.93c-3.94-.49-7-3.85-7-7.93 0-.62.08-1.21.21-1.79L9 15v1c0 1.1.9 2 2 2v1.93zm6.9-2.54c-.26-.81-1-1.39-1.9-1.39h-1v-3c0-.55-.45-1-1-1H8v-2h2c.55 0 1-.45 1-1V7h2c1.1 0 2-.9 2-2v-.41c2.93 1.19 5 4.06 5 7.41 0 2.08-.8 3.97-2.1 5.39z"/>
            </svg>
            <span>Analyze GG 1H</span>
          </button>
          <button id="analyze2plus" class="btn subtle">
            <svg width="20" height="20" viewBox="0 0 24 24" fill="currentColor">
              <path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm5 11H7v-2h10v2z"/>
            </svg>
            <span>Analyze 2+ 1H</span>
          </button>
          <button id="analyzeFT2plus" class="btn btn-ft">
            <svg width="20" height="20" viewBox="0 0 24 24" fill="currentColor">
              <path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-1 17.93c-3.94-.49-7-3.85-7-7.93 0-.62.08-1.21.21-1.79L9 15v1c0 1.1.9 2 2 2v1.93zm6.9-2.54c-.26-.81-1-1.39-1.9-1.39h-1v-3c0-.55-.45-1-1-1H8v-2h2c.55 0 1-.45 1-1V7h2c1.1 0 2-.9 2-2v-.41c2.93 1.19 5 4.06 5 7.41 0 2.08-.8 3.97-2.1 5.39z"/>
            </svg>
            <span>Analyze 2+ FT</span>
          </button>
          <button id="prepareDay" class="btn">
            <svg width="18" height="18" viewBox="0 0 24 24" fill="currentColor">
              <path d="M19 4h-4l-2-2H5a1 1 0 0 0-1 1v16h16V5a1 1 0 0 0-1-1z"/>
            </svg>
            <span>Prepare day</span>
          </button>
        </div>
      </div>
    `;
    // Umetni odmah ispod headera
    const afterHeaderTarget =
      document.querySelector(".app-header")?.nextSibling || document.body.firstChild;
    container.parentNode.insertBefore(panel, container);

    // Reparent stara polja (da ne imamo duple ID-jeve)
    const fromOld = document.getElementById("fromDate");
    const toOld = document.getElementById("toDate");
    const dateRangeLegacy = document.querySelector(".date-range");
    const btnLegacy = document.querySelector(".buttons");
    const fromSlot = panel.querySelector('.field__input:nth-of-type(1)') || panel.querySelector('.field .field__input');
    const toSlot = panel.querySelectorAll('.field .field__input')[1];

    if (fromOld) fromSlot.appendChild(fromOld);
    else {
      const inp = document.createElement("input");
      inp.type = "datetime-local";
      inp.id = "fromDate";
      fromSlot.appendChild(inp);
    }

    if (toOld) toSlot.appendChild(toOld);
    else {
      const inp = document.createElement("input");
      inp.type = "datetime-local";
      inp.id = "toDate";
      toSlot.appendChild(inp);
    }

    // Sakrij legacy blokove ako postoje (CSS ih već skriva, ali i JS fallback)
    if (dateRangeLegacy) dateRangeLegacy.style.display = "none";
    if (btnLegacy) btnLegacy.style.display = "none";
  }

  // Results grid
  if (!document.querySelector(".results__grid")) {
    const grid = document.createElement("section");
    grid.className = "results__grid";
    const top = document.getElementById("top5");
    const other = document.getElementById("other");
    if (top && other) {
      // Wrap sekcije u kartice
      const s1 = document.createElement("div");
      s1.className = "section";
      const s2 = document.createElement("div");
      s2.className = "section";
      // Uvijek stavi title sa count bedgom
      s1.innerHTML = `
        <h3>TOP 5 <span class="count" id="countTop">(0)</span></h3>
        <p class="section-description">These are the top 5 pairs for the selected period according to our analysis</p>
      `;
      s2.innerHTML = `
        <h3>OTHER <span class="count" id="countOther">(0)</span></h3>
        <p class="section-description">Other pairs for the selected period according to our analysis</p>
      `;
      s1.appendChild(top);
      s2.appendChild(other);
      grid.appendChild(s1);
      grid.appendChild(s2);
      // Dodaj pred container (ili unutar njega)
      const c = document.querySelector(".container") || document.body;
      c.appendChild(grid);
    }
  }


}

// ====== UI HELPERS ======
const ANALYZE_BUTTON_IDS = [
  "analyze1p",
  "analyzeGG",
  "analyze2plus",
  "analyzeFT2plus",
  "prepareDay",
];

async function parseJsonSafe(resp) {
  const ct = resp.headers.get("content-type") || "";
  if (ct.includes("application/json")) return await resp.json();
  const txt = await resp.text();
  throw new Error(`HTTP ${resp.status}: ${txt.substring(0, 200)}`);
}


function setBusyUI(busy, note = "") {
  // Don't change button text anymore - the loader handles the UI feedback
  document.body.style.cursor = busy ? "progress" : "";
}

// OLD showLoader function removed - was overriding the new one

// ====== NARATIV ======
function buildNarrative(m, marketHint) {
  const d = m.debug || {};
  const leagueBase = fmt(d.m_league, "%");
  const prior = fmt(d.prior_percent, "%");
  const micro = fmt(d.micro_percent, "%");
  const expSOT = fmt(d.exp_sot1h_total);
  const expDA = fmt(d.exp_da1h_total);
  const pos = fmt(d.pos_edge_percent, "%");
  const wshare = fmt(d.merge_weight_micro);
  const effPrior = fmt(d.effn_prior);
  const effMicro = fmt(d.effn_micro);

  const isGG = marketHint === "gg1h";
  const isO15_1H = marketHint === "1h_over15";
  const isO15_FT = marketHint === "ft_over15";

  let s = `${m.team1} vs ${m.team2}: ligaški baseline je oko ${leagueBase}. `;
  s += `Prior (recent forma + H2H) procenjuje ${prior}, dok mikro-signali (oček. SOT=${expSOT}, DA=${expDA}, posjed-edge=${pos}) daju ${micro}. `;

  if (isO15_1H) {
    const lt = fmt(d.lambda_total),
      lh = fmt(d.lambda_home),
      la = fmt(d.lambda_away);
    s += `Za 2+ gola u 1H koristimo Poisson aproksimaciju: λ_total≈${lt} (home ${lh}, away ${la}). `;
  } else if (isO15_FT) {
    const lt = fmt(d.lambda_total),
      lh = fmt(d.lambda_home),
      la = fmt(d.lambda_away);
    s += `Za 2+ golova FT koristimo Poisson aproksimaciju: λ_total≈${lt} (home ${lh}, away ${la}). `;
  } else if (isGG) {
    const ph = fmt(d.p_home_scores_1h, "%");
    const pa = fmt(d.p_away_scores_1h, "%");
    const rho = d.rho != null ? `, ρ≈${fmt(d.rho)}` : "";
    s += `Ind. verovatnoće da oba tima postignu gol u 1H su ${m.team1} ${ph} i ${m.team2} ${pa}${rho}. `;
  }

  s += `Spajanje je urađeno po preciznosti (effN prior=${effPrior}, micro=${effMicro}; udeo micro≈${wshare}), što daje konačnih ${fmt(
    m.final_percent,
    "%"
  )}.`;
  return s;
}

function buildSimplifiedNarrative(m, marketHint) {
  const d = m.debug || {};
  const isGG = marketHint === "gg1h";
  const isO15_1H = marketHint === "1h_over15";
  const isO15_FT = marketHint === "ft_over15";

  let marketDescription = "";
  if (isGG) {
    marketDescription = "da oba tima postignu gol u prvom poluvremenu";
  } else if (isO15_1H) {
    marketDescription = "da bude preko 1.5 golova u prvom poluvremenu";
  } else if (isO15_FT) {
    marketDescription = "da bude preko 1.5 golova u celom meču";
  } else {
    marketDescription = "da bude preko 0.5 golova u prvom poluvremenu";
  }

  const formData = "forma timova i istorijski rezultati";
  const microData = "mikro-signali i statistike";

  return `Na osnovu analize ${formData}, kao i ${microData}, finalna verovatnoća da će na ovom meču doći ${marketDescription} iznosi ${fmt(m.final_percent, '%')}.`;
}

// ====== RENDER ======
function getAnalysisTitle(market) {
  const titles = {
    '1h_over05': '⚽ Preko 0.5 golova - 1. poluvreme',
    'gg1h': '🥅 Oba tima da postignu gol - 1. poluvreme', 
    '1h_over15': '⚽ Preko 1.5 golova - 1. poluvreme',
    'ft_over15': '⚽ Preko 1.5 golova - ceo meč'
  };
  return titles[market] || '📈 Rezultati analize';
}

function renderResults(data, market) {
  const currentMarket = market || "1h_over05";
  window.currentAnalysisResults = data;

  const top5Container = document.getElementById("top5");
  const otherContainer = document.getElementById("other");

  const total = Array.isArray(data) ? data.length : 0;

  // Dodaj naslov analize iznad rezultata
  const analysisTitle = getAnalysisTitle(currentMarket);
  const resultsSection = document.querySelector('.results');
  if (resultsSection && !document.getElementById('analysis-title')) {
    const titleElement = document.createElement('div');
    titleElement.id = 'analysis-title';
    titleElement.className = 'analysis-title';
    titleElement.innerHTML = `
      <h2>${analysisTitle}</h2>
      <div class="analysis-subtitle">Analiza završena • ${total} mečeva pronađeno</div>
    `;
    resultsSection.insertBefore(titleElement, resultsSection.firstChild);
  } else if (document.getElementById('analysis-title')) {
    const titleEl = document.getElementById('analysis-title');
    titleEl.querySelector('h2').textContent = analysisTitle;
    titleEl.querySelector('.analysis-subtitle').textContent = `Analiza završena • ${total} mečeva pronađeno`;
  }

  // Clear content areas, preserve section titles and descriptions
  const top5ContentArea = top5Container?.querySelector('.section-content');
  const otherContentArea = otherContainer?.querySelector('.section-content');
  
  if (top5ContentArea) top5ContentArea.innerHTML = '';
  if (otherContentArea) otherContentArea.innerHTML = '';

  const cardHTML = (m) => {
    // Format kickoff time
    const formatKickoffTime = (kickoff) => {
      if (!kickoff) return "—";
      try {
        const date = new Date(kickoff);
        return date.toLocaleString('sr-RS', {
          day: '2-digit',
          month: '2-digit',
          year: 'numeric',
          hour: '2-digit',
          minute: '2-digit',
          timeZone: 'Europe/Belgrade'
        });
      } catch (e) {
        return "—";
      }
    };

    return `
      <div class="match">
        <div class="match-header">
          <div class="match-league">${fmt(m.league)}</div>
          <div class="match-teams">${fmt(m.team1)} vs ${fmt(m.team2)}</div>
          <div class="match-kickoff">${formatKickoffTime(m.kickoff)}</div>
        </div>

        <div class="final-result">
          <div class="final-probability">
            <span class="final-label">Final Probability</span>
            <span class="final-value">${fmt(m.final_percent, '%')}</span>
          </div>
          <div class="narrative">
            ${buildSimplifiedNarrative(m, currentMarket)}
          </div>
        </div>
      </div>
    `;
  };

  (data || []).forEach((match, index) => {
    const html = cardHTML(match);
    if (index < 5) {
      top5ContentArea.innerHTML = (top5ContentArea.innerHTML || "") + html;
    } else {
      otherContentArea.innerHTML = (otherContentArea.innerHTML || "") + html;
    }
  });
}

// ====== HELPERS ======
function normalizeResults(json) {
  if (Array.isArray(json)) return json;
  if (json == null) return [];
  if (Array.isArray(json.results)) return json.results;
  if (Array.isArray(json.data)) return json.data;
  if (Array.isArray(json.matches)) return json.matches;
  return [];
}

function setDefaultDatesIfEmpty() {
  const fromEl = document.getElementById("fromDate");
  const toEl = document.getElementById("toDate");
  if (!fromEl || !toEl) return;
  
  const today = new Date();
  const pad = (n) => String(n).padStart(2, "0");
  
  // Set to 8AM today
  const fromDate = new Date(today);
  fromDate.setHours(8, 0, 0, 0);
  
  // Set to 22PM (10PM) today
  const toDate = new Date(today);
  toDate.setHours(22, 0, 0, 0);
  
  const toLocal = (d) =>
    `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(
      d.getHours()
    )}:${pad(d.getMinutes())}`;
    
  if (!fromEl.value) fromEl.value = toLocal(fromDate);
  if (!toEl.value) toEl.value = toLocal(toDate);
}

function localYMD(d) {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

// ====== AUTHENTICATION CHECK ======
function isUserLoggedIn() {
  const user = localStorage.getItem('user');
  return user !== null && user !== undefined;
}

function showAuthRequiredModal() {
  showCustomModal(
    "🔐 Potrebna je autentifikacija",
    "Možete videti analizu mečeva samo kao ulogovani korisnik. Molim vas prijavite se da nastavite.\n\nAko nemate nalog, molim vas registrujte se da vidite analizu.",
    [
      { 
        text: "Prijavi se", 
        type: "primary", 
        onClick: () => {
          window.location.href = '/login';
        }
      },
      { 
        text: "Registruj se", 
        type: "secondary", 
        onClick: () => {
          window.location.href = '/register';
        }
      }
    ]
  );
}

// ====== MAIN ACTION ======
async function fetchAnalysis(type) {
  // Check if user is logged in first
  if (!isUserLoggedIn()) {
    showAuthRequiredModal();
    return;
  }

  const analysisTitles = {
    '1p': '🎯 Analiziram 1+ gol...',
    'GG': '⚽ Analiziram oba tima da postignu gol...',
    'O15': '🔥 Analiziram preko 1.5 golova...',
    'FT_O15': '🚀 Analiziram preko 1.5 golova ceo meč...'
  };

  // 1) Proveri da li se radi Prepare Day
  const isPrepareRunning = await isPrepareDayRunning();
  if (isPrepareRunning) {
    console.log("⏳ [ANALYSIS] Prepare Day is running, waiting for completion...");
    showLoader("⏳ Čekam da se završi priprema dana...");
    setBusyUI(true, "Čekam pripremu dana...");
    
    // Čekaj da se Prepare Day završi
    await waitForPrepareDayToComplete();
  }

  // 2) Pokreni analizu
  showLoader(analysisTitles[type] || '🔍 Analiziram...');
  
  // Prikaži sekciju sa rezultatima
  const resultsSection = document.getElementById('resultsSection');
  if (resultsSection) {
    resultsSection.style.display = 'block';
  }

  try {
    const fromEl = document.getElementById("fromDate");
    const toEl = document.getElementById("toDate");

    if (!fromEl || !toEl || !fromEl.value || !toEl.value) {
      showError("Potrebno je odabrati datume", "Molim vas odaberite i početni i završni datum.");
      hideLoader();
      return;
    }

    const fromDate = new Date(fromEl.value);
    const toDate = new Date(toEl.value);
    if (isNaN(fromDate.getTime()) || isNaN(toDate.getTime())) {
      showError("Neispravni datumi", "Neispravne vrednosti datuma.");
      hideLoader();
      return;
    }
    if (toDate < fromDate) {
      showError("Neispravan opseg datuma", "Završni datum/vreme mora biti posle početnog datuma/vremena.");
      hideLoader();
      return;
    }

    const fromIso = fromDate.toISOString();
    const toIso = toDate.toISOString();

    const fh = fromDate.getHours();
    const th = toDate.getHours() + ((toDate.getMinutes() || toDate.getSeconds()) ? 1 : 0);

    // market
    let market;
    if (type === "GG") market = "gg1h";
    else if (type === "O15") market = "1h_over15";
    else if (type === "FT_O15") market = "ft_over15";
    else market = "1h_over05";

    const url =
      `${BACKEND_URL}/api/analyze` +
      `?from_date=${encodeURIComponent(fromIso)}` +
      `&to_date=${encodeURIComponent(toIso)}` +
      `&from_hour=${fh}` +
      `&to_hour=${th}` +
      `&market=${encodeURIComponent(market)}&no_api=1`;

    console.log("👉 calling:", url);

    setBusyUI(true);

    const MAX_RETRIES = 6;
    let attempt = 0;
    while (true) {
      const res = await fetch(url, { headers: { Accept: "application/json" } });
      const raw = await res.text();

      let json;
      try {
        json = JSON.parse(raw);
      } catch {
        console.error("Non-JSON response:", raw);
        showError("Greška servera", `Server je vratio neispravan odgovor (nije JSON):\n${raw.slice(0, 300)}...`);
        hideLoader();
        break;
      }

      if (res.status === 429) {
        attempt += 1;
        if (attempt > MAX_RETRIES) {
          const msg = json?.detail || "Server je trenutno zauzet. Pokušaj ponovo.";
          showError("Greška servera", msg);
          hideLoader();
          break;
        }
        const wait = Math.min(1000 * Math.pow(1.6, attempt), 5000);
        const note = `Zauzeto (${attempt}/${MAX_RETRIES})… čekam ${(wait / 1000).toFixed(1)}s`;
        console.warn(`429, retry in ${wait}ms`);
        setBusyUI(true, note);
        await sleep(wait);
        continue;
      }

      if (!res.ok) {
        const msg = json?.detail || json?.error || JSON.stringify(json).slice(0, 300);
        console.error("Server error:", msg);
        showError("Greška servera", `Greška servera: ${msg}`);
        hideLoader();
        break;
      }

      const data = normalizeResults(json);
      console.log("🔎 Raw JSON:", json);
      console.log("✅ Normalized results length:", data.length);

      data.sort((a, b) => (b.final_percent ?? 0) - (a.final_percent ?? 0));
      renderResults(data, market);
      showToast(`Završeno • ${data.length} mečeva`, "ok");
      hideLoader();
      break;
    }
  } catch (err) {
    console.error("Fetch/parse error:", err);
    showError("Greška analize", `Greška tokom analize: ${err}`);
    hideLoader();
  } finally {
    setBusyUI(false);
  }
}

async function prepareDay() {
  console.log("🔍 [DEBUG] prepareDay function called!");
  try {
    // izaberi datum (From -> ili To -> ili danas)
    const fromEl = document.getElementById("fromDate");
    const toEl   = document.getElementById("toDate");
    let base = new Date();
    if (fromEl && fromEl.value) base = new Date(fromEl.value);
    else if (toEl && toEl.value) base = new Date(toEl.value);
    const dayStr = localYMD(base); // tvoja postojeća util funkcija

    console.log("🔍 [DEBUG] prepareDay - about to show loader");
    setBusyUI(true, `Pripremam ${dayStr}…`);
    showLoader(`🚀 Preparing ${dayStr}...`);
    console.log("🔍 [DEBUG] prepareDay - loader should be visible now");

    // 1) enqueue
    const user = localStorage.getItem('user');
    const userData = user ? JSON.parse(user) : null;
    const sessionId = userData ? userData.session_id : null;
    
    const resp = await fetch(`/api/prepare-day`, {
      method: "POST",
      headers: { 
        "Content-Type": "application/json", 
        "Accept": "application/json",
        "Authorization": `Bearer ${sessionId}`
      },
      body: JSON.stringify({ date: dayStr, prewarm: true, session_id: sessionId })
    });
    const data = await parseJsonSafe(resp);
    
    // Check for admin access error
    if (resp.status === 403) {
      throw new Error("Access denied. Admin privileges required.");
    }
    if (resp.status === 401) {
      throw new Error("Authentication required. Please log in.");
    }
    
    if (!data.ok || !data.job_id) throw new Error("Neuspešno pokretanje prepare posla");

    const jobId = data.job_id;
    updateLoader("queued");

    // 2) Pokreni globalni loader polling
    console.log("🌍 [GLOBAL LOADER] Starting global loader for prepare day");
    // startGlobalLoaderPolling(); // DISABLED - was hiding our loader

    // 3) poll status
    let lastProgress = -1;
    while (true) {
      await sleep(3000);
      const sResp = await fetch(`/api/prepare-day/status?job_id=${encodeURIComponent(jobId)}`, {
        headers: { "Accept": "application/json" }
      });
      const sData = await parseJsonSafe(sResp);

      if (sData.status === "queued" || sData.status === "running") {
        if (sData.progress !== lastProgress) {
          lastProgress = sData.progress;
          updateLoader(sData.detail || "");
        }
        continue;
      }

      if (sData.status === "done") {
        console.log("🔍 [DEBUG] prepareDay - status done, hiding loader");
        updateLoader("finished");
        const r = sData.result || {};
        const s = [
          `Dan: ${r.day}`,
          `Fixtures u DB: ${r.fixtures_in_db}`,
          `Timova: ${r.teams} | Parova: ${r.pairs}`,
          `Seeded fixtures: ${r.seeded ? "DA" : "NE"}`,
          `Nedostajalo prije: history=${r.history_missing_before}, h2h=${r.h2h_missing_before}`,
          `Stats missing prije: ${r.stats_missing_before}`,
          r.computed ? `Computed: ${Object.entries(r.computed).map(([k,v]) => `${k}: ${v}`).join(", ")}` : ""
        ].filter(Boolean).join("\n");
        showNotification("Prepare Day Complete", s);
        hideLoader();
        break;
      }

      if (sData.status === "error") {
        throw new Error(`Prepare-day greška: ${sData.detail || "nepoznato"}`);
      }

      await sleep(1500);
    }
  } catch (err) {
    console.log("🔍 [DEBUG] prepareDay - error occurred, hiding loader");
    console.error(err);
    showError("Prepare Day Error", `Prepare day error: ${err}`);
    showToast("Prepare-day greška", "error");
    hideLoader();
  } finally {
    console.log("🔍 [DEBUG] prepareDay - finally block, stopping global loader");
    setBusyUI(false);
    // Zaustavi globalni loader polling
    stopGlobalLoaderPolling();
  }
}

// ====== AUTHENTICATION FUNCTIONS ======
function checkAuthStatus() {
  const user = localStorage.getItem('user');
  const authButtons = document.getElementById('authButtons');
  const userMenu = document.getElementById('userMenu');
  const userName = document.getElementById('userName');
  const userEmail = document.getElementById('userEmail');
  const prepareDayBtn = document.getElementById('prepareDay');
  const usersBtn = document.getElementById('usersBtn');
  
  if (user) {
    // User is logged in
    const userData = JSON.parse(user);
    if (authButtons) authButtons.style.display = 'none';
    if (userMenu) userMenu.style.display = 'flex';
    if (userName) userName.textContent = userData.name || 'User';
    if (userEmail) userEmail.textContent = userData.email || 'user@example.com';
    
    // Show admin buttons only for admin user
    const isAdmin = userData.email === 'klisaricf@gmail.com';
    
    if (prepareDayBtn) {
      if (isAdmin) {
        prepareDayBtn.style.display = 'flex';
      } else {
        prepareDayBtn.style.display = 'none';
      }
    }
      if (usersBtn) {
    if (isAdmin) {
      usersBtn.style.display = 'flex';
      console.log('✅ Users button shown for admin');
    } else {
      usersBtn.style.display = 'none';
      console.log('❌ Users button hidden for non-admin');
    }
  } else {
    console.log('❌ Users button element not found');
  }
  } else {
    // User is not logged in
    if (authButtons) authButtons.style.display = 'flex';
    if (userMenu) userMenu.style.display = 'none';
    if (prepareDayBtn) prepareDayBtn.style.display = 'none';
    if (usersBtn) usersBtn.style.display = 'none';
  }
}

async function logout() {
  try {
    const user = localStorage.getItem('user');
    if (user) {
      const userData = JSON.parse(user);
      if (userData.session_id) {
        // Call backend logout
        await fetch(`${BACKEND_URL}/api/auth/logout`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            session_id: userData.session_id
          })
        });
      }
    }
  } catch (error) {
    console.error('Logout error:', error);
  } finally {
    // Always clear local storage and update UI
    localStorage.removeItem('user');
    checkAuthStatus();
    showToast('Logged out successfully!', 'success');
  }
}

// ====== WIRE EVENTS once DOM is ready ======
document.addEventListener("DOMContentLoaded", () => {
  // NE proveravaj globalni loader status automatski - samo kada je potrebno
  // 1) Kreiraj moderni shell (radi i sa starim HTML-om)
  ensureModernShell();

  // 2) Tema
  initTheme();

  // 3) Authentication
  checkAuthStatus();
  
  // 4) Initialize logout button
  const logoutBtn = document.getElementById('logoutBtn');
  if (logoutBtn) {
    logoutBtn.addEventListener('click', logout);
  }

  // 5) Podrazumijevani datumi (ako su prazni)
  setDefaultDatesIfEmpty();
  
  // 4) Start checking for global loader status
  // checkGlobalLoaderStatus(); // DISABLED - was causing too many requests
  // setInterval(checkGlobalLoaderStatus, 3000); // DISABLED

  // 4) Initialize prepare day modal
  initPrepareDayModal();
  
  // 5) Dugmad
  console.log("🔍 [DEBUG] Looking for buttons...");
  const btn1p = document.getElementById("analyze1p");
  const btnGG = document.getElementById("analyzeGG");
  const btn1pls = document.getElementById("analyze2plus");
  const btnFT2pl = document.getElementById("analyzeFT2plus");
  const btnPrep = document.getElementById("prepareDay");
  console.log("🔍 [DEBUG] prepareDay button found:", btnPrep);

  if (btn1p) btn1p.addEventListener("click", () => fetchAnalysis("1p"));
  if (btnGG) btnGG.addEventListener("click", () => fetchAnalysis("GG"));
  if (btn1pls) btn1pls.addEventListener("click", () => fetchAnalysis("O15"));
  if (btnFT2pl) btnFT2pl.addEventListener("click", () => fetchAnalysis("FT_O15"));
  if (btnPrep) {
    console.log("🔍 [DEBUG] Adding click listener to prepareDay button");
    btnPrep.addEventListener("click", () => {
      console.log("🔍 [DEBUG] Prepare Day button clicked!");
      showPrepareDayModal();
    });
  }
  
  // Add users button click listener
  const usersBtn = document.getElementById('usersBtn');
  if (usersBtn) {
    console.log("🔍 [DEBUG] Adding click listener to users button");
    usersBtn.addEventListener("click", (e) => {
      console.log("🔍 [DEBUG] Users button clicked!");
      e.preventDefault();
      loadUsersPage();
    });
  } else {
    console.log("🔍 [DEBUG] Users button not found");
  }
});

// ===== USERS PAGE FUNCTIONS =====

async function loadUsersPage() {
  console.log("🔍 [DEBUG] Loading users page dynamically");
  
  try {
    // Hide main content and show loading
    hideMainContent();
    showUsersPage();
    
    // Load users data from API
    const response = await fetch('/api/users', {
      headers: {
        'Authorization': `Bearer ${localStorage.getItem('token')}`
      }
    });
    
    console.log("📡 [DEBUG] API response status:", response.status);
    
    if (!response.ok) {
      const errorText = await response.text();
      console.log("❌ [DEBUG] API error response:", errorText);
      throw new Error(`Failed to load users: ${response.status} - ${errorText}`);
    }
    
    const data = await response.json();
    console.log("✅ [DEBUG] Users data loaded:", data);
    console.log("✅ [DEBUG] Number of users:", data.users ? data.users.length : 0);
    
    // Render users page
    renderUsersPage(data.users || []);
    
  } catch (error) {
    console.error("❌ [ERROR] Failed to load users page:", error);
    showError("Failed to load users page. Please try again.");
  }
}

function hideMainContent() {
  const mainContent = document.querySelector('.main-content');
  const analysisSection = document.querySelector('.analysis-section');
  const resultsSection = document.querySelector('.results-section');
  const headerControls = document.querySelector('.header-controls');
  
  if (mainContent) mainContent.style.display = 'none';
  if (analysisSection) analysisSection.style.display = 'none';
  if (resultsSection) resultsSection.style.display = 'none';
  if (headerControls) headerControls.style.display = 'none';
}

function showUsersPage() {
  // Create users page container
  let usersContainer = document.getElementById('usersPageContainer');
  if (!usersContainer) {
    usersContainer = document.createElement('div');
    usersContainer.id = 'usersPageContainer';
    usersContainer.innerHTML = `
      <div class="users-page">
        <div class="page-header">
          <div class="page-title">
            <h1>Users Management</h1>
            <p>Manage registered users</p>
          </div>
          <div class="admin-info" id="adminInfo" style="display: none;">
            <span>Admin: <span id="adminEmail"></span></span>
            <button id="backToMain" class="btn subtle">← Back to Main</button>
          </div>
        </div>
        
        <div class="users-controls">
          <div class="search-container">
            <input type="text" id="userSearch" placeholder="Search users by name or email..." />
            <button id="clearSearch" class="btn subtle" style="display: none;">Clear</button>
          </div>
          <div class="users-stats">
            <span>Total: <span id="totalUsers">0</span></span>
            <span>Showing: <span id="showingUsers">0</span></span>
          </div>
        </div>
        
        <div class="users-table-container">
          <div id="tableLoading" class="loading">Loading users...</div>
          <div id="tableError" class="error" style="display: none;">
            <span id="errorMessage">Failed to load users</span>
          </div>
          <div id="noUsers" class="no-data" style="display: none;">
            No users found
          </div>
          <table id="usersTable" class="users-table" style="display: none;">
            <thead>
              <tr>
                <th>#</th>
                <th>First Name</th>
                <th>Last Name</th>
                <th>Email</th>
                <th>Registered</th>
              </tr>
            </thead>
            <tbody id="usersTableBody"></tbody>
          </table>
        </div>
        
        <div id="paginationContainer" class="pagination-container" style="display: none;">
          <div class="pagination-info">
            <span id="paginationInfo">Page 1 of 1</span>
          </div>
          <div class="pagination-controls">
            <button id="prevPage" class="btn subtle" disabled>Previous</button>
            <div id="paginationPages" class="pagination-pages"></div>
            <button id="nextPage" class="btn subtle" disabled>Next</button>
          </div>
        </div>
      </div>
    `;
    document.body.appendChild(usersContainer);
  }
  
  usersContainer.style.display = 'block';
  
  // Show admin info
  const adminInfo = document.getElementById('adminInfo');
  const adminEmail = document.getElementById('adminEmail');
  if (adminInfo) adminInfo.style.display = 'flex';
  if (adminEmail) adminEmail.textContent = localStorage.getItem('userEmail');
  
  // Add back button listener
  const backBtn = document.getElementById('backToMain');
  if (backBtn) {
    backBtn.addEventListener('click', () => {
      usersContainer.style.display = 'none';
      showMainContent();
    });
  }
}

function renderUsersPage(users) {
  console.log("🔍 [DEBUG] Rendering users page with", users.length, "users");
  
  const loading = document.getElementById('tableLoading');
  const error = document.getElementById('tableError');
  const table = document.getElementById('usersTable');
  const noUsers = document.getElementById('noUsers');
  
  if (loading) loading.style.display = 'none';
  if (error) error.style.display = 'none';
  
  if (!users || users.length === 0) {
    console.log("⚠️ [DEBUG] No users found or users array is empty");
    if (noUsers) {
      noUsers.style.display = 'flex';
      noUsers.textContent = 'No users found in database';
    }
    if (table) table.style.display = 'none';
    return;
  }
  
  if (noUsers) noUsers.style.display = 'none';
  if (table) table.style.display = 'table';
  
  // Update stats
  const totalUsers = document.getElementById('totalUsers');
  const showingUsers = document.getElementById('showingUsers');
  if (totalUsers) totalUsers.textContent = users.length;
  if (showingUsers) showingUsers.textContent = users.length;
  
  // Render table
  const tbody = document.getElementById('usersTableBody');
  if (tbody) {
    tbody.innerHTML = users.map((user, index) => {
      const registeredDate = new Date(user.created_at).toLocaleDateString('sr-RS');
      return `
        <tr>
          <td>${index + 1}</td>
          <td>${user.first_name}</td>
          <td>${user.last_name}</td>
          <td>${user.email}</td>
          <td>${registeredDate}</td>
        </tr>
      `;
    }).join('');
  }
  
  console.log("✅ [DEBUG] Users page rendered successfully");
}

function showMainContent() {
  const mainContent = document.querySelector('.main-content');
  const analysisSection = document.querySelector('.analysis-section');
  const resultsSection = document.querySelector('.results-section');
  const headerControls = document.querySelector('.header-controls');
  
  if (mainContent) mainContent.style.display = 'block';
  if (analysisSection) analysisSection.style.display = 'block';
  if (resultsSection) resultsSection.style.display = 'block';
  if (headerControls) headerControls.style.display = 'flex';
}

function showError(message) {
  const usersContainer = document.getElementById('usersPageContainer');
  if (usersContainer) {
    const error = document.getElementById('tableError');
    const errorMessage = document.getElementById('errorMessage');
    if (error) error.style.display = 'flex';
    if (errorMessage) errorMessage.textContent = message;
  }
}

// ===== PREPARE DAY MODAL FUNCTIONS =====

// Check if user is admin
function isAdmin() {
  const user = localStorage.getItem('user');
  if (!user) return false;
  
  try {
    const userData = JSON.parse(user);
    return userData.email === 'klisaricf@gmail.com';
  } catch (e) {
    return false;
  }
}

// Initialize prepare day modal
function initPrepareDayModal() {
  const prepareDayModal = document.getElementById('prepareDayModal');
  const prepareDatePicker = document.getElementById('prepareDatePicker');
  const confirmPrepareBtn = document.getElementById('confirmPrepare');
  const cancelPrepareBtn = document.getElementById('cancelPrepare');
  const closePrepareModal = document.getElementById('closePrepareModal');
  const prepareStatus = document.getElementById('prepareStatus');
  
  if (!prepareDayModal || !prepareDatePicker || !confirmPrepareBtn) {
    console.log("🔍 [DEBUG] Prepare day modal elements not found");
    return;
  }
  
  // Set date picker constraints (next 3 days)
  const today = new Date();
  const maxDate = new Date(today);
  maxDate.setDate(today.getDate() + 3);
  
  prepareDatePicker.min = today.toISOString().split('T')[0];
  prepareDatePicker.max = maxDate.toISOString().split('T')[0];
  
  // Set default to today
  prepareDatePicker.value = today.toISOString().split('T')[0];
  
  // Add event listeners
  confirmPrepareBtn.addEventListener('click', handleConfirmPrepare);
  cancelPrepareBtn.addEventListener('click', closeModal);
  closePrepareModal.addEventListener('click', closeModal);
  prepareDatePicker.addEventListener('change', handlePrepareDateChange);
  
  // Close modal when clicking outside
  prepareDayModal.addEventListener('click', (e) => {
    if (e.target === prepareDayModal) {
      closeModal();
    }
  });
  
  console.log("🔍 [DEBUG] Prepare day modal initialized");
}

// Show prepare day modal
function showPrepareDayModal() {
  const modal = document.getElementById('prepareDayModal');
  const prepareDatePicker = document.getElementById('prepareDatePicker');
  const prepareStatus = document.getElementById('prepareStatus');
  
  if (!isAdmin()) {
    showError("Access Denied", "Admin privileges required to prepare days.");
    return;
  }
  
  // Reset modal state
  prepareDatePicker.value = new Date().toISOString().split('T')[0];
  prepareStatus.textContent = '';
  prepareStatus.className = 'prepare-status';
  
  // Show modal
  modal.style.display = 'flex';
  document.body.style.overflow = 'hidden';
  
  // Check initial date status
  handlePrepareDateChange();
}

// Close modal
function closeModal() {
  const modal = document.getElementById('prepareDayModal');
  const prepareStatus = document.getElementById('prepareStatus');
  
  // Clear status message
  prepareStatus.textContent = '';
  prepareStatus.className = 'prepare-status';
  
  modal.style.display = 'none';
  document.body.style.overflow = 'auto';
}

// Handle prepare date picker change
async function handlePrepareDateChange() {
  const prepareDatePicker = document.getElementById('prepareDatePicker');
  const prepareStatus = document.getElementById('prepareStatus');
  
  if (prepareDatePicker.value) {
    prepareStatus.textContent = `Checking analysis status for ${prepareDatePicker.value}...`;
    prepareStatus.className = 'prepare-status info';
    
    // Check if analysis already exists
    try {
      const analysisStatus = await checkAnalysisExists(prepareDatePicker.value);
      updatePrepareStatus(analysisStatus);
    } catch (error) {
      console.error("Error checking analysis status:", error);
      prepareStatus.textContent = `Error checking status: ${error.message}`;
      prepareStatus.className = 'prepare-status error';
    }
  } else {
    prepareStatus.textContent = '';
    prepareStatus.className = 'prepare-status';
  }
}

// Check if analysis exists for a specific date
async function checkAnalysisExists(dateStr) {
  const user = localStorage.getItem('user');
  const userData = user ? JSON.parse(user) : null;
  const sessionId = userData ? userData.session_id : null;
  
  const response = await fetch(`/api/check-analysis-exists?date=${dateStr}`, {
    method: "GET",
    headers: {
      "Authorization": `Bearer ${sessionId}`
    }
  });
  
  if (!response.ok) {
    const errorData = await response.json();
    throw new Error(errorData.error || 'Failed to check analysis status');
  }
  
  return await response.json();
}

// Update prepare status display
function updatePrepareStatus(status) {
  const prepareStatus = document.getElementById('prepareStatus');
  const confirmPrepareBtn = document.getElementById('confirmPrepare');
  
  // Safely get values with fallbacks
  const date = status.date || 'selected date';
  const fixturesCount = status.fixtures_count || 0;
  const outputsCount = status.model_outputs_count || 0;
  
  if (status.analysis_complete) {
    prepareStatus.textContent = `✅ Analysis complete for ${date} (${fixturesCount} fixtures, ${outputsCount} outputs)`;
    prepareStatus.className = 'prepare-status success';
    confirmPrepareBtn.innerHTML = '<svg width="18" height="18" viewBox="0 0 24 24" aria-hidden="true"><path d="M19 4h-4l-2-2H5a1 1 0 0 0-1 1v16h16V5a1 1 0 0 0-1-1z"/></svg><span>Re-prepare Selected Day</span>';
    confirmPrepareBtn.disabled = false;
  } else if (status.analysis_exists) {
    prepareStatus.textContent = `⚠️ Partial analysis exists for ${date} (${fixturesCount} fixtures, ${outputsCount} outputs) - needs completion`;
    prepareStatus.className = 'prepare-status error';
    confirmPrepareBtn.innerHTML = '<svg width="18" height="18" viewBox="0 0 24 24" aria-hidden="true"><path d="M19 4h-4l-2-2H5a1 1 0 0 0-1 1v16h16V5a1 1 0 0 0-1-1z"/></svg><span>Complete Analysis</span>';
    confirmPrepareBtn.disabled = false;
  } else {
    prepareStatus.textContent = `❌ No analysis found for ${date} (${fixturesCount} fixtures) - needs preparation`;
    prepareStatus.className = 'prepare-status error';
    confirmPrepareBtn.innerHTML = '<svg width="18" height="18" viewBox="0 0 24 24" aria-hidden="true"><path d="M19 4h-4l-2-2H5a1 1 0 0 0-1 1v16h16V5a1 1 0 0 0-1-1z"/></svg><span>Prepare Selected Day</span>';
    confirmPrepareBtn.disabled = false;
  }
}

// Handle confirm prepare
async function handleConfirmPrepare() {
  const prepareDatePicker = document.getElementById('prepareDatePicker');
  const confirmPrepareBtn = document.getElementById('confirmPrepare');
  const prepareStatus = document.getElementById('prepareStatus');
  
  if (!prepareDatePicker.value) {
    prepareStatus.textContent = 'Please select a date first';
    prepareStatus.className = 'prepare-status error';
    return;
  }
  
  const selectedDate = prepareDatePicker.value;
  console.log("🔍 [DEBUG] Preparing selected day:", selectedDate);
  
  // Disable button and show loading
  confirmPrepareBtn.disabled = true;
  confirmPrepareBtn.innerHTML = '<svg width="18" height="18" viewBox="0 0 24 24" aria-hidden="true"><path d="M19 4h-4l-2-2H5a1 1 0 0 0-1 1v16h16V5a1 1 0 0 0-1-1z"/></svg><span>Preparing...</span>';
  prepareStatus.textContent = `Starting preparation for ${selectedDate}...`;
  prepareStatus.className = 'prepare-status info';
  
  try {
    // Call prepare day for selected date
    await prepareDayForDate(selectedDate);
    
    // Show success and close modal
    prepareStatus.textContent = `Successfully started preparation for ${selectedDate}`;
    prepareStatus.className = 'prepare-status success';
    
    // Close modal after a short delay
    setTimeout(() => {
      closeModal();
    }, 1500);
    
  } catch (error) {
    console.error("Error preparing selected day:", error);
    prepareStatus.textContent = `Error: ${error.message}`;
    prepareStatus.className = 'prepare-status error';
  } finally {
    // Re-enable button
    confirmPrepareBtn.disabled = false;
    confirmPrepareBtn.innerHTML = '<svg width="18" height="18" viewBox="0 0 24 24" aria-hidden="true"><path d="M19 4h-4l-2-2H5a1 1 0 0 0-1 1v16h16V5a1 1 0 0 0-1-1z"/></svg><span>Prepare Selected Day</span>';
  }
}

// Prepare day for specific date
async function prepareDayForDate(dateStr) {
  console.log("🔍 [DEBUG] prepareDayForDate called with date:", dateStr);
  
  try {
    // Show loader
    showLoader(`🚀 Preparing ${dateStr}...`);
    
    // Get user session
    const user = localStorage.getItem('user');
    const userData = user ? JSON.parse(user) : null;
    const sessionId = userData ? userData.session_id : null;
    
    console.log("🔍 [DEBUG] Session ID:", sessionId);
    console.log("🔍 [DEBUG] User data:", userData);
    
    // Call prepare day API
    const requestBody = { date: dateStr, prewarm: true, session_id: sessionId };
    console.log("🔍 [DEBUG] Request body:", requestBody);
    
    const resp = await fetch(`/api/prepare-day`, {
      method: "POST",
      headers: { 
        "Content-Type": "application/json", 
        "Accept": "application/json",
        "Authorization": `Bearer ${sessionId}`
      },
      body: JSON.stringify(requestBody)
    });
    
    console.log("🔍 [DEBUG] Response status:", resp.status);
    console.log("🔍 [DEBUG] Response headers:", Object.fromEntries(resp.headers.entries()));
    
    const data = await parseJsonSafe(resp);
    console.log("🔍 [DEBUG] Response data:", data);
    
    // Check for errors
    if (resp.status === 403) {
      throw new Error("Access denied. Admin privileges required.");
    }
    if (resp.status === 401) {
      throw new Error("Authentication required. Please log in.");
    }
    
    if (!data || !data.ok || !data.job_id) {
      throw new Error(`Failed to start prepare job: ${data?.error || 'Unknown error'}`);
    }
    
    const jobId = data.job_id;
    updateLoader("queued");
    
    // Poll for completion
    let lastProgress = -1;
    while (true) {
      await sleep(3000);
      
      const statusResp = await fetch(`/api/prepare-day/status?job_id=${jobId}`);
      const sData = await parseJsonSafe(statusResp);
      
      if (sData.status === "done") {
        console.log("🔍 [DEBUG] prepareDayForDate - status done, hiding loader");
        console.log("🔍 [DEBUG] sData.result:", sData.result);
        
        const result = sData.result || {};
        const fixturesCount = result.fixtures_in_db || result.fixtures || 0;
        const pairsCount = result.pairs || 0;
        const teamsCount = result.teams || 0;
        const duration = result.duration || 'unknown time';
        
        showSuccess("Prepare Day Complete", `Analysis preparation completed successfully for ${dateStr}!\n\n📊 Processed ${fixturesCount} fixtures\n👥 ${teamsCount} teams analyzed\n🔗 ${pairsCount} pairs created\n⏱️ Completed in ${duration}`);
        hideLoader();
        break;
      }
      
      if (sData.status === "error") {
        const errorDetail = sData.detail || "unknown";
        if (errorDetail.includes("pool exhausted") || errorDetail.includes("connection")) {
          throw new Error(`Database connection error. Please try again in a few moments. Server is busy.`);
        } else if (errorDetail.includes("HTTP 500") || errorDetail.includes("Internal Server Error")) {
          throw new Error(`Server error occurred. Please try again in a few moments.`);
        } else {
          throw new Error(`Prepare-day error: ${errorDetail}`);
        }
      }
      
      // Update progress
      if (sData.progress !== lastProgress) {
        lastProgress = sData.progress;
        updateLoader(sData.detail || "Processing...");
      }
    }
    
  } catch (err) {
    console.log("🔍 [DEBUG] prepareDayForDate - error occurred, hiding loader");
    console.error(err);
    
    // Check if it's a database connection error
    if (err.message.includes("Database connection error") || err.message.includes("pool exhausted")) {
      showError("Server Busy", `The server is currently busy with other requests. Please wait a moment and try again.\n\nError: ${err.message}`);
    } else if (err.message.includes("Server error occurred") || err.message.includes("HTTP 500")) {
      showError("Server Error", `A server error occurred. Please wait a moment and try again.\n\nError: ${err.message}`);
    } else {
      showError("Prepare Day Error", `Prepare day error: ${err.message}`);
    }
    
    showToast("Prepare-day greška", "error");
    hideLoader();
    throw err;
  } finally {
    setBusyUI(false);
  }
}

// Safety net: ako je DOM već gotov (npr. skripta učitana kasnije), pozovi ručno
if (document.readyState === "interactive" || document.readyState === "complete") {
  const evt = new Event("DOMContentLoaded");
  document.dispatchEvent(evt);
}

// Log da znamo da je JS podignut
console.log("app.js loaded");
