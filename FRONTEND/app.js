// ====== CONFIG ======
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
  if (document.getElementById("loaderOverlay")) return;
  
  const overlay = document.createElement("div");
  overlay.id = "loaderOverlay";
  overlay.innerHTML = `
    <div id="loaderBox" role="dialog" aria-live="polite" aria-label="Preparing">
      <div id="loaderTitle">🚀 Preparing Analysis...</div>
      <div id="loaderSpinner"></div>
      <div id="loaderDetail">Initializing system...</div>
    </div>`;
  
  document.body.appendChild(overlay);
}
function showLoader(title = "🚀 Preparing Analysis...") {
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
  const el = document.getElementById("loaderOverlay");
  if (el) {
    el.style.display = "none";
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
      s1.innerHTML = `<h3>TOP 5 <span class="count" id="countTop">(0)</span></h3>`;
      s2.innerHTML = `<h3>OTHER <span class="count" id="countOther">(0)</span></h3>`;
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

// ====== RENDER ======
function getAnalysisTitle(market) {
  const titles = {
    '1h_over05': '🎯 Over 0.5 Goals - 1st Half',
    'gg1h': '⚽ Both Teams to Score - 1st Half', 
    '1h_over15': '🎯 Over 1.5 Goals - 1st Half',
    'ft_over15': '🎯 Over 1.5 Goals - Full Time'
  };
  return titles[market] || '📊 Analysis Results';
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
      <div class="analysis-subtitle">Analysis completed • ${total} matches found</div>
    `;
    resultsSection.insertBefore(titleElement, resultsSection.firstChild);
  } else if (document.getElementById('analysis-title')) {
    const titleEl = document.getElementById('analysis-title');
    titleEl.querySelector('h2').textContent = analysisTitle;
    titleEl.querySelector('.analysis-subtitle').textContent = `Analysis completed • ${total} matches found`;
  }

  // naslovi sa count badge
  const countTop = document.getElementById("countTop");
  const countOther = document.getElementById("countOther");
  if (countTop) countTop.textContent = `(${total})`;
  if (top5Container)
    top5Container.innerHTML = `<div class="placeholder">Rendering top picks…</div>`;
  if (otherContainer) otherContainer.innerHTML = "";

  const cardHTML = (m) => {
    const t1Sample = m.team1_total > 0 ? ` (${m.team1_hits}/${m.team1_total})` : "";
    const t2Sample = m.team2_total > 0 ? ` (${m.team2_hits}/${m.team2_total})` : "";
    const h2hSample = m.h2h_total > 0 ? ` (${m.h2h_hits}/${m.h2h_total})` : "";

    const shotHome = m.home_shots_used > 0 ? fmt(m.home_shots_percent, "%") : "—";
    const shotAway = m.away_shots_used > 0 ? fmt(m.away_shots_percent, "%") : "—";

    const attHome = m.home_attacks_used > 0 ? fmt(m.home_attacks_percent, "%") : "—";
    const attAway = m.away_attacks_used > 0 ? fmt(m.away_attacks_percent, "%") : "—";

    const d = m.debug || {};
    const isGG = currentMarket === "gg1h";
    const isO15 = currentMarket === "1h_over15" || currentMarket === "ft_over15";
    const o15Label = currentMarket === "ft_over15" ? "Poisson λ (FT)" : "Poisson λ (1H)";

    return `
      <div class="match">
        <div class="match-header">
          <div class="match-league">${fmt(m.league)}</div>
          <div class="match-teams">${fmt(m.team1)} vs ${fmt(m.team2)}</div>
        </div>

        <div class="match-stats">
          <div class="stat-row">
            <span class="stat-label">${fmt(m.team1)}</span>
            <span class="stat-value">${fmt(m.team1_percent, '%')}</span>
            <span class="stat-sample">(${fmt(m.team1_hits)}/${fmt(m.team1_total)})</span>
          </div>
          <div class="stat-row">
            <span class="stat-label">${fmt(m.team2)}</span>
            <span class="stat-value">${fmt(m.team2_percent, '%')}</span>
            <span class="stat-sample">(${fmt(m.team2_hits)}/${fmt(m.team2_total)})</span>
          </div>
          <div class="stat-row">
            <span class="stat-label">H2H</span>
            <span class="stat-value">${fmt(m.h2h_percent, '%')}</span>
            <span class="stat-sample">(${fmt(m.h2h_hits)}/${fmt(m.h2h_total)})</span>
          </div>
        </div>

        <div class="micro-signals">
          <div class="micro-title">1H Micro Signals</div>
          <div class="micro-grid">
            <div class="micro-item">
              <span class="micro-label">Shots on Target</span>
              <span class="micro-values">H: ${shotHome} | A: ${shotAway}</span>
            </div>
            <div class="micro-item">
              <span class="micro-label">Dangerous Attacks</span>
              <span class="micro-values">H: ${attHome} | A: ${attAway}</span>
            </div>
            <div class="micro-item">
              <span class="micro-label">Form Average</span>
              <span class="micro-values">${fmt(m.form_percent, '%')}</span>
            </div>
          </div>
        </div>

        <div class="model-breakdown">
          <div class="breakdown-title">Model Analysis</div>
          <div class="breakdown-grid">
            <div class="breakdown-item">
              <span class="breakdown-label">Prior (Form + H2H)</span>
              <span class="breakdown-value">${fmt(d.prior_percent, '%')}</span>
            </div>
            <div class="breakdown-item">
              <span class="breakdown-label">Micro (League-normalized)</span>
              <span class="breakdown-value">${fmt(d.micro_percent, '%')}</span>
            </div>
            <div class="breakdown-item">
              <span class="breakdown-label">Micro Share</span>
              <span class="breakdown-value">${fmt(d.merge_weight_micro)}</span>
            </div>
          </div>
          ${isGG ? `
            <div class="breakdown-item">
              <span class="breakdown-label">Team 1 scores 1H</span>
              <span class="breakdown-value">${fmt(d.p_home_scores_1h, '%')}</span>
            </div>
            <div class="breakdown-item">
              <span class="breakdown-label">Team 2 scores 1H</span>
              <span class="breakdown-value">${fmt(d.p_away_scores_1h, '%')}</span>
            </div>
          ` : ``}
            ${isO15 ? `
            <div class="breakdown-item">
              <span class="breakdown-label">${o15Label}</span>
              <span class="breakdown-value">${fmt(d.lambda_total)}</span>
            </div>
          ` : ``}
        </div>

        <div class="final-result">
          <div class="final-probability">
            <span class="final-label">Final Probability</span>
            <span class="final-value">${fmt(m.final_percent, '%')}</span>
          </div>
          <div class="narrative">
            ${buildNarrative(m, currentMarket)}
          </div>
        </div>
      </div>
    `;
  };

  (data || []).forEach((match, index) => {
    const html = cardHTML(match);
    if (index < 5) top5Container.innerHTML = (top5Container.innerHTML || "") + html;
    else otherContainer.innerHTML = (otherContainer.innerHTML || "") + html;
  });

  if (countOther) {
    const otherCount = Math.max(total - 5, 0);
    countOther.textContent = `(${otherCount})`;
  }
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

// ====== MAIN ACTION ======
async function fetchAnalysis(type) {
  const analysisTitles = {
    '1p': '🎯 Analyzing 1+ Goals...',
    'GG': '⚽ Analyzing Both Teams Score...',
    'O15': '🔥 Analyzing Over 1.5 Goals...',
    'FT_O15': '🚀 Analyzing FT Over 1.5 Goals...'
  };

  // 1) Proveri da li se radi Prepare Day
  const isPrepareRunning = await isPrepareDayRunning();
  if (isPrepareRunning) {
    console.log("⏳ [ANALYSIS] Prepare Day is running, waiting for completion...");
    showLoader("⏳ Waiting for Prepare Day to complete...");
    setBusyUI(true, "Waiting for Prepare Day...");
    
    // Čekaj da se Prepare Day završi
    await waitForPrepareDayToComplete();
  }

  // 2) Pokreni analizu
  showLoader(analysisTitles[type] || '🔍 Analyzing...');

  const fromEl = document.getElementById("fromDate");
  const toEl = document.getElementById("toDate");

  if (!fromEl || !toEl || !fromEl.value || !toEl.value) {
    showError("Date Selection Required", "Please select both From and To dates.");
    return;
  }

  const fromDate = new Date(fromEl.value);
  const toDate = new Date(toEl.value);
  if (isNaN(fromDate.getTime()) || isNaN(toDate.getTime())) {
    showError("Invalid Dates", "Invalid date values.");
    return;
  }
  if (toDate < fromDate) {
    showError("Invalid Date Range", "End date/time must be after start date/time.");
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

  try {
    while (true) {
      const res = await fetch(url, { headers: { Accept: "application/json" } });
      const raw = await res.text();

      let json;
      try {
        json = JSON.parse(raw);
      } catch {
        console.error("Non-JSON response:", raw);
        showError("Server Error", `Server returned invalid response (not JSON):\n${raw.slice(0, 300)}...`);
        break;
      }

      if (res.status === 429) {
        attempt += 1;
        if (attempt > MAX_RETRIES) {
          const msg = json?.detail || "Server je trenutno zauzet. Pokušaj ponovo.";
          showError("Server Error", msg);
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
        showError("Server Error", `Server error: ${msg}`);
        break;
      }

      const data = normalizeResults(json);
      console.log("🔎 Raw JSON:", json);
      console.log("✅ Normalized results length:", data.length);

      data.sort((a, b) => (b.final_percent ?? 0) - (a.final_percent ?? 0));
      renderResults(data, market);
      showToast(`Gotovo • ${data.length} utakmica`, "ok");
      break;
    }
  } catch (err) {
    console.error("Fetch/parse error:", err);
    showError("Analysis Error", `Error during analysis: ${err}`);
  } finally {
    hideLoader();
    setBusyUI(false);
  }
}

async function prepareDay() {
  try {
    // izaberi datum (From -> ili To -> ili danas)
    const fromEl = document.getElementById("fromDate");
    const toEl   = document.getElementById("toDate");
    let base = new Date();
    if (fromEl && fromEl.value) base = new Date(fromEl.value);
    else if (toEl && toEl.value) base = new Date(toEl.value);
    const dayStr = localYMD(base); // tvoja postojeća util funkcija

    setBusyUI(true, `Pripremam ${dayStr}…`);
    showLoader(`🚀 Preparing ${dayStr}...`);

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
    startGlobalLoaderPolling();

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
        break;
      }

      if (sData.status === "error") {
        throw new Error(`Prepare-day greška: ${sData.detail || "nepoznato"}`);
      }

      await sleep(1500);
    }
  } catch (err) {
    console.error(err);
    showError("Prepare Day Error", `Prepare day error: ${err}`);
    showToast("Prepare-day greška", "error");
  } finally {
    hideLoader();
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
  
  if (user) {
    // User is logged in
    const userData = JSON.parse(user);
    if (authButtons) authButtons.style.display = 'none';
    if (userMenu) userMenu.style.display = 'flex';
    if (userName) userName.textContent = userData.name || 'User';
    if (userEmail) userEmail.textContent = userData.email || 'user@example.com';
    
    // Show Prepare Day button only for admin user
    if (prepareDayBtn) {
      const isAdmin = userData.email === 'klisaricf@gmail.com';
      if (isAdmin) {
        prepareDayBtn.classList.add('show');
      } else {
        prepareDayBtn.classList.remove('show');
      }
    }
  } else {
    // User is not logged in
    if (authButtons) authButtons.style.display = 'flex';
    if (userMenu) userMenu.style.display = 'none';
    if (prepareDayBtn) prepareDayBtn.classList.remove('show');
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

  // 4) Dugmad
  const btn1p = document.getElementById("analyze1p");
  const btnGG = document.getElementById("analyzeGG");
  const btn1pls = document.getElementById("analyze2plus");
  const btnFT2pl = document.getElementById("analyzeFT2plus");
  const btnPrep = document.getElementById("prepareDay");

  if (btn1p) btn1p.addEventListener("click", () => fetchAnalysis("1p"));
  if (btnGG) btnGG.addEventListener("click", () => fetchAnalysis("GG"));
  if (btn1pls) btn1pls.addEventListener("click", () => fetchAnalysis("O15"));
  if (btnFT2pl) btnFT2pl.addEventListener("click", () => fetchAnalysis("FT_O15"));
  if (btnPrep) btnPrep.addEventListener("click", prepareDay);
});

// Safety net: ako je DOM već gotov (npr. skripta učitana kasnije), pozovi ručno
if (document.readyState === "interactive" || document.readyState === "complete") {
  const evt = new Event("DOMContentLoaded");
  document.dispatchEvent(evt);
}

// Log da znamo da je JS podignut
console.log("app.js loaded");
