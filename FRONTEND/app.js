// ====== CONFIG ======
const BACKEND_URL = window.location.origin;
// ako backend nije isti origin/port, otkomentariši sledeće:
// const BACKEND_URL = "http://127.0.0.1:8000";

// ====== SMALL UTILS ======
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const fmt = (v, suffix = "") =>
  v === null || v === undefined || Number.isNaN(v) ? "—" : `${v}${suffix}`;

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
          <button id="analyze1p" class="btn primary">Analyze 1+ 1H</button>
          <button id="analyzeGG" class="btn subtle">Analyze GG 1H</button>
          <button id="analyze2plus" class="btn subtle">Analyze 2+ 1H</button>
          <button id="analyzeFT2plus" class="btn btn-ft">Analyze 2+ FT</button>
          <button id="prepareDay" class="btn">Prepare day</button>
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

  // FAB za mobile – klik radi isto što i #savePdf (ako postoji CSS .fab)
  if (!document.getElementById("savePdfFab")) {
    const fab = document.createElement("button");
    fab.id = "savePdfFab";
    fab.className = "fab";
    fab.title = "Save PDF";
    fab.innerHTML = `<svg width="20" height="20" viewBox="0 0 24 24"><path fill="currentColor" d="M5 22q-.825 0-1.413-.588T3 20V4q0-.825.588-1.413T5 2h8l6 6v12q0 .825-.588 1.413T17 22H5Zm7-13V3H5v17h12V9h-5Z"/></svg>`;
    document.body.appendChild(fab);
  }
}

// ====== UI HELPERS ======
const ANALYZE_BUTTON_IDS = [
  "analyze1p",
  "analyzeGG",
  "analyze2plus",
  "analyzeFT2plus",
  "savePdf",
  "savePdfFab",
  "prepareDay",
];

function setBusyUI(busy, note = "") {
  ANALYZE_BUTTON_IDS.forEach((id) => {
    const el = document.getElementById(id);
    if (el) {
      el.disabled = busy;
      if (busy) {
        el.dataset._origText = el.dataset._origText || el.textContent;
        if (el.id !== "savePdf" && el.id !== "savePdfFab") {
          el.textContent = note || "Analiziram…";
        }
        el.style.opacity = "0.6";
        el.style.cursor = "not-allowed";
      } else {
        if (el.dataset._origText) el.textContent = el.dataset._origText;
        el.style.opacity = "";
        el.style.cursor = "";
      }
    }
  });
  document.body.style.cursor = busy ? "progress" : "";
}

function showLoader() {
  const top5 = document.getElementById("top5");
  const other = document.getElementById("other");
  const loaderHTML =
    '<div class="loader" style="padding:12px;color:var(--muted)">Loading...</div>';
  if (top5) top5.innerHTML = loaderHTML;
  if (other) other.innerHTML = loaderHTML;
  const ct = document.getElementById("countTop");
  const co = document.getElementById("countOther");
  if (ct) ct.textContent = "(0)";
  if (co) co.textContent = "(0)";
}

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
function renderResults(data, market) {
  const currentMarket = market || "1h_over05";
  window.currentAnalysisResults = data;

  const top5Container = document.getElementById("top5");
  const otherContainer = document.getElementById("other");

  const total = Array.isArray(data) ? data.length : 0;

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
        <div style="font-weight:700;margin-bottom:6px">${fmt(m.league)}: ${fmt(m.team1)} vs ${fmt(m.team2)}</div>

        <div style="margin:6px 0">
          <div>${fmt(m.team1)} – Last ${fmt(m.team1_total)}: <strong>${fmt(m.team1_percent, '%')}</strong>${t1Sample}</div>
          <div>${fmt(m.team2)} – Last ${fmt(m.team2_total)}: <strong>${fmt(m.team2_percent, '%')}</strong>${t2Sample}</div>
          <div>H2H: <strong>${fmt(m.h2h_percent, '%')}</strong>${h2hSample}</div>
        </div>

        <div style="margin:6px 0">
          <div><em>1H mikro signali (po timu):</em></div>
          <div>Shots on Target (1H): Home ${shotHome} (used ${fmt(m.home_shots_used)}), Away ${shotAway} (used ${fmt(m.away_shots_used)})</div>
          <div>Dangerous Attacks (1H): Home ${attHome} (used ${fmt(m.home_attacks_used)}), Away ${attAway} (used ${fmt(m.away_attacks_used)})</div>
          <div>Form (sredina dostupnih signala): <strong>${fmt(m.form_percent, '%')}</strong></div>
        </div>

        <div style="margin:6px 0;padding:8px;background:var(--surface-2);border-radius:10px;border:1px solid var(--border)">
          <div style="font-weight:600;margin-bottom:4px">Model breakdown</div>
          <div>Prior (recent form + H2H): <strong>${fmt(d.prior_percent, '%')}</strong></div>
          <div>Micro (league-normalized): <strong>${fmt(d.micro_percent, '%')}</strong>
            <span style="color:var(--muted)">[exp SOT total: ${fmt(d.exp_sot1h_total)}, exp DA total: ${fmt(d.exp_da1h_total)}, pos-edge: ${fmt(d.pos_edge_percent, '%')}]</span>
          </div>
          ${isGG ? `
            <div>Team 1 scores 1H: <strong>${fmt(d.p_home_scores_1h, '%')}</strong>,
                 Team 2: <strong>${fmt(d.p_away_scores_1h, '%')}</strong>
                 <span style="color:var(--muted)">${d.rho!=null?`(ρ=${fmt(d.rho)})`:''}</span>
            </div>` : ``}
            ${isO15 ? `
            <div>${o15Label}: total <strong>${fmt(d.lambda_total)}</strong>
                <span style="color:var(--muted)">(home ${fmt(d.lambda_home)}, away ${fmt(d.lambda_away)})</span>
            </div>` : ``}
          <div>Merged (precision-weighted): <span style="color:var(--text)">micro share ≈ ${fmt(d.merge_weight_micro)}</span></div>
          <div style="color:var(--muted)">effN prior: ${fmt(d.effn_prior)}, effN micro: ${fmt(d.effn_micro)}, ligaški baseline: ${fmt(d.m_league, '%')}</div>
        </div>

        <div style="margin-top:8px">
          <div><strong>Final Probability: ${fmt(m.final_percent, '%')}</strong></div>
          <div style="margin-top:6px; font-size:0.92em; line-height:1.35; color:var(--text);">
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
  const now = new Date();
  const plus4h = new Date(now.getTime() + 4 * 3600 * 1000);
  const pad = (n) => String(n).padStart(2, "0");
  const toLocal = (d) =>
    `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(
      d.getHours()
    )}:${pad(d.getMinutes())}`;
  if (!fromEl.value) fromEl.value = toLocal(now);
  if (!toEl.value) toEl.value = toLocal(plus4h);
}

function localYMD(d) {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

// ====== MAIN ACTION ======
async function fetchAnalysis(type) {
  showLoader();

  const fromEl = document.getElementById("fromDate");
  const toEl = document.getElementById("toDate");

  if (!fromEl || !toEl || !fromEl.value || !toEl.value) {
    alert("Please select both From and To dates.");
    return;
  }

  const fromDate = new Date(fromEl.value);
  const toDate = new Date(toEl.value);
  if (isNaN(fromDate.getTime()) || isNaN(toDate.getTime())) {
    alert("Invalid date values.");
    return;
  }
  if (toDate < fromDate) {
    alert("End date/time must be after start date/time.");
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
    `&market=${encodeURIComponent(market)}&no_api=0`;

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
        alert(`Server vratio nevalidan odgovor (nije JSON):\n${raw.slice(0, 300)}...`);
        break;
      }

      if (res.status === 429) {
        attempt += 1;
        if (attempt > MAX_RETRIES) {
          const msg = json?.detail || "Server je trenutno zauzet. Pokušaj ponovo.";
          alert(msg);
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
        alert(`Greška sa servera: ${msg}`);
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
    alert(`Došlo je do greške pri analizi: ${err}`);
  } finally {
    setBusyUI(false);
  }
}

async function prepareDay() {
  try {
    const fromEl = document.getElementById("fromDate");
    const toEl = document.getElementById("toDate");

    // uzmi dan sa UI-a (from, ili to, ili današnji lokalno)
    let base = new Date();
    if (fromEl && fromEl.value) base = new Date(fromEl.value);
    else if (toEl && toEl.value) base = new Date(toEl.value);
    const dayStr = localYMD(base);

    setBusyUI(true, `Pripremam ${dayStr}…`);
    const res = await fetch(`${BACKEND_URL}/api/prepare-day`, {
      method: "POST",
      headers: { "Content-Type": "application/json", Accept: "application/json" },
      body: JSON.stringify({ date: dayStr, prewarm: true }),
    });

    const data = await res.json();
    if (!res.ok) {
      alert(`Prepare-day error: ${data.detail || data.error || res.status}`);
      showToast("Prepare-day greška", "error");
      return;
    }

    const s = [
      `Dan: ${data.day}`,
      `Fixtures u DB: ${data.fixtures_in_db}`,
      `Timova: ${data.teams} | Parova: ${data.pairs}`,
      `Seeded fixtures: ${data.seeded ? "DA" : "NE"}`,
      `Nedostajalo prije: history=${data.history_missing_before}, h2h=${data.h2h_missing_before}`,
      `Stats missing prije: ${data.stats_missing_before}`,
    ].join("\n");
    alert(`Done.\n\n${s}`);
    showToast("Cache pre-warm završen");
  } catch (err) {
    console.error(err);
    alert(`Prepare-day greška: ${err}`);
    showToast("Prepare-day greška", "error");
  } finally {
    setBusyUI(false);
  }
}

// ====== WIRE EVENTS once DOM is ready ======
document.addEventListener("DOMContentLoaded", () => {
  // 1) Kreiraj moderni shell (radi i sa starim HTML-om)
  ensureModernShell();

  // 2) Tema
  initTheme();

  // 3) Podrazumijevani datumi (ako su prazni)
  setDefaultDatesIfEmpty();

  // 4) Dugmad
  const btn1p = document.getElementById("analyze1p");
  const btnGG = document.getElementById("analyzeGG");
  const btn1pls = document.getElementById("analyze2plus");
  const btnFT2pl = document.getElementById("analyzeFT2plus");
  const btnPDF = document.getElementById("savePdf");
  const btnPDFFab = document.getElementById("savePdfFab");
  const btnPrep = document.getElementById("prepareDay");

  if (btn1p) btn1p.addEventListener("click", () => fetchAnalysis("1p"));
  if (btnGG) btnGG.addEventListener("click", () => fetchAnalysis("GG"));
  if (btn1pls) btn1pls.addEventListener("click", () => fetchAnalysis("O15"));
  if (btnFT2pl) btnFT2pl.addEventListener("click", () => fetchAnalysis("FT_O15"));
  if (btnPrep) btnPrep.addEventListener("click", prepareDay);

  function doSavePdf() {
    if (!window.currentAnalysisResults || !window.currentAnalysisResults.length) {
      showToast("Nema rezultata za PDF", "error");
      return;
    }
    fetch(`${BACKEND_URL}/api/save-pdf`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ matches: window.currentAnalysisResults }),
    })
      .then((r) => r.blob())
      .then((blob) => {
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = "analysis_results.pdf";
        document.body.appendChild(a);
        a.click();
        a.remove();
        showToast("PDF sačuvan");
      })
      .catch((e) => {
        console.error(e);
        showToast("Greška pri PDF exportu", "error");
      });
  }

  if (btnPDF) btnPDF.addEventListener("click", doSavePdf);
  if (btnPDFFab) btnPDFFab.addEventListener("click", doSavePdf);
});