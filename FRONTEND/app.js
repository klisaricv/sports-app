// ====== GLOBAL VARIABLES ======
const BACKEND_URL = window.location.origin;
let globalLoaderActive = false;
let globalLoaderCheckCount = 0;
const MAX_GLOBAL_LOADER_CHECKS = 100;
let loaderCheckInterval = null;

// Pagination state
let currentPage = 1;
const matchesPerPage = 10;
let allMatches = [];

// ====== UTILITY FUNCTIONS ======
function fmt(val, suffix = "") {
  if (val == null || val === undefined || val === "") return "—";
  if (typeof val === "number") {
    if (suffix === "%") return `${val.toFixed(1)}%`;
    return val.toFixed(2);
  }
  return String(val);
}

function localYMD(d) {
  const year = d.getFullYear();
  const month = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function getAnalysisTitle(market) {
  const titles = {
    '1h_over05': '🎯 Over 0.5 Goals - 1st Half',
    'gg1h': '⚽ Both Teams to Score - 1st Half', 
    '1h_over15': '🎯 Over 1.5 Goals - 1st Half',
    'ft_over15': '🎯 Over 1.5 Goals - Full Time'
  };
  return titles[market] || '📊 Analysis Results';
}

// ====== LOADER FUNCTIONS ======
function showLoader(message = "Loading...", progress = 0) {
  hideLoader();
  
  const loader = document.createElement('div');
  loader.id = 'globalLoader';
  loader.className = 'global-loader-overlay';
  loader.innerHTML = `
    <div class="global-loader">
      <div class="loader-spinner">
        <div class="spinner-ring"></div>
        <div class="spinner-ring"></div>
        <div class="spinner-ring"></div>
      </div>
      <div class="loader-message">${message}</div>
    </div>
  `;
  
  document.body.appendChild(loader);
  globalLoaderActive = true;
}

function hideLoader() {
  const loader = document.getElementById('globalLoader');
  if (loader) {
    loader.remove();
  }
  globalLoaderActive = false;
}

function updateLoader(message, progress = 0) {
  const loaderMessage = document.querySelector('#globalLoader .loader-message');
  if (loaderMessage) {
    loaderMessage.textContent = message;
  }
}

// ====== GLOBAL LOADER FUNCTIONS ======
async function checkGlobalLoaderStatus() {
  try {
    globalLoaderCheckCount++;
    
    if (globalLoaderCheckCount > MAX_GLOBAL_LOADER_CHECKS) {
      console.log("🛑 [GLOBAL LOADER] Max checks reached, stopping polling");
      stopGlobalLoaderPolling();
      return;
    }
    
    const response = await fetch('/api/global-loader-status');
    const data = await response.json();
    
    if (data.active && !globalLoaderActive) {
      console.log("🌍 [GLOBAL LOADER] Showing global loader:", data);
      showGlobalLoader(data.detail || "Preparing analysis...", data.progress || 0);
    } else if (!data.active && globalLoaderActive) {
      console.log("🌍 [GLOBAL LOADER] Hiding global loader");
      hideGlobalLoader();
      stopGlobalLoaderPolling();
    } else if (data.active && globalLoaderActive) {
      updateGlobalLoader(data.detail || "Processing...", data.progress || 0);
    }
    
  } catch (error) {
    console.error("❌ [GLOBAL LOADER] Error checking status:", error);
  }
}

function showGlobalLoader(message = "Processing...", progress = 0) {
  hideGlobalLoader();
  
  const loader = document.createElement('div');
  loader.id = 'globalLoader';
  loader.className = 'global-loader-overlay';
  loader.innerHTML = `
    <div class="global-loader">
      <div class="loader-spinner">
        <div class="spinner-ring"></div>
        <div class="spinner-ring"></div>
        <div class="spinner-ring"></div>
      </div>
      <div class="loader-message">${message}</div>
    </div>
  `;
  
  document.body.appendChild(loader);
  globalLoaderActive = true;
}

function hideGlobalLoader() {
  const loader = document.getElementById('globalLoader');
  if (loader) {
    loader.remove();
  }
  globalLoaderActive = false;
  stopGlobalLoaderPolling();
}

function startGlobalLoaderPolling() {
  if (loaderCheckInterval) {
    clearInterval(loaderCheckInterval);
  }
  
  stopGlobalLoaderSSE();
  
  globalLoaderCheckCount = 0;
  
  try {
    startGlobalLoaderSSE();
  } catch (error) {
    console.log("SSE not supported, falling back to polling");
    loaderCheckInterval = setInterval(checkGlobalLoaderStatus, 2000);
  }
  
  console.log("🌍 [GLOBAL LOADER] Started real-time updates");
}

function stopGlobalLoaderPolling() {
  if (loaderCheckInterval) {
    clearInterval(loaderCheckInterval);
    loaderCheckInterval = null;
  }
  
  stopGlobalLoaderSSE();
  
  console.log("🌍 [GLOBAL LOADER] Stopped all updates");
}

// Server-Sent Events for real-time updates
let globalLoaderSSE = null;

function startGlobalLoaderSSE() {
  if (globalLoaderSSE) {
    globalLoaderSSE.close();
  }
  
  globalLoaderSSE = new EventSource('/api/global-loader-events');
  
  globalLoaderSSE.onmessage = function(event) {
    try {
      const data = JSON.parse(event.data);
      console.log("🌍 [SSE] Received global loader update:", data);
      
      if (data.active && !globalLoaderActive) {
        showGlobalLoader(data.detail || "Preparing analysis...", data.progress || 0);
      } else if (!data.active && globalLoaderActive) {
        hideGlobalLoader();
        stopGlobalLoaderSSE();
      } else if (data.active && globalLoaderActive) {
        updateGlobalLoader(data.detail || "Processing...", data.progress || 0);
      }
    } catch (error) {
      console.error("Error parsing SSE data:", error);
    }
  };
  
  globalLoaderSSE.onerror = function(error) {
    console.error("SSE connection error:", error);
    startGlobalLoaderPolling();
  };
  
  console.log("🌍 [SSE] Started Server-Sent Events connection");
}

function stopGlobalLoaderSSE() {
  if (globalLoaderSSE) {
    globalLoaderSSE.close();
    globalLoaderSSE = null;
    console.log("🌍 [SSE] Stopped Server-Sent Events connection");
  }
}

// ====== RESULTS RENDERING ======
function renderResults(data, market) {
  const currentMarket = market || "1h_over05";
  window.currentAnalysisResults = data;
  allMatches = Array.isArray(data) ? data : [];

  const top5Container = document.getElementById("top5");
  const otherContainer = document.getElementById("other");
  const paginationContainer = document.getElementById("pagination");

  const total = allMatches.length;

  // Define cardHTML function FIRST
  const cardHTML = (m) => {
    const shotHome = m.home_shots_used > 0 ? fmt(m.home_shots_percent, "%") : "—";
    const shotAway = m.away_shots_used > 0 ? fmt(m.away_shots_percent, "%") : "—";
    const attHome = m.home_attacks_used > 0 ? fmt(m.home_attacks_percent, "%") : "—";
    const attAway = m.away_attacks_used > 0 ? fmt(m.away_attacks_percent, "%") : "—";
    const d = m.debug || {};
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
              <span class="breakdown-label">Merged (Precision-weighted)</span>
              <span class="breakdown-value">${fmt(d.merge_weight_micro, '%')}</span>
            </div>
            ${isO15 ? `
            <div class="breakdown-item">
              <span class="breakdown-label">${o15Label}</span>
              <span class="breakdown-value">${fmt(d.poisson_lambda, '')}</span>
            </div>
            ` : ''}
          </div>
        </div>

        <div class="final-probability">
          <div class="probability-label">Final Probability</div>
          <div class="probability-value">${fmt(m.final_probability, '%')}</div>
        </div>

        <div class="match-narrative">
          <div class="narrative-text">
            ${fmt(m.team1)} vs ${fmt(m.team2)}: ${fmt(d.narrative, '')}
          </div>
        </div>
      </div>
    `;
  };

  // Add analysis title
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

  // Reset pagination
  currentPage = 1;

  // Render TOP 5 (always first 5)
  const top5Matches = allMatches.slice(0, 5);
  const countTop = document.getElementById("countTop");
  if (countTop) countTop.textContent = `(${top5Matches.length})`;
  
  if (top5Container) {
    if (top5Matches.length > 0) {
      top5Container.innerHTML = top5Matches.map(cardHTML).join('');
    } else {
      top5Container.innerHTML = `<div class="placeholder">No top matches found.</div>`;
    }
  }

  // Render Other section (remaining matches with pagination)
  const remainingMatches = allMatches.slice(5);
  const countOther = document.getElementById("countOther");
  if (countOther) countOther.textContent = `(${remainingMatches.length})`;

  if (remainingMatches.length > 0) {
    // Show pagination
    if (paginationContainer) {
      paginationContainer.style.display = 'flex';
      renderPagination(remainingMatches.length);
    }
    
    // Render first page of remaining matches
    renderOtherMatches(remainingMatches, 1);
  } else {
    // Hide pagination and show empty state
    if (paginationContainer) {
      paginationContainer.style.display = 'none';
    }
    if (otherContainer) {
      otherContainer.innerHTML = `<div class="placeholder">No additional matches found.</div>`;
    }
  }
}

// ====== PAGINATION FUNCTIONS ======
function renderPagination(totalMatches) {
  const totalPages = Math.ceil(totalMatches / matchesPerPage);
  const paginationInfo = document.getElementById('paginationInfo');
  const paginationPages = document.getElementById('paginationPages');
  const prevBtn = document.getElementById('prevPage');
  const nextBtn = document.getElementById('nextPage');

  // Update pagination info
  const startIndex = (currentPage - 1) * matchesPerPage + 1;
  const endIndex = Math.min(currentPage * matchesPerPage, totalMatches);
  if (paginationInfo) {
    paginationInfo.textContent = `Showing ${startIndex}-${endIndex} of ${totalMatches} matches`;
  }

  // Update prev/next buttons
  if (prevBtn) {
    prevBtn.disabled = currentPage === 1;
  }
  if (nextBtn) {
    nextBtn.disabled = currentPage === totalPages;
  }

  // Render page numbers
  if (paginationPages) {
    paginationPages.innerHTML = '';
    
    // Show max 5 page numbers
    const maxVisiblePages = 5;
    let startPage = Math.max(1, currentPage - Math.floor(maxVisiblePages / 2));
    let endPage = Math.min(totalPages, startPage + maxVisiblePages - 1);
    
    // Adjust start if we're near the end
    if (endPage - startPage + 1 < maxVisiblePages) {
      startPage = Math.max(1, endPage - maxVisiblePages + 1);
    }

    for (let i = startPage; i <= endPage; i++) {
      const pageBtn = document.createElement('button');
      pageBtn.className = `page-btn ${i === currentPage ? 'active' : ''}`;
      pageBtn.textContent = i;
      pageBtn.onclick = () => goToPage(i);
      paginationPages.appendChild(pageBtn);
    }
  }
}

function renderOtherMatches(matches, page) {
  const otherContainer = document.getElementById("other");
  if (!otherContainer) return;

  const startIndex = (page - 1) * matchesPerPage;
  const endIndex = startIndex + matchesPerPage;
  const pageMatches = matches.slice(startIndex, endIndex);

  if (pageMatches.length > 0) {
    // Define cardHTML function for this scope
    const cardHTML = (m) => {
      const shotHome = m.home_shots_used > 0 ? fmt(m.home_shots_percent, "%") : "—";
      const shotAway = m.away_shots_used > 0 ? fmt(m.away_shots_percent, "%") : "—";
      const attHome = m.home_attacks_used > 0 ? fmt(m.home_attacks_percent, "%") : "—";
      const attAway = m.away_attacks_used > 0 ? fmt(m.away_attacks_percent, "%") : "—";
      const d = m.debug || {};
      const currentMarket = window.currentAnalysisResults ? 
        (window.currentAnalysisResults[0]?.market || "1h_over05") : "1h_over05";
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
                <span class="breakdown-label">Merged (Precision-weighted)</span>
                <span class="breakdown-value">${fmt(d.merge_weight_micro, '%')}</span>
              </div>
              ${isO15 ? `
              <div class="breakdown-item">
                <span class="breakdown-label">${o15Label}</span>
                <span class="breakdown-value">${fmt(d.poisson_lambda, '')}</span>
              </div>
              ` : ''}
            </div>
          </div>

          <div class="final-probability">
            <div class="probability-label">Final Probability</div>
            <div class="probability-value">${fmt(m.final_probability, '%')}</div>
          </div>

          <div class="match-narrative">
            <div class="narrative-text">
              ${fmt(m.team1)} vs ${fmt(m.team2)}: ${fmt(d.narrative, '')}
            </div>
          </div>
        </div>
      `;
    };
    
    otherContainer.innerHTML = pageMatches.map(cardHTML).join('');
  } else {
    otherContainer.innerHTML = `<div class="placeholder">No matches found for this page.</div>`;
  }
}

function goToPage(page) {
  if (page === currentPage) return;
  
  currentPage = page;
  const remainingMatches = allMatches.slice(5);
  
  renderOtherMatches(remainingMatches, page);
  renderPagination(remainingMatches.length);
  
  // Scroll to other section
  const otherSection = document.querySelector('.other-section');
  if (otherSection) {
    otherSection.scrollIntoView({ behavior: 'smooth', block: 'start' });
  }
}

function nextPage() {
  const remainingMatches = allMatches.slice(5);
  const totalPages = Math.ceil(remainingMatches.length / matchesPerPage);
  if (currentPage < totalPages) {
    goToPage(currentPage + 1);
  }
}

function prevPage() {
  if (currentPage > 1) {
    goToPage(currentPage - 1);
  }
}

// ====== ANALYSIS FUNCTIONS ======
async function parseJsonSafe(resp) {
  const ct = resp.headers.get("content-type") || "";
  if (ct.includes("application/json")) return await resp.json();
  return { ok: false, error: "Invalid response format" };
}

function normalizeResults(json) {
  if (Array.isArray(json)) return json;
  if (json == null) return [];
  if (Array.isArray(json.results)) return json.results;
  if (Array.isArray(json.data)) return json.data;
  if (Array.isArray(json.matches)) return json.matches;
  return [];
}

async function fetchAnalysis(market) {
  try {
    // Check if prepare day is running
    const prepareCheck = await fetch('/api/global-loader-status');
    const prepareData = await prepareCheck.json();
    
    if (prepareData.active) {
      showToast("Please wait, preparing day is in progress...", "info");
      return;
    }

    const fromEl = document.getElementById("fromDate");
    const toEl = document.getElementById("toDate");
    const fromHour = document.getElementById("fromHour");
    const toHour = document.getElementById("toHour");

    let fromDate, toDate;
    if (fromEl && fromEl.value) {
      fromDate = new Date(fromEl.value);
    } else if (toEl && toEl.value) {
      toDate = new Date(toEl.value);
      fromDate = new Date(toDate.getTime() - 24 * 60 * 60 * 1000);
    } else {
      fromDate = new Date();
      toDate = new Date();
    }

    if (!toDate) toDate = new Date(fromDate.getTime() + 24 * 60 * 60 * 1000);

    const params = new URLSearchParams({
      from_date: fromDate.toISOString(),
      to_date: toDate.toISOString(),
      from_hour: fromHour?.value || 8,
      to_hour: toHour?.value || 22,
      market: market,
      no_api: 1
    });

    console.log("calling:", `${BACKEND_URL}/api/analyze?${params}`);
    
    const response = await fetch(`${BACKEND_URL}/api/analyze?${params}`);
    const data = await parseJsonSafe(response);
    
    console.log("Raw JSON:", data);
    
    const results = normalizeResults(data);
    console.log("✔ Normalized results length:", results.length);
    
    renderResults(results, market);
    
  } catch (error) {
    console.error("Fetch/parse error:", error);
    showToast(`Error during analysis: ${error.message}`, "error");
  }
}

async function prepareDay() {
  try {
    const fromEl = document.getElementById("fromDate");
    const toEl = document.getElementById("toDate");
    let base = new Date();
    if (fromEl && fromEl.value) base = new Date(fromEl.value);
    else if (toEl && toEl.value) base = new Date(toEl.value);
    const dayStr = localYMD(base);

    showLoader(`🚀 Preparing ${dayStr}...`);

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
    
    if (resp.status === 403) {
      throw new Error("Access denied. Admin privileges required.");
    }
    if (resp.status === 401) {
      throw new Error("Authentication required. Please log in.");
    }
    
    if (!data.ok || !data.job_id) throw new Error("Neuspešno pokretanje prepare posla");

    const jobId = data.job_id;
    updateLoader("queued");

    console.log("🌍 [GLOBAL LOADER] Starting global loader for prepare day");
    startGlobalLoaderPolling();

    let lastProgress = -1;
    const startTime = Date.now();
    const MAX_PREPARE_TIME = 300000; // 5 minuta timeout
    
    while (true) {
      if (Date.now() - startTime > MAX_PREPARE_TIME) {
        throw new Error("Prepare day timeout after 5 minutes");
      }
      
      const statusResp = await fetch(`/api/prepare-day-status/${jobId}`);
      const statusData = await parseJsonSafe(statusResp);
      
      if (statusData.status === "completed") {
        updateLoader("Completed!");
        setTimeout(() => hideLoader(), 1000);
        showToast("Prepare day completed successfully!", "success");
        break;
      } else if (statusData.status === "failed") {
        throw new Error(statusData.error || "Prepare day failed");
      } else if (statusData.progress && statusData.progress !== lastProgress) {
        lastProgress = statusData.progress;
        updateLoader(`Processing... ${statusData.progress}%`, statusData.progress);
      }
      
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
    
  } catch (error) {
    console.error("Prepare day error:", error);
    hideLoader();
    showToast(`Prepare day failed: ${error.message}`, "error");
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
    localStorage.removeItem('user');
    checkAuthStatus();
    showToast('Logged out successfully!', 'success');
  }
}

// ====== TOAST FUNCTIONS ======
function showToast(message, type = 'info') {
  const toast = document.createElement('div');
  toast.className = `toast toast-${type}`;
  toast.textContent = message;
  
  document.body.appendChild(toast);
  
  setTimeout(() => {
    toast.classList.add('show');
  }, 100);
  
  setTimeout(() => {
    toast.classList.remove('show');
    setTimeout(() => toast.remove(), 300);
  }, 3000);
}

// ====== THEME FUNCTIONS ======
function initTheme() {
  const root = document.documentElement;
  const btn = document.querySelector('[data-theme-toggle]');
  
  if (btn) {
    btn.addEventListener("click", () => {
      const cur = root.getAttribute("data-theme") || "auto";
      const next = cur === "light" ? "dark" : cur === "dark" ? "auto" : "light";
      root.setAttribute("data-theme", next);
      localStorage.setItem("theme", next);
    });
  }
  
  const savedTheme = localStorage.getItem("theme") || "auto";
  root.setAttribute("data-theme", savedTheme);
}

function setDefaultDatesIfEmpty() {
  const fromEl = document.getElementById("fromDate");
  const toEl = document.getElementById("toDate");
  if (!fromEl || !toEl) return;
  
  const today = new Date();
  const pad = (n) => String(n).padStart(2, "0");
  
  if (!fromEl.value) {
    const fromDate = new Date(today);
    fromDate.setHours(8, 0, 0, 0);
    fromEl.value = fromDate.toISOString().slice(0, 16);
  }
  
  if (!toEl.value) {
    const toDate = new Date(today);
    toDate.setHours(22, 0, 0, 0);
    toEl.value = toDate.toISOString().slice(0, 16);
  }
}

// ====== INITIALIZATION ======
document.addEventListener("DOMContentLoaded", () => {
  // 1) Create modern shell
  ensureModernShell();

  // 2) Theme
  initTheme();

  // 3) Authentication
  checkAuthStatus();
  
  // 4) Initialize logout button
  const logoutBtn = document.getElementById('logoutBtn');
  if (logoutBtn) {
    logoutBtn.addEventListener('click', logout);
  }

  // 5) Default dates
  setDefaultDatesIfEmpty();

  // 6) Button event listeners
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

  // Pagination event listeners
  const prevPageBtn = document.getElementById('prevPage');
  const nextPageBtn = document.getElementById('nextPage');
  
  if (prevPageBtn) prevPageBtn.addEventListener('click', prevPage);
  if (nextPageBtn) nextPageBtn.addEventListener('click', nextPage);

  // PDF save functions
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
    .then(resp => resp.blob())
    .then(blob => {
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `analysis-${new Date().toISOString().slice(0,10)}.pdf`;
      a.click();
      URL.revokeObjectURL(url);
      showToast("PDF saved successfully!", "success");
    })
    .catch(err => {
      console.error("PDF save error:", err);
      showToast("Failed to save PDF", "error");
    });
  }

  const btnPDF = document.getElementById("savePdf");
  const btnPDFFab = document.getElementById("savePdfFab");
  if (btnPDF) btnPDF.addEventListener("click", doSavePdf);
  if (btnPDFFab) btnPDFFab.addEventListener("click", doSavePdf);
});

function ensureModernShell() {
  // This function ensures the modern shell is created
  // Implementation depends on your specific needs
}
