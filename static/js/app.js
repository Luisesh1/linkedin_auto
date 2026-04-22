/**
 * LinkedIn Auto-Poster — Frontend Logic
 */

// ── State ──────────────────────────────────────────────────────────────────

let currentSessionId = null;
let currentTestMode = false;
let currentCategory = "";
let currentResolvedCategory = "";
let eventSource = null;
let loginPollInterval = null;
let publishPollInterval = null;
let pipelineStatePollInterval = null;
let categoryStore = [];
let currentCategoryId = null;
let historyPage = 1;
let historyTotalPages = 1;
let historySearch = "";
let lastPipelineStateSignature = "";
let analyticsPeriod = "30d";
const analyticsCharts = {};

const PIPELINE_SESSION_STORAGE_KEY = "autolinkedin.pipeline.session";
const APP_BOOTSTRAP = window.APP_BOOTSTRAP || {};


function redirectToLogin() {
  const loginUrl = APP_BOOTSTRAP.loginUrl || "/login";
  const next = encodeURIComponent(window.location.pathname + window.location.search);
  window.location.assign(`${loginUrl}?next=${next}`);
}


async function apiFetch(url, options = {}) {
  const opts = { credentials: "same-origin", ...options };
  const method = String(opts.method || "GET").toUpperCase();
  const headers = new Headers(opts.headers || {});
  if (!["GET", "HEAD", "OPTIONS"].includes(method)) {
    const csrfToken = APP_BOOTSTRAP.csrfToken || "";
    if (csrfToken) headers.set("X-CSRF-Token", csrfToken);
  }
  opts.headers = headers;

  const response = await fetch(url, opts);
  if (response.status === 401) {
    redirectToLogin();
    throw new Error("AUTH_REQUIRED");
  }
  return response;
}

// ── Init ───────────────────────────────────────────────────────────────────

document.addEventListener("DOMContentLoaded", async () => {
  if (document.getElementById("auth-status-text")) checkAuthStatus();
  if (document.getElementById("history-container")) loadHistory();
  if (document.getElementById("analytics-card")) loadAnalytics();
  if (document.getElementById("headless-toggle")) loadHeadlessSetting();
  if (document.getElementById("schedule-card")) loadSchedule();
  if (document.getElementById("pipeline-category") || document.getElementById("category-list")) await loadCategories();

  const pipelineCategorySelect = document.getElementById("pipeline-category");
  if (pipelineCategorySelect) {
    pipelineCategorySelect.addEventListener("change", () => {
      currentCategory = getSelectedCategoryName();
      currentResolvedCategory = currentCategory === "random" ? "" : currentCategory;
      renderSelectedCategorySummary(currentCategory);
      renderResolvedCategoryBanner(currentCategory, currentResolvedCategory);
      persistPipelineSessionState();
    });
  }

  const textarea = document.getElementById("post-textarea");
  if (textarea) textarea.addEventListener("input", updateCharCount);

  const historySearchInput = document.getElementById("history-search");
  if (historySearchInput) {
    historySearchInput.addEventListener("keydown", (event) => {
      if (event.key === "Enter") {
        event.preventDefault();
        historyPage = 1;
        historySearch = historySearchInput.value.trim();
        loadHistory();
      }
    });
  }

  if (document.body.dataset.page === "automation") await restorePipelineState();
});

// ── Headless toggle ─────────────────────────────────────────────────────────

async function loadHeadlessSetting() {
  try {
    const res = await apiFetch("/api/headless");
    const data = await res.json();
    const toggle = document.getElementById("headless-toggle");
    if (!toggle) return;
    toggle.checked = data.headless;
    updateHeadlessLabel(data.headless);
  } catch { /* ignore */ }
}

async function setHeadlessMode(headless) {
  updateHeadlessLabel(headless);
  try {
    await apiFetch("/api/headless", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ headless }),
    });
  } catch { /* ignore */ }
}

function updateHeadlessLabel(headless) {
  const label = document.getElementById("headless-label");
  if (!label) return;
  label.textContent = headless ? "Headless" : "GUI";
  label.style.color = headless ? "#adb5bd" : "#ffc107";
}

// ── Auth ───────────────────────────────────────────────────────────────────

async function checkAuthStatus() {
  try {
    const res = await apiFetch("/auth/status");
    const data = await res.json();
    updateAuthBanner(data);
  } catch {
    const status = document.getElementById("auth-status-text");
    if (status) status.textContent = "Error de conexión";
  }
}

function updateAuthBanner(data) {
  const statusEl = document.getElementById("auth-status-text");
  const loginBtn = document.getElementById("login-btn");
  const disconnectBtn = document.getElementById("disconnect-btn");
  if (!statusEl || !loginBtn || !disconnectBtn) return;

  if (data.login_in_progress) {
    statusEl.className = "badge fs-6 px-3 py-2 bg-warning text-dark";
    statusEl.innerHTML = '<i class="bi bi-browser-chrome me-1"></i> Conectando LinkedIn...';
    loginBtn.classList.add("d-none");
    disconnectBtn.classList.add("d-none");
    return;
  }

  if (data.authenticated) {
    const days = data.days_left;
    if (data.needs_reconnect) {
      statusEl.className = "badge fs-6 px-3 py-2 bg-warning text-dark";
      statusEl.innerHTML = `<i class="bi bi-exclamation-triangle-fill me-1"></i> Sesión expira en ${days}d`;
      loginBtn.classList.remove("d-none");
      loginBtn.innerHTML = '<i class="bi bi-arrow-repeat me-1"></i> Renovar sesión';
    } else {
      statusEl.className = "badge fs-6 px-3 py-2 bg-success";
      statusEl.innerHTML = `<i class="bi bi-check-circle-fill me-1"></i> LinkedIn conectado · ${days}d`;
      loginBtn.classList.add("d-none");
    }
    disconnectBtn.classList.remove("d-none");
  } else {
    statusEl.className = "badge fs-6 px-3 py-2 bg-danger";
    statusEl.innerHTML = `<i class="bi bi-x-circle-fill me-1"></i> No conectado`;
    loginBtn.classList.remove("d-none");
    loginBtn.innerHTML = '<i class="bi bi-box-arrow-in-right me-1"></i> Iniciar sesión';
    disconnectBtn.classList.add("d-none");
  }
}

async function startLogin() {
  const loginCard = document.getElementById("login-card");
  const loginMsg = document.getElementById("login-message");
  const loginResult = document.getElementById("login-result");
  const loginProgress = document.getElementById("login-progress");
  const loginBtn = document.getElementById("login-btn");
  if (!loginCard || !loginMsg || !loginResult || !loginProgress || !loginBtn) return;

  loginCard.classList.remove("d-none");
  loginResult.classList.add("d-none");
  loginProgress.classList.remove("d-none");
  loginMsg.textContent = "Iniciando...";

  loginBtn.disabled = true;

  let res, data;
  try {
    res = await apiFetch("/auth/login", { method: "POST" });
    data = await res.json();
  } catch (e) {
    showLoginResult(false, "Error de red: " + e.message);
    return;
  }

  if (data.error) {
    showLoginResult(false, data.error);
    return;
  }

  const jobId = data.job_id;
  loginPollInterval = setInterval(async () => {
    try {
      const statusRes = await fetch(`/auth/login_status/${jobId}`);
      if (statusRes.status === 401) {
        redirectToLogin();
        return;
      }
      const status = await statusRes.json();
      loginMsg.textContent = status.message;

      if (status.status === "done") {
        clearInterval(loginPollInterval);
        showLoginResult(true, status.message);
        checkAuthStatus();
      } else if (status.status === "error") {
        clearInterval(loginPollInterval);
        showLoginResult(false, status.message);
      }
    } catch {
      // network blip, keep polling
    }
  }, 2000);
}

function showLoginResult(success, message) {
  const loginResult = document.getElementById("login-result");
  const loginProgress = document.getElementById("login-progress");
  const loginBtn = document.getElementById("login-btn");
  if (!loginResult || !loginProgress || !loginBtn) return;

  loginProgress.classList.add("d-none");
  loginResult.classList.remove("d-none");
  loginResult.className = `alert ${success ? "alert-success" : "alert-danger"} mt-2`;
  loginResult.innerHTML = `<i class="bi bi-${success ? "check-circle-fill" : "x-circle-fill"} me-2"></i>${escapeHtml(message)}`;
  loginBtn.disabled = false;

  if (success) {
    setTimeout(() => {
      const loginCard = document.getElementById("login-card");
      if (loginCard) loginCard.classList.add("d-none");
    }, 3000);
  }
}

async function disconnectLinkedIn() {
  if (!confirm("¿Cerrar sesión de LinkedIn? Tendrás que iniciar sesión nuevamente para publicar.")) return;
  await apiFetch("/auth/disconnect", { method: "POST" });
  checkAuthStatus();
}


async function logoutApp() {
  try {
    await apiFetch(APP_BOOTSTRAP.logoutUrl || "/logout", { method: "POST" });
  } catch {
    // ignore and redirect locally
  }
  window.location.assign(APP_BOOTSTRAP.loginUrl || "/login");
}

// ── Pipeline ───────────────────────────────────────────────────────────────

function startPipeline() {
  apiFetch("/auth/status").then(r => r.json()).then(data => {
    if (!data.authenticated) {
      alert("Primero inicia sesión en LinkedIn.");
      return;
    }
    runPipeline(false);
  });
}

function startTestPipeline() {
  runPipeline(true);
}

function runPipeline(testMode = false) {
  currentTestMode = testMode;
  currentCategory = getSelectedCategoryName();
  currentResolvedCategory = currentCategory === "random" ? "" : currentCategory;
  stopPipelineStatePolling();
  resetUI();
  const stepsPanel = document.getElementById("steps-panel");
  if (stepsPanel) stepsPanel.classList.remove("d-none");
  renderResolvedCategoryBanner(currentCategory, currentResolvedCategory);

  setPipelineButtonsBusy(testMode);

  _openPipelineSSE(1);
}

function rerunFromStep(step) {
  if (!currentSessionId) return;

  // Hide preview, reset steps from this point forward
  const previewPanel = document.getElementById("preview-panel");
  if (previewPanel) previewPanel.classList.add("d-none");
  const publishSuccess = document.getElementById("publish-success");
  if (publishSuccess) publishSuccess.classList.add("d-none");
  const publishError = document.getElementById("publish-error");
  if (publishError) publishError.classList.add("d-none");
  const publishProgress = document.getElementById("publish-progress");
  if (publishProgress) publishProgress.classList.add("d-none");
  for (let i = step; i <= 6; i++) {
    const icon = document.getElementById(`icon-${i}`);
    if (icon) icon.innerHTML = '<i class="bi bi-circle text-muted"></i>';
    const d = document.getElementById(`detail-${i}`);
    if (d) {
      d.textContent = "";
      d.className = "step-detail text-muted small";
    }
    const rb = document.getElementById(`regen-${i}`);
    if (rb) rb.classList.add("d-none");
  }

  _openPipelineSSE(step, currentSessionId);
}

function _openPipelineSSE(fromStep = 1, sessionId = null) {
  if (eventSource) eventSource.close();
  const params = new URLSearchParams();
  if (currentTestMode) params.set("test", "1");
  if (currentCategory) params.set("category", currentCategory);
  if (fromStep > 1) params.set("from_step", fromStep);
  if (sessionId) params.set("session_id", sessionId);
  const url = `/api/run${params.toString() ? "?" + params.toString() : ""}`;
  eventSource = new EventSource(url);

  eventSource.onmessage = (event) => {
    const data = JSON.parse(event.data);
    handleSSEEvent(data);
  };

  eventSource.onerror = () => {
    if (currentSessionId === null) {
      showGlobalError("Conexión interrumpida. Intenta de nuevo.");
      resetAllBtns();
    }
    eventSource.close();
  };
}

function handleSSEEvent(data) {
  // Init event: capture session_id early so regen buttons work
  if (data.type === "init") {
    currentSessionId = data.session_id;
    currentCategory = data.requested_category || currentCategory || getSelectedCategoryName();
    currentResolvedCategory = data.resolved_category || currentResolvedCategory || "";
    renderResolvedCategoryBanner(currentCategory, currentResolvedCategory);
    persistPipelineSessionState();
    return;
  }

  const { step, status } = data;

  // Global pipeline error (e.g. no session)
  if (step === 0 && status === "error") {
    showGlobalError(data.message);
    resetAllBtns();
    persistPipelineSessionState();
    eventSource.close();
    return;
  }

  if (status === "running") {
    setStepRunning(step, data.message);
  } else if (status === "done") {
    setStepDone(step, data.result);
    if (step >= 1 && step <= 5) {
      const rb = document.getElementById(`regen-${step}`);
      if (rb) rb.classList.remove("d-none");
    }
  } else if (status === "error") {
    setStepError(step, data.message);
    resetAllBtns();
    persistPipelineSessionState();
    eventSource.close();
  } else if (status === "preview") {
    if (!data.test_mode) currentSessionId = data.session_id;
    setStepDone(6, data.test_mode ? "Vista previa generada (modo prueba)" : "Listo para publicar");
    showPreviewPanel(data);
    resetAllBtns();
    const regenerateBtn = document.getElementById("regenerate-btn");
    if (regenerateBtn) regenerateBtn.classList.remove("d-none");
    persistPipelineSessionState();
    eventSource.close();
  }
}

// ── Step UI helpers ────────────────────────────────────────────────────────

function setStepRunning(step, message) {
  const icon = document.getElementById(`icon-${step}`);
  if (icon) icon.innerHTML = '<span class="spinner-border spinner-border-sm text-primary"></span>';
  const detail = document.getElementById(`detail-${step}`);
  if (!detail) return;
  detail.textContent = message;
  detail.className = "step-detail text-primary small";
}

function setStepDone(step, result) {
  const icon = document.getElementById(`icon-${step}`);
  if (icon) icon.innerHTML = '<i class="bi bi-check-circle-fill text-success fs-5"></i>';
  const detail = document.getElementById(`detail-${step}`);
  if (!detail) return;
  let summary = "";
  if (Array.isArray(result)) {
    summary = `${result.length} temas: ${result.slice(0, 3).join(", ")}${result.length > 3 ? "..." : ""}`;
  } else if (typeof result === "object" && result !== null) {
    if (result.topic) summary = `Tema: ${result.topic}`;
    else if (result.selected_family) {
      const family = String(result.selected_family).replaceAll("_", " ");
      const score = typeof result.score !== "undefined" ? ` · score ${Number(result.score).toFixed(1)}` : "";
      summary = `Imagen elegida: ${family}${score}`;
    }
    else if (result.image_url) summary = "Imagen generada correctamente";
    else if (typeof result.score !== "undefined") summary = `Score: ${result.score}`;
    else summary = Object.values(result).join(" · ").substring(0, 80);
  } else {
    summary = String(result || "Completado");
  }
  detail.textContent = summary;
  detail.className = "step-detail text-success small";
}

function setStepError(step, message) {
  const icon = document.getElementById(`icon-${step}`);
  if (icon) icon.innerHTML = '<i class="bi bi-x-circle-fill text-danger fs-5"></i>';
  const detail = document.getElementById(`detail-${step}`);
  if (!detail) return;
  detail.textContent = message;
  detail.className = "step-detail text-danger small";
}

// ── Preview panel ──────────────────────────────────────────────────────────

function showPreviewPanel(data) {
  const panel = document.getElementById("preview-panel");
  if (!panel) return;
  panel.classList.remove("d-none");
  panel.scrollIntoView({ behavior: "smooth", block: "start" });

  const previewImage = document.getElementById("preview-image");
  if (previewImage) previewImage.src = data.image_url;
  const postTextarea = document.getElementById("post-textarea");
  if (postTextarea) postTextarea.value = data.post_text;
  const topicBadge = document.getElementById("preview-topic-badge");
  if (topicBadge) topicBadge.textContent = data.topic;
  const resolvedCategory = data.resolved_category || data.category || currentResolvedCategory || "";
  if (resolvedCategory) {
    if (topicBadge) topicBadge.textContent = `${resolvedCategory} · ${data.topic}`;
  }

  if (data.reasoning) {
    const reasoningText = document.getElementById("reasoning-text");
    if (reasoningText) reasoningText.textContent = data.reasoning;
    const reasoningBox = document.getElementById("reasoning-box");
    if (reasoningBox) reasoningBox.classList.remove("d-none");
  }

  // Test mode: show warning badge, hide publish button
  const testBadge = document.getElementById("test-mode-badge");
  const publishBtn = document.getElementById("publish-btn");
  if (data.test_mode) {
    if (testBadge) testBadge.classList.remove("d-none");
    if (publishBtn) publishBtn.classList.add("d-none");
  } else {
    if (testBadge) testBadge.classList.add("d-none");
    if (publishBtn) publishBtn.classList.remove("d-none");
  }

  updateCharCount();
}

function updateCharCount() {
  const textarea = document.getElementById("post-textarea");
  const count = document.getElementById("char-count");
  if (!textarea || !count) return;
  const words = textarea.value.trim().split(/\s+/).filter(w => w).length;
  count.textContent = `${words} palabras · ${textarea.value.length} caracteres`;
}

// ── Publish ────────────────────────────────────────────────────────────────

async function publishPost() {
  const postText = document.getElementById("post-textarea").value.trim();
  if (!postText) { alert("El texto del post está vacío."); return; }

  const publishBtn = document.getElementById("publish-btn");
  publishBtn.disabled = true;
  publishBtn.innerHTML = '<span class="spinner-border spinner-border-sm me-2"></span>Iniciando...';

  document.getElementById("publish-success").classList.add("d-none");
  document.getElementById("publish-error").classList.add("d-none");
  document.getElementById("publish-progress").classList.remove("d-none");
  document.getElementById("publish-progress-msg").textContent = "Abriendo navegador...";

  let res, data;
  try {
    res = await apiFetch("/api/publish", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ session_id: currentSessionId, post_text_override: postText }),
    });
    data = await res.json();
  } catch (e) {
    showPublishError("Error de red: " + e.message);
    publishBtn.disabled = false;
    publishBtn.innerHTML = '<i class="bi bi-linkedin me-2"></i>Reintentar';
    return;
  }

  if (data.error) {
    showPublishError(data.error);
    publishBtn.disabled = false;
    publishBtn.innerHTML = '<i class="bi bi-linkedin me-2"></i>Reintentar';
    return;
  }

  // Poll publish job
  const jobId = data.job_id;
  publishPollInterval = setInterval(async () => {
    try {
      const statusRes = await apiFetch(`/api/publish_status/${jobId}`);
      const status = await statusRes.json();
      document.getElementById("publish-progress-msg").textContent = status.message;

      // Update screenshots as they arrive
      if (status.screenshots && status.screenshots.length) {
        renderScreenshots(status.screenshots);
      }

      if (status.status === "done") {
        clearInterval(publishPollInterval);
        document.getElementById("publish-progress").classList.add("d-none");
        document.getElementById("publish-success").classList.remove("d-none");
        publishBtn.classList.add("d-none");
        currentSessionId = null;
        loadHistory();
      } else if (status.status === "error") {
        clearInterval(publishPollInterval);
        document.getElementById("publish-progress").classList.add("d-none");
        showPublishError(status.message);
        publishBtn.disabled = false;
        publishBtn.innerHTML = '<i class="bi bi-linkedin me-2"></i>Reintentar';
      }
    } catch {
      // keep polling
    }
  }, 2000);
}

function showPublishError(msg) {
  document.getElementById("publish-error-text").textContent = msg;
  document.getElementById("publish-error").classList.remove("d-none");
  document.getElementById("publish-progress").classList.add("d-none");
}

function renderScreenshots(urls) {
  const section = document.getElementById("screenshots-section");
  const grid = document.getElementById("screenshots-grid");
  const count = document.getElementById("screenshots-count");
  const body = document.getElementById("screenshots-body");

  section.classList.remove("d-none");
  count.textContent = urls.length;

  // Only add new screenshots
  const existing = grid.querySelectorAll("img").length;
  urls.slice(existing).forEach((url, i) => {
    const idx = existing + i + 1;
    const wrapper = document.createElement("div");
    wrapper.style.cssText = "flex: 0 0 auto; width: 220px;";
    wrapper.innerHTML = `
      <a href="${url}" target="_blank">
        <img src="${url}?t=${Date.now()}" alt="Paso ${idx}"
             style="width:100%; border-radius:6px; border:1px solid #444; cursor:zoom-in;"
             title="Paso ${idx}">
      </a>
      <div style="font-size:11px; color:#aaa; text-align:center; margin-top:3px;">Paso ${idx}</div>`;
    grid.appendChild(wrapper);
  });

  // Auto-expand accordion when screenshots arrive
  if (urls.length > 0 && !body.classList.contains("show")) {
    body.classList.add("show");
    body.previousElementSibling.querySelector("button").classList.remove("collapsed");
  }
}

// ── History ────────────────────────────────────────────────────────────────

function setAnalyticsPeriod(period) {
  analyticsPeriod = period;
  document.querySelectorAll("#period-filter .btn").forEach(btn => {
    btn.classList.toggle("active", btn.textContent.trim().toLowerCase() === period);
  });
  loadAnalytics();
}

async function loadAnalytics() {
  const summaryEl = document.getElementById("analytics-summary");
  const recommendationsEl = document.getElementById("analytics-recommendations");
  const insightsEl = document.getElementById("analytics-insights");
  if (!summaryEl || !recommendationsEl || !insightsEl) return;

  try {
    const res = await apiFetch(`/api/analytics/summary?period=${analyticsPeriod}`);
    const data = await res.json();
    renderAnalytics(data);
    renderTrendChart(data.trend_data || []);
    renderComparisonCharts(data.insights || {});
  } catch {
    summaryEl.innerHTML = '<p class="text-danger small mb-0">No se pudo cargar la analítica.</p>';
    recommendationsEl.innerHTML = "";
    insightsEl.innerHTML = "";
  }
  // Refresh the companion sections (pipeline feedback + metrics collector)
  // in parallel — they live in the same card and should stay in sync.
  loadPipelineFeedback();
  loadMetricsCollectionStatus();
}


function renderAnalytics(data) {
  const summaryEl = document.getElementById("analytics-summary");
  const recommendationsEl = document.getElementById("analytics-recommendations");
  const insightsEl = document.getElementById("analytics-insights");
  if (!summaryEl || !recommendationsEl || !insightsEl) return;

  const summary = data.summary || {};
  summaryEl.innerHTML = `
    <div class="row g-3">
      <div class="col-sm-6 col-lg-3">
        <div class="border rounded p-3 h-100">
          <div class="text-muted small">Posts con métricas</div>
          <div class="fs-4 fw-semibold">${summary.tracked_posts || 0}</div>
        </div>
      </div>
      <div class="col-sm-6 col-lg-3">
        <div class="border rounded p-3 h-100">
          <div class="text-muted small">Impresiones totales</div>
          <div class="fs-4 fw-semibold">${formatNumber(summary.total_impressions || 0)}</div>
        </div>
      </div>
      <div class="col-sm-6 col-lg-3">
        <div class="border rounded p-3 h-100">
          <div class="text-muted small">Engagement promedio</div>
          <div class="fs-4 fw-semibold">${formatPercent(summary.avg_engagement_rate || 0)}</div>
        </div>
      </div>
      <div class="col-sm-6 col-lg-3">
        <div class="border rounded p-3 h-100">
          <div class="text-muted small">Comentarios promedio</div>
          <div class="fs-4 fw-semibold">${summary.avg_comments || 0}</div>
        </div>
      </div>
    </div>
  `;

  const recommendations = data.recommendations || [];
  recommendationsEl.innerHTML = recommendations.length
    ? `
      <h6 class="text-muted mb-2">Ajustes sugeridos</h6>
      <ul class="mb-0">
        ${recommendations.map(item => `<li>${escapeHtml(item)}</li>`).join("")}
      </ul>
    `
    : "";

  const topPosts = data.top_posts || [];
  const insightGroups = data.insights || {};
  const cards = [
    renderInsightCard("Mejores hooks", insightGroups.hook_type || []),
    renderInsightCard("Mejores CTAs", insightGroups.cta_type || []),
    renderInsightCard("Mejor estilo visual", insightGroups.visual_style || []),
    renderInsightCard("Mejor longitud", insightGroups.length_bucket || []),
  ].filter(Boolean);

  const topPostsHtml = topPosts.length
    ? `
      <div class="mt-3">
        <h6 class="text-muted mb-2">Top posts por engagement</h6>
        <div class="d-grid gap-2">
          ${topPosts.map(post => `
            <div class="border rounded p-2">
              <div class="d-flex justify-content-between gap-2 flex-wrap">
                <strong>${escapeHtml(post.topic || "Sin tema")}</strong>
                <span class="badge bg-success">${formatPercent(post.engagement_rate || 0)}</span>
              </div>
              <div class="small text-muted mt-1">
                ${formatNumber(post.impressions || 0)} impresiones · ${post.comments || 0} comentarios · ${post.saves || 0} guardados
              </div>
            </div>
          `).join("")}
        </div>
      </div>
    `
    : "";

  insightsEl.innerHTML = cards.length || topPostsHtml
    ? `
      <div class="row g-3">
        ${cards.join("")}
      </div>
      ${topPostsHtml}
    `
    : '<p class="text-muted small mb-0">Agrega métricas a los posts para ver aprendizajes.</p>';
}


function renderInsightCard(title, rows) {
  if (!rows.length) return "";
  const best = rows[0];
  return `
    <div class="col-md-6 col-xl-3">
      <div class="border rounded p-3 h-100">
        <div class="text-muted small">${escapeHtml(title)}</div>
        <div class="fw-semibold mt-1">${escapeHtml(best.key || "unknown")}</div>
        <div class="small text-muted mt-2">
          ${formatPercent(best.avg_engagement_rate || 0)} engagement · ${Math.round(best.avg_impressions || 0)} impresiones promedio
        </div>
      </div>
    </div>
  `;
}


function _destroyChart(id) {
  if (analyticsCharts[id]) {
    analyticsCharts[id].destroy();
    delete analyticsCharts[id];
  }
}

function renderTrendChart(trendData) {
  _destroyChart("chart-trend");
  const trendSection = document.getElementById("analytics-trend");
  const ctx = document.getElementById("chart-trend");
  if (!trendSection || !ctx) return;

  const withData = trendData.filter(d => d.posts_count > 0);
  if (withData.length < 2) {
    trendSection.style.display = "none";
    return;
  }
  trendSection.style.display = "";

  analyticsCharts["chart-trend"] = new Chart(ctx, {
    type: "line",
    data: {
      labels: withData.map(d => d.date),
      datasets: [{
        label: "Engagement %",
        data: withData.map(d => +(d.avg_engagement_rate * 100).toFixed(2)),
        borderColor: "#0d6efd",
        backgroundColor: "rgba(13,110,253,0.08)",
        tension: 0.35,
        fill: true,
        pointRadius: 3,
      }],
    },
    options: {
      plugins: { legend: { display: false } },
      scales: { y: { min: 0, ticks: { callback: v => v + "%" } } },
    },
  });
}

function _renderBarChart(canvasId, rows) {
  _destroyChart(canvasId);
  const ctx = document.getElementById(canvasId);
  if (!ctx || !rows.length) return;

  analyticsCharts[canvasId] = new Chart(ctx, {
    type: "bar",
    data: {
      labels: rows.map(r => r.key || "—"),
      datasets: [{
        data: rows.map(r => +(r.avg_engagement_rate * 100).toFixed(2)),
        backgroundColor: rows.map((_, i) => `hsla(${210 + i * 30}, 70%, 55%, 0.75)`),
        borderRadius: 4,
      }],
    },
    options: {
      indexAxis: "y",
      plugins: { legend: { display: false } },
      scales: {
        x: { min: 0, ticks: { callback: v => v + "%" } },
      },
    },
  });
}

function renderComparisonCharts(insights) {
  const chartsSection = document.getElementById("analytics-charts");
  const hasData = ["category", "hook_type", "hour_bucket", "content_format"]
    .some(k => (insights[k] || []).length > 0);

  if (!chartsSection) return;
  chartsSection.style.display = hasData ? "" : "none";

  _renderBarChart("chart-category", insights.category || []);
  _renderBarChart("chart-hook", insights.hook_type || []);
  _renderBarChart("chart-hour", insights.hour_bucket || []);
  _renderBarChart("chart-format", insights.content_format || []);
}


async function loadHistory() {
  const container = document.getElementById("history-container");
  if (!container) return;
  try {
    const searchInput = document.getElementById("history-search");
    if (searchInput) historySearch = searchInput.value.trim();
    const params = new URLSearchParams({
      page: String(historyPage),
      limit: "8",
    });
    if (historySearch) params.set("search", historySearch);
    const res = await apiFetch(`/api/history?${params.toString()}`);
    const data = await res.json();
    const posts = data.posts || [];
    const pagination = data.pagination || {};
    historyTotalPages = pagination.pages || 1;
    renderHistoryPagination(pagination);

    if (!posts.length) {
      container.innerHTML = '<p class="text-muted small">Sin publicaciones registradas aún.</p>';
      return;
    }

    container.innerHTML = posts.map(p => {
      const rawDate = p.created_at || p.date || "";
      const date = rawDate
        ? new Date(rawDate.endsWith("Z") ? rawDate : rawDate + "Z").toLocaleDateString("es-ES", {
            day: "2-digit", month: "short", year: "numeric",
            hour: "2-digit", minute: "2-digit"
          })
        : "—";
      const imgHtml = p.image_url
        ? `<img src="${escapeHtml(p.image_url)}" alt="imagen" style="width:64px;height:64px;object-fit:cover;border-radius:6px;flex-shrink:0;">`
        : "";
      const descHtml = p.image_desc
        ? `<small class="text-info d-block mt-1"><i class="bi bi-image me-1"></i>${escapeHtml(p.image_desc)}</small>`
        : "";
      return `
        <div class="history-item">
          <div class="d-flex gap-3 align-items-start">
            ${imgHtml}
            <div class="flex-grow-1 min-width-0">
              <div class="d-flex justify-content-between align-items-start flex-wrap gap-1">
                <div class="d-flex gap-1 flex-wrap">
                  <span class="badge bg-secondary">${escapeHtml(p.topic || "—")}</span>
                  ${p.category ? `<span class="badge text-bg-light border">${escapeHtml(p.category)}</span>` : ""}
                </div>
                <small class="text-muted">${date}</small>
              </div>
              <p class="mb-0 mt-1 text-muted small">${escapeHtml((p.post_text || "").substring(0, 120))}${(p.post_text || "").length > 120 ? "..." : ""}</p>
              ${descHtml}
              <div class="d-flex flex-wrap gap-2 mt-2 small">
                <span class="badge text-bg-light border">Imp: ${formatNumber(p.impressions || 0)}</span>
                <span class="badge text-bg-light border">React: ${formatNumber(p.reactions || 0)}</span>
                <span class="badge text-bg-light border">Com: ${formatNumber(p.comments || 0)}</span>
                <span class="badge text-bg-light border">Sav: ${formatNumber(p.saves || 0)}</span>
                <span class="badge ${p.engagement_rate ? "bg-success" : "bg-secondary"}">${formatPercent(p.engagement_rate || 0)}</span>
              </div>
              <div class="row g-2 mt-2">
                <div class="col-6 col-md-2">
                  <input id="metric-impressions-${p.id}" type="number" min="0" class="form-control form-control-sm" placeholder="Imp" value="${p.impressions || 0}">
                </div>
                <div class="col-6 col-md-2">
                  <input id="metric-reactions-${p.id}" type="number" min="0" class="form-control form-control-sm" placeholder="React" value="${p.reactions || 0}">
                </div>
                <div class="col-6 col-md-2">
                  <input id="metric-comments-${p.id}" type="number" min="0" class="form-control form-control-sm" placeholder="Com" value="${p.comments || 0}">
                </div>
                <div class="col-6 col-md-2">
                  <input id="metric-saves-${p.id}" type="number" min="0" class="form-control form-control-sm" placeholder="Sav" value="${p.saves || 0}">
                </div>
                <div class="col-6 col-md-2">
                  <input id="metric-clicks-${p.id}" type="number" min="0" class="form-control form-control-sm" placeholder="Clicks" value="${p.link_clicks || 0}">
                </div>
                <div class="col-6 col-md-2 d-grid">
                  <button class="btn btn-sm btn-outline-primary" onclick="savePostMetrics(${p.id})">
                    Guardar métricas
                  </button>
                </div>
                <div class="col-6 col-md-2 d-grid">
                  <button id="scrape-btn-${p.id}"
                    class="btn btn-sm btn-outline-secondary"
                    onclick="scrapePostMetrics(${p.id})"
                    ${p.linkedin_url ? '' : 'disabled title="URL de LinkedIn no disponible"'}>
                    <i class="bi bi-graph-up me-1"></i>Auto-métricas
                  </button>
                </div>
              </div>
              ${p.linkedin_url ? `<div class="mt-1"><a href="${escapeHtml(p.linkedin_url)}" target="_blank" rel="noopener" class="small text-muted"><i class="bi bi-linkedin me-1"></i>Ver en LinkedIn</a></div>` : ''}
              <div class="mt-2">
                <button class="btn btn-sm btn-outline-info" onclick="togglePostDiagnosis(${p.id})">
                  <i class="bi bi-clipboard-data me-1"></i><span id="diag-toggle-label-${p.id}">Ver diagnóstico</span>
                </button>
              </div>
              <div id="diagnosis-${p.id}" class="mt-2" style="display:none"></div>
            </div>
          </div>
        </div>`;
    }).join("");
  } catch {
    container.innerHTML = '<p class="text-danger small">Error al cargar historial.</p>';
    renderHistoryPagination({ page: 1, pages: 1, total: 0 });
  }
}

function renderHistoryPagination(pagination) {
  const wrap = document.getElementById("history-pagination");
  const label = document.getElementById("history-page-label");
  const prev = document.getElementById("history-prev-btn");
  const next = document.getElementById("history-next-btn");
  if (!wrap || !label || !prev || !next) return;

  const page = pagination.page || 1;
  const pages = pagination.pages || 1;
  const total = pagination.total || 0;
  historyPage = page;
  historyTotalPages = pages;

  if (total <= 8 && !historySearch) {
    wrap.classList.add("d-none");
    return;
  }

  wrap.classList.remove("d-none");
  label.textContent = `Página ${page} de ${pages} · ${total} publicaciones`;
  prev.disabled = page <= 1;
  next.disabled = page >= pages;
}

function changeHistoryPage(delta) {
  const nextPage = historyPage + delta;
  if (nextPage < 1 || nextPage > historyTotalPages) return;
  historyPage = nextPage;
  loadHistory();
}


async function savePostMetrics(postId) {
  const payload = {
    impressions: Number(document.getElementById(`metric-impressions-${postId}`)?.value || 0),
    reactions: Number(document.getElementById(`metric-reactions-${postId}`)?.value || 0),
    comments: Number(document.getElementById(`metric-comments-${postId}`)?.value || 0),
    saves: Number(document.getElementById(`metric-saves-${postId}`)?.value || 0),
    link_clicks: Number(document.getElementById(`metric-clicks-${postId}`)?.value || 0),
  };

  try {
    const res = await apiFetch(`/api/history/${postId}/metrics`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const data = await res.json();
    if (!res.ok || data.error) {
      showGlobalError(data.error || "No se pudieron guardar las métricas.");
      return;
    }
    await Promise.all([loadHistory(), loadAnalytics()]);
  } catch {
    showGlobalError("Error de red al guardar métricas del post.");
  }
}

async function scrapePostMetrics(postId) {
  const btn = document.getElementById(`scrape-btn-${postId}`);
  if (btn) {
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Scraping...';
  }

  try {
    const res = await apiFetch(`/api/history/${postId}/scrape_metrics`, { method: "POST" });
    const data = await res.json();
    if (!res.ok || data.error) {
      showGlobalError(data.error || "No se pudo iniciar el scraping de métricas.");
      if (btn) { btn.disabled = false; btn.innerHTML = '<i class="bi bi-graph-up me-1"></i>Auto-métricas'; }
      return;
    }

    const jobId = data.job_id;
    const pollInterval = setInterval(async () => {
      try {
        const statusRes = await apiFetch(`/api/job_status/${jobId}`);
        const status = await statusRes.json();
        if (status.status === "done") {
          clearInterval(pollInterval);
          if (btn) { btn.disabled = false; btn.innerHTML = '<i class="bi bi-graph-up me-1"></i>Auto-métricas'; }
          await Promise.all([loadHistory(), loadAnalytics()]);
        } else if (status.status === "error") {
          clearInterval(pollInterval);
          if (btn) { btn.disabled = false; btn.innerHTML = '<i class="bi bi-graph-up me-1"></i>Auto-métricas'; }
          showGlobalError(status.message || "Error al obtener métricas automáticamente.");
        }
      } catch {
        // keep polling
      }
    }, 2000);
  } catch {
    if (btn) { btn.disabled = false; btn.innerHTML = '<i class="bi bi-graph-up me-1"></i>Auto-métricas'; }
    showGlobalError("Error de red al iniciar scraping de métricas.");
  }
}


// ── Per-post diagnosis ─────────────────────────────────────────────────────

const _DIAG_VERDICT_CLASS = {
  top: "bg-success text-white",
  above: "bg-info text-white",
  average: "bg-secondary text-white",
  below: "bg-danger text-white",
  no_data: "bg-warning text-dark",
};

async function togglePostDiagnosis(postId) {
  const container = document.getElementById(`diagnosis-${postId}`);
  const label = document.getElementById(`diag-toggle-label-${postId}`);
  if (!container) return;

  if (container.style.display !== "none") {
    container.style.display = "none";
    if (label) label.textContent = "Ver diagnóstico";
    return;
  }
  container.style.display = "";
  if (label) label.textContent = "Ocultar diagnóstico";
  if (container.dataset.loaded === "1") return;
  container.innerHTML =
    '<div class="text-muted small"><span class="spinner-border spinner-border-sm me-2"></span>Analizando...</div>';
  try {
    const res = await apiFetch(`/api/history/${postId}/diagnosis`);
    const data = await res.json();
    if (!res.ok || data.error) {
      container.innerHTML = `<div class="text-danger small">${escapeHtml(data.error || "No se pudo cargar el diagnóstico.")}</div>`;
      return;
    }
    renderPostDiagnosis(container, data.diagnosis || {});
    container.dataset.loaded = "1";
  } catch (error) {
    container.innerHTML = `<div class="text-danger small">${escapeHtml(error.message || "Error de red al pedir el diagnóstico.")}</div>`;
  }
}

function renderPostDiagnosis(container, diagnosis) {
  const verdict = diagnosis.verdict || "no_data";
  const badgeClass = _DIAG_VERDICT_CLASS[verdict] || "bg-secondary text-white";
  const score = typeof diagnosis.score === "number" ? diagnosis.score.toFixed(1) : "—";
  const engagementVs = typeof diagnosis.engagement_vs_peers === "number" ? `${diagnosis.engagement_vs_peers.toFixed(2)}×` : "—";
  const poolLabel = diagnosis.comparison_pool_label === "category" ? "tu categoría" : "tu historial";
  const poolSize = diagnosis.comparison_pool_size || 0;
  const highlights = diagnosis.highlights || [];
  const weaknesses = diagnosis.weaknesses || [];

  const renderList = (items, icon, color) => items.length
    ? `<ul class="list-unstyled mb-0">${items.map(item => `<li class="small ${color}"><i class="bi ${icon} me-1"></i>${escapeHtml(item)}</li>`).join("")}</ul>`
    : '<p class="small text-muted mb-0">Sin observaciones.</p>';

  container.innerHTML = `
    <div class="border rounded p-3 bg-white">
      <div class="d-flex flex-wrap align-items-center gap-2 mb-2">
        <span class="badge ${badgeClass}">${escapeHtml(diagnosis.verdict_label || verdict)}</span>
        <span class="small text-muted">Score ${score}/10</span>
        <span class="small text-muted">·</span>
        <span class="small text-muted">${engagementVs} vs media de ${escapeHtml(poolLabel)} (${poolSize} posts)</span>
      </div>
      <div class="row g-2">
        <div class="col-md-6">
          <div class="text-success small fw-semibold mb-1"><i class="bi bi-check-circle me-1"></i>Qué funcionó</div>
          ${renderList(highlights, "bi-arrow-up-circle", "text-success")}
        </div>
        <div class="col-md-6">
          <div class="text-danger small fw-semibold mb-1"><i class="bi bi-exclamation-circle me-1"></i>Qué falló</div>
          ${renderList(weaknesses, "bi-arrow-down-circle", "text-danger")}
        </div>
      </div>
    </div>
  `;
}


// ── Pipeline feedback (analytics card section) ─────────────────────────────

async function loadPipelineFeedback() {
  const box = document.getElementById("pipeline-feedback-box");
  if (!box) return;
  box.textContent = "Cargando retroalimentación...";
  try {
    const res = await apiFetch("/api/analytics/pipeline_feedback");
    const data = await res.json();
    if (!res.ok || data.error) {
      box.textContent = data.error || "No se pudo cargar la retroalimentación.";
      return;
    }
    if (!data.feedback) {
      box.textContent = `Aún no hay suficientes posts con métricas (basado en ${data.based_on_posts || 0} posts). Captura métricas para que el sistema empiece a aprender.`;
      return;
    }
    box.textContent = data.feedback;
  } catch (error) {
    box.textContent = error.message || "Error de red al pedir la retroalimentación.";
  }
}


// ── Metrics collection settings ────────────────────────────────────────────

async function loadMetricsCollectionStatus() {
  const enabledEl = document.getElementById("metrics-collection-enabled");
  const intervalEl = document.getElementById("metrics-collection-interval");
  const statusEl = document.getElementById("metrics-collector-status");
  if (!enabledEl || !intervalEl || !statusEl) return;
  try {
    const res = await apiFetch("/api/metrics/collection_status");
    const data = await res.json();
    if (!res.ok || data.error) {
      statusEl.innerHTML = `<span class="text-danger">${escapeHtml(data.error || "No se pudo cargar el estado.")}</span>`;
      return;
    }
    const settings = data.settings || {};
    enabledEl.checked = !!settings.enabled;
    intervalEl.value = Number(settings.interval_hours || 6);
    const run = data.current_run || {};
    const lastCollected = settings.last_collected_at || "—";
    const runStatus = run.status || "idle";
    const runMessage = run.message || "Sin actividad reciente.";
    statusEl.innerHTML = `
      <div><strong>Estado:</strong> ${escapeHtml(runStatus)}</div>
      <div>${escapeHtml(runMessage)}</div>
      <div>Última recolección: ${escapeHtml(lastCollected)}</div>
      <div>Procesados/actualizados/errores en último ciclo: ${run.processed || 0} / ${run.updated || 0} / ${run.errors || 0}</div>
    `;
  } catch (error) {
    statusEl.innerHTML = `<span class="text-danger">${escapeHtml(error.message || "Error de red.")}</span>`;
  }
}

async function saveMetricsCollectionSettings() {
  const enabledEl = document.getElementById("metrics-collection-enabled");
  const intervalEl = document.getElementById("metrics-collection-interval");
  if (!enabledEl || !intervalEl) return;
  const payload = {
    enabled: enabledEl.checked,
    interval_hours: parseInt(intervalEl.value || "6", 10),
  };
  try {
    const res = await apiFetch("/api/metrics/collection_settings", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const data = await res.json();
    if (!res.ok || data.error) {
      showGlobalError(data.error || "No se pudo guardar la configuración.");
      return;
    }
    await loadMetricsCollectionStatus();
  } catch (error) {
    showGlobalError(error.message || "Error de red al guardar la configuración.");
  }
}

async function collectMetricsNow() {
  const btn = document.getElementById("metrics-collect-now-btn");
  if (btn) {
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Recolectando...';
  }
  try {
    const res = await apiFetch("/api/metrics/collect_now", { method: "POST" });
    const data = await res.json();
    if (!res.ok || data.error) {
      showGlobalError(data.error || "No se pudo iniciar la recolección.");
      return;
    }
    // Poll status until idle
    const poll = setInterval(async () => {
      await loadMetricsCollectionStatus();
      const statusBox = document.getElementById("metrics-collector-status");
      if (statusBox && statusBox.textContent.includes("idle")) {
        clearInterval(poll);
        if (btn) { btn.disabled = false; btn.innerHTML = '<i class="bi bi-play-fill me-1"></i>Ejecutar ahora'; }
        await Promise.all([loadHistory(), loadAnalytics(), loadPipelineFeedback()]);
      }
    }, 2500);
  } catch (error) {
    if (btn) { btn.disabled = false; btn.innerHTML = '<i class="bi bi-play-fill me-1"></i>Ejecutar ahora'; }
    showGlobalError(error.message || "Error de red al iniciar la recolección.");
  }
}


// ── Utility ────────────────────────────────────────────────────────────────

function resetUI() {
  currentSessionId = null;
  renderResolvedCategoryBanner(currentCategory, currentResolvedCategory);
  const previewPanel = document.getElementById("preview-panel");
  if (!previewPanel) return;
  previewPanel.classList.add("d-none");
  const regenerateBtn = document.getElementById("regenerate-btn");
  if (regenerateBtn) regenerateBtn.classList.add("d-none");
  const publishSuccess = document.getElementById("publish-success");
  if (publishSuccess) publishSuccess.classList.add("d-none");
  const publishError = document.getElementById("publish-error");
  if (publishError) publishError.classList.add("d-none");
  const publishProgress = document.getElementById("publish-progress");
  if (publishProgress) publishProgress.classList.add("d-none");
  const publishBtn = document.getElementById("publish-btn");
  if (publishBtn) {
    publishBtn.disabled = false;
    publishBtn.classList.remove("d-none");
    publishBtn.innerHTML = '<i class="bi bi-linkedin me-2"></i>Publicar en LinkedIn';
  }
  const reasoningBox = document.getElementById("reasoning-box");
  if (reasoningBox) reasoningBox.classList.add("d-none");
  const testModeBadge = document.getElementById("test-mode-badge");
  if (testModeBadge) testModeBadge.classList.add("d-none");
  const screenshotsSection = document.getElementById("screenshots-section");
  if (screenshotsSection) screenshotsSection.classList.add("d-none");
  const screenshotsGrid = document.getElementById("screenshots-grid");
  if (screenshotsGrid) screenshotsGrid.innerHTML = "";
  const screenshotsCount = document.getElementById("screenshots-count");
  if (screenshotsCount) screenshotsCount.textContent = "";
  for (let i = 1; i <= 7; i++) {
    const icon = document.getElementById(`icon-${i}`);
    if (icon) icon.innerHTML = '<i class="bi bi-circle text-muted"></i>';
    const d = document.getElementById(`detail-${i}`);
    if (d) {
      d.textContent = "";
      d.className = "step-detail text-muted small";
    }
    const rb = document.getElementById(`regen-${i}`);
    if (rb) rb.classList.add("d-none");
  }
}

function resetAllBtns() {
  const gen = document.getElementById("generate-btn");
  if (gen) {
    gen.disabled = false;
    gen.innerHTML = '<i class="bi bi-magic me-2"></i>Generar Post';
  }
  const test = document.getElementById("test-btn");
  if (test) {
    test.disabled = false;
    test.innerHTML = '<i class="bi bi-eye me-2"></i>Modo Prueba';
  }
}

function setPipelineButtonsBusy(testMode) {
  const generateBtn = document.getElementById("generate-btn");
  if (generateBtn) {
    generateBtn.disabled = true;
    generateBtn.innerHTML = '<span class="spinner-border spinner-border-sm me-2"></span>Generando...';
  }
  const testBtn = document.getElementById("test-btn");
  if (testBtn) {
    testBtn.disabled = true;
    testBtn.innerHTML = testMode
      ? '<span class="spinner-border spinner-border-sm me-2"></span>Generando...'
      : '<i class="bi bi-eye me-2"></i>Modo Prueba';
  }
}

function regenerate() {
  const regenerateBtn = document.getElementById("regenerate-btn");
  if (regenerateBtn) regenerateBtn.classList.add("d-none");
  runPipeline();
}

function showGlobalError(msg) {
  const alert = document.createElement("div");
  alert.className = "alert alert-danger alert-dismissible mt-3";
  alert.innerHTML = `<i class="bi bi-exclamation-triangle-fill me-2"></i>${escapeHtml(msg)}
    <button type="button" class="btn-close" data-bs-dismiss="alert"></button>`;
  document.querySelector("main").prepend(alert);
}

function getPersistedPipelineSession() {
  try {
    const raw = localStorage.getItem(PIPELINE_SESSION_STORAGE_KEY);
    return raw ? JSON.parse(raw) : null;
  } catch {
    return null;
  }
}

function persistPipelineSessionState() {
  try {
    const payload = {
      session_id: currentSessionId,
      category: currentCategory || getSelectedCategoryName(),
      resolved_category: currentResolvedCategory,
      test_mode: !!currentTestMode,
    };
    localStorage.setItem(PIPELINE_SESSION_STORAGE_KEY, JSON.stringify(payload));
  } catch {
    // ignore storage issues
  }
}

function clearPersistedPipelineSession() {
  try {
    localStorage.removeItem(PIPELINE_SESSION_STORAGE_KEY);
  } catch {
    // ignore storage issues
  }
}

function stopPipelineStatePolling() {
  if (!pipelineStatePollInterval) return;
  clearInterval(pipelineStatePollInterval);
  pipelineStatePollInterval = null;
}

function startPipelineStatePolling() {
  stopPipelineStatePolling();
  if (!currentSessionId) return;
  pipelineStatePollInterval = setInterval(async () => {
    await refreshPipelineState();
  }, 2000);
}

async function restorePipelineState() {
  const saved = getPersistedPipelineSession();
  if (!saved?.session_id) return;
  currentSessionId = saved.session_id;
  currentTestMode = !!saved.test_mode;
  if (saved.category) currentCategory = saved.category;
  if (saved.resolved_category) currentResolvedCategory = saved.resolved_category;
  await refreshPipelineState();
}

async function refreshPipelineState() {
  if (!currentSessionId) return;
  try {
    const res = await apiFetch(`/api/pipeline_sessions/${currentSessionId}`);
    if (res.status === 404) {
      stopPipelineStatePolling();
      clearPersistedPipelineSession();
      currentSessionId = null;
      lastPipelineStateSignature = "";
      return;
    }
    const state = await res.json();
    const signature = `${state.updated_at || ""}:${(state.events || []).length}:${state.status || ""}`;
    if (signature !== lastPipelineStateSignature) {
      renderPersistedPipelineState(state);
      lastPipelineStateSignature = signature;
    }
    if (state.status === "running") startPipelineStatePolling();
    else stopPipelineStatePolling();
  } catch {
    // ignore transient refresh issues
  }
}

function renderPersistedPipelineState(state) {
  currentSessionId = state.id;
  currentTestMode = !!state.test_mode;
  currentCategory = state.requested_category || state.category || currentCategory || getSelectedCategoryName();
  currentResolvedCategory = state.resolved_category || state.category || currentResolvedCategory || "";
  persistPipelineSessionState();

  const select = document.getElementById("pipeline-category");
  if (select && currentCategory && Array.from(select.options).some(option => option.value === currentCategory)) {
    select.value = currentCategory;
    renderSelectedCategorySummary(currentCategory);
  }

  resetUI();
  const stepsPanel = document.getElementById("steps-panel");
  if (stepsPanel) stepsPanel.classList.remove("d-none");
  renderResolvedCategoryBanner(currentCategory, currentResolvedCategory);

  const events = Array.isArray(state.events) ? state.events : [];
  let hasPreview = false;
  let hasError = false;

  for (const event of events) {
    if (event.step === 0 && event.status === "error") {
      hasError = true;
      continue;
    }
    if (event.status === "running") setStepRunning(event.step, event.message || "");
    else if (event.status === "done") setStepDone(event.step, event.result);
    else if (event.status === "error") {
      hasError = true;
      setStepError(event.step, event.message || "");
    } else if (event.status === "preview") {
      hasPreview = true;
      setStepDone(6, event.test_mode ? "Vista previa generada (modo prueba)" : "Listo para publicar");
    }
  }

  if (state.preview) {
    hasPreview = true;
    setStepDone(6, state.preview.test_mode ? "Vista previa generada (modo prueba)" : "Listo para publicar");
    showPreviewPanel(state.preview);
    const regenerateBtn = document.getElementById("regenerate-btn");
    if (regenerateBtn) regenerateBtn.classList.remove("d-none");
  }

  if (state.status === "running") setPipelineButtonsBusy(currentTestMode);
  else resetAllBtns();

  if (hasError && events.length) {
    const latestError = [...events].reverse().find(event => event.status === "error");
    if (latestError?.message) showGlobalError(latestError.message);
  }

  if (!hasPreview && state.status === "ready" && state.preview) showPreviewPanel(state.preview);
}

function renderResolvedCategoryBanner(requestedCategory, resolvedCategory) {
  const banner = document.getElementById("resolved-category-banner");
  const nameEl = document.getElementById("resolved-category-name");
  if (!banner || !nameEl) return;

  if (requestedCategory === "random" && resolvedCategory) {
    nameEl.textContent = resolvedCategory;
    banner.classList.remove("d-none");
    return;
  }

  nameEl.textContent = "";
  banner.classList.add("d-none");
}

function escapeHtml(str) {
  const div = document.createElement("div");
  div.appendChild(document.createTextNode(str));
  return div.innerHTML;
}


function formatNumber(value) {
  return Number(value || 0).toLocaleString("es-ES");
}


function formatPercent(value) {
  return `${(Number(value || 0) * 100).toFixed(1)}%`;
}

// ── Pipeline categories ─────────────────────────────────────────────────────

async function loadCategories() {
  try {
    const res = await apiFetch("/api/categories");
    const data = await res.json();
    categoryStore = data.categories || [];
    renderCategorySelect(data.default_category || "");
    renderCategorySettings(data.default_category || "");
    renderScheduleCategorySelect();
  } catch {
    // ignore
  }
}

function getSelectedCategoryName() {
  const select = document.getElementById("pipeline-category");
  return select ? select.value : "";
}

function getCategoryByName(name) {
  return categoryStore.find(cat => cat.name === name) || null;
}

function getDefaultCategoryName() {
  return (categoryStore.find(cat => cat.is_default) || categoryStore[0] || {}).name || "";
}

function renderCategorySelect(defaultCategory) {
  const select = document.getElementById("pipeline-category");
  if (!select) return;

  if (!categoryStore.length) {
    select.innerHTML = '<option value="">Sin categorías</option>';
    currentCategory = "";
    renderSelectedCategorySummary("");
    return;
  }

  select.innerHTML = '<option value="random">random · elegir categoría al azar</option>' + categoryStore.map(cat =>
    `<option value="${escapeHtml(cat.name)}">${escapeHtml(cat.name)}</option>`
  ).join("");
  const selectedCategory = currentCategory && Array.from(select.options).some(option => option.value === currentCategory)
    ? currentCategory
    : (defaultCategory || getDefaultCategoryName() || categoryStore[0].name);
  select.value = selectedCategory;
  currentCategory = select.value;
  renderSelectedCategorySummary(select.value);
}

function renderSelectedCategorySummary(categoryName) {
  const nameEl = document.getElementById("pipeline-category-name");
  const descriptionEl = document.getElementById("pipeline-category-description");
  const badgeEl = document.getElementById("pipeline-category-default-badge");
  if (!nameEl || !descriptionEl || !badgeEl) return;

  if (categoryName === "random") {
    nameEl.textContent = "Random";
    descriptionEl.textContent = "El pipeline elegirá al azar una categoría disponible al iniciar una nueva generación.";
    badgeEl.classList.add("d-none");
    return;
  }

  const category = getCategoryByName(categoryName);
  if (!category) {
    nameEl.textContent = "Sin categorías";
    descriptionEl.textContent = "Crea o importa una categoría para personalizar el pipeline.";
    badgeEl.classList.add("d-none");
    return;
  }

  nameEl.textContent = category.name;
  descriptionEl.textContent = category.description || "Categoría personalizada";
  badgeEl.classList.toggle("d-none", !category.is_default);
}

function renderCategorySettings(defaultCategory) {
  const list = document.getElementById("category-list");
  const pill = document.getElementById("settings-default-pill");
  if (pill) pill.textContent = defaultCategory || "Default";
  if (!list) return;

  if (!categoryStore.length) {
    list.innerHTML = '<p class="text-muted small mb-0">No hay categorías todavía.</p>';
    createNewCategory();
    return;
  }

  list.innerHTML = categoryStore.map(cat => `
    <button type="button"
      class="settings-category-item ${currentCategoryId === cat.id ? "active" : ""}"
      onclick="selectCategoryForEdit(${cat.id})">
      <div class="d-flex justify-content-between align-items-start gap-2">
        <div>
          <div class="fw-semibold">${escapeHtml(cat.name)}</div>
          <div class="text-muted small">${escapeHtml(cat.description || "Sin descripción")}</div>
        </div>
        ${cat.is_default ? '<span class="badge bg-primary">Default</span>' : ""}
      </div>
    </button>
  `).join("");

  if (!currentCategoryId) {
    const initial = categoryStore.find(c => c.name === defaultCategory) || categoryStore[0];
    if (initial) selectCategoryForEdit(initial.id);
  }
}

function selectCategoryForEdit(categoryId) {
  currentCategoryId = categoryId;
  const category = categoryStore.find(cat => cat.id === categoryId);
  if (!category) return;

  const fields = {
    "category-id": category.id,
    "category-name": category.name,
    "category-description": category.description || "",
    "category-trends-prompt": category.trends_prompt || "",
    "category-history-prompt": category.history_prompt || "",
    "category-content-prompt": category.content_prompt || "",
    "category-image-prompt": category.image_prompt || "",
  };

  Object.entries(fields).forEach(([id, value]) => {
    const el = document.getElementById(id);
    if (el) el.value = value;
  });
  const defaultCheckbox = document.getElementById("category-default");
  if (defaultCheckbox) defaultCheckbox.checked = !!category.is_default;

  const deleteBtn = document.getElementById("delete-category-btn");
  if (deleteBtn) deleteBtn.disabled = !!category.is_default;

  // New controls
  const pl = document.getElementById("category-post-length");
  if (pl) {
    pl.value = category.post_length || 200;
    document.getElementById("post-length-val").textContent = (category.post_length || 200) + " palabras";
  }
  const langVal = category.language || "auto";
  const langEl = document.getElementById(`lang-${langVal}`);
  if (langEl) langEl.checked = true;
  const hc = document.getElementById("category-hashtag-count");
  if (hc) {
    hc.value = category.hashtag_count ?? 4;
    const hcv = document.getElementById("hashtag-count-val");
    if (hcv) hcv.textContent = category.hashtag_count ?? 4;
  }
  const emojiEl = document.getElementById("category-use-emojis");
  if (emojiEl) emojiEl.checked = !!category.use_emojis;
  _catKeywords = Array.isArray(category.topic_keywords) ? [...category.topic_keywords] : [];
  renderCatKeywords();

  const negEl = document.getElementById("category-negative-prompt");
  if (negEl) negEl.value = category.negative_prompt || "";
  _catForbiddenPhrases = Array.isArray(category.forbidden_phrases) ? [...category.forbidden_phrases] : [];
  renderCatForbidden();
  _catVoiceExamples = Array.isArray(category.voice_examples) ? [...category.voice_examples] : [];
  renderCatVoiceExamples();
  _catFallbackTopics = Array.isArray(category.fallback_topics) ? [...category.fallback_topics] : [];
  renderCatFallbacks();
  const originalityEl = document.getElementById("category-originality-level");
  if (originalityEl) {
    originalityEl.value = category.originality_level || 3;
    const ov = document.getElementById("originality-level-val");
    if (ov) ov.textContent = `${category.originality_level || 3}/5`;
  }
  const evidenceEl = document.getElementById("category-evidence-mode");
  if (evidenceEl) evidenceEl.value = category.evidence_mode || "balanced";
  const hookStyleEl = document.getElementById("category-hook-style");
  if (hookStyleEl) hookStyleEl.value = category.hook_style || "auto";
  const ctaStyleEl = document.getElementById("category-cta-style");
  if (ctaStyleEl) ctaStyleEl.value = category.cta_style || "auto";
  const audienceEl = document.getElementById("category-audience-focus");
  if (audienceEl) audienceEl.value = category.audience_focus || "";
  _catPreferredFormats = Array.isArray(category.preferred_formats) ? [...category.preferred_formats] : [];
  _catPreferredVisualStyles = Array.isArray(category.preferred_visual_styles) ? [...category.preferred_visual_styles] : [];
  renderPreferredFormatButtons();
  renderPreferredVisualStyleButtons();

  renderCategorySettings(categoryStore.find(cat => cat.is_default)?.name || "");
}

function createNewCategory() {
  currentCategoryId = -1;
  [
    "category-id",
    "category-name",
    "category-description",
    "category-trends-prompt",
    "category-history-prompt",
    "category-content-prompt",
    "category-image-prompt",
  ].forEach((id) => {
    const el = document.getElementById(id);
    if (el) el.value = "";
  });
  const defaultCheckbox = document.getElementById("category-default");
  if (defaultCheckbox) defaultCheckbox.checked = false;
  const deleteBtn = document.getElementById("delete-category-btn");
  if (deleteBtn) deleteBtn.disabled = true;
  showSettingsAlert("", "");
  // Reset new controls
  const pl = document.getElementById("category-post-length");
  if (pl) { pl.value = 200; }
  const plv = document.getElementById("post-length-val");
  if (plv) plv.textContent = "200 palabras";
  const langAuto = document.getElementById("lang-auto");
  if (langAuto) langAuto.checked = true;
  const hc = document.getElementById("category-hashtag-count");
  if (hc) hc.value = 4;
  const hcv = document.getElementById("hashtag-count-val");
  if (hcv) hcv.textContent = "4";
  const emojiEl = document.getElementById("category-use-emojis");
  if (emojiEl) emojiEl.checked = false;
  _catKeywords = [];
  renderCatKeywords();
  const negEl = document.getElementById("category-negative-prompt");
  if (negEl) negEl.value = "";
  _catForbiddenPhrases = [];
  renderCatForbidden();
  _catVoiceExamples = [];
  renderCatVoiceExamples();
  _catFallbackTopics = [];
  renderCatFallbacks();
  const originalityEl = document.getElementById("category-originality-level");
  if (originalityEl) originalityEl.value = 3;
  const ov = document.getElementById("originality-level-val");
  if (ov) ov.textContent = "3/5";
  const evidenceEl = document.getElementById("category-evidence-mode");
  if (evidenceEl) evidenceEl.value = "balanced";
  const hookStyleEl = document.getElementById("category-hook-style");
  if (hookStyleEl) hookStyleEl.value = "auto";
  const ctaStyleEl = document.getElementById("category-cta-style");
  if (ctaStyleEl) ctaStyleEl.value = "auto";
  const audienceEl = document.getElementById("category-audience-focus");
  if (audienceEl) audienceEl.value = "";
  _catPreferredFormats = [];
  _catPreferredVisualStyles = [];
  renderPreferredFormatButtons();
  renderPreferredVisualStyleButtons();
  document.querySelectorAll(".preset-btn").forEach(btn => {
    btn.classList.remove("btn-primary", "active");
    btn.classList.add("btn-outline-secondary");
  });
  renderCategorySettings(categoryStore.find(cat => cat.is_default)?.name || "");
}

async function saveCategorySettings() {
  const nameEl = document.getElementById("category-name");
  if (!nameEl) return;
  const payload = {
    id: document.getElementById("category-id")?.value || null,
    name: nameEl.value.trim(),
    description: document.getElementById("category-description")?.value.trim() || "",
    trends_prompt: document.getElementById("category-trends-prompt")?.value.trim() || "",
    history_prompt: document.getElementById("category-history-prompt")?.value.trim() || "",
    content_prompt: document.getElementById("category-content-prompt")?.value.trim() || "",
    image_prompt: document.getElementById("category-image-prompt")?.value.trim() || "",
    is_default: !!document.getElementById("category-default")?.checked,
    post_length: parseInt(document.getElementById("category-post-length")?.value || 200),
    language: document.querySelector("input[name='cat-lang']:checked")?.value || "auto",
    hashtag_count: parseInt(document.getElementById("category-hashtag-count")?.value || 4),
    use_emojis: !!document.getElementById("category-use-emojis")?.checked,
    topic_keywords: [..._catKeywords],
    negative_prompt: document.getElementById("category-negative-prompt")?.value || "",
    fallback_topics: [..._catFallbackTopics],
    originality_level: parseInt(document.getElementById("category-originality-level")?.value || 3),
    evidence_mode: document.getElementById("category-evidence-mode")?.value || "balanced",
    hook_style: document.getElementById("category-hook-style")?.value || "auto",
    cta_style: document.getElementById("category-cta-style")?.value || "auto",
    audience_focus: document.getElementById("category-audience-focus")?.value.trim() || "",
    preferred_formats: [..._catPreferredFormats],
    preferred_visual_styles: [..._catPreferredVisualStyles],
    forbidden_phrases: [..._catForbiddenPhrases],
    voice_examples: [..._catVoiceExamples],
  };

  try {
    const res = await apiFetch("/api/categories", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const data = await res.json();
    if (!res.ok || data.error) {
      showSettingsAlert(data.error || "No se pudo guardar la categoría.", "danger");
      return;
    }
    showSettingsAlert("Categoría guardada correctamente.", "success");
    await loadCategories();
    if (data.category?.id) selectCategoryForEdit(data.category.id);
  } catch {
    showSettingsAlert("Error de red al guardar la categoría.", "danger");
  }
}

async function deleteCurrentCategory() {
  const id = document.getElementById("category-id")?.value;
  if (!id) return;
  if (!confirm("¿Eliminar esta categoría?")) return;

  try {
    const res = await apiFetch(`/api/categories/${id}`, { method: "DELETE" });
    const data = await res.json();
    if (!res.ok || data.error) {
      showSettingsAlert(data.error || "No se pudo eliminar la categoría.", "danger");
      return;
    }
    createNewCategory();
    await loadCategories();
    showSettingsAlert("Categoría eliminada.", "success");
  } catch {
    showSettingsAlert("Error de red al eliminar la categoría.", "danger");
  }
}

function showSettingsAlert(message, tone) {
  const alert = document.getElementById("settings-alert");
  if (!alert) return;
  if (!message) {
    alert.className = "alert d-none";
    alert.textContent = "";
    return;
  }
  alert.className = `alert alert-${tone}`;
  alert.textContent = message;
}

// ── Category tone presets ────────────────────────────────────────────────────

const TONE_PRESETS = {
  professional: {
    trends_prompt: "Prioriza temas sobre liderazgo, estrategia empresarial, productividad y mercado laboral con impacto real y reciente.",
    history_prompt: "Mantén un tono formal y evita repetir perspectivas o insights ya abordados en publicaciones anteriores.",
    content_prompt: "Escribe una publicación ejecutiva con datos o ejemplos concretos. Estructura: hook impactante, 2-3 insights accionables para profesionales senior, cierre con pregunta estratégica. Sin clichés motivacionales.",
    image_prompt: "Imagen editorial sobria y minimalista. Paleta corporativa: azules profundos, grises y plateados. Composición limpia, sin texto ni personajes explícitos.",
  },
  storytelling: {
    trends_prompt: "Busca situaciones laborales cotidianas, dilemas profesionales o momentos de aprendizaje personal con fuerte resonancia emocional.",
    history_prompt: "Asegúrate de que la historia tenga un arco narrativo diferente a las anteriores: contexto distinto, conflicto nuevo, lección diferente.",
    content_prompt: "Narra una historia en primera persona con estructura storytelling: situación inicial, conflicto o desafío real, aprendizaje clave y reflexión final. Tono auténtico, humano y vulnerable.",
    image_prompt: "Ilustración narrativa que capture un momento de tensión o transformación personal. Estilo cálido, simbólico y emotivo.",
  },
  technical: {
    trends_prompt: "Enfócate en novedades de tecnología, herramientas de desarrollo, IA aplicada, frameworks o tendencias de ingeniería de software de las últimas semanas.",
    history_prompt: "Evita repetir el mismo stack, herramienta o área técnica cubierta recientemente. Busca variedad técnica.",
    content_prompt: "Explica un concepto técnico de forma clara y aplicable para ingenieros. Incluye ejemplos concretos o casos de uso reales. Tono directo, sin fluff, con valor práctico inmediato.",
    image_prompt: "Interfaz futurista, arquitectura de sistemas abstracta o visualización de datos. Estilo oscuro, técnico y preciso. Paleta: cian, verde neón sobre fondo negro.",
  },
  opinion: {
    trends_prompt: "Elige temas en debate activo en la comunidad tech o empresarial que polaricen opiniones. Busca ángulos contraintuitivos o posiciones minoritarias bien argumentadas.",
    history_prompt: "No repitas posiciones ya tomadas. Elige un ángulo fresco o que contradiga el consenso habitual.",
    content_prompt: "Comparte una opinión clara y fundamentada desde la primera línea. Incluye argumentos sólidos con evidencia o lógica, reconoce el otro punto de vista y lanza una pregunta que incite al debate genuino.",
    image_prompt: "Imagen conceptual abstracta que refleje contraste, tensión o dualidad. Composición dinámica con colores contrastantes. Sin texto.",
  },
  tutorial: {
    trends_prompt: "Identifica habilidades en demanda, herramientas populares o procesos que los profesionales quieren dominar ahora mismo.",
    history_prompt: "Varía el nivel de dificultad y la temática respecto a tutoriales anteriores para maximizar variedad.",
    content_prompt: "Enseña algo útil y concreto en formato numerado (máximo 5 pasos). Aplica el concepto desde la primera línea. Sé específico, evita generalidades, termina con el resultado esperado.",
    image_prompt: "Infografía o diagrama conceptual que ilustre el proceso enseñado. Estilo educativo, limpio y estructurado. Colores guía claros.",
  },
};

function applyPreset(presetKey) {
  const preset = TONE_PRESETS[presetKey];
  if (!preset) return;

  // Fill prompts
  ["trends", "history", "content", "image"].forEach(key => {
    const el = document.getElementById(`category-${key}-prompt`);
    if (el) el.value = preset[`${key}_prompt`] || "";
  });

  // Highlight active preset button
  document.querySelectorAll(".preset-btn").forEach(btn => btn.classList.remove("active", "btn-primary"));
  const activeBtn = document.querySelector(`.preset-btn[onclick="applyPreset('${presetKey}')"]`);
  if (activeBtn) {
    activeBtn.classList.remove("btn-outline-secondary");
    activeBtn.classList.add("btn-primary");
  }

  showSettingsAlert(`Preset "${presetKey}" aplicado. Ajusta los prompts según necesites y guarda.`, "info");
}

// ── Category keyword chips ───────────────────────────────────────────────────

let _catKeywords = [];
let _catFallbackTopics = [];
let _catPreferredFormats = [];
let _catPreferredVisualStyles = [];
let _catForbiddenPhrases = [];
let _catVoiceExamples = [];

function addCatKeyword() {
  const input = document.getElementById("cat-keyword-input");
  if (!input) return;
  const val = input.value.trim().toLowerCase();
  if (!val || _catKeywords.includes(val)) { input.value = ""; return; }
  _catKeywords.push(val);
  renderCatKeywords();
  input.value = "";
}

function togglePreferredFormat(format) {
  if (_catPreferredFormats.includes(format)) {
    _catPreferredFormats = _catPreferredFormats.filter(item => item !== format);
  } else {
    _catPreferredFormats.push(format);
  }
  renderPreferredFormatButtons();
}

function renderPreferredFormatButtons() {
  document.querySelectorAll(".pref-format-btn").forEach((btn) => {
    const key = btn.dataset.format || "";
    const active = _catPreferredFormats.includes(key);
    btn.classList.toggle("btn-primary", active);
    btn.classList.toggle("btn-outline-secondary", !active);
  });
}

function togglePreferredVisualStyle(style) {
  if (_catPreferredVisualStyles.includes(style)) {
    _catPreferredVisualStyles = _catPreferredVisualStyles.filter(item => item !== style);
  } else {
    _catPreferredVisualStyles.push(style);
  }
  renderPreferredVisualStyleButtons();
}

function renderPreferredVisualStyleButtons() {
  document.querySelectorAll(".pref-style-btn").forEach((btn) => {
    const key = btn.dataset.style || "";
    const active = _catPreferredVisualStyles.includes(key);
    btn.classList.toggle("btn-primary", active);
    btn.classList.toggle("btn-outline-secondary", !active);
  });
}

function removeCatKeyword(kw) {
  _catKeywords = _catKeywords.filter(k => k !== kw);
  renderCatKeywords();
}

function renderCatKeywords() {
  const el = document.getElementById("cat-keywords-list");
  if (!el) return;
  el.innerHTML = _catKeywords.map(kw =>
    `<span class="badge bg-primary d-flex align-items-center gap-1" style="font-size:12px">
      <i class="bi bi-tag me-1"></i>${escapeHtml(kw)}
      <button type="button" class="btn-close btn-close-white ms-1"
              style="font-size:8px" onclick="removeCatKeyword('${escapeHtml(kw)}')"></button>
    </span>`
  ).join("");
}

function addCatFallback() {
  const input = document.getElementById("cat-fallback-input");
  if (!input) return;
  const val = input.value.trim();
  if (!val || _catFallbackTopics.includes(val)) { input.value = ""; return; }
  _catFallbackTopics.push(val);
  renderCatFallbacks();
  input.value = "";
}

function removeCatFallback(idx) {
  _catFallbackTopics.splice(idx, 1);
  renderCatFallbacks();
}

function renderCatFallbacks() {
  const el = document.getElementById("cat-fallback-list");
  if (!el) return;
  el.innerHTML = _catFallbackTopics.map((topic, idx) =>
    `<span class="badge bg-info text-dark d-flex align-items-center gap-1" style="font-size:12px;max-width:280px;white-space:normal;text-align:left">
      <i class="bi bi-bookmark me-1"></i>${escapeHtml(topic)}
      <button type="button" class="btn-close ms-1"
              style="font-size:8px" onclick="removeCatFallback(${idx})"></button>
    </span>`
  ).join("");
}

function addCatForbidden() {
  const input = document.getElementById("cat-forbidden-input");
  if (!input) return;
  const val = input.value.trim();
  if (!val || _catForbiddenPhrases.includes(val)) { input.value = ""; return; }
  _catForbiddenPhrases.push(val);
  renderCatForbidden();
  input.value = "";
}

function removeCatForbidden(idx) {
  _catForbiddenPhrases.splice(idx, 1);
  renderCatForbidden();
}

function renderCatForbidden() {
  const el = document.getElementById("cat-forbidden-list");
  if (!el) return;
  el.innerHTML = _catForbiddenPhrases.map((phrase, idx) =>
    `<span class="badge bg-danger d-flex align-items-center gap-1" style="font-size:12px;max-width:280px;white-space:normal;text-align:left">
      <i class="bi bi-x-octagon me-1"></i>${escapeHtml(phrase)}
      <button type="button" class="btn-close btn-close-white ms-1"
              style="font-size:8px" onclick="removeCatForbidden(${idx})"></button>
    </span>`
  ).join("");
}

function addCatVoiceExample() {
  const input = document.getElementById("cat-voice-input");
  if (!input) return;
  const val = input.value.trim();
  if (!val || _catVoiceExamples.includes(val)) { input.value = ""; return; }
  if (_catVoiceExamples.length >= 5) {
    showSettingsAlert("Máximo 5 ejemplos de voz por categoría.", "warning");
    return;
  }
  _catVoiceExamples.push(val);
  renderCatVoiceExamples();
  input.value = "";
}

function removeCatVoiceExample(idx) {
  _catVoiceExamples.splice(idx, 1);
  renderCatVoiceExamples();
}

function renderCatVoiceExamples() {
  const el = document.getElementById("cat-voice-list");
  if (!el) return;
  el.innerHTML = _catVoiceExamples.map((example, idx) =>
    `<div class="border rounded p-2 d-flex align-items-start gap-2" style="font-size:12px;background:#f8f9fa">
      <i class="bi bi-chat-quote text-success mt-1"></i>
      <div class="flex-grow-1" style="white-space:pre-wrap">${escapeHtml(example)}</div>
      <button type="button" class="btn-close" style="font-size:10px"
              onclick="removeCatVoiceExample(${idx})"></button>
    </div>`
  ).join("");
}

function adjustHashtags(delta) {
  const slider = document.getElementById("category-hashtag-count");
  if (!slider) return;
  const newVal = Math.max(0, Math.min(10, parseInt(slider.value) + delta));
  slider.value = newVal;
  document.getElementById("hashtag-count-val").textContent = newVal;
}

function duplicateCategory() {
  const name = document.getElementById("category-name")?.value;
  if (!name) { showSettingsAlert("No hay categoría seleccionada para duplicar.", "warning"); return; }

  // Clear ID so it saves as new
  const idEl = document.getElementById("category-id");
  if (idEl) idEl.value = "";

  // Append " (copia)" to name
  const nameEl = document.getElementById("category-name");
  if (nameEl) nameEl.value = name + " (copia)";

  // Uncheck default
  const defEl = document.getElementById("category-default");
  if (defEl) defEl.checked = false;

  showSettingsAlert("Formulario listo para guardar como nueva categoría. Ajusta el nombre y guarda.", "info");
}

// ── Schedule ────────────────────────────────────────────────────────────────

let _schedPollInterval = null;
let _schedTimes = [];
let _schedDays = [];  // [] = all days; otherwise list of ints 0=Mon..6=Sun
let _scheduleCategory = "";
let _schedRules = [];  // list of {days: int[], times: string[], category: string}

const _DAY_LABELS = ["Lun", "Mar", "Mié", "Jue", "Vie", "Sáb", "Dom"];

async function loadSchedule() {
  if (!document.getElementById("schedule-card")) return;
  try {
    const res = await apiFetch("/api/schedule");
    const data = await res.json();
    renderSchedule(data);
  } catch { /* ignore */ }
}

function renderSchedule(data) {
  if (!document.getElementById("schedule-card")) return;
  const cfg = data.config || {};
  const runs = data.recent_runs || [];
  const cur = data.current_run || {};
  _scheduleCategory = cfg.category_name || _scheduleCategory || getDefaultCategoryName();
  renderScheduleCategorySelect();

  // Enabled toggle
  document.getElementById("sched-enabled").checked = !!cfg.enabled;
  updateSchedBadge(!!cfg.enabled, cur.status === "running");

  // Mode
  const mode = cfg.mode || "interval";
  document.querySelector(`input[name="sched-mode"][value="${mode}"]`).checked = true;
  showSchedMode(mode);

  // Interval
  const hours = cfg.interval_hours || 24;
  document.getElementById("sched-interval-range").value = hours;
  document.getElementById("sched-interval-val").textContent = hours + "h";

  // Times
  _schedTimes = cfg.times_of_day || [];
  renderSchedTimes();

  // Days of week
  _schedDays = cfg.days_of_week || [];
  renderSchedDays();

  // Rules (advanced mode)
  _schedRules = Array.isArray(cfg.rules) ? cfg.rules : [];
  renderSchedRules();

  // Next / last run
  document.getElementById("sched-next-run").textContent = cfg.next_run_at
    ? fmtDate(cfg.next_run_at) : "—";
  document.getElementById("sched-last-run").textContent = cfg.last_run_at
    ? fmtDate(cfg.last_run_at) : "—";

  // Current run message
  const msgEl = document.getElementById("sched-current-msg");
  const msgText = document.getElementById("sched-current-msg-text");
  if (cur.status === "running") {
    msgEl.classList.remove("d-none");
    msgText.textContent = cur.message || "Ejecutando...";
  } else {
    msgEl.classList.add("d-none");
  }

  // Recent runs
  const listEl = document.getElementById("sched-runs-list");
  if (!runs.length) {
    listEl.innerHTML = '<p class="text-muted small mb-0">Sin ejecuciones aún.</p>';
  } else {
    listEl.innerHTML = runs.map(r => {
      const icon = r.status === "done"
        ? '<i class="bi bi-check-circle-fill text-success me-1"></i>'
        : r.status === "error"
        ? '<i class="bi bi-x-circle-fill text-danger me-1"></i>'
        : '<span class="spinner-border spinner-border-sm me-1"></span>';
      return `<div class="d-flex align-items-start gap-2 mb-1 small">
        ${icon}
        <div>
          <span class="text-muted">${fmtDate(r.started_at)}</span>
          ${r.topic ? `<span class="badge bg-secondary ms-1">${escapeHtml(r.topic)}</span>` : ""}
          ${r.message ? `<span class="text-muted ms-1">${escapeHtml(r.message.substring(0, 60))}</span>` : ""}
        </div>
      </div>`;
    }).join("");
  }

  // Auto-poll while running
  if (cur.status === "running") {
    if (!_schedPollInterval) {
      _schedPollInterval = setInterval(loadSchedule, 3000);
    }
  } else {
    if (_schedPollInterval) {
      clearInterval(_schedPollInterval);
      _schedPollInterval = null;
    }
  }
}

function updateSchedBadge(enabled, running) {
  const badge = document.getElementById("sched-status-badge");
  if (!badge) return;
  if (running) {
    badge.className = "badge bg-warning text-dark";
    badge.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Ejecutando';
  } else if (enabled) {
    badge.className = "badge bg-success";
    badge.textContent = "Activo";
  } else {
    badge.className = "badge bg-secondary";
    badge.textContent = "Desactivado";
  }
}

function onScheduleEnabledChange() {
  saveSchedule();
}

function onSchedModeChange() {
  const selected = document.querySelector("input[name='sched-mode']:checked");
  if (!selected) return;
  const mode = selected.value;
  showSchedMode(mode);
}

function showSchedMode(mode) {
  if (!document.getElementById("sched-interval-section")) return;
  const isRules = mode === "rules";
  document.getElementById("sched-interval-section").classList.toggle("d-none", mode !== "interval");
  document.getElementById("sched-times-section").classList.toggle("d-none", mode !== "times");
  const rulesSection = document.getElementById("sched-rules-section");
  if (rulesSection) rulesSection.classList.toggle("d-none", !isRules);
  // Hide global category + days selectors when using rules (each rule has its own)
  const catSection = document.getElementById("sched-category-section");
  if (catSection) catSection.classList.toggle("d-none", isRules);
  const daysSection = document.getElementById("sched-days-section");
  if (daysSection) daysSection.classList.toggle("d-none", isRules);
}

function renderScheduleCategorySelect() {
  const select = document.getElementById("sched-category");
  if (!select) return;

  select.disabled = false;
  select.innerHTML = [
    '<option value="random">random · cualquier categoría</option>',
    ...categoryStore.map(cat => `<option value="${escapeHtml(cat.name)}">${escapeHtml(cat.name)}</option>`),
  ].join("");

  const selectedCategory = _scheduleCategory && Array.from(select.options).some(option => option.value === _scheduleCategory)
    ? _scheduleCategory
    : getDefaultCategoryName();
  select.value = selectedCategory || "random";
  _scheduleCategory = select.value;
}

function _categoryOptionsHtml(selected) {
  const safeSelected = selected || "random";
  const options = [
    `<option value="random"${safeSelected === "random" ? " selected" : ""}>random · cualquier categoría</option>`,
    ...categoryStore.map(cat => {
      const name = escapeHtml(cat.name);
      const sel = cat.name === safeSelected ? " selected" : "";
      return `<option value="${name}"${sel}>${name}</option>`;
    }),
  ];
  return options.join("");
}

function renderSchedRules() {
  const list = document.getElementById("sched-rules-list");
  if (!list) return;
  if (!_schedRules.length) {
    list.innerHTML = '<p class="text-muted small mb-0">Sin reglas. Pulsa "Agregar regla" para empezar.</p>';
    return;
  }
  list.innerHTML = _schedRules.map((rule, idx) => {
    const days = Array.isArray(rule.days) ? rule.days : [];
    const times = Array.isArray(rule.times) ? rule.times : [];
    const category = rule.category || "random";
    const dayBtns = _DAY_LABELS.map((label, dayIdx) => {
      const active = days.includes(dayIdx);
      return `<button type="button" class="btn btn-sm ${active ? "btn-primary" : "btn-outline-secondary"}" style="min-width:42px"
        onclick="toggleSchedRuleDay(${idx}, ${dayIdx})">${label}</button>`;
    }).join("");
    const timeChips = times.map(t => `
      <span class="badge bg-primary d-flex align-items-center gap-1" style="font-size:13px">
        <i class="bi bi-clock me-1"></i>${escapeHtml(t)}
        <button type="button" class="btn-close btn-close-white ms-1" style="font-size:9px"
          onclick="removeSchedRuleTime(${idx}, '${escapeHtml(t)}')"></button>
      </span>`).join("");
    return `
      <div class="border rounded p-3" data-rule-index="${idx}">
        <div class="d-flex align-items-center justify-content-between mb-2">
          <span class="fw-semibold small">Regla #${idx + 1}</span>
          <button class="btn btn-sm btn-outline-danger" onclick="removeSchedRule(${idx})">
            <i class="bi bi-trash"></i>
          </button>
        </div>
        <div class="mb-2">
          <label class="form-label small mb-1">Categoría</label>
          <select class="form-select form-select-sm" onchange="updateSchedRuleCategory(${idx}, this.value)">
            ${_categoryOptionsHtml(category)}
          </select>
        </div>
        <div class="mb-2">
          <label class="form-label small mb-1">Días <small class="text-muted">(vacío = todos)</small></label>
          <div class="d-flex flex-wrap gap-1">${dayBtns}</div>
        </div>
        <div>
          <label class="form-label small mb-1">Horas (UTC)</label>
          <div class="d-flex flex-wrap gap-2 mb-2">${timeChips || '<small class="text-muted">Aún sin horas</small>'}</div>
          <div class="d-flex gap-2">
            <input type="time" class="form-control form-control-sm" id="sched-rule-time-input-${idx}" style="width:130px">
            <button class="btn btn-sm btn-outline-secondary" onclick="addSchedRuleTime(${idx})">
              <i class="bi bi-plus-lg"></i> Agregar
            </button>
          </div>
        </div>
      </div>`;
  }).join("");
}

function addSchedRule() {
  _schedRules = [..._schedRules, { days: [], times: [], category: "random" }];
  renderSchedRules();
  saveSchedule();
}

function removeSchedRule(idx) {
  _schedRules = _schedRules.filter((_, i) => i !== idx);
  renderSchedRules();
  saveSchedule();
}

function toggleSchedRuleDay(idx, day) {
  const rule = _schedRules[idx];
  if (!rule) return;
  const days = Array.isArray(rule.days) ? [...rule.days] : [];
  if (days.includes(day)) {
    rule.days = days.filter(d => d !== day);
  } else {
    rule.days = [...days, day].sort();
  }
  renderSchedRules();
  saveSchedule();
}

function addSchedRuleTime(idx) {
  const input = document.getElementById(`sched-rule-time-input-${idx}`);
  if (!input) return;
  const val = input.value;
  if (!val) return;
  const rule = _schedRules[idx];
  if (!rule) return;
  const times = Array.isArray(rule.times) ? [...rule.times] : [];
  if (times.includes(val)) return;
  times.push(val);
  times.sort();
  rule.times = times;
  input.value = "";
  renderSchedRules();
  saveSchedule();
}

function removeSchedRuleTime(idx, time) {
  const rule = _schedRules[idx];
  if (!rule) return;
  rule.times = (rule.times || []).filter(t => t !== time);
  renderSchedRules();
  saveSchedule();
}

function updateSchedRuleCategory(idx, value) {
  const rule = _schedRules[idx];
  if (!rule) return;
  rule.category = value || "random";
  saveSchedule();
}

function addScheduleTime() {
  const input = document.getElementById("sched-time-input");
  if (!input) return;
  const val = input.value;
  if (!val || _schedTimes.includes(val)) return;
  _schedTimes.push(val);
  _schedTimes.sort();
  renderSchedTimes();
  input.value = "";
}

function removeScheduleTime(t) {
  _schedTimes = _schedTimes.filter(x => x !== t);
  renderSchedTimes();
}

function renderSchedDays() {
  const row = document.getElementById("sched-days-row");
  if (!row) return;
  row.innerHTML = _DAY_LABELS.map((label, i) => {
    const active = _schedDays.includes(i);
    return `<button type="button"
      class="btn btn-sm ${active ? "btn-primary" : "btn-outline-secondary"}"
      style="min-width:48px"
      onclick="toggleSchedDay(${i})">${label}</button>`;
  }).join("");
}

function toggleSchedDay(dayIndex) {
  if (_schedDays.includes(dayIndex)) {
    _schedDays = _schedDays.filter(d => d !== dayIndex);
  } else {
    _schedDays = [..._schedDays, dayIndex].sort();
  }
  renderSchedDays();
}

function renderSchedTimes() {
  const el = document.getElementById("sched-times-list");
  if (!el) return;
  el.innerHTML = _schedTimes.map(t =>
    `<span class="badge bg-primary d-flex align-items-center gap-1" style="font-size:13px">
      <i class="bi bi-clock me-1"></i>${t}
      <button type="button" class="btn-close btn-close-white ms-1"
              style="font-size:9px" onclick="removeScheduleTime('${t}')"></button>
    </span>`
  ).join("");
}

async function saveSchedule() {
  if (!document.getElementById("sched-enabled")) return;
  const enabled = document.getElementById("sched-enabled").checked;
  const selected = document.querySelector("input[name='sched-mode']:checked");
  if (!selected) return;
  const mode = selected.value;
  const interval_hours = parseInt(document.getElementById("sched-interval-range").value);
  const times_of_day = [..._schedTimes];
  const days_of_week = [..._schedDays];
  const category_name = document.getElementById("sched-category")?.value || "";
  _scheduleCategory = category_name;
  const rules = _schedRules.map(r => ({
    days: Array.isArray(r.days) ? r.days : [],
    times: Array.isArray(r.times) ? r.times : [],
    category: r.category || "random",
  }));

  try {
    const res = await apiFetch("/api/schedule", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ enabled, mode, interval_hours, times_of_day, days_of_week, category_name, rules }),
    });
    const data = await res.json();
    if (!res.ok || data.error) {
      showGlobalError(data.error || "No se pudo guardar la configuración del scheduler.");
      return;
    }
    if (data.next_run_at) {
      document.getElementById("sched-next-run").textContent = fmtDate(data.next_run_at);
    }
    updateSchedBadge(enabled, false);
  } catch {
    showGlobalError("Error de red al guardar la configuración del scheduler.");
  }
}

async function scheduleRunNow() {
  const btn = document.getElementById("sched-run-now-btn");
  if (!btn) return;
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Iniciando...';
  try {
    const res = await apiFetch("/api/schedule/run_now", { method: "POST" });
    const data = await res.json();
    if (!res.ok || data.error) {
      showGlobalError(data.error || "No se pudo iniciar la ejecución manual.");
      return;
    }
    await loadSchedule();
    _schedPollInterval = setInterval(loadSchedule, 3000);
  } catch {
    showGlobalError("Error de red al lanzar la ejecución manual.");
  }
  setTimeout(() => {
    btn.disabled = false;
    btn.innerHTML = '<i class="bi bi-play-fill me-1"></i>Ejecutar ahora';
  }, 3000);
}

function fmtDate(iso) {
  if (!iso) return "—";
  try {
    return new Date(iso.endsWith("Z") ? iso : iso + "Z").toLocaleString("es-ES", {
      day: "2-digit", month: "short", year: "numeric",
      hour: "2-digit", minute: "2-digit"
    });
  } catch { return iso; }
}
