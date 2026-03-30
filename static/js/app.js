/**
 * LinkedIn Auto-Poster — Frontend Logic
 */

// ── State ──────────────────────────────────────────────────────────────────

let currentSessionId = null;
let currentTestMode = false;
let currentCategory = "";
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
  if (document.getElementById("headless-toggle")) loadHeadlessSetting();
  if (document.getElementById("schedule-card")) loadSchedule();
  if (document.getElementById("pipeline-category") || document.getElementById("category-list")) await loadCategories();

  const pipelineCategorySelect = document.getElementById("pipeline-category");
  if (pipelineCategorySelect) {
    pipelineCategorySelect.addEventListener("change", () => {
      currentCategory = getSelectedCategoryName();
      renderSelectedCategorySummary(currentCategory);
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
  stopPipelineStatePolling();
  resetUI();
  const stepsPanel = document.getElementById("steps-panel");
  if (stepsPanel) stepsPanel.classList.remove("d-none");

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
  for (let i = step; i <= 7; i++) {
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
    if (step >= 1 && step <= 6) {
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
    setStepDone(7, data.test_mode ? "Vista previa generada (modo prueba)" : "Listo para publicar");
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
  if (data.category) {
    if (topicBadge) topicBadge.textContent = `${data.category} · ${data.topic}`;
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

// ── Utility ────────────────────────────────────────────────────────────────

function resetUI() {
  currentSessionId = null;
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
  currentCategory = state.category || currentCategory || getSelectedCategoryName();
  persistPipelineSessionState();

  const select = document.getElementById("pipeline-category");
  if (select && currentCategory && Array.from(select.options).some(option => option.value === currentCategory)) {
    select.value = currentCategory;
    renderSelectedCategorySummary(currentCategory);
  }

  resetUI();
  const stepsPanel = document.getElementById("steps-panel");
  if (stepsPanel) stepsPanel.classList.remove("d-none");

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
      setStepDone(7, event.test_mode ? "Vista previa generada (modo prueba)" : "Listo para publicar");
    }
  }

  if (state.preview) {
    hasPreview = true;
    setStepDone(7, state.preview.test_mode ? "Vista previa generada (modo prueba)" : "Listo para publicar");
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

function escapeHtml(str) {
  const div = document.createElement("div");
  div.appendChild(document.createTextNode(str));
  return div.innerHTML;
}

// ── Pipeline categories ─────────────────────────────────────────────────────

async function loadCategories() {
  try {
    const res = await apiFetch("/api/categories");
    const data = await res.json();
    categoryStore = data.categories || [];
    renderCategorySelect(data.default_category || "");
    renderCategorySettings(data.default_category || "");
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

function renderCategorySelect(defaultCategory) {
  const select = document.getElementById("pipeline-category");
  if (!select) return;

  if (!categoryStore.length) {
    select.innerHTML = '<option value="">Sin categorías</option>';
    currentCategory = "";
    renderSelectedCategorySummary("");
    return;
  }

  select.innerHTML = categoryStore.map(cat =>
    `<option value="${escapeHtml(cat.name)}">${escapeHtml(cat.name)}</option>`
  ).join("");
  const selectedCategory = getCategoryByName(defaultCategory) || categoryStore[0];
  select.value = selectedCategory.name;
  currentCategory = select.value;
  renderSelectedCategorySummary(select.value);
}

function renderSelectedCategorySummary(categoryName) {
  const nameEl = document.getElementById("pipeline-category-name");
  const descriptionEl = document.getElementById("pipeline-category-description");
  const badgeEl = document.getElementById("pipeline-category-default-badge");
  if (!nameEl || !descriptionEl || !badgeEl) return;

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
  document.getElementById("sched-interval-section").classList.toggle("d-none", mode !== "interval");
  document.getElementById("sched-times-section").classList.toggle("d-none", mode !== "times");
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

  try {
    const res = await apiFetch("/api/schedule", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ enabled, mode, interval_hours, times_of_day, days_of_week }),
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
