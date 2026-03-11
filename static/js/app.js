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
let categoryStore = [];
let currentCategoryId = null;

// ── Init ───────────────────────────────────────────────────────────────────

document.addEventListener("DOMContentLoaded", () => {
  if (document.getElementById("auth-status-text")) checkAuthStatus();
  if (document.getElementById("history-container")) loadHistory();
  if (document.getElementById("headless-toggle")) loadHeadlessSetting();
  if (document.getElementById("schedule-card")) loadSchedule();
  if (document.getElementById("pipeline-category") || document.getElementById("category-list")) loadCategories();

  const textarea = document.getElementById("post-textarea");
  if (textarea) textarea.addEventListener("input", updateCharCount);
});

// ── Headless toggle ─────────────────────────────────────────────────────────

async function loadHeadlessSetting() {
  try {
    const res = await fetch("/api/headless");
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
    await fetch("/api/headless", {
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
    const res = await fetch("/auth/status");
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
    res = await fetch("/auth/login", { method: "POST" });
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
  await fetch("/auth/disconnect", { method: "POST" });
  checkAuthStatus();
}

// ── Pipeline ───────────────────────────────────────────────────────────────

function startPipeline() {
  fetch("/auth/status").then(r => r.json()).then(data => {
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
  resetUI();
  document.getElementById("steps-panel").classList.remove("d-none");

  const activeBtn = testMode ? "test-btn" : "generate-btn";
  document.getElementById(activeBtn).disabled = true;
  document.getElementById(activeBtn).innerHTML =
    '<span class="spinner-border spinner-border-sm me-2"></span>Generando...';

  _openPipelineSSE(1);
}

function rerunFromStep(step) {
  if (!currentSessionId) return;

  // Hide preview, reset steps from this point forward
  document.getElementById("preview-panel").classList.add("d-none");
  document.getElementById("publish-success").classList.add("d-none");
  document.getElementById("publish-error").classList.add("d-none");
  document.getElementById("publish-progress").classList.add("d-none");
  for (let i = step; i <= 5; i++) {
    document.getElementById(`icon-${i}`).innerHTML = '<i class="bi bi-circle text-muted"></i>';
    const d = document.getElementById(`detail-${i}`);
    d.textContent = "";
    d.className = "step-detail text-muted small";
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
    return;
  }

  const { step, status } = data;

  // Global pipeline error (e.g. no session)
  if (step === 0 && status === "error") {
    showGlobalError(data.message);
    resetAllBtns();
    eventSource.close();
    return;
  }

  if (status === "running") {
    setStepRunning(step, data.message);
  } else if (status === "done") {
    setStepDone(step, data.result);
    // Show per-step regen button for steps 1-4 once they're done
    if (step >= 1 && step <= 4) {
      const rb = document.getElementById(`regen-${step}`);
      if (rb) rb.classList.remove("d-none");
    }
  } else if (status === "error") {
    setStepError(step, data.message);
    resetAllBtns();
    eventSource.close();
  } else if (status === "preview") {
    if (!data.test_mode) currentSessionId = data.session_id;
    setStepDone(5, data.test_mode ? "Vista previa generada (modo prueba)" : "Listo para publicar");
    showPreviewPanel(data);
    resetAllBtns();
    document.getElementById("regenerate-btn").classList.remove("d-none");
    eventSource.close();
  }
}

// ── Step UI helpers ────────────────────────────────────────────────────────

function setStepRunning(step, message) {
  document.getElementById(`icon-${step}`).innerHTML =
    '<span class="spinner-border spinner-border-sm text-primary"></span>';
  const detail = document.getElementById(`detail-${step}`);
  detail.textContent = message;
  detail.className = "step-detail text-primary small";
}

function setStepDone(step, result) {
  document.getElementById(`icon-${step}`).innerHTML =
    '<i class="bi bi-check-circle-fill text-success fs-5"></i>';
  const detail = document.getElementById(`detail-${step}`);
  let summary = "";
  if (Array.isArray(result)) {
    summary = `${result.length} temas: ${result.slice(0, 3).join(", ")}${result.length > 3 ? "..." : ""}`;
  } else if (typeof result === "object" && result !== null) {
    if (result.topic) summary = `Tema: ${result.topic}`;
    else if (result.image_url) summary = "Imagen generada correctamente";
    else summary = Object.values(result).join(" · ").substring(0, 80);
  } else {
    summary = String(result || "Completado");
  }
  detail.textContent = summary;
  detail.className = "step-detail text-success small";
}

function setStepError(step, message) {
  document.getElementById(`icon-${step}`).innerHTML =
    '<i class="bi bi-x-circle-fill text-danger fs-5"></i>';
  const detail = document.getElementById(`detail-${step}`);
  detail.textContent = message;
  detail.className = "step-detail text-danger small";
}

// ── Preview panel ──────────────────────────────────────────────────────────

function showPreviewPanel(data) {
  const panel = document.getElementById("preview-panel");
  if (!panel) return;
  panel.classList.remove("d-none");
  panel.scrollIntoView({ behavior: "smooth", block: "start" });

  document.getElementById("preview-image").src = data.image_url;
  document.getElementById("post-textarea").value = data.post_text;
  document.getElementById("preview-topic-badge").textContent = data.topic;
  if (data.category) {
    document.getElementById("preview-topic-badge").textContent = `${data.category} · ${data.topic}`;
  }

  if (data.reasoning) {
    document.getElementById("reasoning-text").textContent = data.reasoning;
    document.getElementById("reasoning-box").classList.remove("d-none");
  }

  // Test mode: show warning badge, hide publish button
  const testBadge = document.getElementById("test-mode-badge");
  const publishBtn = document.getElementById("publish-btn");
  if (data.test_mode) {
    testBadge.classList.remove("d-none");
    publishBtn.classList.add("d-none");
  } else {
    testBadge.classList.add("d-none");
    publishBtn.classList.remove("d-none");
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
    res = await fetch("/api/publish", {
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
      const statusRes = await fetch(`/api/publish_status/${jobId}`);
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
    const res = await fetch("/api/history");
    const data = await res.json();
    const posts = [...data.posts].reverse();

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
  }
}

// ── Utility ────────────────────────────────────────────────────────────────

function resetUI() {
  currentSessionId = null;
  if (!document.getElementById("preview-panel")) return;
  document.getElementById("preview-panel").classList.add("d-none");
  document.getElementById("regenerate-btn").classList.add("d-none");
  document.getElementById("publish-success").classList.add("d-none");
  document.getElementById("publish-error").classList.add("d-none");
  document.getElementById("publish-progress").classList.add("d-none");
  document.getElementById("publish-btn").disabled = false;
  document.getElementById("publish-btn").classList.remove("d-none");
  document.getElementById("publish-btn").innerHTML = '<i class="bi bi-linkedin me-2"></i>Publicar en LinkedIn';
  document.getElementById("reasoning-box").classList.add("d-none");
  document.getElementById("test-mode-badge").classList.add("d-none");
  document.getElementById("screenshots-section").classList.add("d-none");
  document.getElementById("screenshots-grid").innerHTML = "";
  document.getElementById("screenshots-count").textContent = "";
  for (let i = 1; i <= 5; i++) {
    document.getElementById(`icon-${i}`).innerHTML = '<i class="bi bi-circle text-muted"></i>';
    const d = document.getElementById(`detail-${i}`);
    d.textContent = "";
    d.className = "step-detail text-muted small";
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

function regenerate() {
  document.getElementById("regenerate-btn").classList.add("d-none");
  runPipeline();
}

function showGlobalError(msg) {
  const alert = document.createElement("div");
  alert.className = "alert alert-danger alert-dismissible mt-3";
  alert.innerHTML = `<i class="bi bi-exclamation-triangle-fill me-2"></i>${escapeHtml(msg)}
    <button type="button" class="btn-close" data-bs-dismiss="alert"></button>`;
  document.querySelector("main").prepend(alert);
}

function escapeHtml(str) {
  const div = document.createElement("div");
  div.appendChild(document.createTextNode(str));
  return div.innerHTML;
}

// ── Pipeline categories ─────────────────────────────────────────────────────

async function loadCategories() {
  try {
    const res = await fetch("/api/categories");
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

function renderCategorySelect(defaultCategory) {
  const select = document.getElementById("pipeline-category");
  if (!select) return;

  if (!categoryStore.length) {
    select.innerHTML = '<option value="">Sin categorías</option>';
    return;
  }

  select.innerHTML = categoryStore.map(cat =>
    `<option value="${escapeHtml(cat.name)}">${escapeHtml(cat.name)}${cat.is_default ? " · default" : ""}</option>`
  ).join("");
  select.value = defaultCategory || categoryStore[0].name;
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
  };

  try {
    const res = await fetch("/api/categories", {
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
    const res = await fetch(`/api/categories/${id}`, { method: "DELETE" });
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

// ── Schedule ────────────────────────────────────────────────────────────────

let _schedPollInterval = null;
let _schedTimes = [];
let _schedDays = [];  // [] = all days; otherwise list of ints 0=Mon..6=Sun

const _DAY_LABELS = ["Lun", "Mar", "Mié", "Jue", "Vie", "Sáb", "Dom"];

async function loadSchedule() {
  if (!document.getElementById("schedule-card")) return;
  try {
    const res = await fetch("/api/schedule");
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
    const res = await fetch("/api/schedule", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ enabled, mode, interval_hours, times_of_day, days_of_week }),
    });
    const data = await res.json();
    if (data.next_run_at) {
      document.getElementById("sched-next-run").textContent = fmtDate(data.next_run_at);
    }
    updateSchedBadge(enabled, false);
  } catch { /* ignore */ }
}

async function scheduleRunNow() {
  const btn = document.getElementById("sched-run-now-btn");
  if (!btn) return;
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Iniciando...';
  try {
    await fetch("/api/schedule/run_now", { method: "POST" });
    await loadSchedule();
    _schedPollInterval = setInterval(loadSchedule, 3000);
  } catch { /* ignore */ }
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
