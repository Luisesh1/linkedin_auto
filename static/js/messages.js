let messageThreads = [];
let selectedThreadId = null;
let currentMessageThread = null;
let calendarBlocks = [];
let messagePagePoll = null;
let messageSearchDebounce = null;

const MESSAGE_WEEKDAYS = [
  { value: 0, label: "Lunes" },
  { value: 1, label: "Martes" },
  { value: 2, label: "Miercoles" },
  { value: 3, label: "Jueves" },
  { value: 4, label: "Viernes" },
  { value: 5, label: "Sabado" },
  { value: 6, label: "Domingo" },
];

document.addEventListener("DOMContentLoaded", () => {
  if (document.body.dataset.page !== "messages") return;
  initMessagesPage();
});

window.addEventListener("beforeunload", () => {
  if (messagePagePoll) clearInterval(messagePagePoll);
});

async function initMessagesPage() {
  const search = document.getElementById("msg-search");
  if (search) {
    search.addEventListener("input", () => {
      clearTimeout(messageSearchDebounce);
      messageSearchDebounce = setTimeout(() => loadInbox(), 250);
    });
  }

  await Promise.all([loadMessageAutomation(), loadAvailability(), loadInbox()]);
  messagePagePoll = setInterval(async () => {
    await Promise.all([loadMessageAutomation(false), loadInbox(false)]);
    if (selectedThreadId) await loadConversation(selectedThreadId, false);
  }, 12000);
}

function safeFmtDate(iso) {
  if (!iso) return "Sin fecha";
  if (typeof fmtDate === "function") return fmtDate(iso);
  try {
    return new Date(iso).toLocaleString("es-ES");
  } catch {
    return iso;
  }
}

function stateBadgeClass(state) {
  if (state === "meeting_booked") return "bg-success-subtle text-success";
  if (state === "awaiting_user") return "bg-warning-subtle text-warning-emphasis";
  if (state === "closed") return "bg-secondary";
  return "bg-info-subtle text-info-emphasis";
}

function intentLabel(intent) {
  const labels = {
    recruiter: "Recruiter",
    meeting: "Reunion",
    networking: "Networking",
    general: "General",
    sensitive: "Sensible",
    unknown: "Por revisar",
  };
  return labels[intent] || "General";
}

function updateMessageRun(run) {
  const el = document.getElementById("msg-current-run");
  if (!el) return;
  const status = run?.status || "idle";
  const message = run?.message || "Sin actividad reciente.";
  const processed = Number(run?.processed || 0);
  el.innerHTML = `
    <div><strong>Estado:</strong> ${escapeHtml(status)}</div>
    <div>${escapeHtml(message)}</div>
    <div>Procesados en el ultimo ciclo: ${processed}</div>
  `;
}

function renderReviewQueue(items) {
  const root = document.getElementById("msg-review-queue");
  if (!root) return;
  if (!items.length) {
    root.innerHTML = '<p class="text-muted small mb-0">Sin items pendientes.</p>';
    return;
  }
  root.innerHTML = items.map((item) => `
    <div class="review-item">
      <div class="d-flex justify-content-between gap-2 align-items-start">
        <div>
          <div class="fw-semibold small">${escapeHtml(item.contact_name || "Contacto")}</div>
          <div class="small text-muted">${escapeHtml(item.reason || "Requiere revision")}</div>
        </div>
        <span class="badge rounded-pill ${stateBadgeClass(item.state)}">${escapeHtml(intentLabel(item.intent))}</span>
      </div>
      ${item.suggested_reply ? `<div class="review-suggestion small mt-2">${escapeHtml(item.suggested_reply)}</div>` : ""}
      <div class="d-flex flex-wrap gap-2 mt-3">
        <button class="btn btn-sm btn-outline-primary" onclick="openThreadFromReview(${item.thread_id})">
          <i class="bi bi-chat-left-text me-1"></i>Abrir hilo
        </button>
        <button class="btn btn-sm btn-success" onclick="resolveReviewItem(${item.id}, 'approved')">
          <i class="bi bi-check2 me-1"></i>Aprobar
        </button>
        <button class="btn btn-sm btn-outline-secondary" onclick="resolveReviewItem(${item.id}, 'dismissed')">
          <i class="bi bi-x-lg me-1"></i>Descartar
        </button>
      </div>
    </div>
  `).join("");
}

function renderBookings(bookings) {
  const root = document.getElementById("calendar-bookings");
  if (!root) return;
  if (!bookings.length) {
    root.innerHTML = '<p class="text-muted small mb-0">Sin reservas.</p>';
    return;
  }
  root.innerHTML = bookings.slice(0, 6).map((booking) => `
    <div class="calendar-booking-item">
      <div class="fw-semibold small">${escapeHtml(booking.contact_name || "Contacto")}</div>
      <div class="small text-muted">${escapeHtml(safeFmtDate(booking.start_at))}</div>
    </div>
  `).join("");
}

async function loadMessageAutomation(showErrors = true) {
  try {
    const res = await apiFetch("/api/messages/automation");
    const data = await res.json();
    const config = data.config || {};
    document.getElementById("msg-enabled").checked = !!config.enabled;
    document.getElementById("msg-interval").value = config.poll_interval_minutes || 5;
    document.getElementById("msg-auto-send").checked = !!config.auto_send_default;
    document.getElementById("msg-public-base-url").value = config.public_base_url || "";
    document.getElementById("msg-meeting-location").value = config.meeting_location || "";
    document.getElementById("msg-sync-limit").value = config.sync_limit || 15;
    document.getElementById("msg-max-threads").value = config.max_threads_per_cycle || 5;
    document.getElementById("msg-booking-token").textContent = config.booking_token || "";
    const statusBadge = document.getElementById("msg-auto-status");
    if (statusBadge) {
      statusBadge.className = `badge ${config.enabled ? "bg-success" : "bg-secondary"}`;
      statusBadge.textContent = config.enabled ? "Activa" : "Desactivada";
    }
    updateMessageRun(data.current_run || {});
    renderReviewQueue(data.review_queue || []);
    renderBookings(data.bookings || []);
  } catch (error) {
    if (showErrors) showGlobalError(error.message || "No se pudo cargar la automatizacion de mensajes.");
  }
}

async function saveMessageAutomation() {
  try {
    const payload = {
      enabled: document.getElementById("msg-enabled").checked,
      poll_interval_minutes: parseInt(document.getElementById("msg-interval").value || "5", 10),
      auto_send_default: document.getElementById("msg-auto-send").checked,
      public_base_url: document.getElementById("msg-public-base-url").value.trim(),
      meeting_location: document.getElementById("msg-meeting-location").value.trim(),
      sync_limit: parseInt(document.getElementById("msg-sync-limit").value || "15", 10),
      max_threads_per_cycle: parseInt(document.getElementById("msg-max-threads").value || "5", 10),
    };
    const res = await apiFetch("/api/messages/automation", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const data = await res.json();
    if (!res.ok || data.error) {
      showGlobalError(data.error || "No se pudo guardar la configuracion.");
      return;
    }
    await loadMessageAutomation(false);
  } catch (error) {
    showGlobalError(error.message || "Error de red al guardar la configuracion.");
  }
}

async function regenerateBookingToken() {
  try {
    const res = await apiFetch("/api/messages/automation/regenerate_booking_token", { method: "POST" });
    const data = await res.json();
    if (!res.ok || data.error) {
      showGlobalError(data.error || "No se pudo regenerar el token.");
      return;
    }
    document.getElementById("msg-booking-token").textContent = data.config.booking_token || "";
  } catch (error) {
    showGlobalError(error.message || "Error regenerando el token.");
  }
}

async function syncMessagesNow() {
  try {
    const res = await apiFetch("/api/messages/sync", { method: "POST" });
    const data = await res.json();
    if (!res.ok || data.error) {
      showGlobalError(data.error || "No se pudo iniciar la sincronizacion.");
      return;
    }
    setTimeout(() => {
      loadMessageAutomation(false);
      loadInbox(false);
    }, 1200);
  } catch (error) {
    showGlobalError(error.message || "Error al sincronizar mensajes.");
  }
}

async function loadInbox(showErrors = true) {
  try {
    const query = document.getElementById("msg-search")?.value.trim() || "";
    const params = new URLSearchParams();
    if (query) params.set("query", query);
    const res = await apiFetch(`/api/messages/inbox?${params.toString()}`);
    const data = await res.json();
    messageThreads = data.threads || [];
    renderThreadList();
    renderReviewQueue(data.review_queue || []);
    if (selectedThreadId && !messageThreads.some((item) => item.id === selectedThreadId)) {
      selectedThreadId = null;
      currentMessageThread = null;
      renderEmptyThreadDetail();
    }
  } catch (error) {
    if (showErrors) showGlobalError(error.message || "No se pudo cargar el inbox.");
  }
}

function renderThreadList() {
  const root = document.getElementById("message-thread-list");
  if (!root) return;
  if (!messageThreads.length) {
    root.innerHTML = '<div class="p-4 text-muted small">No hay conversaciones cargadas todavia.</div>';
    renderSimThreadOptions();
    return;
  }
  root.innerHTML = messageThreads.map((thread) => `
    <button type="button" class="message-thread-row ${thread.id === selectedThreadId ? "active" : ""}" onclick="openMessageThread(${thread.id})">
      <div class="message-thread-row-head">
        <strong>${escapeHtml(thread.contact_name || "Contacto")}</strong>
        <span class="badge rounded-pill ${stateBadgeClass(thread.state)}">${escapeHtml(intentLabel(thread.intent))}</span>
      </div>
      <div class="small text-muted text-start">${escapeHtml(thread.latest_snippet || thread.crm_summary || "Sin snippet reciente")}</div>
      <div class="message-thread-row-meta">
        <span>${escapeHtml(thread.next_action || "Sin accion")}</span>
        <span>${escapeHtml(safeFmtDate(thread.updated_at))}</span>
      </div>
    </button>
  `).join("");
  renderSimThreadOptions();
}

function renderSimThreadOptions() {
  const select = document.getElementById("sim-thread-target");
  if (!select) return;
  const previous = select.value;
  const options = ['<option value="">Crear hilo nuevo</option>'];
  for (const thread of messageThreads) {
    const label = `${thread.contact_name || "Contacto"} — ${intentLabel(thread.intent)}`;
    options.push(`<option value="${thread.id}">${escapeHtml(label)}</option>`);
  }
  select.innerHTML = options.join("");
  if (previous && messageThreads.some((thread) => String(thread.id) === previous)) {
    select.value = previous;
  }
}

function renderEmptyThreadDetail() {
  const root = document.getElementById("message-thread-detail");
  if (!root) return;
  root.innerHTML = '<div class="p-4 text-muted small">Selecciona una conversación para ver el detalle.</div>';
}

async function openMessageThread(threadId) {
  selectedThreadId = threadId;
  renderThreadList();
  await loadConversation(threadId);
}

async function openThreadFromReview(threadId) {
  await openMessageThread(threadId);
}

async function loadConversation(threadId, showErrors = true) {
  try {
    const res = await apiFetch(`/api/messages/conversations/${threadId}`);
    const data = await res.json();
    currentMessageThread = data.thread || null;
    renderThreadDetail(data.thread || {}, data.events || [], data.profile || {});
  } catch (error) {
    if (showErrors) showGlobalError(error.message || "No se pudo cargar la conversación.");
  }
}

function renderThreadDetail(thread, events, profile) {
  const root = document.getElementById("message-thread-detail");
  if (!root) return;
  if (!thread.id) {
    renderEmptyThreadDetail();
    return;
  }
  root.innerHTML = `
    <div class="message-detail-shell">
      <div class="message-detail-header">
        <div>
          <div class="d-flex flex-wrap gap-2 align-items-center mb-2">
            <h2 class="h5 mb-0">${escapeHtml(thread.contact_name || "Contacto")}</h2>
            <span class="badge rounded-pill ${stateBadgeClass(thread.state)}">${escapeHtml(thread.state || "new")}</span>
            <span class="badge rounded-pill bg-light text-dark border">${escapeHtml(intentLabel(thread.intent))}</span>
          </div>
          <p class="text-muted small mb-0">${escapeHtml(profile.summary || thread.crm_summary || "Sin resumen guardado.")}</p>
        </div>
        <div class="d-flex flex-wrap gap-2">
          <button class="btn btn-sm btn-outline-secondary" onclick="togglePauseThread()">
            <i class="bi bi-${thread.paused ? "play" : "pause"}-fill me-1"></i>${thread.paused ? "Reanudar" : "Pausar"}
          </button>
          <button class="btn btn-sm btn-outline-danger" onclick="closeCurrentThread()">
            <i class="bi bi-x-circle me-1"></i>Cerrar
          </button>
        </div>
      </div>

      <div class="message-detail-meta">
        <div><strong>Proximo paso:</strong> ${escapeHtml(thread.next_action || "Sin definir")}</div>
        <div><strong>Ultimo inbound:</strong> ${escapeHtml(safeFmtDate(thread.last_inbound_at || thread.last_message_at))}</div>
        <div><strong>Ultima respuesta automatica:</strong> ${escapeHtml(safeFmtDate(thread.last_auto_reply_at))}</div>
      </div>

      <div class="message-event-list">
        ${events.length ? events.map((event) => renderEventRow(event)).join("") : '<div class="text-muted small">No hay mensajes persistidos aun.</div>'}
      </div>

      <div class="message-reply-box">
        <label class="form-label fw-semibold small">Responder manualmente</label>
        <textarea id="message-reply-text" class="form-control mb-3" rows="4" placeholder="Escribe una respuesta manual para este hilo."></textarea>
        <div class="d-flex flex-wrap justify-content-between gap-2 align-items-center">
          <div class="small text-muted">Cuando envias manualmente, el hilo queda en espera de respuesta del contacto.</div>
          <button class="btn btn-primary" onclick="sendManualReply()">
            <i class="bi bi-send-fill me-2"></i>Enviar respuesta
          </button>
        </div>
      </div>
    </div>
  `;
}

function renderEventRow(event) {
  const role = event.sender_role || "system";
  const bubbleClass = role === "self" ? "self" : role === "contact" ? "contact" : "system";
  const label = role === "self" ? "Yo" : role === "contact" ? "Contacto" : "Sistema";
  return `
    <div class="message-event ${bubbleClass}">
      <div class="message-event-bubble">
        <div class="message-event-head">
          <strong>${escapeHtml(label)}</strong>
          <span>${escapeHtml(safeFmtDate(event.happened_at))}</span>
        </div>
        <div class="message-event-text">${escapeHtml(event.text || "")}</div>
      </div>
    </div>
  `;
}

async function sendManualReply() {
  if (!currentMessageThread?.id) return;
  const textarea = document.getElementById("message-reply-text");
  const text = textarea?.value.trim() || "";
  if (!text) {
    showGlobalError("Escribe una respuesta antes de enviar.");
    return;
  }
  try {
    const res = await apiFetch(`/api/messages/conversations/${currentMessageThread.id}/reply`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ text }),
    });
    const data = await res.json();
    if (!res.ok || data.error) {
      showGlobalError(data.error || "No se pudo enviar la respuesta.");
      return;
    }
    textarea.value = "";
    await Promise.all([loadInbox(false), loadConversation(currentMessageThread.id, false), loadMessageAutomation(false)]);
  } catch (error) {
    showGlobalError(error.message || "Error enviando el mensaje.");
  }
}

async function togglePauseThread() {
  if (!currentMessageThread?.id) return;
  const endpoint = currentMessageThread.paused ? "resume" : "pause";
  try {
    const res = await apiFetch(`/api/messages/conversations/${currentMessageThread.id}/${endpoint}`, { method: "POST" });
    const data = await res.json();
    if (!res.ok || data.error) {
      showGlobalError(data.error || "No se pudo actualizar el estado del hilo.");
      return;
    }
    await Promise.all([loadInbox(false), loadConversation(currentMessageThread.id, false)]);
  } catch (error) {
    showGlobalError(error.message || "Error actualizando el hilo.");
  }
}

async function closeCurrentThread() {
  if (!currentMessageThread?.id) return;
  try {
    const res = await apiFetch(`/api/messages/conversations/${currentMessageThread.id}/close`, { method: "POST" });
    const data = await res.json();
    if (!res.ok || data.error) {
      showGlobalError(data.error || "No se pudo cerrar la conversación.");
      return;
    }
    selectedThreadId = null;
    currentMessageThread = null;
    await loadInbox(false);
    renderEmptyThreadDetail();
  } catch (error) {
    showGlobalError(error.message || "Error cerrando la conversación.");
  }
}

async function resolveReviewItem(reviewId, status) {
  try {
    const res = await apiFetch(`/api/messages/review/${reviewId}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ status }),
    });
    const data = await res.json();
    if (!res.ok || data.error) {
      showGlobalError(data.error || "No se pudo resolver el item.");
      return;
    }
    await Promise.all([loadMessageAutomation(false), loadInbox(false)]);
    if (selectedThreadId) await loadConversation(selectedThreadId, false);
  } catch (error) {
    showGlobalError(error.message || "Error resolviendo el item.");
  }
}

function buildAvailabilityBlock(block = {}) {
  return {
    weekday: Number(block.weekday ?? 0),
    start_time: block.start_time || "09:00",
    end_time: block.end_time || "17:00",
    timezone: block.timezone || Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC",
  };
}

function renderAvailabilityBlocks() {
  const root = document.getElementById("calendar-blocks");
  if (!root) return;
  if (!calendarBlocks.length) {
    root.innerHTML = '<p class="text-muted small mb-0">Sin bloques cargados. Agrega al menos uno para habilitar reservas.</p>';
    return;
  }
  root.innerHTML = calendarBlocks.map((block, index) => `
    <div class="calendar-block-item">
      <div class="row g-2 align-items-end">
        <div class="col-md-4">
          <label class="form-label small fw-semibold">Dia</label>
          <select class="form-select form-select-sm" data-field="weekday" data-index="${index}">
            ${MESSAGE_WEEKDAYS.map((day) => `<option value="${day.value}" ${day.value === Number(block.weekday) ? "selected" : ""}>${day.label}</option>`).join("")}
          </select>
        </div>
        <div class="col-6 col-md-3">
          <label class="form-label small fw-semibold">Inicio</label>
          <input type="time" class="form-control form-control-sm" data-field="start_time" data-index="${index}" value="${escapeHtml(block.start_time)}">
        </div>
        <div class="col-6 col-md-3">
          <label class="form-label small fw-semibold">Fin</label>
          <input type="time" class="form-control form-control-sm" data-field="end_time" data-index="${index}" value="${escapeHtml(block.end_time)}">
        </div>
        <div class="col-md-2 text-md-end">
          <button class="btn btn-sm btn-outline-danger w-100" onclick="removeAvailabilityBlock(${index})">
            <i class="bi bi-trash"></i>
          </button>
        </div>
      </div>
      <div class="mt-2">
        <label class="form-label small fw-semibold">Timezone</label>
        <input type="text" class="form-control form-control-sm" data-field="timezone" data-index="${index}" value="${escapeHtml(block.timezone)}">
      </div>
    </div>
  `).join("");
}

function syncAvailabilityBlocksFromDom() {
  const rows = document.querySelectorAll("#calendar-blocks [data-index]");
  rows.forEach((element) => {
    const index = Number(element.dataset.index);
    const field = element.dataset.field;
    if (!calendarBlocks[index]) return;
    calendarBlocks[index][field] = element.value;
  });
}

async function loadAvailability(showErrors = true) {
  try {
    const res = await apiFetch("/api/calendar/availability");
    const data = await res.json();
    calendarBlocks = (data.blocks || []).map((item) => buildAvailabilityBlock(item));
    renderAvailabilityBlocks();
    renderBookings(data.bookings || []);
  } catch (error) {
    if (showErrors) showGlobalError(error.message || "No se pudo cargar la disponibilidad.");
  }
}

function addAvailabilityBlock() {
  syncAvailabilityBlocksFromDom();
  calendarBlocks.push(buildAvailabilityBlock());
  renderAvailabilityBlocks();
}

function removeAvailabilityBlock(index) {
  syncAvailabilityBlocksFromDom();
  calendarBlocks = calendarBlocks.filter((_, currentIndex) => currentIndex !== index);
  renderAvailabilityBlocks();
}

async function saveAvailability() {
  syncAvailabilityBlocksFromDom();
  try {
    const res = await apiFetch("/api/calendar/availability", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ blocks: calendarBlocks }),
    });
    const data = await res.json();
    if (!res.ok || data.error) {
      showGlobalError(data.error || "No se pudo guardar la disponibilidad.");
      return;
    }
    calendarBlocks = (data.blocks || []).map((item) => buildAvailabilityBlock(item));
    renderAvailabilityBlocks();
    await loadAvailability(false);
  } catch (error) {
    showGlobalError(error.message || "Error guardando la disponibilidad.");
  }
}

// ─── Simulación de mensajes entrantes ───────────────────────────────────────

const SIM_TEMPLATES = {
  recruiter: {
    contact: "Recruiter Jane",
    text: "Hola, soy recruiter en una empresa SaaS B2B. Vi tu perfil y quería compartir una vacante de Senior Engineer. ¿Tienes disponibilidad para una llamada esta semana?",
  },
  meeting: {
    contact: "Carla Operaciones",
    text: "Hola, ¿podemos agendar una reunión la próxima semana para revisar el caso del cliente Acme? Tengo huecos jueves y viernes.",
  },
  networking: {
    contact: "Diego Networking",
    text: "Hola, me encantaría conectar y tomar un café virtual para intercambiar contexto sobre IA aplicada en operaciones. ¿Te animas?",
  },
  sensitive: {
    contact: "HR Internal",
    text: "Hola, queríamos confirmar contigo el rango salarial y los detalles del equity package antes de avanzar con el contrato.",
  },
};

function applySimTemplate(key) {
  const tpl = SIM_TEMPLATES[key];
  if (!tpl) return;
  const nameEl = document.getElementById("sim-contact-name");
  const textEl = document.getElementById("sim-message-text");
  if (nameEl && !nameEl.value.trim()) nameEl.value = tpl.contact;
  if (textEl) textEl.value = tpl.text;
}

function renderSimResult(result, status) {
  const root = document.getElementById("sim-result");
  if (!root) return;
  if (status === "loading") {
    root.innerHTML = '<div class="text-muted"><i class="bi bi-hourglass-split me-1"></i>Procesando mensaje simulado...</div>';
    return;
  }
  if (status === "error") {
    root.innerHTML = `<div class="alert alert-danger py-2 mb-0 small">${escapeHtml(result || "Error inesperado.")}</div>`;
    return;
  }
  if (!result) {
    root.innerHTML = "";
    return;
  }
  const thread = result.thread || {};
  const reply = result.bot_reply || null;
  const sent = (result.auto_sent || []).length;
  const intent = thread.intent || "unknown";
  const state = thread.state || "new";
  const reviewNote = thread.assigned_review
    ? '<div class="text-warning small mt-1"><i class="bi bi-flag-fill me-1"></i>Este caso fue escalado a revisión manual.</div>'
    : "";
  const replyBlock = reply
    ? `
      <div class="border rounded p-2 bg-light mt-2">
        <div class="fw-semibold small mb-1">Respuesta automática generada:</div>
        <div class="small" style="white-space:pre-wrap">${escapeHtml(reply.text || "")}</div>
      </div>
    `
    : '<div class="small text-muted mt-2">No se generó respuesta automática (revisar escalación).</div>';
  root.innerHTML = `
    <div class="alert alert-success py-2 mb-2 small">
      <strong>Hilo:</strong> ${escapeHtml(thread.contact_name || "Contacto")}
      &middot; <span class="badge ${stateBadgeClass(state)}">${escapeHtml(intentLabel(intent))}</span>
      &middot; estado <code>${escapeHtml(state)}</code>
    </div>
    ${replyBlock}
    <div class="small text-muted mt-2">
      Mensajes en el hilo: ${(result.events || []).length} ·
      Replies "enviadas" (no salieron a LinkedIn real): ${sent}
    </div>
    ${reviewNote}
    ${thread.id ? `<button class="btn btn-link btn-sm p-0 mt-2" onclick="openMessageThread(${thread.id})">Ver hilo completo →</button>` : ""}
  `;
}

async function submitSimulatedMessage() {
  const text = (document.getElementById("sim-message-text")?.value || "").trim();
  const contactName = (document.getElementById("sim-contact-name")?.value || "").trim();
  const threadIdRaw = (document.getElementById("sim-thread-target")?.value || "").trim();
  if (!text) {
    renderSimResult("Escribe el contenido del mensaje simulado.", "error");
    return;
  }
  const payload = {
    text,
    contact_name: contactName || "Contacto simulado",
  };
  if (threadIdRaw) payload.thread_id = parseInt(threadIdRaw, 10);
  renderSimResult(null, "loading");
  try {
    const res = await apiFetch("/api/messages/simulate", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const data = await res.json();
    if (!res.ok || data.error) {
      renderSimResult(data.error || "No se pudo simular el mensaje.", "error");
      return;
    }
    renderSimResult(data, "ok");
    await Promise.all([loadInbox(false), loadMessageAutomation(false)]);
    if (data.thread?.id) {
      selectedThreadId = data.thread.id;
      await loadConversation(data.thread.id, false);
    }
  } catch (error) {
    renderSimResult(error.message || "Error de red al simular el mensaje.", "error");
  }
}
