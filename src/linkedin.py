"""
LinkedIn automation module using Playwright.
Uses email/password login with persistent browser session.
No LinkedIn API — posts directly via the browser like a real user.
"""

import json
import os
import re
import socket
import threading
import time
from datetime import datetime

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

from src.config import get_setting
from src.logging_utils import get_logger

SESSION_DIR = os.path.abspath("linkedin_session")
HISTORY_FILE = "post_history.json"
SESSION_FLAG = os.path.join(SESSION_DIR, "session_ok.json")
logger = get_logger(__name__)
_SESSION_PROBE_CACHE = {"checked_at": 0.0, "valid": False}
_SESSION_PROBE_TTL_SECONDS = 120
_LOGIN_STATE_LOCK = threading.Lock()
_LOGIN_IN_PROGRESS = False
_PROFILE_LOCK_PATTERN = re.compile(r"^(?P<host>.+)-(?P<pid>\d+)$")
_STALE_PROFILE_LOCK_FILES = ("SingletonLock", "SingletonCookie", "SingletonSocket", "Default/LOCK")

# Script injected to hide automation signals from LinkedIn
_STEALTH_SCRIPT = """
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3] });
Object.defineProperty(navigator, 'languages', { get: () => ['es-ES','es','en-US','en'] });
window.chrome = { runtime: {} };
"""


# ─── Config helper ────────────────────────────────────────────────────────────

def _is_headless() -> bool:
    return bool(get_setting("app", "headless", True))


# ─── Session state ────────────────────────────────────────────────────────────

def _set_session_probe_cache(valid: bool) -> None:
    _SESSION_PROBE_CACHE["checked_at"] = time.time()
    _SESSION_PROBE_CACHE["valid"] = bool(valid)


def is_login_in_progress() -> bool:
    with _LOGIN_STATE_LOCK:
        return _LOGIN_IN_PROGRESS


def _set_login_in_progress(value: bool) -> None:
    global _LOGIN_IN_PROGRESS
    with _LOGIN_STATE_LOCK:
        _LOGIN_IN_PROGRESS = bool(value)


def _pid_exists(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False


def _singleton_lock_details() -> dict | None:
    lock_path = os.path.join(SESSION_DIR, "SingletonLock")
    if not os.path.lexists(lock_path):
        return None
    try:
        raw_value = os.readlink(lock_path) if os.path.islink(lock_path) else open(lock_path, encoding="utf-8").read().strip()
    except OSError:
        return None
    match = _PROFILE_LOCK_PATTERN.match(os.path.basename(str(raw_value).strip()))
    if not match:
        return None
    return {"host": match.group("host"), "pid": int(match.group("pid"))}


def _profile_lock_is_stale() -> bool:
    details = _singleton_lock_details()
    if details:
        if details["host"] != socket.gethostname():
            return True
        return not _pid_exists(details["pid"])

    default_lock = os.path.join(SESSION_DIR, "Default", "LOCK")
    return os.path.exists(default_lock)


def _cleanup_stale_profile_locks(log=print) -> bool:
    if not _profile_lock_is_stale():
        return False

    removed_any = False
    lock_candidates = [os.path.join(SESSION_DIR, relative_path) for relative_path in _STALE_PROFILE_LOCK_FILES]
    lock_candidates.extend(
        os.path.join(SESSION_DIR, name)
        for name in os.listdir(SESSION_DIR)
        if name.startswith(".org.chromium.Chromium.")
    )

    for path in lock_candidates:
        if not os.path.lexists(path):
            continue
        try:
            os.unlink(path)
            removed_any = True
        except IsADirectoryError:
            continue
        except OSError as exc:
            log(f"No se pudo limpiar el lock obsoleto del perfil ({os.path.basename(path)}): {exc}")

    if removed_any:
        log("Se detecto un lock viejo del perfil de Chromium. Limpiando archivos temporales y reintentando...")
    return removed_any


def _is_profile_locked_error(exc: Exception) -> bool:
    message = str(exc).lower()
    lock_tokens = (
        "profile appears to be in use",
        "chromium has locked the profile",
        "singletonlock",
        "user data directory is already in use",
    )
    return any(token in message for token in lock_tokens)


def _probe_session_via_browser(log=print) -> bool:
    with sync_playwright() as p:
        context = _get_context(p, headless=True, log=log)
        page = context.new_page()
        try:
            _goto_with_retry(page, "https://www.linkedin.com/feed/", log)
            current = page.url.lower()
            valid = not any(token in current for token in ("login", "authwall", "checkpoint", "challenge", "captcha", "verification"))
            if not valid:
                _clear_session()
            return valid
        except Exception as exc:
            log(f"No se pudo verificar la sesión real de LinkedIn: {exc}")
            return False
        finally:
            context.close()


def is_session_valid(*, verify_browser: bool = False, log=print, max_probe_age_seconds: int = _SESSION_PROBE_TTL_SECONDS) -> bool:
    if not os.path.exists(SESSION_FLAG):
        return False
    try:
        with open(SESSION_FLAG) as f:
            data = json.load(f)
        if time.time() >= data.get("expires_at", 0):
            return False
        if not verify_browser:
            return True
        age = time.time() - float(_SESSION_PROBE_CACHE.get("checked_at", 0) or 0)
        if age <= max_probe_age_seconds:
            return bool(_SESSION_PROBE_CACHE.get("valid", False))
        valid = _probe_session_via_browser(log=log)
        _set_session_probe_cache(valid)
        return valid
    except Exception:
        return False


def session_days_left() -> int:
    if not os.path.exists(SESSION_FLAG):
        return 0
    try:
        with open(SESSION_FLAG) as f:
            data = json.load(f)
        return max(0, int((data.get("expires_at", 0) - time.time()) / 86400))
    except Exception:
        return 0


def _write_session_flag():
    os.makedirs(SESSION_DIR, exist_ok=True)
    with open(SESSION_FLAG, "w") as f:
        json.dump({"expires_at": time.time() + 55 * 86400}, f)
    _set_session_probe_cache(True)


def _clear_session():
    if os.path.exists(SESSION_FLAG):
        os.remove(SESSION_FLAG)
    _set_session_probe_cache(False)


# ─── Browser context helper ───────────────────────────────────────────────────

def _launch_persistent_context(playwright, *, headless: bool):
    return playwright.chromium.launch_persistent_context(
        user_data_dir=SESSION_DIR,
        headless=headless,
        viewport={"width": 1280, "height": 800},
        user_agent=(
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/131.0.0.0 Safari/537.36"
        ),
        locale=str(get_setting("linkedin_browser", "locale", "es-ES")),
        timezone_id=str(get_setting("linkedin_browser", "timezone_id", "America/Mexico_City")),
        args=[
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
            "--disable-dev-shm-usage",
        ],
        ignore_default_args=["--enable-automation"],
    )


def _get_context(playwright, headless: bool = False, log=print):
    """Launch a persistent browser context with anti-detection measures."""
    os.makedirs(SESSION_DIR, exist_ok=True)
    try:
        ctx = _launch_persistent_context(playwright, headless=headless)
    except Exception as exc:
        if not _is_profile_locked_error(exc):
            raise
        recovered = _cleanup_stale_profile_locks(log=log)
        if not recovered:
            raise RuntimeError(
                "El perfil de LinkedIn esta bloqueado por otra instancia de Chromium. "
                "Cierra cualquier ventana de automatizacion abierta y vuelve a intentarlo."
            ) from exc
        try:
            ctx = _launch_persistent_context(playwright, headless=headless)
        except Exception as retry_exc:
            if _is_profile_locked_error(retry_exc):
                raise RuntimeError(
                    "El perfil de LinkedIn sigue bloqueado despues de limpiar locks viejos. "
                    "Cierra cualquier navegador Chrome/Chromium usado por la automatizacion y reintenta."
                ) from retry_exc
            raise
    ctx.add_init_script(_STEALTH_SCRIPT)
    return ctx


# ─── Login ────────────────────────────────────────────────────────────────────

def login(email: str, password: str, log=print) -> bool:
    """Open a headed browser, log in to LinkedIn, save the session."""
    log("Abriendo navegador para iniciar sesión en LinkedIn...")
    _set_login_in_progress(True)
    try:
        with sync_playwright() as p:
            # Login always headed so user can handle 2FA/CAPTCHA
            context = _get_context(p, headless=False, log=log)
            page = context.new_page()
            try:
                page.goto("https://www.linkedin.com/login",
                          wait_until="domcontentloaded", timeout=30000)
                _human_delay(page)

                if "feed" in page.url or "mynetwork" in page.url:
                    log("Sesión ya activa.")
                    _write_session_flag()
                    context.close()
                    return True

                log("Ingresando credenciales...")
                page.fill("#username", email)
                _human_delay(page, 0.5, 1.0)
                page.fill("#password", password)
                _human_delay(page, 0.5, 1.0)
                page.click("button[type='submit']")

                try:
                    page.wait_for_url("**/feed/**", timeout=20000)
                    log("Inicio de sesión exitoso.")
                    _write_session_flag()
                    context.close()
                    return True
                except PWTimeout:
                    current = page.url
                    if any(k in current for k in ("checkpoint", "challenge", "captcha", "verification")):
                        log("Verificación requerida (2FA/CAPTCHA). Complétala en el navegador...")
                        try:
                            page.wait_for_url("**/feed/**", timeout=180000)
                            log("Verificación completada.")
                            _write_session_flag()
                            context.close()
                            return True
                        except PWTimeout:
                            log("Tiempo de espera agotado para la verificación.")
                            context.close()
                            return False
                    else:
                        log(f"No se pudo confirmar el inicio de sesión. URL: {current}")
                        context.close()
                        return False

            except Exception as e:
                log(f"Error durante el inicio de sesión: {e}")
                try:
                    context.close()
                except Exception:
                    pass
                return False
    finally:
        _set_login_in_progress(False)


# ─── Publish post ─────────────────────────────────────────────────────────────

def _save_screenshot(page, session_id: str, label: str, screenshots: list):
    """Save a screenshot and append its URL to the screenshots list."""
    try:
        debug_dir = os.path.abspath(os.path.join("static", "debug"))
        os.makedirs(debug_dir, exist_ok=True)
        filename = f"{session_id}_{label}.png"
        path = os.path.join(debug_dir, filename)
        page.screenshot(path=path, full_page=False)
        screenshots.append(f"/static/debug/{filename}")
    except Exception as ex:
        logger.info(
            "No se pudo guardar screenshot de debug",
            extra={"event": "linkedin.debug_screenshot_error"},
            exc_info=ex,
        )


def _save_debug(page, label: str = "debug"):
    """Save screenshot + HTML for debugging."""
    try:
        debug_dir = os.path.abspath(os.path.join("static", "debug"))
        os.makedirs(debug_dir, exist_ok=True)
        page.screenshot(path=os.path.join(debug_dir, f"{label}.png"), full_page=True)
        with open(os.path.join(debug_dir, f"{label}.html"), "w", encoding="utf-8") as f:
            f.write(page.content())
    except Exception as ex:
        logger.info(
            "No se pudo guardar dump de debug",
            extra={"event": "linkedin.debug_dump_error"},
            exc_info=ex,
        )


def _goto_with_retry(page, url: str, log=print, retries: int = 3):
    """Navigate to URL using domcontentloaded (not networkidle). Retries on timeout."""
    for attempt in range(1, retries + 1):
        try:
            page.goto(
                url,
                wait_until="domcontentloaded",
                timeout=int(get_setting("linkedin_browser", "feed_timeout_ms", 30000)),
            )
            # Extra wait for dynamic content to settle
            page.wait_for_timeout(2000)
            return
        except PWTimeout:
            if attempt < retries:
                log(f"Timeout cargando página (intento {attempt}/{retries}), reintentando...")
                page.wait_for_timeout(3000)
            else:
                raise RuntimeError(f"No se pudo cargar {url} después de {retries} intentos.")
        except Exception as e:
            raise RuntimeError(f"Error al navegar a {url}: {e}")


def collect_feed_signals(limit: int = 8, log=print) -> list[str]:
    """
    Read a handful of visible LinkedIn feed texts from an existing session.
    This is used as an input signal for topic discovery, not for publishing.
    """
    if not is_session_valid():
        return []

    with sync_playwright() as p:
        context = _get_context(p, headless=True, log=log)
        page = context.new_page()
        try:
            _goto_with_retry(page, "https://www.linkedin.com/feed/", log)
            page.wait_for_timeout(3000)

            for _ in range(3):
                page.mouse.wheel(0, 1800)
                page.wait_for_timeout(1200)

            items = page.evaluate(
                """(limit) => {
                    const roots = Array.from(
                      document.querySelectorAll('div.feed-shared-update-v2, div.occludable-update, article')
                    );
                    const seen = new Set();
                    const out = [];

                    for (const root of roots) {
                      const text = (root.innerText || '')
                        .replace(/\\s+/g, ' ')
                        .trim();
                      if (!text || text.length < 120) continue;
                      if (seen.has(text)) continue;
                      seen.add(text);
                      out.push(text.slice(0, 700));
                      if (out.length >= limit) break;
                    }
                    return out;
                }""",
                limit,
            )
            return [str(item) for item in items if str(item).strip()]
        except Exception as exc:
            log(f"No se pudieron leer señales del feed de LinkedIn: {exc}")
            return []
        finally:
            context.close()


def _click_start_post_with_retry(page, log=print, retries: int = 3):
    """Try _click_start_post up to `retries` times, scrolling to top between attempts."""
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            _click_start_post(page)
            return
        except RuntimeError as e:
            last_err = e
            if attempt < retries:
                log(f"No se encontró el botón de publicar (intento {attempt}/{retries}), reintentando...")
                page.evaluate("window.scrollTo(0, 0)")
                page.wait_for_timeout(2000)
                # Also try refreshing the page on last-but-one attempt
                if attempt == retries - 1:
                    log("Recargando página antes del último intento...")
                    try:
                        page.reload(wait_until="domcontentloaded", timeout=20000)
                        page.wait_for_timeout(2500)
                    except Exception:
                        pass
    _save_debug(page, "feed_click_failed")
    raise RuntimeError(f"No se pudo abrir el compositor después de {retries} intentos: {last_err}")


def _submit_post_with_retry(page, log=print, retries: int = 3):
    """Try _submit_post, then fall back to keyboard shortcut Ctrl+Enter."""
    for attempt in range(1, retries + 1):
        try:
            _submit_post(page)
            return
        except RuntimeError:
            if attempt < retries:
                log(f"Botón 'Publicar' no encontrado (intento {attempt}/{retries}), reintentando...")
                page.wait_for_timeout(1500)
    # Keyboard fallback: Ctrl+Enter
    log("Intentando publicar con atajo de teclado (Ctrl+Enter)...")
    try:
        page.keyboard.press("Control+Return")
        page.wait_for_timeout(4000)
        return
    except Exception as e:
        raise RuntimeError(f"No se pudo publicar el post: {e}")


def publish_post(post_text: str, image_path: str, log=print, on_screenshot=None) -> dict:
    """Open browser with saved session and publish a post to LinkedIn.

    on_screenshot(url): optional callback called each time a screenshot is saved,
    so callers can push the URL to a live job status dict.
    """
    if not is_session_valid():
        raise PermissionError("No hay sesión de LinkedIn activa. Inicia sesión primero.")

    import uuid as _uuid
    session_id = _uuid.uuid4().hex[:8]
    screenshots = []

    def snap(page, label):
        _save_screenshot(page, session_id, label, screenshots)
        if on_screenshot and screenshots:
            on_screenshot(screenshots[-1])

    headless = _is_headless()
    log(f"Abriendo navegador {'(headless)' if headless else '(visible)'}...")

    with sync_playwright() as p:
        context = _get_context(p, headless=headless, log=log)
        page = context.new_page()

        try:
            # ── 1. Navigate to feed ──────────────────────────────────────────
            log("Cargando LinkedIn feed...")
            _goto_with_retry(page, "https://www.linkedin.com/feed/", log)
            _human_delay(page, 1.5, 2.5)
            snap(page, "01_feed")

            if "login" in page.url or "authwall" in page.url:
                _clear_session()
                _save_debug(page, "authwall")
                raise PermissionError("Sesión expirada. Inicia sesión nuevamente.")

            # ── 2. Open post composer ────────────────────────────────────────
            log("Abriendo el cuadro de publicación...")
            _click_start_post_with_retry(page, log)
            _human_delay(page, 1.2, 1.8)
            snap(page, "02_post_modal")

            # ── 3. Type post text ────────────────────────────────────────────
            log("Escribiendo el texto del post...")
            _type_post_text(page, post_text)
            _human_delay(page, 0.8, 1.2)
            snap(page, "03_text_typed")

            # ── 4. Upload image (with fallback: skip image on failure) ───────
            log("Subiendo la imagen...")
            try:
                _upload_image(page, image_path)
                _human_delay(page, 3.0, 4.0)
                snap(page, "04_image_uploaded")
            except Exception as img_err:
                log(f"Advertencia: no se pudo subir la imagen ({img_err}). Publicando sin imagen...")
                snap(page, "04_image_failed")

            # ── 5. Submit ────────────────────────────────────────────────────
            log("Publicando el post...")
            _submit_post_with_retry(page, log)
            _human_delay(page, 2.0, 3.0)
            snap(page, "05_published")

            log("¡Post publicado exitosamente!")

            # ── 6. Capture post URL from success notification ────────────────
            post_url = ""
            try:
                view_link = page.locator("a[href*='/feed/update/']").first
                view_link.wait_for(timeout=6000)
                href = view_link.get_attribute("href") or ""
                if href:
                    post_url = href if href.startswith("http") else f"https://www.linkedin.com{href}"
                    log(f"URL del post capturada: {post_url}")
            except Exception:
                log("No se pudo capturar la URL del post (se guardará vacía).")

            context.close()
            return {"success": True, "screenshots": screenshots, "post_url": post_url}

        except Exception as e:
            log(f"Error al publicar: {e}")
            try:
                snap(page, "error_final")
                _save_debug(page, "publish_error")
            except Exception:
                pass
            try:
                context.close()
            except Exception:
                pass
            raise


# ─── Post flow helpers ────────────────────────────────────────────────────────

_EDITOR_SELECTOR = (
    ".ql-editor[contenteditable='true'], "
    "div[contenteditable='true'][data-placeholder], "
    "div[role='textbox'][contenteditable='true']"
)


def _is_compose_modal_open(page) -> bool:
    """Return True if the LinkedIn compose/post dialog is already visible."""
    try:
        return page.locator(_EDITOR_SELECTOR).first.is_visible(timeout=1500)
    except Exception:
        return False


def _click_start_post(page):
    """Open the LinkedIn compose modal — multiple strategies.
    Short-circuits if the modal is already open.
    """
    # Fast-path: modal already open (persistent session state)
    if _is_compose_modal_open(page):
        return

    # Strategy 1: CSS selectors
    css_selectors = [
        "button.share-box-feed-entry__trigger",
        ".share-box-feed-entry__trigger",
        "[data-control-name='create_post_trigger']",
        "button[aria-label='Start a post']",
        "button[aria-label*='post' i]",
        ".share-box-feed-entry button",
        # 2025-2026 LinkedIn: text input styled as button
        ".share-creation-state__placeholder",
        "[data-placeholder*='publicación' i]",
        "[data-placeholder*='post' i]",
    ]
    for sel in css_selectors:
        try:
            el = page.locator(sel).first
            if el.is_visible(timeout=2000):
                el.click()
                page.wait_for_selector(_EDITOR_SELECTOR, timeout=6000)
                return
        except Exception:
            continue

    # Strategy 2: Role-based
    for name in ("Start a post", "Comparte un post", "Comenzar una publicación",
                 "Crear publicación", "¿Sobre qué quieres hablar?"):
        try:
            btn = page.get_by_role("button", name=name)
            if btn.first.is_visible(timeout=2000):
                btn.first.click()
                page.wait_for_selector(_EDITOR_SELECTOR, timeout=6000)
                return
        except Exception:
            continue

    # Strategy 3: Placeholder text
    for placeholder in ("Start a post", "Comparte una publicación",
                        "¿Sobre qué quieres hablar?", "What do you want to talk about?"):
        try:
            el = page.get_by_placeholder(placeholder)
            if el.first.is_visible(timeout=2000):
                el.first.click()
                page.wait_for_selector(_EDITOR_SELECTOR, timeout=6000)
                return
        except Exception:
            continue

    # Strategy 4: JS scan — broad keyword match
    try:
        keywords = [
            'start a post', 'comparte un post', 'comenzar',
            'crear publicación', 'sobre qué quieres', 'what do you want',
        ]
        clicked = page.evaluate(f"""() => {{
            const keywords = {keywords};
            const candidates = [
                ...document.querySelectorAll('button'),
                ...document.querySelectorAll('[role="button"]'),
                ...document.querySelectorAll('div[tabindex="0"]'),
                ...document.querySelectorAll('[data-placeholder]'),
            ];
            for (const el of candidates) {{
                const text = (
                    el.textContent +
                    (el.getAttribute('aria-label') || '') +
                    (el.getAttribute('data-placeholder') || '')
                ).toLowerCase();
                if (keywords.some(k => text.includes(k))) {{
                    el.click();
                    return true;
                }}
            }}
            return false;
        }}""")
        if clicked:
            page.wait_for_selector(_EDITOR_SELECTOR, timeout=7000)
            return
    except Exception:
        pass

    # Strategy 5: Scroll + JS retry
    try:
        page.evaluate("window.scrollTo(0, 0)")
        page.wait_for_timeout(1500)
        page.evaluate("""() => {
            const el = document.querySelector('.share-box-feed-entry__trigger') ||
                       document.querySelector('[data-control-name=\"create_post_trigger\"]');
            if (el) el.click();
        }""")
        page.wait_for_selector(_EDITOR_SELECTOR, timeout=7000)
        return
    except Exception:
        pass

    raise RuntimeError("No se encontró el botón 'Start a post'")


def _type_post_text(page, text: str):
    """Type text into the post editor modal."""
    editor_selectors = [
        ".ql-editor[contenteditable='true']",
        "[contenteditable='true'][data-placeholder]",
        "div[contenteditable='true']",
        ".share-creation-state__text-input",
        "[role='textbox']",
    ]
    editor = None
    for sel in editor_selectors:
        try:
            el = page.locator(sel).first
            el.wait_for(state="visible", timeout=8000)
            editor = el
            break
        except Exception:
            continue

    if not editor:
        raise RuntimeError("No se encontró el editor de texto del post")

    editor.click()
    _human_delay(page, 0.3, 0.6)
    page.keyboard.type(text, delay=18)


def _upload_image(page, image_path: str):
    """Upload image to the post and dismiss the image-editing dialog if it appears."""
    abs_path = os.path.abspath(image_path)

    # Strategy 1: find visible media/photo button and use file chooser
    media_selectors = [
        "button[aria-label*='photo' i]",
        "button[aria-label*='imagen' i]",
        "button[aria-label*='image' i]",
        "button[aria-label*='media' i]",
        "button[aria-label*='Add a photo' i]",
        "button[aria-label*='Add media' i]",
        ".share-creation-state__attachments button",
        ".toolbar__wrapper button",
        "[data-control-name='share.add_media']",
        # 2025-2026 toolbar icons
        "button[aria-label*='foto' i]",
        "button[aria-label*='Añadir' i]",
    ]
    uploaded = False
    for sel in media_selectors:
        try:
            el = page.locator(sel).first
            if el.is_visible(timeout=2000):
                with page.expect_file_chooser(timeout=5000) as fc_info:
                    el.click()
                fc_info.value.set_files(abs_path)
                uploaded = True
                break
        except Exception:
            continue

    if not uploaded:
        # Strategy 2: direct file input
        try:
            file_input = page.locator("input[type='file']").first
            file_input.set_input_files(abs_path)
            uploaded = True
        except Exception:
            pass

    if not uploaded:
        # Strategy 3: JS button scan
        try:
            with page.expect_file_chooser(timeout=4000) as fc_info:
                page.evaluate("""() => {
                    const btns = [...document.querySelectorAll('button')];
                    const btn = btns.find(b =>
                        /photo|image|media|foto|añadir/i.test(
                            b.getAttribute('aria-label') || b.textContent
                        )
                    );
                    if (btn) btn.click();
                }""")
            fc_info.value.set_files(abs_path)
            uploaded = True
        except Exception:
            pass

    if not uploaded:
        raise RuntimeError("No se encontró el botón para subir imagen")

    # Wait for upload to process, then dismiss any image editing/crop dialog
    page.wait_for_timeout(2000)
    _dismiss_image_edit_dialog(page)


def _dismiss_image_edit_dialog(page):
    """Dismiss the image editing/crop dialog that LinkedIn shows after upload.

    LinkedIn opens a secondary modal with crop/filter controls. We need to
    click 'Done', 'Siguiente', 'Aplicar', or 'Save' to return to the compose modal.
    """
    # Selectors for the "Done/Next" button in image editor
    done_selectors = [
        "button[aria-label*='Done' i]",
        "button[aria-label*='Listo' i]",
        "button[aria-label*='Siguiente' i]",
        "button[aria-label*='Next' i]",
        "button[aria-label*='Aplicar' i]",
        "button[aria-label*='Save' i]",
        "button[aria-label*='Guardar' i]",
    ]
    for sel in done_selectors:
        try:
            el = page.locator(sel).first
            if el.is_visible(timeout=2000):
                el.click()
                page.wait_for_timeout(1500)
                return
        except Exception:
            continue

    # Role-based fallback
    for name in ("Done", "Listo", "Siguiente", "Next", "Aplicar", "Save", "Guardar"):
        try:
            btn = page.get_by_role("button", name=name)
            if btn.first.is_visible(timeout=1500):
                btn.first.click()
                page.wait_for_timeout(1500)
                return
        except Exception:
            continue

    # JS scan for done/next buttons in any dialog above the compose modal
    try:
        page.evaluate("""() => {
            const keywords = ['done', 'listo', 'siguiente', 'next', 'aplicar', 'save', 'guardar'];
            const dialogs = document.querySelectorAll('[role="dialog"]');
            for (const dialog of [...dialogs].reverse()) {
                const btns = [...dialog.querySelectorAll('button')];
                for (const btn of btns.reverse()) {
                    const t = (btn.textContent || btn.getAttribute('aria-label') || '').toLowerCase().trim();
                    if (keywords.some(k => t.includes(k))) {
                        btn.click();
                        return true;
                    }
                }
            }
            return false;
        }""")
        page.wait_for_timeout(1500)
    except Exception:
        pass  # No image edit dialog — that's fine


def _submit_post(page):
    """Click the final Publicar/Post submit button.

    LinkedIn 2025-2026 UI: the button lives at the bottom-right of the compose
    dialog and reads 'Publicar' (Spanish) or 'Post' (English).  It may be
    temporarily disabled while media is processing — we wait up to 8 s for it
    to become enabled before clicking.
    """
    # Wait for any lingering image-processing overlay to disappear
    _dismiss_image_edit_dialog(page)
    page.wait_for_timeout(500)

    # ── CSS selectors ──────────────────────────────────────────────────────────
    css_selectors = [
        # New LinkedIn 2025 DOM
        "button.share-actions__primary-action",
        ".share-actions__primary-action button",
        "div.share-actions button.artdeco-button--primary",
        # Aria label
        "button[aria-label='Post']",
        "button[aria-label='Publicar']",
        "button[aria-label*='Post' i]",
        "button[aria-label*='Publicar' i]",
    ]
    for sel in css_selectors:
        try:
            el = page.locator(sel).first
            if el.is_visible(timeout=2000):
                # Wait until enabled (image processing can disable it briefly)
                for _ in range(16):
                    if not el.is_disabled():
                        break
                    page.wait_for_timeout(500)
                el.click()
                page.wait_for_timeout(4000)
                return
        except Exception:
            continue

    # ── Role-based — picks button by accessible name (covers text + aria-label) ──
    for name in ("Post", "Publicar", "Share", "Compartir"):
        try:
            # Use `.last` to get the primary action (not "Post to feed" sub-menu)
            btn = page.get_by_role("button", name=name).last
            if btn.is_visible(timeout=2000):
                for _ in range(16):
                    if not btn.is_disabled():
                        break
                    page.wait_for_timeout(500)
                btn.click()
                page.wait_for_timeout(4000)
                return
        except Exception:
            continue

    # ── get_by_text — broadest match ─────────────────────────────────────────
    for text in ("Publicar", "Post"):
        try:
            btn = page.get_by_text(text, exact=True).last
            if btn.is_visible(timeout=2000):
                btn.click()
                page.wait_for_timeout(4000)
                return
        except Exception:
            continue

    # ── JS fallback — scan all dialogs, find last primary-looking button ──────
    try:
        found = page.evaluate("""() => {
            const dialogs = [...document.querySelectorAll('[role="dialog"]')];
            const target = dialogs[dialogs.length - 1] || document;
            const btns = [...target.querySelectorAll('button')];
            // Try exact match first
            const exact = btns.find(b =>
                /^(post|publicar|share|compartir)$/i.test((b.textContent || '').trim())
            );
            if (exact) { exact.click(); return true; }
            // Partial match
            const partial = btns.find(b =>
                /(publicar|\\bpost\\b|share|compartir)/i.test(b.textContent || '')
            );
            if (partial) { partial.click(); return true; }
            return false;
        }""")
        if found:
            page.wait_for_timeout(4000)
            return
    except Exception:
        pass

    raise RuntimeError("No se encontró el botón 'Publicar' para publicar")


# ─── Human delay helper ───────────────────────────────────────────────────────

def _human_delay(page, min_s: float = 0.8, max_s: float = 1.5):
    import random
    ms = int(random.uniform(min_s, max_s) * 1000)
    page.wait_for_timeout(ms)


# ─── Metrics scraping ────────────────────────────────────────────────────────

def scrape_post_metrics(post_url: str, *, log=print) -> dict | None:
    """Navigate to a LinkedIn post URL and extract analytics metrics.

    Returns a dict with the keys impressions, reactions, comments, reposts,
    saves, link_clicks, profile_visits — using 0 when a particular metric
    is not exposed by LinkedIn for the current account. Returns None if
    none of the metrics could be extracted.
    """
    if not post_url:
        log("scrape_post_metrics: URL vacía, saltando.")
        return None

    headless = _is_headless()
    log(f"Iniciando scraping de métricas para: {post_url}")

    with sync_playwright() as p:
        context = _get_context(p, headless=headless, log=log)
        page = context.new_page()
        try:
            _goto_with_retry(page, post_url, log)
            _human_delay(page, 1.5, 2.5)

            if "login" in page.url or "authwall" in page.url:
                raise PermissionError("Sesión expirada. Inicia sesión nuevamente.")

            # ── Try to extract visible metrics directly from the post ────────
            # LinkedIn shows "X reactions · X comments · X reposts" below the post
            # and a "X impressions" stat for your own posts
            metrics = page.evaluate("""
                () => {
                    const toInt = (txt) => parseInt((txt || '').replace(/[^0-9]/g, '')) || 0;

                    // Impressions: shown as "X impressions" or "X views" on own posts
                    const impEl = document.querySelector(
                        '[data-test-id="social-actions__impressions"], '
                        + '.social-details-social-counts__item--impressions, '
                        + 'button[aria-label*="impression"], '
                        + 'span[aria-label*="impression"]'
                    );
                    const impressions = impEl ? toInt(impEl.textContent) : 0;

                    // Reactions
                    const reactEl = document.querySelector(
                        '.social-details-social-counts__reactions-count, '
                        + '[data-test-id="social-actions__reaction-count"], '
                        + 'span.social-details-social-counts__reactions span'
                    );
                    const reactions = reactEl ? toInt(reactEl.textContent) : 0;

                    // Comments
                    const commentEl = document.querySelector(
                        'button[aria-label*="comment" i], '
                        + '.social-details-social-counts__comments span, '
                        + '[data-test-id="social-actions__comments"]'
                    );
                    const comments = commentEl ? toInt(commentEl.textContent) : 0;

                    // Reposts
                    const repostEl = document.querySelector(
                        'button[aria-label*="repost" i], '
                        + '[data-test-id="social-actions__reposts"]'
                    );
                    const reposts = repostEl ? toInt(repostEl.textContent) : 0;

                    return { impressions, reactions, comments, reposts, saves: 0, link_clicks: 0, profile_visits: 0 };
                }
            """)

            # If impressions is still 0, try clicking the analytics link/button
            if metrics.get("impressions", 0) == 0:
                log("Impressions no detectadas en DOM directo, intentando panel de analytics...")
                try:
                    analytics_trigger = page.locator(
                        "button[aria-label*='analytic' i], "
                        "a[href*='analytics'], "
                        ".analytics-entry-point, "
                        "span[aria-label*='impression' i]"
                    ).first
                    analytics_trigger.click(timeout=5000)
                    _human_delay(page, 1.0, 1.5)
                    # Re-run extraction after panel opens — also pull
                    # saves / link clicks / profile visits when the analytics
                    # modal exposes them.
                    metrics = page.evaluate("""
                        () => {
                            const toInt = (txt) => parseInt((txt || '').replace(/[^0-9]/g, '')) || 0;
                            const all = [...document.querySelectorAll('[class*="analytic"], [class*="metric"], [class*="stat"], li, div, span')];
                            const findText = (labels) => {
                                const lowered = labels.map(l => l.toLowerCase());
                                const el = all.find(e => {
                                    const t = (e.textContent || '').toLowerCase();
                                    return lowered.some(label => t.includes(label));
                                });
                                return el ? toInt(el.textContent) : 0;
                            };
                            return {
                                impressions: findText(['impression', 'impresion', 'view']),
                                reactions: findText(['reaction', 'reaccion']),
                                comments: findText(['comment', 'comentario']),
                                reposts: findText(['repost', 'compartido']),
                                saves: findText(['save', 'saved', 'guardado']),
                                link_clicks: findText(['link click', 'click on link', 'clic en enlace']),
                                profile_visits: findText(['profile view', 'visita al perfil', 'profile visit']),
                            };
                        }
                    """)
                except Exception as click_err:
                    log(f"No se pudo abrir el panel de analytics: {click_err}")

            # Defensive: ensure all expected keys exist (older callers expect 4 fields).
            for key in ("impressions", "reactions", "comments", "reposts", "saves", "link_clicks", "profile_visits"):
                metrics.setdefault(key, 0)

            log(f"Métricas extraídas: {metrics}")
            context.close()
            return metrics if any(v > 0 for v in metrics.values()) else None

        except Exception as exc:
            log(f"Error en scrape_post_metrics: {exc}")
            try:
                context.close()
            except Exception:
                pass
            return None


# ─── Messaging automation ────────────────────────────────────────────────────

def _locator_first_text(root, selectors: list[str]) -> str:
    for selector in selectors:
        try:
            locator = root.locator(selector).first
            if locator.count() <= 0:
                continue
            text = re.sub(r"\s+", " ", locator.inner_text(timeout=2000)).strip()
            if text:
                return text
        except Exception:
            continue
    return ""


def _locator_digit_count(root, selectors: list[str]) -> int:
    for selector in selectors:
        try:
            locator = root.locator(selector).first
            if locator.count() <= 0:
                continue
            text = re.sub(r"\D+", "", locator.inner_text(timeout=1500) or "")
            if text:
                return int(text)
        except Exception:
            continue
    return 0


def fetch_inbox_threads(limit: int = 15, *, log=print) -> list[dict]:
    if not is_session_valid():
        return []

    headless = _is_headless()
    with sync_playwright() as p:
        context = _get_context(p, headless=headless, log=log)
        page = context.new_page()
        try:
            log("Abriendo inbox de LinkedIn...")
            _goto_with_retry(page, "https://www.linkedin.com/messaging/", log)
            _human_delay(page, 1.5, 2.0)
            rows = page.locator("li.msg-conversation-listitem, .msg-conversation-listitem")
            row_count = min(rows.count(), max(0, int(limit)))
            seen: set[str] = set()
            out: list[dict] = []

            for index in range(row_count):
                row = rows.nth(index)
                row.scroll_into_view_if_needed(timeout=5000)
                _human_delay(page, 0.1, 0.2)

                contact_name = _locator_first_text(
                    row,
                    [
                        ".msg-conversation-listitem__participant-names",
                        ".msg-conversation-card__participant-names",
                        "h3",
                    ],
                ) or f"Contacto {index + 1}"
                latest_snippet = _locator_first_text(
                    row,
                    [
                        ".msg-conversation-card__message-snippet",
                        ".msg-conversation-listitem__message-snippet",
                        "p",
                    ],
                )
                last_message_at = _locator_first_text(
                    row,
                    [
                        "time",
                        ".msg-conversation-listitem__time-stamp",
                    ],
                )
                row_text = ""
                try:
                    row_text = re.sub(r"\s+", " ", row.inner_text(timeout=2000)).strip()
                except Exception:
                    row_text = ""
                unread_count = _locator_digit_count(
                    row,
                    [
                        ".notification-badge__count",
                        ".msg-conversation-card__unread-count",
                        ".msg-conversation-listitem__unread-count",
                        ".artdeco-pill",
                    ],
                )

                click_target = row.locator(
                    ".msg-conversation-listitem__link, .msg-conversation-card, [tabindex='0']"
                ).first
                try:
                    click_target.click(timeout=5000)
                    page.wait_for_timeout(1200)
                except Exception as exc:
                    log(f"No se pudo abrir la conversación #{index + 1}: {exc}")
                    continue

                thread_url = str(page.url or "").strip()
                if not thread_url:
                    continue
                thread_key = thread_url
                if thread_key in seen:
                    continue
                seen.add(thread_key)
                contact_profile_url = ""
                try:
                    contact_profile_url = page.locator(".msg-thread__link-to-profile").first.get_attribute("href", timeout=2000) or ""
                except Exception:
                    contact_profile_url = ""

                out.append(
                    {
                        "thread_key": thread_key,
                        "thread_url": thread_url,
                        "contact_name": contact_name,
                        "latest_snippet": latest_snippet or row_text[:180],
                        "last_message_at": last_message_at,
                        "unread_count": unread_count,
                        "contact_profile_url": contact_profile_url,
                    }
                )
                if len(out) >= limit:
                    break
            return out
        finally:
            context.close()


def fetch_conversation(thread_url: str, *, log=print, limit: int = 30) -> dict | None:
    if not thread_url or not is_session_valid():
        return None

    headless = _is_headless()
    with sync_playwright() as p:
        context = _get_context(p, headless=headless, log=log)
        page = context.new_page()
        try:
            _goto_with_retry(page, thread_url, log)
            _human_delay(page, 1.2, 1.8)
            page.wait_for_timeout(1200)
            data = page.evaluate(
                """(limit) => {
                    const normalized = (value) => (value || '').replace(/\\s+/g, ' ').trim();
                    const rawContactName = normalized(
                      document.querySelector('.msg-thread__link-to-profile .artdeco-entity-lockup__title')?.innerText
                      || document.querySelector('.msg-thread__link-to-profile span[dir="ltr"]')?.innerText
                      || document.querySelector('.msg-thread__heading')?.innerText
                      || document.querySelector('h2')?.innerText
                    ) || 'Contacto';
                    const contactName = rawContactName
                      .split('Estado:')[0]
                      .split('•')[0]
                      .trim() || 'Contacto';
                    const nodes = Array.from(document.querySelectorAll(
                      '.msg-s-message-list__event, .msg-s-event-listitem, li.msg-s-message-list__event'
                    ));
                    const items = [];
                    for (const node of nodes.slice(-limit)) {
                      const text = normalized(
                        node.querySelector('.msg-s-event-listitem__body, .msg-s-event-listitem__message-bubble, .msg-s-message-group__messages')?.innerText
                        || node.innerText
                      );
                      if (!text) continue;
                      const rootText = normalized(node.innerText).toLowerCase();
                      const own = node.className.includes('--me') || rootText.startsWith('tu ') || rootText.startsWith('tú ') || rootText.includes('you sent');
                      items.push({
                        sender_role: own ? 'self' : 'contact',
                        sender_name: own ? 'Yo' : contactName,
                        text,
                        happened_at: normalized(node.querySelector('time')?.getAttribute('datetime') || node.querySelector('time')?.innerText || ''),
                        external_message_id: node.getAttribute('data-id') || '',
                      });
                    }
                    return {
                      thread_url: window.location.href,
                      contact_name: contactName,
                      latest_snippet: items.length ? items[items.length - 1].text.slice(0, 180) : '',
                      last_message_at: items.length ? items[items.length - 1].happened_at : '',
                      contact_profile_url: document.querySelector('.msg-thread__link-to-profile')?.href || '',
                      unread_count: 0,
                      messages: items,
                    };
                }""",
                limit,
            )
            return dict(data or {})
        finally:
            context.close()


def send_message_reply(thread_url: str, message_text: str, *, log=print) -> None:
    if not thread_url or not str(message_text or "").strip():
        raise ValueError("Faltan thread_url o message_text para responder.")
    if not is_session_valid():
        raise PermissionError("No hay sesión activa de LinkedIn.")

    headless = _is_headless()
    with sync_playwright() as p:
        context = _get_context(p, headless=headless, log=log)
        page = context.new_page()
        try:
            _goto_with_retry(page, thread_url, log)
            _human_delay(page, 1.0, 1.5)

            editor = page.locator(
                "div.msg-form__contenteditable[contenteditable='true'], "
                "div[role='textbox'][contenteditable='true'], "
                "div[contenteditable='true'][data-artdeco-is-focused='true']"
            ).first
            editor.wait_for(timeout=10000)
            editor.click()
            editor.fill("")
            editor.type(message_text, delay=18)
            _human_delay(page, 0.4, 0.7)

            send_button = page.locator(
                "button.msg-form__send-button, "
                "button[aria-label*='Enviar' i], "
                "button[aria-label*='Send' i]"
            ).first
            send_button.click()
            _human_delay(page, 1.0, 1.4)
        finally:
            context.close()


# ─── Post history (local) ─────────────────────────────────────────────────────

def get_recent_posts_local(n: int = 5) -> list:
    try:
        from src import db

        posts = db.get_recent_posts(n)
        if posts:
            return posts
    except Exception as exc:
        logger.info(
            "No se pudo leer historial desde SQLite, usando archivo local",
            extra={"event": "linkedin.history_db_fallback"},
            exc_info=exc,
        )

    if not os.path.exists(HISTORY_FILE):
        return []
    with open(HISTORY_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    posts = data.get("posts", [])
    return posts[-n:] if posts else []


def save_to_history(topic: str, post_text: str, post_id: str = "", category: str = "default") -> None:
    try:
        from src import db

        db.save_post(
            topic=topic,
            post_text=post_text,
            category=category,
            published=True,
        )
        return
    except Exception as exc:
        logger.info(
            "No se pudo guardar historial en SQLite, usando archivo local",
            extra={"event": "linkedin.history_file_fallback"},
            exc_info=exc,
        )

    history = {"posts": []}
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            history = json.load(f)
    history["posts"].append({
        "date": datetime.utcnow().isoformat(),
        "topic": topic,
        "category": category,
        "post_text": post_text[:300],
        "linkedin_post_id": post_id,
    })
    with open(HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(history, f, indent=2, ensure_ascii=False)
