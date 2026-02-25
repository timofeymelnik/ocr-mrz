from __future__ import annotations

import json
import logging
import os
import re
import shutil
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from threading import RLock
from typing import Any
from urllib.parse import urlparse

import requests
from playwright.sync_api import (
    Browser,
    BrowserContext,
    Page,
    Playwright,
    sync_playwright,
)

from app.autofill.form_filler import DEFAULT_CHROME_UA
from app.autofill.target_autofill import (
    autofill_existing_html_page,
    autofill_target_preview,
    extract_html_placeholder_mappings,
    extract_pdf_placeholder_mappings_from_url,
    inspect_form_fields,
    inspect_pdf_fields_from_url,
    is_template_debug_capture_enabled,
)


@dataclass
class BrowserSessionRecord:
    session_id: str
    browser: Browser
    context: BrowserContext
    page: Page
    target_url: str
    lock: RLock = field(default_factory=RLock)


_SESSIONS: dict[str, BrowserSessionRecord] = {}
_SESSIONS_LOCK = RLock()
_PLAYWRIGHT: Playwright | None = None
_PLAYWRIGHT_LOCK = RLock()
LOGGER = logging.getLogger(__name__)


def _get_or_start_playwright() -> Playwright:
    global _PLAYWRIGHT
    with _PLAYWRIGHT_LOCK:
        if _PLAYWRIGHT is None:
            _PLAYWRIGHT = sync_playwright().start()
        return _PLAYWRIGHT


def _stop_playwright_if_idle() -> None:
    global _PLAYWRIGHT
    with _SESSIONS_LOCK:
        has_sessions = bool(_SESSIONS)
    if has_sessions:
        return
    with _PLAYWRIGHT_LOCK:
        if _PLAYWRIGHT is None:
            return
        playwright = _PLAYWRIGHT
        _PLAYWRIGHT = None
    playwright.stop()


def _chromium_executable_path() -> str | None:
    explicit = os.getenv("PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH", "").strip()
    if explicit:
        return explicit
    for candidate in [
        "chromium",
        "chromium-browser",
        "google-chrome",
        "google-chrome-stable",
    ]:
        found = shutil.which(candidate)
        if found:
            return found
    return None


def _launch_chromium(p, *, headless: bool, slow_mo: int):
    launch_kwargs: dict[str, Any] = {"headless": headless, "slow_mo": slow_mo}
    executable_path = _chromium_executable_path()
    if executable_path:
        launch_kwargs["executable_path"] = executable_path
    return p.chromium.launch(**launch_kwargs)


def _attach_context_dialog_strategy(context: BrowserContext, page: Page) -> None:
    # Attach no-op handlers so Playwright does not auto-dismiss dialogs.
    def _noop_dialog_handler(dialog) -> None:
        _ = dialog

    context.on("dialog", _noop_dialog_handler)
    page.on("dialog", _noop_dialog_handler)


def _looks_like_pdf_url(url: str) -> bool:
    value = (url or "").lower()
    if ".pdf" in value:
        return True
    target = (url or "").strip()
    if not target:
        return False
    parsed = urlparse(target)
    host = (parsed.netloc or "").lower()
    path = (parsed.path or "").lower()
    # inclusion.gob.es serves many PDF documents via extension-less /documents/d/... URLs.
    if "inclusion.gob.es" in host and path.startswith("/documents/d/"):
        return True
    headers = {"User-Agent": DEFAULT_CHROME_UA}
    try:
        head = requests.head(target, timeout=8, headers=headers, allow_redirects=True)
        content_type = (head.headers.get("content-type") or "").lower()
        content_disp = (head.headers.get("content-disposition") or "").lower()
        final_url = (head.url or "").lower()
        if (
            "application/pdf" in content_type
            or ".pdf" in final_url
            or ".pdf" in content_disp
        ):
            return True
    except Exception:
        pass
    try:
        probe = requests.get(
            target, timeout=8, headers=headers, allow_redirects=True, stream=True
        )
        content_type = (probe.headers.get("content-type") or "").lower()
        content_disp = (probe.headers.get("content-disposition") or "").lower()
        final_url = (probe.url or "").lower()
        return (
            "application/pdf" in content_type
            or ".pdf" in final_url
            or ".pdf" in content_disp
        )
    except Exception:
        return False


def _new_context(browser: Browser) -> BrowserContext:
    return browser.new_context(
        accept_downloads=True,
        user_agent=DEFAULT_CHROME_UA,
        locale="es-ES",
        extra_http_headers={"Accept-Language": "es-ES,es;q=0.9,en;q=0.8"},
    )


def _debug_root_dir() -> Path:
    return Path(__file__).resolve().parent / "runtime" / "template_debug"


def _debug_safe(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", (value or "").lower()).strip("_")


def _capture_template_debug_bundle(
    *,
    session: BrowserSessionRecord,
    stage: str,
    payload: dict[str, Any],
    explicit_mappings: list[dict[str, Any]] | None = None,
) -> None:
    if not is_template_debug_capture_enabled():
        return
    try:
        current_url = (
            session.page.url if not session.page.is_closed() else session.target_url
        )
        host = (urlparse(current_url).netloc or "").lower()
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        run_dir = (
            _debug_root_dir()
            / f"{ts}_{_debug_safe(host)}_{session.session_id[:8]}_{_debug_safe(stage)}"
        )
        run_dir.mkdir(parents=True, exist_ok=True)

        meta = {
            "session_id": session.session_id,
            "stage": stage,
            "target_url": session.target_url,
            "current_url": current_url,
            "captured_at": datetime.now().isoformat(),
            "pdf_detected": bool(
                _looks_like_pdf_url(current_url)
                or _looks_like_pdf_url(session.target_url)
            ),
        }
        (run_dir / "meta.json").write_text(
            json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        (run_dir / "payload.json").write_text(
            json.dumps(payload or {}, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        (run_dir / "explicit_mappings.json").write_text(
            json.dumps(list(explicit_mappings or []), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        if _looks_like_pdf_url(current_url) or _looks_like_pdf_url(session.target_url):
            url_for_pdf = (
                current_url if _looks_like_pdf_url(current_url) else session.target_url
            )
            fields = inspect_pdf_fields_from_url(url_for_pdf, timeout_ms=15000)
            mappings, unknown_vars = extract_pdf_placeholder_mappings_from_url(
                url_for_pdf, timeout_ms=15000
            )
            (run_dir / "fields.json").write_text(
                json.dumps(fields, ensure_ascii=False, indent=2), encoding="utf-8"
            )
            (run_dir / "placeholder_mappings.json").write_text(
                json.dumps(
                    {"mappings": mappings, "unknown_vars": unknown_vars},
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
        else:
            fields = inspect_form_fields(session.page)
            mappings, unknown_vars = extract_html_placeholder_mappings(session.page)
            (run_dir / "fields.json").write_text(
                json.dumps(fields, ensure_ascii=False, indent=2), encoding="utf-8"
            )
            (run_dir / "placeholder_mappings.json").write_text(
                json.dumps(
                    {"mappings": mappings, "unknown_vars": unknown_vars},
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            (run_dir / "page.html").write_text(session.page.content(), encoding="utf-8")
            session.page.screenshot(path=str(run_dir / "page.png"), full_page=True)
    except Exception:
        LOGGER.exception("Failed capturing template debug bundle at stage=%s", stage)


def _navigate_with_fallback(page: Page, target_url: str, timeout_ms: int) -> None:
    errors: list[str] = []
    target_looks_like_pdf = _looks_like_pdf_url(target_url)
    for wait_until in ("domcontentloaded", "load", "commit"):
        try:
            page.goto(target_url, wait_until=wait_until, timeout=timeout_ms)
            return
        except Exception as exc:
            message = str(exc)
            errors.append(f"{wait_until}: {message}")
            # Some PDF/document navigations can throw ERR_ABORTED after redirect/download handoff.
            # Treat it as acceptable for PDF-like targets even when page.url stays about:blank.
            if "ERR_ABORTED" in message.upper() and (
                target_looks_like_pdf or (page.url and page.url != "about:blank")
            ):
                return
    raise RuntimeError(
        f"Load failed for URL: {target_url}. Attempts: {' | '.join(errors)}"
    )


def _get_session(session_id: str) -> BrowserSessionRecord:
    with _SESSIONS_LOCK:
        session = _SESSIONS.get(session_id)
    if not session:
        raise ValueError(f"Browser session not found: {session_id}")
    return session


def open_browser_session(
    target_url: str,
    *,
    headless: bool = False,
    slowmo: int = 80,
    timeout_ms: int = 25000,
) -> dict[str, Any]:
    p = _get_or_start_playwright()
    browser = _launch_chromium(
        p,
        headless=headless,
        slow_mo=slowmo,
    )
    context = _new_context(browser)
    page = context.new_page()
    page.set_default_timeout(timeout_ms)
    _attach_context_dialog_strategy(context, page)
    _navigate_with_fallback(page, target_url, timeout_ms)

    session_id = uuid.uuid4().hex
    record = BrowserSessionRecord(
        session_id=session_id,
        browser=browser,
        context=context,
        page=page,
        target_url=target_url,
    )
    with _SESSIONS_LOCK:
        _SESSIONS[session_id] = record
    _capture_template_debug_bundle(
        session=record,
        stage="session_opened",
        payload={},
        explicit_mappings=None,
    )
    return {
        "session_id": session_id,
        "target_url": target_url,
        "current_url": page.url,
        "alive": True,
    }


def get_browser_session_state(session_id: str) -> dict[str, Any]:
    session = _get_session(session_id)
    with session.lock:
        page = session.page
        alive = not page.is_closed()
        current_url = page.url if alive else ""
        title = ""
        if alive:
            try:
                title = page.title()
            except Exception:
                title = ""
        return {
            "session_id": session_id,
            "alive": alive,
            "current_url": current_url,
            "title": title,
        }


def fill_browser_session(
    session_id: str,
    payload: dict[str, Any],
    out_dir: Path,
    *,
    timeout_ms: int = 25000,
    explicit_mappings: list[dict[str, Any]] | None = None,
    fill_strategy: str = "strict_template",
) -> dict[str, Any]:
    session = _get_session(session_id)
    out_dir.mkdir(parents=True, exist_ok=True)
    with session.lock:
        if session.page.is_closed():
            raise RuntimeError("Browser session page is closed.")
        _capture_template_debug_bundle(
            session=session,
            stage="before_fill",
            payload=payload,
            explicit_mappings=explicit_mappings,
        )
        current_url = session.page.url
        pdf_target_url = ""
        if _looks_like_pdf_url(current_url):
            pdf_target_url = current_url
        elif _looks_like_pdf_url(session.target_url):
            # Some government portals render an HTML viewer URL in page.url
            # while the original target points to a PDF resource.
            pdf_target_url = session.target_url
        elif session.target_url:
            # Final fallback: probe target URL by trying to inspect PDF fields directly.
            # If this succeeds, force PDF mode even when URL/headers look ambiguous.
            try:
                _ = inspect_pdf_fields_from_url(
                    session.target_url, timeout_ms=min(timeout_ms, 15000)
                )
                pdf_target_url = session.target_url
            except Exception:
                pdf_target_url = ""
        if pdf_target_url:
            result = autofill_target_preview(
                payload,
                pdf_target_url,
                out_dir,
                timeout_ms=timeout_ms,
                slowmo=0,
                headless=True,
                explicit_mappings=explicit_mappings,
                strict_template=(fill_strategy != "heuristic_fallback"),
            )
            _capture_template_debug_bundle(
                session=session,
                stage="after_fill",
                payload=payload,
                explicit_mappings=explicit_mappings,
            )
            return {
                **result,
                "session_id": session_id,
                "current_url": current_url,
            }

        result = autofill_existing_html_page(
            session.page,
            payload,
            out_dir,
            explicit_mappings=explicit_mappings,
            strict_template=(fill_strategy != "heuristic_fallback"),
        )
        _capture_template_debug_bundle(
            session=session,
            stage="after_fill",
            payload=payload,
            explicit_mappings=explicit_mappings,
        )
        return {
            **result,
            "session_id": session_id,
            "current_url": session.page.url,
        }


def close_browser_session(session_id: str) -> None:
    with _SESSIONS_LOCK:
        session = _SESSIONS.pop(session_id, None)
    if not session:
        return
    with session.lock:
        try:
            session.context.close()
        finally:
            session.browser.close()
    _stop_playwright_if_idle()
