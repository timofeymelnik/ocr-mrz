from __future__ import annotations

import logging
import re
import base64
import os
import shutil
from datetime import datetime
from pathlib import Path
from time import monotonic
from typing import Any
from urllib.parse import urlparse

from playwright.sync_api import Error, Locator, Page, TimeoutError as PlaywrightTimeoutError, expect, sync_playwright

LOGGER = logging.getLogger(__name__)
FORM_URL = "https://sede.policia.gob.es/Tasa790_012/ImpresoRellenar"
DEFAULT_CHROME_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)


def _chromium_executable_path() -> str | None:
    explicit = os.getenv("PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH", "").strip()
    if explicit:
        return explicit
    for candidate in ["chromium", "chromium-browser", "google-chrome", "google-chrome-stable"]:
        found = shutil.which(candidate)
        if found:
            return found
    return None


def _launch_chromium(p, *, headless: bool, slow_mo: int, args: list[str] | None = None):
    launch_kwargs: dict[str, Any] = {
        "headless": headless,
        "slow_mo": slow_mo,
    }
    if args:
        launch_kwargs["args"] = args
    executable_path = _chromium_executable_path()
    if executable_path:
        launch_kwargs["executable_path"] = executable_path
    return p.chromium.launch(**launch_kwargs)


def _safe_value(payload: dict[str, Any], *path: str) -> str:
    node: Any = payload
    for key in path:
        if not isinstance(node, dict):
            return ""
        node = node.get(key)
    if node is None:
        return ""
    return str(node).strip()


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", s.lower()).strip()


def _slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


def _wait_visible(locator: Locator, timeout: int) -> None:
    expect(locator).to_be_visible(timeout=timeout)


def _attach_context_dialog_strategy(context, page: Page) -> None:
    # Keep empty dialog handlers attached so Playwright does not auto-dismiss
    # native confirm/prompt dialogs; the user can decide in Chromium UI.
    def _noop_dialog_handler(dialog) -> None:
        _ = dialog

    context.on("dialog", _noop_dialog_handler)
    page.on("dialog", _noop_dialog_handler)


def _is_locator_visible(locator: Locator) -> bool:
    try:
        return locator.count() > 0 and locator.first.is_visible()
    except Exception:
        return False


def _is_blocked_page_html(html: str) -> bool:
    text = (html or "").lower()
    blocked_markers = [
        "esta direcci",
        "web est",
        "bloqueada",
        "página bloqueada",
        "pagina bloqueada",
        "contacte con el administrador",
    ]
    return sum(1 for marker in blocked_markers if marker in text) >= 2


def _new_context(browser, *, accept_downloads: bool = True):
    return browser.new_context(
        accept_downloads=accept_downloads,
        user_agent=DEFAULT_CHROME_UA,
        locale="es-ES",
        extra_http_headers={"Accept-Language": "es-ES,es;q=0.9,en;q=0.8"},
    )


def _ensure_form_loaded(page: Page, timeout_ms: int, *, target_dir: Path | None = None, stage: str = "form") -> None:
    markers: list[tuple[str, Locator]] = [
        ("heading", page.get_by_role("heading", name=re.compile(r"Tasa modelo\s*790", re.I))),
        ("nif_input", page.get_by_label(re.compile(r"N\.?I\.?F\.?\s*/\s*N\.?I\.?E\.?", re.I))),
        ("name_input", page.get_by_label(re.compile(r"Apellidos y nombre|raz[oó]n social", re.I))),
        ("download_button", page.get_by_role("button", name=re.compile(r"Descargar impreso rellenado", re.I))),
        ("nif_text", page.get_by_text(re.compile(r"N\.?I\.?F\.?\s*/\s*N\.?I\.?E\.?", re.I))),
    ]

    deadline = monotonic() + (timeout_ms / 1000.0)
    while monotonic() < deadline:
        html = ""
        try:
            html = page.content()
        except Exception:
            html = ""
        if html and _is_blocked_page_html(html):
            details = (
                f"Target form page is blocked by network policy (stage={stage}, url={page.url}). "
                "Open the URL in your normal browser and check network/proxy filtering."
            )
            if not target_dir:
                raise RuntimeError(details)
            dump_path = target_dir / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{_slugify(stage)}_blocked_page.html"
            screenshot_path: Path | None = None
            try:
                dump_path.write_text(html, encoding="utf-8")
            except Exception:
                LOGGER.exception("Failed saving blocked-page dump.")
            try:
                screenshot_path = _save_screenshot(page, target_dir, f"{stage}_blocked_page")
            except Exception:
                LOGGER.exception("Failed saving blocked-page screenshot.")
            if screenshot_path:
                details += f" Screenshot: {screenshot_path}."
            details += f" Dump: {dump_path}."
            raise RuntimeError(details)

        for marker_name, locator in markers:
            if _is_locator_visible(locator):
                LOGGER.info("Form shell detected via marker '%s' (stage=%s)", marker_name, stage)
                return
        page.wait_for_timeout(200)

    details = f"Unable to detect Tasa 790 form markup after navigation (stage={stage}, url={page.url})."
    if not target_dir:
        raise RuntimeError(details)

    dump_path = target_dir / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{_slugify(stage)}_form_not_ready.html"
    screenshot_path: Path | None = None
    try:
        dump_path.write_text(page.content(), encoding="utf-8")
    except Exception:
        LOGGER.exception("Failed to save HTML dump for form-not-ready case.")
    try:
        screenshot_path = _save_screenshot(page, target_dir, f"{stage}_form_not_ready")
    except Exception:
        LOGGER.exception("Failed to save screenshot for form-not-ready case.")
    if screenshot_path:
        details += f" Screenshot: {screenshot_path}."
    details += f" Dump: {dump_path}."
    raise RuntimeError(details)


def _fill_if_present(page: Page, label: str, value: str, timeout: int, fallback_hint: str | None = None) -> bool:
    if not value:
        return False

    try:
        by_label = page.get_by_label(label, exact=False)
        if by_label.count() > 0:
            input_box = by_label.first
            _wait_visible(input_box, timeout)
            input_box.fill(value, timeout=timeout)
            return True
    except Exception:
        pass

    if fallback_hint:
        input_box = _input_by_near_text(page, fallback_hint)
        if input_box:
            _wait_visible(input_box, timeout)
            input_box.fill(value, timeout=timeout)
            return True
    return False


def _fill_required(page: Page, label: str, value: str, timeout: int, fallback_hint: str | None = None) -> None:
    if not value:
        raise ValueError(f"Required payload field is empty for: {label}")
    ok = _fill_if_present(page, label, value, timeout, fallback_hint=fallback_hint)
    if not ok:
        raise RuntimeError(f"Unable to locate/fill required field: {label}")


def _input_by_near_text(page: Page, text_hint: str) -> Locator | None:
    containers = page.locator("div,td,tr,section").filter(has_text=re.compile(re.escape(text_hint), re.I))
    for i in range(min(containers.count(), 5)):
        item = containers.nth(i)
        candidate = item.locator("input[type='text'], input:not([type])").first
        if candidate.count() > 0:
            return candidate
    return None


def _select_radio_by_text(page: Page, text: str, timeout: int) -> None:
    wrapper = page.locator("label,div,span,tr,td").filter(has_text=re.compile(re.escape(text), re.I)).first
    _wait_visible(wrapper, timeout)
    radio = wrapper.locator("input[type='radio']").first
    if radio.count() == 0:
        radio = page.get_by_role("radio", name=re.compile(re.escape(text), re.I)).first
    _wait_visible(radio, timeout)
    radio.check(timeout=timeout)


def _select_forma_pago(page: Page, forma_pago: str, timeout: int) -> None:
    ingreso_block = page.locator("div").filter(has_text=re.compile(r"Forma de pago", re.I)).first
    _wait_visible(ingreso_block, timeout)

    target_text = "Adeudo en cuenta" if forma_pago == "adeudo" else "En efectivo"
    candidate = ingreso_block.locator("div,label,span").filter(
        has_text=re.compile(re.escape(target_text), re.I)
    ).first
    _wait_visible(candidate, timeout)

    radio = candidate.locator("input[type='radio']").first
    if radio.count() == 0:
        radio = page.get_by_role("radio", name=re.compile(re.escape(target_text), re.I)).first
    if radio.count() == 0:
        radios = ingreso_block.locator("input[type='radio']")
        idx = 1 if forma_pago == "adeudo" else 0
        if radios.count() > idx:
            radio = radios.nth(idx)
    _wait_visible(radio, timeout)
    radio.check(timeout=timeout, force=True)
    try:
        expect(radio).to_be_checked(timeout=timeout)
    except Exception:
        pass

    # Hard fallback for custom/legacy markup: strict selection only inside INGRESO section.
    js_ok = page.evaluate(
        """({target}) => {
            const lower = (s) => (s || '').toLowerCase();
            const wanted = target === 'adeudo' ? 'adeudo' : 'efectivo';

            const ingresoBlock = Array.from(document.querySelectorAll('div'))
              .find(d => {
                const t = lower(d.innerText || '');
                return t.includes('forma de pago') && (t.includes('efectivo') || t.includes('adeudo'));
              });
            if (!ingresoBlock) return false;

            const localRadios = Array.from(ingresoBlock.querySelectorAll('input[type="radio"]'));
            if (!localRadios.length) return false;

            const textAround = (el) => {
              const host = el.closest('label,div,td,tr,span') || el.parentElement;
              return lower(host ? host.innerText : '');
            };

            let selected = null;
            for (const r of localRadios) {
              const t = textAround(r);
              const isEfectivo = t.includes('en efectivo');
              const isAdeudo = t.includes('adeudo en cuenta') || t.includes('e.c. adeudo');
              if ((wanted === 'efectivo' && isEfectivo) || (wanted === 'adeudo' && isAdeudo)) {
                selected = r;
                break;
              }
            }
            if (!selected && localRadios.length >= 2) {
              selected = localRadios[wanted === 'adeudo' ? 1 : 0];
            }
            if (!selected) selected = localRadios[0];

            selected.checked = true;
            selected.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true }));
            selected.dispatchEvent(new Event('input', { bubbles: true }));
            selected.dispatchEvent(new Event('change', { bubbles: true }));

            return localRadios.some(r => r.checked);
        }""",
        {"target": forma_pago},
    )
    if not js_ok:
        raise RuntimeError("Unable to select forma de pago (efectivo/adeudo).")

    # Final strict assertion in INGRESO block.
    ok = bool(
        page.evaluate(
            """() => {
                const lower = (s) => (s || '').toLowerCase();
                const ingresoBlock = Array.from(document.querySelectorAll('div'))
                  .find(d => {
                    const t = lower(d.innerText || '');
                    return t.includes('forma de pago') && (t.includes('efectivo') || t.includes('adeudo'));
                  });
                if (!ingresoBlock) return false;
                return Array.from(ingresoBlock.querySelectorAll('input[type="radio"]')).some(r => r.checked);
            }"""
        )
    )
    if not ok:
        raise RuntimeError("Forma de pago still not selected in INGRESO block.")


def _fill_complementaria(page: Page, payload: dict[str, Any], timeout: int) -> None:
    num = _safe_value(payload, "autoliquidacion", "num_justificante")
    importe = payload.get("autoliquidacion", {}).get("importe_complementaria")
    if not num or importe in ("", None):
        raise ValueError("Complementaria selected but num_justificante/importe_complementaria are missing.")

    digits = re.sub(r"\D", "", num)
    enabled_boxes = page.locator("input[type='text']:enabled")
    target_boxes = []
    for i in range(enabled_boxes.count()):
        c = enabled_boxes.nth(i)
        placeholder = (c.get_attribute("placeholder") or "").lower()
        if placeholder in {"7", "9", "0", "1", "2"}:
            continue
        target_boxes.append(c)
    if len(target_boxes) < 7:
        # fallback: search around Num. Justificante block
        block = page.locator("div").filter(has_text=re.compile(r"Num\.?\s*Justificante", re.I)).first
        local_enabled = block.locator("input[type='text']:enabled")
        target_boxes = [local_enabled.nth(i) for i in range(local_enabled.count())]
    if len(target_boxes) < 7:
        raise RuntimeError("Could not locate editable Num. Justificante fields.")

    for i, d in enumerate(digits[: len(target_boxes)]):
        target_boxes[i].fill(d, timeout=timeout)

    integer_part, decimal_part = _split_amount(importe)
    if not _fill_if_present(page, "parte entera", integer_part, timeout):
        ent = page.get_by_label(re.compile(r"parte entera", re.I))
        if ent.count():
            ent.first.fill(integer_part, timeout=timeout)
    if not _fill_if_present(page, "parte decimal", decimal_part, timeout):
        dec = page.get_by_label(re.compile(r"parte decimal", re.I))
        if dec.count():
            dec.first.fill(decimal_part, timeout=timeout)


def _split_amount(amount: Any) -> tuple[str, str]:
    val = float(amount)
    euros = int(val)
    cents = int(round((val - euros) * 100))
    return str(euros), f"{cents:02d}"


def _find_group_table(page: Page, group_text: str, timeout: int) -> Locator:
    tables = page.locator("table")
    expected = _norm(group_text)
    for i in range(tables.count()):
        table = tables.nth(i)
        header = table.locator("th").first
        if header.count() == 0:
            continue
        header_text = _norm(header.inner_text())
        if expected in header_text:
            _wait_visible(table, timeout)
            return table
    raise ValueError(f"No trámite group table matched: {group_text}")


def _select_tramite(page: Page, payload: dict[str, Any], timeout: int) -> None:
    group_text = _safe_value(payload, "tramite", "grupo")
    option_text = _safe_value(payload, "tramite", "opcion")
    if not group_text or not option_text:
        raise ValueError("tramite.grupo and tramite.opcion are required.")

    table = _find_group_table(page, group_text, timeout)
    row = table.locator("tr").filter(has_text=re.compile(re.escape(option_text), re.I)).first
    if row.count() == 0:
        raise ValueError(f"No trámite option row matched: {option_text}")

    radio = row.locator("input[type='radio']").first
    _wait_visible(radio, timeout)
    radio.check(timeout=timeout)
    LOGGER.info("Trámite selected: group='%s' option='%s'", group_text, option_text)

    row_text = _norm(row.inner_text())
    needs_qty = any(k in row_text for k in ["cada día", "certificados o informes", "por cada documento"])
    qty = _safe_value(payload, "tramite", "cantidad") or _safe_value(payload, "tramite", "dias")
    if needs_qty:
        if not qty:
            raise ValueError("Selected trámite requires tramite.cantidad or tramite.dias.")
        row_input = row.locator("input[type='text'], input:not([type])").first
        if row_input.count() == 0:
            raise RuntimeError("Selected trámite appears to need quantity but no editable input was found in row.")
        row_input.fill(qty, timeout=timeout)
        LOGGER.info("Trámite quantity set: %s", qty)


def _save_screenshot(page: Page, download_dir: Path, name: str) -> Path:
    download_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{_slugify(name)}.png"
    path = download_dir / filename
    page.screenshot(path=str(path), full_page=True)
    return path


def _fill_main_sections(page: Page, payload: dict[str, Any], timeout: int, *, select_tramite: bool = True) -> None:
    _fill_required(page, "N.I.F./N.I.E.", _safe_value(payload, "identificacion", "nif_nie"), timeout, "N.I.F./N.I.E")
    _fill_required(
        page,
        "Apellidos y nombre o razón social",
        _safe_value(payload, "identificacion", "nombre_apellidos"),
        timeout,
        "Apellidos y nombre o razón social",
    )

    # Domicilio
    _fill_required(page, "Tipo de vía", _safe_value(payload, "domicilio", "tipo_via"), timeout, "Tipo de vía")
    _fill_required(page, "Nombre de la vía pública", _safe_value(payload, "domicilio", "nombre_via"), timeout, "Nombre de la vía pública")
    _fill_required(page, "Núm.", _safe_value(payload, "domicilio", "numero"), timeout, "Núm.")
    _fill_if_present(page, "Escalera", _safe_value(payload, "domicilio", "escalera"), timeout, "Escalera")
    _fill_if_present(page, "Piso", _safe_value(payload, "domicilio", "piso"), timeout, "Piso")
    _fill_if_present(page, "Puerta", _safe_value(payload, "domicilio", "puerta"), timeout, "Puerta")
    _fill_if_present(page, "Teléfono", _safe_value(payload, "domicilio", "telefono"), timeout, "Teléfono")
    _fill_required(page, "Municipio", _safe_value(payload, "domicilio", "municipio"), timeout, "Municipio")
    _fill_required(page, "Provincia", _safe_value(payload, "domicilio", "provincia"), timeout, "Provincia")
    _fill_required(page, "Código Postal", _safe_value(payload, "domicilio", "cp"), timeout, "Código Postal")

    # Autoliquidacion
    autoliquidacion_tipo = _safe_value(payload, "autoliquidacion", "tipo").lower() or "principal"
    if autoliquidacion_tipo == "complementaria":
        _select_radio_by_text(page, "Complementaria", timeout)
        _fill_complementaria(page, payload, timeout)
        LOGGER.info("Autoliquidación complementaria selected.")
    else:
        _select_radio_by_text(page, "Principal", timeout)
        LOGGER.info("Autoliquidación principal selected.")

    if select_tramite:
        _select_tramite(page, payload, timeout)

    # Declarante
    _fill_required(page, "Localidad", _safe_value(payload, "declarante", "localidad"), timeout, "Localidad")
    _fill_required(page, "Fecha", _safe_value(payload, "declarante", "fecha"), timeout, "Fecha")

    # Ingreso
    forma_pago = _safe_value(payload, "ingreso", "forma_pago").lower()
    if forma_pago == "adeudo":
        _select_forma_pago(page, "adeudo", timeout)
        iban = _safe_value(payload, "ingreso", "iban")
        _fill_if_present(page, "Código IBAN de la cuenta", iban, timeout, "Código IBAN de la cuenta")
        LOGGER.info("Forma de pago: adeudo.")
    else:
        _select_forma_pago(page, "efectivo", timeout)
        LOGGER.info("Forma de pago: efectivo.")


def _download_filename(payload: dict[str, Any], suggested_name: str) -> str:
    prefix = _safe_value(payload, "download", "filename_prefix") or "tasa790_012"
    nif_nie = _safe_value(payload, "identificacion", "nif_nie") or "unknown"
    day = datetime.now().strftime("%Y%m%d")
    ext = Path(suggested_name or "document.pdf").suffix or ".pdf"
    return f"{prefix}_{nif_nie}_{day}{ext}"


def _mandatory_page_checks(page: Page, timeout: int) -> list[str]:
    issues: list[str] = []

    checks = [
        "N.I.F./N.I.E.",
        "Apellidos y nombre o razón social",
        "Tipo de vía",
        "Nombre de la vía pública",
        "Núm.",
        "Municipio",
        "Provincia",
        "Código Postal",
        "Localidad",
    ]
    for label in checks:
        loc = page.get_by_label(label, exact=False)
        if loc.count() == 0:
            continue
        expect(loc.first).to_be_visible(timeout=timeout)
        val = (loc.first.input_value() or "").strip()
        if not val:
            issues.append(f"Field empty on page: {label}")

    if page.locator("input[type='radio']:checked").count() == 0:
        issues.append("No radio selected for trámite/sections.")

    forma_pago_ok = bool(
        page.evaluate(
            """() => {
                const lower = (s) => (s || '').toLowerCase();
                const radios = Array.from(document.querySelectorAll('input[type="radio"]'));
                if (!radios.length) return false;

                for (const r of radios) {
                  const host = r.closest('label,div,td,tr,span') || r.parentElement;
                  const txt = lower(host ? host.innerText : '');
                  if ((txt.includes('efectivo') || txt.includes('adeudo')) && r.checked) return true;
                }

                const ingresoBlock = Array.from(document.querySelectorAll('div'))
                  .find(d => lower(d.innerText || '').includes('forma de pago'));
                if (!ingresoBlock) return false;
                return Array.from(ingresoBlock.querySelectorAll('input[type="radio"]')).some(r => r.checked);
            }"""
        )
    )
    if not forma_pago_ok:
        issues.append("Forma de pago is not selected.")

    return issues


def _check_download_content(path: Path) -> tuple[bool, str]:
    head = path.read_bytes()[:512].lower()
    if b"<html" in head or b"<!doctype html" in head:
        return False, "Downloaded content appears to be HTML, not a document."
    return True, ""


def _is_pdf_bytes(content: bytes) -> bool:
    return content.startswith(b"%PDF")


def _extract_known_server_error(body: bytes) -> str:
    text = body.decode("utf-8", errors="ignore")
    normalized = text.lower()
    if "error en captcha" in normalized:
        return "Server returned CAPTCHA error: invalid/expired captcha. Enter the NEW captcha shown by the page and retry."
    if "debe introducir una forma de pago" in normalized:
        return "Server validation error: forma de pago not selected."
    if "debe seleccionar uno de los trámites" in normalized:
        return "Server validation error: trámite option not selected."
    return ""


def _save_from_popup_page(
    *,
    popup: Page,
    context,
    payload: dict[str, Any],
    target_dir: Path,
    timeout_ms: int,
) -> Path | None:
    url = popup.url
    if not url or url.startswith("about:"):
        return None
    LOGGER.info("Popup opened with URL: %s", url)
    try:
        response = context.request.get(url, timeout=timeout_ms)
    except Exception:
        LOGGER.exception("Failed to request popup URL for saving.")
        return None

    if not response.ok:
        LOGGER.warning("Popup URL response not OK: %s", response.status)
        return None

    content = response.body()
    content_type = (response.headers.get("content-type") or "").lower()
    suggested = Path(urlparse(url).path).name or "document.pdf"
    if _is_pdf_bytes(content):
        filename = _download_filename(payload, suggested if suggested.endswith(".pdf") else "document.pdf")
        output_path = target_dir / filename
        output_path.write_bytes(content)
        return output_path

    dump = target_dir / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_popup_response.html"
    try:
        dump.write_bytes(content)
    except Exception:
        pass
    LOGGER.error("Popup did not return real PDF bytes. Content-Type=%s, dump=%s", content_type, dump)
    return None


def _save_from_page_context(
    *,
    current_page: Page,
    context,
    payload: dict[str, Any],
    target_dir: Path,
    timeout_ms: int,
) -> Path | None:
    # 1) Direct URL fetch (works for normal http(s) PDF pages)
    url = current_page.url
    if url and not url.startswith("about:") and not url.startswith("blob:"):
        try:
            response = context.request.get(url, timeout=timeout_ms)
            if response.ok:
                body = response.body()
                ctype = (response.headers.get("content-type") or "").lower()
                if _is_pdf_bytes(body):
                    suggested = Path(urlparse(url).path).name or "document.pdf"
                    out = target_dir / _download_filename(payload, suggested if suggested.endswith(".pdf") else "document.pdf")
                    out.write_bytes(body)
                    return out
        except Exception:
            LOGGER.exception("Direct fetch from current page URL failed: %s", url)

    # 2) blob/document source in page (works for PDF viewer with blob URL)
    try:
        blob_b64 = current_page.evaluate(
            """async () => {
                const candidates = [];
                if (location.href && location.href.startsWith('blob:')) candidates.push(location.href);
                const emb = document.querySelector('embed[src], object[data], iframe[src]');
                if (emb) {
                  const src = emb.getAttribute('src') || emb.getAttribute('data');
                  if (src) candidates.push(src);
                }
                for (const src of candidates) {
                  try {
                    const res = await fetch(src);
                    if (!res.ok) continue;
                    const buf = await res.arrayBuffer();
                    const bytes = new Uint8Array(buf);
                    let binary = '';
                    const chunk = 0x8000;
                    for (let i = 0; i < bytes.length; i += chunk) {
                      binary += String.fromCharCode(...bytes.subarray(i, i + chunk));
                    }
                    return btoa(binary);
                  } catch (e) {}
                }
                return '';
            }"""
        )
        if blob_b64:
            data = base64.b64decode(blob_b64)
            if _is_pdf_bytes(data):
                out = target_dir / _download_filename(payload, "document.pdf")
                out.write_bytes(data)
                return out
    except Exception:
        LOGGER.exception("Blob/context PDF extraction failed from page: %s", current_page.url)

    return None


def _save_from_form_fetch(page: Page, payload: dict[str, Any], target_dir: Path, timeout_ms: int) -> Path | None:
    try:
        result = page.evaluate(
            """async () => {
                const form = document.querySelector('form');
                if (!form) return { ok: false, reason: 'form_not_found' };
                const action = form.getAttribute('action') || window.location.href;
                const method = (form.getAttribute('method') || 'POST').toUpperCase();

                const fd = new FormData(form);
                const params = new URLSearchParams();
                for (const [k, v] of fd.entries()) {
                    params.append(k, String(v));
                }

                const resp = await fetch(action, {
                    method,
                    headers: { 'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8' },
                    body: params.toString(),
                    credentials: 'same-origin',
                });
                const ctype = (resp.headers.get('content-type') || '').toLowerCase();
                const status = resp.status;
                const buf = await resp.arrayBuffer();
                const bytes = new Uint8Array(buf);
                let binary = '';
                const chunk = 0x8000;
                for (let i = 0; i < bytes.length; i += chunk) {
                    binary += String.fromCharCode(...bytes.subarray(i, i + chunk));
                }
                const b64 = btoa(binary);
                return { ok: resp.ok, status, ctype, b64 };
            }"""
        )
    except Exception:
        LOGGER.exception("Form fetch fallback failed in page context.")
        return None

    if not isinstance(result, dict):
        return None

    ctype = str(result.get("ctype", ""))
    status = int(result.get("status", 0) or 0)
    b64 = str(result.get("b64", ""))
    if not b64:
        LOGGER.warning("Form fetch fallback returned empty body. status=%s ctype=%s", status, ctype)
        return None

    try:
        body = base64.b64decode(b64)
    except Exception:
        LOGGER.exception("Failed to decode base64 body from form fetch fallback.")
        return None

    if not _is_pdf_bytes(body):
        dump = target_dir / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_form_fetch_response.bin"
        dump.write_bytes(body)
        LOGGER.warning(
            "Form fetch fallback returned non-PDF bytes. status=%s ctype=%s dump=%s",
            status,
            ctype,
            dump,
        )
        return None

    out = target_dir / _download_filename(payload, "document.pdf")
    out.write_bytes(body)
    LOGGER.info("Downloaded file saved from direct form fetch fallback: %s", out)
    return out


def fill_and_download(
    payload: dict[str, Any],
    *,
    headless: bool = False,
    slowmo: int = 150,
    timeout_ms: int = 20000,
    download_dir: str | None = None,
) -> Path:
    target_dir = Path(download_dir or _safe_value(payload, "download", "dir") or "./downloads").resolve()
    target_dir.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        browser = _launch_chromium(
            p,
            headless=headless,
            slow_mo=slowmo,
            args=["--disable-pdf-viewer"],
        )
        context = _new_context(browser, accept_downloads=True)
        page = context.new_page()
        page.set_default_timeout(timeout_ms)
        _attach_context_dialog_strategy(context, page)
        popup_pages: list[Page] = []
        pdf_responses = []
        descargar_requests = []

        def _on_new_page(new_page: Page) -> None:
            popup_pages.append(new_page)
            LOGGER.info("Detected new page/tab event.")

        context.on("page", _on_new_page)

        def _on_response(response) -> None:
            try:
                ctype = (response.headers.get("content-type") or "").lower()
            except Exception:
                ctype = ""
            url = response.url or ""
            if "application/pdf" in ctype or url.lower().endswith(".pdf"):
                pdf_responses.append(response)
                LOGGER.info("Captured PDF-like response: %s", url)

        context.on("response", _on_response)

        def _on_request(request) -> None:
            url = request.url or ""
            if "ImpresoRellenarDescargar" in url:
                descargar_requests.append(request)
                LOGGER.info("Captured descargar request: %s %s", request.method, url)

        context.on("request", _on_request)

        try:
            LOGGER.info("Navigating to form: %s", FORM_URL)
            page.goto(FORM_URL, wait_until="domcontentloaded")
            _ensure_form_loaded(page, timeout_ms, target_dir=target_dir, stage="auto_submit_open")

            _fill_main_sections(page, payload, timeout_ms)
            shot1 = _save_screenshot(page, target_dir, "after_fill")
            LOGGER.info("Screenshot saved: %s", shot1)

            manual_captcha = str(payload.get("captcha", {}).get("manual", True)).lower() != "false"
            if manual_captcha:
                LOGGER.info("Reached CAPTCHA step, waiting for manual solve.")
                print("Введи CAPTCHA на странице и нажми Enter в терминале, чтобы продолжить.")
                input()

            shot2 = _save_screenshot(page, target_dir, "before_download")
            LOGGER.info("Screenshot saved: %s", shot2)

            page_issues = _mandatory_page_checks(page, timeout_ms)
            if page_issues:
                shot = _save_screenshot(page, target_dir, "before_download_validation_error")
                raise RuntimeError(
                    "Form still has missing mandatory values before download:\n"
                    + "\n".join(page_issues)
                    + f"\nScreenshot: {shot}"
                )

            download_button = page.get_by_role("button", name=re.compile(r"Descargar impreso rellenado", re.I))
            _wait_visible(download_button, timeout_ms)

            manual_download = str(payload.get("download", {}).get("manual_confirm", True)).lower() != "false"
            download = None
            if manual_download:
                LOGGER.info("Manual confirm mode is enabled: waiting for user click on download/confirm.")
                print("Нажмите кнопку скачивания/подтверждения в окне Chromium вручную.")
                print("После клика здесь ничего вводить не нужно: ожидаю событие download...")
                manual_timeout = max(timeout_ms, 10 * 60 * 1000)
                try:
                    download = page.wait_for_event("download", timeout=manual_timeout)
                except PlaywrightTimeoutError:
                    download = None
            else:
                try:
                    download_button.click()
                    download = page.wait_for_event("download", timeout=timeout_ms)
                except PlaywrightTimeoutError:
                    download = None

            if download is not None:
                filename = _download_filename(payload, download.suggested_filename)
                output_path = target_dir / filename
                download.save_as(str(output_path))
                ok, reason = _check_download_content(output_path)
                if not ok:
                    html_dump = target_dir / f"{output_path.stem}_response_dump.html"
                    html_dump.write_text(page.content(), encoding="utf-8")
                    shot = _save_screenshot(page, target_dir, "download_html_error")
                    raise RuntimeError(f"{reason} Dump: {html_dump}. Screenshot: {shot}")

                shot3 = _save_screenshot(page, target_dir, "after_download")
                LOGGER.info("Screenshot saved: %s", shot3)
                LOGGER.info("Downloaded file saved: %s", output_path)
                return output_path

            if manual_download:
                html_dump = target_dir / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_manual_download_timeout_dump.html"
                html_dump.write_text(page.content(), encoding="utf-8")
                shot = _save_screenshot(page, target_dir, "manual_download_timeout")
                LOGGER.error("Manual download did not start in time. Dump saved: %s ; screenshot: %s", html_dump, shot)
                raise RuntimeError(
                    "Manual download/confirm was not completed in time. "
                    f"Please click download/confirm in browser and retry. Dump: {html_dump}. Screenshot: {shot}"
                )

            # Fallback 0: if the POST response is PDF-like, persist it first.
            if pdf_responses:
                for idx, resp in enumerate(pdf_responses, start=1):
                    try:
                        body = resp.body()
                        if not body:
                            continue
                        ctype = (resp.headers.get("content-type") or "").lower()
                        suggested = Path(urlparse(resp.url).path).name or f"network_capture_{idx}.pdf"
                        out = target_dir / _download_filename(
                            payload,
                            suggested if suggested.endswith(".pdf") else "document.pdf",
                        )
                        if _is_pdf_bytes(body):
                            out.write_bytes(body)
                            shot3 = _save_screenshot(page, target_dir, "after_download_network_capture")
                            LOGGER.info("Screenshot saved: %s", shot3)
                            LOGGER.info("Downloaded file saved from network response: %s", out)
                            return out

                        raw_dump = target_dir / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_network_response_{idx}.bin"
                        raw_dump.write_bytes(body)
                        server_err = _extract_known_server_error(body)
                        if server_err:
                            raise RuntimeError(f"{server_err} Dump: {raw_dump}")
                        LOGGER.warning(
                            "Captured response #%s is not PDF bytes (content-type=%s). dump=%s",
                            idx,
                            ctype,
                            raw_dump,
                        )
                    except RuntimeError:
                        raise
                    except Exception:
                        LOGGER.exception("Failed to persist captured response #%s", idx)

            # Fallback 0.5: replay exact captured request through APIRequestContext
            if descargar_requests:
                for idx, req in enumerate(descargar_requests, start=1):
                    try:
                        replay_resp = context.request.fetch(req, timeout=timeout_ms)
                        body = replay_resp.body()
                        ctype = (replay_resp.headers.get("content-type") or "").lower()
                        if body and _is_pdf_bytes(body):
                            out = target_dir / _download_filename(payload, f"replay_{idx}.pdf")
                            out.write_bytes(body)
                            shot3 = _save_screenshot(page, target_dir, "after_download_replay_fetch")
                            LOGGER.info("Screenshot saved: %s", shot3)
                            LOGGER.info("Downloaded file saved from replayed request: %s", out)
                            return out
                        dump = target_dir / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_replay_response_{idx}.bin"
                        dump.write_bytes(body or b"")
                        server_err = _extract_known_server_error(body or b"")
                        if server_err:
                            raise RuntimeError(f"{server_err} Dump: {dump}")
                        LOGGER.warning(
                            "Replayed request #%s did not return PDF bytes. status=%s ctype=%s dump=%s",
                            idx,
                            replay_resp.status,
                            ctype,
                            dump,
                        )
                    except RuntimeError:
                        raise
                    except Exception:
                        LOGGER.exception("Failed replaying captured descargar request #%s", idx)

            # Fallbacks: popup tab, same tab navigation, blob PDF viewer
            try:
                candidate_pages: list[Page] = []
                candidate_pages.extend(popup_pages)
                candidate_pages.extend([p for p in context.pages if p not in candidate_pages])
                if page not in candidate_pages:
                    candidate_pages.insert(0, page)

                for idx, cand in enumerate(candidate_pages, start=1):
                    try:
                        cand.wait_for_load_state("domcontentloaded", timeout=5000)
                    except Exception:
                        pass
                    try:
                        shot_popup = _save_screenshot(cand, target_dir, f"popup_after_click_{idx}")
                        LOGGER.info("Candidate page screenshot saved: %s", shot_popup)
                    except Exception:
                        pass

                    saved = None
                    if cand.url and cand.url not in {"about:blank", FORM_URL} and not cand.url.startswith("blob:"):
                        saved = _save_from_popup_page(
                            popup=cand,
                            context=context,
                            payload=payload,
                            target_dir=target_dir,
                            timeout_ms=timeout_ms,
                        )
                    if not saved:
                        saved = _save_from_page_context(
                            current_page=cand,
                            context=context,
                            payload=payload,
                            target_dir=target_dir,
                            timeout_ms=timeout_ms,
                        )
                    if saved:
                        ok, reason = _check_download_content(saved)
                        if ok:
                            shot3 = _save_screenshot(page, target_dir, "after_download_popup")
                            LOGGER.info("Screenshot saved: %s", shot3)
                            LOGGER.info("Downloaded file saved by fallback: %s", saved)
                            return saved
                        LOGGER.error("Fallback saved file invalid: %s", reason)
            except Exception:
                LOGGER.exception("Popup/page fallback download handling failed.")

            # Final fallback: submit current form via in-page fetch and persist response.
            fetched = _save_from_form_fetch(page, payload, target_dir, timeout_ms)
            if fetched:
                shot3 = _save_screenshot(page, target_dir, "after_download_form_fetch")
                LOGGER.info("Screenshot saved: %s", shot3)
                return fetched

            html_dump = target_dir / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_download_timeout_dump.html"
            html_dump.write_text(page.content(), encoding="utf-8")
            shot = _save_screenshot(page, target_dir, "download_timeout")
            LOGGER.error("Download did not start. Dump saved: %s ; screenshot: %s", html_dump, shot)
            raise RuntimeError("Download did not start (possible validation errors or unresolved CAPTCHA).")
        except Error:
            shot = _save_screenshot(page, target_dir, "playwright_error")
            LOGGER.exception("Playwright error. Screenshot: %s", shot)
            raise
        finally:
            context.close()
            browser.close()


def fill_for_manual_handoff(
    payload: dict[str, Any],
    *,
    form_url: str = FORM_URL,
    headless: bool = False,
    slowmo: int = 150,
    timeout_ms: int = 20000,
    download_dir: str | None = None,
    wait_for_user_close: bool = True,
    save_dom_snapshot: bool = False,
) -> dict[str, str]:
    target_dir = Path(download_dir or _safe_value(payload, "download", "dir") or "./downloads").resolve()
    target_dir.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        browser = _launch_chromium(
            p,
            headless=headless,
            slow_mo=slowmo,
        )
        context = _new_context(browser, accept_downloads=True)
        page = context.new_page()
        page.set_default_timeout(timeout_ms)
        _attach_context_dialog_strategy(context, page)
        try:
            LOGGER.info("Navigating to form (manual handoff mode): %s", form_url)
            page.goto(form_url, wait_until="domcontentloaded")
            _ensure_form_loaded(page, timeout_ms, target_dir=target_dir, stage="manual_handoff_open")

            _fill_main_sections(page, payload, timeout_ms, select_tramite=False)
            shot = _save_screenshot(page, target_dir, "after_autofill_manual_handoff")
            LOGGER.info("Manual handoff screenshot saved: %s", shot)
            dom_snapshot = ""
            if save_dom_snapshot:
                dom_name = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_autofill_manual_handoff.html"
                dom_path = target_dir / dom_name
                dom_path.write_text(page.content(), encoding="utf-8")
                dom_snapshot = str(dom_path)
                LOGGER.info("Manual handoff DOM snapshot saved: %s", dom_path)
            if wait_for_user_close:
                print("Автозаполнение полей заявителя завершено.")
                print("Дальше вручную: выберите Trámite, введите CAPTCHA и скачайте документ на странице.")
                input("Нажмите Enter, чтобы закрыть браузер...")
            return {
                "screenshot": str(shot),
                "dom_snapshot": dom_snapshot,
            }
        finally:
            context.close()
            browser.close()


def fill_for_manual_handoff_on_page(
    page: Page,
    payload: dict[str, Any],
    *,
    target_dir: Path,
    timeout_ms: int = 20000,
    save_dom_snapshot: bool = False,
) -> dict[str, str]:
    target_dir.mkdir(parents=True, exist_ok=True)
    page.set_default_timeout(timeout_ms)
    # Do not attach dialog handlers here (see _attach_context_dialog_strategy).

    _ensure_form_loaded(page, timeout_ms, target_dir=target_dir, stage="manual_handoff_existing_page")
    _fill_main_sections(page, payload, timeout_ms, select_tramite=False)
    shot = _save_screenshot(page, target_dir, "after_autofill_manual_handoff_existing_page")
    LOGGER.info("Manual handoff (existing page) screenshot saved: %s", shot)
    dom_snapshot = ""
    if save_dom_snapshot:
        dom_name = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_autofill_manual_handoff_existing_page.html"
        dom_path = target_dir / dom_name
        dom_path.write_text(page.content(), encoding="utf-8")
        dom_snapshot = str(dom_path)
        LOGGER.info("Manual handoff (existing page) DOM snapshot saved: %s", dom_path)
    return {
        "screenshot": str(shot),
        "dom_snapshot": dom_snapshot,
    }


def fetch_tramite_catalog(timeout_ms: int = 20000) -> list[dict[str, Any]]:
    catalog: list[dict[str, Any]] = []
    with sync_playwright() as p:
        browser = _launch_chromium(
            p,
            headless=True,
            slow_mo=0,
        )
        context = _new_context(browser, accept_downloads=False)
        page = context.new_page()
        page.set_default_timeout(timeout_ms)
        try:
            page.goto(FORM_URL, wait_until="domcontentloaded")
            tables = page.locator("table")
            for i in range(tables.count()):
                table = tables.nth(i)
                header = table.locator("th").first
                if header.count() == 0:
                    continue
                group = _clean_spaces(header.inner_text())
                options: list[str] = []
                rows = table.locator("tr")
                for j in range(rows.count()):
                    row = rows.nth(j)
                    if row.locator("input[type='radio']").count() == 0:
                        continue
                    cells = row.locator("td")
                    if cells.count() == 0:
                        text = _clean_spaces(row.inner_text())
                    else:
                        text = _clean_spaces(cells.first.inner_text())
                    if text and text not in options:
                        options.append(text)
                if group and options:
                    catalog.append({"group": group, "options": options})
            return catalog
        finally:
            context.close()
            browser.close()


def _clean_spaces(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()
