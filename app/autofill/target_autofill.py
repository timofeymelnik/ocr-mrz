from __future__ import annotations

import logging
import os
import re
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import fitz  # PyMuPDF
import requests
from playwright.sync_api import Page, sync_playwright
from requests.exceptions import SSLError

from app.autofill.placeholder_helpers import (
    canonical_from_placeholder as _canonical_from_placeholder_impl,
    canonical_keys_from_placeholder_tokens as _canonical_keys_from_placeholder_tokens_impl,
    eval_checked_when as _eval_checked_when_impl,
    rule_context as _rule_context_impl,
    select_canonical_for_composite_placeholder as _select_canonical_for_composite_placeholder_impl,
)
from app.autofill.target_helpers import (
    build_date_split_field_values as _build_date_split_field_values,
    compose_floor_door_token as _compose_floor_door_token,
    infer_spanish_province_from_cp as _infer_spanish_province_from_cp,
    norm_text as _norm_text,
    normalize_ascii_upper as _normalize_ascii_upper,
    normalize_door_token,
    normalize_signal as _normalize_signal,
    sanitize_floor_token as _sanitize_floor_token,
    split_address_details as _split_address_details,
    split_compact_floor_door as _split_compact_floor_door,
    split_date_parts as _split_date_parts,
    strip_extra_spaces as _strip_extra_spaces,
)
from app.autofill.target_pdf_helpers import (
    build_nif_split_field_map as _build_nif_split_field_map_impl,
    infer_pdf_checkbox_expected as _infer_pdf_checkbox_expected_impl,
    pdf_value_for_field as _pdf_value_for_field_impl,
    should_ignore_pdf_mapping as _should_ignore_pdf_mapping_impl,
)

try:
    import certifi
except Exception:  # pragma: no cover
    certifi = None  # type: ignore[assignment]

LOGGER = logging.getLogger(__name__)

CANONICAL_FIELD_KEYS: list[str] = [
    "nif_nie",
    "nif_nie_prefix",
    "nif_nie_number",
    "nif_nie_suffix",
    "pasaporte",
    "nombre_apellidos",
    "primer_apellido",
    "segundo_apellido",
    "nombre",
    "sexo",
    "tipo_via",
    "nombre_via",
    "domicilio_en_espana",
    "numero",
    "escalera",
    "piso",
    "puerta",
    "piso_puerta",
    "telefono",
    "municipio",
    "provincia",
    "cp",
    "localidad",
    "fecha",
    "fecha_dia",
    "fecha_mes",
    "fecha_anio",
    "importe_euros",
    "forma_pago",
    "iban",
    "email",
    "fecha_nacimiento",
    "fecha_nacimiento_dia",
    "fecha_nacimiento_mes",
    "fecha_nacimiento_anio",
    "nacionalidad",
    "pais_nacimiento",
    "estado_civil",
    "lugar_nacimiento",
    "nombre_padre",
    "nombre_madre",
    "representante_legal",
    "representante_documento",
    "titulo_representante",
    "hijos_escolarizacion_espana",
]

CANONICAL_FILL_PRIORITY: list[str] = list(CANONICAL_FIELD_KEYS)
CANONICAL_FIELD_KEY_SET: set[str] = set(CANONICAL_FIELD_KEYS)
PLACEHOLDER_RE = re.compile(r"^\{([a-z_]+)\}$", re.I)
PLACEHOLDER_TOKEN_RE = re.compile(r"\{([a-z_]+)\}", re.I)


def _env_flag(name: str, default: bool) -> bool:
    raw = os.getenv(name, "1" if default else "0").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def is_template_debug_capture_enabled() -> bool:
    # Dev-only verbose artifacts for template debugging.
    return _env_flag("TEMPLATE_DEBUG_CAPTURE", False)


def should_save_artifact_screenshots() -> bool:
    # Screenshots are allowed only in explicit template debug mode.
    return is_template_debug_capture_enabled() and _env_flag(
        "SAVE_ARTIFACT_SCREENSHOTS", False
    )


def should_save_artifact_screenshots_on_error() -> bool:
    # Error screenshots must also stay behind debug gate.
    return is_template_debug_capture_enabled() and _env_flag(
        "SAVE_ARTIFACT_SCREENSHOTS_ON_ERROR", True
    )


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


def _slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", (value or "").lower()).strip("_")


def _safe(payload: dict[str, Any], *path: str) -> str:
    node: Any = payload
    for key in path:
        if not isinstance(node, dict):
            return ""
        node = node.get(key)
    if node is None:
        return ""
    return str(node).strip()


def _infer_target_type(target_url: str) -> str:
    raw = (target_url or "").lower()
    parsed = urlparse(target_url)
    path = (parsed.path or "").lower()
    host = (parsed.netloc or "").lower()
    query = (parsed.query or "").lower()
    if path.endswith(".pdf") or ".pdf" in raw or ".pdf" in query:
        return "pdf"
    # inclusion.gob.es serves some PDF forms under extension-less /documents/d/... URLs.
    if "inclusion.gob.es" in host and path.startswith("/documents/d/"):
        return "pdf"
    # Some official portals serve PDFs on extension-less URLs.
    # Probe headers/redirect target to avoid false html mode.
    headers = {"User-Agent": "Mozilla/5.0 OCR-MRZ Autofill"}
    try:
        head = requests.head(
            target_url, timeout=8, headers=headers, allow_redirects=True
        )
        content_type = (head.headers.get("content-type") or "").lower()
        content_disp = (head.headers.get("content-disposition") or "").lower()
        final_path = (urlparse(head.url).path or "").lower()
        if (
            "application/pdf" in content_type
            or ".pdf" in final_path
            or ".pdf" in content_disp
        ):
            return "pdf"
    except Exception:
        pass
    try:
        probe = requests.get(
            target_url, timeout=8, headers=headers, allow_redirects=True, stream=True
        )
        content_type = (probe.headers.get("content-type") or "").lower()
        content_disp = (probe.headers.get("content-disposition") or "").lower()
        final_path = (urlparse(probe.url).path or "").lower()
        if (
            "application/pdf" in content_type
            or ".pdf" in final_path
            or ".pdf" in content_disp
        ):
            return "pdf"
    except Exception:
        pass
    return "html"


def _fetch_pdf_bytes(target_url: str, timeout_ms: int) -> tuple[bytes, str]:
    headers = {"User-Agent": "Mozilla/5.0 OCR-MRZ Autofill"}
    ssl_verify_env = os.getenv("PDF_SSL_VERIFY", "1").strip().lower()
    ssl_verify_enabled = ssl_verify_env not in {"0", "false", "no", "off"}
    verify_value: Any = True
    if ssl_verify_enabled and certifi is not None:
        verify_value = certifi.where()
    elif not ssl_verify_enabled:
        verify_value = False

    try:
        resp = requests.get(
            target_url,
            timeout=max(10, timeout_ms // 1000),
            headers=headers,
            verify=verify_value,
        )
    except SSLError:
        insecure_fallback = os.getenv(
            "PDF_SSL_INSECURE_FALLBACK", "0"
        ).strip().lower() in {"1", "true", "yes", "on"}
        if not insecure_fallback:
            raise
        LOGGER.warning(
            "SSL verification failed for PDF URL. Retrying with verify=False due to PDF_SSL_INSECURE_FALLBACK=1"
        )
        resp = requests.get(
            target_url,
            timeout=max(10, timeout_ms // 1000),
            headers=headers,
            verify=False,
        )
    resp.raise_for_status()
    content_type = (resp.headers.get("content-type") or "").lower()
    data = resp.content or b""
    if not data.startswith(b"%PDF") and "application/pdf" not in content_type:
        raise RuntimeError(
            f"Target URL does not look like PDF (content-type={content_type})."
        )
    return data, content_type


def _build_value_map(payload: dict[str, Any]) -> dict[str, str]:
    nombre_apellidos = _safe(payload, "identificacion", "nombre_apellidos")
    nacionalidad = _safe(payload, "extra", "nacionalidad")
    apellido1, apellido2, nombre = _split_name_for_spanish_fields(
        nombre_apellidos, nacionalidad
    )
    explicit_apellido1 = _safe(payload, "identificacion", "primer_apellido")
    explicit_apellido2 = _safe(payload, "identificacion", "segundo_apellido")
    explicit_nombre = _safe(payload, "identificacion", "nombre")
    normalized_nombre_apellidos = _strip_extra_spaces(
        " ".join(
            x
            for x in [
                explicit_apellido1 or apellido1,
                explicit_apellido2 or apellido2,
                explicit_nombre or nombre,
            ]
            if x
        )
    )
    if not normalized_nombre_apellidos:
        normalized_nombre_apellidos = _strip_extra_spaces(
            nombre_apellidos.replace(",", " ")
        )
    tipo_via = _safe(payload, "domicilio", "tipo_via")
    raw_nombre_via = _safe(payload, "domicilio", "nombre_via")
    (
        cleaned_nombre_via,
        inferred_numero,
        inferred_escalera,
        inferred_piso,
        inferred_puerta,
    ) = _split_address_details(raw_nombre_via)
    nombre_via = cleaned_nombre_via or raw_nombre_via
    numero = _safe(payload, "domicilio", "numero") or inferred_numero
    escalera = _safe(payload, "domicilio", "escalera") or inferred_escalera
    piso = _sanitize_floor_token(_safe(payload, "domicilio", "piso")) or inferred_piso
    puerta = _safe(payload, "domicilio", "puerta") or inferred_puerta
    piso, puerta = _split_compact_floor_door(piso, puerta)
    piso_puerta = _compose_floor_door_token(piso, puerta)
    # Spanish forms usually split street line from number/floor/door fields.
    domicilio_en_espana = " ".join(x for x in [tipo_via, nombre_via] if x).strip()
    nif_nie = _safe(payload, "identificacion", "nif_nie").upper()
    m_nie = re.fullmatch(r"([XYZ])(\d{7})([A-Z])", re.sub(r"[^A-Z0-9]", "", nif_nie))
    nie_prefix = m_nie.group(1) if m_nie else ""
    nie_number = m_nie.group(2) if m_nie else ""
    nie_suffix = m_nie.group(3) if m_nie else ""
    fecha_decl = _safe(payload, "declarante", "fecha")
    fecha_nac = _safe(payload, "extra", "fecha_nacimiento")
    ingreso_forma_pago = _safe(payload, "ingreso", "forma_pago")
    ingreso_iban = _safe(payload, "ingreso", "iban")
    importe_euros = (
        _safe(payload, "autoliquidacion", "importe_euros")
        or _safe(payload, "autoliquidacion", "importe")
        or _safe(payload, "autoliquidacion", "importe_complementaria")
    )
    fecha_dia, fecha_mes, fecha_anio = _split_date_parts(fecha_decl)
    fecha_nacimiento_dia, fecha_nacimiento_mes, fecha_nacimiento_anio = (
        _split_date_parts(fecha_nac)
    )
    return {
        "nif_nie": nif_nie,
        "nif_nie_prefix": nie_prefix,
        "nif_nie_number": nie_number,
        "nif_nie_suffix": nie_suffix,
        "pasaporte": _safe(payload, "identificacion", "pasaporte"),
        "nombre_apellidos": normalized_nombre_apellidos or nombre_apellidos,
        "primer_apellido": explicit_apellido1 or apellido1,
        "segundo_apellido": explicit_apellido2 or apellido2,
        "nombre": explicit_nombre or nombre,
        "sexo": _safe(payload, "extra", "sexo"),
        "tipo_via": tipo_via,
        "nombre_via": nombre_via,
        "domicilio_en_espana": domicilio_en_espana,
        "numero": numero,
        "escalera": escalera,
        "piso": piso,
        "puerta": puerta,
        "piso_puerta": piso_puerta,
        "telefono": _safe(payload, "domicilio", "telefono"),
        "municipio": _safe(payload, "domicilio", "municipio"),
        "provincia": _safe(payload, "domicilio", "provincia"),
        "cp": _safe(payload, "domicilio", "cp"),
        "localidad": _safe(payload, "declarante", "localidad"),
        "fecha": fecha_decl,
        "fecha_dia": fecha_dia,
        "fecha_mes": fecha_mes,
        "fecha_anio": fecha_anio,
        "importe_euros": importe_euros,
        "forma_pago": ingreso_forma_pago,
        "iban": ingreso_iban,
        "email": _safe(payload, "extra", "email"),
        "fecha_nacimiento": fecha_nac,
        "fecha_nacimiento_dia": fecha_nacimiento_dia,
        "fecha_nacimiento_mes": fecha_nacimiento_mes,
        "fecha_nacimiento_anio": fecha_nacimiento_anio,
        "nacionalidad": nacionalidad,
        "pais_nacimiento": _safe(payload, "extra", "pais_nacimiento"),
        "estado_civil": _safe(payload, "extra", "estado_civil"),
        "lugar_nacimiento": _safe(payload, "extra", "lugar_nacimiento"),
        "nombre_padre": _safe(payload, "extra", "nombre_padre"),
        "nombre_madre": _safe(payload, "extra", "nombre_madre"),
        "representante_legal": _safe(payload, "extra", "representante_legal"),
        "representante_documento": _safe(payload, "extra", "representante_documento"),
        "titulo_representante": _safe(payload, "extra", "titulo_representante"),
        "hijos_escolarizacion_espana": _safe(
            payload, "extra", "hijos_escolarizacion_espana"
        ),
    }


def build_autofill_value_map(payload: dict[str, Any]) -> dict[str, str]:
    return _build_value_map(payload)


build_date_split_field_values = _build_date_split_field_values
_normalize_door_token = normalize_door_token


def _canonical_from_placeholder(value: str) -> str:
    return _canonical_from_placeholder_impl(
        value,
        placeholder_re=PLACEHOLDER_RE,
        canonical_field_keys=CANONICAL_FIELD_KEY_SET,
    )


def _canonical_keys_from_placeholder_tokens(value: str) -> tuple[list[str], list[str]]:
    return _canonical_keys_from_placeholder_tokens_impl(
        value,
        placeholder_token_re=PLACEHOLDER_TOKEN_RE,
        canonical_field_keys=CANONICAL_FIELD_KEY_SET,
    )


def _select_canonical_for_composite_placeholder(keys: list[str]) -> str:
    return _select_canonical_for_composite_placeholder_impl(keys)


def _set_if_possible(page: Page, selectors: list[str], value: str) -> bool:
    if not value:
        return False
    for selector in selectors:
        try:
            loc = page.locator(selector).first
            if loc.count() == 0:
                continue
            if not loc.is_visible():
                continue
            if loc.is_disabled():
                continue
            loc.fill(value)
            return True
        except Exception:
            continue
    return False


def _select_if_possible(page: Page, selectors: list[str], value: str) -> bool:
    if not value:
        return False
    desired = value.strip().lower()
    desired_norm = _normalize_ascii_upper(value)
    for selector in selectors:
        try:
            loc = page.locator(selector).first
            if loc.count() == 0:
                continue
            if not loc.is_visible() or loc.is_disabled():
                continue
            options = loc.locator("option")
            if options.count() == 0:
                continue
            for i in range(options.count()):
                opt = options.nth(i)
                text = (opt.inner_text() or "").strip()
                val = (opt.get_attribute("value") or "").strip()
                text_norm = _normalize_ascii_upper(text)
                val_norm = _normalize_ascii_upper(val)
                if (
                    text.lower() == desired
                    or val.lower() == desired
                    or text_norm == desired_norm
                    or val_norm == desired_norm
                ):
                    if val:
                        loc.select_option(value=val)
                    else:
                        loc.select_option(label=text)
                    return True
                if (
                    desired in text.lower()
                    or (val and desired in val.lower())
                    or (desired_norm and desired_norm in text_norm)
                    or (desired_norm and desired_norm in val_norm)
                ):
                    if val:
                        loc.select_option(value=val)
                    else:
                        loc.select_option(label=text)
                    return True
        except Exception:
            continue
    return False


def _fill_by_label(page: Page, patterns: list[str], value: str) -> bool:
    if not value:
        return False
    for pattern in patterns:
        try:
            loc = page.get_by_label(re.compile(pattern, re.I)).first
            if loc.count() == 0:
                continue
            if not loc.is_visible():
                continue
            loc.fill(value)
            return True
        except Exception:
            continue
    return False


def inspect_form_fields(page: Page) -> list[dict[str, Any]]:
    rows = page.evaluate(
        """
        () => {
          const elements = Array.from(document.querySelectorAll("input, select, textarea"));
          const rows = [];
          for (const el of elements) {
            const type = (el.getAttribute("type") || "").toLowerCase();
            if (type === "hidden" || type === "submit" || type === "button" || type === "reset") continue;
            if (el.disabled) continue;
            let selector = "";
            if (el.id) {
              selector = "#" + CSS.escape(el.id);
            } else if (el.name) {
              selector = `${el.tagName.toLowerCase()}[name="${el.name.replace(/"/g, '\\"')}"]`;
            } else {
              continue;
            }
            let label = "";
            if (el.id) {
              const byFor = document.querySelector(`label[for="${el.id}"]`);
              if (byFor) label = (byFor.textContent || "").trim();
            }
            if (!label) {
              const wrapped = el.closest("label");
              if (wrapped) label = (wrapped.textContent || "").trim();
            }
            rows.push({
              selector,
              tag: el.tagName.toLowerCase(),
              type,
              id: el.id || "",
              name: el.getAttribute("name") || "",
              label,
              placeholder: el.getAttribute("placeholder") || "",
              aria_label: el.getAttribute("aria-label") || "",
              visible: !!(el.offsetWidth || el.offsetHeight || el.getClientRects().length),
            });
          }
          return rows;
        }
        """
    )
    return rows if isinstance(rows, list) else []


def extract_html_placeholder_mappings(
    page: Page,
) -> tuple[list[dict[str, Any]], list[str]]:
    rows = page.evaluate(
        """
        () => {
          const elements = Array.from(document.querySelectorAll("input, select, textarea"));
          const out = [];
          for (const el of elements) {
            const type = (el.getAttribute("type") || "").toLowerCase();
            if (type === "hidden" || type === "submit" || type === "button" || type === "reset") continue;
            if (el.disabled) continue;
            let selector = "";
            if (el.id) selector = "#" + CSS.escape(el.id);
            else if (el.name) selector = `${el.tagName.toLowerCase()}[name="${el.name.replace(/"/g, '\\"')}"]`;
            else continue;
            const value = (el.value || "").trim();
            if (!value) continue;
            out.push({ selector, value });
          }
          return out;
        }
        """
    )
    mappings: list[dict[str, Any]] = []
    unknown_vars: list[str] = []
    for row in rows or []:
        selector = str(row.get("selector") or "").strip()
        value = str(row.get("value") or "").strip()
        if not selector or not value:
            continue
        key = _canonical_from_placeholder(value)
        if key:
            mappings.append(
                {
                    "selector": selector,
                    "canonical_key": key,
                    "source": "placeholder",
                    "confidence": 1.0,
                }
            )
            continue
        m = PLACEHOLDER_RE.fullmatch(value)
        if m:
            unknown_vars.append(m.group(1).strip().lower())
    return mappings, sorted(set(unknown_vars))


def inspect_pdf_fields_from_bytes(data: bytes) -> list[dict[str, Any]]:
    doc = fitz.open(stream=data, filetype="pdf")
    rows: list[dict[str, Any]] = []
    try:
        for page_index, page in enumerate(doc):
            blocks = page.get_text("blocks") or []

            def guess_label_for_rect(
                rect: fitz.Rect, text_blocks: list[tuple[Any, ...]] = blocks
            ) -> str:
                candidates: list[tuple[float, str]] = []
                for block in text_blocks:
                    if len(block) < 5:
                        continue
                    x0, y0, x1, y1 = block[:4]
                    text = str(block[4] or "").strip()
                    if not text:
                        continue
                    # Prefer text on the left in the same row.
                    same_row = abs(((y0 + y1) / 2) - ((rect.y0 + rect.y1) / 2)) <= max(
                        12, rect.height * 0.8
                    )
                    on_left = x1 <= rect.x0 + 6
                    if same_row and on_left:
                        dist = (rect.x0 - x1) + abs(
                            ((y0 + y1) / 2) - ((rect.y0 + rect.y1) / 2)
                        )
                        candidates.append((dist, text))
                        continue
                    # Then text above field.
                    above = y1 <= rect.y0 + 4 and abs(x0 - rect.x0) <= max(
                        60, rect.width * 0.8
                    )
                    if above:
                        dist = (rect.y0 - y1) + abs(x0 - rect.x0)
                        candidates.append((dist + 40.0, text))
                if not candidates:
                    return ""
                candidates.sort(key=lambda item: item[0])
                return re.sub(r"\s+", " ", candidates[0][1]).strip(" :.-")

            widgets = page.widgets() or []
            for w in widgets:
                field_name = str((w.field_name or "")).strip()
                if not field_name:
                    continue
                rect = getattr(w, "rect", None) or fitz.Rect(0, 0, 0, 0)
                label_guess = guess_label_for_rect(rect)
                rows.append(
                    {
                        "selector": f"pdf:{field_name}",
                        "tag": "pdf_widget",
                        "type": str(getattr(w, "field_type_string", "") or ""),
                        "id": field_name,
                        "name": field_name,
                        "label": label_guess or field_name,
                        "pdf_field_name": field_name,
                        "pdf_label_guess": label_guess,
                        "placeholder": "",
                        "aria_label": "",
                        "visible": True,
                        "page_index": page_index,
                        "rect": {
                            "x0": round(float(rect.x0), 2),
                            "y0": round(float(rect.y0), 2),
                            "x1": round(float(rect.x1), 2),
                            "y1": round(float(rect.y1), 2),
                        },
                    }
                )
    finally:
        doc.close()
    return rows


def inspect_pdf_fields_from_url(
    target_url: str, *, timeout_ms: int = 20000
) -> list[dict[str, Any]]:
    data, _ = _fetch_pdf_bytes(target_url, timeout_ms)
    return inspect_pdf_fields_from_bytes(data)


def extract_pdf_placeholder_mappings_from_bytes(
    data: bytes,
) -> tuple[list[dict[str, Any]], list[str]]:
    doc = fitz.open(stream=data, filetype="pdf")
    mappings: list[dict[str, Any]] = []
    unknown_vars: list[str] = []
    try:
        for page in doc:
            widgets = page.widgets() or []
            for w in widgets:
                field_name = str((w.field_name or "")).strip()
                if not field_name:
                    continue
                value = str((w.field_value or "")).strip()
                if not value:
                    continue
                key = _canonical_from_placeholder(value)
                if key:
                    mappings.append(
                        {
                            "selector": f"pdf:{field_name}",
                            "canonical_key": key,
                            "field_kind": "text",
                            "match_value": "",
                            "checked_when": "",
                            "source": "template_pdf",
                            "confidence": 0.7,
                        }
                    )
                    continue
                keys, unknown = _canonical_keys_from_placeholder_tokens(value)
                if keys:
                    selected = _select_canonical_for_composite_placeholder(keys)
                    mappings.append(
                        {
                            "selector": f"pdf:{field_name}",
                            "canonical_key": selected,
                            "field_kind": "text",
                            "match_value": "",
                            "checked_when": "",
                            "source": "template_pdf",
                            "confidence": 0.65 if len(keys) > 1 else 0.7,
                        }
                    )
                unknown_vars.extend(unknown)
    finally:
        doc.close()
    return mappings, sorted(set(unknown_vars))


def extract_pdf_placeholder_mappings_from_url(
    target_url: str, *, timeout_ms: int = 20000
) -> tuple[list[dict[str, Any]], list[str]]:
    data, _ = _fetch_pdf_bytes(target_url, timeout_ms)
    mappings, unknown_vars = extract_pdf_placeholder_mappings_from_bytes(data)
    for item in mappings:
        item["source"] = "placeholder"
    return mappings, unknown_vars


def suggest_mappings_for_fields(
    fields: list[dict[str, Any]],
    payload: dict[str, Any],
    mapping_hints: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    values = _build_value_map(payload)
    hints = mapping_hints or {}

    patterns: dict[str, list[str]] = {
        "nif_nie": [r"nif", r"nie", r"document", r"identidad"],
        "pasaporte": [r"pasaport", r"passport"],
        "primer_apellido": [
            r"primerapellido",
            r"apellido1",
            r"firstsurname",
            r"razonsocial",
        ],
        "segundo_apellido": [
            r"segundoapellido",
            r"2apellido",
            r"apellido2",
            r"secondsurname",
        ],
        "nombre": [r"^nombre$", r"nombre\*$", r"name", r"forename"],
        "nombre_apellidos": [r"nombre", r"apellidos", r"razonsocial", r"fullname"],
        "sexo": [r"sexo", r"sex"],
        "tipo_via": [r"tipovia", r"calleplaza", r"avda"],
        "nombre_via": [r"nombrevia", r"viapublica"],
        "domicilio_en_espana": [r"domicilioenespana", r"domicilio", r"direccion"],
        "numero": [r"numero", r"\bnum\b"],
        "escalera": [r"escalera", r"\besc\b"],
        "piso": [r"piso", r"planta"],
        "puerta": [r"puerta", r"\bpta\b"],
        "telefono": [r"telefono", r"phone", r"movil"],
        "municipio": [r"municipio", r"ciudad", r"city"],
        "provincia": [r"provincia", r"province"],
        "cp": [r"cpostal", r"codigopostal", r"\bcp\b", r"postal"],
        "localidad": [r"localidad"],
        "fecha": [r"fecha"],
        "fecha_dia": [r"fecha", r"dia"],
        "fecha_mes": [r"fecha", r"mes"],
        "fecha_anio": [r"fecha", r"ano", r"año"],
        "email": [r"email", r"correo", r"mail"],
        "fecha_nacimiento": [r"fechanac", r"birth"],
        "fecha_nacimiento_dia": [r"fechanac", r"dia"],
        "fecha_nacimiento_mes": [r"fechanac", r"mes"],
        "fecha_nacimiento_anio": [r"fechanac", r"ano", r"año", r"year"],
        "nacionalidad": [r"nacionalidad", r"nationality"],
        "pais_nacimiento": [r"pais", r"country"],
        "estado_civil": [r"estadocivil", r"civil"],
        "lugar_nacimiento": [r"lugarnac", r"birthplace"],
        "nombre_padre": [r"padre", r"father"],
        "nombre_madre": [r"madre", r"mother"],
        "representante_legal": [r"representantelegal", r"representante"],
        "representante_documento": [
            r"dni",
            r"pas",
            r"dniniepas",
            r"documentorepresentante",
        ],
        "titulo_representante": [r"titulo"],
        "hijos_escolarizacion_espana": [r"hijas", r"hijos", r"escolarizacion"],
    }

    suggestions: list[dict[str, Any]] = []
    for field in fields or []:
        selector = str(field.get("selector") or "").strip()
        field_type = str(field.get("type") or "").lower()
        field_kind = "text"
        if field_type == "radio":
            field_kind = "radio"
        elif field_type == "checkbox":
            field_kind = "checkbox"
        elif str(field.get("tag") or "").lower() == "select":
            field_kind = "select"
        label_signal = _normalize_signal(
            " ".join(
                [
                    str(field.get("label") or ""),
                    str(field.get("name") or ""),
                    str(field.get("id") or ""),
                    str(field.get("placeholder") or ""),
                    str(field.get("aria_label") or ""),
                ]
            )
        )
        from_hint = hints.get(selector, "")
        canonical_key = ""
        confidence = 0.0
        source = "heuristic"
        if from_hint in CANONICAL_FIELD_KEYS:
            canonical_key = from_hint
            confidence = 0.99
            source = "learned"
        else:
            best_score = 0
            for key, key_patterns in patterns.items():
                score = 0
                for pat in key_patterns:
                    if re.search(pat, label_signal):
                        score += 1
                if score > best_score:
                    best_score = score
                    canonical_key = key
            if best_score > 0:
                confidence = min(0.85, 0.5 + best_score * 0.15)
            else:
                canonical_key = ""
                confidence = 0.0
        suggestions.append(
            {
                **field,
                "canonical_key": canonical_key,
                "field_kind": field_kind,
                "confidence": round(confidence, 2),
                "source": source,
                "value_preview": values.get(canonical_key, "") if canonical_key else "",
            }
        )
    order_map = {key: idx for idx, key in enumerate(CANONICAL_FILL_PRIORITY)}
    suggestions.sort(
        key=lambda item: (
            order_map.get(str(item.get("canonical_key") or ""), 999),
            str(item.get("selector") or ""),
        )
    )
    return suggestions


def _split_name_for_spanish_fields(
    full_name: str, nationality: str = ""
) -> tuple[str, str, str]:
    raw = (full_name or "").strip()
    if not raw:
        return "", "", ""

    # If OCR produced "APELLIDOS, NOMBRE", respect that explicitly.
    if "," in raw:
        left, right = [x.strip() for x in raw.split(",", 1)]
        surname_tokens = [t for t in re.split(r"\s+", left) if t]
        given = right
        surname1 = surname_tokens[0] if surname_tokens else ""
        surname2 = " ".join(surname_tokens[1:]) if len(surname_tokens) > 1 else ""
        return surname1, surname2, given

    tokens = [t for t in re.split(r"\s+", raw) if t]
    if not tokens:
        return "", "", ""
    if len(tokens) == 1:
        return tokens[0], "", ""
    if len(tokens) == 2:
        return tokens[0], "", tokens[1]

    nat = (nationality or "").strip().upper()
    is_spanish = nat in {"ESP", "ESPAÑA", "ESPANA", "SPAIN"}
    if is_spanish:
        # Spanish pattern: APELLIDO1 APELLIDO2 NOMBRE...
        return tokens[0], tokens[1], " ".join(tokens[2:])

    # Non-Spanish default: SURNAME + all remaining name parts.
    return tokens[0], "", " ".join(tokens[1:])


def _normalize_nationality_for_spanish_select(value: str) -> str:
    v = (value or "").strip().upper()
    if not v:
        return ""
    code_map = {
        "UKR": "UCRANIA",
        "ESP": "ESPAÑA",
        "DEU": "ALEMANIA",
        "FRA": "FRANCIA",
        "ITA": "ITALIA",
        "PRT": "PORTUGAL",
        "POL": "POLONIA",
        "ROU": "RUMANIA",
        "RUS": "RUSIA",
    }
    return code_map.get(v, value)


def _save_html_snapshot(page: Page, out_dir: Path, prefix: str) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = (
        out_dir / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{_slugify(prefix)}.html"
    )
    path.write_text(page.content(), encoding="utf-8")
    return path


def _save_screenshot(page: Page, out_dir: Path, prefix: str) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = (
        out_dir / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{_slugify(prefix)}.png"
    )
    page.screenshot(path=str(path), full_page=True)
    return path


def _append_filled(filled: list[str], field: str) -> None:
    if field and field not in filled:
        filled.append(field)


def _rule_context(values: dict[str, str]) -> dict[str, str]:
    return _rule_context_impl(values)


def _eval_checked_when(rule: str, context: dict[str, str]) -> bool | None:
    return _eval_checked_when_impl(rule, context)


def _set_check_if_possible(page: Page, selectors: list[str], checked: bool) -> bool:
    for selector in selectors:
        try:
            loc = page.locator(selector).first
            if loc.count() == 0:
                continue
            if not loc.is_visible() or loc.is_disabled():
                continue
            typ = str(loc.get_attribute("type") or "").lower()
            if typ in {"checkbox", "radio"}:
                if checked:
                    loc.check()
                else:
                    loc.uncheck()
                return True
            page.evaluate(
                """([sel, state]) => {
                    const el = document.querySelector(sel);
                    if (!el) return false;
                    if (el.type === "checkbox" || el.type === "radio") {
                        el.checked = !!state;
                        el.dispatchEvent(new Event("input", { bubbles: true }));
                        el.dispatchEvent(new Event("change", { bubbles: true }));
                        return true;
                    }
                    return false;
                }""",
                [selector, checked],
            )
            return True
        except Exception:
            continue
    return False


def _apply_explicit_mappings(
    page: Page,
    values: dict[str, str],
    filled: list[str],
    mappings: list[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    applied: list[dict[str, Any]] = []
    context = _rule_context(values)
    for item in mappings or []:
        selector = str(item.get("selector") or "").strip()
        canonical_key = str(item.get("canonical_key") or "").strip()
        field_kind = str(item.get("field_kind") or "text").strip().lower()
        if not selector:
            continue
        ok = False
        reason = "rule_evaluated_true"
        if field_kind in {"checkbox", "radio"}:
            checked_when = str(item.get("checked_when") or "").strip()
            match_value = str(item.get("match_value") or "").strip()
            result = _eval_checked_when(checked_when, context)
            if result is None:
                continue
            expected_state = bool(result and match_value)
            ok = _set_check_if_possible(page, [selector], expected_state)
            reason = "rule_evaluated_true" if expected_state else "rule_evaluated_false"
        else:
            if canonical_key not in CANONICAL_FIELD_KEYS:
                continue
            value = values.get(canonical_key, "")
            if not value:
                continue
            if field_kind == "select":
                ok = _select_if_possible(page, [selector], value)
                if not ok and canonical_key == "provincia":
                    # In strict-template mode payload province can be non-canonical
                    # (e.g. OCR/demo value). Fallback to CP-derived Spanish province.
                    inferred = _infer_spanish_province_from_cp(values.get("cp", ""))
                    if inferred:
                        ok = _select_if_possible(page, [selector], inferred)
                        if ok:
                            reason = "cp_inferred_fallback"
            else:
                ok = _select_if_possible(page, [selector], value) or _set_if_possible(
                    page, [selector], value
                )
        if ok:
            _append_filled(filled, canonical_key)
            applied.append(
                {
                    "selector": selector,
                    "canonical_key": canonical_key,
                    "field_kind": field_kind,
                    "source": str(item.get("source") or "manual"),
                    "confidence": float(item.get("confidence") or 1.0),
                    "reason": reason,
                }
            )
    return applied


def _apply_adapter_admin_tasas_pdf(
    page: Page, values: dict[str, str], filled: list[str]
) -> None:
    surname1, surname2, given_name = _split_name_for_spanish_fields(
        values.get("nombre_apellidos", ""),
        values.get("nacionalidad", ""),
    )
    nationality = _normalize_nationality_for_spanish_select(
        values.get("nacionalidad", "")
    )

    if values.get("nif_nie") and _set_if_possible(
        page, ["#Ctrl_NIFRem", "input[name='Ctrl_NIFRem']"], values["nif_nie"]
    ):
        _append_filled(filled, "nif_nie")
    if surname1 and _set_if_possible(
        page, ["#Ctrl_Apellido1", "input[name='Ctrl_Apellido1']"], surname1
    ):
        _append_filled(filled, "primer_apellido")
    if surname2 and _set_if_possible(
        page, ["#Ctrl_Apellido2", "input[name='Ctrl_Apellido2']"], surname2
    ):
        _append_filled(filled, "segundo_apellido")
    if given_name and _set_if_possible(
        page, ["#Ctrl_NombreRem", "input[name='Ctrl_NombreRem']"], given_name
    ):
        _append_filled(filled, "nombre")
    if nationality and _select_if_possible(
        page,
        ["#Ctrl_SelNacionalidad", "select[name='Ctrl_SelNacionalidad']"],
        nationality,
    ):
        _append_filled(filled, "nacionalidad")
    if values.get("tipo_via") and _select_if_possible(
        page,
        ["#Ctrl_TipoViaDom", "select[name='Ctrl_TipoViaDom']"],
        values["tipo_via"],
    ):
        _append_filled(filled, "tipo_via")
    if values.get("nombre_via") and _set_if_possible(
        page, ["#Ctrl_ViaDom", "input[name='Ctrl_ViaDom']"], values["nombre_via"]
    ):
        _append_filled(filled, "nombre_via")
    if values.get("numero") and _set_if_possible(
        page, ["#Ctrl_NumeroDom", "input[name='Ctrl_NumeroDom']"], values["numero"]
    ):
        _append_filled(filled, "numero")
    if values.get("escalera") and _set_if_possible(
        page,
        ["#Ctrl_EscaleraDom", "input[name='Ctrl_EscaleraDom']"],
        values["escalera"],
    ):
        _append_filled(filled, "escalera")
    if values.get("piso") and _set_if_possible(
        page, ["#Ctrl_PisoDom", "input[name='Ctrl_PisoDom']"], values["piso"]
    ):
        _append_filled(filled, "piso")
    if values.get("puerta") and _set_if_possible(
        page, ["#Ctrl_PuertaDom", "input[name='Ctrl_PuertaDom']"], values["puerta"]
    ):
        _append_filled(filled, "puerta")
    if values.get("municipio") and _set_if_possible(
        page,
        ["#Ctrl_MunicipioDom", "input[name='Ctrl_MunicipioDom']"],
        values["municipio"],
    ):
        _append_filled(filled, "municipio")
    province_selected = False
    if values.get("provincia"):
        province_selected = _select_if_possible(
            page,
            ["#Ctrl_ProvinciaDom", "select[name='Ctrl_ProvinciaDom']"],
            values["provincia"],
        )
    if not province_selected:
        inferred_province = _infer_spanish_province_from_cp(values.get("cp", ""))
        if inferred_province:
            province_selected = _select_if_possible(
                page,
                ["#Ctrl_ProvinciaDom", "select[name='Ctrl_ProvinciaDom']"],
                inferred_province,
            )
    if province_selected:
        _append_filled(filled, "provincia")
    if values.get("cp") and _set_if_possible(
        page, ["#Ctrl_CPostalDom", "input[name='Ctrl_CPostalDom']"], values["cp"]
    ):
        _append_filled(filled, "cp")
    if values.get("telefono") and _set_if_possible(
        page,
        ["#Ctrl_TelefonoDom", "input[name='Ctrl_TelefonoDom']"],
        values["telefono"],
    ):
        _append_filled(filled, "telefono")


def _apply_adapter_generic_html(
    page: Page, values: dict[str, str], filled: list[str]
) -> None:
    surname1, surname2, given_name = _split_name_for_spanish_fields(
        values.get("nombre_apellidos", ""),
        values.get("nacionalidad", ""),
    )
    nationality = _normalize_nationality_for_spanish_select(
        values.get("nacionalidad", "")
    )

    if values.get("nif_nie") and _fill_by_label(
        page, [r"NIF\s*/\s*NIE", r"NIE", r"NIF"], values["nif_nie"]
    ):
        _append_filled(filled, "nif_nie")
    if surname1 and _fill_by_label(
        page, [r"Primer apellido", r"Raz[oó]n Social"], surname1
    ):
        _append_filled(filled, "primer_apellido")
    if surname2 and _fill_by_label(page, [r"Segundo apellido"], surname2):
        _append_filled(filled, "segundo_apellido")
    if given_name and _fill_by_label(page, [r"^Nombre"], given_name):
        _append_filled(filled, "nombre")
    if nationality:
        if _select_if_possible(
            page,
            [
                "select[name*='nacionalidad' i]",
                "select[id*='nacionalidad' i]",
            ],
            nationality,
        ):
            _append_filled(filled, "nacionalidad")
        elif _fill_by_label(page, [r"Nacionalidad"], nationality):
            _append_filled(filled, "nacionalidad")
    if values.get("tipo_via"):
        if (
            _select_if_possible(
                page,
                [
                    "select[name*='via' i]",
                    "select[id*='via' i]",
                    "select[name*='calle' i]",
                ],
                values["tipo_via"],
            )
            or _set_if_possible(
                page,
                [
                    "#calle",
                    "input[name='calle']",
                    "input[id='calle']",
                ],
                values["tipo_via"],
            )
            or _fill_by_label(
                page, [r"Tipo\s+de\s+v[ií]a", r"Calle/plaza/Avda"], values["tipo_via"]
            )
        ):
            _append_filled(filled, "tipo_via")
    if values.get("nombre_via") and _fill_by_label(
        page,
        [r"Nombre de la v[ií]a p[uú]blica", r"v[ií]a p[uú]blica"],
        values["nombre_via"],
    ):
        _append_filled(filled, "nombre_via")
    if values.get("numero") and _fill_by_label(
        page, [r"Num", r"N[uú]m"], values["numero"]
    ):
        _append_filled(filled, "numero")
    if values.get("escalera") and _fill_by_label(page, [r"Esc"], values["escalera"]):
        _append_filled(filled, "escalera")
    if values.get("piso") and _fill_by_label(page, [r"Piso"], values["piso"]):
        _append_filled(filled, "piso")
    if values.get("puerta") and _fill_by_label(page, [r"Pta"], values["puerta"]):
        _append_filled(filled, "puerta")
    if values.get("municipio") and _fill_by_label(
        page, [r"Municipio"], values["municipio"]
    ):
        _append_filled(filled, "municipio")
    if values.get("provincia"):
        if _select_if_possible(
            page,
            [
                "select[name*='provincia' i]",
                "select[id*='provincia' i]",
            ],
            values["provincia"],
        ):
            _append_filled(filled, "provincia")
        elif _fill_by_label(page, [r"Provincia"], values["provincia"]):
            _append_filled(filled, "provincia")
    if values.get("cp") and _fill_by_label(
        page, [r"C\.?\s*Postal", r"C[oó]digo postal", r"CP"], values["cp"]
    ):
        _append_filled(filled, "cp")
    if values.get("telefono") and _fill_by_label(
        page, [r"Tel[eé]fono", r"Phone"], values["telefono"]
    ):
        _append_filled(filled, "telefono")

    mapping: list[tuple[str, list[str], list[str]]] = [
        (
            "email",
            [
                "#email",
                "input[type='email']",
                "input[name*='mail' i]",
                "input[name*='email' i]",
            ],
            [r"mail", r"email", r"correo"],
        ),
        (
            "fecha",
            ["#fecha", "input[name*='fecha' i]", "input[type='date']"],
            [r"fecha"],
        ),
        (
            "nombre_apellidos",
            [
                "#full_name",
                "input[name*='full_name' i]",
                "input[name*='nombre_apellidos' i]",
            ],
            [r"nombre\s*y\s*apellidos", r"apellidos\s*y\s*nombre", r"full\s*name"],
        ),
    ]
    for key, selectors, labels in mapping:
        value = values.get(key, "")
        if not value:
            continue
        if _set_if_possible(page, selectors, value) or _fill_by_label(
            page, labels, value
        ):
            _append_filled(filled, key)


def _dismiss_open_datepicker(page: Page) -> None:
    try:
        page.evaluate(
            """
            () => {
              const input = document.querySelector("#fecha, input[name='fecha']");
              if (input) {
                input.dispatchEvent(new Event("change", { bubbles: true }));
                input.dispatchEvent(new Event("blur", { bubbles: true }));
                input.blur();
              }
              const closeBtn = document.querySelector("#ui-datepicker-div .ui-datepicker-close");
              if (closeBtn && closeBtn instanceof HTMLElement && closeBtn.offsetParent !== null) {
                closeBtn.click();
                return;
              }
              if (typeof window.jQuery === "function") {
                try { window.jQuery("#fecha").datepicker("hide"); } catch (e) {}
              }
              if (document.body && document.body instanceof HTMLElement) {
                document.body.click();
              }
            }
            """
        )
    except Exception:
        pass
    try:
        page.keyboard.press("Escape")
    except Exception:
        pass


def _pick_html_adapters(target_url: str) -> list[tuple[str, Any]]:
    parsed = urlparse(target_url)
    host = (parsed.netloc or "").lower()
    path = (parsed.path or "").lower()
    adapters: list[tuple[str, Any]] = []
    if "sede.administracionespublicas.gob.es" in host and path.startswith("/tasaspdf"):
        adapters.append(("admin_tasas_pdf", _apply_adapter_admin_tasas_pdf))
    adapters.append(("generic_html", _apply_adapter_generic_html))
    return adapters


def autofill_existing_html_page(
    page: Page,
    payload: dict[str, Any],
    out_dir: Path,
    *,
    explicit_mappings: list[dict[str, Any]] | None = None,
    strict_template: bool = False,
) -> dict[str, Any]:
    values = _build_value_map(payload)
    filled: list[str] = []
    attempted_adapters: list[str] = []
    applied_explicit = _apply_explicit_mappings(page, values, filled, explicit_mappings)

    if not strict_template:
        for adapter_name, adapter in _pick_html_adapters(page.url):
            attempted_adapters.append(adapter_name)
            try:
                adapter(page, values, filled)
            except Exception:
                LOGGER.exception("Adapter failed: %s", adapter_name)
    _dismiss_open_datepicker(page)

    screenshot_path = (
        _save_screenshot(page, out_dir, "target_html_autofill")
        if should_save_artifact_screenshots()
        else None
    )
    dom_snapshot_path = (
        _save_html_snapshot(page, out_dir, "target_html_autofill")
        if is_template_debug_capture_enabled()
        else None
    )
    return {
        "mode": "html_playwright",
        "adapter": attempted_adapters[0] if attempted_adapters else "unknown",
        "attempted_adapters": attempted_adapters,
        "applied_mappings": applied_explicit,
        "target_url": page.url,
        "filled_fields": filled,
        "screenshot": str(screenshot_path) if screenshot_path else "",
        "dom_snapshot": str(dom_snapshot_path) if dom_snapshot_path else "",
        "filled_pdf": "",
        "warnings": [],
    }


def _autofill_html_target(
    payload: dict[str, Any],
    target_url: str,
    out_dir: Path,
    *,
    timeout_ms: int,
    slowmo: int,
    headless: bool,
    explicit_mappings: list[dict[str, Any]] | None = None,
    strict_template: bool = False,
) -> dict[str, Any]:
    with sync_playwright() as p:
        browser = _launch_chromium(
            p,
            headless=headless,
            slow_mo=slowmo,
        )
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        page.set_default_timeout(timeout_ms)
        try:
            page.goto(target_url, wait_until="domcontentloaded")
            page.wait_for_timeout(800)
            return autofill_existing_html_page(
                page,
                payload,
                out_dir,
                explicit_mappings=explicit_mappings,
                strict_template=strict_template,
            )
        finally:
            context.close()
            browser.close()


def _pdf_value_for_field(field_name: str, value_map: dict[str, str]) -> str:
    return _pdf_value_for_field_impl(
        field_name,
        value_map,
        norm_text=_norm_text,
        strip_extra_spaces=_strip_extra_spaces,
    )


def _build_nif_split_field_map(
    doc: fitz.Document, explicit_by_field: dict[str, str], value_map: dict[str, str]
) -> dict[str, str]:
    return _build_nif_split_field_map_impl(doc, explicit_by_field, value_map)


def infer_pdf_checkbox_expected(
    field_name: str, mapped_key: str, value_map: dict[str, str]
) -> bool | None:
    return _infer_pdf_checkbox_expected_impl(
        field_name, mapped_key, value_map, norm_text=_norm_text
    )


def _should_ignore_pdf_mapping(
    field_name: str, mapped_key: str, source: str, widget_type: str
) -> bool:
    return _should_ignore_pdf_mapping_impl(field_name, mapped_key, source, widget_type)


def _autofill_pdf_target(
    payload: dict[str, Any],
    target_url: str,
    out_dir: Path,
    *,
    timeout_ms: int,
    explicit_mappings: list[dict[str, Any]] | None = None,
    strict_template: bool = False,
) -> dict[str, Any]:
    data, _ = _fetch_pdf_bytes(target_url, timeout_ms)

    out_dir.mkdir(parents=True, exist_ok=True)
    source_path: Path | None = None
    if is_template_debug_capture_enabled():
        source_path = (
            out_dir / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_target_source.pdf"
        )
        source_path.write_bytes(data)

    value_map = _build_value_map(payload)
    doc = fitz.open(stream=data, filetype="pdf")
    filled_count = 0
    touched_fields: list[str] = []
    applied_mappings: list[dict[str, Any]] = []
    explicit_by_field: dict[str, dict[str, Any]] = {}
    for item in explicit_mappings or []:
        selector = str(item.get("selector") or "").strip()
        key = str(item.get("canonical_key") or "").strip()
        field_kind = str(item.get("field_kind") or "text").strip().lower()
        match_value = str(item.get("match_value") or "").strip()
        checked_when = str(item.get("checked_when") or "").strip()
        source = str(item.get("source") or "").strip()
        if selector.startswith("pdf:"):
            explicit_by_field[selector[4:]] = {
                "key": key,
                "source": source,
                "field_kind": field_kind,
                "match_value": match_value,
                "checked_when": checked_when,
            }
        elif selector:
            explicit_by_field[selector] = {
                "key": key,
                "source": source,
                "field_kind": field_kind,
                "match_value": match_value,
                "checked_when": checked_when,
            }
    try:
        strict_explicit_mode = strict_template or bool(explicit_by_field)
        nif_split_field_map = _build_nif_split_field_map(
            doc,
            {
                name: str(meta.get("key") or "")
                for name, meta in explicit_by_field.items()
                if str(meta.get("key") or "")
            },
            value_map,
        )
        date_split_field_values = _build_date_split_field_values(
            doc,
            {
                name: str(meta.get("key") or "")
                for name, meta in explicit_by_field.items()
                if str(meta.get("key") or "")
            },
            value_map,
        )
        context = _rule_context(value_map)
        sexo_value = (value_map.get("sexo", "") or "").strip().upper()
        sexo_fields = {
            name
            for name, meta in explicit_by_field.items()
            if str(meta.get("key") or "").strip().lower() == "sexo"
            and str(meta.get("field_kind") or "").strip().lower()
            in {"checkbox", "radio"}
        }
        estado_civil_fields = {
            name
            for name, meta in explicit_by_field.items()
            if str(meta.get("key") or "").strip().lower() == "estado_civil"
            and str(meta.get("field_kind") or "").strip().lower()
            in {"checkbox", "radio"}
        }
        if not sexo_fields or not estado_civil_fields:
            detected_sexo_fields: set[str] = set()
            detected_estado_fields: set[str] = set()
            for p in doc:
                for widget in p.widgets() or []:
                    widget_type = str(
                        getattr(widget, "field_type_string", "") or ""
                    ).lower()
                    if "check" not in widget_type:
                        continue
                    name = str((widget.field_name or "")).strip()
                    upper_name = name.upper()
                    if upper_name in {"H", "M", "CHKBOX"}:
                        detected_sexo_fields.add(name)
                    if upper_name in {"C", "V", "D", "SP", "CHKBOX-0"}:
                        detected_estado_fields.add(name)
            if not sexo_fields:
                sexo_fields = detected_sexo_fields
            if not estado_civil_fields:
                estado_civil_fields = detected_estado_fields
        estado_civil_value = (value_map.get("estado_civil", "") or "").strip().upper()
        sexo_target_by_field: dict[str, bool] = {}
        estado_target_by_field: dict[str, bool] = {}

        def _build_checkbox_group_targets(
            field_names: set[str],
            logical_order: list[str],
            selected_code: str,
            *,
            allow_two_state_sex_fallback: bool = False,
        ) -> dict[str, bool]:
            if not field_names:
                return {}
            positioned: list[tuple[str, float, float]] = []
            for p in doc:
                for widget in p.widgets() or []:
                    fname = str((widget.field_name or "")).strip()
                    if fname not in field_names:
                        continue
                    wtype = str(getattr(widget, "field_type_string", "") or "").lower()
                    if "check" not in wtype:
                        continue
                    rect = widget.rect
                    positioned.append((fname, float(rect.x0), float(rect.y0)))
            if not positioned:
                return {}
            positioned.sort(key=lambda item: (item[2], item[1]))
            # Keep only the first visual row if parser picks noisy duplicates on other rows.
            first_row_y = positioned[0][2]
            row = [item for item in positioned if abs(item[2] - first_row_y) <= 25.0]
            row.sort(key=lambda item: item[1])
            effective_order = logical_order
            # Some official templates (e.g. EX-11) expose only two sexo checkboxes (H/M).
            # In that case we must not shift by X and should map left->H, right->M.
            if (
                allow_two_state_sex_fallback
                and len(row) == 2
                and len(logical_order) >= 2
            ):
                effective_order = ["H", "M"]
            out: dict[str, bool] = {}
            for idx, (fname, _, _) in enumerate(row):
                if idx >= len(effective_order):
                    break
                out[fname] = selected_code == effective_order[idx]
            return out

        sexo_target_by_field = _build_checkbox_group_targets(
            sexo_fields,
            ["X", "H", "M"],
            sexo_value,
            allow_two_state_sex_fallback=True,
        )
        estado_civil_target = "SP" if estado_civil_value == "SP" else estado_civil_value
        estado_target_by_field = _build_checkbox_group_targets(
            estado_civil_fields,
            ["S", "C", "V", "D", "SP"],
            estado_civil_target,
        )

        def _set_checkbox(widget, checked: bool) -> None:
            on_state = "Yes"
            try:
                if hasattr(widget, "on_state"):
                    on_state = str(widget.on_state() or "Yes")
            except Exception:
                on_state = "Yes"
            target_value = on_state if checked else "Off"
            try:
                # For many government PDFs, explicit export values ("Yes"/"Off")
                # are more reliable than bool assignment for checkbox state.
                widget.field_value = target_value
                widget.update()
                return
            except Exception:
                pass
            try:
                widget.field_value = bool(checked)
                widget.update()
            except Exception:
                LOGGER.exception(
                    "Failed setting checkbox field '%s'",
                    getattr(widget, "field_name", ""),
                )

        for page in doc:
            widgets = page.widgets() or []
            for w in widgets:
                field_name = (w.field_name or "").strip()
                if not field_name:
                    continue
                widget_type = str(getattr(w, "field_type_string", "") or "").lower()
                mapping_meta = explicit_by_field.get(field_name, {})
                mapped_key = str(mapping_meta.get("key") or "")
                mapped_source = str(mapping_meta.get("source") or "")
                field_kind = str(mapping_meta.get("field_kind") or "").lower()
                if field_name in nif_split_field_map:
                    mapped_key = nif_split_field_map[field_name]
                    mapped_source = "nif_split_inferred"
                if _should_ignore_pdf_mapping(
                    field_name, mapped_key, mapped_source, widget_type
                ):
                    mapped_key = ""
                if "check" in widget_type:
                    if field_name in sexo_fields:
                        checked_value = sexo_target_by_field.get(field_name)
                        if checked_value is None:
                            checked_value = False
                        _set_checkbox(w, checked_value)
                        filled_count += 1
                        touched_fields.append(field_name)
                        continue
                    if field_name in estado_civil_fields:
                        checked_value = estado_target_by_field.get(field_name)
                        if checked_value is None:
                            checked_value = False
                        _set_checkbox(w, checked_value)
                        filled_count += 1
                        touched_fields.append(field_name)
                        continue
                    checked_value = None
                    if field_kind in {"checkbox", "radio"}:
                        checked_value = _eval_checked_when(
                            str(mapping_meta.get("checked_when") or ""), context
                        )
                        if checked_value is not None:
                            checked_value = bool(
                                checked_value
                                and str(mapping_meta.get("match_value") or "").strip()
                            )
                    if checked_value is None:
                        checked_value = infer_pdf_checkbox_expected(
                            field_name, mapped_key, value_map
                        )
                    if checked_value is not None:
                        _set_checkbox(w, checked_value)
                        filled_count += 1
                        touched_fields.append(field_name)
                        if field_kind in {"checkbox", "radio"}:
                            applied_mappings.append(
                                {
                                    "selector": f"pdf:{field_name}",
                                    "canonical_key": mapped_key,
                                    "field_kind": field_kind,
                                    "source": mapped_source or "explicit",
                                    "confidence": float(
                                        mapping_meta.get("confidence") or 1.0
                                    ),
                                    "reason": (
                                        "rule_evaluated_true"
                                        if checked_value
                                        else "rule_evaluated_false"
                                    ),
                                }
                            )
                        continue
                if field_name in date_split_field_values:
                    value = date_split_field_values[field_name]
                elif "nombreyapellidosdeltitular" in _norm_text(field_name):
                    value = _strip_extra_spaces(
                        " ".join(
                            x
                            for x in [
                                value_map.get("nombre", ""),
                                value_map.get("primer_apellido", ""),
                                value_map.get("segundo_apellido", ""),
                            ]
                            if x
                        )
                    )
                elif mapped_key in CANONICAL_FIELD_KEYS:
                    value = value_map.get(mapped_key, "")
                else:
                    if strict_explicit_mode:
                        continue
                    else:
                        value = _pdf_value_for_field(field_name, value_map)
                if not value:
                    continue
                try:
                    w.field_value = value
                    w.update()
                    filled_count += 1
                    touched_fields.append(field_name)
                    if mapped_key:
                        applied_mappings.append(
                            {
                                "selector": f"pdf:{field_name}",
                                "canonical_key": mapped_key,
                                "field_kind": field_kind or "text",
                                "source": "explicit",
                                "confidence": 1.0,
                                "reason": "rule_evaluated_true",
                            }
                        )
                except Exception:
                    LOGGER.exception("Failed setting PDF field '%s'", field_name)

        if hasattr(doc, "need_appearances"):
            try:
                doc.need_appearances(True)
            except Exception:
                LOGGER.exception("Failed setting need_appearances on filled PDF.")
        flatten_widgets = os.getenv("PDF_FLATTEN_WIDGETS", "0").strip().lower() not in {
            "0",
            "false",
            "no",
            "off",
        }
        if flatten_widgets and hasattr(doc, "bake"):
            try:
                # Some viewers render checkbox appearances incorrectly even when
                # /V values are correct. Baking widgets makes visual output stable.
                doc.bake(annots=False, widgets=True)
            except Exception:
                LOGGER.exception("Failed baking widgets for filled PDF.")
        filled_pdf = (
            out_dir / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_target_filled.pdf"
        )
        doc.save(str(filled_pdf))

        screenshot_path: Path | None = None
        if should_save_artifact_screenshots():
            first_page = doc[0]
            pix = first_page.get_pixmap(dpi=160, alpha=False)
            screenshot_path = (
                out_dir
                / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_target_pdf_preview.png"
            )
            pix.save(str(screenshot_path))

        warnings: list[str] = []
        if filled_count == 0:
            warnings.append(
                "PDF has no matched fillable fields; saved original structure for manual completion."
            )
        if len(touched_fields) == 0 and len(doc) > 0:
            warnings.append(
                "No PDF widgets were filled. Check mappings and field names."
            )

        return {
            "mode": "pdf_pymupdf",
            "target_url": target_url,
            "filled_fields": touched_fields,
            "applied_mappings": applied_mappings,
            "screenshot": str(screenshot_path) if screenshot_path else "",
            # Keep API contract stable; in non-debug mode avoid extra dumps.
            "dom_snapshot": str(source_path) if source_path else "",
            "filled_pdf": str(filled_pdf),
            "warnings": warnings,
        }
    finally:
        doc.close()


def autofill_target_preview(
    payload: dict[str, Any],
    target_url: str,
    out_dir: Path,
    *,
    timeout_ms: int = 20000,
    slowmo: int = 80,
    headless: bool = True,
    explicit_mappings: list[dict[str, Any]] | None = None,
    strict_template: bool = False,
) -> dict[str, Any]:
    target = (target_url or "").strip()
    if not target:
        raise ValueError("target_url is required.")

    target_type = _infer_target_type(target)
    if target_type == "pdf":
        return _autofill_pdf_target(
            payload,
            target,
            out_dir,
            timeout_ms=timeout_ms,
            explicit_mappings=explicit_mappings,
            strict_template=strict_template,
        )
    return _autofill_html_target(
        payload,
        target,
        out_dir,
        timeout_ms=timeout_ms,
        slowmo=slowmo,
        headless=headless,
        explicit_mappings=explicit_mappings,
        strict_template=strict_template,
    )
