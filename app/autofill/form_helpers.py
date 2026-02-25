"""Shared pure helpers extracted from form_filler monolith."""

from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import Any


def slugify(value: str) -> str:
    """Convert free text to file-safe lowercase slug."""
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


def is_blocked_page_html(html: str) -> bool:
    """Detect known block-page fragments in HTML content."""
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


def split_amount(amount: Any) -> tuple[str, str]:
    """Split numeric amount into euro and cent strings."""
    val = float(amount)
    euros = int(val)
    cents = int(round((val - euros) * 100))
    return str(euros), f"{cents:02d}"


def _safe_value(payload: dict[str, Any], *path: str) -> str:
    node: Any = payload
    for key in path:
        if not isinstance(node, dict):
            return ""
        node = node.get(key)
    if node is None:
        return ""
    return str(node).strip()


def download_filename(payload: dict[str, Any], suggested_name: str) -> str:
    """Build deterministic output filename for downloaded document."""
    prefix = _safe_value(payload, "download", "filename_prefix") or "tasa790_012"
    nif_nie = _safe_value(payload, "identificacion", "nif_nie") or "unknown"
    day = datetime.now().strftime("%Y%m%d")
    ext = Path(suggested_name or "document.pdf").suffix or ".pdf"
    return f"{prefix}_{nif_nie}_{day}{ext}"


def check_download_content(path: Path) -> tuple[bool, str]:
    """Validate downloaded artifact header is not HTML content."""
    head = path.read_bytes()[:512].lower()
    if b"<html" in head or b"<!doctype html" in head:
        return False, "Downloaded content appears to be HTML, not a document."
    return True, ""


def is_pdf_bytes(content: bytes) -> bool:
    """Return ``True`` when content looks like PDF bytes."""
    return content.startswith(b"%PDF")


def extract_known_server_error(body: bytes) -> str:
    """Extract known server-side validation errors from response body."""
    text = body.decode("utf-8", errors="ignore")
    normalized = text.lower()
    if "error en captcha" in normalized:
        return (
            "Server returned CAPTCHA error: invalid/expired captcha. "
            "Enter the NEW captcha shown by the page and retry."
        )
    if "debe introducir una forma de pago" in normalized:
        return "Server validation error: forma de pago not selected."
    if "debe seleccionar uno de los trámites" in normalized:
        return "Server validation error: trámite option not selected."
    return ""
