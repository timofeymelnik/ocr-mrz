from __future__ import annotations

import os
import time
from datetime import UTC, datetime
from typing import Any

from app.crm.mapper import build_crm_profile
from app.core.validators import normalize_payload_for_form


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _env_flag(name: str, default: bool) -> bool:
    raw = os.getenv(name, "1" if default else "0").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _stage(
    *,
    name: str,
    status: str,
    started_at: float,
    error: str = "",
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    finished_at = time.time()
    return {
        "name": name,
        "status": status,
        "started_at": datetime.fromtimestamp(started_at, UTC).isoformat(),
        "finished_at": datetime.fromtimestamp(finished_at, UTC).isoformat(),
        "duration_ms": int((finished_at - started_at) * 1000),
        "error": error,
        "details": details or {},
    }


def attach_pipeline_metadata(
    *,
    document: dict[str, Any],
    source_files: list[str],
    ocr_details: dict[str, Any],
    parse_stage: dict[str, Any],
    crm_stage: dict[str, Any],
    ocr_stage: dict[str, Any],
) -> dict[str, Any]:
    forms = document.get("forms") or {}
    form_keys = sorted(forms.keys()) if isinstance(forms, dict) else []

    crm_profile = build_crm_profile(document)
    form_payload = normalize_payload_for_form(document)

    document["crm_profile"] = crm_profile
    document["human_tasks"] = [
        {
            "task": "verify_filled_fields",
            "required": True,
            "description": "Human must verify all autofilled values before final submit/download.",
        },
        {
            "task": "submit_or_download_manually",
            "required": True,
            "description": "Human must perform the final submit/download step manually.",
        },
    ]
    document["pipeline"] = {
        "version": "1.0.0",
        "mode": "human_in_the_loop",
        "created_at": _now_iso(),
        "stages": [ocr_stage, parse_stage, crm_stage],
        "artifacts": {
            "source_files": source_files,
            "ocr": ocr_details,
            "form_keys": form_keys,
            "form_payload_for_playwright": form_payload,
            "template_source": "",
            "template_updated_at": "",
            "template_valid": False,
            "screenshot_enabled": _env_flag("SAVE_ARTIFACT_SCREENSHOTS", False),
        },
        "ui_contract": {
            "upload": {
                "supported_extensions": [".jpg", ".jpeg", ".png", ".pdf"],
                "expected_inputs": ["document_front", "document_back_optional", "overrides_optional"],
            },
            "manual_steps": ["verify_filled_fields", "submit_or_download_manually"],
            "autofill_scope": [
                "identificacion",
                "domicilio",
                "declarante",
                "ingreso",
            ],
        },
    }
    return document


def stage_start() -> float:
    return time.time()


def stage_success(name: str, started_at: float, details: dict[str, Any] | None = None) -> dict[str, Any]:
    return _stage(name=name, status="success", started_at=started_at, details=details)


def stage_failed(name: str, started_at: float, error: str, details: dict[str, Any] | None = None) -> dict[str, Any]:
    return _stage(name=name, status="failed", started_at=started_at, error=error, details=details)
