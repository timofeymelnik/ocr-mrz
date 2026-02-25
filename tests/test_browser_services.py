from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest
from fastapi import HTTPException

from app.browser.session_fill_service import BrowserSessionFillService
from app.browser.template_mapping_service import TemplateMappingService


class _FakeMappingsRepo:
    """In-memory template repository for tests."""

    def __init__(self, template: dict[str, Any] | None) -> None:
        self._template = template

    def get_latest_for_url(self, target_url: str) -> dict[str, Any] | None:
        _ = target_url
        return self._template


class _FakeCRMRepo:
    """Minimal CRM persistence stub for fill service tests."""

    def save_edited_payload(
        self,
        *,
        document_id: str,
        payload: dict[str, Any],
        missing_fields: list[str],
    ) -> dict[str, Any]:
        _ = (document_id, payload, missing_fields)
        return {}


def _safe(value: Any) -> str:
    """Normalize optional values into strings."""
    if value is None:
        return ""
    return str(value).strip()


def test_template_mapping_service_returns_effective_mappings() -> None:
    """Template service should normalize latest template mapping records."""
    service = TemplateMappingService(
        form_mapping_repo=_FakeMappingsRepo(
            {
                "source": "learned",
                "mappings": [
                    {
                        "selector": "#field-nie",
                        "canonical_key": "nif_nie",
                        "field_kind": "text",
                    }
                ],
            }
        ),
        safe_value=_safe,
        collect_validation_errors=lambda payload, require_tramite: [],
        collect_validation_issues=lambda payload, require_tramite: [],
    )

    resolution = service.resolve_for_url("https://example.com")

    assert resolution.is_valid is True
    assert resolution.template_source == "learned"
    assert len(resolution.effective_mappings) == 1
    assert resolution.effective_mappings[0]["selector"] == "#field-nie"


def test_template_mapping_service_returns_not_found_error() -> None:
    """Template service should return structured error for absent template."""
    service = TemplateMappingService(
        form_mapping_repo=_FakeMappingsRepo(None),
        safe_value=_safe,
        collect_validation_errors=lambda payload, require_tramite: [],
        collect_validation_issues=lambda payload, require_tramite: [],
    )

    status_code, payload = service.build_template_response(
        document_id="doc-1",
        current_url="https://example.com/form",
        payload={},
        fill_strategy="strict_template",
    )

    assert status_code == 422
    assert payload["error_code"] == "TEMPLATE_NOT_FOUND"


def test_browser_fill_service_requires_opened_session() -> None:
    """Fill service should reject fill request without opened browser session."""
    template_service = TemplateMappingService(
        form_mapping_repo=_FakeMappingsRepo(None),
        safe_value=_safe,
        collect_validation_errors=lambda payload, require_tramite: [],
        collect_validation_issues=lambda payload, require_tramite: [],
    )
    service = BrowserSessionFillService(
        read_or_bootstrap_record=lambda document_id: {"document_id": document_id},
        write_record=lambda document_id, record: None,
        safe_value=_safe,
        collect_validation_errors=lambda payload, require_tramite: [],
        collect_validation_issues=lambda payload, require_tramite: [],
        run_browser_call=lambda *args, **kwargs: asyncio.sleep(0),
        get_browser_session_state=lambda session_id: {},
        fill_browser_session=lambda *args, **kwargs: {},
        template_mapping_service=template_service,
        crm_repo=_FakeCRMRepo(),
        autofill_dir=Path("runtime/autofill"),
        artifact_url_from_value=lambda value: "",
        latest_artifact_url=lambda base_dir, pattern: "",
        should_save_artifact_screenshots_on_error=lambda: False,
        logger_info=lambda *args, **kwargs: None,
    )

    async def scenario() -> None:
        with pytest.raises(HTTPException):
            await service.fill_opened_session(
                document_id="doc-1",
                payload={},
                timeout_ms=1000,
                fill_strategy="strict_template",
            )

    asyncio.run(scenario())
