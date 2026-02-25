"""Service for filling opened browser sessions using resolved templates."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Awaitable, Callable, Protocol

from fastapi import HTTPException

from app.browser.template_mapping_service import TemplateMappingService


class CRMRepositoryProtocol(Protocol):
    """Protocol for CRM payload persistence used by browser fill service."""

    def save_edited_payload(
        self,
        *,
        document_id: str,
        payload: dict[str, Any],
        missing_fields: list[str],
    ) -> dict[str, Any]:
        """Persist edited payload and missing fields for document."""


class BrowserSessionFillService:
    """Orchestrates browser session fill flow and persistence side effects."""

    def __init__(
        self,
        *,
        read_or_bootstrap_record: Callable[[str], dict[str, Any]],
        write_record: Callable[[str, dict[str, Any]], None],
        safe_value: Callable[[Any], str],
        collect_validation_errors: Callable[[dict[str, Any], bool], list[str]],
        collect_validation_issues: Callable[
            [dict[str, Any], bool], list[dict[str, Any]]
        ],
        run_browser_call: Callable[..., Awaitable[Any]],
        get_browser_session_state: Callable[..., Any],
        fill_browser_session: Callable[..., Any],
        template_mapping_service: TemplateMappingService,
        crm_repo: CRMRepositoryProtocol,
        autofill_dir: Path,
        artifact_url_from_value: Callable[[Any], str],
        latest_artifact_url: Callable[[Path, str], str],
        should_save_artifact_screenshots_on_error: Callable[[], bool],
        logger_info: Callable[..., Any],
    ) -> None:
        """Initialize fill service with injected collaborators."""
        self._read_or_bootstrap_record = read_or_bootstrap_record
        self._write_record = write_record
        self._safe_value = safe_value
        self._collect_validation_errors = collect_validation_errors
        self._collect_validation_issues = collect_validation_issues
        self._run_browser_call = run_browser_call
        self._get_browser_session_state = get_browser_session_state
        self._fill_browser_session = fill_browser_session
        self._template_mapping_service = template_mapping_service
        self._crm_repo = crm_repo
        self._autofill_dir = autofill_dir
        self._artifact_url_from_value = artifact_url_from_value
        self._latest_artifact_url = latest_artifact_url
        self._should_save_artifact_screenshots_on_error = (
            should_save_artifact_screenshots_on_error
        )
        self._logger_info = logger_info

    async def fill_opened_session(
        self,
        *,
        document_id: str,
        payload: dict[str, Any],
        timeout_ms: int,
        fill_strategy: str,
    ) -> tuple[int, dict[str, Any]]:
        """Fill active browser session for document and return API payload."""
        record = self._read_or_bootstrap_record(document_id)
        session_id = self._safe_value(record.get("browser_session_id"))
        if not session_id:
            raise HTTPException(
                status_code=400,
                detail="Browser session is not opened. Click 'Перейти по адресу' first.",
            )

        missing_fields = self._collect_validation_errors(payload, False)
        validation_issues = self._collect_validation_issues(payload, False)
        out_dir = self._autofill_dir / document_id
        out_dir.mkdir(parents=True, exist_ok=True)

        try:
            session_state = await self._run_browser_call(
                self._get_browser_session_state,
                session_id,
            )
            current_url = self._safe_value(session_state.get("current_url"))
        except Exception:
            current_url = ""

        if not current_url:
            raise HTTPException(
                status_code=422,
                detail="Current URL is empty in browser session.",
            )

        resolution = self._template_mapping_service.resolve_for_url(current_url)
        if not resolution.is_valid:
            return (
                422,
                {
                    "status": "error",
                    "error_code": resolution.error_code,
                    "document_id": document_id,
                    "message": resolution.message,
                    "form_url": current_url,
                },
            )

        self._logger_info(
            "autofill.start document_id=%s session_id=%s url=%s template_source=%s mappings=%s missing_fields=%s validation_issues=%s",
            document_id,
            session_id,
            current_url,
            resolution.template_source,
            len(resolution.effective_mappings),
            len(missing_fields),
            len(validation_issues),
        )

        try:
            result = await self._run_browser_call(
                self._fill_browser_session,
                session_id,
                payload,
                out_dir,
                timeout_ms=timeout_ms,
                explicit_mappings=resolution.effective_mappings,
                fill_strategy=fill_strategy,
            )
        except Exception as exc:
            detail = str(exc) or exc.__class__.__name__
            screenshot_url = (
                self._latest_artifact_url(out_dir, "*.png")
                if self._should_save_artifact_screenshots_on_error()
                else ""
            )
            dom_snapshot_url = self._latest_artifact_url(out_dir, "*.html")
            record["payload"] = payload
            record["missing_fields"] = missing_fields
            record["autofill_preview"] = {
                "status": "error",
                "error": detail,
                "screenshot_url": screenshot_url,
                "dom_snapshot_url": dom_snapshot_url,
            }
            self._write_record(document_id, record)
            self._crm_repo.save_edited_payload(
                document_id=document_id,
                payload=payload,
                missing_fields=missing_fields,
            )
            return (
                422,
                {
                    "status": "error",
                    "error_code": "FILL_FAILED",
                    "document_id": document_id,
                    "message": "Fill in opened browser session failed.",
                    "form_url": record.get("target_url") or current_url,
                },
            )

        record["payload"] = payload
        record["missing_fields"] = missing_fields
        filled_fields = list(result.get("filled_fields", []) or [])
        mode = str(result.get("mode", "") or "")
        filled_pdf_url = self._artifact_url_from_value(result.get("filled_pdf"))
        record["autofill_preview"] = {
            "status": "ok",
            "mode": mode,
            "screenshot": result.get("screenshot", ""),
            "dom_snapshot": result.get("dom_snapshot", ""),
            "filled_pdf": self._safe_value(result.get("filled_pdf")),
            "warnings": result.get("warnings", []),
            "filled_fields": filled_fields,
        }
        self._write_record(document_id, record)
        self._crm_repo.save_edited_payload(
            document_id=document_id,
            payload=payload,
            missing_fields=missing_fields,
        )

        screenshot_url = self._artifact_url_from_value(result.get("screenshot"))
        dom_snapshot_url = self._artifact_url_from_value(result.get("dom_snapshot"))
        self._logger_info(
            "autofill.result document_id=%s mode=%s filled_fields=%s warnings=%s screenshot_url=%s dom_snapshot_url=%s filled_pdf_url=%s",
            document_id,
            mode,
            len(filled_fields),
            len(list(result.get("warnings", []) or [])),
            screenshot_url,
            dom_snapshot_url,
            filled_pdf_url,
        )

        if mode == "pdf_pymupdf" and len(filled_fields) == 0:
            return (
                422,
                {
                    "status": "error",
                    "error_code": "FILL_PARTIAL",
                    "document_id": document_id,
                    "message": "PDF was processed, but no fillable fields were matched.",
                    "form_url": (
                        result.get("current_url")
                        or record.get("target_url")
                        or current_url
                    ),
                },
            )

        return (
            200,
            {
                "document_id": document_id,
                "form_url": result.get("current_url")
                or record.get("target_url")
                or current_url,
                "filled_pdf_url": filled_pdf_url,
            },
        )
