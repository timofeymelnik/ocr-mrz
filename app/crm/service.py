"""Business logic for CRM document endpoints."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Awaitable, Callable, Protocol

from fastapi import HTTPException


class CRMRepositoryProtocol(Protocol):
    """Protocol describing repository methods used by CRM service."""

    def search_documents(self, query: str, limit: int) -> list[dict[str, Any]]:
        """Search stored CRM documents with optional text query."""

    def get_document(self, document_id: str) -> dict[str, Any] | None:
        """Return CRM document by id, or ``None`` when it does not exist."""

    def delete_document(self, document_id: str) -> bool:
        """Delete CRM document and return success flag."""


def build_record_from_crm(
    document_id: str,
    crm_doc: dict[str, Any],
    default_target_url: str,
) -> dict[str, Any]:
    """Build UI record payload from CRM storage document."""
    payload = (
        crm_doc.get("effective_payload")
        or crm_doc.get("edited_payload")
        or crm_doc.get("ocr_payload")
        or {}
    )
    source = crm_doc.get("source") or {}
    return {
        "document_id": document_id,
        "preview_url": source.get("preview_url") or "",
        "source": source,
        "document": crm_doc.get("ocr_document") or {},
        "payload": payload,
        "missing_fields": crm_doc.get("missing_fields") or [],
        "manual_steps_required": crm_doc.get("manual_steps_required")
        or ["verify_filled_fields", "submit_or_download_manually"],
        "form_url": crm_doc.get("form_url") or default_target_url,
        "target_url": crm_doc.get("target_url") or default_target_url,
        "browser_session_id": crm_doc.get("browser_session_id") or "",
        "identity_match_found": bool(crm_doc.get("identity_match_found")),
        "identity_source_document_id": crm_doc.get("identity_source_document_id")
        or "",
        "enrichment_preview": crm_doc.get("enrichment_preview") or [],
        "merge_candidates": crm_doc.get("merge_candidates") or [],
        "family_links": crm_doc.get("family_links") or [],
        "family_reference": crm_doc.get("family_reference") or {},
    }


class CRMService:
    """Application service for CRM read/delete operations."""

    def __init__(
        self,
        *,
        repo: CRMRepositoryProtocol,
        default_target_url: str,
        safe_value: Callable[[Any], str],
        read_record: Callable[[str], dict[str, Any]],
        run_browser_call: Callable[..., Awaitable[Any]],
        close_browser_session: Callable[..., Any],
        record_path: Callable[[str], Path],
        logger: logging.Logger,
    ) -> None:
        """Initialize service with IO dependencies."""
        self._repo = repo
        self._default_target_url = default_target_url
        self._safe_value = safe_value
        self._read_record = read_record
        self._run_browser_call = run_browser_call
        self._close_browser_session = close_browser_session
        self._record_path = record_path
        self._logger = logger

    def list_documents(self, query: str, limit: int) -> list[dict[str, Any]]:
        """Return CRM summaries for listing API."""
        return self._repo.search_documents(query=query, limit=limit)

    def get_document(self, document_id: str) -> dict[str, Any]:
        """Return CRM document converted to response shape or raise 404."""
        crm_doc = self._repo.get_document(document_id)
        if not crm_doc:
            raise HTTPException(
                status_code=404,
                detail=f"CRM document not found: {document_id}",
            )
        return build_record_from_crm(
            document_id=document_id,
            crm_doc=crm_doc,
            default_target_url=self._default_target_url,
        )

    async def delete_document(self, document_id: str) -> dict[str, Any]:
        """Delete CRM document and linked runtime state."""
        crm_doc = self._repo.get_document(document_id)
        if not crm_doc:
            raise HTTPException(
                status_code=404,
                detail=f"CRM document not found: {document_id}",
            )

        session_id = self._safe_value(crm_doc.get("browser_session_id"))
        if not session_id:
            session_id = self._read_session_from_local_record(document_id=document_id)

        if session_id:
            try:
                await self._run_browser_call(self._close_browser_session, session_id)
            except Exception:
                self._logger.exception(
                    "Failed closing browser session during CRM delete: %s",
                    session_id,
                )

        deleted = self._repo.delete_document(document_id)
        if not deleted:
            raise HTTPException(
                status_code=500,
                detail=f"Failed deleting CRM document: {document_id}",
            )

        self._delete_local_record(document_id=document_id)
        return {"document_id": document_id, "deleted": True}

    def _read_session_from_local_record(self, document_id: str) -> str:
        """Try reading browser session id from runtime record file."""
        try:
            local_record = self._read_record(document_id)
        except HTTPException:
            return ""
        return self._safe_value(local_record.get("browser_session_id"))

    def _delete_local_record(self, document_id: str) -> None:
        """Delete runtime record file; keep request successful on cleanup errors."""
        record_path = self._record_path(document_id)
        if not record_path.exists():
            return
        try:
            record_path.unlink()
        except OSError:
            self._logger.exception(
                "Failed deleting local document record: %s",
                record_path,
            )
