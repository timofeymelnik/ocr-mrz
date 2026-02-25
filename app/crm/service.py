"""Business logic for CRM document endpoints."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Protocol

from fastapi import HTTPException

from app.api.errors import ApiError, ApiErrorCode
from app.documents.workflow import resolve_workflow_stage, stage_to_next_step


class CRMRepositoryProtocol(Protocol):
    """Protocol describing repository methods used by CRM service."""

    def search_documents(
        self, query: str, limit: int, dedupe: bool
    ) -> list[dict[str, Any]]:
        """Search stored CRM documents with optional text query."""

    def list_documents_by_client(
        self, client_id: str, *, limit: int, include_merged: bool
    ) -> list[dict[str, Any]]:
        """List all client-linked documents."""

    def list_clients(self, query: str, limit: int) -> list[dict[str, Any]]:
        """List client-centric summaries."""

    def get_document(self, document_id: str) -> dict[str, Any] | None:
        """Return CRM document by id, or ``None`` when it does not exist."""

    def delete_document(self, document_id: str) -> bool:
        """Delete CRM document and return success flag."""

    def get_client(self, client_id: str) -> dict[str, Any] | None:
        """Return client entity by id."""

    def list_full_documents_by_client(self, client_id: str) -> list[dict[str, Any]]:
        """Return full linked documents for client."""

    def update_client_profile(
        self,
        client_id: str,
        profile_payload: dict[str, Any],
        *,
        profile_source_document_id: str = "",
        profile_merge_meta: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Persist client profile payload."""

    def delete_client(self, client_id: str) -> bool:
        """Delete client entity only."""

    def delete_documents_by_client(self, client_id: str) -> list[str]:
        """Delete all linked documents for a client."""


def _is_empty_value(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, list):
        return len(value) == 0
    if isinstance(value, dict):
        return len(value) == 0
    return False


def _deep_merge_first_non_empty(base: Any, incoming: Any) -> Any:
    if isinstance(base, dict) and isinstance(incoming, dict):
        merged: dict[str, Any] = {}
        keys = set(base.keys()) | set(incoming.keys())
        for key in keys:
            if key in base and key in incoming:
                merged[key] = _deep_merge_first_non_empty(base[key], incoming[key])
            elif key in base:
                merged[key] = base[key]
            else:
                merged[key] = incoming[key]
        return merged
    if _is_empty_value(base) and not _is_empty_value(incoming):
        return incoming
    return base


def _flatten_payload(payload: dict[str, Any], *, prefix: str = "") -> dict[str, str]:
    rows: dict[str, str] = {}
    for key, value in payload.items():
        path = f"{prefix}.{key}" if prefix else key
        if isinstance(value, dict):
            rows.update(_flatten_payload(value, prefix=path))
            continue
        if isinstance(value, list):
            text = ", ".join(str(item) for item in value if str(item).strip())
            rows[path] = text.strip()
            continue
        rows[path] = str(value or "").strip()
    return rows


def _set_path(payload: dict[str, Any], path: str, value: Any) -> None:
    node = payload
    parts = [part for part in path.split(".") if part]
    for part in parts[:-1]:
        child = node.get(part)
        if not isinstance(child, dict):
            child = {}
            node[part] = child
        node = child
    if parts:
        node[parts[-1]] = value


def _get_path(payload: dict[str, Any], path: str) -> Any:
    node: Any = payload
    for part in [part for part in path.split(".") if part]:
        if not isinstance(node, dict):
            return ""
        node = node.get(part)
    return node


def build_record_from_crm(
    document_id: str,
    crm_doc: dict[str, Any],
    default_target_url: str,
    artifact_url_from_value: Callable[[Any], str] | None = None,
) -> dict[str, Any]:
    """Build UI record payload from CRM storage document."""
    payload = (
        crm_doc.get("effective_payload")
        or crm_doc.get("edited_payload")
        or crm_doc.get("ocr_payload")
        or {}
    )
    source = crm_doc.get("source") or {}
    preview_url = str(source.get("preview_url") or "").strip()
    if not preview_url and artifact_url_from_value is not None:
        preview_url = str(
            artifact_url_from_value(source.get("stored_path") or "")
            or ""
        ).strip()
    workflow_stage = resolve_workflow_stage(crm_doc)
    return {
        "document_id": document_id,
        "client_id": str(crm_doc.get("client_id") or ""),
        "preview_url": preview_url,
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
        "identity_source_document_id": crm_doc.get("identity_source_document_id") or "",
        "source_kind_input": source.get("source_kind_input") or "",
        "source_kind_detected": source.get("source_kind_detected")
        or source.get("source_kind")
        or "",
        "source_kind_confidence": float(source.get("source_kind_confidence") or 0.0),
        "source_kind_auto": bool(source.get("source_kind_auto")),
        "source_kind_requires_review": bool(source.get("source_kind_requires_review")),
        "workflow_stage": workflow_stage,
        "workflow_next_step": stage_to_next_step(workflow_stage),
        "client_match": crm_doc.get("client_match") or {},
        "client_match_decision": crm_doc.get("client_match_decision") or "none",
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
        artifact_url_from_value: Callable[[Any], str] | None = None,
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
        self._artifact_url_from_value = artifact_url_from_value
        self._read_record = read_record
        self._run_browser_call = run_browser_call
        self._close_browser_session = close_browser_session
        self._record_path = record_path
        self._logger = logger

    def list_documents(
        self, query: str, limit: int, include_duplicates: bool = False
    ) -> list[dict[str, Any]]:
        """Return CRM summaries for listing API."""
        return self._repo.search_documents(
            query=query,
            limit=limit,
            dedupe=not include_duplicates,
        )

    def list_clients(self, query: str, limit: int) -> list[dict[str, Any]]:
        """Return client-centric CRM summaries."""
        return self._repo.list_clients(query=query, limit=limit)

    def list_client_documents(
        self, client_id: str, limit: int, include_merged: bool = True
    ) -> list[dict[str, Any]]:
        """Return documents linked to a client entity."""
        return self._repo.list_documents_by_client(
            client_id=client_id,
            limit=limit,
            include_merged=include_merged,
        )

    @staticmethod
    def _profile_sort_key(record: dict[str, Any]) -> tuple[int, str]:
        has_edited = bool(record.get("edited_payload"))
        status = str(record.get("status") or "").strip().lower()
        quality = 1 if has_edited or status == "confirmed" else 0
        return quality, str(record.get("updated_at") or "")

    def _build_profile_from_documents(self, docs: list[dict[str, Any]]) -> dict[str, Any]:
        profile: dict[str, Any] = {}
        for doc in sorted(docs, key=self._profile_sort_key, reverse=True):
            payload = doc.get("effective_payload") or doc.get("edited_payload") or {}
            if not isinstance(payload, dict):
                continue
            profile = _deep_merge_first_non_empty(profile, payload)
        return profile

    def _get_client_or_404(self, client_id: str) -> dict[str, Any]:
        client = self._repo.get_client(client_id)
        if not client:
            raise ApiError(
                status_code=404,
                error_code=ApiErrorCode.CRM_DOCUMENT_NOT_FOUND,
                message=f"CRM client not found: {client_id}",
            )
        return client

    def get_client_profile(self, client_id: str) -> dict[str, Any]:
        """Return client profile payload with lazy backfill."""
        client = self._get_client_or_404(client_id)
        docs = self._repo.list_full_documents_by_client(client_id)
        profile_payload = client.get("profile_payload")
        if not isinstance(profile_payload, dict) or not profile_payload:
            profile_payload = self._build_profile_from_documents(docs)
            client = self._repo.update_client_profile(
                client_id,
                profile_payload,
                profile_source_document_id=str(client.get("primary_document_id") or ""),
            )
        return {
            "client_id": str(client.get("client_id") or ""),
            "profile_payload": profile_payload,
            "profile_meta": {
                "profile_source_document_id": str(
                    client.get("profile_source_document_id") or ""
                ),
                "profile_updated_at": str(client.get("profile_updated_at") or ""),
                "profile_merge_meta": client.get("profile_merge_meta") or {},
            },
            "missing_fields": [],
            "validation_issues": [],
        }

    def update_client_profile(
        self, client_id: str, profile_payload: dict[str, Any]
    ) -> dict[str, Any]:
        """Persist full client profile payload."""
        client = self._get_client_or_404(client_id)
        updated = self._repo.update_client_profile(
            client_id,
            profile_payload,
            profile_source_document_id=str(client.get("profile_source_document_id") or ""),
        )
        return {
            "client_id": str(updated.get("client_id") or ""),
            "profile_payload": updated.get("profile_payload") or {},
            "profile_meta": {
                "profile_source_document_id": str(
                    updated.get("profile_source_document_id") or ""
                ),
                "profile_updated_at": str(updated.get("profile_updated_at") or ""),
                "profile_merge_meta": updated.get("profile_merge_meta") or {},
            },
            "missing_fields": [],
            "validation_issues": [],
        }

    def get_client_card(self, client_id: str) -> dict[str, Any]:
        """Return client card with profile and linked documents."""
        client = self._get_client_or_404(client_id)
        docs = self._repo.list_documents_by_client(
            client_id=client_id,
            limit=500,
            include_merged=True,
        )
        profile = self.get_client_profile(client_id)
        return {
            "client_id": str(client.get("client_id") or ""),
            "primary_document_id": str(client.get("primary_document_id") or ""),
            "display_name": str(client.get("display_name") or ""),
            "documents_count": int(client.get("documents_count") or len(docs)),
            "updated_at": str(client.get("updated_at") or ""),
            "documents": docs,
            "profile_payload": profile.get("profile_payload") or {},
            "profile_meta": profile.get("profile_meta") or {},
            "missing_fields": profile.get("missing_fields") or [],
            "validation_issues": profile.get("validation_issues") or [],
        }

    def get_client_profile_merge_candidates(self, client_id: str) -> dict[str, Any]:
        """Return candidate documents to merge into client profile."""
        profile = self.get_client_profile(client_id)
        profile_payload = profile.get("profile_payload") or {}
        if not isinstance(profile_payload, dict):
            profile_payload = {}
        profile_flat = _flatten_payload(profile_payload)
        doc_number = str(
            _get_path(profile_payload, "identificacion.nif_nie")
            or _get_path(profile_payload, "identificacion.pasaporte")
            or ""
        ).strip()
        current_docs = {
            row.get("document_id")
            for row in self._repo.list_documents_by_client(
                client_id=client_id,
                limit=500,
                include_merged=True,
            )
        }
        candidates: list[dict[str, Any]] = []
        pool = self._repo.search_documents(query=doc_number, limit=200, dedupe=False)
        for row in pool:
            source_document_id = str(row.get("document_id") or "").strip()
            if not source_document_id or source_document_id in current_docs:
                continue
            source_doc = self._repo.get_document(source_document_id)
            if not source_doc:
                continue
            source_payload = source_doc.get("effective_payload") or source_doc.get(
                "edited_payload"
            )
            if not isinstance(source_payload, dict):
                continue
            source_flat = _flatten_payload(source_payload)
            overlap = sum(
                1
                for key, value in source_flat.items()
                if value and profile_flat.get(key) and profile_flat.get(key) == value
            )
            fillables = sum(
                1
                for key, value in source_flat.items()
                if value and not str(profile_flat.get(key) or "").strip()
            )
            score = overlap * 10 + fillables
            candidates.append(
                {
                    "document_id": source_document_id,
                    "name": str(row.get("name") or ""),
                    "document_number": str(row.get("document_number") or ""),
                    "updated_at": str(row.get("updated_at") or ""),
                    "score": score,
                    "reasons": (
                        ["document_match"] if doc_number and doc_number in str(row) else []
                    ),
                }
            )
        candidates.sort(key=lambda item: int(item.get("score") or 0), reverse=True)
        return {
            "client_id": client_id,
            "merge_candidates": candidates[:20],
        }

    def enrich_client_profile_by_identity(
        self,
        client_id: str,
        *,
        apply: bool,
        source_document_id: str,
        selected_fields: list[str] | None = None,
    ) -> dict[str, Any]:
        """Preview/apply merge from source document to client profile."""
        profile = self.get_client_profile(client_id)
        profile_payload = profile.get("profile_payload") or {}
        if not isinstance(profile_payload, dict):
            profile_payload = {}
        source_doc = self._repo.get_document(source_document_id)
        if not source_doc:
            raise ApiError(
                status_code=404,
                error_code=ApiErrorCode.CRM_DOCUMENT_NOT_FOUND,
                message=f"Source document not found: {source_document_id}",
            )
        source_payload = source_doc.get("effective_payload") or source_doc.get(
            "edited_payload"
        )
        if not isinstance(source_payload, dict):
            raise ApiError(
                status_code=422,
                error_code=ApiErrorCode.DOCUMENT_INVALID_PAYLOAD,
                message=f"Source payload is invalid: {source_document_id}",
            )
        profile_flat = _flatten_payload(profile_payload)
        source_flat = _flatten_payload(source_payload)
        preview: list[dict[str, Any]] = []
        for field, suggested_value in source_flat.items():
            if not suggested_value:
                continue
            current_value = str(profile_flat.get(field) or "").strip()
            if current_value:
                continue
            preview.append(
                {
                    "field": field,
                    "current_value": current_value,
                    "suggested_value": suggested_value,
                    "source": source_document_id,
                }
            )
        preview.sort(key=lambda row: str(row.get("field") or ""))
        if not apply:
            return {
                "client_id": client_id,
                "profile_payload": profile_payload,
                "applied_fields": [],
                "skipped_fields": [],
                "enrichment_preview": preview,
                "merge_candidates": self.get_client_profile_merge_candidates(client_id).get(
                    "merge_candidates"
                )
                or [],
            }

        selected = set(selected_fields or [])
        applied_fields: list[str] = []
        skipped_fields: list[str] = []
        updated_payload = dict(profile_payload)
        for row in preview:
            field = str(row.get("field") or "")
            if selected and field not in selected:
                skipped_fields.append(field)
                continue
            _set_path(updated_payload, field, row.get("suggested_value"))
            applied_fields.append(field)

        updated = self._repo.update_client_profile(
            client_id,
            updated_payload,
            profile_source_document_id=source_document_id,
            profile_merge_meta={
                "source_document_id": source_document_id,
                "applied_fields": applied_fields,
                "skipped_fields": skipped_fields,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            },
        )
        return {
            "client_id": client_id,
            "profile_payload": updated.get("profile_payload") or {},
            "applied_fields": applied_fields,
            "skipped_fields": skipped_fields,
            "enrichment_preview": preview,
            "merge_candidates": self.get_client_profile_merge_candidates(client_id).get(
                "merge_candidates"
            )
            or [],
        }

    def get_document(self, document_id: str) -> dict[str, Any]:
        """Return CRM document converted to response shape or raise 404."""
        crm_doc = self._repo.get_document(document_id)
        if not crm_doc:
            raise ApiError(
                status_code=404,
                error_code=ApiErrorCode.CRM_DOCUMENT_NOT_FOUND,
                message=f"CRM document not found: {document_id}",
            )
        return build_record_from_crm(
            document_id=document_id,
            crm_doc=crm_doc,
            default_target_url=self._default_target_url,
            artifact_url_from_value=self._artifact_url_from_value,
        )

    async def delete_document(self, document_id: str) -> dict[str, Any]:
        """Delete CRM document and linked runtime state."""
        crm_doc = self._repo.get_document(document_id)
        if not crm_doc:
            raise ApiError(
                status_code=404,
                error_code=ApiErrorCode.CRM_DOCUMENT_NOT_FOUND,
                message=f"CRM document not found: {document_id}",
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
            raise ApiError(
                status_code=500,
                error_code=ApiErrorCode.CRM_DELETE_FAILED,
                message=f"Failed deleting CRM document: {document_id}",
            )

        self._delete_local_record(document_id=document_id)
        self._delete_document_source_file(crm_doc)
        return {"document_id": document_id, "deleted": True}

    async def delete_client_cascade(self, client_id: str) -> dict[str, Any]:
        """Delete client and all linked documents with runtime artifacts."""
        client = self._repo.get_client(client_id)
        if not client:
            raise ApiError(
                status_code=404,
                error_code=ApiErrorCode.CRM_DOCUMENT_NOT_FOUND,
                message=f"CRM client not found: {client_id}",
            )

        docs = self._repo.list_full_documents_by_client(client_id)
        for doc in docs:
            document_id = self._safe_value(doc.get("document_id"))
            session_id = self._safe_value(doc.get("browser_session_id"))
            if not session_id and document_id:
                session_id = self._read_session_from_local_record(document_id=document_id)
            if session_id:
                try:
                    await self._run_browser_call(self._close_browser_session, session_id)
                except Exception:
                    self._logger.exception(
                        "Failed closing browser session during CRM client delete: %s",
                        session_id,
                    )
            if document_id:
                self._delete_local_record(document_id=document_id)
            self._delete_document_source_file(doc)

        deleted_doc_ids = self._repo.delete_documents_by_client(client_id)
        deleted_client = self._repo.delete_client(client_id)
        if not deleted_client:
            raise ApiError(
                status_code=500,
                error_code=ApiErrorCode.CRM_DELETE_FAILED,
                message=f"Failed deleting CRM client: {client_id}",
            )
        return {
            "client_id": client_id,
            "deleted": True,
            "deleted_document_ids": deleted_doc_ids,
        }

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

    def _delete_document_source_file(self, crm_doc: dict[str, Any]) -> None:
        source = crm_doc.get("source")
        source_data = source if isinstance(source, dict) else {}
        stored_path = self._safe_value(source_data.get("stored_path"))
        if not stored_path:
            return
        path = Path(stored_path)
        if not path.exists() or not path.is_file():
            return
        try:
            path.unlink()
        except OSError:
            self._logger.exception("Failed deleting source file: %s", path)
