"""Application service for document endpoints."""

from __future__ import annotations

from typing import Any, Callable, Protocol

from fastapi import HTTPException


class CRMRepositoryProtocol(Protocol):
    """Protocol of CRM repository methods used by document service."""

    def save_edited_payload(
        self,
        *,
        document_id: str,
        payload: dict[str, Any],
        missing_fields: list[str] | None = None,
    ) -> dict[str, Any]:
        """Persist edited payload for the document."""

    def update_document_fields(self, document_id: str, patch: dict[str, Any]) -> dict[str, Any]:
        """Patch selected CRM fields for the document."""


class DocumentsService:
    """Business logic for document fetch/confirm/merge flows."""

    def __init__(
        self,
        *,
        crm_repo: CRMRepositoryProtocol,
        read_or_bootstrap_record: Callable[[str], dict[str, Any]],
        write_record: Callable[[str, dict[str, Any]], None],
        merge_candidates_for_payload: Callable[[str, dict[str, Any], int], list[dict[str, Any]]],
        collect_validation_errors: Callable[[dict[str, Any], bool], list[str]],
        collect_validation_issues: Callable[[dict[str, Any], bool], list[dict[str, Any]]],
        sync_family_reference: Callable[[str, dict[str, Any], dict[str, Any]], dict[str, Any]],
        enrich_record_payload_by_identity: Callable[[str, dict[str, Any], bool, str], dict[str, Any]],
        safe_value: Callable[[Any], str],
    ) -> None:
        """Store dependencies used by service methods."""
        self._crm_repo = crm_repo
        self._read_or_bootstrap_record = read_or_bootstrap_record
        self._write_record = write_record
        self._merge_candidates_for_payload = merge_candidates_for_payload
        self._collect_validation_errors = collect_validation_errors
        self._collect_validation_issues = collect_validation_issues
        self._sync_family_reference = sync_family_reference
        self._enrich_record_payload_by_identity = enrich_record_payload_by_identity
        self._safe_value = safe_value

    def get_document(self, document_id: str) -> dict[str, Any]:
        """Return document record restored from runtime/CRM storage."""
        return self._read_or_bootstrap_record(document_id)

    def get_merge_candidates(self, document_id: str) -> dict[str, Any]:
        """Recompute merge candidates and persist them for the document."""
        record = self._read_or_bootstrap_record(document_id)
        payload = record.get("payload") or {}
        if not isinstance(payload, dict):
            raise HTTPException(status_code=422, detail="Invalid payload in document record.")

        candidates = self._merge_candidates_for_payload(document_id, payload, 10)
        record["merge_candidates"] = candidates
        self._write_record(document_id, record)
        self._crm_repo.update_document_fields(document_id, {"merge_candidates": candidates})

        return {"document_id": document_id, "merge_candidates": candidates}

    def confirm_document(self, document_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        """Persist operator-confirmed payload and refresh derived fields."""
        record = self._read_or_bootstrap_record(document_id)
        merge_candidates = self._merge_candidates_for_payload(document_id, payload, 10)
        missing_fields = self._collect_validation_errors(payload, False)
        validation_issues = self._collect_validation_issues(payload, False)

        record["payload"] = payload
        record["missing_fields"] = missing_fields
        record["merge_candidates"] = merge_candidates
        self._write_record(document_id, record)

        self._crm_repo.save_edited_payload(
            document_id=document_id,
            payload=payload,
            missing_fields=missing_fields,
        )
        self._crm_repo.update_document_fields(document_id, {"merge_candidates": merge_candidates})

        family_sync = self._sync_family_reference(
            document_id,
            payload,
            record.get("source") or {},
        )
        if family_sync.get("linked"):
            record["family_links"] = family_sync.get("family_links") or []
            record["family_reference"] = family_sync.get("family_reference") or {}
            self._write_record(document_id, record)
            self._crm_repo.update_document_fields(
                document_id,
                {
                    "family_links": record["family_links"],
                    "family_reference": record["family_reference"],
                },
            )

        return {
            "document_id": document_id,
            "missing_fields": missing_fields,
            "validation_issues": validation_issues,
            "payload": payload,
            "manual_steps_required": record.get("manual_steps_required", []),
            "identity_match_found": bool(record.get("identity_match_found")),
            "identity_source_document_id": self._safe_value(
                record.get("identity_source_document_id")
            ),
            "enrichment_preview": list(record.get("enrichment_preview") or []),
            "merge_candidates": merge_candidates,
            "family_links": record.get("family_links") or [],
            "family_reference": record.get("family_reference") or {},
        }

    def enrich_by_identity(
        self,
        document_id: str,
        *,
        apply: bool,
        source_document_id: str,
    ) -> dict[str, Any]:
        """Preview or apply identity-based enrichment."""
        record = self._read_or_bootstrap_record(document_id)
        payload = record.get("payload") or {}
        if not isinstance(payload, dict):
            raise HTTPException(status_code=422, detail="Invalid payload in document record.")

        enrichment = self._enrich_record_payload_by_identity(
            document_id,
            payload,
            bool(apply),
            source_document_id,
        )
        enriched_payload = (
            enrichment.get("payload") if isinstance(enrichment.get("payload"), dict) else payload
        )

        if not apply:
            merge_candidates = self._merge_candidates_for_payload(document_id, payload, 10)
            return {
                "document_id": document_id,
                "identity_match_found": bool(enrichment.get("identity_match_found")),
                "identity_source_document_id": self._safe_value(
                    enrichment.get("identity_source_document_id")
                ),
                "identity_key": self._safe_value(enrichment.get("identity_key")),
                "applied_fields": enrichment.get("applied_fields", []),
                "skipped_fields": enrichment.get("skipped_fields", []),
                "enrichment_preview": enrichment.get("enrichment_preview", []),
                "merge_candidates": merge_candidates,
                "missing_fields": self._collect_validation_errors(payload, False),
                "validation_issues": self._collect_validation_issues(payload, False),
                "payload": payload,
            }

        missing_fields = self._collect_validation_errors(enriched_payload, False)
        validation_issues = self._collect_validation_issues(enriched_payload, False)
        merge_candidates = self._merge_candidates_for_payload(document_id, enriched_payload, 10)

        updated_record = self._read_or_bootstrap_record(document_id)
        updated_record["merge_candidates"] = merge_candidates
        self._write_record(document_id, updated_record)
        self._crm_repo.update_document_fields(document_id, {"merge_candidates": merge_candidates})

        return {
            "document_id": document_id,
            "identity_match_found": bool(enrichment.get("identity_match_found")),
            "identity_source_document_id": self._safe_value(
                enrichment.get("identity_source_document_id")
            ),
            "identity_key": self._safe_value(enrichment.get("identity_key")),
            "applied_fields": enrichment.get("applied_fields", []),
            "skipped_fields": enrichment.get("skipped_fields", []),
            "enrichment_preview": enrichment.get("enrichment_preview", []),
            "merge_candidates": merge_candidates,
            "missing_fields": missing_fields,
            "validation_issues": validation_issues,
            "payload": enriched_payload,
        }
