"""Service that handles document upload and OCR normalization flow."""

from __future__ import annotations

import os
import shutil
import uuid
from pathlib import Path
from typing import Any, Callable, Protocol

from fastapi import HTTPException, UploadFile


class OCRClientProtocol(Protocol):
    """Protocol for OCR client used in upload flow."""

    def extract_text(self, source_path: Path) -> Any:
        """Extract text from uploaded source path."""


class CRMRepositoryProtocol(Protocol):
    """Protocol for CRM repository methods used during upload."""

    def upsert_from_upload(
        self,
        *,
        document_id: str,
        payload: dict[str, Any],
        ocr_document: dict[str, Any],
        source: dict[str, Any],
        missing_fields: list[str],
        manual_steps_required: list[str],
        form_url: str,
        target_url: str,
        identity_match_found: bool = False,
        identity_source_document_id: str = "",
        enrichment_preview: list[dict[str, Any]] | None = None,
        merge_candidates: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Create or update CRM record after upload."""

    def update_document_fields(self, document_id: str, patch: dict[str, Any]) -> dict[str, Any]:
        """Patch selected CRM fields."""


class UploadService:
    """Upload orchestration with injected side-effect dependencies."""

    def __init__(
        self,
        *,
        uploads_dir: Path,
        default_target_url: str,
        crm_repo: CRMRepositoryProtocol,
        safe_value: Callable[[Any], str],
        runtime_url: Callable[[Path], str],
        allowed_suffix: Callable[[str], bool],
        write_record: Callable[[str, dict[str, Any]], None],
        merge_candidates_for_payload: Callable[[str, dict[str, Any], int], list[dict[str, Any]]],
        collect_validation_errors: Callable[[dict[str, Any], bool], list[str]],
        collect_validation_issues: Callable[[dict[str, Any], bool], list[dict[str, Any]]],
        build_tasa_document: Callable[..., dict[str, Any]],
        normalize_payload_for_form: Callable[[dict[str, Any]], dict[str, Any]],
        attach_pipeline_metadata: Callable[..., dict[str, Any]],
        stage_start: Callable[[], float],
        stage_success: Callable[..., dict[str, Any]],
        create_ocr_client: Callable[[], OCRClientProtocol],
        sync_family_reference: Callable[[str, dict[str, Any], dict[str, Any]], dict[str, Any]],
    ) -> None:
        """Initialize upload service with explicit collaborators."""
        self._uploads_dir = uploads_dir
        self._default_target_url = default_target_url
        self._crm_repo = crm_repo
        self._safe_value = safe_value
        self._runtime_url = runtime_url
        self._allowed_suffix = allowed_suffix
        self._write_record = write_record
        self._merge_candidates_for_payload = merge_candidates_for_payload
        self._collect_validation_errors = collect_validation_errors
        self._collect_validation_issues = collect_validation_issues
        self._build_tasa_document = build_tasa_document
        self._normalize_payload_for_form = normalize_payload_for_form
        self._attach_pipeline_metadata = attach_pipeline_metadata
        self._stage_start = stage_start
        self._stage_success = stage_success
        self._create_ocr_client = create_ocr_client
        self._sync_family_reference = sync_family_reference

    async def upload_document(
        self,
        *,
        file: UploadFile,
        tasa_code: str,
        source_kind: str,
    ) -> dict[str, Any]:
        """Handle uploaded file, OCR extraction and CRM persistence."""
        if not file.filename or not self._allowed_suffix(file.filename):
            raise HTTPException(
                status_code=400,
                detail="Only .jpg/.jpeg/.png/.pdf are supported.",
            )

        normalized_source_kind = self._safe_value(source_kind).lower()
        if normalized_source_kind not in {
            "anketa",
            "fmiliar",
            "familiar",
            "passport",
            "nie_tie",
            "visa",
        }:
            raise HTTPException(
                status_code=422,
                detail=(
                    "source_kind is required and must be one of: "
                    "anketa, fmiliar, passport, nie_tie, visa"
                ),
            )

        document_id = uuid.uuid4().hex
        suffix = Path(file.filename).suffix.lower()
        stored_name = f"{document_id}{suffix}"
        upload_path = self._uploads_dir / stored_name

        with upload_path.open("wb") as fh:
            shutil.copyfileobj(file.file, fh)

        ocr_client = self._create_ocr_client()
        google_maps_api_key = os.getenv("GOOGLE_MAPS_API_KEY", "").strip() or os.getenv(
            "GOOGLE_CLOUD_VISION_API_KEY", ""
        ).strip()

        ocr_started = self._stage_start()
        ocr_result = ocr_client.extract_text(upload_path)
        ocr_stage = self._stage_success(
            "ocr",
            ocr_started,
            details={
                "source": self._safe_value(getattr(ocr_result, "ocr_source", "live"))
                or "live",
                "used_cached_ocr": False,
                "pages": len(ocr_result.pages),
            },
        )

        parse_started = self._stage_start()
        document = self._build_tasa_document(
            ocr_front=ocr_result.full_text,
            ocr_back="",
            user_overrides={},
            geocode_candidates=None,
            google_maps_api_key=google_maps_api_key,
            tasa_code=tasa_code,
            source_file=file.filename,
            source_kind=normalized_source_kind,
        )
        parse_stage = self._stage_success(
            "parse_extract_map",
            parse_started,
            details={"forms_available": sorted((document.get("forms") or {}).keys())},
        )
        crm_stage = self._stage_success("crm_mapping", self._stage_start())
        document = self._attach_pipeline_metadata(
            document=document,
            source_files=[file.filename],
            ocr_details={
                "front_text_len": len(ocr_result.full_text or ""),
                "back_text_len": 0,
                "used_cached_ocr": False,
                "source": self._safe_value(getattr(ocr_result, "ocr_source", "live"))
                or "live",
            },
            parse_stage=parse_stage,
            crm_stage=crm_stage,
            ocr_stage=ocr_stage,
        )

        payload = self._normalize_payload_for_form(document)
        merge_candidates = self._merge_candidates_for_payload(document_id, payload, 10)
        missing_fields = self._collect_validation_errors(payload, False)
        validation_issues = self._collect_validation_issues(payload, False)

        record = {
            "document_id": document_id,
            "tasa_code": tasa_code,
            "source": {
                "original_filename": file.filename,
                "stored_path": str(upload_path),
                "preview_url": self._runtime_url(upload_path),
                "source_kind": normalized_source_kind,
            },
            "document": document,
            "payload": payload,
            "missing_fields": missing_fields,
            "validation_issues": validation_issues,
            "manual_steps_required": [
                "verify_filled_fields",
                "submit_or_download_manually",
            ],
            "form_url": self._default_target_url,
            "target_url": self._default_target_url,
            "identity_match_found": False,
            "identity_source_document_id": "",
            "enrichment_preview": [],
            "merge_candidates": merge_candidates,
            "family_links": [],
            "family_reference": {},
        }

        self._write_record(document_id, record)
        self._crm_repo.upsert_from_upload(
            document_id=document_id,
            payload=payload,
            ocr_document=document,
            source=record["source"],
            missing_fields=missing_fields,
            manual_steps_required=record["manual_steps_required"],
            form_url=self._default_target_url,
            target_url=self._default_target_url,
            identity_match_found=bool(record.get("identity_match_found")),
            identity_source_document_id=self._safe_value(
                record.get("identity_source_document_id")
            ),
            enrichment_preview=list(record.get("enrichment_preview") or []),
            merge_candidates=merge_candidates,
        )

        family_sync = self._sync_family_reference(document_id, payload, record["source"])
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
            "preview_url": record["source"]["preview_url"],
            "form_url": self._default_target_url,
            "target_url": self._default_target_url,
            "payload": payload,
            "document": document,
            "missing_fields": missing_fields,
            "validation_issues": validation_issues,
            "manual_steps_required": record["manual_steps_required"],
            "identity_match_found": bool(record.get("identity_match_found")),
            "identity_source_document_id": self._safe_value(
                record.get("identity_source_document_id")
            ),
            "enrichment_preview": list(record.get("enrichment_preview") or []),
            "merge_candidates": merge_candidates,
            "family_links": record.get("family_links") or [],
            "family_reference": record.get("family_reference") or {},
        }
