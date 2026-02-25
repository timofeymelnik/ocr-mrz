"""Application service for document endpoints."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Callable, Protocol

from app.api.errors import ApiError, ApiErrorCode
from app.data_builder.address_parser import expand_abbrev, parse_address_parts
from app.data_builder.geocoding import fetch_geocode_candidates
from app.documents.source_kind import (
    CANONICAL_SOURCE_KINDS,
    normalize_source_kind,
)
from app.documents.workflow import WORKFLOW_PREPARE, WORKFLOW_REVIEW, stage_to_next_step


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

    def update_document_fields(
        self, document_id: str, patch: dict[str, Any]
    ) -> dict[str, Any]:
        """Patch selected CRM fields for the document."""

    def ensure_client_entity(
        self, *, document_id: str, source_document_id: str = ""
    ) -> dict[str, Any]:
        """Create/update client entity and link selected documents."""


class OCRClientProtocol(Protocol):
    """Protocol for OCR client used during document reprocessing."""

    def extract_text(self, source_path: Path) -> Any:
        """Extract OCR text from source file."""


class DocumentsService:
    """Business logic for document fetch/confirm/merge flows."""

    def __init__(
        self,
        *,
        crm_repo: CRMRepositoryProtocol,
        read_or_bootstrap_record: Callable[[str], dict[str, Any]],
        write_record: Callable[[str, dict[str, Any]], None],
        merge_candidates_for_payload: Callable[
            [str, dict[str, Any], int], list[dict[str, Any]]
        ],
        collect_validation_errors: Callable[[dict[str, Any], bool], list[str]],
        collect_validation_issues: Callable[
            [dict[str, Any], bool], list[dict[str, Any]]
        ],
        sync_family_reference: Callable[
            [str, dict[str, Any], dict[str, Any]], dict[str, Any]
        ],
        enrich_record_payload_by_identity: Callable[
            [str, dict[str, Any], bool, str, list[str] | None], dict[str, Any]
        ],
        build_tasa_document: Callable[..., dict[str, Any]] | None = None,
        normalize_payload_for_form: (
            Callable[[dict[str, Any]], dict[str, Any]] | None
        ) = None,
        create_ocr_client: Callable[[], OCRClientProtocol] | None = None,
        artifact_url_from_value: Callable[[Any], str] | None = None,
        safe_value: Callable[[Any], str],
        google_maps_api_key: str | None = None,
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
        self._build_tasa_document = build_tasa_document
        self._normalize_payload_for_form = normalize_payload_for_form
        self._create_ocr_client = create_ocr_client
        self._artifact_url_from_value = artifact_url_from_value
        self._safe_value = safe_value
        self._google_maps_api_key = (
            str(google_maps_api_key or "").strip()
            or os.getenv("GOOGLE_MAPS_API_KEY", "").strip()
            or os.getenv("GOOGLE_CLOUD_VISION_API_KEY", "").strip()
        )

    @staticmethod
    def _safe_text(value: Any) -> str:
        """Convert optional value to trimmed string."""
        if value is None:
            return ""
        return str(value).strip()

    @classmethod
    def _geocode_component(cls, candidate: dict[str, Any], component_type: str) -> str:
        """Extract geocode component by Google type."""
        components = candidate.get("address_components") or []
        if not isinstance(components, list):
            return ""
        for component in components:
            if not isinstance(component, dict):
                continue
            types = component.get("types") or []
            if component_type in types:
                return cls._safe_text(component.get("long_name"))
        return ""

    @classmethod
    def _extract_via_from_route(cls, route: str) -> tuple[str, str]:
        """Split route string into tipo_via and nombre_via."""
        normalized = cls._safe_text(route)
        if not normalized:
            return "", ""
        parts = normalized.split(maxsplit=1)
        if len(parts) == 1:
            return "", normalized
        return parts[0].title(), parts[1].strip()

    @classmethod
    def _extract_floor_and_door(cls, line: str) -> tuple[str, str]:
        """Extract piso/puerta from compact Spanish address fragments."""
        raw = cls._safe_text(line).upper()
        if not raw:
            return "", ""

        labeled = re.search(
            r"\b(?:PISO|PLANTA)\s*([0-9A-Zºª]{1,3})\b(?:\s*(?:PUERTA|P(?:TA)?\.?)\s*([0-9A-Z]{1,4}))?",
            raw,
        )
        if labeled:
            piso = cls._safe_text(labeled.group(1))
            puerta = cls._safe_text(labeled.group(2))
            return piso, puerta

        compact = re.search(r"(?:^|,)\s*(\d{1,2})\s+([A-Z])(?:\s*,|$)", raw)
        if compact:
            return cls._safe_text(compact.group(1)), cls._safe_text(compact.group(2))
        return "", ""

    @classmethod
    def _extract_city_by_zip_context(cls, line: str, cp: str) -> str:
        """Extract municipio around CP token from comma-separated address."""
        text = cls._safe_text(line)
        if not text:
            return ""
        zip_code = cls._safe_text(cp)
        if zip_code:
            match = re.search(rf",\s*([^,]+)\s*,\s*{re.escape(zip_code)}\b", text, re.I)
            if match:
                return cls._safe_text(match.group(1)).title()
            match = re.search(rf"\b{re.escape(zip_code)}\b\s*,\s*([^,]+)", text, re.I)
            if match:
                return cls._safe_text(match.group(1)).title()
        parts = [
            cls._safe_text(part) for part in text.split(",") if cls._safe_text(part)
        ]
        if len(parts) >= 2:
            maybe_city = parts[-2]
            if not re.fullmatch(r"\d{5}", maybe_city):
                return maybe_city.title()
        return ""

    @staticmethod
    def _identifiers_from_payload(payload: dict[str, Any]) -> dict[str, str]:
        """Build CRM identifiers map from normalized payload."""
        ident = payload.get("identificacion") if isinstance(payload, dict) else {}
        if not isinstance(ident, dict):
            ident = {}
        nif_nie = str(ident.get("nif_nie") or "").strip()
        passport = str(ident.get("pasaporte") or "").strip()
        return {
            "document_number": nif_nie or passport,
            "nif_nie": nif_nie,
            "passport": passport,
            "name": str(ident.get("nombre_apellidos") or "").strip(),
        }

    @staticmethod
    def _pick_client_match_candidate(
        merge_candidates: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Choose strongest candidate similarly to upload pipeline rules."""
        if not merge_candidates:
            return {}
        by_identity = [
            row
            for row in merge_candidates
            if "document_match" in (row.get("reasons") or [])
        ]
        if by_identity:
            return dict(by_identity[0])
        top = merge_candidates[0]
        if int(top.get("score") or 0) >= 100:
            return dict(top)
        return {}

    def _resolve_preview_url(self, source_data: dict[str, Any]) -> str:
        """Resolve preview URL using stored preview or source path fallback."""
        preview_url = self._safe_value(source_data.get("preview_url"))
        if preview_url:
            return preview_url
        if self._artifact_url_from_value is None:
            return ""
        stored_path = self._safe_value(source_data.get("stored_path"))
        if not stored_path:
            return ""
        return self._safe_value(self._artifact_url_from_value(stored_path))

    def get_document(self, document_id: str) -> dict[str, Any]:
        """Return document record restored from runtime/CRM storage."""
        record = self._read_or_bootstrap_record(document_id)
        source = record.get("source")
        source_data = source if isinstance(source, dict) else {}
        preview_url = self._resolve_preview_url(source_data)
        if preview_url:
            record["preview_url"] = preview_url
        return record

    def reprocess_document_ocr(
        self,
        document_id: str,
        *,
        source_kind: str,
        tasa_code: str = "",
    ) -> dict[str, Any]:
        """Re-run OCR/build pipeline for existing document with manual source kind."""
        if (
            self._build_tasa_document is None
            or self._normalize_payload_for_form is None
            or self._create_ocr_client is None
        ):
            raise ApiError(
                status_code=500,
                error_code=ApiErrorCode.INTERNAL_SERVER_ERROR,
                message="OCR reprocess dependencies are not configured.",
            )
        normalized_source_kind = normalize_source_kind(source_kind)
        if not normalized_source_kind:
            raise ApiError(
                status_code=422,
                error_code=ApiErrorCode.VALIDATION_ERROR,
                message=(
                    "source_kind must be one of: "
                    + ", ".join(sorted(CANONICAL_SOURCE_KINDS))
                ),
            )

        record = self._read_or_bootstrap_record(document_id)
        source = record.get("source")
        source_data = source if isinstance(source, dict) else {}
        stored_path_raw = self._safe_value(source_data.get("stored_path"))
        if not stored_path_raw:
            raise ApiError(
                status_code=422,
                error_code=ApiErrorCode.DOCUMENT_INVALID_PAYLOAD,
                message="Document source path is missing; cannot rerun OCR.",
            )
        source_path = Path(stored_path_raw)
        if not source_path.exists():
            raise ApiError(
                status_code=422,
                error_code=ApiErrorCode.DOCUMENT_INVALID_PAYLOAD,
                message=f"Stored source file not found: {stored_path_raw}",
            )

        ocr_client = self._create_ocr_client()
        ocr_result = ocr_client.extract_text(source_path)
        effective_tasa_code = self._safe_value(tasa_code) or self._safe_value(
            record.get("tasa_code")
        )
        if not effective_tasa_code:
            effective_tasa_code = "790_012"
        source_file = (
            self._safe_value(source_data.get("original_filename")) or source_path.name
        )
        document = self._build_tasa_document(
            ocr_front=self._safe_value(getattr(ocr_result, "full_text", "")),
            ocr_back="",
            user_overrides={},
            geocode_candidates=None,
            google_maps_api_key=self._google_maps_api_key,
            tasa_code=effective_tasa_code,
            source_file=source_file,
            source_kind=normalized_source_kind,
        )
        payload = self._normalize_payload_for_form(document)
        merge_candidates = self._merge_candidates_for_payload(document_id, payload, 10)
        client_match = self._pick_client_match_candidate(merge_candidates)
        identity_source_document_id = self._safe_value(client_match.get("document_id"))
        identity_match_found = bool(identity_source_document_id)
        workflow_stage = "client_match" if identity_match_found else WORKFLOW_REVIEW
        missing_fields = self._collect_validation_errors(payload, False)
        validation_issues = self._collect_validation_issues(payload, False)

        updated_source = {
            **source_data,
            "source_kind": normalized_source_kind,
            "source_kind_input": normalized_source_kind,
            "source_kind_detected": normalized_source_kind,
            "source_kind_confidence": 1.0,
            "source_kind_auto": False,
            "source_kind_requires_review": False,
        }
        record.update(
            {
                "source": updated_source,
                "document": document,
                "payload": payload,
                "missing_fields": missing_fields,
                "validation_issues": validation_issues,
                "merge_candidates": merge_candidates,
                "client_match": client_match,
                "client_match_decision": "pending" if identity_match_found else "none",
                "identity_match_found": identity_match_found,
                "identity_source_document_id": identity_source_document_id,
                "workflow_stage": workflow_stage,
                "tasa_code": self._safe_value(document.get("tasa_code"))
                or effective_tasa_code,
            }
        )
        self._write_record(document_id, record)
        self._crm_repo.update_document_fields(
            document_id,
            {
                "source": updated_source,
                "ocr_document": document,
                "ocr_payload": payload,
                "edited_payload": None,
                "effective_payload": payload,
                "identifiers": self._identifiers_from_payload(payload),
                "missing_fields": missing_fields,
                "merge_candidates": merge_candidates,
                "client_match": client_match,
                "client_match_decision": (
                    "pending" if identity_match_found else "none"
                ),
                "identity_match_found": identity_match_found,
                "identity_source_document_id": identity_source_document_id,
                "workflow_stage": workflow_stage,
                "status": "uploaded",
            },
        )

        preview_url = self._resolve_preview_url(updated_source)
        return {
            "document_id": document_id,
            "preview_url": preview_url,
            "source": updated_source,
            "document": document,
            "payload": payload,
            "missing_fields": missing_fields,
            "validation_issues": validation_issues,
            "manual_steps_required": record.get("manual_steps_required", []),
            "form_url": self._safe_value(record.get("form_url")),
            "target_url": self._safe_value(record.get("target_url")),
            "identity_match_found": identity_match_found,
            "identity_source_document_id": identity_source_document_id,
            "workflow_stage": workflow_stage,
            "workflow_next_step": stage_to_next_step(workflow_stage),
            "source_kind_input": normalized_source_kind,
            "source_kind_detected": normalized_source_kind,
            "source_kind_confidence": 1.0,
            "source_kind_auto": False,
            "source_kind_requires_review": False,
            "client_match": client_match,
            "client_match_decision": ("pending" if identity_match_found else "none"),
            "enrichment_preview": list(record.get("enrichment_preview") or []),
            "merge_candidates": merge_candidates,
            "family_links": record.get("family_links") or [],
            "family_reference": record.get("family_reference") or {},
        }

    def autofill_address_from_line(
        self, document_id: str, address_line: str
    ) -> dict[str, Any]:
        """Parse and geocode free-form address line to domicilio fields."""
        source_line = self._safe_text(address_line)
        if not source_line:
            raise ApiError(
                status_code=422,
                error_code=ApiErrorCode.VALIDATION_ERROR,
                message="address_line is required.",
            )

        _ = self._read_or_bootstrap_record(document_id)
        expanded_line, _abbr = expand_abbrev(source_line)
        parsed = parse_address_parts(expanded_line, overrides={})
        inferred_piso, inferred_puerta = self._extract_floor_and_door(source_line)

        route_tipo = self._safe_text(parsed.get("tipo_via")).title()
        route_name = self._safe_text(parsed.get("nombre_via_publica"))

        geocode_candidates = fetch_geocode_candidates(
            expanded_line,
            self._google_maps_api_key,
            region="es",
        )
        best = geocode_candidates[0] if geocode_candidates else {}

        geocoded_route = self._geocode_component(best, "route")
        geocoded_tipo, geocoded_nombre = self._extract_via_from_route(geocoded_route)
        geocoded_numero = self._geocode_component(best, "street_number")
        geocoded_cp = self._geocode_component(best, "postal_code")
        geocoded_municipio = self._geocode_component(
            best, "locality"
        ) or self._geocode_component(best, "administrative_area_level_2")
        geocoded_provincia = self._geocode_component(
            best, "administrative_area_level_1"
        )
        parsed_cp = self._safe_text(parsed.get("codigo_postal")) or geocoded_cp
        inferred_city = self._extract_city_by_zip_context(source_line, parsed_cp)

        return {
            "document_id": document_id,
            "address_line": source_line,
            "normalized_address": self._safe_text(best.get("formatted_address")),
            "geocode_used": bool(best),
            "domicilio": {
                "tipo_via": route_tipo or geocoded_tipo,
                "nombre_via": route_name or geocoded_nombre,
                "numero": self._safe_text(parsed.get("numero")) or geocoded_numero,
                "escalera": self._safe_text(parsed.get("escalera")),
                "piso": self._safe_text(parsed.get("piso")) or inferred_piso,
                "puerta": self._safe_text(parsed.get("puerta")) or inferred_puerta,
                "municipio": self._safe_text(parsed.get("municipio"))
                or inferred_city
                or geocoded_municipio,
                "provincia": self._safe_text(parsed.get("provincia"))
                or geocoded_provincia,
                "cp": parsed_cp,
            },
        }

    def get_client_match(self, document_id: str) -> dict[str, Any]:
        """Return current client-match decision context for a document."""
        record = self._read_or_bootstrap_record(document_id)
        client_match = record.get("client_match") or {}
        identity_source_document_id = self._safe_value(
            record.get("identity_source_document_id")
        ) or self._safe_value((client_match or {}).get("document_id"))
        identity_match_found = bool(identity_source_document_id)
        workflow_stage = (
            str(record.get("workflow_stage") or "").strip().lower() or WORKFLOW_REVIEW
        )
        return {
            "document_id": document_id,
            "identity_match_found": identity_match_found,
            "identity_source_document_id": identity_source_document_id,
            "client_match": client_match,
            "client_match_decision": self._safe_value(
                record.get("client_match_decision")
            )
            or "none",
            "merge_candidates": record.get("merge_candidates") or [],
            "workflow_stage": workflow_stage,
            "workflow_next_step": stage_to_next_step(workflow_stage),
        }

    def resolve_client_match(
        self,
        document_id: str,
        *,
        action: str,
        source_document_id: str,
    ) -> dict[str, Any]:
        """Resolve client-match decision without auto-applying merge fields."""
        normalized_action = self._safe_value(action).lower()
        if normalized_action not in {"confirm", "reject"}:
            raise ApiError(
                status_code=422,
                error_code=ApiErrorCode.VALIDATION_ERROR,
                message="action must be one of: confirm, reject.",
            )

        record = self._read_or_bootstrap_record(document_id)
        candidate_id = self._safe_value(source_document_id) or self._safe_value(
            record.get("identity_source_document_id")
        )
        if not candidate_id:
            candidate_id = self._safe_value(
                (record.get("client_match") or {}).get("document_id")
            )

        if normalized_action == "confirm":
            if not candidate_id:
                raise ApiError(
                    status_code=422,
                    error_code=ApiErrorCode.VALIDATION_ERROR,
                    message="source_document_id is required for confirm action.",
                )
            payload = record.get("payload") or {}
            if not isinstance(payload, dict):
                raise ApiError(
                    status_code=422,
                    error_code=ApiErrorCode.DOCUMENT_INVALID_PAYLOAD,
                    message="Invalid payload in document record.",
                )
            record = self._read_or_bootstrap_record(document_id)
            record["client_match_decision"] = "confirmed"
            record["workflow_stage"] = WORKFLOW_REVIEW
            record["identity_match_found"] = True
            record["identity_source_document_id"] = candidate_id
            record["enrichment_preview"] = []
            self._write_record(document_id, record)
            self._crm_repo.update_document_fields(
                document_id,
                {
                    "client_match_decision": "confirmed",
                    "workflow_stage": WORKFLOW_REVIEW,
                    "identity_match_found": True,
                    "identity_source_document_id": record[
                        "identity_source_document_id"
                    ],
                    "enrichment_preview": [],
                },
            )
            missing_fields = self._collect_validation_errors(payload, False)
            validation_issues = self._collect_validation_issues(payload, False)
            merge_candidates = self._merge_candidates_for_payload(
                document_id, payload, 10
            )
            record["merge_candidates"] = merge_candidates
            self._write_record(document_id, record)
            self._crm_repo.update_document_fields(
                document_id,
                {"merge_candidates": merge_candidates},
            )
            client_entity = self._crm_repo.ensure_client_entity(
                document_id=document_id,
                source_document_id=candidate_id,
            )
            record["client_id"] = self._safe_value(client_entity.get("client_id"))
            self._write_record(document_id, record)
            self._crm_repo.update_document_fields(
                document_id,
                {"client_id": record["client_id"]},
            )
            return {
                "document_id": document_id,
                "missing_fields": missing_fields,
                "validation_issues": validation_issues,
                "payload": payload,
                "manual_steps_required": record.get("manual_steps_required", []),
                "identity_match_found": True,
                "identity_source_document_id": self._safe_value(
                    record.get("identity_source_document_id")
                ),
                "workflow_stage": WORKFLOW_REVIEW,
                "workflow_next_step": stage_to_next_step(WORKFLOW_REVIEW),
                "client_match": record.get("client_match") or {},
                "client_match_decision": "confirmed",
                "enrichment_preview": [],
                "merge_candidates": merge_candidates,
                "family_links": record.get("family_links") or [],
                "family_reference": record.get("family_reference") or {},
                "client_id": record.get("client_id") or "",
            }

        record["client_match_decision"] = "rejected"
        record["workflow_stage"] = WORKFLOW_REVIEW
        record["identity_match_found"] = False
        record["identity_source_document_id"] = ""
        self._write_record(document_id, record)
        self._crm_repo.update_document_fields(
            document_id,
            {
                "client_match_decision": "rejected",
                "workflow_stage": WORKFLOW_REVIEW,
                "identity_match_found": False,
                "identity_source_document_id": "",
            },
        )
        payload = record.get("payload") or {}
        if not isinstance(payload, dict):
            payload = {}
        return {
            "document_id": document_id,
            "missing_fields": self._collect_validation_errors(payload, False),
            "validation_issues": self._collect_validation_issues(payload, False),
            "payload": payload,
            "manual_steps_required": record.get("manual_steps_required", []),
            "identity_match_found": False,
            "identity_source_document_id": "",
            "workflow_stage": WORKFLOW_REVIEW,
            "workflow_next_step": stage_to_next_step(WORKFLOW_REVIEW),
            "client_match": record.get("client_match") or {},
            "client_match_decision": "rejected",
            "enrichment_preview": list(record.get("enrichment_preview") or []),
            "merge_candidates": record.get("merge_candidates") or [],
            "family_links": record.get("family_links") or [],
            "family_reference": record.get("family_reference") or {},
        }

    def get_merge_candidates(self, document_id: str) -> dict[str, Any]:
        """Recompute merge candidates and persist them for the document."""
        record = self._read_or_bootstrap_record(document_id)
        payload = record.get("payload") or {}
        if not isinstance(payload, dict):
            raise ApiError(
                status_code=422,
                error_code=ApiErrorCode.DOCUMENT_INVALID_PAYLOAD,
                message="Invalid payload in document record.",
            )

        candidates = self._merge_candidates_for_payload(document_id, payload, 10)
        record["merge_candidates"] = candidates
        self._write_record(document_id, record)
        self._crm_repo.update_document_fields(
            document_id, {"merge_candidates": candidates}
        )

        return {"document_id": document_id, "merge_candidates": candidates}

    def confirm_document(
        self, document_id: str, payload: dict[str, Any]
    ) -> dict[str, Any]:
        """Persist operator-confirmed payload and refresh derived fields."""
        record = self._read_or_bootstrap_record(document_id)
        merge_candidates = self._merge_candidates_for_payload(document_id, payload, 10)
        missing_fields = self._collect_validation_errors(payload, False)
        validation_issues = self._collect_validation_issues(payload, False)

        record["payload"] = payload
        record["missing_fields"] = missing_fields
        record["merge_candidates"] = merge_candidates
        record["workflow_stage"] = WORKFLOW_PREPARE
        self._write_record(document_id, record)

        self._crm_repo.save_edited_payload(
            document_id=document_id,
            payload=payload,
            missing_fields=missing_fields,
        )
        self._crm_repo.update_document_fields(
            document_id,
            {
                "merge_candidates": merge_candidates,
                "workflow_stage": WORKFLOW_PREPARE,
            },
        )

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
            "workflow_stage": WORKFLOW_PREPARE,
            "workflow_next_step": stage_to_next_step(WORKFLOW_PREPARE),
            "client_match": record.get("client_match") or {},
            "client_match_decision": record.get("client_match_decision") or "none",
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
        selected_fields: list[str] | None = None,
    ) -> dict[str, Any]:
        """Preview or apply identity-based enrichment."""
        record = self._read_or_bootstrap_record(document_id)
        payload = record.get("payload") or {}
        if not isinstance(payload, dict):
            raise ApiError(
                status_code=422,
                error_code=ApiErrorCode.DOCUMENT_INVALID_PAYLOAD,
                message="Invalid payload in document record.",
            )

        enrichment = self._enrich_record_payload_by_identity(
            document_id,
            payload,
            bool(apply),
            source_document_id,
            selected_fields,
        )
        payload_candidate = enrichment.get("payload")
        enriched_payload = (
            payload_candidate if isinstance(payload_candidate, dict) else payload
        )

        if not apply:
            merge_candidates = self._merge_candidates_for_payload(
                document_id, payload, 10
            )
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
                "enrichment_skipped": enrichment.get("enrichment_skipped", []),
                "merge_candidates": merge_candidates,
                "missing_fields": self._collect_validation_errors(payload, False),
                "validation_issues": self._collect_validation_issues(payload, False),
                "payload": payload,
                "workflow_stage": record.get("workflow_stage") or WORKFLOW_REVIEW,
                "workflow_next_step": stage_to_next_step(
                    str(record.get("workflow_stage") or WORKFLOW_REVIEW)
                ),
                "client_match": record.get("client_match") or {},
                "client_match_decision": record.get("client_match_decision") or "none",
            }

        missing_fields = self._collect_validation_errors(enriched_payload, False)
        validation_issues = self._collect_validation_issues(enriched_payload, False)
        merge_candidates = self._merge_candidates_for_payload(
            document_id, enriched_payload, 10
        )

        updated_record = self._read_or_bootstrap_record(document_id)
        updated_record["merge_candidates"] = merge_candidates
        updated_record["workflow_stage"] = WORKFLOW_REVIEW
        self._write_record(document_id, updated_record)
        self._crm_repo.update_document_fields(
            document_id,
            {"merge_candidates": merge_candidates, "workflow_stage": WORKFLOW_REVIEW},
        )

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
            "enrichment_skipped": enrichment.get("enrichment_skipped", []),
            "merge_candidates": merge_candidates,
            "missing_fields": missing_fields,
            "validation_issues": validation_issues,
            "payload": enriched_payload,
            "workflow_stage": WORKFLOW_REVIEW,
            "workflow_next_step": stage_to_next_step(WORKFLOW_REVIEW),
            "client_match": updated_record.get("client_match") or {},
            "client_match_decision": updated_record.get("client_match_decision")
            or "none",
        }
