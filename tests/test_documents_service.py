from __future__ import annotations

from pathlib import Path

import pytest

from app.api.errors import ApiError
from app.documents.service import DocumentsService


class _CRMRepo:
    def __init__(self) -> None:
        self.saved_payload: dict[str, object] = {}
        self.updated: dict[str, object] = {}

    def save_edited_payload(
        self,
        *,
        document_id: str,
        payload: dict[str, object],
        missing_fields: list[str] | None = None,
    ) -> dict[str, object]:
        self.saved_payload = {
            "document_id": document_id,
            "payload": payload,
            "missing_fields": missing_fields or [],
        }
        return self.saved_payload

    def update_document_fields(
        self, document_id: str, patch: dict[str, object]
    ) -> dict[str, object]:
        self.updated = {"document_id": document_id, **patch}
        return self.updated

    def ensure_client_entity(
        self, *, document_id: str, source_document_id: str = ""
    ) -> dict[str, object]:
        return {
            "client_id": "client-1",
            "primary_document_id": document_id,
            "document_ids": (
                [document_id, source_document_id]
                if source_document_id
                else [document_id]
            ),
        }


def _build_service(record: dict[str, object]) -> DocumentsService:
    repo = _CRMRepo()

    def _read(doc_id: str) -> dict[str, object]:
        _ = doc_id
        return dict(record)

    def _write(doc_id: str, payload: dict[str, object]) -> None:
        _ = doc_id
        record.update(payload)

    return DocumentsService(
        crm_repo=repo,
        read_or_bootstrap_record=_read,
        write_record=_write,
        merge_candidates_for_payload=lambda document_id, payload, limit: [
            {"document_id": "related", "score": 100}
        ],
        collect_validation_errors=lambda payload, require_tramite: [],
        collect_validation_issues=lambda payload, require_tramite: [],
        sync_family_reference=lambda doc_id, payload, source: {
            "linked": False,
            "family_links": [],
            "family_reference": {},
        },
        enrich_record_payload_by_identity=lambda doc_id, payload, apply, source_id, selected_fields: {
            "identity_match_found": True,
            "identity_source_document_id": "source-1",
            "identity_key": "X1",
            "applied_fields": ["identificacion.nombre"],
            "skipped_fields": [],
            "enrichment_preview": [{"field": "identificacion.nombre"}],
            "payload": payload,
        },
        safe_value=lambda value: str(value or ""),
    )


def _build_service_with_reprocess(record: dict[str, object]) -> DocumentsService:
    repo = _CRMRepo()

    def _read(doc_id: str) -> dict[str, object]:
        _ = doc_id
        return dict(record)

    def _write(doc_id: str, payload: dict[str, object]) -> None:
        _ = doc_id
        record.update(payload)

    class _OCRClient:
        def extract_text(self, source: Path):
            _ = source
            return type("OCRResult", (), {"full_text": "OCR TEXT EX-17"})()

    return DocumentsService(
        crm_repo=repo,
        read_or_bootstrap_record=_read,
        write_record=_write,
        merge_candidates_for_payload=lambda document_id, payload, limit: [],
        collect_validation_errors=lambda payload, require_tramite: [],
        collect_validation_issues=lambda payload, require_tramite: [],
        sync_family_reference=lambda doc_id, payload, source: {
            "linked": False,
            "family_links": [],
            "family_reference": {},
        },
        enrich_record_payload_by_identity=lambda doc_id, payload, apply, source_id, selected_fields: {
            "identity_match_found": False,
            "identity_source_document_id": "",
            "identity_key": "",
            "applied_fields": [],
            "skipped_fields": [],
            "enrichment_preview": [],
            "payload": payload,
        },
        build_tasa_document=lambda **kwargs: {
            "tasa_code": "ex_17",
            "source": {
                "source_file": kwargs.get("source_file", ""),
                "source_kind": kwargs.get("source_kind", ""),
            },
        },
        normalize_payload_for_form=lambda document: {
            "identificacion": {
                "nif_nie": "Y9840934F",
                "pasaporte": "",
                "nombre_apellidos": "Melnyk Tymofii",
            },
            "domicilio": {},
            "declarante": {},
            "ingreso": {},
            "extra": {},
            "autoliquidacion": {},
            "tramite": {},
            "captcha": {"manual": True},
            "download": {"dir": "./downloads", "filename_prefix": "test"},
        },
        create_ocr_client=_OCRClient,
        artifact_url_from_value=lambda value: "/runtime/uploads/from-stored-path.pdf"
        if str(value)
        else "",
        safe_value=lambda value: str(value or ""),
    )


def test_documents_service_resolve_client_match_confirm_only_links_client() -> None:
    record = {
        "document_id": "doc-6",
        "payload": {"identificacion": {"nif_nie": "X1"}},
        "source": {},
        "client_match": {"document_id": "source-1", "score": 100},
        "identity_source_document_id": "",
        "identity_match_found": True,
        "workflow_stage": "client_match",
        "merge_candidates": [{"document_id": "source-1", "score": 100}],
        "enrichment_preview": [{"field": "identificacion.nombre"}],
    }
    service = _build_service(record)

    response = service.resolve_client_match(
        "doc-6", action="confirm", source_document_id=""
    )

    assert response["document_id"] == "doc-6"
    assert response["identity_source_document_id"] == "source-1"
    assert response["client_match_decision"] == "confirmed"
    assert response["workflow_stage"] == "review"
    assert response["payload"] == {"identificacion": {"nif_nie": "X1"}}
    assert response["enrichment_preview"] == []


def test_documents_service_confirm_document_returns_contract_fields() -> None:
    record = {"document_id": "doc-1", "payload": {"identificacion": {}}, "source": {}}
    service = _build_service(record)

    response = service.confirm_document("doc-1", {"identificacion": {"nombre": "A"}})

    assert response["document_id"] == "doc-1"
    assert response["merge_candidates"][0]["document_id"] == "related"


def test_documents_service_enrich_preview_mode() -> None:
    record = {"document_id": "doc-2", "payload": {"identificacion": {}}, "source": {}}
    service = _build_service(record)

    response = service.enrich_by_identity("doc-2", apply=False, source_document_id="")

    assert response["identity_match_found"] is True
    assert response["identity_source_document_id"] == "source-1"


def test_documents_service_invalid_payload_raises_api_error() -> None:
    record = {"document_id": "doc-3", "payload": "invalid", "source": {}}
    service = _build_service(record)

    with pytest.raises(ApiError) as exc:
        service.enrich_by_identity("doc-3", apply=False, source_document_id="")

    assert exc.value.status_code == 422


def test_documents_service_get_client_match_from_record() -> None:
    record = {
        "document_id": "doc-4",
        "payload": {"identificacion": {}},
        "source": {},
        "client_match": {"document_id": "source-1", "score": 100},
        "identity_source_document_id": "source-1",
        "identity_match_found": True,
        "client_match_decision": "pending",
        "workflow_stage": "client_match",
        "merge_candidates": [{"document_id": "source-1", "score": 100}],
    }
    service = _build_service(record)

    response = service.get_client_match("doc-4")

    assert response["document_id"] == "doc-4"
    assert response["identity_match_found"] is True
    assert response["workflow_stage"] == "client_match"


def test_documents_service_resolve_client_match_reject_moves_to_review() -> None:
    record = {
        "document_id": "doc-5",
        "payload": {"identificacion": {"nif_nie": "X1"}},
        "source": {},
        "client_match": {"document_id": "source-1", "score": 100},
        "identity_source_document_id": "source-1",
        "identity_match_found": True,
        "workflow_stage": "client_match",
        "merge_candidates": [{"document_id": "source-1", "score": 100}],
    }
    service = _build_service(record)

    response = service.resolve_client_match(
        "doc-5", action="reject", source_document_id=""
    )

    assert response["document_id"] == "doc-5"
    assert response["identity_match_found"] is False
    assert response["workflow_stage"] == "review"


def test_documents_service_address_autofill_parses_line_without_geocode() -> None:
    record = {"document_id": "doc-7", "payload": {"domicilio": {}}, "source": {}}
    service = _build_service(record)

    response = service.autofill_address_from_line(
        "doc-7",
        "Calle Enrique Monsonis Domingo, 5, 2 B, Alicante, 03013",
    )

    domicilio = response["domicilio"]
    assert response["document_id"] == "doc-7"
    assert domicilio["tipo_via"]
    assert domicilio["numero"] == "5"
    assert domicilio["piso"] == "2"
    assert domicilio["puerta"] == "B"
    assert domicilio["municipio"] == "Alicante"
    assert domicilio["cp"] == "03013"


def test_documents_service_address_autofill_uses_geocode_components(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    record = {"document_id": "doc-8", "payload": {"domicilio": {}}, "source": {}}
    service = _build_service(record)

    def _fake_geocode(*args: object, **kwargs: object) -> list[dict[str, object]]:
        _ = args
        _ = kwargs
        return [
            {
                "formatted_address": "Calle Mayor, 7, 03001 Alicante, Spain",
                "address_components": [
                    {"long_name": "Calle Mayor", "types": ["route"]},
                    {"long_name": "7", "types": ["street_number"]},
                    {"long_name": "03001", "types": ["postal_code"]},
                    {"long_name": "Alicante", "types": ["locality"]},
                    {
                        "long_name": "Alicante",
                        "types": ["administrative_area_level_1"],
                    },
                ],
            }
        ]

    monkeypatch.setattr(
        "app.documents.service.fetch_geocode_candidates",
        _fake_geocode,
    )

    response = service.autofill_address_from_line("doc-8", "Mayor 7 Alicante")

    domicilio = response["domicilio"]
    assert response["geocode_used"] is True
    assert response["normalized_address"] == "Calle Mayor, 7, 03001 Alicante, Spain"
    assert domicilio["municipio"] == "Alicante"
    assert domicilio["provincia"] == "Alicante"
    assert domicilio["cp"] == "03001"


def test_documents_service_reprocess_document_ocr_updates_payload(
    tmp_path: Path,
) -> None:
    source_file = tmp_path / "source.pdf"
    source_file.write_bytes(b"%PDF-1.4 mock")
    record = {
        "document_id": "doc-9",
        "source": {
            "stored_path": str(source_file),
            "original_filename": "source.pdf",
            "preview_url": "/runtime/uploads/source.pdf",
            "source_kind": "anketa",
        },
        "form_url": "https://example.test/form",
        "target_url": "https://example.test/form",
        "manual_steps_required": ["verify_filled_fields"],
        "family_links": [],
        "family_reference": {},
        "enrichment_preview": [],
    }
    service = _build_service_with_reprocess(record)

    response = service.reprocess_document_ocr(
        "doc-9", source_kind="nie_tie", tasa_code="ex_17"
    )

    assert response["document_id"] == "doc-9"
    assert response["source_kind_detected"] == "nie_tie"
    assert response["preview_url"] == "/runtime/uploads/source.pdf"
    assert response["payload"]["identificacion"]["nif_nie"] == "Y9840934F"


def test_documents_service_get_document_resolves_preview_from_stored_path() -> None:
    record = {
        "document_id": "doc-10",
        "source": {
            "stored_path": "/tmp/source.pdf",
            "preview_url": "",
        },
    }

    service = DocumentsService(
        crm_repo=_CRMRepo(),
        read_or_bootstrap_record=lambda _doc_id: dict(record),
        write_record=lambda _doc_id, _payload: None,
        merge_candidates_for_payload=lambda _document_id, _payload, _limit: [],
        collect_validation_errors=lambda _payload, _require_tramite: [],
        collect_validation_issues=lambda _payload, _require_tramite: [],
        sync_family_reference=lambda _doc_id, _payload, _source: {
            "linked": False,
            "family_links": [],
            "family_reference": {},
        },
        enrich_record_payload_by_identity=lambda _doc_id, payload, _apply, _source_id, _selected_fields: {
            "identity_match_found": False,
            "identity_source_document_id": "",
            "identity_key": "",
            "applied_fields": [],
            "skipped_fields": [],
            "enrichment_preview": [],
            "payload": payload,
        },
        artifact_url_from_value=lambda value: "/runtime/uploads/recovered.pdf"
        if str(value)
        else "",
        safe_value=lambda value: str(value or ""),
    )

    response = service.get_document("doc-10")

    assert response["preview_url"] == "/runtime/uploads/recovered.pdf"
