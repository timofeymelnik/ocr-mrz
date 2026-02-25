from __future__ import annotations

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
