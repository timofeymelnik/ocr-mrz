from __future__ import annotations

from typing import Any

from app.documents.enrichment_service import DocumentEnrichmentService


class _FakeRepo:
    def __init__(self) -> None:
        self.docs: dict[str, dict[str, Any]] = {}
        self.saved_payloads: list[dict[str, Any]] = []
        self.updated_fields: list[tuple[str, dict[str, Any]]] = []
        self.upsert_calls: list[dict[str, Any]] = []

    def get_document(self, document_id: str) -> dict[str, Any] | None:
        return self.docs.get(document_id)

    def search_documents(self, *, query: str, limit: int) -> list[dict[str, Any]]:
        _ = query
        out: list[dict[str, Any]] = []
        for doc_id, doc in list(self.docs.items())[:limit]:
            out.append(
                {
                    "document_id": doc_id,
                    "name": str(doc.get("name") or ""),
                    "document_number": str(doc.get("document_number") or ""),
                    "updated_at": str(doc.get("updated_at") or ""),
                }
            )
        return out

    def find_latest_by_identities(
        self, identities: list[str], *, exclude_document_id: str
    ) -> dict[str, Any] | None:
        identity_set = set(identities)
        for doc_id, doc in self.docs.items():
            if doc_id == exclude_document_id:
                continue
            payload = (
                doc.get("effective_payload")
                or doc.get("edited_payload")
                or doc.get("ocr_payload")
                or {}
            )
            if not isinstance(payload, dict):
                continue
            nif_nie = str(
                (((payload.get("identificacion") or {}).get("nif_nie")) or "")
            ).strip()
            pasaporte = str(
                (((payload.get("identificacion") or {}).get("pasaporte")) or "")
            ).strip()
            if nif_nie in identity_set or pasaporte in identity_set:
                return {"document_id": doc_id, **doc}
        return None

    def save_edited_payload(
        self, *, document_id: str, payload: dict[str, Any], missing_fields: list[str]
    ) -> None:
        self.saved_payloads.append(
            {
                "document_id": document_id,
                "payload": payload,
                "missing_fields": missing_fields,
            }
        )
        doc = self.docs.setdefault(document_id, {})
        doc["edited_payload"] = payload
        doc["missing_fields"] = missing_fields

    def update_document_fields(self, document_id: str, fields: dict[str, Any]) -> None:
        self.updated_fields.append((document_id, fields))
        doc = self.docs.setdefault(document_id, {})
        doc.update(fields)

    def upsert_from_upload(self, **kwargs: Any) -> None:
        self.upsert_calls.append(kwargs)
        self.docs[kwargs["document_id"]] = {
            "effective_payload": kwargs["payload"],
            "document_id": kwargs["document_id"],
        }


def _service(
    repo: _FakeRepo,
    *,
    records: dict[str, dict[str, Any]] | None = None,
) -> DocumentEnrichmentService:
    record_store = records if records is not None else {}

    def _read_or_bootstrap(document_id: str) -> dict[str, Any]:
        return record_store.setdefault(document_id, {"payload": {}})

    def _write_record(document_id: str, data: dict[str, Any]) -> None:
        record_store[document_id] = data

    return DocumentEnrichmentService(
        repo=repo,
        default_target_url="https://example.test/form",
        safe_value=lambda value: "" if value is None else str(value).strip(),
        normalize_payload_for_form=lambda payload: payload,
        collect_validation_errors=lambda payload, require_tramite: (
            [] if payload else ["missing"]
        ),
        read_or_bootstrap_record=_read_or_bootstrap,
        write_record=_write_record,
    )


def test_enrichment_service_identity_and_family_extractors() -> None:
    repo = _FakeRepo()
    service = _service(repo)
    payload = {
        "identificacion": {"nif_nie": "x-1234-z", "pasaporte": "p 99"},
        "referencias": {
            "familiar_que_da_derecho": {
                "pasaporte": " ab-123 ",
                "nombre_apellidos": "",
                "primer_apellido": "GARCIA",
                "nombre": "ANA",
            }
        },
    }

    candidates = service.identity_candidates(payload)
    family_ref = service.family_reference_from_payload(payload)

    assert candidates == ["X1234Z", "P99"]
    assert family_ref["document_number"] == "AB123"
    assert family_ref["nombre_apellidos"] == "GARCIA ANA"


def test_enrichment_service_fill_empty_applies_and_skips() -> None:
    repo = _FakeRepo()
    service = _service(repo)
    payload = {"identificacion": {"nombre": "ALFA"}}
    source = {"identificacion": {"nombre": "BETA", "primer_apellido": "TEST"}}

    enriched, applied, skipped = service.enrich_payload_fill_empty(
        payload=payload,
        source_payload=source,
        source_document_id="src-1",
    )

    assert enriched["identificacion"]["nombre"] == "ALFA"
    assert enriched["identificacion"]["primer_apellido"] == "TEST"
    assert any(row["field"] == "identificacion.primer_apellido" for row in applied)
    assert any(row["field"] == "identificacion.nombre" for row in skipped)
    assert any(row["reason"] == "conflict" for row in skipped)


def test_enrichment_service_fill_empty_marks_equal_reason() -> None:
    repo = _FakeRepo()
    service = _service(repo)
    payload = {"identificacion": {"nombre": "ALFA"}}
    source = {"identificacion": {"nombre": "ALFA"}}

    _, _, skipped = service.enrich_payload_fill_empty(
        payload=payload,
        source_payload=source,
        source_document_id="src-1",
    )

    assert len(skipped) == 1
    assert skipped[0]["reason"] == "equal"


def test_enrichment_service_merge_candidates_prioritizes_identity_match() -> None:
    repo = _FakeRepo()
    repo.docs = {
        "doc-a": {
            "name": "ALFA TEST",
            "document_number": "X1",
            "updated_at": "2026-02-24T10:00:00Z",
            "effective_payload": {
                "identificacion": {
                    "nif_nie": "X1",
                    "primer_apellido": "ALFA",
                    "nombre": "TEST",
                }
            },
        },
        "doc-b": {
            "name": "BETA TEST",
            "document_number": "Y1",
            "updated_at": "2026-02-24T09:00:00Z",
            "effective_payload": {
                "identificacion": {
                    "nif_nie": "Y1",
                    "primer_apellido": "ALFA",
                    "nombre": "TEST",
                }
            },
        },
    }
    service = _service(repo)
    payload = {
        "identificacion": {"nif_nie": "X1", "primer_apellido": "ALFA", "nombre": "TEST"}
    }

    out = service.merge_candidates_for_payload("doc-main", payload, limit=10)

    assert len(out) == 2
    assert out[0]["document_id"] == "doc-a"
    assert "document_match" in out[0]["reasons"]


def test_enrichment_service_sync_family_reference_creates_related_document() -> None:
    repo = _FakeRepo()
    repo.docs["doc-main"] = {"family_links": []}
    service = _service(repo)
    payload = {
        "identificacion": {"nif_nie": "X123"},
        "referencias": {
            "familiar_que_da_derecho": {
                "pasaporte": "P999",
                "nombre_apellidos": "FAMILY MEMBER",
            }
        },
    }
    source = {"original_filename": "src.pdf", "stored_path": "/tmp/s.pdf"}

    result = service.sync_family_reference("doc-main", payload, source)

    assert result["linked"] is True
    assert result["created"] is True
    assert len(repo.upsert_calls) == 1
    assert any(row[0] == "doc-main" for row in repo.updated_fields)


def test_enrichment_service_enrich_record_payload_by_identity_persists() -> None:
    repo = _FakeRepo()
    repo.docs["source-1"] = {
        "document_id": "source-1",
        "effective_payload": {
            "identificacion": {"nif_nie": "X1", "nombre": "ALFA"},
            "domicilio": {"provincia": "MADRID"},
        },
    }
    records = {"doc-main": {"payload": {"identificacion": {"nif_nie": "X1"}}}}
    service = _service(repo, records=records)
    payload = {"identificacion": {"nif_nie": "X1"}}

    result = service.enrich_record_payload_by_identity(
        "doc-main", payload, persist=True
    )

    assert result["identity_match_found"] is True
    assert result["identity_source_document_id"] == "source-1"
    assert records["doc-main"]["identity_key"] == "X1"
    assert len(repo.saved_payloads) == 1


def test_enrichment_service_enrich_record_payload_by_identity_no_match() -> None:
    repo = _FakeRepo()
    service = _service(repo)
    payload = {"identificacion": {"nif_nie": ""}}

    result = service.enrich_record_payload_by_identity(
        "doc-main", payload, persist=False
    )

    assert result["identity_match_found"] is False
    assert result["identity_source_document_id"] == ""
