from __future__ import annotations

import asyncio
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Any, cast

from fastapi import UploadFile

from app.api.errors import ApiError
from app.documents.upload_service import UploadService


@dataclass
class _FakeOCRResult:
    full_text: str
    pages: list[int]
    ocr_source: str


class _FakeOCRClient:
    def extract_text(self, source_path: Path) -> _FakeOCRResult:
        _ = source_path
        return _FakeOCRResult(full_text="OCR TEXT", pages=[1], ocr_source="live")


class _FakeCRMRepo:
    def __init__(self) -> None:
        self.upsert_calls: list[dict[str, Any]] = []
        self.update_calls: list[tuple[str, dict[str, Any]]] = []

    def upsert_from_upload(self, **kwargs: Any) -> dict[str, Any]:
        self.upsert_calls.append(kwargs)
        return {"document_id": kwargs["document_id"]}

    def update_document_fields(
        self, document_id: str, patch: dict[str, Any]
    ) -> dict[str, Any]:
        self.update_calls.append((document_id, patch))
        return {"document_id": document_id, **patch}


def _upload_file(filename: str, data: bytes = b"abc") -> UploadFile:
    return UploadFile(filename=filename, file=BytesIO(data))


def _service(
    tmp_path: Path,
    repo: _FakeCRMRepo,
    *,
    family_sync_result: dict[str, Any] | None = None,
) -> tuple[UploadService, dict[str, dict[str, Any]]]:
    records: dict[str, dict[str, Any]] = {}

    def _write_record(document_id: str, data: dict[str, Any]) -> None:
        records[document_id] = data

    service = UploadService(
        uploads_dir=tmp_path,
        default_target_url="https://example.test/form",
        crm_repo=repo,
        safe_value=lambda value: "" if value is None else str(value).strip(),
        runtime_url=lambda path: f"/runtime/{path.name}",
        allowed_suffix=lambda filename: Path(filename).suffix.lower()
        in {".jpg", ".jpeg", ".png", ".pdf"},
        write_record=_write_record,
        merge_candidates_for_payload=lambda document_id, payload, limit: [
            {
                "document_id": "candidate-1",
                "score": 100,
                "limit_used": limit,
                "source_document_id": document_id,
                "name": str(payload.get("name") or ""),
            }
        ],
        collect_validation_errors=lambda payload, require_tramite: (
            [] if payload else ["missing"]
        ),
        collect_validation_issues=lambda payload, require_tramite: (
            [] if payload else [{"code": "missing"}]
        ),
        build_tasa_document=lambda **kwargs: {
            "name": "doc",
            "forms": {"base": {}},
            "source_kind": kwargs["source_kind"],
            "ocr_text": kwargs["ocr_front"],
        },
        normalize_payload_for_form=lambda document: {
            "identificacion": {"nif_nie": "X1"},
            "name": str(document.get("name") or ""),
        },
        attach_pipeline_metadata=lambda **kwargs: kwargs["document"],
        stage_start=lambda: 0.0,
        stage_success=lambda stage, started, details=None: {
            "stage": stage,
            "details": details or {},
            "started": started,
        },
        create_ocr_client=_FakeOCRClient,
        sync_family_reference=lambda document_id, payload, source: family_sync_result
        or {"linked": False, "family_links": []},
    )
    return service, records


def test_upload_service_rejects_invalid_file_suffix(tmp_path: Path) -> None:
    repo = _FakeCRMRepo()
    service, _ = _service(tmp_path, repo)

    try:
        asyncio.run(
            service.upload_document(
                file=_upload_file("sample.txt"),
                tasa_code="790_012",
                source_kind="passport",
            )
        )
    except ApiError as exc:
        detail: dict[str, Any] = (
            cast(dict[str, Any], exc.detail) if isinstance(exc.detail, dict) else {}
        )
        assert exc.status_code == 400
        assert str(detail.get("error_code", "")) == "VALIDATION_ERROR"
    else:
        raise AssertionError("Expected ApiError for invalid suffix")


def test_upload_service_rejects_invalid_source_kind(tmp_path: Path) -> None:
    repo = _FakeCRMRepo()
    service, _ = _service(tmp_path, repo)

    try:
        asyncio.run(
            service.upload_document(
                file=_upload_file("sample.pdf"),
                tasa_code="790_012",
                source_kind="unknown",
            )
        )
    except ApiError as exc:
        detail: dict[str, Any] = (
            cast(dict[str, Any], exc.detail) if isinstance(exc.detail, dict) else {}
        )
        assert exc.status_code == 422
        assert str(detail.get("error_code", "")) == "VALIDATION_ERROR"
    else:
        raise AssertionError("Expected ApiError for invalid source_kind")


def test_upload_service_happy_path_persists_record_and_crm(tmp_path: Path) -> None:
    repo = _FakeCRMRepo()
    service, records = _service(tmp_path, repo)

    result = asyncio.run(
        service.upload_document(
            file=_upload_file("sample.pdf"),
            tasa_code="790_012",
            source_kind="passport",
        )
    )

    document_id = str(result["document_id"])
    assert document_id in records
    assert result["target_url"] == "https://example.test/form"
    assert result["manual_steps_required"] == [
        "verify_filled_fields",
        "submit_or_download_manually",
    ]
    assert len(repo.upsert_calls) == 1
    assert repo.upsert_calls[0]["document_id"] == document_id
    assert len(repo.update_calls) == 1
    assert repo.update_calls[0][0] == document_id
    assert repo.update_calls[0][1]["workflow_stage"] == "client_match"


def test_upload_service_auto_detects_source_kind_when_not_provided(
    tmp_path: Path,
) -> None:
    repo = _FakeCRMRepo()
    service, records = _service(tmp_path, repo)

    result = asyncio.run(
        service.upload_document(
            file=_upload_file("sample.pdf"),
            tasa_code="790_012",
            source_kind="",
        )
    )

    document_id = str(result["document_id"])
    source = records[document_id]["source"]
    assert source["source_kind_detected"] in {
        "anketa",
        "passport",
        "nie_tie",
        "visa",
        "fmiliar",
    }
    assert source["source_kind_auto"] is True
    assert isinstance(result["source_kind_confidence"], float)


def test_upload_service_updates_family_links_when_sync_linked(tmp_path: Path) -> None:
    repo = _FakeCRMRepo()
    family_sync = {
        "linked": True,
        "family_links": [{"related_document_id": "rel-1", "relation": "familiar"}],
        "family_reference": {"pasaporte": "P123"},
    }
    service, records = _service(tmp_path, repo, family_sync_result=family_sync)

    result = asyncio.run(
        service.upload_document(
            file=_upload_file("sample.pdf"),
            tasa_code="790_012",
            source_kind="fmiliar",
        )
    )

    document_id = str(result["document_id"])
    assert records[document_id]["family_links"] == family_sync["family_links"]
    assert records[document_id]["family_reference"] == family_sync["family_reference"]
    assert len(repo.update_calls) == 2
    assert repo.update_calls[0][0] == document_id
    assert repo.update_calls[1][0] == document_id
