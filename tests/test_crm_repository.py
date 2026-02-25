from __future__ import annotations

import json
from pathlib import Path
from time import sleep
from typing import Any

from app.crm.repository import CRMRepository


def _payload(nif_nie: str, name: str) -> dict[str, Any]:
    return {
        "identificacion": {
            "nif_nie": nif_nie,
            "pasaporte": "",
            "nombre_apellidos": name,
        }
    }


def test_crm_repository_upsert_get_and_save_edited_payload(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.delenv("MONGODB_URI", raising=False)
    repo = CRMRepository(tmp_path)

    uploaded = repo.upsert_from_upload(
        document_id="doc-1",
        payload=_payload("X1", "ALFA TEST"),
        ocr_document={"raw": "ocr"},
        source={"source_kind": "passport"},
        missing_fields=["f1"],
        manual_steps_required=["step"],
        form_url="u1",
        target_url="u1",
    )
    edited = repo.save_edited_payload(
        document_id="doc-1",
        payload=_payload("X1", "ALFA TEST EDITED"),
        missing_fields=[],
    )
    loaded = repo.get_document("doc-1")

    assert uploaded["status"] == "uploaded"
    assert edited["status"] == "confirmed"
    assert loaded is not None
    assert loaded["identifiers"]["name"] == "ALFA TEST EDITED"
    assert loaded["edited_payload"]["identificacion"]["nombre_apellidos"] == "ALFA TEST EDITED"


def test_crm_repository_search_documents_dedupes_by_document_number(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.delenv("MONGODB_URI", raising=False)
    repo = CRMRepository(tmp_path)

    repo.upsert_from_upload(
        document_id="doc-old",
        payload=_payload("X1", "ALFA"),
        ocr_document={},
        source={},
        missing_fields=[],
        manual_steps_required=[],
        form_url="u",
        target_url="u",
    )
    sleep(0.01)
    repo.upsert_from_upload(
        document_id="doc-new",
        payload=_payload("x-1", "ALFA NEW"),
        ocr_document={},
        source={},
        missing_fields=[],
        manual_steps_required=[],
        form_url="u",
        target_url="u",
    )

    summaries = repo.search_documents(limit=10)
    ids = [str(row.get("document_id")) for row in summaries]

    assert "doc-new" in ids
    assert "doc-old" not in ids


def test_crm_repository_find_latest_by_identities_and_exclude(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.delenv("MONGODB_URI", raising=False)
    repo = CRMRepository(tmp_path)
    repo.upsert_from_upload(
        document_id="doc-1",
        payload=_payload("X1", "NAME 1"),
        ocr_document={},
        source={},
        missing_fields=[],
        manual_steps_required=[],
        form_url="u",
        target_url="u",
    )
    repo.upsert_from_upload(
        document_id="doc-2",
        payload=_payload("Y2", "NAME 2"),
        ocr_document={},
        source={},
        missing_fields=[],
        manual_steps_required=[],
        form_url="u",
        target_url="u",
    )

    found = repo.find_latest_by_identities(["x-1"], exclude_document_id="")
    excluded = repo.find_latest_by_identities(["x-1"], exclude_document_id="doc-1")

    assert found is not None
    assert found["document_id"] == "doc-1"
    assert excluded is None


def test_crm_repository_update_set_session_and_delete(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.delenv("MONGODB_URI", raising=False)
    repo = CRMRepository(tmp_path)
    repo.upsert_from_upload(
        document_id="doc-1",
        payload=_payload("X1", "NAME 1"),
        ocr_document={},
        source={},
        missing_fields=[],
        manual_steps_required=[],
        form_url="u",
        target_url="u",
    )

    updated = repo.update_document_fields("doc-1", {"status": "merged"})
    repo.set_browser_session("doc-1", "sess-1")
    loaded = repo.get_document("doc-1")
    deleted = repo.delete_document("doc-1")
    after = repo.get_document("doc-1")

    assert updated["status"] == "merged"
    assert loaded is not None
    assert loaded["browser_session_id"] == "sess-1"
    assert deleted is True
    assert after is None


def test_crm_repository_search_ignores_merged_docs_and_handles_bad_json(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.delenv("MONGODB_URI", raising=False)
    repo = CRMRepository(tmp_path)
    fallback_dir = tmp_path / "runtime" / "crm_store"
    fallback_dir.mkdir(parents=True, exist_ok=True)

    merged_path = fallback_dir / "merged.json"
    merged_path.write_text(
        json.dumps(
            {
                "document_id": "merged",
                "identifiers": {"document_number": "M1", "name": "Merged"},
                "updated_at": "2026-01-01T00:00:00Z",
                "merged_into_document_id": "target",
            }
        ),
        encoding="utf-8",
    )
    broken_path = fallback_dir / "broken.json"
    broken_path.write_text("{ not-json", encoding="utf-8")

    summaries = repo.search_documents(limit=10)

    assert summaries == []
