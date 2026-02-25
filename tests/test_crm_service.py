from __future__ import annotations

import asyncio
import logging
from pathlib import Path

import pytest

from app.api.errors import ApiError, ApiErrorCode
from app.crm.service import CRMService


class _Repo:
    def __init__(self, docs: dict[str, dict[str, object]]) -> None:
        self.docs = docs
        self.clients: dict[str, dict[str, object]] = {
            "client-1": {
                "client_id": "client-1",
                "primary_document_id": "doc-1",
                "document_ids": list(docs.keys()),
                "documents_count": len(docs),
                "profile_payload": {
                    "identificacion": {"nombre_apellidos": "User", "nif_nie": "X1"}
                },
                "profile_updated_at": "2026-01-01T00:00:00+00:00",
            }
        }

    def search_documents(
        self,
        query: str,
        limit: int,
        dedupe: bool,
    ) -> list[dict[str, object]]:
        _ = (query, limit, dedupe)
        return [{"document_id": "doc-1", "name": "User", "document_number": "X1"}]

    def get_document(self, document_id: str) -> dict[str, object] | None:
        return self.docs.get(document_id)

    def list_clients(self, query: str, limit: int) -> list[dict[str, object]]:
        _ = (query, limit)
        return [
            {
                "document_id": "doc-1",
                "client_id": "client-1",
                "document_number": "X1",
                "name": "User",
            }
        ]

    def delete_document(self, document_id: str) -> bool:
        return self.docs.pop(document_id, None) is not None

    def list_documents_by_client(
        self, client_id: str, *, limit: int, include_merged: bool
    ) -> list[dict[str, object]]:
        _ = (limit, include_merged)
        if client_id != "client-1":
            return []
        return [
            {
                "document_id": key,
                "client_id": "client-1",
                "document_number": "X1",
                "name": "User",
                "updated_at": "2026-01-01T00:00:00+00:00",
                "status": "review",
            }
            for key in self.docs.keys()
        ]

    def list_full_documents_by_client(self, client_id: str) -> list[dict[str, object]]:
        if client_id != "client-1":
            return []
        return [dict(value) for value in self.docs.values()]

    def get_client(self, client_id: str) -> dict[str, object] | None:
        return self.clients.get(client_id)

    def update_client_profile(
        self,
        client_id: str,
        profile_payload: dict[str, object],
        *,
        profile_source_document_id: str = "",
        profile_merge_meta: dict[str, object] | None = None,
    ) -> dict[str, object]:
        current = dict(self.clients.get(client_id) or {"client_id": client_id})
        current["profile_payload"] = profile_payload
        current["profile_source_document_id"] = profile_source_document_id
        current["profile_merge_meta"] = profile_merge_meta or {}
        current["profile_updated_at"] = "2026-01-02T00:00:00+00:00"
        self.clients[client_id] = current
        return current

    def delete_client(self, client_id: str) -> bool:
        return self.clients.pop(client_id, None) is not None

    def delete_documents_by_client(self, client_id: str) -> list[str]:
        if client_id != "client-1":
            return []
        ids = list(self.docs.keys())
        self.docs = {}
        return ids


def _build_service(repo: _Repo, tmp_path: Path) -> CRMService:
    def _read_record(doc_id: str) -> dict[str, object]:
        _ = doc_id
        raise ApiError(
            status_code=404,
            error_code=ApiErrorCode.DOCUMENT_NOT_FOUND,
            message="missing",
        )

    async def _run_browser_call(fn, *args, **kwargs):
        _ = (fn, args, kwargs)
        return None

    return CRMService(
        repo=repo,
        default_target_url="",
        safe_value=lambda value: str(value or ""),
        artifact_url_from_value=lambda value: "/runtime/uploads/recovered.pdf"
        if str(value)
        else "",
        read_record=_read_record,
        run_browser_call=_run_browser_call,
        close_browser_session=lambda session_id: None,
        record_path=lambda document_id: tmp_path / f"{document_id}.json",
        logger=logging.getLogger("test"),
    )


def test_crm_service_get_document_not_found() -> None:
    service = _build_service(_Repo({}), Path("/tmp"))

    with pytest.raises(ApiError) as exc:
        service.get_document("missing")

    assert exc.value.status_code == 404


def test_crm_service_list_documents() -> None:
    service = _build_service(_Repo({}), Path("/tmp"))

    items = service.list_documents("", 10)

    assert items and items[0]["document_id"] == "doc-1"


def test_crm_service_list_clients() -> None:
    service = _build_service(_Repo({}), Path("/tmp"))

    items = service.list_clients("", 10)

    assert items and items[0]["client_id"] == "client-1"


def test_crm_service_delete_document_success(tmp_path: Path) -> None:
    repo = _Repo({"doc-1": {"document_id": "doc-1", "browser_session_id": ""}})
    service = _build_service(repo, tmp_path)

    result = asyncio.run(service.delete_document("doc-1"))

    assert result["deleted"] is True


def test_crm_service_get_document_preview_fallback_from_stored_path(
    tmp_path: Path,
) -> None:
    repo = _Repo(
        {
            "doc-2": {
                "source": {
                    "stored_path": "/tmp/source.pdf",
                    "preview_url": "",
                },
                "effective_payload": {"identificacion": {}},
            }
        }
    )
    service = _build_service(repo, tmp_path)

    record = service.get_document("doc-2")

    assert record["preview_url"] == "/runtime/uploads/recovered.pdf"


def test_crm_service_get_and_update_client_profile(tmp_path: Path) -> None:
    repo = _Repo(
        {
            "doc-1": {
                "document_id": "doc-1",
                "effective_payload": {
                    "identificacion": {"nombre_apellidos": "User", "nif_nie": "X1"}
                },
                "source": {},
            }
        }
    )
    service = _build_service(repo, tmp_path)

    profile = service.get_client_profile("client-1")
    assert profile["client_id"] == "client-1"
    assert profile["profile_payload"]["identificacion"]["nif_nie"] == "X1"

    updated = service.update_client_profile(
        "client-1",
        {"identificacion": {"nombre_apellidos": "Updated User", "nif_nie": "X1"}},
    )
    assert updated["profile_payload"]["identificacion"]["nombre_apellidos"] == "Updated User"


def test_crm_service_delete_client_cascade(tmp_path: Path) -> None:
    repo = _Repo(
        {
            "doc-1": {
                "document_id": "doc-1",
                "browser_session_id": "",
                "source": {},
                "effective_payload": {"identificacion": {"nif_nie": "X1"}},
            }
        }
    )
    service = _build_service(repo, tmp_path)

    result = asyncio.run(service.delete_client_cascade("client-1"))

    assert result["deleted"] is True
    assert result["client_id"] == "client-1"
    assert result["deleted_document_ids"] == ["doc-1"]
