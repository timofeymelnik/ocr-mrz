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

    def delete_document(self, document_id: str) -> bool:
        return self.docs.pop(document_id, None) is not None


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


def test_crm_service_delete_document_success(tmp_path: Path) -> None:
    repo = _Repo({"doc-1": {"document_id": "doc-1", "browser_session_id": ""}})
    service = _build_service(repo, tmp_path)

    result = asyncio.run(service.delete_document("doc-1"))

    assert result["deleted"] is True
