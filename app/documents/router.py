"""FastAPI router for document endpoints."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from app.documents.service import DocumentsService


class ConfirmRequest(BaseModel):
    """Request body for document confirmation endpoint."""

    payload: dict[str, Any]


class EnrichByIdentityRequest(BaseModel):
    """Request body for identity enrichment endpoint."""

    apply: bool = True
    source_document_id: str | None = None


class DocumentsRouter:
    """Router factory wrapper for document read/merge endpoints."""

    def __init__(self, service: DocumentsService) -> None:
        """Store service dependency used by handlers."""
        self._service = service

    def build(self) -> APIRouter:
        """Create configured API router."""
        router = APIRouter(tags=["documents"])

        @router.get("/api/documents/{document_id}")
        def get_document(document_id: str) -> JSONResponse:
            """Get document data for UI editing flow."""
            record = self._service.get_document(document_id=document_id)
            return JSONResponse(record)

        @router.get("/api/documents/{document_id}/merge-candidates")
        def get_merge_candidates(document_id: str) -> JSONResponse:
            """Rebuild merge candidates and return them."""
            payload = self._service.get_merge_candidates(document_id=document_id)
            return JSONResponse(payload)

        @router.post("/api/documents/{document_id}/confirm")
        def confirm_document(document_id: str, req: ConfirmRequest) -> JSONResponse:
            """Persist user-confirmed payload."""
            payload = self._service.confirm_document(
                document_id=document_id,
                payload=req.payload,
            )
            return JSONResponse(payload)

        @router.post("/api/documents/{document_id}/enrich-by-identity")
        def enrich_by_identity(
            document_id: str,
            req: EnrichByIdentityRequest,
        ) -> JSONResponse:
            """Preview or apply enrichment from identity-linked document."""
            payload = self._service.enrich_by_identity(
                document_id=document_id,
                apply=bool(req.apply),
                source_document_id=(req.source_document_id or "").strip(),
            )
            return JSONResponse(payload)

        return router


def create_documents_router(service: DocumentsService) -> APIRouter:
    """Create router for document endpoints."""
    return DocumentsRouter(service=service).build()
