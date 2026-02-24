"""FastAPI router for CRM endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from app.crm.service import CRMService


class CRMRouter:
    """Factory wrapper that builds CRM API router from a service."""

    def __init__(self, service: CRMService) -> None:
        """Store service dependency used by route handlers."""
        self._service = service

    def build(self) -> APIRouter:
        """Create and return configured CRM router."""
        router = APIRouter(tags=["crm"])

        @router.get("/api/crm/documents")
        def list_crm_documents(
            query: str = Query(default="", alias="query"),
            limit: int = Query(default=30, ge=1, le=200, alias="limit"),
        ) -> JSONResponse:
            """List CRM documents available to the operator UI."""
            items = self._service.list_documents(query=query, limit=limit)
            return JSONResponse({"items": items})

        @router.get("/api/crm/documents/{document_id}")
        def get_crm_document(document_id: str) -> JSONResponse:
            """Get CRM document details by identifier."""
            record = self._service.get_document(document_id=document_id)
            return JSONResponse(record)

        @router.delete("/api/crm/documents/{document_id}")
        async def delete_crm_document(document_id: str) -> JSONResponse:
            """Delete CRM document and cleanup linked runtime artifacts."""
            payload = await self._service.delete_document(document_id=document_id)
            return JSONResponse(payload)

        return router


def create_crm_router(service: CRMService) -> APIRouter:
    """Create CRM router using provided application service."""
    return CRMRouter(service=service).build()
