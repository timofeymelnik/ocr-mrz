"""FastAPI router for CRM endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Query

from app.api.contracts import (
    ApiErrorResponse,
    CRMDocumentsListResponse,
    DeleteDocumentResponse,
    DocumentPayloadResponse,
)
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
            include_duplicates: bool = Query(
                default=False,
                alias="include_duplicates",
            ),
        ) -> CRMDocumentsListResponse:
            """List CRM documents available to the operator UI."""
            items = self._service.list_documents(
                query=query,
                limit=limit,
                include_duplicates=include_duplicates,
            )
            return CRMDocumentsListResponse(items=items)

        @router.get("/api/crm/clients/{client_id}/documents")
        def list_client_documents(
            client_id: str,
            limit: int = Query(default=200, ge=1, le=500, alias="limit"),
            include_merged: bool = Query(
                default=True,
                alias="include_merged",
            ),
        ) -> CRMDocumentsListResponse:
            """List all documents bound to a single client entity."""
            items = self._service.list_client_documents(
                client_id=client_id,
                limit=limit,
                include_merged=include_merged,
            )
            return CRMDocumentsListResponse(items=items)

        @router.get(
            "/api/crm/documents/{document_id}",
            response_model=DocumentPayloadResponse,
            responses={404: {"model": ApiErrorResponse}},
        )
        def get_crm_document(document_id: str) -> DocumentPayloadResponse:
            """Get CRM document details by identifier."""
            record = self._service.get_document(document_id=document_id)
            return DocumentPayloadResponse(**record)

        @router.delete(
            "/api/crm/documents/{document_id}",
            response_model=DeleteDocumentResponse,
            responses={
                404: {"model": ApiErrorResponse},
                500: {"model": ApiErrorResponse},
            },
        )
        async def delete_crm_document(document_id: str) -> DeleteDocumentResponse:
            """Delete CRM document and cleanup linked runtime artifacts."""
            payload = await self._service.delete_document(document_id=document_id)
            return DeleteDocumentResponse(**payload)

        return router


def create_crm_router(service: CRMService) -> APIRouter:
    """Create CRM router using provided application service."""
    return CRMRouter(service=service).build()
