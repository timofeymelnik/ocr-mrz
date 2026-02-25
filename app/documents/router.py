"""FastAPI router for document endpoints."""

from __future__ import annotations

from typing import Any, Literal

from fastapi import APIRouter
from pydantic import BaseModel, ConfigDict

from app.api.contracts import (
    AddressAutofillResponse,
    ApiErrorResponse,
    ClientMatchResponse,
    DocumentPayloadResponse,
    MergeCandidatesResponse,
)
from app.documents.service import DocumentsService


class ConfirmRequest(BaseModel):
    """Request body for document confirmation endpoint."""

    model_config = ConfigDict(extra="forbid")

    payload: dict[str, Any]


class EnrichByIdentityRequest(BaseModel):
    """Request body for identity enrichment endpoint."""

    model_config = ConfigDict(extra="forbid")

    apply: bool = True
    source_document_id: str | None = None
    selected_fields: list[str] | None = None


class ClientMatchDecisionRequest(BaseModel):
    """Request body for client-match decision endpoint."""

    model_config = ConfigDict(extra="forbid")

    action: Literal["confirm", "reject"]
    source_document_id: str | None = None


class AddressAutofillRequest(BaseModel):
    """Request body for address line parsing endpoint."""

    model_config = ConfigDict(extra="forbid")

    address_line: str


class ReprocessOCRRequest(BaseModel):
    """Request body for OCR reprocess with manual source-kind override."""

    model_config = ConfigDict(extra="forbid")

    source_kind: str
    tasa_code: str | None = None


class DocumentsRouter:
    """Router factory wrapper for document read/merge endpoints."""

    def __init__(self, service: DocumentsService) -> None:
        """Store service dependency used by handlers."""
        self._service = service

    def build(self) -> APIRouter:
        """Create configured API router."""
        router = APIRouter(tags=["documents"])

        @router.get(
            "/api/documents/{document_id}",
            response_model=DocumentPayloadResponse,
            responses={404: {"model": ApiErrorResponse}},
        )
        def get_document(document_id: str) -> DocumentPayloadResponse:
            """Get document data for UI editing flow."""
            record = self._service.get_document(document_id=document_id)
            return DocumentPayloadResponse(**record)

        @router.get(
            "/api/documents/{document_id}/merge-candidates",
            response_model=MergeCandidatesResponse,
            responses={404: {"model": ApiErrorResponse}},
        )
        def get_merge_candidates(document_id: str) -> MergeCandidatesResponse:
            """Rebuild merge candidates and return them."""
            payload = self._service.get_merge_candidates(document_id=document_id)
            return MergeCandidatesResponse(**payload)

        @router.get(
            "/api/documents/{document_id}/client-match",
            response_model=ClientMatchResponse,
            responses={404: {"model": ApiErrorResponse}},
        )
        def get_client_match(document_id: str) -> ClientMatchResponse:
            """Return best identity-based client match for operator decision."""
            payload = self._service.get_client_match(document_id=document_id)
            return ClientMatchResponse(**payload)

        @router.post(
            "/api/documents/{document_id}/client-match",
            response_model=DocumentPayloadResponse,
            responses={
                404: {"model": ApiErrorResponse},
                422: {"model": ApiErrorResponse},
            },
        )
        def resolve_client_match(
            document_id: str, req: ClientMatchDecisionRequest
        ) -> DocumentPayloadResponse:
            """Apply or reject selected client-match candidate."""
            payload = self._service.resolve_client_match(
                document_id=document_id,
                action=req.action,
                source_document_id=(req.source_document_id or "").strip(),
            )
            return DocumentPayloadResponse(**payload)

        @router.post(
            "/api/documents/{document_id}/address-autofill",
            response_model=AddressAutofillResponse,
            responses={
                404: {"model": ApiErrorResponse},
                422: {"model": ApiErrorResponse},
            },
        )
        def address_autofill(
            document_id: str,
            req: AddressAutofillRequest,
        ) -> AddressAutofillResponse:
            """Parse free-form address and return structured domicilio fields."""
            payload = self._service.autofill_address_from_line(
                document_id=document_id,
                address_line=req.address_line,
            )
            return AddressAutofillResponse(**payload)

        @router.post(
            "/api/documents/{document_id}/reprocess-ocr",
            response_model=DocumentPayloadResponse,
            responses={
                404: {"model": ApiErrorResponse},
                422: {"model": ApiErrorResponse},
                500: {"model": ApiErrorResponse},
            },
        )
        def reprocess_document_ocr(
            document_id: str,
            req: ReprocessOCRRequest,
        ) -> DocumentPayloadResponse:
            """Re-run OCR/build pipeline for existing stored source with manual type."""
            payload = self._service.reprocess_document_ocr(
                document_id=document_id,
                source_kind=(req.source_kind or "").strip(),
                tasa_code=(req.tasa_code or "").strip(),
            )
            return DocumentPayloadResponse(**payload)

        @router.post(
            "/api/documents/{document_id}/confirm",
            response_model=DocumentPayloadResponse,
            responses={
                404: {"model": ApiErrorResponse},
                422: {"model": ApiErrorResponse},
            },
        )
        def confirm_document(
            document_id: str, req: ConfirmRequest
        ) -> DocumentPayloadResponse:
            """Persist user-confirmed payload."""
            payload = self._service.confirm_document(
                document_id=document_id,
                payload=req.payload,
            )
            return DocumentPayloadResponse(**payload)

        @router.post(
            "/api/documents/{document_id}/enrich-by-identity",
            response_model=DocumentPayloadResponse,
            responses={
                404: {"model": ApiErrorResponse},
                422: {"model": ApiErrorResponse},
            },
        )
        def enrich_by_identity(
            document_id: str,
            req: EnrichByIdentityRequest,
        ) -> DocumentPayloadResponse:
            """Preview or apply enrichment from identity-linked document."""
            payload = self._service.enrich_by_identity(
                document_id=document_id,
                apply=bool(req.apply),
                source_document_id=(req.source_document_id or "").strip(),
                selected_fields=req.selected_fields or [],
            )
            return DocumentPayloadResponse(**payload)

        return router


def create_documents_router(service: DocumentsService) -> APIRouter:
    """Create router for document endpoints."""
    return DocumentsRouter(service=service).build()
