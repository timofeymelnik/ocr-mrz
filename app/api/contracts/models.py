"""Pydantic API response models used in OpenAPI contracts."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class ApiErrorResponse(BaseModel):
    """Stable error envelope for API responses."""

    error_code: str = Field(description="Machine-readable error code")
    message: str = Field(description="Human-readable error message")


class HealthResponse(BaseModel):
    """Health check response payload."""

    status: Literal["ok"]


class TaskAcceptedResponse(BaseModel):
    """Response payload for accepted async task submission."""

    task_id: str
    status: Literal["queued"]
    status_url: str


class TaskStatusResponse(BaseModel):
    """Response payload for async task status endpoint."""

    task_id: str
    task_type: str
    status: str
    attempts: int
    max_retries: int
    created_at: int
    updated_at: int
    expires_at: int
    result: dict[str, Any] | None = None
    error: str = ""
    dead_letter_reason: str = ""


class AuthUserClaimsResponse(BaseModel):
    """Authenticated user claims payload."""

    user_id: str
    email: str
    role: str
    email_verified: bool


class AuthSessionResponse(BaseModel):
    """Authentication session response payload."""

    access_token: str
    refresh_token: str
    token_type: str
    expires_in: int
    user: dict[str, str | bool]


class AuthMeResponse(BaseModel):
    """Current user endpoint response payload."""

    user: dict[str, str | bool]


class LogoutResponse(BaseModel):
    """Logout response payload."""

    status: Literal["ok"]


class CRMSummaryItemResponse(BaseModel):
    """CRM listing item payload."""

    document_id: str
    client_id: str = ""
    document_number: str = ""
    name: str = ""
    updated_at: str = ""
    status: str = "unknown"
    has_edited: bool = False


class CRMDocumentsListResponse(BaseModel):
    """CRM listing endpoint response payload."""

    items: list[dict[str, Any]]


class DeleteDocumentResponse(BaseModel):
    """Delete document response payload."""

    document_id: str
    deleted: bool


class DocumentPayloadResponse(BaseModel):
    """Generic document payload used by documents endpoints."""

    document_id: str
    client_id: str = ""
    preview_url: str = ""
    source: dict[str, Any] = Field(default_factory=dict)
    document: dict[str, Any] = Field(default_factory=dict)
    payload: dict[str, Any]
    missing_fields: list[str] = Field(default_factory=list)
    validation_issues: list[dict[str, Any]] = Field(default_factory=list)
    manual_steps_required: list[str] = Field(default_factory=list)
    form_url: str = ""
    target_url: str = ""
    browser_session_id: str = ""
    enrichment_preview: list[dict[str, Any]] = Field(default_factory=list)
    enrichment_skipped: list[dict[str, Any]] = Field(default_factory=list)
    merge_candidates: list[dict[str, Any]] = Field(default_factory=list)
    family_links: list[dict[str, Any]] = Field(default_factory=list)
    family_reference: dict[str, Any] = Field(default_factory=dict)
    identity_match_found: bool = False
    identity_source_document_id: str = ""
    source_kind_input: str = ""
    source_kind_detected: str = ""
    source_kind_confidence: float = 0.0
    source_kind_auto: bool = False
    source_kind_requires_review: bool = False
    workflow_stage: str = "review"
    workflow_next_step: str = "prepare"
    client_match: dict[str, Any] = Field(default_factory=dict)
    client_match_decision: str = "none"


class AddressAutofillDomicilioResponse(BaseModel):
    """Structured address fields resolved from free-form line."""

    tipo_via: str = ""
    nombre_via: str = ""
    numero: str = ""
    escalera: str = ""
    piso: str = ""
    puerta: str = ""
    municipio: str = ""
    provincia: str = ""
    cp: str = ""


class AddressAutofillResponse(BaseModel):
    """Address parsing and geocoding response payload."""

    document_id: str
    address_line: str
    normalized_address: str = ""
    geocode_used: bool = False
    domicilio: AddressAutofillDomicilioResponse


class ClientMatchResponse(BaseModel):
    """Client match resolution payload."""

    document_id: str
    identity_match_found: bool = False
    identity_source_document_id: str = ""
    client_match: dict[str, Any] = Field(default_factory=dict)
    client_match_decision: str = "none"
    merge_candidates: list[dict[str, Any]] = Field(default_factory=list)
    workflow_stage: str = "review"
    workflow_next_step: str = "prepare"


class MergeCandidatesResponse(BaseModel):
    """Merge-candidates endpoint response payload."""

    document_id: str
    merge_candidates: list[dict[str, Any]]
