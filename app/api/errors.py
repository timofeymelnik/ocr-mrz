"""Shared API error types and helpers."""

from __future__ import annotations

from enum import StrEnum
from typing import Any

from fastapi import HTTPException


class ApiErrorCode(StrEnum):
    """Machine-readable API error codes."""

    VALIDATION_ERROR = "VALIDATION_ERROR"
    AUTH_MISSING_TOKEN = "AUTH_MISSING_TOKEN"
    AUTH_INVALID_CREDENTIALS = "AUTH_INVALID_CREDENTIALS"
    AUTH_TOKEN_INVALID = "AUTH_TOKEN_INVALID"
    AUTH_RATE_LIMITED = "AUTH_RATE_LIMITED"
    DOCUMENT_NOT_FOUND = "DOCUMENT_NOT_FOUND"
    DOCUMENT_INVALID_PAYLOAD = "DOCUMENT_INVALID_PAYLOAD"
    CRM_DOCUMENT_NOT_FOUND = "CRM_DOCUMENT_NOT_FOUND"
    CRM_DELETE_FAILED = "CRM_DELETE_FAILED"
    QUEUE_TASK_NOT_FOUND = "QUEUE_TASK_NOT_FOUND"
    REQUEST_TOO_LARGE = "REQUEST_TOO_LARGE"
    INTERNAL_SERVER_ERROR = "INTERNAL_SERVER_ERROR"


class ApiError(HTTPException):
    """HTTP exception carrying stable API error envelope."""

    def __init__(
        self, *, status_code: int, error_code: ApiErrorCode, message: str
    ) -> None:
        """Build an HTTP exception with standard detail structure."""
        super().__init__(
            status_code=status_code,
            detail={"error_code": str(error_code), "message": message},
        )


def to_error_payload(detail: Any, status_code: int) -> dict[str, str]:
    """Normalize HTTP exception detail into stable error payload."""
    if isinstance(detail, dict):
        error_code = str(detail.get("error_code") or f"HTTP_{status_code}")
        message = str(detail.get("message") or detail.get("detail") or "HTTP error")
        return {"error_code": error_code, "message": message}
    return {
        "error_code": f"HTTP_{status_code}",
        "message": str(detail or "HTTP error"),
    }
