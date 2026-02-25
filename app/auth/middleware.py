"""HTTP middleware that enforces auth on protected API routes."""

from __future__ import annotations

from typing import Callable

from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse

from app.api.contracts import ApiErrorResponse
from app.api.errors import ApiErrorCode, to_error_payload
from app.auth.service import AuthService


def _extract_bearer_token(authorization: str) -> str:
    """Extract bearer token from authorization header value."""
    parts = (authorization or "").strip().split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return ""
    return parts[1].strip()


def create_auth_middleware(service: AuthService) -> Callable:
    """Create middleware function that validates access tokens when enabled."""

    async def auth_middleware(request: Request, call_next: Callable):
        """Validate auth for protected API paths and attach user to request state."""
        if not service.enabled:
            return await call_next(request)

        path = request.url.path
        if not path.startswith("/api/"):
            return await call_next(request)

        if path in {
            "/api/health",
            "/api/auth/login",
            "/api/auth/refresh",
            "/api/auth/logout",
            "/api/auth/register",
            "/api/auth/verify-email",
        }:
            return await call_next(request)

        token = _extract_bearer_token(request.headers.get("authorization", ""))
        if not token:
            return JSONResponse(
                status_code=401,
                content=ApiErrorResponse(
                    error_code=ApiErrorCode.AUTH_MISSING_TOKEN,
                    message="Missing bearer token",
                ).model_dump(),
            )

        try:
            user = service.verify_access_token(token)
        except HTTPException as exc:
            return JSONResponse(
                status_code=exc.status_code,
                content=to_error_payload(exc.detail, exc.status_code),
            )

        request.state.user = user
        return await call_next(request)

    return auth_middleware
