"""Authentication API router."""

from __future__ import annotations

from fastapi import APIRouter, Header, Request

from app.api.contracts import (
    ApiErrorResponse,
    AuthMeResponse,
    AuthSessionResponse,
    LogoutResponse,
)
from app.api.errors import ApiError, ApiErrorCode
from app.auth.models import LoginRequest, LogoutRequest, RefreshRequest
from app.auth.rate_limiter import LoginRateLimiter
from app.auth.service import AuthService


def _extract_bearer_token(authorization: str | None) -> str:
    """Extract bearer token from Authorization header."""
    if not authorization:
        return ""
    parts = authorization.strip().split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return ""
    return parts[1].strip()


def create_auth_router(
    service: AuthService, rate_limiter: LoginRateLimiter
) -> APIRouter:
    """Build authentication router with login/refresh/logout/me endpoints."""
    router = APIRouter(tags=["auth"])

    @router.post(
        "/api/auth/login",
        response_model=AuthSessionResponse,
        responses={401: {"model": ApiErrorResponse}, 429: {"model": ApiErrorResponse}},
    )
    def login(req: LoginRequest, request: Request) -> AuthSessionResponse:
        """Authenticate user and return token pair."""
        client_ip = (request.client.host if request.client else "") or "unknown"
        normalized_email = req.email.strip().lower()
        rate_limiter.assert_allowed(email=normalized_email, client_ip=client_ip)
        try:
            session = service.login(req.email, req.password)
        except ApiError:
            rate_limiter.record_failure(email=normalized_email, client_ip=client_ip)
            raise
        rate_limiter.record_success(email=normalized_email, client_ip=client_ip)
        return AuthSessionResponse(**session.model_dump())

    @router.post(
        "/api/auth/refresh",
        response_model=AuthSessionResponse,
        responses={401: {"model": ApiErrorResponse}},
    )
    def refresh(req: RefreshRequest) -> AuthSessionResponse:
        """Rotate refresh token and issue new session tokens."""
        session = service.refresh(req.refresh_token)
        return AuthSessionResponse(**session.model_dump())

    @router.post("/api/auth/logout", response_model=LogoutResponse)
    def logout(req: LogoutRequest) -> LogoutResponse:
        """Invalidate supplied refresh token."""
        service.logout(req.refresh_token)
        return LogoutResponse(status="ok")

    @router.get(
        "/api/auth/me",
        response_model=AuthMeResponse,
        responses={401: {"model": ApiErrorResponse}},
    )
    def me(authorization: str | None = Header(default=None)) -> AuthMeResponse:
        """Return current authenticated user claims from access token."""
        token = _extract_bearer_token(authorization)
        if not token:
            raise ApiError(
                status_code=401,
                error_code=ApiErrorCode.AUTH_MISSING_TOKEN,
                message="Missing bearer token",
            )
        user = service.verify_access_token(token)
        return AuthMeResponse(user=user)

    # Phase-2 placeholders for self-signup + email verification.
    @router.post("/api/auth/register", responses={404: {"model": ApiErrorResponse}})
    def register_placeholder() -> None:
        """Registration placeholder for phase 2 rollout."""
        raise ApiError(
            status_code=404,
            error_code=ApiErrorCode.AUTH_TOKEN_INVALID,
            message="Registration is not enabled yet.",
        )

    @router.post(
        "/api/auth/verify-email",
        responses={404: {"model": ApiErrorResponse}},
    )
    def verify_email_placeholder() -> None:
        """Email verification placeholder for phase 2 rollout."""
        raise ApiError(
            status_code=404,
            error_code=ApiErrorCode.AUTH_TOKEN_INVALID,
            message="Email verification is not enabled yet.",
        )

    return router
