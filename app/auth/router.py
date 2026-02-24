"""Authentication API router."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Header, HTTPException
from fastapi.responses import JSONResponse

from app.auth.models import LoginRequest, LogoutRequest, RefreshRequest
from app.auth.service import AuthService


def _extract_bearer_token(authorization: str | None) -> str:
    """Extract bearer token from Authorization header."""
    if not authorization:
        return ""
    parts = authorization.strip().split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return ""
    return parts[1].strip()


def create_auth_router(service: AuthService) -> APIRouter:
    """Build authentication router with login/refresh/logout/me endpoints."""
    router = APIRouter(tags=["auth"])

    @router.post("/api/auth/login")
    def login(req: LoginRequest) -> JSONResponse:
        """Authenticate user and return token pair."""
        session = service.login(req.email, req.password)
        return JSONResponse(session.model_dump())

    @router.post("/api/auth/refresh")
    def refresh(req: RefreshRequest) -> JSONResponse:
        """Rotate refresh token and issue new session tokens."""
        session = service.refresh(req.refresh_token)
        return JSONResponse(session.model_dump())

    @router.post("/api/auth/logout")
    def logout(req: LogoutRequest) -> JSONResponse:
        """Invalidate supplied refresh token."""
        service.logout(req.refresh_token)
        return JSONResponse({"status": "ok"})

    @router.get("/api/auth/me")
    def me(authorization: str | None = Header(default=None)) -> JSONResponse:
        """Return current authenticated user claims from access token."""
        token = _extract_bearer_token(authorization)
        if not token:
            raise HTTPException(status_code=401, detail="Missing bearer token")
        user = service.verify_access_token(token)
        return JSONResponse({"user": user})

    # Phase-2 placeholders for self-signup + email verification.
    @router.post("/api/auth/register")
    def register_placeholder() -> JSONResponse:
        """Registration placeholder for phase 2 rollout."""
        raise HTTPException(status_code=404, detail="Registration is not enabled yet.")

    @router.post("/api/auth/verify-email")
    def verify_email_placeholder() -> JSONResponse:
        """Email verification placeholder for phase 2 rollout."""
        raise HTTPException(status_code=404, detail="Email verification is not enabled yet.")

    return router
