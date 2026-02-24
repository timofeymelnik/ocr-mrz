"""Pydantic models for authentication domain."""

from __future__ import annotations

from pydantic import BaseModel, Field


class AuthUser(BaseModel):
    """Persisted auth user model."""

    user_id: str
    email: str
    password_hash: str
    role: str = "admin"
    is_active: bool = True
    email_verified: bool = True
    email_verification_token: str = ""


class LoginRequest(BaseModel):
    """Login request payload."""

    email: str = Field(min_length=3)
    password: str = Field(min_length=1)


class RefreshRequest(BaseModel):
    """Refresh request payload."""

    refresh_token: str = Field(min_length=1)


class LogoutRequest(BaseModel):
    """Logout request payload."""

    refresh_token: str | None = None


class AuthSession(BaseModel):
    """Auth session response payload with tokens."""

    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int
    user: dict[str, str | bool]


class RefreshTokenRecord(BaseModel):
    """Refresh token persistence record."""

    jti: str
    user_id: str
    token_hash: str
    expires_at: int
    revoked: bool = False
