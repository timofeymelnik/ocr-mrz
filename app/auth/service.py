"""Authentication service for login, refresh and auth verification."""

from __future__ import annotations

import hashlib
import time
import uuid
from typing import Any

from fastapi import HTTPException

from app.auth.models import AuthSession, AuthUser, RefreshTokenRecord
from app.auth.repository import AuthRepository
from app.core.config import AuthConfig
from app.core.security import (
    build_signed_token,
    decode_signed_token,
    hash_password,
    verify_password,
)


class AuthService:
    """Authentication domain service with phase-2 extension points."""

    def __init__(self, repo: AuthRepository, config: AuthConfig) -> None:
        """Initialize service dependencies."""
        self._repo = repo
        self._config = config

    @property
    def enabled(self) -> bool:
        """Return whether auth checks should be enforced."""
        return self._config.enabled

    def bootstrap_admin_user(self) -> None:
        """Ensure bootstrap admin user exists from environment values."""
        existing = self._repo.get_user_by_email(self._config.admin_email)
        if existing is not None:
            return

        self._repo.upsert_user(
            AuthUser(
                user_id=uuid.uuid4().hex,
                email=self._config.admin_email,
                password_hash=hash_password(self._config.admin_password),
                role="admin",
                is_active=True,
                email_verified=True,
                # Phase-2 hook: can store pending verification token here.
                email_verification_token="",
            )
        )

    def login(self, email: str, password: str) -> AuthSession:
        """Authenticate credentials and issue access/refresh token pair."""
        user = self._repo.get_user_by_email(email.strip().lower())
        if user is None or not user.is_active:
            raise HTTPException(status_code=401, detail="Invalid credentials")
        if not verify_password(password, user.password_hash):
            raise HTTPException(status_code=401, detail="Invalid credentials")
        return self._issue_session_for_user(user)

    def _issue_session_for_user(self, user: AuthUser) -> AuthSession:
        """Issue fresh access and refresh tokens for given user."""

        # Phase-2 hook: require verified email when self-signup is enabled.
        now_ts = int(time.time())
        access_jti = uuid.uuid4().hex
        refresh_jti = uuid.uuid4().hex

        access_payload = {
            "iss": self._config.issuer,
            "sub": user.user_id,
            "email": user.email,
            "role": user.role,
            "email_verified": bool(user.email_verified),
            "type": "access",
            "iat": now_ts,
            "exp": now_ts + self._config.access_token_ttl_seconds,
            "jti": access_jti,
        }
        refresh_payload = {
            "iss": self._config.issuer,
            "sub": user.user_id,
            "email": user.email,
            "role": user.role,
            "type": "refresh",
            "iat": now_ts,
            "exp": now_ts + self._config.refresh_token_ttl_seconds,
            "jti": refresh_jti,
        }

        access_token = build_signed_token(access_payload, self._config.secret_key)
        refresh_token = build_signed_token(refresh_payload, self._config.secret_key)

        self._repo.save_refresh_token(
            RefreshTokenRecord(
                jti=refresh_jti,
                user_id=user.user_id,
                token_hash=self._hash_token(refresh_token),
                expires_at=refresh_payload["exp"],
                revoked=False,
            )
        )

        return AuthSession(
            access_token=access_token,
            refresh_token=refresh_token,
            token_type="bearer",
            expires_in=self._config.access_token_ttl_seconds,
            user={
                "user_id": user.user_id,
                "email": user.email,
                "role": user.role,
                "email_verified": bool(user.email_verified),
            },
        )

    def refresh(self, refresh_token: str) -> AuthSession:
        """Validate refresh token and rotate token pair."""
        payload = self._decode_token(refresh_token, expected_type="refresh")
        jti = str(payload.get("jti") or "")
        record = self._repo.get_refresh_token(jti)
        if record is None or record.revoked:
            raise HTTPException(status_code=401, detail="Invalid refresh token")
        if record.expires_at < int(time.time()):
            self._repo.revoke_refresh_token(jti)
            raise HTTPException(status_code=401, detail="Refresh token expired")
        if record.token_hash != self._hash_token(refresh_token):
            self._repo.revoke_refresh_token(jti)
            raise HTTPException(status_code=401, detail="Refresh token mismatch")

        self._repo.revoke_refresh_token(jti)
        email = str(payload.get("email") or "").strip().lower()
        user = self._repo.get_user_by_email(email)
        if user is None or not user.is_active:
            raise HTTPException(status_code=401, detail="User not found")
        return self._issue_session_for_user(user)

    def logout(self, refresh_token: str | None) -> None:
        """Revoke provided refresh token when available."""
        if not refresh_token:
            return
        try:
            payload = self._decode_token(refresh_token, expected_type="refresh")
        except HTTPException:
            return
        jti = str(payload.get("jti") or "")
        if jti:
            self._repo.revoke_refresh_token(jti)

    def verify_access_token(self, token: str) -> dict[str, Any]:
        """Validate access token and return normalized user claims."""
        payload = self._decode_token(token, expected_type="access")
        return {
            "user_id": str(payload.get("sub") or ""),
            "email": str(payload.get("email") or ""),
            "role": str(payload.get("role") or "operator"),
            "email_verified": bool(payload.get("email_verified")),
        }

    def _decode_token(self, token: str, *, expected_type: str) -> dict[str, Any]:
        """Decode signed token and validate issuer/type claims."""
        try:
            payload = decode_signed_token(token, self._config.secret_key)
        except ValueError as exc:
            raise HTTPException(status_code=401, detail=str(exc)) from exc

        if str(payload.get("iss") or "") != self._config.issuer:
            raise HTTPException(status_code=401, detail="Invalid token issuer")
        if str(payload.get("type") or "") != expected_type:
            raise HTTPException(status_code=401, detail="Invalid token type")
        return payload

    @staticmethod
    def _hash_token(token: str) -> str:
        """Hash raw token for storage/comparison."""
        return hashlib.sha256(token.encode("utf-8")).hexdigest()
