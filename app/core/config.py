"""Application configuration loaded from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class AuthConfig:
    """Authentication-related configuration."""

    enabled: bool
    secret_key: str
    access_token_ttl_seconds: int
    refresh_token_ttl_seconds: int
    issuer: str
    admin_email: str
    admin_password: str


@dataclass(frozen=True)
class AppConfig:
    """Top-level application configuration."""

    auth: AuthConfig

    @staticmethod
    def from_env() -> "AppConfig":
        """Build app config from process environment."""
        auth_enabled = os.getenv("AUTH_ENABLED", "0").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        secret_key = os.getenv("AUTH_SECRET_KEY", "").strip() or "dev-insecure-secret-change-me"
        access_ttl = int(os.getenv("AUTH_ACCESS_TOKEN_TTL_SECONDS", "900"))
        refresh_ttl = int(os.getenv("AUTH_REFRESH_TOKEN_TTL_SECONDS", "604800"))
        issuer = os.getenv("AUTH_ISSUER", "ocr-mrz").strip() or "ocr-mrz"
        admin_email = os.getenv("AUTH_ADMIN_EMAIL", "admin@local").strip().lower()
        admin_password = os.getenv("AUTH_ADMIN_PASSWORD", "admin123").strip()

        return AppConfig(
            auth=AuthConfig(
                enabled=auth_enabled,
                secret_key=secret_key,
                access_token_ttl_seconds=access_ttl,
                refresh_token_ttl_seconds=refresh_ttl,
                issuer=issuer,
                admin_email=admin_email,
                admin_password=admin_password,
            )
        )
