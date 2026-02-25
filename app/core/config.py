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
class QueueConfig:
    """Durable task queue runtime configuration."""

    sqlite_path: str
    default_ttl_seconds: int
    default_max_retries: int
    default_retry_delay_seconds: int


@dataclass(frozen=True)
class LoggingConfig:
    """Structured logging configuration."""

    level: str


@dataclass(frozen=True)
class SecurityConfig:
    """API perimeter security settings."""

    cors_allowed_origins: list[str]
    request_max_bytes: int
    upload_max_bytes: int
    login_rate_limit_max_attempts: int
    login_rate_limit_window_seconds: int
    login_rate_limit_lock_seconds: int


@dataclass(frozen=True)
class AppConfig:
    """Top-level application configuration."""

    auth: AuthConfig
    queue: QueueConfig
    logging: LoggingConfig
    security: SecurityConfig

    @staticmethod
    def from_env() -> "AppConfig":
        """Build app config from process environment."""
        auth_enabled = os.getenv("AUTH_ENABLED", "0").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        secret_key = (
            os.getenv("AUTH_SECRET_KEY", "").strip() or "dev-insecure-secret-change-me"
        )
        access_ttl = int(os.getenv("AUTH_ACCESS_TOKEN_TTL_SECONDS", "900"))
        refresh_ttl = int(os.getenv("AUTH_REFRESH_TOKEN_TTL_SECONDS", "604800"))
        issuer = os.getenv("AUTH_ISSUER", "ocr-mrz").strip() or "ocr-mrz"
        admin_email = os.getenv("AUTH_ADMIN_EMAIL", "admin@local").strip().lower()
        admin_password = os.getenv("AUTH_ADMIN_PASSWORD", "admin123").strip()
        queue_sqlite_path = (
            os.getenv("TASK_QUEUE_SQLITE_PATH", "runtime/app_state.db").strip()
            or "runtime/app_state.db"
        )
        queue_ttl = int(os.getenv("TASK_QUEUE_TTL_SECONDS", "86400"))
        queue_max_retries = int(os.getenv("TASK_QUEUE_MAX_RETRIES", "3"))
        queue_retry_delay = int(os.getenv("TASK_QUEUE_RETRY_DELAY_SECONDS", "5"))
        log_level = os.getenv("LOG_LEVEL", "INFO").strip() or "INFO"
        cors_allowed_origins = [
            origin.strip()
            for origin in os.getenv(
                "CORS_ALLOWED_ORIGINS",
                "http://localhost:3000,http://127.0.0.1:3000",
            ).split(",")
            if origin.strip()
        ]
        request_max_bytes = int(os.getenv("REQUEST_MAX_BYTES", str(10 * 1024 * 1024)))
        upload_max_bytes = int(os.getenv("UPLOAD_MAX_BYTES", str(20 * 1024 * 1024)))
        login_rate_limit_max_attempts = int(
            os.getenv("LOGIN_RATE_LIMIT_MAX_ATTEMPTS", "5")
        )
        login_rate_limit_window_seconds = int(
            os.getenv("LOGIN_RATE_LIMIT_WINDOW_SECONDS", "300")
        )
        login_rate_limit_lock_seconds = int(
            os.getenv("LOGIN_RATE_LIMIT_LOCK_SECONDS", "600")
        )

        return AppConfig(
            auth=AuthConfig(
                enabled=auth_enabled,
                secret_key=secret_key,
                access_token_ttl_seconds=access_ttl,
                refresh_token_ttl_seconds=refresh_ttl,
                issuer=issuer,
                admin_email=admin_email,
                admin_password=admin_password,
            ),
            queue=QueueConfig(
                sqlite_path=queue_sqlite_path,
                default_ttl_seconds=queue_ttl,
                default_max_retries=queue_max_retries,
                default_retry_delay_seconds=queue_retry_delay,
            ),
            logging=LoggingConfig(level=log_level),
            security=SecurityConfig(
                cors_allowed_origins=cors_allowed_origins,
                request_max_bytes=request_max_bytes,
                upload_max_bytes=upload_max_bytes,
                login_rate_limit_max_attempts=login_rate_limit_max_attempts,
                login_rate_limit_window_seconds=login_rate_limit_window_seconds,
                login_rate_limit_lock_seconds=login_rate_limit_lock_seconds,
            ),
        )
