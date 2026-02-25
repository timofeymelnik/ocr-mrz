"""Login brute-force protection backed by SQLite runtime state."""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from threading import Lock

from app.api.errors import ApiError, ApiErrorCode
from app.core.migrations import apply_migrations


class LoginRateLimiter:
    """Rate limiter for login attempts by normalized (email, ip) tuple."""

    def __init__(
        self,
        *,
        database_path: Path,
        max_attempts: int,
        window_seconds: int,
        lock_seconds: int,
    ) -> None:
        """Initialize limiter storage and policy parameters."""
        apply_migrations(database_path)
        self._connection = sqlite3.connect(str(database_path), check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        self._lock = Lock()
        self._max_attempts = max(1, int(max_attempts))
        self._window_seconds = max(1, int(window_seconds))
        self._lock_seconds = max(1, int(lock_seconds))

    def assert_allowed(self, *, email: str, client_ip: str) -> None:
        """Raise 429 when login attempts are currently locked for the principal."""
        now = int(time.time())
        key_email = email.strip().lower()
        key_ip = client_ip.strip() or "unknown"
        with self._lock:
            row = self._connection.execute(
                """
                SELECT failed_attempts, first_failed_at, locked_until
                FROM auth_login_attempts
                WHERE email = ? AND client_ip = ?
                """,
                (key_email, key_ip),
            ).fetchone()
            if row is None:
                return

            locked_until = int(row["locked_until"] or 0)
            if locked_until > now:
                retry_after = locked_until - now
                raise ApiError(
                    status_code=429,
                    error_code=ApiErrorCode.AUTH_RATE_LIMITED,
                    message=(
                        "Too many login attempts. "
                        f"Retry after {retry_after} seconds."
                    ),
                )

            first_failed_at = int(row["first_failed_at"] or 0)
            if first_failed_at and (now - first_failed_at) > self._window_seconds:
                self._connection.execute(
                    "DELETE FROM auth_login_attempts WHERE email = ? AND client_ip = ?",
                    (key_email, key_ip),
                )
                self._connection.commit()

    def record_success(self, *, email: str, client_ip: str) -> None:
        """Reset limiter state after successful login."""
        key_email = email.strip().lower()
        key_ip = client_ip.strip() or "unknown"
        with self._lock:
            self._connection.execute(
                "DELETE FROM auth_login_attempts WHERE email = ? AND client_ip = ?",
                (key_email, key_ip),
            )
            self._connection.commit()

    def record_failure(self, *, email: str, client_ip: str) -> None:
        """Record failed login and apply lock when threshold is exceeded."""
        now = int(time.time())
        key_email = email.strip().lower()
        key_ip = client_ip.strip() or "unknown"
        with self._lock:
            row = self._connection.execute(
                """
                SELECT failed_attempts, first_failed_at
                FROM auth_login_attempts
                WHERE email = ? AND client_ip = ?
                """,
                (key_email, key_ip),
            ).fetchone()

            if row is None:
                failed_attempts = 1
                first_failed_at = now
            else:
                previous_first = int(row["first_failed_at"] or 0)
                if previous_first and (now - previous_first) > self._window_seconds:
                    failed_attempts = 1
                    first_failed_at = now
                else:
                    failed_attempts = int(row["failed_attempts"] or 0) + 1
                    first_failed_at = previous_first or now

            locked_until = (
                now + self._lock_seconds if failed_attempts >= self._max_attempts else 0
            )

            self._connection.execute(
                """
                INSERT INTO auth_login_attempts(
                  email, client_ip, failed_attempts, first_failed_at, last_failed_at, locked_until
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(email, client_ip) DO UPDATE SET
                  failed_attempts = excluded.failed_attempts,
                  first_failed_at = excluded.first_failed_at,
                  last_failed_at = excluded.last_failed_at,
                  locked_until = excluded.locked_until
                """,
                (
                    key_email,
                    key_ip,
                    failed_attempts,
                    first_failed_at,
                    now,
                    locked_until,
                ),
            )
            self._connection.commit()

    def close(self) -> None:
        """Close SQLite resources."""
        with self._lock:
            self._connection.close()
