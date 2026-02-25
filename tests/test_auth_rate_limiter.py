from __future__ import annotations

from pathlib import Path

import pytest

from app.api.errors import ApiError
from app.auth.rate_limiter import LoginRateLimiter


def test_login_rate_limiter_blocks_after_threshold(tmp_path: Path) -> None:
    limiter = LoginRateLimiter(
        database_path=tmp_path / "state.db",
        max_attempts=2,
        window_seconds=300,
        lock_seconds=120,
    )

    limiter.assert_allowed(email="test@example.com", client_ip="127.0.0.1")
    limiter.record_failure(email="test@example.com", client_ip="127.0.0.1")
    limiter.record_failure(email="test@example.com", client_ip="127.0.0.1")

    with pytest.raises(ApiError) as exc:
        limiter.assert_allowed(email="test@example.com", client_ip="127.0.0.1")

    limiter.close()

    assert exc.value.status_code == 429
    assert "AUTH_RATE_LIMITED" in str(exc.value.detail)


def test_login_rate_limiter_resets_after_success(tmp_path: Path) -> None:
    limiter = LoginRateLimiter(
        database_path=tmp_path / "state.db",
        max_attempts=2,
        window_seconds=300,
        lock_seconds=120,
    )

    limiter.record_failure(email="ok@example.com", client_ip="127.0.0.1")
    limiter.record_success(email="ok@example.com", client_ip="127.0.0.1")

    limiter.assert_allowed(email="ok@example.com", client_ip="127.0.0.1")
    limiter.close()
