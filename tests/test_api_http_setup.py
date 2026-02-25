from __future__ import annotations

import asyncio
import inspect
import logging
from typing import Any, Awaitable, Coroutine, cast

from fastapi import FastAPI, HTTPException
from fastapi.exceptions import RequestValidationError
from starlette.requests import Request
from starlette.responses import Response

from app.api.http_setup import register_exception_handlers, register_http_middleware
from app.core.config import (
    AppConfig,
    AuthConfig,
    LoggingConfig,
    QueueConfig,
    SecurityConfig,
)

LOGGER = logging.getLogger(__name__)


def _config() -> AppConfig:
    return AppConfig(
        auth=AuthConfig(
            enabled=True,
            secret_key="secret",
            access_token_ttl_seconds=900,
            refresh_token_ttl_seconds=3600,
            issuer="test",
            admin_email="admin@test.local",
            admin_password="pass",
        ),
        queue=QueueConfig(
            sqlite_path="runtime/test.db",
            default_ttl_seconds=60,
            default_max_retries=3,
            default_retry_delay_seconds=1,
        ),
        logging=LoggingConfig(level="INFO"),
        security=SecurityConfig(
            cors_allowed_origins=["http://localhost:3000"],
            request_max_bytes=8,
            upload_max_bytes=16,
            login_rate_limit_max_attempts=5,
            login_rate_limit_window_seconds=300,
            login_rate_limit_lock_seconds=600,
        ),
    )


def _request(path: str, method: str = "GET", headers: list[tuple[bytes, bytes]] | None = None) -> Request:
    scope: dict[str, Any] = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": method,
        "scheme": "http",
        "path": path,
        "raw_path": path.encode("utf-8"),
        "query_string": b"",
        "root_path": "",
        "headers": headers or [],
        "client": ("127.0.0.1", 1234),
        "server": ("testserver", 80),
    }

    async def receive() -> dict[str, Any]:
        return {"type": "http.request", "body": b"", "more_body": False}

    return Request(scope, receive)


def _dispatch_by_name(app: FastAPI, name: str):
    for middleware in app.user_middleware:
        dispatch = middleware.kwargs.get("dispatch")
        if callable(dispatch) and getattr(dispatch, "__name__", "") == name:
            return dispatch
    raise AssertionError(f"Dispatch {name!r} not found")


def _resolve_response(result: Response | Awaitable[Response]) -> Response:
    if inspect.iscoroutine(result):
        return asyncio.run(cast(Coroutine[Any, Any, Response], result))
    return cast(Response, result)


def test_http_setup_adds_security_headers_and_request_id() -> None:
    app = FastAPI()
    register_http_middleware(app, config=_config(), logger=LOGGER)
    register_exception_handlers(app, logger=LOGGER)
    dispatch = _dispatch_by_name(app, "request_logging_middleware")

    request = _request("/ok", headers=[(b"x-request-id", b"req-123")])

    async def call_next(_request: Request) -> Response:
        return Response(content="ok", status_code=200)

    response = asyncio.run(dispatch(request, call_next))
    assert response.headers["X-Request-ID"] == "req-123"
    assert response.headers["X-Frame-Options"] == "DENY"
    assert response.headers["X-Content-Type-Options"] == "nosniff"


def test_http_setup_rejects_large_request_before_handler() -> None:
    app = FastAPI()
    register_http_middleware(app, config=_config(), logger=LOGGER)
    register_exception_handlers(app, logger=LOGGER)
    dispatch = _dispatch_by_name(app, "request_size_limit_middleware")
    request = _request("/echo", method="POST", headers=[(b"content-length", b"20")])

    async def call_next(_request: Request) -> Response:
        return Response(content="ok", status_code=200)

    response = asyncio.run(dispatch(request, call_next))
    assert response.status_code == 413


def test_http_setup_serializes_http_exception_payload() -> None:
    app = FastAPI()
    register_http_middleware(app, config=_config(), logger=LOGGER)
    register_exception_handlers(app, logger=LOGGER)
    request = _request("/not-found")
    handler = app.exception_handlers[HTTPException]
    response: Response = _resolve_response(
        handler(
            request,
            HTTPException(
                status_code=404,
                detail={"error_code": "DOCUMENT_NOT_FOUND", "message": "missing"},
            ),
        )
    )
    assert response.status_code == 404
    assert b"DOCUMENT_NOT_FOUND" in response.body


def test_http_setup_handles_unexpected_exceptions() -> None:
    app = FastAPI()
    register_http_middleware(app, config=_config(), logger=LOGGER)
    register_exception_handlers(app, logger=LOGGER)
    request = _request("/boom")
    handler = app.exception_handlers[Exception]
    response: Response = _resolve_response(handler(request, RuntimeError("boom")))
    assert response.status_code == 500
    assert b"INTERNAL_SERVER_ERROR" in response.body


def test_http_setup_handles_validation_exception() -> None:
    app = FastAPI()
    register_http_middleware(app, config=_config(), logger=LOGGER)
    register_exception_handlers(app, logger=LOGGER)
    request = _request("/validation")
    handler = app.exception_handlers[RequestValidationError]
    response: Response = _resolve_response(
        handler(request, RequestValidationError([]))
    )
    assert response.status_code == 422
    assert b"VALIDATION_ERROR" in response.body
