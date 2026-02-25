"""HTTP middleware and exception handler wiring for FastAPI apps."""

from __future__ import annotations

import uuid
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app.api.contracts import ApiErrorResponse
from app.api.errors import ApiErrorCode, to_error_payload
from app.core.config import AppConfig
from app.core.logging import set_correlation_id


def register_http_middleware(app: FastAPI, *, config: AppConfig, logger: Any) -> None:
    """Attach common security and observability middleware to an app."""

    @app.middleware("http")
    async def request_size_limit_middleware(request: Request, call_next):
        content_length = request.headers.get("content-length")
        if content_length:
            try:
                parsed_length = int(content_length)
            except ValueError:
                parsed_length = 0
            if parsed_length > config.security.request_max_bytes:
                return JSONResponse(
                    status_code=413,
                    content=ApiErrorResponse(
                        error_code=ApiErrorCode.REQUEST_TOO_LARGE,
                        message=(
                            "Request size exceeds configured limit "
                            f"({config.security.request_max_bytes} bytes)."
                        ),
                    ).model_dump(),
                )
        return await call_next(request)

    @app.middleware("http")
    async def request_logging_middleware(request: Request, call_next):
        correlation_id = (
            request.headers.get("x-request-id")
            or request.headers.get("x-correlation-id")
            or uuid.uuid4().hex
        )
        set_correlation_id(correlation_id)
        response = await call_next(request)
        response.headers["X-Request-ID"] = correlation_id
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["Content-Security-Policy"] = (
            "default-src 'none'; frame-ancestors 'none'; base-uri 'none'"
        )
        response.headers["Permissions-Policy"] = (
            "geolocation=(), microphone=(), camera=()"
        )
        logger.info(
            "request_completed",
            extra={
                "path": request.url.path,
                "method": request.method,
                "status_code": response.status_code,
            },
        )
        return response


def register_exception_handlers(app: FastAPI, *, logger: Any) -> None:
    """Attach API exception handlers that return stable error contracts."""

    @app.exception_handler(HTTPException)
    async def handle_http_exception(
        request: Request, exc: HTTPException
    ) -> JSONResponse:
        payload = to_error_payload(exc.detail, exc.status_code)
        logger.warning(
            "http_exception",
            extra={
                "path": request.url.path,
                "method": request.method,
                "status_code": exc.status_code,
            },
        )
        return JSONResponse(
            status_code=exc.status_code,
            content=ApiErrorResponse(**payload).model_dump(),
        )

    @app.exception_handler(RequestValidationError)
    async def handle_validation_exception(
        request: Request,
        exc: RequestValidationError,
    ) -> JSONResponse:
        logger.warning(
            "validation_exception",
            extra={
                "path": request.url.path,
                "method": request.method,
                "status_code": 422,
            },
        )
        return JSONResponse(
            status_code=422,
            content=ApiErrorResponse(
                error_code=ApiErrorCode.VALIDATION_ERROR,
                message=str(exc),
            ).model_dump(),
        )

    @app.exception_handler(Exception)
    async def handle_unexpected_exception(
        request: Request,
        exc: Exception,
    ) -> JSONResponse:
        logger.exception(
            "unexpected_exception",
            extra={
                "path": request.url.path,
                "method": request.method,
                "status_code": 500,
            },
        )
        return JSONResponse(
            status_code=500,
            content=ApiErrorResponse(
                error_code=ApiErrorCode.INTERNAL_SERVER_ERROR,
                message=str(exc) or "Internal server error",
            ).model_dump(),
        )
