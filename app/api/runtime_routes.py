"""Runtime route registration for upload/task/browser endpoints."""

from __future__ import annotations

from base64 import b64decode, b64encode
from dataclasses import dataclass
from io import BytesIO
from typing import Any, Awaitable, Callable, cast

from fastapi import FastAPI, File, Form, Header, UploadFile
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict

from app.api.contracts import (
    ApiErrorResponse,
    HealthResponse,
    TaskAcceptedResponse,
    TaskStatusResponse,
)
from app.api.errors import ApiError, ApiErrorCode
from app.core.config import AppConfig

UPLOAD_FILE_PARAM = File(...)
TASA_CODE_PARAM = Form(default="790_012")
SOURCE_KIND_PARAM = Form(default="")


class BrowserSessionOpenRequest(BaseModel):
    """Payload for opening a managed browser session."""

    model_config = ConfigDict(extra="forbid")

    target_url: str | None = None
    timeout_ms: int = 25000
    slowmo: int = 80
    headless: bool = False


class BrowserSessionFillRequest(BaseModel):
    """Payload for autofill operation in an opened browser session."""

    model_config = ConfigDict(extra="forbid")

    payload: dict[str, Any] | None = None
    timeout_ms: int = 25000
    explicit_mappings: list[dict[str, Any]] | None = None
    fill_strategy: str = "strict_template"


class BrowserSessionTemplateRequest(BaseModel):
    """Payload for resolving template mapping for client-browser flow."""

    model_config = ConfigDict(extra="forbid")

    current_url: str
    payload: dict[str, Any] | None = None
    fill_strategy: str = "strict_template"


@dataclass(frozen=True)
class RuntimeRouteDeps:
    """Dependencies required to mount runtime routes."""

    config: AppConfig
    safe: Callable[[Any], str]
    upload_service: Any
    task_queue: Any
    browser_lifecycle_service: Any
    browser_fill_service: Any
    template_mapping_service: Any
    read_or_bootstrap_record: Callable[[str], dict[str, Any]]
    process_upload_task: Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]
    on_shutdown: Callable[[], None]


def register_runtime_routes(app: FastAPI, *, deps: RuntimeRouteDeps) -> None:
    """Register health/upload/task/browser endpoints and lifecycle hooks."""
    deps.task_queue.register_handler("document_upload", deps.process_upload_task)

    @app.on_event("startup")
    async def startup_task_queue_worker() -> None:
        await deps.task_queue.start()

    @app.on_event("shutdown")
    async def shutdown_task_queue_worker() -> None:
        await deps.task_queue.stop()
        deps.task_queue.close()
        deps.on_shutdown()

    @app.get(
        "/api/health",
        response_model=HealthResponse,
    )
    def health() -> HealthResponse:
        return HealthResponse(status="ok")

    @app.post("/api/documents/upload")
    async def upload_document(
        file: UploadFile = UPLOAD_FILE_PARAM,
        tasa_code: str = TASA_CODE_PARAM,
        source_kind: str = SOURCE_KIND_PARAM,
    ) -> JSONResponse:
        if not file.filename:
            raise ApiError(
                status_code=400,
                error_code=ApiErrorCode.VALIDATION_ERROR,
                message="Filename is required.",
            )
        file_bytes = await file.read()
        if len(file_bytes) > deps.config.security.upload_max_bytes:
            raise ApiError(
                status_code=413,
                error_code=ApiErrorCode.REQUEST_TOO_LARGE,
                message=(
                    "Uploaded file exceeds configured limit "
                    f"({deps.config.security.upload_max_bytes} bytes)."
                ),
            )
        queued_upload = UploadFile(
            file=BytesIO(file_bytes),
            filename=file.filename,
        )
        payload = await deps.upload_service.upload_document(
            file=queued_upload,
            tasa_code=tasa_code,
            source_kind=source_kind,
        )
        return JSONResponse(payload)

    @app.post(
        "/api/documents/upload-async",
        response_model=TaskAcceptedResponse,
        status_code=202,
        responses={
            400: {"model": ApiErrorResponse},
            413: {"model": ApiErrorResponse},
            422: {"model": ApiErrorResponse},
        },
    )
    async def upload_document_async(
        file: UploadFile = UPLOAD_FILE_PARAM,
        tasa_code: str = TASA_CODE_PARAM,
        source_kind: str = SOURCE_KIND_PARAM,
        idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    ) -> TaskAcceptedResponse:
        if not file.filename:
            raise ApiError(
                status_code=400,
                error_code=ApiErrorCode.VALIDATION_ERROR,
                message="Filename is required.",
            )

        file_bytes = await file.read()
        if len(file_bytes) > deps.config.security.upload_max_bytes:
            raise ApiError(
                status_code=413,
                error_code=ApiErrorCode.REQUEST_TOO_LARGE,
                message=(
                    "Uploaded file exceeds configured limit "
                    f"({deps.config.security.upload_max_bytes} bytes)."
                ),
            )
        task_payload = {
            "filename": file.filename,
            "file_bytes_b64": b64encode(file_bytes).decode("ascii"),
            "tasa_code": tasa_code,
            "source_kind": source_kind,
        }
        task_id = deps.task_queue.submit(
            task_type="document_upload",
            payload=task_payload,
            idempotency_key=deps.safe(idempotency_key),
        )
        return TaskAcceptedResponse(
            task_id=task_id,
            status="queued",
            status_url=f"/api/tasks/{task_id}",
        )

    @app.get(
        "/api/tasks/{task_id}",
        response_model=TaskStatusResponse,
        responses={404: {"model": ApiErrorResponse}},
    )
    def get_task_status(task_id: str) -> TaskStatusResponse:
        task_state = deps.task_queue.get(task_id)
        if not task_state:
            raise ApiError(
                status_code=404,
                error_code=ApiErrorCode.QUEUE_TASK_NOT_FOUND,
                message=f"Task not found: {task_id}",
            )
        return TaskStatusResponse(**task_state)

    @app.post("/api/documents/{document_id}/browser-session/open")
    async def open_managed_browser_session(
        document_id: str, req: BrowserSessionOpenRequest
    ) -> JSONResponse:
        payload = await deps.browser_lifecycle_service.open_session(
            document_id=document_id,
            target_url=req.target_url,
            headless=req.headless,
            slowmo=req.slowmo,
            timeout_ms=req.timeout_ms,
        )
        return JSONResponse(payload)

    @app.get("/api/documents/{document_id}/browser-session/state")
    async def browser_session_state(document_id: str) -> JSONResponse:
        payload = await deps.browser_lifecycle_service.get_state(
            document_id=document_id
        )
        return JSONResponse(payload)

    @app.post("/api/documents/{document_id}/browser-session/fill")
    async def fill_opened_browser_session(
        document_id: str, req: BrowserSessionFillRequest
    ) -> JSONResponse:
        record = deps.read_or_bootstrap_record(document_id)
        payload = req.payload or record.get("payload") or {}
        status_code, content = await deps.browser_fill_service.fill_opened_session(
            document_id=document_id,
            payload=payload,
            timeout_ms=req.timeout_ms,
            fill_strategy=req.fill_strategy,
        )
        return JSONResponse(status_code=status_code, content=content)

    @app.post("/api/documents/{document_id}/browser-session/template")
    def resolve_template_for_client_browser(
        document_id: str, req: BrowserSessionTemplateRequest
    ) -> JSONResponse:
        record = deps.read_or_bootstrap_record(document_id)
        current_url = deps.safe(req.current_url)
        if not current_url:
            raise ApiError(
                status_code=422,
                error_code=ApiErrorCode.VALIDATION_ERROR,
                message="current_url is required.",
            )

        payload = req.payload or record.get("payload") or {}
        status_code, content = deps.template_mapping_service.build_template_response(
            document_id=document_id,
            current_url=current_url,
            payload=payload,
            fill_strategy=req.fill_strategy,
        )
        return JSONResponse(status_code=status_code, content=content)

    @app.post("/api/documents/{document_id}/browser-session/close")
    async def close_managed_browser_session(document_id: str) -> JSONResponse:
        payload = await deps.browser_lifecycle_service.close_session(
            document_id=document_id
        )
        return JSONResponse(payload)


async def decode_queued_upload(
    encoded_bytes: str,
    *,
    file_name: str,
    upload_service: Any,
    tasa_code: str,
    source_kind: str,
) -> dict[str, Any]:
    """Decode queued upload payload and forward it to upload service."""
    queued_upload = UploadFile(
        file=BytesIO(b64decode(encoded_bytes.encode("ascii"))),
        filename=file_name,
    )
    result = await upload_service.upload_document(
        file=queued_upload,
        tasa_code=tasa_code,
        source_kind=source_kind,
    )
    return cast(dict[str, Any], result)
