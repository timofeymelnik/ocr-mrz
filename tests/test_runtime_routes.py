from __future__ import annotations

import asyncio
from io import BytesIO
from typing import Any, cast

from fastapi import FastAPI, UploadFile
from fastapi.routing import APIRoute

from app.api.errors import ApiError
from app.api.runtime_routes import (
    BrowserSessionFillRequest,
    BrowserSessionOpenRequest,
    BrowserSessionTemplateRequest,
    RuntimeRouteDeps,
    decode_queued_upload,
    register_runtime_routes,
)
from app.core.config import (
    AppConfig,
    AuthConfig,
    LoggingConfig,
    QueueConfig,
    SecurityConfig,
)


class _DummyUploadService:
    async def upload_document(
        self, *, file: UploadFile, tasa_code: str, source_kind: str
    ) -> dict[str, str]:
        return {
            "filename": file.filename or "",
            "tasa_code": tasa_code,
            "source_kind": source_kind,
        }


class _DummyTaskQueue:
    def __init__(self) -> None:
        self._handler: tuple[str, object] | None = None
        self.started = False
        self.stopped = False
        self.closed = False
        self.submitted_payload: dict[str, str] | None = None

    def register_handler(self, task_type: str, handler: object) -> None:
        self._handler = (task_type, handler)

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    def close(self) -> None:
        self.closed = True

    def submit(
        self, *, task_type: str, payload: dict[str, str], idempotency_key: str
    ) -> str:
        self.submitted_payload = {
            "task_type": task_type,
            "idempotency_key": idempotency_key,
            "filename": payload["filename"],
        }
        return "task-1"

    def get(self, task_id: str) -> dict[str, object]:
        return {
            "task_id": task_id,
            "task_type": "document_upload",
            "status": "queued",
            "attempts": 0,
            "max_retries": 3,
            "created_at": 1,
            "updated_at": 1,
            "expires_at": 60,
            "result": None,
            "error": "",
            "dead_letter_reason": "",
        }


class _DummyLifecycleService:
    async def open_session(self, **kwargs: Any) -> dict[str, str]:
        return {"status": "opened", "document_id": str(kwargs["document_id"])}

    async def get_state(self, *, document_id: str) -> dict[str, str]:
        return {"status": "open", "document_id": document_id}

    async def close_session(self, *, document_id: str) -> dict[str, str]:
        return {"status": "closed", "document_id": document_id}


class _DummyFillService:
    async def fill_opened_session(self, **kwargs: Any) -> tuple[int, dict[str, str]]:
        return 200, {"status": "filled", "document_id": str(kwargs["document_id"])}


class _DummyTemplateMappingService:
    def build_template_response(self, **kwargs: Any) -> tuple[int, dict[str, str]]:
        return 200, {"status": "ok", "current_url": str(kwargs["current_url"])}


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
            request_max_bytes=1024,
            upload_max_bytes=4,
            login_rate_limit_max_attempts=5,
            login_rate_limit_window_seconds=300,
            login_rate_limit_lock_seconds=600,
        ),
    )


def _build_app() -> tuple[FastAPI, _DummyTaskQueue, dict[str, bool]]:
    app = FastAPI()
    task_queue = _DummyTaskQueue()
    shutdown_state = {"closed": False}

    async def process_upload_task(payload: dict[str, Any]) -> dict[str, Any]:
        return payload

    deps = RuntimeRouteDeps(
        config=_config(),
        safe=lambda value: "" if value is None else str(value).strip(),
        upload_service=_DummyUploadService(),
        task_queue=task_queue,
        browser_lifecycle_service=_DummyLifecycleService(),
        browser_fill_service=_DummyFillService(),
        template_mapping_service=_DummyTemplateMappingService(),
        read_or_bootstrap_record=lambda _document_id: {"payload": {"a": 1}},
        process_upload_task=process_upload_task,
        on_shutdown=lambda: shutdown_state.__setitem__("closed", True),
    )
    register_runtime_routes(app, deps=deps)
    return app, task_queue, shutdown_state


def _route(app: FastAPI, path: str, method: str):
    for route in app.routes:
        if isinstance(route, APIRoute) and route.path == path and method in route.methods:
            return route.endpoint
    raise AssertionError(f"Route {method} {path} not found")


def test_runtime_routes_health_and_task_status() -> None:
    app, _, _ = _build_app()
    health = _route(app, "/api/health", "GET")
    get_task = _route(app, "/api/tasks/{task_id}", "GET")

    health_payload = health()
    task_payload = get_task(task_id="task-123")

    assert health_payload.model_dump() == {"status": "ok"}
    assert task_payload.model_dump()["task_id"] == "task-123"


def test_runtime_routes_upload_async_submits_task_with_idempotency_key() -> None:
    app, task_queue, _ = _build_app()
    upload_async = _route(app, "/api/documents/upload-async", "POST")
    upload_file = UploadFile(filename="sample.pdf", file=BytesIO(b"abc"))

    response = asyncio.run(
        upload_async(
            file=upload_file,
            tasa_code="790_012",
            source_kind="upload",
            idempotency_key="idem-1",
        )
    )

    assert response.model_dump()["task_id"] == "task-1"
    assert task_queue.submitted_payload == {
        "task_type": "document_upload",
        "idempotency_key": "idem-1",
        "filename": "sample.pdf",
    }


def test_runtime_routes_upload_rejects_file_larger_than_config_limit() -> None:
    app, _, _ = _build_app()
    upload = _route(app, "/api/documents/upload", "POST")
    upload_file = UploadFile(filename="sample.pdf", file=BytesIO(b"12345"))

    try:
        asyncio.run(
            upload(
                file=upload_file,
                tasa_code="790_012",
                source_kind="upload",
            )
        )
    except ApiError as exc:
        assert exc.status_code == 413
        detail: dict[str, Any] = (
            cast(dict[str, Any], exc.detail) if isinstance(exc.detail, dict) else {}
        )
        assert str(detail.get("error_code", "")) == "REQUEST_TOO_LARGE"
    else:
        raise AssertionError("Expected ApiError for oversized upload")


def test_runtime_routes_browser_endpoints_use_services() -> None:
    app, _, _ = _build_app()
    open_session = _route(app, "/api/documents/{document_id}/browser-session/open", "POST")
    get_state = _route(app, "/api/documents/{document_id}/browser-session/state", "GET")
    fill_session = _route(app, "/api/documents/{document_id}/browser-session/fill", "POST")
    resolve_template = _route(
        app, "/api/documents/{document_id}/browser-session/template", "POST"
    )
    close_session = _route(
        app, "/api/documents/{document_id}/browser-session/close", "POST"
    )

    open_response = asyncio.run(
        open_session(document_id="doc-1", req=BrowserSessionOpenRequest())
    )
    state_response = asyncio.run(get_state(document_id="doc-1"))
    fill_response = asyncio.run(
        fill_session(document_id="doc-1", req=BrowserSessionFillRequest())
    )
    template_response = resolve_template(
        document_id="doc-1",
        req=BrowserSessionTemplateRequest(current_url="https://example.com"),
    )
    close_response = asyncio.run(close_session(document_id="doc-1"))

    assert open_response.status_code == 200
    assert state_response.status_code == 200
    assert fill_response.status_code == 200
    assert template_response.status_code == 200
    assert close_response.status_code == 200


def test_runtime_routes_startup_and_shutdown_hooks_manage_queue_lifecycle() -> None:
    app, task_queue, shutdown_state = _build_app()

    for hook in app.router.on_startup:
        asyncio.run(hook())
    for hook in app.router.on_shutdown:
        asyncio.run(hook())

    assert task_queue.started is True
    assert task_queue.stopped is True
    assert task_queue.closed is True
    assert shutdown_state["closed"] is True


def test_decode_queued_upload_decodes_and_calls_upload_service() -> None:
    service = _DummyUploadService()
    payload = asyncio.run(
        decode_queued_upload(
            "YWJj",
            file_name="queued.pdf",
            upload_service=service,
            tasa_code="790_012",
            source_kind="upload",
        )
    )
    assert payload["filename"] == "queued.pdf"
