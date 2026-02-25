from __future__ import annotations

import asyncio
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from pathlib import Path
from typing import Any, cast

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from app.api.errors import ApiError, ApiErrorCode
from app.api.http_setup import register_exception_handlers, register_http_middleware
from app.api.runtime_routes import (
    RuntimeRouteDeps,
    decode_queued_upload,
    register_runtime_routes,
)
from app.auth.middleware import create_auth_middleware
from app.auth.rate_limiter import LoginRateLimiter
from app.auth.repository import AuthRepository
from app.auth.router import create_auth_router
from app.auth.service import AuthService
from app.autofill.target_autofill import should_save_artifact_screenshots_on_error
from app.browser.session_fill_service import BrowserSessionFillService
from app.browser.session_lifecycle_service import BrowserSessionLifecycleService
from app.browser.session_manager import (
    close_browser_session,
    fill_browser_session,
    get_browser_session_state,
    open_browser_session,
)
from app.browser.template_mapping_service import TemplateMappingService
from app.core.config import AppConfig
from app.core.logging import setup_logging
from app.core.mongo_migrations import apply_mongo_migrations
from app.core.task_queue import QueueSettings, TaskQueue
from app.core.validators import (
    collect_validation_errors,
    collect_validation_issues,
    normalize_payload_for_form,
)
from app.crm.repository import CRMRepository
from app.crm.router import create_crm_router
from app.crm.service import CRMService, build_record_from_crm
from app.data_builder.data_builder import build_tasa_document
from app.documents.enrichment_service import DocumentEnrichmentService
from app.documents.router import create_documents_router
from app.documents.service import DocumentsService
from app.documents.upload_service import UploadService
from app.mappings.repository import FormMappingRepository
from app.ocr_extract.ocr import VisionOCRClient
from app.pipeline.runner import attach_pipeline_metadata, stage_start, stage_success

load_dotenv()
APP_CONFIG = AppConfig.from_env()
setup_logging(APP_CONFIG.logging.level)
LOGGER = logging.getLogger(__name__)

APP_ROOT = Path(__file__).resolve().parent
RUNTIME_DIR = APP_ROOT / "runtime"
UPLOADS_DIR = RUNTIME_DIR / "uploads"
DOCS_DIR = RUNTIME_DIR / "documents"
AUTOFILL_DIR = RUNTIME_DIR / "autofill"
QUEUE_UPLOADS_DIR = RUNTIME_DIR / "queued_uploads"

for directory in [RUNTIME_DIR, UPLOADS_DIR, DOCS_DIR, AUTOFILL_DIR, QUEUE_UPLOADS_DIR]:
    directory.mkdir(parents=True, exist_ok=True)
CRM_REPO = CRMRepository(APP_ROOT)
FORM_MAPPING_REPO = FormMappingRepository(APP_ROOT)
_BROWSER_EXECUTOR = ThreadPoolExecutor(
    max_workers=1, thread_name_prefix="playwright-sync"
)
DEFAULT_TARGET_URL = ""


def _safe(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _record_path(document_id: str) -> Path:
    return DOCS_DIR / f"{document_id}.json"


def _read_record(document_id: str) -> dict[str, Any]:
    path = _record_path(document_id)
    if not path.exists():
        raise ApiError(
            status_code=404,
            error_code=ApiErrorCode.DOCUMENT_NOT_FOUND,
            message=f"Document not found: {document_id}",
        )
    return cast(dict[str, Any], json.loads(path.read_text(encoding="utf-8")))


def _write_record(document_id: str, data: dict[str, Any]) -> None:
    path = _record_path(document_id)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _runtime_url(path: Path) -> str:
    rel = path.resolve().relative_to(RUNTIME_DIR.resolve())
    return f"/runtime/{str(rel).replace(os.sep, '/')}"


def _artifact_url_from_value(value: Any) -> str:
    raw = _safe(value)
    if not raw:
        return ""
    path = Path(raw)
    if not path.exists():
        return ""
    try:
        return _runtime_url(path)
    except Exception:
        return ""


def _latest_artifact_url(base_dir: Path, pattern: str) -> str:
    files = sorted(
        base_dir.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True
    )
    if not files:
        return ""
    try:
        return _runtime_url(files[0])
    except Exception:
        return ""


def _allowed_suffix(filename: str) -> bool:
    return Path(filename).suffix.lower() in {".jpg", ".jpeg", ".png", ".pdf"}


async def _run_browser_call(fn, *args, **kwargs):
    loop = asyncio.get_running_loop()
    call = partial(fn, *args, **kwargs)
    return await loop.run_in_executor(_BROWSER_EXECUTOR, call)


def create_app() -> FastAPI:
    app = FastAPI(title="OCR Tasa UI API", version="1.1.0")
    apply_mongo_migrations()
    app.add_middleware(
        CORSMiddleware,
        allow_origins=APP_CONFIG.security.cors_allowed_origins,
        allow_credentials=True,
        allow_methods=["GET", "POST", "DELETE", "OPTIONS"],
        allow_headers=["Authorization", "Content-Type", "Idempotency-Key"],
    )
    app.mount("/runtime", StaticFiles(directory=str(RUNTIME_DIR)), name="runtime")
    register_http_middleware(app, config=APP_CONFIG, logger=LOGGER)
    register_exception_handlers(app, logger=LOGGER)

    auth_repo = AuthRepository(APP_ROOT)
    auth_service = AuthService(auth_repo, APP_CONFIG.auth)
    state_db_path = (APP_ROOT / APP_CONFIG.queue.sqlite_path).resolve()
    login_rate_limiter = LoginRateLimiter(
        database_path=state_db_path,
        max_attempts=APP_CONFIG.security.login_rate_limit_max_attempts,
        window_seconds=APP_CONFIG.security.login_rate_limit_window_seconds,
        lock_seconds=APP_CONFIG.security.login_rate_limit_lock_seconds,
    )
    auth_service.bootstrap_admin_user()
    app.include_router(create_auth_router(auth_service, login_rate_limiter))
    app.middleware("http")(create_auth_middleware(auth_service))
    crm_service = CRMService(
        repo=CRM_REPO,
        default_target_url=DEFAULT_TARGET_URL,
        safe_value=_safe,
        read_record=_read_record,
        run_browser_call=_run_browser_call,
        close_browser_session=close_browser_session,
        record_path=_record_path,
        logger=LOGGER,
    )
    app.include_router(create_crm_router(service=crm_service))

    def read_or_bootstrap_record(document_id: str) -> dict[str, Any]:
        try:
            return _read_record(document_id)
        except HTTPException as exc:
            if exc.status_code != 404:
                raise
            crm_doc = CRM_REPO.get_document(document_id)
            if not crm_doc:
                raise
            record = build_record_from_crm(document_id, crm_doc, DEFAULT_TARGET_URL)
            _write_record(document_id, record)
            return record

    enrichment_service = DocumentEnrichmentService(
        repo=CRM_REPO,
        default_target_url=DEFAULT_TARGET_URL,
        safe_value=_safe,
        normalize_payload_for_form=normalize_payload_for_form,
        collect_validation_errors=lambda payload, require_tramite: collect_validation_errors(
            payload, require_tramite=require_tramite
        ),
        read_or_bootstrap_record=read_or_bootstrap_record,
        write_record=_write_record,
    )

    documents_service = DocumentsService(
        crm_repo=CRM_REPO,
        read_or_bootstrap_record=read_or_bootstrap_record,
        write_record=_write_record,
        merge_candidates_for_payload=lambda document_id, payload, limit: enrichment_service.merge_candidates_for_payload(
            document_id, payload, limit=limit
        ),
        collect_validation_errors=lambda payload, require_tramite: collect_validation_errors(
            payload, require_tramite=require_tramite
        ),
        collect_validation_issues=lambda payload, require_tramite: collect_validation_issues(
            payload, require_tramite=require_tramite
        ),
        sync_family_reference=enrichment_service.sync_family_reference,
        enrich_record_payload_by_identity=lambda document_id, payload, persist, source_document_id, selected_fields=None: enrichment_service.enrich_record_payload_by_identity(
            document_id,
            payload,
            persist=persist,
            source_document_id=source_document_id,
            selected_fields=selected_fields,
        ),
        safe_value=_safe,
    )
    app.include_router(create_documents_router(service=documents_service))
    upload_service = UploadService(
        uploads_dir=UPLOADS_DIR,
        default_target_url=DEFAULT_TARGET_URL,
        crm_repo=CRM_REPO,
        safe_value=_safe,
        runtime_url=_runtime_url,
        allowed_suffix=_allowed_suffix,
        write_record=_write_record,
        merge_candidates_for_payload=lambda document_id, payload, limit: enrichment_service.merge_candidates_for_payload(
            document_id, payload, limit=limit
        ),
        collect_validation_errors=lambda payload, require_tramite: collect_validation_errors(
            payload, require_tramite=require_tramite
        ),
        collect_validation_issues=lambda payload, require_tramite: collect_validation_issues(
            payload, require_tramite=require_tramite
        ),
        build_tasa_document=build_tasa_document,
        normalize_payload_for_form=normalize_payload_for_form,
        attach_pipeline_metadata=attach_pipeline_metadata,
        stage_start=stage_start,
        stage_success=stage_success,
        create_ocr_client=VisionOCRClient,
        sync_family_reference=enrichment_service.sync_family_reference,
    )
    browser_lifecycle_service = BrowserSessionLifecycleService(
        default_target_url=DEFAULT_TARGET_URL,
        read_or_bootstrap_record=read_or_bootstrap_record,
        write_record=_write_record,
        safe_value=_safe,
        run_browser_call=_run_browser_call,
        open_browser_session=open_browser_session,
        get_browser_session_state=get_browser_session_state,
        close_browser_session=close_browser_session,
        crm_repo=cast(Any, CRM_REPO),
        logger_exception=LOGGER.exception,
    )
    template_mapping_service = TemplateMappingService(
        form_mapping_repo=FORM_MAPPING_REPO,
        safe_value=_safe,
        collect_validation_errors=lambda payload, require_tramite: collect_validation_errors(
            payload, require_tramite=require_tramite
        ),
        collect_validation_issues=lambda payload, require_tramite: collect_validation_issues(
            payload, require_tramite=require_tramite
        ),
    )
    browser_fill_service = BrowserSessionFillService(
        read_or_bootstrap_record=read_or_bootstrap_record,
        write_record=_write_record,
        safe_value=_safe,
        collect_validation_errors=lambda payload, require_tramite: collect_validation_errors(
            payload, require_tramite=require_tramite
        ),
        collect_validation_issues=lambda payload, require_tramite: collect_validation_issues(
            payload, require_tramite=require_tramite
        ),
        run_browser_call=_run_browser_call,
        get_browser_session_state=get_browser_session_state,
        fill_browser_session=fill_browser_session,
        template_mapping_service=template_mapping_service,
        crm_repo=CRM_REPO,
        autofill_dir=AUTOFILL_DIR,
        artifact_url_from_value=_artifact_url_from_value,
        latest_artifact_url=_latest_artifact_url,
        should_save_artifact_screenshots_on_error=should_save_artifact_screenshots_on_error,
        logger_info=LOGGER.info,
    )
    task_queue = TaskQueue(
        QueueSettings(
            database_path=state_db_path,
            default_ttl_seconds=APP_CONFIG.queue.default_ttl_seconds,
            default_max_retries=APP_CONFIG.queue.default_max_retries,
            default_retry_delay_seconds=APP_CONFIG.queue.default_retry_delay_seconds,
        )
    )

    async def process_upload_task(payload: dict[str, Any]) -> dict[str, Any]:
        encoded_bytes = _safe(payload.get("file_bytes_b64"))
        file_name = _safe(payload.get("filename"))
        return await decode_queued_upload(
            encoded_bytes,
            file_name=file_name,
            upload_service=upload_service,
            tasa_code=_safe(payload.get("tasa_code")),
            source_kind=_safe(payload.get("source_kind")),
        )

    register_runtime_routes(
        app,
        deps=RuntimeRouteDeps(
            config=APP_CONFIG,
            safe=_safe,
            upload_service=upload_service,
            task_queue=task_queue,
            browser_lifecycle_service=browser_lifecycle_service,
            browser_fill_service=browser_fill_service,
            template_mapping_service=template_mapping_service,
            read_or_bootstrap_record=read_or_bootstrap_record,
            process_upload_task=process_upload_task,
            on_shutdown=login_rate_limiter.close,
        ),
    )

    return app


app = create_app()
