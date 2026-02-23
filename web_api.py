from __future__ import annotations

import json
import logging
import os
import re
import shutil
import uuid
import asyncio
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from fastapi import FastAPI, File, Form, HTTPException, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from crm_repository import CRMRepository
from form_mapping_repository import FormMappingRepository
from ocr import VisionOCRClient
from pipeline_runner import attach_pipeline_metadata, stage_start, stage_success
from browser_session_manager import (
    close_browser_session,
    collect_browser_session_placeholder_mappings,
    fill_browser_session,
    get_browser_session_state,
    inspect_browser_session_fields,
    open_browser_session,
)
from tasa_data_builder import build_tasa_document
from target_autofill import (
    CANONICAL_FIELD_KEYS,
    extract_pdf_placeholder_mappings_from_bytes,
    inspect_pdf_fields_from_bytes,
    should_save_artifact_screenshots_on_error,
    suggest_mappings_for_fields,
)
from pdf_validation import validate_filled_pdf_against_mapping
from validators import collect_validation_errors, collect_validation_issues, normalize_payload_for_form

load_dotenv()
LOGGER = logging.getLogger(__name__)

APP_ROOT = Path(__file__).resolve().parent
RUNTIME_DIR = APP_ROOT / "runtime"
UPLOADS_DIR = RUNTIME_DIR / "uploads"
DOCS_DIR = RUNTIME_DIR / "documents"
AUTOFILL_DIR = RUNTIME_DIR / "autofill"

for directory in [RUNTIME_DIR, UPLOADS_DIR, DOCS_DIR, AUTOFILL_DIR]:
    directory.mkdir(parents=True, exist_ok=True)
CRM_REPO = CRMRepository(APP_ROOT)
FORM_MAPPING_REPO = FormMappingRepository(APP_ROOT)
_BROWSER_EXECUTOR = ThreadPoolExecutor(max_workers=1, thread_name_prefix="playwright-sync")
DEFAULT_TARGET_URL = ""


class ConfirmRequest(BaseModel):
    payload: dict[str, Any]


class BrowserSessionOpenRequest(BaseModel):
    target_url: str | None = None
    timeout_ms: int = 25000
    slowmo: int = 80
    headless: bool = False


class BrowserSessionFillRequest(BaseModel):
    payload: dict[str, Any] | None = None
    timeout_ms: int = 25000
    explicit_mappings: list[dict[str, Any]] | None = None
    fill_strategy: str = "strict_template"


class BrowserSessionTemplateRequest(BaseModel):
    current_url: str
    payload: dict[str, Any] | None = None
    fill_strategy: str = "strict_template"


class BrowserSessionAnalyzeRequest(BaseModel):
    payload: dict[str, Any] | None = None


class BrowserSessionLearnRequest(BaseModel):
    fields: list[dict[str, Any]]
    mappings: list[dict[str, Any]]


class BrowserSessionSaveMapperRequest(BaseModel):
    overwrite: bool = False
    use_auto_pdf_fallback: bool = False


class BrowserSessionUploadMapperRequest(BaseModel):
    mappings: list[dict[str, Any]]
    overwrite: bool = False


class AutofillValidateRequest(BaseModel):
    filled_pdf_url: str | None = None
    filled_pdf_path: str | None = None


class EnrichByIdentityRequest(BaseModel):
    apply: bool = True
    source_document_id: str | None = None


def _safe(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _record_path(document_id: str) -> Path:
    return DOCS_DIR / f"{document_id}.json"


def _read_record(document_id: str) -> dict[str, Any]:
    path = _record_path(document_id)
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"Document not found: {document_id}")
    return json.loads(path.read_text(encoding="utf-8"))


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
    files = sorted(base_dir.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        return ""
    try:
        return _runtime_url(files[0])
    except Exception:
        return ""


def _allowed_suffix(filename: str) -> bool:
    return Path(filename).suffix.lower() in {".jpg", ".jpeg", ".png", ".pdf"}


def _normalize_identity(value: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", (value or "").upper())


def _identity_candidates(payload: dict[str, Any]) -> list[str]:
    out: list[str] = []
    for path in ["identificacion.nif_nie", "identificacion.pasaporte"]:
        v = _normalize_identity(_safe_payload_get(payload, path))
        if v and v not in out:
            out.append(v)
    return out


def _name_tokens(payload: dict[str, Any]) -> set[str]:
    parts = [
        _safe_payload_get(payload, "identificacion.primer_apellido"),
        _safe_payload_get(payload, "identificacion.segundo_apellido"),
        _safe_payload_get(payload, "identificacion.nombre"),
        _safe_payload_get(payload, "identificacion.nombre_apellidos"),
    ]
    joined = " ".join(parts).upper()
    return {t for t in re.split(r"[^A-Z0-9]+", joined) if len(t) >= 2}


def _merge_candidates_for_payload(document_id: str, payload: dict[str, Any], *, limit: int = 10) -> list[dict[str, Any]]:
    target_ids = set(_identity_candidates(payload))
    target_name_tokens = _name_tokens(payload)

    out: list[dict[str, Any]] = []
    summaries = CRM_REPO.search_documents(query="", limit=200)
    for item in summaries:
        candidate_id = _safe(item.get("document_id"))
        if not candidate_id or candidate_id == document_id:
            continue
        crm_doc = CRM_REPO.get_document(candidate_id) or {}
        source_payload = (
            crm_doc.get("effective_payload")
            or crm_doc.get("edited_payload")
            or crm_doc.get("ocr_payload")
            or {}
        )
        if not isinstance(source_payload, dict):
            continue
        candidate_ids = set(_identity_candidates(source_payload))
        candidate_name_tokens = _name_tokens(source_payload)

        identity_overlap = sorted(target_ids & candidate_ids)
        name_overlap = sorted(target_name_tokens & candidate_name_tokens)
        score = 0
        reasons: list[str] = []
        if identity_overlap:
            score += 100
            reasons.append("document_match")
        if len(name_overlap) >= 2:
            score += 40
            reasons.append("name_overlap")
        elif len(name_overlap) == 1:
            score += 15
            reasons.append("partial_name_overlap")
        if score <= 0:
            continue

        out.append(
            {
                "document_id": candidate_id,
                "name": _safe(item.get("name")),
                "document_number": _safe(item.get("document_number")),
                "updated_at": _safe(item.get("updated_at")),
                "score": score,
                "reasons": reasons,
                "identity_overlap": identity_overlap,
                "name_overlap": name_overlap,
            }
        )

    out.sort(key=lambda row: (int(row.get("score") or 0), row.get("updated_at") or ""), reverse=True)
    return out[:limit]


def _safe_payload_get(payload: dict[str, Any], path: str) -> str:
    node: Any = payload
    for part in path.split("."):
        if not isinstance(node, dict):
            return ""
        node = node.get(part)
    if node is None:
        return ""
    return str(node).strip()


def _safe_payload_set(payload: dict[str, Any], path: str, value: str) -> None:
    parts = path.split(".")
    node: Any = payload
    for part in parts[:-1]:
        if not isinstance(node.get(part), dict):
            node[part] = {}
        node = node[part]
    node[parts[-1]] = value


ENRICHMENT_PATHS: list[str] = [
    "identificacion.nif_nie",
    "identificacion.pasaporte",
    "identificacion.documento_tipo",
    "identificacion.nombre_apellidos",
    "identificacion.primer_apellido",
    "identificacion.segundo_apellido",
    "identificacion.nombre",
    "domicilio.tipo_via",
    "domicilio.nombre_via",
    "domicilio.numero",
    "domicilio.escalera",
    "domicilio.piso",
    "domicilio.puerta",
    "domicilio.telefono",
    "domicilio.municipio",
    "domicilio.provincia",
    "domicilio.cp",
    "declarante.localidad",
    "declarante.fecha",
    "declarante.fecha_dia",
    "declarante.fecha_mes",
    "declarante.fecha_anio",
    "ingreso.forma_pago",
    "ingreso.iban",
    "extra.email",
    "extra.fecha_nacimiento",
    "extra.fecha_nacimiento_dia",
    "extra.fecha_nacimiento_mes",
    "extra.fecha_nacimiento_anio",
    "extra.nacionalidad",
    "extra.pais_nacimiento",
    "extra.sexo",
    "extra.estado_civil",
    "extra.lugar_nacimiento",
    "extra.nombre_padre",
    "extra.nombre_madre",
    "extra.representante_legal",
    "extra.representante_documento",
    "extra.titulo_representante",
    "extra.hijos_escolarizacion_espana",
]


def _enrich_payload_fill_empty(
    *,
    payload: dict[str, Any],
    source_payload: dict[str, Any],
    source_document_id: str,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    applied: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    out = json.loads(json.dumps(payload, ensure_ascii=False))
    for path in ENRICHMENT_PATHS:
        current = _safe_payload_get(out, path)
        suggested = _safe_payload_get(source_payload, path)
        if not suggested:
            continue
        if current:
            skipped.append({"field": path, "current_value": current, "suggested_value": suggested, "reason": "already_filled"})
            continue
        _safe_payload_set(out, path, suggested)
        applied.append({"field": path, "current_value": current, "suggested_value": suggested, "source": source_document_id})
    return out, applied, skipped


def _resolve_runtime_path(value: str) -> Path:
    raw = (value or "").strip()
    if not raw:
        return Path("")
    if raw.startswith("http://") or raw.startswith("https://"):
        marker = "/runtime/"
        idx = raw.find(marker)
        if idx >= 0:
            raw = raw[idx:]
    if raw.startswith("/runtime/"):
        rel = raw[len("/runtime/") :]
        return (RUNTIME_DIR / rel).resolve()
    return Path(raw).resolve()


async def _run_browser_call(fn, *args, **kwargs):
    loop = asyncio.get_running_loop()
    call = partial(fn, *args, **kwargs)
    return await loop.run_in_executor(_BROWSER_EXECUTOR, call)


def _record_from_crm(document_id: str, crm_doc: dict[str, Any]) -> dict[str, Any]:
    payload = (
        crm_doc.get("effective_payload")
        or crm_doc.get("edited_payload")
        or crm_doc.get("ocr_payload")
        or {}
    )
    source = crm_doc.get("source") or {}
    return {
        "document_id": document_id,
        "preview_url": source.get("preview_url") or "",
        "source": source,
        "document": crm_doc.get("ocr_document") or {},
        "payload": payload,
        "missing_fields": crm_doc.get("missing_fields") or [],
        "manual_steps_required": crm_doc.get("manual_steps_required") or ["verify_filled_fields", "submit_or_download_manually"],
        "form_url": crm_doc.get("form_url") or DEFAULT_TARGET_URL,
        "target_url": crm_doc.get("target_url") or DEFAULT_TARGET_URL,
        "browser_session_id": crm_doc.get("browser_session_id") or "",
        "identity_match_found": bool(crm_doc.get("identity_match_found")),
        "identity_source_document_id": crm_doc.get("identity_source_document_id") or "",
        "enrichment_preview": crm_doc.get("enrichment_preview") or [],
        "merge_candidates": crm_doc.get("merge_candidates") or [],
    }


def create_app() -> FastAPI:
    app = FastAPI(title="OCR Tasa UI API", version="1.0.0")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.mount("/runtime", StaticFiles(directory=str(RUNTIME_DIR)), name="runtime")

    def read_or_bootstrap_record(document_id: str) -> dict[str, Any]:
        try:
            return _read_record(document_id)
        except HTTPException as exc:
            if exc.status_code != 404:
                raise
            crm_doc = CRM_REPO.get_document(document_id)
            if not crm_doc:
                raise
            record = _record_from_crm(document_id, crm_doc)
            _write_record(document_id, record)
            return record

    def enrich_record_payload_by_identity(
        document_id: str,
        payload: dict[str, Any],
        *,
        persist: bool = True,
        source_document_id: str = "",
    ) -> dict[str, Any]:
        identity_candidates = _identity_candidates(payload)
        source_doc_id = _safe(source_document_id)
        if not identity_candidates and not source_doc_id:
            return {
                "identity_match_found": False,
                "identity_source_document_id": "",
                "identity_key": "",
                "enrichment_preview": [],
                "applied_fields": [],
                "skipped_fields": [],
                "payload": payload,
            }

        source_record: dict[str, Any] | None = None
        if source_doc_id:
            source_record = CRM_REPO.get_document(source_doc_id)
            if not source_record or _safe(source_record.get("document_id")) == document_id:
                return {
                    "identity_match_found": False,
                    "identity_source_document_id": "",
                    "identity_key": identity_candidates[0] if identity_candidates else "",
                    "enrichment_preview": [],
                    "applied_fields": [],
                    "skipped_fields": [],
                    "payload": payload,
                }
        elif identity_candidates:
            source_record = CRM_REPO.find_latest_by_identities(identity_candidates, exclude_document_id=document_id)
        if not source_record:
            return {
                "identity_match_found": False,
                "identity_source_document_id": "",
                "identity_key": identity_candidates[0] if identity_candidates else "",
                "enrichment_preview": [],
                "applied_fields": [],
                "skipped_fields": [],
                "payload": payload,
            }

        source_payload = (
            source_record.get("effective_payload")
            or source_record.get("edited_payload")
            or source_record.get("ocr_payload")
            or {}
        )
        source_document_id = str(source_record.get("document_id") or "")
        source_candidates = _identity_candidates(source_payload if isinstance(source_payload, dict) else {})
        identity_key = next((c for c in identity_candidates if c in source_candidates), identity_candidates[0] if identity_candidates else "")
        enriched, applied, skipped = _enrich_payload_fill_empty(
            payload=payload,
            source_payload=source_payload if isinstance(source_payload, dict) else {},
            source_document_id=source_document_id,
        )
        if persist:
            rec = read_or_bootstrap_record(document_id)
            rec["payload"] = enriched
            rec["identity_key"] = identity_key
            rec["identity_match_found"] = True
            rec["identity_source_document_id"] = source_document_id
            rec["enrichment_preview"] = applied
            rec["enrichment_log"] = {
                "applied_fields": applied,
                "skipped_fields": skipped,
            }
            rec["missing_fields"] = collect_validation_errors(enriched, require_tramite=False)
            _write_record(document_id, rec)
            CRM_REPO.save_edited_payload(
                document_id=document_id,
                payload=enriched,
                missing_fields=rec["missing_fields"],
            )
            CRM_REPO.update_document_fields(
                document_id,
                {
                    "identity_key": identity_key,
                    "identity_match_found": True,
                    "identity_source_document_id": source_document_id,
                    "enrichment_preview": applied,
                    "enrichment_log": {
                        "applied_fields": applied,
                        "skipped_fields": skipped,
                    },
                },
            )
            if source_doc_id and source_doc_id != document_id:
                CRM_REPO.update_document_fields(
                    source_doc_id,
                    {
                        "status": "merged",
                        "merged_into_document_id": document_id,
                    },
                )
        return {
            "identity_match_found": True,
            "identity_source_document_id": source_document_id,
            "identity_key": identity_key,
            "enrichment_preview": applied,
            "applied_fields": [row["field"] for row in applied],
            "skipped_fields": [row["field"] for row in skipped],
            "payload": enriched,
        }

    def attach_template_artifacts_to_record(record: dict[str, Any], template: dict[str, Any]) -> None:
        document = record.get("document")
        if not isinstance(document, dict):
            return
        pipeline = document.get("pipeline")
        if not isinstance(pipeline, dict):
            return
        artifacts = pipeline.get("artifacts")
        if not isinstance(artifacts, dict):
            return
        artifacts["template_source"] = _safe(template.get("source"))
        artifacts["template_valid"] = bool(template.get("valid", True))
        artifacts["template_updated_at"] = _safe(template.get("updated_at"))
        artifacts["screenshot_enabled"] = bool(os.getenv("SAVE_ARTIFACT_SCREENSHOTS", "0").strip().lower() in {"1", "true", "yes", "on"})

    @app.get("/api/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/api/mappers/pdf/inspect")
    async def inspect_pdf_mapper_file(file: UploadFile = File(...)) -> JSONResponse:
        _ = file
        LOGGER.info("disabled endpoint called: /api/mappers/pdf/inspect")
        raise HTTPException(status_code=404, detail="Mapper endpoints are disabled. Templates are managed manually via files.")

    @app.post("/api/documents/{document_id}/browser-session/mapping/import-template-pdf")
    async def import_pdf_template_mapper(
        document_id: str,
        file: UploadFile = File(...),
        overwrite: bool = Form(default=False),
    ) -> JSONResponse:
        _ = document_id
        _ = file
        _ = overwrite
        LOGGER.info("disabled endpoint called: /api/documents/{document_id}/browser-session/mapping/import-template-pdf")
        raise HTTPException(status_code=404, detail="Mapper endpoints are disabled. Templates are managed manually via files.")

    @app.get("/api/crm/documents")
    def list_crm_documents(
        query: str = Query(default="", alias="query"),
        limit: int = Query(default=30, ge=1, le=200, alias="limit"),
    ) -> JSONResponse:
        items = CRM_REPO.search_documents(query=query, limit=limit)
        return JSONResponse({"items": items})

    @app.get("/api/crm/documents/{document_id}")
    def get_crm_document(document_id: str) -> JSONResponse:
        crm_doc = CRM_REPO.get_document(document_id)
        if not crm_doc:
            raise HTTPException(status_code=404, detail=f"CRM document not found: {document_id}")
        record = _record_from_crm(document_id, crm_doc)
        return JSONResponse(record)

    @app.delete("/api/crm/documents/{document_id}")
    async def delete_crm_document(document_id: str) -> JSONResponse:
        crm_doc = CRM_REPO.get_document(document_id)
        if not crm_doc:
            raise HTTPException(status_code=404, detail=f"CRM document not found: {document_id}")

        session_id = _safe(crm_doc.get("browser_session_id"))
        if not session_id:
            try:
                local_record = _read_record(document_id)
                session_id = _safe(local_record.get("browser_session_id"))
            except HTTPException:
                session_id = ""
        if session_id:
            try:
                await _run_browser_call(close_browser_session, session_id)
            except Exception:
                LOGGER.exception("Failed closing browser session during CRM delete: %s", session_id)

        deleted = CRM_REPO.delete_document(document_id)
        if not deleted:
            raise HTTPException(status_code=500, detail=f"Failed deleting CRM document: {document_id}")

        record_path = _record_path(document_id)
        if record_path.exists():
            try:
                record_path.unlink()
            except Exception:
                LOGGER.exception("Failed deleting local document record: %s", record_path)

        return JSONResponse({"document_id": document_id, "deleted": True})

    @app.post("/api/documents/upload")
    async def upload_document(
        file: UploadFile = File(...),
        tasa_code: str = Form(default="790_012"),
        source_kind: str = Form(...),
    ) -> JSONResponse:
        if not file.filename or not _allowed_suffix(file.filename):
            raise HTTPException(status_code=400, detail="Only .jpg/.jpeg/.png/.pdf are supported.")
        source_kind = _safe(source_kind).lower()
        if source_kind not in {"anketa", "passport", "nie_tie", "visa"}:
            raise HTTPException(status_code=422, detail="source_kind is required and must be one of: anketa, passport, nie_tie, visa")

        document_id = uuid.uuid4().hex
        suffix = Path(file.filename).suffix.lower()
        stored_name = f"{document_id}{suffix}"
        upload_path = UPLOADS_DIR / stored_name

        with upload_path.open("wb") as fh:
            shutil.copyfileobj(file.file, fh)

        ocr_client = VisionOCRClient()
        google_maps_api_key = os.getenv("GOOGLE_MAPS_API_KEY", "").strip() or os.getenv(
            "GOOGLE_CLOUD_VISION_API_KEY", ""
        ).strip()

        ocr_started = stage_start()
        ocr_result = ocr_client.extract_text(upload_path)
        ocr_stage = stage_success(
            "ocr",
            ocr_started,
            details={
                "source": _safe(getattr(ocr_result, "ocr_source", "live")) or "live",
                "used_cached_ocr": False,
                "pages": len(ocr_result.pages),
            },
        )

        parse_started = stage_start()
        document = build_tasa_document(
            ocr_front=ocr_result.full_text,
            ocr_back="",
            user_overrides={},
            geocode_candidates=None,
            google_maps_api_key=google_maps_api_key,
            tasa_code=tasa_code,
            source_file=file.filename,
            source_kind=source_kind,
        )
        parse_stage = stage_success(
            "parse_extract_map",
            parse_started,
            details={"forms_available": sorted((document.get("forms") or {}).keys())},
        )
        crm_stage = stage_success("crm_mapping", stage_start())
        document = attach_pipeline_metadata(
            document=document,
            source_files=[file.filename],
            ocr_details={
                "front_text_len": len(ocr_result.full_text or ""),
                "back_text_len": 0,
                "used_cached_ocr": False,
                "source": _safe(getattr(ocr_result, "ocr_source", "live")) or "live",
            },
            parse_stage=parse_stage,
            crm_stage=crm_stage,
            ocr_stage=ocr_stage,
        )

        payload = normalize_payload_for_form(document)
        merge_candidates = _merge_candidates_for_payload(document_id, payload, limit=10)
        missing_fields = collect_validation_errors(payload, require_tramite=False)
        validation_issues = collect_validation_issues(payload, require_tramite=False)

        record = {
            "document_id": document_id,
            "tasa_code": tasa_code,
            "source": {
                "original_filename": file.filename,
                "stored_path": str(upload_path),
                "preview_url": _runtime_url(upload_path),
                "source_kind": source_kind,
            },
            "document": document,
            "payload": payload,
            "missing_fields": missing_fields,
            "validation_issues": validation_issues,
            "manual_steps_required": ["verify_filled_fields", "submit_or_download_manually"],
            "form_url": DEFAULT_TARGET_URL,
            "target_url": DEFAULT_TARGET_URL,
            "identity_match_found": False,
            "identity_source_document_id": "",
            "enrichment_preview": [],
            "merge_candidates": merge_candidates,
        }
        _write_record(document_id, record)
        CRM_REPO.upsert_from_upload(
            document_id=document_id,
            payload=payload,
            ocr_document=document,
            source=record["source"],
            missing_fields=missing_fields,
            manual_steps_required=record["manual_steps_required"],
            form_url=DEFAULT_TARGET_URL,
            target_url=DEFAULT_TARGET_URL,
            identity_match_found=bool(record.get("identity_match_found")),
            identity_source_document_id=_safe(record.get("identity_source_document_id")),
            enrichment_preview=list(record.get("enrichment_preview") or []),
            merge_candidates=merge_candidates,
        )

        return JSONResponse(
            {
                "document_id": document_id,
                "preview_url": record["source"]["preview_url"],
                "form_url": DEFAULT_TARGET_URL,
                "target_url": DEFAULT_TARGET_URL,
                "payload": payload,
                "document": document,
                "missing_fields": missing_fields,
                "validation_issues": validation_issues,
                "manual_steps_required": record["manual_steps_required"],
                "identity_match_found": bool(record.get("identity_match_found")),
                "identity_source_document_id": _safe(record.get("identity_source_document_id")),
                "enrichment_preview": list(record.get("enrichment_preview") or []),
                "merge_candidates": merge_candidates,
            }
        )

    @app.get("/api/documents/{document_id}")
    def get_document(document_id: str) -> JSONResponse:
        record = read_or_bootstrap_record(document_id)
        return JSONResponse(record)

    @app.get("/api/documents/{document_id}/merge-candidates")
    def get_merge_candidates(document_id: str) -> JSONResponse:
        record = read_or_bootstrap_record(document_id)
        payload = record.get("payload") or {}
        if not isinstance(payload, dict):
            raise HTTPException(status_code=422, detail="Invalid payload in document record.")
        candidates = _merge_candidates_for_payload(document_id, payload, limit=10)
        record["merge_candidates"] = candidates
        _write_record(document_id, record)
        CRM_REPO.update_document_fields(document_id, {"merge_candidates": candidates})
        return JSONResponse({"document_id": document_id, "merge_candidates": candidates})

    @app.post("/api/documents/{document_id}/confirm")
    def confirm_document(document_id: str, req: ConfirmRequest) -> JSONResponse:
        record = read_or_bootstrap_record(document_id)
        payload = req.payload
        merge_candidates = _merge_candidates_for_payload(document_id, payload, limit=10)
        missing_fields = collect_validation_errors(payload, require_tramite=False)
        validation_issues = collect_validation_issues(payload, require_tramite=False)
        record["payload"] = payload
        record["missing_fields"] = missing_fields
        record["merge_candidates"] = merge_candidates
        _write_record(document_id, record)
        CRM_REPO.save_edited_payload(
            document_id=document_id,
            payload=payload,
            missing_fields=missing_fields,
        )
        CRM_REPO.update_document_fields(document_id, {"merge_candidates": merge_candidates})
        return JSONResponse(
            {
                "document_id": document_id,
                "missing_fields": missing_fields,
                "validation_issues": validation_issues,
                "payload": payload,
                "manual_steps_required": record.get("manual_steps_required", []),
                "identity_match_found": bool(record.get("identity_match_found")),
                "identity_source_document_id": _safe(record.get("identity_source_document_id")),
                "enrichment_preview": list(record.get("enrichment_preview") or []),
                "merge_candidates": merge_candidates,
            }
        )

    @app.post("/api/documents/{document_id}/enrich-by-identity")
    def enrich_by_identity(document_id: str, req: EnrichByIdentityRequest) -> JSONResponse:
        record = read_or_bootstrap_record(document_id)
        payload = record.get("payload") or {}
        if not isinstance(payload, dict):
            raise HTTPException(status_code=422, detail="Invalid payload in document record.")

        enrichment = enrich_record_payload_by_identity(
            document_id,
            payload,
            persist=bool(req.apply),
            source_document_id=_safe(req.source_document_id),
        )
        enriched_payload = enrichment.get("payload") if isinstance(enrichment.get("payload"), dict) else payload
        missing_fields = collect_validation_errors(enriched_payload, require_tramite=False)
        validation_issues = collect_validation_issues(enriched_payload, require_tramite=False)

        if not req.apply:
            merge_candidates = _merge_candidates_for_payload(document_id, payload, limit=10)
            return JSONResponse(
                {
                    "document_id": document_id,
                    "identity_match_found": bool(enrichment.get("identity_match_found")),
                    "identity_source_document_id": _safe(enrichment.get("identity_source_document_id")),
                    "identity_key": _safe(enrichment.get("identity_key")),
                    "applied_fields": enrichment.get("applied_fields", []),
                    "skipped_fields": enrichment.get("skipped_fields", []),
                    "enrichment_preview": enrichment.get("enrichment_preview", []),
                    "merge_candidates": merge_candidates,
                    "missing_fields": collect_validation_errors(payload, require_tramite=False),
                    "validation_issues": collect_validation_issues(payload, require_tramite=False),
                    "payload": payload,
                }
            )

        merge_candidates = _merge_candidates_for_payload(document_id, enriched_payload, limit=10)
        updated_record = read_or_bootstrap_record(document_id)
        updated_record["merge_candidates"] = merge_candidates
        _write_record(document_id, updated_record)
        CRM_REPO.update_document_fields(document_id, {"merge_candidates": merge_candidates})

        return JSONResponse(
            {
                "document_id": document_id,
                "identity_match_found": bool(enrichment.get("identity_match_found")),
                "identity_source_document_id": _safe(enrichment.get("identity_source_document_id")),
                "identity_key": _safe(enrichment.get("identity_key")),
                "applied_fields": enrichment.get("applied_fields", []),
                "skipped_fields": enrichment.get("skipped_fields", []),
                "enrichment_preview": enrichment.get("enrichment_preview", []),
                "merge_candidates": merge_candidates,
                "missing_fields": missing_fields,
                "validation_issues": validation_issues,
                "payload": enriched_payload,
            }
        )

    @app.post("/api/documents/{document_id}/browser-session/open")
    async def open_managed_browser_session(document_id: str, req: BrowserSessionOpenRequest) -> JSONResponse:
        record = read_or_bootstrap_record(document_id)
        target_url = (req.target_url or record.get("target_url") or record.get("form_url") or DEFAULT_TARGET_URL).strip()
        if not target_url:
            raise HTTPException(status_code=422, detail="Target URL is required.")
        prev_session_id = _safe(record.get("browser_session_id"))
        if prev_session_id:
            try:
                await _run_browser_call(close_browser_session, prev_session_id)
            except Exception:
                LOGGER.exception("Failed closing previous browser session: %s", prev_session_id)
        session = await _run_browser_call(
            open_browser_session,
            target_url,
            headless=req.headless,
            slowmo=req.slowmo,
            timeout_ms=req.timeout_ms,
        )
        record["target_url"] = target_url
        record["browser_session_id"] = session["session_id"]
        _write_record(document_id, record)
        CRM_REPO.set_browser_session(document_id, session["session_id"])
        return JSONResponse(
            {
                "document_id": document_id,
                "session_id": session["session_id"],
                "target_url": target_url,
                "current_url": session.get("current_url", ""),
                "alive": bool(session.get("alive", True)),
            }
        )

    @app.get("/api/documents/{document_id}/browser-session/state")
    async def browser_session_state(document_id: str) -> JSONResponse:
        record = read_or_bootstrap_record(document_id)
        session_id = _safe(record.get("browser_session_id"))
        if not session_id:
            raise HTTPException(status_code=404, detail="Browser session is not opened.")
        try:
            state = await _run_browser_call(get_browser_session_state, session_id)
        except Exception as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return JSONResponse(
            {
                "document_id": document_id,
                **state,
            }
        )

    @app.post("/api/documents/{document_id}/browser-session/fields/analyze")
    async def analyze_browser_session_fields(document_id: str, req: BrowserSessionAnalyzeRequest) -> JSONResponse:
        _ = document_id
        _ = req
        LOGGER.info("disabled endpoint called: /api/documents/{document_id}/browser-session/fields/analyze")
        raise HTTPException(status_code=404, detail="Mapper endpoints are disabled. Templates are managed manually via files.")

    @app.post("/api/documents/{document_id}/browser-session/fields/learn")
    async def learn_browser_session_field_mappings(document_id: str, req: BrowserSessionLearnRequest) -> JSONResponse:
        _ = document_id
        _ = req
        LOGGER.info("disabled endpoint called: /api/documents/{document_id}/browser-session/fields/learn")
        raise HTTPException(status_code=404, detail="Mapper endpoints are disabled. Templates are managed manually via files.")

    @app.post("/api/documents/{document_id}/browser-session/mapping/save")
    async def save_mapper_from_placeholders(document_id: str, req: BrowserSessionSaveMapperRequest) -> JSONResponse:
        _ = document_id
        _ = req
        LOGGER.info("disabled endpoint called: /api/documents/{document_id}/browser-session/mapping/save")
        raise HTTPException(status_code=404, detail="Mapper endpoints are disabled. Templates are managed manually via files.")

    @app.post("/api/documents/{document_id}/browser-session/mapping/upload")
    async def upload_mapper_template(document_id: str, req: BrowserSessionUploadMapperRequest) -> JSONResponse:
        _ = document_id
        _ = req
        LOGGER.info("disabled endpoint called: /api/documents/{document_id}/browser-session/mapping/upload")
        raise HTTPException(status_code=404, detail="Mapper endpoints are disabled. Templates are managed manually via files.")

    @app.post("/api/documents/{document_id}/browser-session/fill")
    async def fill_opened_browser_session(document_id: str, req: BrowserSessionFillRequest) -> JSONResponse:
        record = read_or_bootstrap_record(document_id)
        session_id = _safe(record.get("browser_session_id"))
        if not session_id:
            raise HTTPException(status_code=400, detail="Browser session is not opened. Click 'Перейти по адресу' first.")

        payload = req.payload or record.get("payload") or {}
        missing_fields = collect_validation_errors(payload, require_tramite=False)
        validation_issues = collect_validation_issues(payload, require_tramite=False)
        out_dir = AUTOFILL_DIR / document_id
        out_dir.mkdir(parents=True, exist_ok=True)
        fill_strategy = "strict_template"
        try:
            session_state = await _run_browser_call(get_browser_session_state, session_id)
            current_url = _safe(session_state.get("current_url"))
        except Exception:
            current_url = ""
        if not current_url:
            raise HTTPException(status_code=422, detail="Current URL is empty in browser session.")
        template = FORM_MAPPING_REPO.get_latest_for_url(current_url) if current_url else None
        if not template:
            return JSONResponse(
                status_code=422,
                content={
                    "status": "error",
                    "error_code": "TEMPLATE_NOT_FOUND",
                    "document_id": document_id,
                    "message": "Template mapping not found for current URL.",
                    "form_url": current_url,
                },
            )
        learned = list(template.get("mappings") or [])
        if not learned:
            return JSONResponse(
                status_code=422,
                content={
                    "status": "error",
                    "error_code": "TEMPLATE_INVALID",
                    "document_id": document_id,
                    "message": "Template has no mappings.",
                    "form_url": current_url,
                },
            )
        merged_map: dict[str, dict[str, Any]] = {}
        for item in learned:
            selector = _safe(item.get("selector"))
            if selector:
                merged_map[selector] = {
                    "selector": selector,
                    "canonical_key": _safe(item.get("canonical_key")),
                    "field_kind": _safe(item.get("field_kind")) or "text",
                    "match_value": _safe(item.get("match_value")),
                    "checked_when": _safe(item.get("checked_when")),
                    "source": _safe(item.get("source")) or "template",
                    "confidence": float(item.get("confidence") or 0.99),
                }
        effective_mappings = list(merged_map.values())
        LOGGER.info(
            "autofill.start document_id=%s session_id=%s url=%s template_source=%s mappings=%s missing_fields=%s validation_issues=%s",
            document_id,
            session_id,
            current_url,
            _safe(template.get("source")),
            len(effective_mappings),
            len(missing_fields),
            len(validation_issues),
        )

        try:
            result = await _run_browser_call(
                fill_browser_session,
                session_id,
                payload,
                out_dir,
                timeout_ms=req.timeout_ms,
                explicit_mappings=effective_mappings,
                fill_strategy=fill_strategy,
            )
        except Exception as exc:
            detail = str(exc) or exc.__class__.__name__
            screenshot_url = _latest_artifact_url(out_dir, "*.png") if should_save_artifact_screenshots_on_error() else ""
            dom_snapshot_url = _latest_artifact_url(out_dir, "*.html")
            record["payload"] = payload
            record["missing_fields"] = missing_fields
            record["autofill_preview"] = {
                "status": "error",
                "error": detail,
                "screenshot_url": screenshot_url,
                "dom_snapshot_url": dom_snapshot_url,
            }
            _write_record(document_id, record)
            CRM_REPO.save_edited_payload(
                document_id=document_id,
                payload=payload,
                missing_fields=missing_fields,
            )
            return JSONResponse(
                status_code=422,
                content={
                    "status": "error",
                    "error_code": "FILL_FAILED",
                    "document_id": document_id,
                    "message": "Fill in opened browser session failed.",
                    "form_url": record.get("target_url") or current_url,
                },
            )

        record["payload"] = payload
        record["missing_fields"] = missing_fields
        filled_fields = list(result.get("filled_fields", []) or [])
        mode = str(result.get("mode", "") or "")
        filled_pdf_url = _artifact_url_from_value(result.get("filled_pdf"))
        record["autofill_preview"] = {
            "status": "ok",
            "mode": mode,
            "screenshot": result.get("screenshot", ""),
            "dom_snapshot": result.get("dom_snapshot", ""),
            "filled_pdf": _safe(result.get("filled_pdf")),
            "warnings": result.get("warnings", []),
            "filled_fields": filled_fields,
        }
        _write_record(document_id, record)
        CRM_REPO.save_edited_payload(
            document_id=document_id,
            payload=payload,
            missing_fields=missing_fields,
        )

        screenshot_url = _artifact_url_from_value(result.get("screenshot"))
        dom_snapshot_url = _artifact_url_from_value(result.get("dom_snapshot"))
        LOGGER.info(
            "autofill.result document_id=%s mode=%s filled_fields=%s warnings=%s screenshot_url=%s dom_snapshot_url=%s filled_pdf_url=%s",
            document_id,
            mode,
            len(filled_fields),
            len(list(result.get("warnings", []) or [])),
            screenshot_url,
            dom_snapshot_url,
            filled_pdf_url,
        )
        if mode == "pdf_pymupdf" and len(filled_fields) == 0:
            return JSONResponse(
                status_code=422,
                content={
                    "status": "error",
                    "error_code": "FILL_PARTIAL",
                    "document_id": document_id,
                    "message": "PDF was processed, but no fillable fields were matched.",
                    "form_url": result.get("current_url") or record.get("target_url") or current_url,
                },
            )
        return JSONResponse(
            {
                "document_id": document_id,
                "form_url": result.get("current_url") or record.get("target_url") or current_url,
                "filled_pdf_url": filled_pdf_url,
            }
        )

    @app.post("/api/documents/{document_id}/browser-session/template")
    def resolve_template_for_client_browser(document_id: str, req: BrowserSessionTemplateRequest) -> JSONResponse:
        record = read_or_bootstrap_record(document_id)
        current_url = _safe(req.current_url)
        if not current_url:
            raise HTTPException(status_code=422, detail="current_url is required.")

        payload = req.payload or record.get("payload") or {}
        missing_fields = collect_validation_errors(payload, require_tramite=False)
        validation_issues = collect_validation_issues(payload, require_tramite=False)

        template = FORM_MAPPING_REPO.get_latest_for_url(current_url)
        if not template:
            return JSONResponse(
                status_code=422,
                content={
                    "status": "error",
                    "error_code": "TEMPLATE_NOT_FOUND",
                    "document_id": document_id,
                    "message": "Template mapping not found for current URL.",
                    "form_url": current_url,
                },
            )

        learned = list(template.get("mappings") or [])
        if not learned:
            return JSONResponse(
                status_code=422,
                content={
                    "status": "error",
                    "error_code": "TEMPLATE_INVALID",
                    "document_id": document_id,
                    "message": "Template has no mappings.",
                    "form_url": current_url,
                },
            )

        merged_map: dict[str, dict[str, Any]] = {}
        for item in learned:
            selector = _safe(item.get("selector"))
            if selector:
                merged_map[selector] = {
                    "selector": selector,
                    "canonical_key": _safe(item.get("canonical_key")),
                    "field_kind": _safe(item.get("field_kind")) or "text",
                    "match_value": _safe(item.get("match_value")),
                    "checked_when": _safe(item.get("checked_when")),
                    "source": _safe(item.get("source")) or "template",
                    "confidence": float(item.get("confidence") or 0.99),
                }

        return JSONResponse(
            {
                "document_id": document_id,
                "form_url": current_url,
                "fill_strategy": req.fill_strategy,
                "template_source": _safe(template.get("source")),
                "effective_mappings": list(merged_map.values()),
                "missing_fields": missing_fields,
                "validation_issues": validation_issues,
            }
        )

    @app.post("/api/documents/{document_id}/autofill-validate")
    def validate_autofill(document_id: str, req: AutofillValidateRequest) -> JSONResponse:
        _ = document_id
        _ = req
        LOGGER.info("disabled endpoint called: /api/documents/{document_id}/autofill-validate")
        raise HTTPException(status_code=404, detail="Validation endpoint is disabled.")

    @app.post("/api/documents/{document_id}/browser-session/close")
    async def close_managed_browser_session(document_id: str) -> JSONResponse:
        record = read_or_bootstrap_record(document_id)
        session_id = _safe(record.get("browser_session_id"))
        if session_id:
            try:
                await _run_browser_call(close_browser_session, session_id)
            except Exception:
                LOGGER.exception("Failed closing browser session: %s", session_id)
        record["browser_session_id"] = ""
        _write_record(document_id, record)
        CRM_REPO.set_browser_session(document_id, "")
        return JSONResponse({"document_id": document_id, "status": "closed"})

    return app


app = create_app()
