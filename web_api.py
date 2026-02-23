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
    ) -> dict[str, Any]:
        identity_key = _normalize_identity(_safe_payload_get(payload, "identificacion.nif_nie"))
        if not identity_key:
            return {
                "identity_match_found": False,
                "identity_source_document_id": "",
                "identity_key": "",
                "enrichment_preview": [],
                "applied_fields": [],
                "skipped_fields": [],
                "payload": payload,
            }

        source_record = CRM_REPO.find_latest_by_identity(identity_key, exclude_document_id=document_id)
        if not source_record:
            return {
                "identity_match_found": False,
                "identity_source_document_id": "",
                "identity_key": identity_key,
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
        filename = file.filename or ""
        if Path(filename).suffix.lower() != ".pdf":
            raise HTTPException(status_code=400, detail="Only .pdf is supported for mapper draft generation.")
        data = await file.read()
        if not data:
            raise HTTPException(status_code=400, detail="Uploaded file is empty.")
        try:
            fields = inspect_pdf_fields_from_bytes(data)
            suggestions = suggest_mappings_for_fields(fields, payload={}, mapping_hints={})
        except Exception as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        field_by_selector = {_safe(f.get("selector")): f for f in fields}

        def _signal(*parts: str) -> str:
            return "".join(_safe(p).lower() for p in parts)

        mappings: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        for item in sorted(suggestions, key=lambda x: float(x.get("confidence") or 0.0), reverse=True):
            selector = _safe(item.get("selector"))
            key = _safe(item.get("canonical_key"))
            conf = float(item.get("confidence") or 0.0)
            if not selector or not key:
                continue
            field = field_by_selector.get(selector) or {}
            sig = _signal(selector, _safe(field.get("label")), _safe(field.get("name")), _safe(field.get("id")))
            ftype = _safe(field.get("type")).lower()

            # Strong filters to avoid noisy auto mappings.
            if conf < 0.75:
                continue
            if key == "nif_nie" and not any(t in sig for t in ["nie", "nif", "dninie", "pasaport"]):
                continue
            if key == "cp" and not any(t in sig for t in ["cp", "postal"]):
                continue
            if key == "nombre_apellidos" and not ("nombre" in sig and "apellido" in sig):
                continue
            if key in {"sexo", "estado_civil", "hijos_escolarizacion_espana"} and "check" not in ftype:
                continue
            if key in {"nif_nie_prefix", "nif_nie_number", "nif_nie_suffix"} and "text" not in ftype:
                continue
            tag = (selector, key)
            if tag in seen:
                continue
            seen.add(tag)
            mappings.append(
                {
                    "selector": selector,
                    "canonical_key": key,
                    "field_kind": "text",
                    "match_value": "",
                    "checked_when": "",
                    "source": "pdf_inspect_auto",
                    "confidence": conf,
                }
            )
        return JSONResponse(
            {
                "status": "ok",
                "fields_count": len(fields),
                "mappings_count": len(mappings),
                "mappings": mappings,
            }
        )

    @app.post("/api/documents/{document_id}/browser-session/mapping/import-template-pdf")
    async def import_pdf_template_mapper(
        document_id: str,
        file: UploadFile = File(...),
        overwrite: bool = Form(default=False),
    ) -> JSONResponse:
        record = read_or_bootstrap_record(document_id)
        session_id = _safe(record.get("browser_session_id"))
        if not session_id:
            raise HTTPException(status_code=400, detail="Browser session is not opened. Click 'Перейти по адресу' first.")

        filename = file.filename or ""
        if Path(filename).suffix.lower() != ".pdf":
            raise HTTPException(status_code=400, detail="Template mapper source must be a .pdf file.")
        raw = await file.read()
        if not raw:
            raise HTTPException(status_code=400, detail="Uploaded template PDF is empty.")

        try:
            mappings, unknown_vars = extract_pdf_placeholder_mappings_from_bytes(raw)
        except Exception as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

        if not mappings:
            raise HTTPException(
                status_code=422,
                detail="No placeholders found in template PDF. Put values like {nombre}, {cp}, {nif_nie}.",
            )

        warnings: list[str] = []
        unique_keys = {
            "cp",
            "piso",
            "provincia",
            "municipio",
            "nombre_madre",
            "nombre_padre",
            "email",
            "lugar_nacimiento",
            "primer_apellido",
            "segundo_apellido",
            "nombre",
            "pais_nacimiento",
            "nacionalidad",
            "telefono",
        }
        grouped: dict[str, list[dict[str, Any]]] = {}
        for item in mappings:
            key = _safe(item.get("canonical_key"))
            if key:
                grouped.setdefault(key, []).append(item)
        filtered: list[dict[str, Any]] = []
        for key, rows in grouped.items():
            if key not in unique_keys or len(rows) == 1:
                filtered.extend(rows)
                continue
            preferred = sorted(
                rows,
                key=lambda r: (
                    0 if "textfield" not in _safe(r.get("selector")).lower() else 1,
                    -float(r.get("confidence") or 0.0),
                    _safe(r.get("selector")),
                ),
            )[0]
            filtered.append(preferred)
            dropped = [r for r in rows if r is not preferred]
            warnings.append(
                f"Canonical key '{key}' had {len(rows)} candidates; kept '{_safe(preferred.get('selector'))}', "
                f"dropped {len(dropped)}."
            )
        mappings = filtered

        try:
            state = await _run_browser_call(get_browser_session_state, session_id)
            current_url = _safe(state.get("current_url"))
            analyzed = await _run_browser_call(inspect_browser_session_fields, session_id, payload={}, mapping_hints={})
            fields = list(analyzed.get("fields") or [])
        except Exception as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

        existing = FORM_MAPPING_REPO.get_latest_for_url(current_url)
        if existing and not overwrite:
            raise HTTPException(
                status_code=409,
                detail="Mapper already exists for this URL. Use overwrite=true to replace it.",
            )

        template = FORM_MAPPING_REPO.save_template(
            target_url=current_url,
            fields=fields,
            mappings=mappings,
            template_pdf_bytes=raw,
            source="template_pdf",
        )
        attach_template_artifacts_to_record(record, template)
        _write_record(document_id, record)
        return JSONResponse(
            {
                "document_id": document_id,
                "session_id": session_id,
                "current_url": current_url,
                "saved": True,
                "overwrite": bool(overwrite),
                "mappings_count": len(mappings),
                "mapping_source": "template_pdf",
                "unknown_vars": unknown_vars,
                "warnings": warnings,
                "template": template,
            }
        )

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

    @app.post("/api/documents/upload")
    async def upload_document(
        file: UploadFile = File(...),
        tasa_code: str = Form(default="790_012"),
    ) -> JSONResponse:
        if not file.filename or not _allowed_suffix(file.filename):
            raise HTTPException(status_code=400, detail="Only .jpg/.jpeg/.png/.pdf are supported.")

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
        enrichment = enrich_record_payload_by_identity(document_id, payload, persist=False)
        payload = enrichment.get("payload") if isinstance(enrichment.get("payload"), dict) else payload
        missing_fields = collect_validation_errors(payload, require_tramite=False)
        validation_issues = collect_validation_issues(payload, require_tramite=False)

        record = {
            "document_id": document_id,
            "tasa_code": tasa_code,
            "source": {
                "original_filename": file.filename,
                "stored_path": str(upload_path),
                "preview_url": _runtime_url(upload_path),
            },
            "document": document,
            "payload": payload,
            "missing_fields": missing_fields,
            "validation_issues": validation_issues,
            "manual_steps_required": ["verify_filled_fields", "submit_or_download_manually"],
            "form_url": DEFAULT_TARGET_URL,
            "target_url": DEFAULT_TARGET_URL,
            "identity_match_found": bool(enrichment.get("identity_match_found")),
            "identity_source_document_id": _safe(enrichment.get("identity_source_document_id")),
            "enrichment_preview": list(enrichment.get("enrichment_preview") or []),
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
            }
        )

    @app.get("/api/documents/{document_id}")
    def get_document(document_id: str) -> JSONResponse:
        record = read_or_bootstrap_record(document_id)
        return JSONResponse(record)

    @app.post("/api/documents/{document_id}/confirm")
    def confirm_document(document_id: str, req: ConfirmRequest) -> JSONResponse:
        record = read_or_bootstrap_record(document_id)
        payload = req.payload
        enrichment = enrich_record_payload_by_identity(document_id, payload, persist=False)
        payload = enrichment.get("payload") if isinstance(enrichment.get("payload"), dict) else payload
        missing_fields = collect_validation_errors(payload, require_tramite=False)
        validation_issues = collect_validation_issues(payload, require_tramite=False)
        record["payload"] = payload
        record["missing_fields"] = missing_fields
        record["identity_match_found"] = bool(enrichment.get("identity_match_found"))
        record["identity_source_document_id"] = _safe(enrichment.get("identity_source_document_id"))
        record["enrichment_preview"] = list(enrichment.get("enrichment_preview") or [])
        _write_record(document_id, record)
        CRM_REPO.save_edited_payload(
            document_id=document_id,
            payload=payload,
            missing_fields=missing_fields,
        )
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
            }
        )

    @app.post("/api/documents/{document_id}/enrich-by-identity")
    def enrich_by_identity(document_id: str, req: EnrichByIdentityRequest) -> JSONResponse:
        record = read_or_bootstrap_record(document_id)
        payload = record.get("payload") or {}
        if not isinstance(payload, dict):
            raise HTTPException(status_code=422, detail="Invalid payload in document record.")

        enrichment = enrich_record_payload_by_identity(document_id, payload, persist=bool(req.apply))
        enriched_payload = enrichment.get("payload") if isinstance(enrichment.get("payload"), dict) else payload
        missing_fields = collect_validation_errors(enriched_payload, require_tramite=False)
        validation_issues = collect_validation_issues(enriched_payload, require_tramite=False)

        if not req.apply:
            return JSONResponse(
                {
                    "document_id": document_id,
                    "identity_match_found": bool(enrichment.get("identity_match_found")),
                    "identity_source_document_id": _safe(enrichment.get("identity_source_document_id")),
                    "identity_key": _safe(enrichment.get("identity_key")),
                    "applied_fields": enrichment.get("applied_fields", []),
                    "skipped_fields": enrichment.get("skipped_fields", []),
                    "enrichment_preview": enrichment.get("enrichment_preview", []),
                    "missing_fields": missing_fields,
                    "validation_issues": validation_issues,
                    "payload": enriched_payload,
                }
            )

        return JSONResponse(
            {
                "document_id": document_id,
                "identity_match_found": bool(enrichment.get("identity_match_found")),
                "identity_source_document_id": _safe(enrichment.get("identity_source_document_id")),
                "identity_key": _safe(enrichment.get("identity_key")),
                "applied_fields": enrichment.get("applied_fields", []),
                "skipped_fields": enrichment.get("skipped_fields", []),
                "enrichment_preview": enrichment.get("enrichment_preview", []),
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
        record = read_or_bootstrap_record(document_id)
        session_id = _safe(record.get("browser_session_id"))
        if not session_id:
            raise HTTPException(status_code=400, detail="Browser session is not opened. Click 'Перейти по адресу' first.")

        payload = req.payload or record.get("payload") or {}
        try:
            session_state = await _run_browser_call(get_browser_session_state, session_id)
            current_url = _safe(session_state.get("current_url"))
            template = FORM_MAPPING_REPO.get_latest_for_url(current_url)
            hint_map: dict[str, str] = {}
            if template:
                for item in template.get("mappings") or []:
                    selector = _safe(item.get("selector"))
                    key = _safe(item.get("canonical_key"))
                    if selector and key:
                        hint_map[selector] = key
            analyzed = await _run_browser_call(inspect_browser_session_fields, session_id, payload, mapping_hints=hint_map)
        except Exception as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

        return JSONResponse(
            {
                "document_id": document_id,
                "session_id": session_id,
                "current_url": analyzed.get("current_url", ""),
                "fields": analyzed.get("fields", []),
                "suggestions": analyzed.get("suggestions", []),
                "template_mappings": list((template or {}).get("mappings") or []),
                "canonical_keys": CANONICAL_FIELD_KEYS,
                "template_loaded": bool(template),
            }
        )

    @app.post("/api/documents/{document_id}/browser-session/fields/learn")
    async def learn_browser_session_field_mappings(document_id: str, req: BrowserSessionLearnRequest) -> JSONResponse:
        record = read_or_bootstrap_record(document_id)
        session_id = _safe(record.get("browser_session_id"))
        if not session_id:
            raise HTTPException(status_code=400, detail="Browser session is not opened. Click 'Перейти по адресу' first.")
        try:
            session_state = await _run_browser_call(get_browser_session_state, session_id)
            current_url = _safe(session_state.get("current_url"))
            template = FORM_MAPPING_REPO.save_template(
                target_url=current_url,
                fields=req.fields or [],
                mappings=req.mappings or [],
                source="learned",
            )
            attach_template_artifacts_to_record(record, template)
            _write_record(document_id, record)
        except Exception as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return JSONResponse(
            {
                "document_id": document_id,
                "session_id": session_id,
                "current_url": current_url,
                "saved": True,
                "template": template,
            }
        )

    @app.post("/api/documents/{document_id}/browser-session/mapping/save")
    async def save_mapper_from_placeholders(document_id: str, req: BrowserSessionSaveMapperRequest) -> JSONResponse:
        record = read_or_bootstrap_record(document_id)
        session_id = _safe(record.get("browser_session_id"))
        if not session_id:
            raise HTTPException(status_code=400, detail="Browser session is not opened. Click 'Перейти по адресу' first.")
        try:
            collected = await _run_browser_call(collect_browser_session_placeholder_mappings, session_id, timeout_ms=25000)
            current_url = _safe(collected.get("current_url"))
            fields = list(collected.get("fields") or [])
            mappings = list(collected.get("mappings") or [])
            unknown_vars = list(collected.get("unknown_vars") or [])
        except Exception as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

        mapping_source = "placeholder"
        if not mappings:
            # Optional: explicit fallback for PDF when placeholders cannot be extracted from viewer.
            if ".pdf" in current_url.lower() and req.use_auto_pdf_fallback:
                payload = record.get("payload") or {}
                analyzed = await _run_browser_call(inspect_browser_session_fields, session_id, payload, mapping_hints={})
                auto_suggestions = list(analyzed.get("suggestions") or [])
                mappings = [
                    {
                        "selector": _safe(item.get("selector")),
                        "canonical_key": _safe(item.get("canonical_key")),
                        "field_kind": "text",
                        "match_value": "",
                        "checked_when": "",
                        "source": "auto_label",
                        "confidence": float(item.get("confidence") or 0.0),
                    }
                    for item in auto_suggestions
                    if _safe(item.get("selector"))
                    and _safe(item.get("canonical_key"))
                    and float(item.get("confidence") or 0.0) >= 0.6
                ]
                if not mappings:
                    raise HTTPException(
                        status_code=422,
                        detail=(
                            "No mapper placeholders found and auto-PDF mapping also failed. "
                            "Try typing placeholders into real PDF form fields or share this PDF for a specific adapter."
                        ),
                    )
                mapping_source = "auto_label_pdf_fallback"
            else:
                raise HTTPException(
                    status_code=422,
                    detail=(
                        "No mapper placeholders found. Fill fields with variables like {nombre}, {primer_apellido}, {cp}. "
                        "If needed, enable use_auto_pdf_fallback=true explicitly."
                    ),
                )

        existing = FORM_MAPPING_REPO.get_latest_for_url(current_url)
        if existing and not req.overwrite:
            raise HTTPException(
                status_code=409,
                detail="Mapper already exists for this URL. Use overwrite=true to replace it.",
            )

        template = FORM_MAPPING_REPO.save_template(
            target_url=current_url,
            fields=fields,
            mappings=mappings,
            source=mapping_source,
        )
        attach_template_artifacts_to_record(record, template)
        _write_record(document_id, record)
        return JSONResponse(
            {
                "document_id": document_id,
                "session_id": session_id,
                "current_url": current_url,
                "saved": True,
                "overwrite": bool(req.overwrite),
                "mappings_count": len(mappings),
                "mapping_source": mapping_source,
                "unknown_vars": unknown_vars,
                "template": template,
            }
        )

    @app.post("/api/documents/{document_id}/browser-session/mapping/upload")
    async def upload_mapper_template(document_id: str, req: BrowserSessionUploadMapperRequest) -> JSONResponse:
        record = read_or_bootstrap_record(document_id)
        session_id = _safe(record.get("browser_session_id"))
        if not session_id:
            raise HTTPException(status_code=400, detail="Browser session is not opened. Click 'Перейти по адресу' first.")

        mappings = [
            {
                "selector": _safe(item.get("selector")),
                "canonical_key": _safe(item.get("canonical_key")),
                "field_kind": _safe(item.get("field_kind")) or "text",
                "match_value": _safe(item.get("match_value")),
                "checked_when": _safe(item.get("checked_when")),
                "source": "uploaded_file",
                "confidence": float(item.get("confidence") or 1.0),
            }
            for item in (req.mappings or [])
            if _safe(item.get("selector"))
        ]
        if not mappings:
            raise HTTPException(status_code=422, detail="Mapper file has no valid mappings.")

        try:
            session_state = await _run_browser_call(get_browser_session_state, session_id)
            current_url = _safe(session_state.get("current_url"))
            analyzed = await _run_browser_call(inspect_browser_session_fields, session_id, payload={}, mapping_hints={})
            fields = list(analyzed.get("fields") or [])
        except Exception as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

        existing = FORM_MAPPING_REPO.get_latest_for_url(current_url)
        if existing and not req.overwrite:
            raise HTTPException(
                status_code=409,
                detail="Mapper already exists for this URL. Use overwrite=true to replace it.",
            )
        template = FORM_MAPPING_REPO.save_template(
            target_url=current_url,
            fields=fields,
            mappings=mappings,
            source="uploaded_file",
        )
        attach_template_artifacts_to_record(record, template)
        _write_record(document_id, record)
        return JSONResponse(
            {
                "document_id": document_id,
                "session_id": session_id,
                "current_url": current_url,
                "saved": True,
                "overwrite": bool(req.overwrite),
                "mappings_count": len(mappings),
                "mapping_source": "uploaded_file",
                "template": template,
            }
        )

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
        explicit_mappings = req.explicit_mappings or []
        fill_strategy = _safe(req.fill_strategy).lower() or "strict_template"
        if fill_strategy not in {"strict_template", "heuristic_fallback"}:
            raise HTTPException(status_code=422, detail="fill_strategy must be strict_template or heuristic_fallback.")
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
                    "message": "Template mapping is required for strict autofill.",
                    "detail": "Create/save mapper for current URL, then retry fill.",
                    "form_url": current_url,
                    "missing_fields": missing_fields,
                    "validation_issues": validation_issues,
                    "manual_steps_required": ["analyze_fields", "save_template", "retry_fill"],
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
                    "detail": "Update template mappings and retry.",
                    "form_url": current_url,
                    "missing_fields": missing_fields,
                    "validation_issues": validation_issues,
                    "manual_steps_required": ["update_template", "retry_fill"],
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
        for item in explicit_mappings:
            selector = _safe(item.get("selector"))
            if selector:
                merged_map[selector] = {
                    "selector": selector,
                    "canonical_key": _safe(item.get("canonical_key")),
                    "field_kind": _safe(item.get("field_kind")) or "text",
                    "match_value": _safe(item.get("match_value")),
                    "checked_when": _safe(item.get("checked_when")),
                    "source": _safe(item.get("source")) or "user",
                    "confidence": float(item.get("confidence") or 1.0),
                }
        effective_mappings = list(merged_map.values())

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
                    "detail": detail,
                    "form_url": record.get("target_url") or current_url,
                    "missing_fields": missing_fields,
                    "validation_issues": validation_issues,
                    "manual_steps_required": ["verify_fields", "continue_manually_if_needed"],
                    "screenshot_url": screenshot_url,
                    "dom_snapshot_url": dom_snapshot_url,
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
        if mode == "pdf_pymupdf" and len(filled_fields) == 0:
            return JSONResponse(
                status_code=422,
                content={
                    "status": "error",
                    "error_code": "FILL_PARTIAL",
                    "document_id": document_id,
                    "message": "PDF was processed, but no fillable fields were matched.",
                    "detail": "Run field analysis, map pdf:* fields manually, then retry fill.",
                    "form_url": result.get("current_url") or record.get("target_url") or current_url,
                    "mode": mode,
                    "warnings": result.get("warnings", []),
                    "filled_fields": filled_fields,
                    "filled_pdf_url": filled_pdf_url,
                    "missing_fields": missing_fields,
                    "validation_issues": validation_issues,
                    "manual_steps_required": ["map_pdf_fields", "retry_fill"],
                    "screenshot_url": screenshot_url,
                    "dom_snapshot_url": dom_snapshot_url,
                },
            )
        return JSONResponse(
            {
                "document_id": document_id,
                "form_url": result.get("current_url") or record.get("target_url") or current_url,
                "mode": mode,
                "warnings": result.get("warnings", []),
                "filled_fields": filled_fields,
                "applied_mappings": result.get("applied_mappings", []),
                "filled_pdf_url": filled_pdf_url,
                "missing_fields": missing_fields,
                "validation_issues": validation_issues,
                "manual_steps_required": ["verify_filled_fields", "submit_or_download_manually"],
                "screenshot_url": screenshot_url,
                "dom_snapshot_url": dom_snapshot_url,
            }
        )

    @app.post("/api/documents/{document_id}/autofill-validate")
    def validate_autofill(document_id: str, req: AutofillValidateRequest) -> JSONResponse:
        record = read_or_bootstrap_record(document_id)
        payload = record.get("payload") or {}
        if not isinstance(payload, dict):
            raise HTTPException(status_code=422, detail="Invalid payload in document record.")

        target_url = _safe(record.get("target_url") or record.get("form_url") or "")
        if not target_url:
            raise HTTPException(status_code=422, detail="Target URL is missing for this document.")

        resolved_filled = _resolve_runtime_path(req.filled_pdf_path or req.filled_pdf_url or "")
        if not resolved_filled:
            preview = record.get("autofill_preview") or {}
            fallback = _safe(preview.get("filled_pdf") or preview.get("dom_snapshot"))
            resolved_filled = _resolve_runtime_path(fallback)

        if not resolved_filled.exists():
            raise HTTPException(status_code=404, detail=f"Filled PDF not found: {resolved_filled}")

        template = FORM_MAPPING_REPO.get_latest_for_url(target_url)
        if not template:
            raise HTTPException(status_code=404, detail="Template mapping not found for target URL.")

        mappings = [m for m in list(template.get("mappings") or []) if _safe(m.get("canonical_key"))]
        if not mappings:
            raise HTTPException(status_code=422, detail="Template mapping has no mappings.")

        try:
            report = validate_filled_pdf_against_mapping(
                payload=payload,
                filled_pdf_path=resolved_filled,
                mappings=mappings,
            )
        except Exception as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

        report["document_id"] = document_id
        report["filled_pdf_path"] = str(resolved_filled)
        report["filled_pdf_url"] = _artifact_url_from_value(resolved_filled)
        report["template_updated_at"] = _safe(template.get("updated_at"))
        report["template_source"] = _safe(template.get("source"))
        return JSONResponse(report)

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
