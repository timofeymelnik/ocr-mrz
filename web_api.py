from __future__ import annotations

import json
import logging
import os
import re
import uuid
import asyncio
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from app.crm.router import create_crm_router
from app.crm.service import CRMService, build_record_from_crm
from app.documents.upload_service import UploadService
from app.documents.router import create_documents_router
from app.documents.service import DocumentsService
from app.browser.session_lifecycle_service import BrowserSessionLifecycleService
from app.auth.middleware import create_auth_middleware
from app.auth.repository import AuthRepository
from app.auth.router import create_auth_router
from app.auth.service import AuthService
from app.core.config import AppConfig
from app.crm.repository import CRMRepository
from app.mappings.repository import FormMappingRepository
from app.ocr_extract.ocr import VisionOCRClient
from app.pipeline.runner import attach_pipeline_metadata, stage_start, stage_success
from app.browser.session_manager import (
    close_browser_session,
    collect_browser_session_placeholder_mappings,
    fill_browser_session,
    get_browser_session_state,
    inspect_browser_session_fields,
    open_browser_session,
)
from app.data_builder.data_builder import build_tasa_document
from app.autofill.target_autofill import (
    CANONICAL_FIELD_KEYS,
    extract_pdf_placeholder_mappings_from_bytes,
    inspect_pdf_fields_from_bytes,
    should_save_artifact_screenshots_on_error,
    suggest_mappings_for_fields,
)
from app.core.validators import collect_validation_errors, collect_validation_issues, normalize_payload_for_form

load_dotenv()
LOGGER = logging.getLogger(__name__)
APP_CONFIG = AppConfig.from_env()

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


def _split_full_name_simple(value: str) -> tuple[str, str, str]:
    raw = _safe(value)
    if not raw:
        return "", "", ""
    if "," in raw:
        left, right = [x.strip() for x in raw.split(",", 1)]
        parts = [p for p in re.split(r"\s+", left) if p]
        return parts[0] if parts else "", " ".join(parts[1:]) if len(parts) > 1 else "", right
    parts = [p for p in re.split(r"\s+", raw) if p]
    if len(parts) == 1:
        return parts[0], "", ""
    if len(parts) == 2:
        return parts[0], "", parts[1]
    return parts[0], parts[1], " ".join(parts[2:])


def _family_reference_from_payload(payload: dict[str, Any]) -> dict[str, str]:
    refs = payload.get("referencias") if isinstance(payload.get("referencias"), dict) else {}
    fam = refs.get("familiar_que_da_derecho") if isinstance(refs.get("familiar_que_da_derecho"), dict) else {}
    if not fam:
        return {}
    nif_nie = _normalize_identity(_safe(fam.get("nif_nie")))
    pasaporte = _normalize_identity(_safe(fam.get("pasaporte")))
    nombre_apellidos = _safe(fam.get("nombre_apellidos"))
    primer_apellido = _safe(fam.get("primer_apellido"))
    nombre = _safe(fam.get("nombre"))
    if not nombre_apellidos:
        nombre_apellidos = " ".join(x for x in [primer_apellido, nombre] if x).strip()
    document_number = nif_nie or pasaporte
    if not document_number:
        return {}
    return {
        "document_number": document_number,
        "nif_nie": nif_nie,
        "pasaporte": pasaporte,
        "nombre_apellidos": nombre_apellidos,
        "primer_apellido": primer_apellido,
        "nombre": nombre,
    }


def _build_family_payload(family_ref: dict[str, str]) -> dict[str, Any]:
    first_last, second_last, first_name = _split_full_name_simple(_safe(family_ref.get("nombre_apellidos")))
    primer_apellido = _safe(family_ref.get("primer_apellido")) or first_last
    nombre = _safe(family_ref.get("nombre")) or first_name
    payload = {
        "identificacion": {
            "nif_nie": _safe(family_ref.get("nif_nie")),
            "pasaporte": _safe(family_ref.get("pasaporte")),
            "documento_tipo": "pasaporte" if _safe(family_ref.get("pasaporte")) and not _safe(family_ref.get("nif_nie")) else "nif_tie_nie_dni",
            "nombre_apellidos": _safe(family_ref.get("nombre_apellidos")),
            "primer_apellido": primer_apellido,
            "segundo_apellido": second_last,
            "nombre": nombre,
        },
        "domicilio": {},
        "autoliquidacion": {"tipo": "principal", "num_justificante": "", "importe_complementaria": None},
        "tramite": {},
        "declarante": {},
        "ingreso": {"forma_pago": "efectivo", "iban": ""},
        "extra": {},
        "captcha": {"manual": True},
        "download": {"dir": "./downloads", "filename_prefix": "family_related"},
    }
    return normalize_payload_for_form(payload)


def _merge_family_links(existing: list[dict[str, Any]], new_link: dict[str, Any]) -> list[dict[str, Any]]:
    links: list[dict[str, Any]] = [x for x in existing if isinstance(x, dict)]
    key = (
        _safe(new_link.get("related_document_id")),
        _safe(new_link.get("relation")),
        _safe(new_link.get("document_number")),
    )
    for row in links:
        row_key = (
            _safe(row.get("related_document_id")),
            _safe(row.get("relation")),
            _safe(row.get("document_number")),
        )
        if row_key == key:
            return links
    links.append(new_link)
    return links


def _sync_family_reference(document_id: str, payload: dict[str, Any], source: dict[str, Any]) -> dict[str, Any]:
    family_ref = _family_reference_from_payload(payload)
    if not family_ref:
        return {"linked": False, "family_links": []}

    family_payload = _build_family_payload(family_ref)
    identity_keys = [v for v in [_safe(family_ref.get("nif_nie")), _safe(family_ref.get("pasaporte"))] if v]
    linked_doc = CRM_REPO.find_latest_by_identities(identity_keys, exclude_document_id=document_id)
    related_document_id = _safe((linked_doc or {}).get("document_id"))
    created = False

    if related_document_id:
        existing_payload = (
            linked_doc.get("effective_payload")
            or linked_doc.get("edited_payload")
            or linked_doc.get("ocr_payload")
            or {}
        )
        if isinstance(existing_payload, dict):
            merged_payload, applied, _ = _enrich_payload_fill_empty(
                payload=existing_payload,
                source_payload=family_payload,
                source_document_id=document_id,
            )
            if applied:
                CRM_REPO.save_edited_payload(
                    document_id=related_document_id,
                    payload=merged_payload,
                    missing_fields=collect_validation_errors(merged_payload, require_tramite=False),
                )
    else:
        related_document_id = uuid.uuid4().hex
        created = True
        CRM_REPO.upsert_from_upload(
            document_id=related_document_id,
            payload=family_payload,
            ocr_document={},
            source={
                "source_kind": "family_reference_auto",
                "origin_document_id": document_id,
                "original_filename": _safe(source.get("original_filename")),
                "stored_path": _safe(source.get("stored_path")),
                "preview_url": _safe(source.get("preview_url")),
            },
            missing_fields=collect_validation_errors(family_payload, require_tramite=False),
            manual_steps_required=["verify_filled_fields", "submit_or_download_manually"],
            form_url=DEFAULT_TARGET_URL,
            target_url=DEFAULT_TARGET_URL,
        )

    forward_link = {
        "relation": "familiar_que_da_derecho",
        "related_document_id": related_document_id,
        "document_number": _safe(family_ref.get("document_number")),
        "created_from_reference": created,
    }
    backward_link = {
        "relation": "titular_familiar_dependiente",
        "related_document_id": document_id,
        "document_number": _safe(_identity_candidates(payload)[0] if _identity_candidates(payload) else ""),
        "created_from_reference": False,
    }

    primary_doc = CRM_REPO.get_document(document_id) or {}
    primary_links = _merge_family_links(primary_doc.get("family_links") or [], forward_link)
    CRM_REPO.update_document_fields(document_id, {"family_links": primary_links})

    if related_document_id:
        related_doc = CRM_REPO.get_document(related_document_id) or {}
        related_links = _merge_family_links(related_doc.get("family_links") or [], backward_link)
        CRM_REPO.update_document_fields(related_document_id, {"family_links": related_links})

    return {
        "linked": True,
        "related_document_id": related_document_id,
        "created": created,
        "family_links": primary_links,
        "family_reference": family_ref,
    }


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
    auth_repo = AuthRepository(APP_ROOT)
    auth_service = AuthService(auth_repo, APP_CONFIG.auth)
    auth_service.bootstrap_admin_user()
    app.include_router(create_auth_router(auth_service))
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

    documents_service = DocumentsService(
        crm_repo=CRM_REPO,
        read_or_bootstrap_record=read_or_bootstrap_record,
        write_record=_write_record,
        merge_candidates_for_payload=lambda document_id, payload, limit: _merge_candidates_for_payload(
            document_id, payload, limit=limit
        ),
        collect_validation_errors=lambda payload, require_tramite: collect_validation_errors(
            payload, require_tramite=require_tramite
        ),
        collect_validation_issues=lambda payload, require_tramite: collect_validation_issues(
            payload, require_tramite=require_tramite
        ),
        sync_family_reference=_sync_family_reference,
        enrich_record_payload_by_identity=lambda document_id, payload, persist, source_document_id: enrich_record_payload_by_identity(
            document_id,
            payload,
            persist=persist,
            source_document_id=source_document_id,
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
        merge_candidates_for_payload=lambda document_id, payload, limit: _merge_candidates_for_payload(
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
        sync_family_reference=_sync_family_reference,
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
        crm_repo=CRM_REPO,
        logger_exception=LOGGER.exception,
    )

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

    @app.post("/api/documents/upload")
    async def upload_document(
        file: UploadFile = File(...),
        tasa_code: str = Form(default="790_012"),
        source_kind: str = Form(...),
    ) -> JSONResponse:
        payload = await upload_service.upload_document(
            file=file,
            tasa_code=tasa_code,
            source_kind=source_kind,
        )
        return JSONResponse(payload)

    @app.post("/api/documents/{document_id}/browser-session/open")
    async def open_managed_browser_session(document_id: str, req: BrowserSessionOpenRequest) -> JSONResponse:
        payload = await browser_lifecycle_service.open_session(
            document_id=document_id,
            target_url=req.target_url,
            headless=req.headless,
            slowmo=req.slowmo,
            timeout_ms=req.timeout_ms,
        )
        return JSONResponse(payload)

    @app.get("/api/documents/{document_id}/browser-session/state")
    async def browser_session_state(document_id: str) -> JSONResponse:
        payload = await browser_lifecycle_service.get_state(document_id=document_id)
        return JSONResponse(payload)

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
        payload = await browser_lifecycle_service.close_session(document_id=document_id)
        return JSONResponse(payload)

    return app


app = create_app()
