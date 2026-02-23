from __future__ import annotations

import asyncio
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from pathlib import Path
from typing import Any, Optional

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from browser_session_manager import (
    close_browser_session,
    fill_browser_session,
    get_browser_session_state,
    inspect_browser_session_fields,
    open_browser_session,
)

load_dotenv()
LOGGER = logging.getLogger(__name__)

APP_ROOT = Path(__file__).resolve().parent
RUNTIME_DIR = APP_ROOT / "runtime"
AUTOFILL_DIR = RUNTIME_DIR / "autofill_client_agent"

for directory in [RUNTIME_DIR, AUTOFILL_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

_BROWSER_EXECUTOR = ThreadPoolExecutor(max_workers=1, thread_name_prefix="client-playwright-sync")


class BrowserSessionOpenRequest(BaseModel):
    target_url: str
    timeout_ms: int = 25000
    slowmo: int = 40
    headless: bool = False


class BrowserSessionFillRequest(BaseModel):
    payload: dict[str, Any]
    timeout_ms: int = 25000
    explicit_mappings: Optional[list[dict[str, Any]]] = None
    fill_strategy: str = "strict_template"
    document_id: Optional[str] = None


class BrowserSessionInspectRequest(BaseModel):
    payload: dict[str, Any]
    mapping_hints: Optional[dict[str, str]] = None


def _safe(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


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


async def _run_browser_call(fn, *args, **kwargs):
    loop = asyncio.get_running_loop()
    call = partial(fn, *args, **kwargs)
    return await loop.run_in_executor(_BROWSER_EXECUTOR, call)


def create_app() -> FastAPI:
    app = FastAPI(title="OCR MRZ Client Browser Agent", version="1.0.0")
    allowed_origins_env = os.getenv("CLIENT_AGENT_ALLOWED_ORIGINS", "").strip()
    allowed_origins = [v.strip() for v in allowed_origins_env.split(",") if v.strip()]
    if not allowed_origins:
        allowed_origins = [
            "http://localhost:3000",
            "http://127.0.0.1:3000",
            "http://192.168.1.145:3000",
        ]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=allowed_origins,
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.mount("/runtime", StaticFiles(directory=str(RUNTIME_DIR)), name="runtime")

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/api/browser-session/open")
    async def open_session(req: BrowserSessionOpenRequest) -> dict[str, Any]:
        target_url = _safe(req.target_url)
        if not target_url:
            raise HTTPException(status_code=422, detail="target_url is required.")
        try:
            return await _run_browser_call(
                open_browser_session,
                target_url,
                headless=req.headless,
                slowmo=req.slowmo,
                timeout_ms=req.timeout_ms,
            )
        except Exception as exc:
            raise HTTPException(status_code=422, detail=str(exc) or "Failed to open browser session.") from exc

    @app.get("/api/browser-session/{session_id}/state")
    async def session_state(session_id: str) -> dict[str, Any]:
        try:
            return await _run_browser_call(get_browser_session_state, session_id)
        except Exception as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.post("/api/browser-session/{session_id}/fields/inspect")
    async def inspect_fields(session_id: str, req: BrowserSessionInspectRequest) -> dict[str, Any]:
        try:
            return await _run_browser_call(
                inspect_browser_session_fields,
                session_id,
                req.payload,
                mapping_hints=req.mapping_hints,
            )
        except Exception as exc:
            raise HTTPException(status_code=422, detail=str(exc) or "Failed to inspect session fields.") from exc

    @app.post("/api/browser-session/{session_id}/fill")
    async def fill_session(session_id: str, req: BrowserSessionFillRequest) -> dict[str, Any]:
        out_key = _safe(req.document_id) or session_id
        out_dir = AUTOFILL_DIR / out_key
        out_dir.mkdir(parents=True, exist_ok=True)
        try:
            result = await _run_browser_call(
                fill_browser_session,
                session_id,
                req.payload,
                out_dir,
                timeout_ms=req.timeout_ms,
                explicit_mappings=req.explicit_mappings,
                fill_strategy=req.fill_strategy,
            )
        except Exception as exc:
            raise HTTPException(status_code=422, detail=str(exc) or "Failed to fill browser session.") from exc

        return {
            "session_id": result.get("session_id") or session_id,
            "current_url": result.get("current_url") or "",
            "mode": result.get("mode") or "",
            "filled_fields": list(result.get("filled_fields") or []),
            "warnings": list(result.get("warnings") or []),
            "screenshot_url": _artifact_url_from_value(result.get("screenshot")),
            "dom_snapshot_url": _artifact_url_from_value(result.get("dom_snapshot")),
            "filled_pdf_url": _artifact_url_from_value(result.get("filled_pdf")),
            "screenshot_path": _safe(result.get("screenshot")),
            "dom_snapshot_path": _safe(result.get("dom_snapshot")),
            "filled_pdf_path": _safe(result.get("filled_pdf")),
        }

    @app.post("/api/browser-session/{session_id}/close")
    async def close_session(session_id: str) -> dict[str, str]:
        try:
            await _run_browser_call(close_browser_session, session_id)
        except Exception as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"session_id": session_id, "status": "closed"}

    return app


app = create_app()
