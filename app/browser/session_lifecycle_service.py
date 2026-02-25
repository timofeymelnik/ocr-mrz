"""Service for browser session open/state/close document lifecycle endpoints."""

from __future__ import annotations

from typing import Any, Awaitable, Callable, Protocol

from fastapi import HTTPException


class CRMRepositoryProtocol(Protocol):
    """Protocol for CRM browser session updates."""

    def set_browser_session(self, document_id: str, session_id: str) -> dict[str, Any]:
        """Persist browser session id for document."""


class BrowserSessionLifecycleService:
    """Open, check and close browser sessions linked to documents."""

    def __init__(
        self,
        *,
        default_target_url: str,
        read_or_bootstrap_record: Callable[[str], dict[str, Any]],
        write_record: Callable[[str, dict[str, Any]], None],
        safe_value: Callable[[Any], str],
        run_browser_call: Callable[..., Awaitable[Any]],
        open_browser_session: Callable[..., Any],
        get_browser_session_state: Callable[..., Any],
        close_browser_session: Callable[..., Any],
        crm_repo: CRMRepositoryProtocol,
        logger_exception: Callable[[str, Any], None],
    ) -> None:
        """Initialize service with explicit collaborators."""
        self._default_target_url = default_target_url
        self._read_or_bootstrap_record = read_or_bootstrap_record
        self._write_record = write_record
        self._safe_value = safe_value
        self._run_browser_call = run_browser_call
        self._open_browser_session = open_browser_session
        self._get_browser_session_state = get_browser_session_state
        self._close_browser_session = close_browser_session
        self._crm_repo = crm_repo
        self._logger_exception = logger_exception

    async def open_session(
        self,
        *,
        document_id: str,
        target_url: str | None,
        headless: bool,
        slowmo: int,
        timeout_ms: int,
    ) -> dict[str, Any]:
        """Open managed browser session for document target url."""
        record = self._read_or_bootstrap_record(document_id)
        resolved_target = (
            target_url
            or record.get("target_url")
            or record.get("form_url")
            or self._default_target_url
        )
        resolved_target = (
            resolved_target.strip() if isinstance(resolved_target, str) else ""
        )
        if not resolved_target:
            raise HTTPException(status_code=422, detail="Target URL is required.")

        previous_session_id = self._safe_value(record.get("browser_session_id"))
        if previous_session_id:
            try:
                await self._run_browser_call(
                    self._close_browser_session,
                    previous_session_id,
                )
            except Exception:
                self._logger_exception(
                    "Failed closing previous browser session: %s",
                    previous_session_id,
                )

        session = await self._run_browser_call(
            self._open_browser_session,
            resolved_target,
            headless=headless,
            slowmo=slowmo,
            timeout_ms=timeout_ms,
        )

        record["target_url"] = resolved_target
        record["browser_session_id"] = session["session_id"]
        self._write_record(document_id, record)
        self._crm_repo.set_browser_session(document_id, session["session_id"])

        return {
            "document_id": document_id,
            "session_id": session["session_id"],
            "target_url": resolved_target,
            "current_url": session.get("current_url", ""),
            "alive": bool(session.get("alive", True)),
        }

    async def get_state(self, *, document_id: str) -> dict[str, Any]:
        """Return state for active browser session."""
        record = self._read_or_bootstrap_record(document_id)
        session_id = self._safe_value(record.get("browser_session_id"))
        if not session_id:
            raise HTTPException(
                status_code=404, detail="Browser session is not opened."
            )

        try:
            state = await self._run_browser_call(
                self._get_browser_session_state, session_id
            )
        except Exception as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

        return {"document_id": document_id, **state}

    async def close_session(self, *, document_id: str) -> dict[str, Any]:
        """Close active browser session for document if present."""
        record = self._read_or_bootstrap_record(document_id)
        session_id = self._safe_value(record.get("browser_session_id"))
        if session_id:
            try:
                await self._run_browser_call(self._close_browser_session, session_id)
            except Exception:
                self._logger_exception(
                    "Failed closing browser session: %s",
                    session_id,
                )

        record["browser_session_id"] = ""
        self._write_record(document_id, record)
        self._crm_repo.set_browser_session(document_id, "")
        return {"document_id": document_id, "closed": True, "status": "closed"}
