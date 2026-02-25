"""Durable async task queue with retries, dead-letter, TTL and idempotency."""

from __future__ import annotations

import asyncio
import json
import sqlite3
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Any, Awaitable, Callable

from app.core.migrations import apply_migrations

TaskHandler = Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]

TASK_STATUS_QUEUED = "queued"
TASK_STATUS_RUNNING = "running"
TASK_STATUS_RETRYING = "retrying"
TASK_STATUS_COMPLETED = "completed"
TASK_STATUS_FAILED = "failed"
TASK_STATUS_DEAD_LETTER = "dead_letter"
TERMINAL_TASK_STATUSES = {
    TASK_STATUS_COMPLETED,
    TASK_STATUS_FAILED,
    TASK_STATUS_DEAD_LETTER,
}


@dataclass(frozen=True)
class QueueSettings:
    """Queue runtime settings."""

    database_path: Path
    default_ttl_seconds: int = 24 * 60 * 60
    default_max_retries: int = 3
    default_retry_delay_seconds: int = 5
    worker_poll_interval_seconds: float = 0.5


class TaskQueue:
    """SQLite-backed task queue that survives process restarts."""

    def __init__(self, settings: QueueSettings) -> None:
        """Initialize durable queue and ensure database schema is migrated."""
        self._settings = settings
        apply_migrations(settings.database_path)
        self._connection = sqlite3.connect(
            str(settings.database_path),
            check_same_thread=False,
        )
        self._connection.row_factory = sqlite3.Row
        self._lock = Lock()
        self._handlers: dict[str, TaskHandler] = {}
        self._worker_task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()

    def register_handler(self, task_type: str, handler: TaskHandler) -> None:
        """Register async task handler by task type."""
        normalized_type = task_type.strip().lower()
        if not normalized_type:
            raise ValueError("task_type is required")
        self._handlers[normalized_type] = handler

    async def start(self) -> None:
        """Start background worker loop if not already running."""
        if self._worker_task and not self._worker_task.done():
            return
        self._stop_event.clear()
        self._worker_task = asyncio.create_task(self._worker_loop())

    async def stop(self) -> None:
        """Stop background worker loop gracefully."""
        self._stop_event.set()
        if self._worker_task:
            await self._worker_task
            self._worker_task = None

    def close(self) -> None:
        """Close SQLite connection resources."""
        with self._lock:
            self._connection.close()

    def submit(
        self,
        *,
        task_type: str,
        payload: dict[str, Any],
        idempotency_key: str = "",
        ttl_seconds: int | None = None,
        max_retries: int | None = None,
        retry_delay_seconds: int | None = None,
    ) -> str:
        """Enqueue task and return task identifier with idempotency de-dup."""
        now = int(time.time())
        ttl = int(ttl_seconds or self._settings.default_ttl_seconds)
        retries = max(0, int(max_retries or self._settings.default_max_retries))
        retry_delay = max(
            1,
            int(retry_delay_seconds or self._settings.default_retry_delay_seconds),
        )
        task_kind = task_type.strip().lower()
        if not task_kind:
            raise ValueError("task_type is required")

        dedupe_key = idempotency_key.strip() or None

        with self._lock:
            cursor = self._connection.cursor()
            self._purge_expired_tasks(cursor, now)

            if dedupe_key:
                existing = cursor.execute(
                    """
                    SELECT task_id
                    FROM task_queue
                    WHERE idempotency_key = ?
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (dedupe_key,),
                ).fetchone()
                if existing:
                    return str(existing["task_id"])

            task_id = uuid.uuid4().hex
            cursor.execute(
                """
                INSERT INTO task_queue(
                  task_id,
                  task_type,
                  payload_json,
                  status,
                  attempts,
                  max_retries,
                  retry_delay_seconds,
                  available_at,
                  created_at,
                  updated_at,
                  expires_at,
                  idempotency_key,
                  last_error,
                  result_json,
                  dead_letter_reason
                )
                VALUES (?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?, '', NULL, '')
                """,
                (
                    task_id,
                    task_kind,
                    json.dumps(payload, ensure_ascii=False),
                    TASK_STATUS_QUEUED,
                    retries,
                    retry_delay,
                    now,
                    now,
                    now,
                    now + ttl,
                    dedupe_key,
                ),
            )
            self._connection.commit()
        return task_id

    def get(self, task_id: str) -> dict[str, Any] | None:
        """Return serialized task state by id when available."""
        now = int(time.time())
        with self._lock:
            cursor = self._connection.cursor()
            self._purge_expired_tasks(cursor, now)
            row = cursor.execute(
                "SELECT * FROM task_queue WHERE task_id = ?",
                (task_id,),
            ).fetchone()
            self._connection.commit()

        if row is None:
            return None

        result: dict[str, Any] | None = None
        if row["result_json"]:
            try:
                decoded = json.loads(str(row["result_json"]))
                if isinstance(decoded, dict):
                    result = decoded
            except json.JSONDecodeError:
                result = None

        return {
            "task_id": str(row["task_id"]),
            "task_type": str(row["task_type"]),
            "status": str(row["status"]),
            "attempts": int(row["attempts"]),
            "max_retries": int(row["max_retries"]),
            "created_at": int(row["created_at"]),
            "updated_at": int(row["updated_at"]),
            "expires_at": int(row["expires_at"]),
            "result": result,
            "error": str(row["last_error"] or ""),
            "dead_letter_reason": str(row["dead_letter_reason"] or ""),
        }

    async def _worker_loop(self) -> None:
        """Poll queue and execute due tasks until stop event is set."""
        while not self._stop_event.is_set():
            processed = await self._process_next_due_task()
            if not processed:
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(),
                        timeout=self._settings.worker_poll_interval_seconds,
                    )
                except asyncio.TimeoutError:
                    continue

    async def _process_next_due_task(self) -> bool:
        """Claim and process one due queued/retrying task."""
        now = int(time.time())
        with self._lock:
            cursor = self._connection.cursor()
            row = cursor.execute(
                """
                SELECT *
                FROM task_queue
                WHERE status IN (?, ?)
                  AND available_at <= ?
                ORDER BY available_at ASC, created_at ASC
                LIMIT 1
                """,
                (TASK_STATUS_QUEUED, TASK_STATUS_RETRYING, now),
            ).fetchone()
            if row is None:
                return False

            attempts = int(row["attempts"]) + 1
            cursor.execute(
                """
                UPDATE task_queue
                SET status = ?, attempts = ?, updated_at = ?
                WHERE task_id = ?
                """,
                (TASK_STATUS_RUNNING, attempts, now, str(row["task_id"])),
            )
            self._connection.commit()

        task_id = str(row["task_id"])
        task_type = str(row["task_type"])
        handler = self._handlers.get(task_type)
        if handler is None:
            self._mark_failed(
                task_id,
                error_message=f"No handler registered for task_type={task_type}",
                dead_letter=True,
                dead_letter_reason="handler_not_found",
            )
            return True

        try:
            payload_raw = json.loads(str(row["payload_json"]))
            payload = payload_raw if isinstance(payload_raw, dict) else {}
        except json.JSONDecodeError:
            self._mark_failed(
                task_id,
                error_message="Invalid payload JSON",
                dead_letter=True,
                dead_letter_reason="payload_decode_error",
            )
            return True

        try:
            result = await handler(payload)
        except Exception as exc:  # pragma: no cover - defensive
            self._mark_retry_or_dead_letter(task_id, str(exc) or exc.__class__.__name__)
            return True

        self._mark_completed(task_id, result)
        return True

    def _mark_completed(self, task_id: str, result: dict[str, Any]) -> None:
        """Persist successful completion state."""
        now = int(time.time())
        with self._lock:
            self._connection.execute(
                """
                UPDATE task_queue
                SET status = ?,
                    result_json = ?,
                    last_error = '',
                    dead_letter_reason = '',
                    updated_at = ?
                WHERE task_id = ?
                """,
                (
                    TASK_STATUS_COMPLETED,
                    json.dumps(result, ensure_ascii=False),
                    now,
                    task_id,
                ),
            )
            self._connection.commit()

    def _mark_retry_or_dead_letter(self, task_id: str, error_message: str) -> None:
        """Persist retry/dead-letter state based on retry policy."""
        now = int(time.time())
        with self._lock:
            cursor = self._connection.cursor()
            row = cursor.execute(
                "SELECT attempts, max_retries, retry_delay_seconds FROM task_queue WHERE task_id = ?",
                (task_id,),
            ).fetchone()
            if row is None:
                return
            attempts = int(row["attempts"])
            max_retries = int(row["max_retries"])
            retry_delay_seconds = int(row["retry_delay_seconds"])

            if attempts <= max_retries:
                next_available = now + (retry_delay_seconds * attempts)
                cursor.execute(
                    """
                    UPDATE task_queue
                    SET status = ?,
                        available_at = ?,
                        updated_at = ?,
                        last_error = ?,
                        dead_letter_reason = ''
                    WHERE task_id = ?
                    """,
                    (
                        TASK_STATUS_RETRYING,
                        next_available,
                        now,
                        error_message,
                        task_id,
                    ),
                )
            else:
                cursor.execute(
                    """
                    UPDATE task_queue
                    SET status = ?,
                        updated_at = ?,
                        last_error = ?,
                        dead_letter_reason = ?
                    WHERE task_id = ?
                    """,
                    (
                        TASK_STATUS_DEAD_LETTER,
                        now,
                        error_message,
                        "max_retries_exceeded",
                        task_id,
                    ),
                )
            self._connection.commit()

    def _mark_failed(
        self,
        task_id: str,
        *,
        error_message: str,
        dead_letter: bool,
        dead_letter_reason: str,
    ) -> None:
        """Persist explicit failure or dead-letter state."""
        status = TASK_STATUS_DEAD_LETTER if dead_letter else TASK_STATUS_FAILED
        now = int(time.time())
        with self._lock:
            self._connection.execute(
                """
                UPDATE task_queue
                SET status = ?,
                    updated_at = ?,
                    last_error = ?,
                    dead_letter_reason = ?
                WHERE task_id = ?
                """,
                (status, now, error_message, dead_letter_reason, task_id),
            )
            self._connection.commit()

    @staticmethod
    def _purge_expired_tasks(cursor: sqlite3.Cursor, now: int) -> None:
        """Delete expired terminal tasks from queue storage."""
        cursor.execute(
            """
            DELETE FROM task_queue
            WHERE expires_at <= ?
              AND status IN (?, ?, ?)
            """,
            (
                now,
                TASK_STATUS_COMPLETED,
                TASK_STATUS_FAILED,
                TASK_STATUS_DEAD_LETTER,
            ),
        )
