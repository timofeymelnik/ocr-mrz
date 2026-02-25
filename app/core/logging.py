"""Structured JSON logging with correlation-id context."""

from __future__ import annotations

import json
import logging
import sys
from contextvars import ContextVar
from datetime import datetime, timezone
from typing import Any

CORRELATION_ID_CTX: ContextVar[str] = ContextVar("correlation_id", default="")


class JsonLogFormatter(logging.Formatter):
    """Serialize log records into compact JSON lines."""

    def format(self, record: logging.LogRecord) -> str:
        """Return JSON string for the given log record."""
        payload: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "correlation_id": CORRELATION_ID_CTX.get(),
        }

        for key in ["document_id", "task_id", "path", "method", "status_code"]:
            value = getattr(record, key, None)
            if value not in (None, ""):
                payload[key] = value

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(payload, ensure_ascii=False)


def setup_logging(level: str = "INFO") -> None:
    """Configure root logger to emit structured JSON logs."""
    normalized_level = getattr(logging, level.upper(), logging.INFO)
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(JsonLogFormatter())

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(normalized_level)
    root_logger.addHandler(handler)


def set_correlation_id(correlation_id: str) -> None:
    """Store correlation id in request-local context."""
    CORRELATION_ID_CTX.set(correlation_id)
