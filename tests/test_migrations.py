from __future__ import annotations

import sqlite3
from pathlib import Path

from app.core.migrations import apply_migrations


def test_apply_migrations_creates_queue_tables(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"

    apply_migrations(db_path)

    connection = sqlite3.connect(str(db_path))
    try:
        cursor = connection.cursor()
        tables = {
            row[0]
            for row in cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }

        assert "schema_migrations" in tables
        assert "task_queue" in tables

        migration_ids = {
            row[0]
            for row in cursor.execute(
                "SELECT migration_id FROM schema_migrations"
            ).fetchall()
        }
        assert "0001_task_queue.sql" in migration_ids
        assert "0002_task_queue_dead_letter_index.sql" in migration_ids
        assert "0003_auth_login_rate_limit.sql" in migration_ids
    finally:
        connection.close()
