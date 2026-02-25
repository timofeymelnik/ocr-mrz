"""SQLite migration runner for runtime infrastructure tables."""

from __future__ import annotations

import sqlite3
from pathlib import Path

MIGRATIONS_DIR = Path(__file__).resolve().parent / "sql"


def apply_migrations(database_path: Path) -> None:
    """Apply all SQL migrations in ascending order for the given database."""
    database_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(str(database_path))
    try:
        cursor = connection.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
              migration_id TEXT PRIMARY KEY,
              applied_at INTEGER NOT NULL
            )
            """
        )
        migration_files = sorted(MIGRATIONS_DIR.glob("*.sql"))

        for migration_file in migration_files:
            migration_id = migration_file.name
            already_applied = cursor.execute(
                "SELECT 1 FROM schema_migrations WHERE migration_id = ?",
                (migration_id,),
            ).fetchone()
            if already_applied:
                continue
            sql = migration_file.read_text(encoding="utf-8")
            cursor.executescript(sql)
            cursor.execute(
                "INSERT INTO schema_migrations(migration_id, applied_at) VALUES (?, strftime('%s','now'))",
                (migration_id,),
            )
        connection.commit()
    finally:
        connection.close()
