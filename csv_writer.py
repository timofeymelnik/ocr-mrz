from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterable, Mapping

CSV_COLUMNS = [
    "surname",
    "name",
    "document_number",
    "date_of_birth",
    "expiry_date",
    "nationality",
]


def append_rows(csv_path: Path, rows: Iterable[Mapping[str, str]]) -> int:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    exists = csv_path.exists()
    count = 0
    with csv_path.open("a", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_COLUMNS)
        if not exists:
            writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in CSV_COLUMNS})
            count += 1
    return count


def read_rows(csv_path: Path) -> list[dict[str, str]]:
    if not csv_path.exists():
        return []
    with csv_path.open("r", newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        return [dict(r) for r in reader]
