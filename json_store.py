from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable


def append_documents_jsonl(path: Path, documents: Iterable[dict[str, Any]]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("a", encoding="utf-8") as fh:
        for doc in documents:
            fh.write(json.dumps(doc, ensure_ascii=False) + "\n")
            count += 1
    return count


def read_documents_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    docs: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            docs.append(json.loads(line))
    return docs
