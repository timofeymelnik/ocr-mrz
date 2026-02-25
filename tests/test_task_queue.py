from __future__ import annotations

import asyncio
from pathlib import Path

from app.core.task_queue import QueueSettings, TaskQueue


def _build_queue(tmp_path: Path) -> TaskQueue:
    return TaskQueue(
        QueueSettings(
            database_path=tmp_path / "queue.db",
            default_ttl_seconds=60,
            default_max_retries=2,
            default_retry_delay_seconds=1,
            worker_poll_interval_seconds=0.01,
        )
    )


async def _wait_terminal(queue: TaskQueue, task_id: str) -> dict[str, object]:
    for _ in range(300):
        state = queue.get(task_id)
        if state and str(state.get("status")) in {
            "completed",
            "failed",
            "dead_letter",
        }:
            return state
        await asyncio.sleep(0.01)
    raise AssertionError("Task did not reach terminal state in time")


def test_task_queue_executes_registered_handler(tmp_path: Path) -> None:
    async def scenario() -> None:
        queue = _build_queue(tmp_path)

        async def handler(payload: dict[str, object]) -> dict[str, object]:
            return {"value": int(payload.get("value", 0)) + 1}

        queue.register_handler("sample", handler)
        await queue.start()
        task_id = queue.submit(task_type="sample", payload={"value": 41})
        result = await _wait_terminal(queue, task_id)
        await queue.stop()
        queue.close()

        assert result["status"] == "completed"
        assert result["result"] == {"value": 42}

    asyncio.run(scenario())


def test_task_queue_moves_to_dead_letter_after_retries(tmp_path: Path) -> None:
    async def scenario() -> None:
        queue = _build_queue(tmp_path)

        async def handler(payload: dict[str, object]) -> dict[str, object]:
            _ = payload
            raise RuntimeError("boom")

        queue.register_handler("unstable", handler)
        await queue.start()
        task_id = queue.submit(
            task_type="unstable",
            payload={},
            max_retries=1,
            retry_delay_seconds=1,
        )
        result = await _wait_terminal(queue, task_id)
        await queue.stop()
        queue.close()

        assert result["status"] == "dead_letter"
        assert result["dead_letter_reason"] == "max_retries_exceeded"
        assert "boom" in str(result["error"])

    asyncio.run(scenario())


def test_task_queue_respects_idempotency_key(tmp_path: Path) -> None:
    async def scenario() -> None:
        queue = _build_queue(tmp_path)

        async def handler(payload: dict[str, object]) -> dict[str, object]:
            return {"ok": True, "value": payload.get("value")}

        queue.register_handler("idem", handler)
        await queue.start()

        task_id_one = queue.submit(
            task_type="idem",
            payload={"value": 1},
            idempotency_key="upload-123",
        )
        task_id_two = queue.submit(
            task_type="idem",
            payload={"value": 2},
            idempotency_key="upload-123",
        )

        result = await _wait_terminal(queue, task_id_one)
        await queue.stop()
        queue.close()

        assert task_id_one == task_id_two
        assert result["status"] == "completed"

    asyncio.run(scenario())
