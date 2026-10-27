"""In-memory asyncpg.Pool stand-in for tests and the demo."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from typing import Any

CheckpointKey = tuple[str, str, int]   # (session_id, batch_id, query_index)
CheckpointRow = tuple[str, str]        # (query, result)


class FakeConnection:
    def __init__(self, pool: FakePool) -> None:
        self._pool = pool
        self._tx: list[tuple[CheckpointKey, CheckpointRow]] | None = None

    @contextlib.asynccontextmanager
    async def transaction(self) -> AsyncIterator[None]:
        self._tx = []
        try:
            yield
        except BaseException:
            self._pool.rollbacks += 1
            raise
        else:
            for key, row in self._tx:
                self._pool.checkpoints.setdefault(key, row)
            self._pool.commits += 1
        finally:
            self._tx = None

    async def execute(self, _sql: str, session_id: str, batch_id: str,
                      index: int, query: str, result: str) -> str:
        await asyncio.sleep(0)
        entry = ((session_id, batch_id, index), (query, result))
        if self._tx is not None:
            self._tx.append(entry)
        else:
            self._pool.checkpoints.setdefault(*entry)
        return "INSERT 0 1"

    async def fetch(self, _sql: str, session_id: str, batch_id: str) -> list[dict[str, Any]]:
        await asyncio.sleep(0)
        return [
            {"query_index": i, "query": q, "result": r}
            for (s, b, i), (q, r) in self._pool.checkpoints.items()
            if s == session_id and b == batch_id
        ]


class FakePool:
    def __init__(self, max_size: int = 10) -> None:
        self._max_size = max_size
        self._free = asyncio.Semaphore(max_size)
        self.in_use = 0
        self.peak_in_use = 0
        self.commits = 0
        self.rollbacks = 0
        self.checkpoints: dict[CheckpointKey, CheckpointRow] = {}

    def get_max_size(self) -> int:
        return self._max_size

    @contextlib.asynccontextmanager
    async def acquire(self, *, timeout: float | None = None) -> AsyncIterator[FakeConnection]:
        async with self._free:
            self.in_use += 1
            self.peak_in_use = max(self.peak_in_use, self.in_use)
            try:
                yield FakeConnection(self)
            finally:
                self.in_use -= 1
