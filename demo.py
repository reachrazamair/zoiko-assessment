"""Side-by-side demo: original vs corrected manager.

    python3 demo.py
"""

from __future__ import annotations

import asyncio
import logging
import sys
import time
import types

try:
    import asyncpg  # noqa: F401
except ImportError:
    _stub = types.ModuleType("asyncpg")
    _stub.Pool = object  # type: ignore[attr-defined]
    sys.modules["asyncpg"] = _stub

import original
from fakes import FakePool
from session_manager import AgentSessionManager

logging.basicConfig(level=logging.ERROR)
QUERIES = [f"q{i}" for i in range(20)]


class FlakyManager(AgentSessionManager):
    """Fails q7 once to exercise failure isolation and checkpoint resume."""

    failed_once = False

    async def _execute_vector_lookup(self, connection, session_id, query):
        if query == "q7" and not self.failed_once:
            self.failed_once = True
            raise ConnectionError("transient vector-store error")
        return await super()._execute_vector_lookup(connection, session_id, query)


async def timed(label, coro, pool):
    start = time.perf_counter()
    result = await coro
    print(f"{label:<34} {time.perf_counter() - start:5.2f}s   "
          f"peak connections={pool.peak_in_use}")
    return result


async def main() -> None:
    print("20 queries, 100 ms each, pool max_size=10\n")

    pool = FakePool(max_size=10)
    await timed("original (sequential)", original.AgentSessionManager(pool)
                .process_batch_stream("s1", QUERIES), pool)

    pool = FakePool(max_size=10)
    await timed("corrected (max_concurrency=5)", AgentSessionManager(pool, max_concurrency=5)
                .process_batch_stream("s1", QUERIES), pool)

    print("\n-- failure isolation + checkpoint resume --")
    pool = FakePool(max_size=10)
    mgr = FlakyManager(pool, max_concurrency=5)
    first = await mgr.process_batch_stream("s1", QUERIES, batch_id="batch-42")
    bad = [r for r in first if not r.ok]
    print(f"attempt 1: {len(first) - len(bad)} ok, {len(bad)} failed -> {bad[0].error}")
    second = await mgr.process_batch_stream("s1", QUERIES, batch_id="batch-42")
    resumed = sum(r.from_checkpoint for r in second)
    print(f"attempt 2: {sum(r.ok for r in second)} ok "
          f"({resumed} from checkpoint, {len(second) - resumed} executed)")


if __name__ == "__main__":
    asyncio.run(main())
