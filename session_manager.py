"""AgentSessionManager — concurrent, checkpointed query execution over asyncpg.

Requires Python 3.11+ (asyncio.TaskGroup, asyncio.timeout, Task.cancelling).

How it works
------------
- Queries in a batch run concurrently through a bounded worker pool
  (per_batch_concurrency) rather than sequentially.
- A manager-wide semaphore (max_concurrency) caps total in-flight DB work
  across all sessions so the connection pool is never oversubscribed.
- Every query is protected by a timeout covering acquisition + execution.
- Per-query errors are isolated; cancellation is never swallowed. TaskGroup
  guarantees all siblings are awaited before the batch exits.
- Each successful query commits its result and checkpoint row in the same
  transaction, keyed by (session_id, batch_id, query_index). Re-submitting
  the same batch_id resumes from wherever it left off.
- Batches for the same session are serialised so state updates have a
  consistent order; different sessions run in parallel.
"""

from __future__ import annotations

import asyncio
import contextlib
import enum
import logging
import uuid
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    import asyncpg

logger = logging.getLogger(__name__)


# DDL lives in a migration in production; kept here for reference.
CHECKPOINT_DDL = """
CREATE TABLE IF NOT EXISTS session_query_checkpoints (
    session_id  text        NOT NULL,
    batch_id    text        NOT NULL,
    query_index integer     NOT NULL,
    query       text        NOT NULL,
    result      text        NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (session_id, batch_id, query_index)
);
"""

_LOAD_CHECKPOINTS_SQL = """
SELECT query_index, query, result
FROM session_query_checkpoints
WHERE session_id = $1 AND batch_id = $2
"""

# ON CONFLICT DO NOTHING: safe to call on retry; duplicate rows are a no-op.
_SAVE_CHECKPOINT_SQL = """
INSERT INTO session_query_checkpoints (session_id, batch_id, query_index, query, result)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (session_id, batch_id, query_index) DO NOTHING
"""


class QueryStatus(str, enum.Enum):
    OK = "ok"
    ERROR = "error"
    TIMEOUT = "timeout"


@dataclass(frozen=True, slots=True)
class QueryResult:
    index: int
    query: str
    status: QueryStatus
    value: str | None = None
    error: str | None = None
    from_checkpoint: bool = False

    @property
    def ok(self) -> bool:
        return self.status is QueryStatus.OK


class SessionManagerClosedError(RuntimeError):
    """Raised when work is submitted after aclose()."""


class BatchCancelledError(Exception):
    """Raised when a batch is cancelled by cancel_session() or aclose(),
    not by the caller's own task. Completed queries are already checkpointed;
    re-submit with the same batch_id to resume.
    """

    def __init__(self, session_id: str, batch_id: str) -> None:
        super().__init__(f"batch {batch_id!r} for session {session_id!r} was cancelled")
        self.session_id = session_id
        self.batch_id = batch_id


@dataclass(slots=True)
class _SessionSlot:
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    refs: int = 0


class AgentSessionManager:
    def __init__(
        self,
        db_pool: asyncpg.Pool,
        *,
        max_concurrency: int = 10,
        per_batch_concurrency: int | None = None,
        query_timeout: float = 5.0,
    ) -> None:
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be >= 1")
        if per_batch_concurrency is not None and per_batch_concurrency < 1:
            raise ValueError("per_batch_concurrency must be >= 1")
        if query_timeout <= 0:
            raise ValueError("query_timeout must be > 0")

        pool_max = getattr(db_pool, "get_max_size", None)
        if callable(pool_max) and max_concurrency > pool_max():
            logger.warning(
                "max_concurrency=%d exceeds pool max_size=%d; queries will queue "
                "inside the pool and that wait counts against query_timeout",
                max_concurrency, pool_max(),
            )

        self.db_pool = db_pool
        self._per_batch_concurrency = per_batch_concurrency or max_concurrency
        self._query_timeout = query_timeout
        self._db_slots = asyncio.Semaphore(max_concurrency)
        self._active_tasks: dict[str, set[asyncio.Task[list[QueryResult]]]] = {}
        self._session_slots: dict[str, _SessionSlot] = {}
        self._closed = False

    # ------------------------------------------------------------------

    @property
    def active_sessions(self) -> frozenset[str]:
        return frozenset(self._active_tasks)

    async def process_batch_stream(
        self,
        session_id: str,
        queries: Sequence[str],
        *,
        batch_id: str | None = None,
    ) -> list[QueryResult]:
        """Run queries concurrently and return results in input order.

        batch_id is an idempotency key: re-submitting the same batch skips
        queries that were already committed.
        """
        if self._closed:
            raise SessionManagerClosedError("session manager is closed")
        if isinstance(queries, str):
            raise TypeError("queries must be a sequence of strings, not a single string")
        if not queries:
            return []

        batch_id = batch_id or uuid.uuid4().hex
        batch = asyncio.create_task(
            self._run_batch(session_id, batch_id, list(queries)),
            name=f"agent-batch:{session_id}:{batch_id}",
        )
        self._track(session_id, batch)
        try:
            return await batch
        except asyncio.CancelledError:
            current = asyncio.current_task()
            caller_cancelled = current is not None and current.cancelling() > 0
            if batch.cancelled() and not caller_cancelled:
                raise BatchCancelledError(session_id, batch_id) from None
            raise

    def cancel_session(self, session_id: str) -> int:
        """Cancel every in-flight batch for session_id. Returns the count signalled."""
        return sum(task.cancel() for task in tuple(self._active_tasks.get(session_id, ())))

    async def aclose(self) -> None:
        """Stop accepting work, cancel all in-flight batches and wait for cleanup."""
        self._closed = True
        tasks = [task for tasks in self._active_tasks.values() for task in tasks]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def __aenter__(self) -> AgentSessionManager:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.aclose()

    # ------------------------------------------------------------------

    def _track(self, session_id: str, task: asyncio.Task[Any]) -> None:
        self._active_tasks.setdefault(session_id, set()).add(task)

        def _untrack(done: asyncio.Task[Any]) -> None:
            tasks = self._active_tasks.get(session_id)
            if tasks is not None:
                tasks.discard(done)
                if not tasks:
                    del self._active_tasks[session_id]

        task.add_done_callback(_untrack)

    @contextlib.asynccontextmanager
    async def _session_guard(self, session_id: str) -> AsyncIterator[None]:
        """Hold the per-session lock for the duration of one batch."""
        slot = self._session_slots.get(session_id)
        if slot is None:  # no await between get and set — no race in asyncio
            slot = self._session_slots[session_id] = _SessionSlot()
        slot.refs += 1
        try:
            async with slot.lock:
                yield
        finally:
            slot.refs -= 1
            if slot.refs == 0:
                del self._session_slots[session_id]

    async def _run_batch(
        self, session_id: str, batch_id: str, queries: list[str]
    ) -> list[QueryResult]:
        async with self._session_guard(session_id):
            results: list[QueryResult | None] = [None] * len(queries)

            for index, (query, value) in (await self._load_checkpoints(session_id, batch_id)).items():
                if index >= len(queries) or queries[index] != query:
                    raise ValueError(
                        f"batch_id {batch_id!r} was already used with different queries"
                    )
                results[index] = QueryResult(
                    index, query, QueryStatus.OK, value=value, from_checkpoint=True
                )

            pending = [i for i, r in enumerate(results) if r is None]
            if pending:
                work = iter(pending)  # shared iterator; each index is claimed exactly once

                async def worker() -> None:
                    for index in work:
                        results[index] = await self._run_query(
                            session_id, batch_id, index, queries[index]
                        )

                async with asyncio.TaskGroup() as tg:
                    for n in range(min(self._per_batch_concurrency, len(pending))):
                        tg.create_task(worker(), name=f"agent-worker:{session_id}:{n}")

            if any(r is None for r in results):
                raise RuntimeError("BUG: some query results were not populated")
            return results  # type: ignore[return-value]

    async def _load_checkpoints(
        self, session_id: str, batch_id: str
    ) -> dict[int, tuple[str, str]]:
        async with self._db_slots:
            async with asyncio.timeout(self._query_timeout):
                async with self.db_pool.acquire() as conn:
                    rows = await conn.fetch(_LOAD_CHECKPOINTS_SQL, session_id, batch_id)
        return {row["query_index"]: (row["query"], row["result"]) for row in rows}

    async def _run_query(
        self, session_id: str, batch_id: str, index: int, query: str
    ) -> QueryResult:
        try:
            async with self._db_slots:
                # Timeout covers pool acquisition + execution.
                # Waiting for _db_slots is intentionally unbounded — that's back-pressure.
                async with asyncio.timeout(self._query_timeout):
                    async with self.db_pool.acquire() as conn:
                        async with conn.transaction():
                            value = await self._execute_vector_lookup(conn, session_id, query)
                            await conn.execute(
                                _SAVE_CHECKPOINT_SQL, session_id, batch_id, index, query, value
                            )
        except TimeoutError:
            logger.warning("query %d of session %s timed out", index, session_id)
            return QueryResult(
                index, query, QueryStatus.TIMEOUT,
                error=f"timed out after {self._query_timeout}s",
            )
        except Exception as exc:  # CancelledError is BaseException — never caught here
            logger.warning("query %d of session %s failed", index, session_id, exc_info=True)
            return QueryResult(
                index, query, QueryStatus.ERROR, error=f"{type(exc).__name__}: {exc}"
            )
        return QueryResult(index, query, QueryStatus.OK, value=value)

    async def _execute_vector_lookup(
        self, connection: asyncpg.Connection, session_id: str, query: str
    ) -> str:
        """pgvector ORDER BY embedding <=> $1 + session state UPDATE go here."""
        await asyncio.sleep(0.1)
        return f"Processed {query} for session {session_id}"
