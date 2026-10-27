"""Tests for the original and corrected AgentSessionManager.

Run (Python 3.11+, no third-party dependencies):

    python3 -m unittest -v test_session_manager.py

The ``OriginalImplementation`` tests document how the supplied code actually
behaves - including concerns that turn out *not* to be defects.
"""

from __future__ import annotations

import asyncio
import logging
import sys
import time
import types
import unittest

# The original module imports asyncpg only for a type annotation; stub it so the
# tests run without the driver installed.
try:
    import asyncpg  # noqa: F401
except ImportError:  # pragma: no cover
    _stub = types.ModuleType("asyncpg")
    _stub.Pool = object  # type: ignore[attr-defined]
    sys.modules["asyncpg"] = _stub

import original
from fakes import FakePool
from session_manager import (
    AgentSessionManager,
    BatchCancelledError,
    QueryStatus,
    SessionManagerClosedError,
)

# Failures/timeouts below are intentional; keep the test output readable.
logging.getLogger("session_manager").setLevel(logging.CRITICAL)


class InstrumentedManager(AgentSessionManager):
    """Corrected manager with a controllable, observable lookup."""

    def __init__(self, pool, *, latency=0.05, fail_on=(), slow_on=(), **kwargs):
        super().__init__(pool, **kwargs)
        self.latency = latency
        self.fail_on = set(fail_on)
        self.slow_on = set(slow_on)
        self.calls: list[tuple[str, str]] = []
        self.running = 0
        self.peak_running = 0

    async def _execute_vector_lookup(self, connection, session_id, query):
        self.calls.append((session_id, query))
        self.running += 1
        self.peak_running = max(self.peak_running, self.running)
        try:
            await asyncio.sleep(60 if query in self.slow_on else self.latency)
            if query in self.fail_on:
                raise RuntimeError(f"vector index unavailable for {query!r}")
            return f"Processed {query} for session {session_id}"
        finally:
            self.running -= 1


def queries(n: int) -> list[str]:
    return [f"q{i}" for i in range(n)]


# --------------------------------------------------------------------------
# Original implementation: what is (and is not) actually wrong
# --------------------------------------------------------------------------
class OriginalImplementation(unittest.IsolatedAsyncioTestCase):

    async def test_runs_sequentially_despite_create_task(self):
        pool = FakePool()
        mgr = original.AgentSessionManager(pool)
        start = time.perf_counter()
        await mgr.process_batch_stream("s1", queries(5))
        elapsed = time.perf_counter() - start
        self.assertGreaterEqual(elapsed, 0.5)       # 5 x 100 ms, no overlap
        self.assertEqual(pool.peak_in_use, 1)       # never more than one connection

    async def test_tracks_only_last_task_and_retains_references(self):
        mgr = original.AgentSessionManager(FakePool())
        # Two concurrent batches for one session: only one task is tracked.
        both = asyncio.gather(
            mgr.process_batch_stream("s1", queries(3)),
            mgr.process_batch_stream("s1", queries(3)),
        )
        await asyncio.sleep(0.05)
        self.assertEqual(len(mgr._active_tasks), 1)
        await both
        # Finished tasks are never removed: grows with every distinct session.
        for sid in ("s2", "s3", "s4"):
            await mgr.process_batch_stream(sid, ["q"])
        self.assertEqual(len(mgr._active_tasks), 4)
        self.assertTrue(all(t.done() for t in mgr._active_tasks.values()))

    async def test_one_failure_aborts_batch_and_discards_completed_results(self):
        class Failing(original.AgentSessionManager):
            calls: list[str] = []

            async def _execute_vector_lookup(self, session_id, query):
                self.calls.append(query)
                if query == "b":
                    raise RuntimeError("boom")
                return await super()._execute_vector_lookup(session_id, query)

        mgr = Failing(FakePool())
        with self.assertRaises(RuntimeError):
            await mgr.process_batch_stream("s1", ["a", "b", "c"])
        self.assertEqual(mgr.calls, ["a", "b"])     # "c" never runs, "a" is lost

    async def test_NOT_A_DEFECT_cancellation_propagates_and_connection_is_released(self):
        pool = FakePool()
        mgr = original.AgentSessionManager(pool)
        outer = asyncio.create_task(mgr.process_batch_stream("s1", queries(5)))
        await asyncio.sleep(0.05)
        outer.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await outer
        # Awaiting a Task forwards cancellation to it, and `async with acquire()`
        # releases the connection: no orphaned task, no leaked connection.
        self.assertTrue(mgr._active_tasks["s1"].cancelled())
        self.assertEqual(pool.in_use, 0)


# --------------------------------------------------------------------------
# Corrected implementation
# --------------------------------------------------------------------------
class Concurrency(unittest.IsolatedAsyncioTestCase):

    async def test_runs_concurrently_and_preserves_input_order(self):
        mgr = InstrumentedManager(FakePool(), latency=0.1, max_concurrency=10)
        start = time.perf_counter()
        results = await mgr.process_batch_stream("s1", queries(10))
        elapsed = time.perf_counter() - start
        self.assertLess(elapsed, 0.35)              # ~0.1 s instead of 1.0 s
        self.assertEqual([r.query for r in results], queries(10))
        self.assertTrue(all(r.ok for r in results))
        self.assertEqual(results[3].value, "Processed q3 for session s1")

    async def test_global_limit_is_shared_across_sessions(self):
        pool = FakePool(max_size=10)
        mgr = InstrumentedManager(pool, latency=0.02, max_concurrency=3)
        await asyncio.gather(
            mgr.process_batch_stream("s1", queries(10)),
            mgr.process_batch_stream("s2", queries(10)),
        )
        self.assertEqual(mgr.peak_running, 3)
        self.assertLessEqual(pool.peak_in_use, 3)

    async def test_per_batch_limit(self):
        mgr = InstrumentedManager(FakePool(), latency=0.02,
                                  max_concurrency=10, per_batch_concurrency=2)
        await mgr.process_batch_stream("s1", queries(6))
        self.assertEqual(mgr.peak_running, 2)

    async def test_same_session_batches_are_serialised_other_sessions_are_not(self):
        mgr = InstrumentedManager(FakePool(), latency=0.1)
        start = time.perf_counter()
        await asyncio.gather(mgr.process_batch_stream("s1", ["a"]),
                             mgr.process_batch_stream("s1", ["b"]))
        self.assertGreaterEqual(time.perf_counter() - start, 0.2)

        start = time.perf_counter()
        await asyncio.gather(mgr.process_batch_stream("s1", ["a"]),
                             mgr.process_batch_stream("s2", ["b"]))
        self.assertLess(time.perf_counter() - start, 0.18)


class FailureHandling(unittest.IsolatedAsyncioTestCase):

    async def test_failure_is_isolated_and_not_checkpointed(self):
        pool = FakePool()
        mgr = InstrumentedManager(pool, fail_on={"b"})
        results = await mgr.process_batch_stream("s1", ["a", "b", "c"], batch_id="b1")
        self.assertEqual([r.status for r in results],
                         [QueryStatus.OK, QueryStatus.ERROR, QueryStatus.OK])
        self.assertIn("vector index unavailable", results[1].error)
        self.assertEqual(sorted(k[2] for k in pool.checkpoints), [0, 2])
        self.assertEqual(pool.rollbacks, 1)

    async def test_timeout_frees_slot_and_connection(self):
        pool = FakePool()
        mgr = InstrumentedManager(pool, slow_on={"slow"}, query_timeout=0.2)
        start = time.perf_counter()
        results = await mgr.process_batch_stream("s1", ["a", "slow"])
        self.assertLess(time.perf_counter() - start, 1.0)
        self.assertEqual(results[1].status, QueryStatus.TIMEOUT)
        self.assertEqual(pool.in_use, 0)

    async def test_input_validation(self):
        mgr = InstrumentedManager(FakePool())
        self.assertEqual(await mgr.process_batch_stream("s1", []), [])
        with self.assertRaises(TypeError):
            await mgr.process_batch_stream("s1", "not-a-list")


class Checkpointing(unittest.IsolatedAsyncioTestCase):

    async def test_resubmitting_batch_resumes_from_checkpoint(self):
        pool = FakePool()
        mgr = InstrumentedManager(pool, fail_on={"b"})
        first = await mgr.process_batch_stream("s1", ["a", "b", "c"], batch_id="b1")
        self.assertFalse(first[1].ok)

        mgr.fail_on.clear()
        mgr.calls.clear()
        second = await mgr.process_batch_stream("s1", ["a", "b", "c"], batch_id="b1")
        self.assertTrue(all(r.ok for r in second))
        self.assertEqual(mgr.calls, [("s1", "b")])  # only the failed query re-ran
        self.assertEqual([r.from_checkpoint for r in second], [True, False, True])

    async def test_batch_id_reuse_with_different_queries_is_rejected(self):
        mgr = InstrumentedManager(FakePool())
        await mgr.process_batch_stream("s1", ["a", "b"], batch_id="b1")
        with self.assertRaises(ValueError):
            await mgr.process_batch_stream("s1", ["x", "y"], batch_id="b1")


class CancellationAndLifecycle(unittest.IsolatedAsyncioTestCase):

    async def test_caller_cancellation_propagates_and_cleans_up(self):
        pool = FakePool()
        mgr = InstrumentedManager(pool, latency=0.2, max_concurrency=2)
        caller = asyncio.create_task(mgr.process_batch_stream("s1", queries(6)))
        await asyncio.sleep(0.05)                   # two lookups in flight
        caller.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await caller
        self.assertEqual(pool.in_use, 0)            # connections returned
        self.assertEqual(pool.rollbacks, 2)         # in-flight work rolled back
        self.assertEqual(pool.checkpoints, {})
        self.assertEqual(mgr.running, 0)
        self.assertEqual(mgr.active_sessions, frozenset())
        self.assertEqual(mgr._session_slots, {})

    async def test_cancel_session_keeps_completed_work(self):
        pool = FakePool()
        mgr = InstrumentedManager(pool, latency=0.1, max_concurrency=2)
        caller = asyncio.create_task(
            mgr.process_batch_stream("s1", queries(6), batch_id="b1"))
        await asyncio.sleep(0.15)                   # 2 done, 2 in flight
        self.assertEqual(mgr.cancel_session("s1"), 1)
        with self.assertRaises(BatchCancelledError):
            await caller
        self.assertEqual(len(pool.checkpoints), 2)  # committed work survives
        self.assertEqual(pool.in_use, 0)
        self.assertEqual(mgr.active_sessions, frozenset())

    async def test_tracking_state_does_not_leak(self):
        mgr = InstrumentedManager(FakePool(), latency=0.01)
        for sid in ("s1", "s2", "s3"):
            await mgr.process_batch_stream(sid, queries(3))
        self.assertEqual(mgr.active_sessions, frozenset())
        self.assertEqual(mgr._session_slots, {})

    async def test_aclose_cancels_inflight_and_rejects_new_work(self):
        pool = FakePool()
        mgr = InstrumentedManager(pool, latency=0.2)
        caller = asyncio.create_task(mgr.process_batch_stream("s1", queries(3)))
        await asyncio.sleep(0.05)
        await mgr.aclose()
        with self.assertRaises(BatchCancelledError):
            await caller
        with self.assertRaises(SessionManagerClosedError):
            await mgr.process_batch_stream("s1", ["a"])
        self.assertEqual(pool.in_use, 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
