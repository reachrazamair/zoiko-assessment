# Assessment 1 – Async Concurrency & State Management

| File | Purpose |
|---|---|
| [session_manager.py](session_manager.py) | Corrected implementation |
| [test_session_manager.py](test_session_manager.py) | 17 stdlib tests (original behaviour + corrected behaviour) |
| [original.py](original.py) | Supplied code, unchanged, used as a test subject |
| [fakes.py](fakes.py) | In-memory asyncpg-compatible pool (acquire / transaction / rollback) |
| [demo.py](demo.py) | Side-by-side timing and checkpoint-resume demo |

```bash
# Python 3.11+, no third-party packages needed
python3 -m unittest -v test_session_manager.py
python3 demo.py
```

```
20 queries, 100 ms each, pool max_size=10
original (sequential)               2.03s   peak connections=1
corrected (max_concurrency=5)       0.40s   peak connections=5
```

---

## 1. Defects in the original

| # | Defect | Consequence |
|---|---|---|
| D1 | **No concurrency.** Each task is awaited right after `create_task`, so the loop runs one query at a time. | Latency is N × lookup time. `create_task` costs scheduling overhead and gives nothing back. |
| D2 | **Task tracking is broken.** `_active_tasks[session_id]` is overwritten on every query, so it only ever holds the *last* task. Concurrent batches for the same session overwrite each other. Entries are never removed, and nothing reads the dict. | Tracking gives no lifecycle control: you can't cancel or inspect a session's work. The dict grows by one finished `Task` per distinct session for as long as the process lives. Each entry keeps its result or exception (and traceback) alive. |
| D3 | **All-or-nothing failure.** The first exception stops the loop. | Results already computed are thrown away, the remaining queries never run, and the caller can't tell which query failed. |
| D4 | **No timeouts.** asyncpg's `acquire()` waits forever by default, and so does the lookup itself. | A query that hangs holds a connection forever. If the pool is exhausted, callers hang forever too. |
| D5 | **No state consistency or checkpointing.** The comment says "state update", but there is no transaction and no durable progress record. Concurrent batches for the same session can interleave. | A crash, cancel or error mid-batch loses all progress. A retry redoes everything, which applies non-idempotent state updates twice. Updates for one session land in an undefined order. |
| D6 | **Unbounded concurrency (latent).** Nothing limits work at the manager level. D1 hides this today. | The obvious fix, `gather(*all_queries)`, creates N tasks that all queue inside the pool. One large batch then starves every other session and runs into timeouts. |
| D7 | **API mismatch.** The method is called `process_batch_stream` but returns a list at the end. The return type is untyped. | Callers can't tell from the name or signature what they get back. *(Design limitation, not a bug.)* |

## 2. Suspected concerns that the code does **not** support

The brief warns that not every suspected defect is real. The last test in `OriginalImplementation` checks the most likely false positive empirically.

| Suspected concern | Verdict |
|---|---|
| "Cancelling the caller orphans the task / leaks the connection" | **Not present.** `await task` forwards cancellation to the awaited task (`Task.cancel()` cancels its `_fut_waiter`), and `async with pool.acquire()` releases the connection on `CancelledError`. Covered by `test_NOT_A_DEFECT_cancellation_propagates_and_connection_is_released`. |
| "Connection-pool exhaustion / connection leak" | **Not present.** The context manager always releases the connection, and sequential execution uses at most one connection per batch. Exhaustion only becomes a risk *after* a naive fix (D6). |
| "Data race / thread-unsafe dict" | **Not present.** asyncio runs on a single thread, and dict operations between `await`s are atomic. The real problem is the *logical* overwrite (D2), not memory corruption. |
| "Task exception was never retrieved" | **Not present.** Every task is awaited, so exceptions propagate to the caller. |
| "Tasks can be garbage-collected mid-flight" (the loop keeps only weak refs) | **Not present.** The local variable plus the `await` keep a strong reference. |
| "Blocks the event loop" | **Not present.** `asyncio.sleep` doesn't block, and there is no synchronous I/O. |
| "Results come back out of order" | **Not present.** Sequential execution keeps input order. The corrected version keeps it too, by writing results by index. |

## 3. Corrected design

| Concern | Change |
|---|---|
| Concurrency | Each batch runs `min(per_batch_concurrency, pending)` workers that pull indices from a shared iterator, so the number of tasks per batch is bounded and not O(N). |
| Resource bounds | A manager-wide `asyncio.Semaphore(max_concurrency)` covers *all* DB work across sessions. The constructor warns if `max_concurrency` > pool `max_size`. |
| Timeouts | `asyncio.timeout(query_timeout)` covers **acquire + execute**. Waiting for a semaphore slot is deliberately not timed: that wait is our back-pressure. Callers can wrap the whole batch in their own timeout. |
| Failure isolation | Each query returns a `QueryResult` (`ok` / `error` / `timeout`). One failure doesn't discard its siblings. |
| Cancellation | `asyncio.TaskGroup` gives structured concurrency: when a batch is cancelled, every worker is cancelled and awaited before the call returns. `except Exception` never catches `CancelledError` (a `BaseException`), so cancellation is never swallowed. |
| Task lifecycle | `_active_tasks: dict[session_id, set[Task]]` tracks **every** in-flight batch. A done-callback removes entries, so nothing leaks. `cancel_session()` and `aclose()` (also available as an async context manager) build on this. |
| Cancel semantics | If the caller is cancelled, the call raises `CancelledError` (via `Task.cancelling()`). If `cancel_session`/`aclose` cancels the batch, it raises `BatchCancelledError`, so a caller that wasn't cancelled isn't told it is being torn down. |
| State + checkpoint | The state update and the checkpoint row commit in **one transaction** per query, keyed by `(session_id, batch_id, query_index)` with `ON CONFLICT DO NOTHING`. Errors, timeouts and cancellation roll back. |
| Resume | Re-submitting with the same `batch_id` loads the checkpoints and runs only the missing queries. Reusing a `batch_id` with different queries raises `ValueError` (the same rule as idempotency keys). |
| Session ordering | A per-session `asyncio.Lock` (reference-counted and removed when idle) serialises batches for the same session. Different sessions run in parallel. |

### Cancellation / failure semantics

| Event | Caller sees | Effect on state |
|---|---|---|
| A query raises | `QueryResult(status=ERROR)` | That query's transaction rolls back. The others commit. |
| A query exceeds the timeout | `QueryResult(status=TIMEOUT)` | Rolled back. The connection and the semaphore slot are freed. |
| The caller's task is cancelled | `asyncio.CancelledError` | In-flight queries roll back. Completed ones stay checkpointed. |
| `cancel_session()` / `aclose()` | `BatchCancelledError` | Same as above. Re-submit with the same `batch_id` to resume. |
| A bug inside a worker | `ExceptionGroup` | Sibling workers are cancelled and awaited (fail fast). |

### Pool sizing guidance
- `max_concurrency` ≤ the pool's `max_size`, minus headroom for other users of the same pool.
- `replicas × pool max_size` ≤ Postgres `max_connections`. If that doesn't fit, put PgBouncer (transaction pooling) in front.
- Set a server-side `statement_timeout` as a backstop to the client-side timeout. Cancelling on the client side alone doesn't guarantee the server stops working.

## 4. Assumptions
1. Lookups within a batch are independent, and their per-query state updates are keyed or commutative, so they can run in parallel. If strict in-batch ordering matters, set `per_batch_concurrency=1`, or collect the results and apply them in one ordered transaction at the end.
2. The per-session lock only works **within one process**. Running on several replicas needs the distributed controls in §5.
3. `batch_id` is an idempotency key supplied by the caller. Without one, a random id is used and resuming isn't possible.
4. `_execute_vector_lookup` stays a simulation, as in the brief. A real implementation would run a pgvector query plus a state `UPDATE` on the connection it is given.
5. Python 3.11+ (`TaskGroup`, `asyncio.timeout`, `Task.cancelling`).

## 5. Production: distributed session state and rate limiting

**Session state consistency**
- **Postgres is the source of truth.** Use a `sessions` row with a `version` column and compare-and-set updates (`UPDATE … WHERE id=$1 AND version=$2`). A lost race gets retried instead of silently overwriting.
- **One writer per session.** Either route by `session_id` (a Kafka partition key or consistent hashing, which also gives ordering), or take `pg_advisory_xact_lock(hashtext(session_id))`. If you use Redis locks, give them a lease and a **fencing token** checked on write, because a lease alone is unsafe when a process pauses.
- Keep the **checkpoint table** shown above, and use the **outbox pattern** for side effects (events, downstream calls) so they are published exactly once relative to the state commit.

**Rate limiting**
- A per-tenant/session **token bucket in Redis**, updated atomically by a Lua script (or GCRA), shared by all replicas. Return `429` with `Retry-After` when the bucket is empty.
- Keep the local semaphore as a **hard per-process concurrency cap**. The global limit controls rate, the local one protects the process and its pool.
- Retry transient errors with **exponential backoff and jitter**, put a **circuit breaker** around the vector store, and add per-tenant fairness so one tenant can't take every slot.
- Monitor queue wait time, pool utilisation, timeout and rollback counts, and 429s.
