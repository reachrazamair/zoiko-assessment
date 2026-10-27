"""Original implementation, reproduced verbatim from the assessment brief.

Kept unmodified so that ``test_session_manager.py`` can demonstrate which
suspected defects are real and which are not.
"""
import asyncio
from typing import List
import asyncpg

class AgentSessionManager:
    def __init__(self, db_pool: asyncpg.Pool):
        self.db_pool = db_pool
        self._active_tasks = {}

    async def process_batch_stream(
        self,
        session_id: str,
        queries: List[str]
    ):
        results = []

        for query in queries:
            task = asyncio.create_task(
                self._execute_vector_lookup(session_id, query)
            )

            self._active_tasks[session_id] = task
            results.append(await task)

        return results

    async def _execute_vector_lookup(
        self,
        session_id: str,
        query: str
    ):
        async with self.db_pool.acquire() as connection:
            # Simulated vector lookup and state update
            await asyncio.sleep(0.1)
            return f"Processed {query} for session {session_id}"
