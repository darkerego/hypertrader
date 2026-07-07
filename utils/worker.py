#!/usr/bin/env python3
"""
Class-based asyncio task queue with bounded concurrency.

Runs up to `concurrency` async jobs at the same time and yields results as soon
as each job completes, not in submission order.

Run:
    python3 asyncio_task_queue_class.py
"""

from __future__ import annotations

import asyncio
import random
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable
from dataclasses import dataclass
from typing import Generic, TypeVar


T = TypeVar("T")


@dataclass(frozen=True)
class QueueResult(Generic[T]):
    """
    Result emitted by AsyncTaskQueue.

    index:
        Original zero-based submission index.

    ok:
        True when the coroutine returned normally.
        False when it raised an exception.

    value:
        Return value from the coroutine when ok=True.

    error:
        Exception raised by the coroutine when ok=False.

    elapsed:
        Runtime for this individual coroutine in seconds.
    """

    index: int
    ok: bool
    value: T | None
    error: BaseException | None
    elapsed: float


class AsyncTaskQueue(Generic[T]):
    """
    Async task queue with bounded concurrency.

    The queue accepts an iterable of zero-argument callables. Each callable must
    return an awaitable/coroutine when called.

    Example:
        jobs = [
            lambda item=item: process_item(item)
            for item in items
        ]

        queue = AsyncTaskQueue(jobs, concurrency=10)

        async for result in queue.run():
            print(result)

    Use callables instead of already-created coroutine objects so the queue
    controls when each coroutine is created and scheduled.
    """

    def __init__(
        self,
        jobs: Iterable[Callable[[], Awaitable[T]]],
        concurrency: int = 10,
    ) -> None:
        if concurrency < 1:
            raise ValueError("concurrency must be >= 1")

        self.jobs = list(jobs)
        self.concurrency = concurrency
        self._work_queue: asyncio.Queue[
            tuple[int, Callable[[], Awaitable[T]]] | None
        ] = asyncio.Queue()
        self._result_queue: asyncio.Queue[QueueResult[T]] = asyncio.Queue()
        self._workers: list[asyncio.Task[None]] = []
        self._started = False

    def __aiter__(self) -> AsyncIterator[QueueResult[T]]:
        return self.run()

    @property
    def total_jobs(self) -> int:
        return len(self.jobs)

    @property
    def worker_count(self) -> int:
        return min(self.concurrency, self.total_jobs) if self.total_jobs else 0

    async def run(self) -> AsyncIterator[QueueResult[T]]:
        """
        Run queued jobs and yield QueueResult objects as jobs complete.
        """

        if self._started:
            raise RuntimeError("AsyncTaskQueue instances can only be run once")

        self._started = True
        self._load_jobs()
        self._start_workers()

        try:
            for _ in range(self.total_jobs):
                yield await self._result_queue.get()
        finally:
            await self.aclose()

    def _load_jobs(self) -> None:
        for index, job in enumerate(self.jobs):
            self._work_queue.put_nowait((index, job))

        for _ in range(self.worker_count):
            self._work_queue.put_nowait(None)

    def _start_workers(self) -> None:
        self._workers = [
            asyncio.create_task(self._worker())
            for _ in range(self.worker_count)
        ]

    async def _worker(self) -> None:
        while True:
            item = await self._work_queue.get()

            try:
                if item is None:
                    return

                index, job = item
                result = await self._run_one(index=index, job=job)
                await self._result_queue.put(result)
            finally:
                self._work_queue.task_done()

    async def _run_one(
        self,
        index: int,
        job: Callable[[], Awaitable[T]],
    ) -> QueueResult[T]:
        started = time.perf_counter()

        try:
            value = await job()
            return QueueResult(
                index=index,
                ok=True,
                value=value,
                error=None,
                elapsed=time.perf_counter() - started,
            )
        except BaseException as exc:
            return QueueResult(
                index=index,
                ok=False,
                value=None,
                error=exc,
                elapsed=time.perf_counter() - started,
            )

    async def aclose(self) -> None:
        """
        Cancel any still-running workers and wait for them to exit.

        This is called automatically when run() exits, including when the
        consuming async-for loop breaks early.
        """

        for task in self._workers:
            if not task.done():
                task.cancel()

        if self._workers:
            await asyncio.gather(*self._workers, return_exceptions=True)


async def example_job(job_id: int) -> str:
    """
    Example async task.

    This sleeps for a random amount of time to prove results are yielded in
    completion order rather than submission order.
    """

    delay = random.uniform(0.1, 2.0)
    await asyncio.sleep(delay)

    if job_id == 17:
        raise RuntimeError("simulated failure for job 17")

    return f"job {job_id} finished after {delay:.2f}s"


async def main() -> None:
    total_jobs = 30

    jobs = [
        lambda job_id=job_id: example_job(job_id)
        for job_id in range(total_jobs)
    ]

    queue = AsyncTaskQueue(jobs=jobs, concurrency=10)

    async for result in queue:
        if result.ok:
            print(
                f"[ok]   index={result.index:02d} "
                f"elapsed={result.elapsed:.2f}s "
                f"value={result.value}"
            )
        else:
            print(
                f"[fail] index={result.index:02d} "
                f"elapsed={result.elapsed:.2f}s "
                f"error={type(result.error).__name__}: {result.error}"
            )


if __name__ == "__main__":
    asyncio.run(main())