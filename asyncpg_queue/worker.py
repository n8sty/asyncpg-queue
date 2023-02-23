import asyncio
import contextvars
import functools
import json
import os
import signal
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from logging import getLogger
from threading import current_thread
from typing import Any, Callable, Mapping, Self

import asyncpg

from asyncpg_queue.queue import pop

log = getLogger(__name__)


class Worker:
    """
    Create a worker that processes the specified task queues stored in Postgres.

    :param dsn:
        Postgres connection string that follows the form specified by
        https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING.
    :param tasks:
        Container of tasks specifying a mapping of callables to queues for processing
        by the worker instance.
    :param burst:
        Upon the worker exhausting the specified queues, shut the worker down after
        all task executions have been completed or failed.
    :param concurrency:
        The maximum number of concurrent tasks that can be processed at once. This
        number is synonomous with the number of database connections plus one that will
        be instantiated _and_ the number of threads that will be used to create a
        :class:`concurrent.futures.ThreadPoolExecutor` for processing synchronous
        tasks. This number has one added to it to account for a background housekeeping
        task that is used for handling Postgres notifications among other activities.
    :param timeout:
        Default timeout in seconds for each task instance. Task instances that exceed
        this threshold will be cancelled by a :exc:`TimeoutError`. To disable default
        task timeouts use `None`.
    """

    __slots__ = (
        "burst",
        "concurrency",
        "default_timeout",
        "tasks",
        "_awake",
        "_dsn",
        "_housekeeping_seconds",
        "_loop",
        "_running_tasks",
        "_shutdown_event",
        "_wait_for_seconds",
        "_worker_tasks",
    )

    _worker_tasks: set[asyncio.Future[Any]]

    def __init__(
        self: Self,
        dsn: str,
        *,
        tasks: Mapping[str, Callable[..., Any]],
        burst: bool = False,
        concurrency: int = 9,
        timeout: timedelta | float | int | None = timedelta(minutes=1),  # noqa: B008
        _housekeeping_seconds: float | int = 300
    ) -> None:
        """Initialize a worker instance."""
        self.burst = burst
        self.concurrency = concurrency
        if not isinstance(timeout, timedelta):
            self.default_timeout = timeout or float("inf")
        else:
            self.default_timeout = timeout.total_seconds()
        self.tasks = tasks
        self._awake = asyncio.Lock()
        self._dsn = dsn
        self._housekeeping_seconds = _housekeeping_seconds
        self._loop = asyncio.get_event_loop()
        self._shutdown_event = asyncio.Event()
        self._wait_for_seconds = float(min(0.5, self.default_timeout))
        self._worker_tasks = set()

    async def run(self: Self) -> None:
        """Run the worker."""
        self._install_signal_handlers()
        pid = os.getpid()
        log.info("Running worker (%s)", pid)
        async with asyncpg.create_pool(
            self._dsn, max_size=self.concurrency + 1, min_size=2
        ) as pool:
            with ThreadPoolExecutor(
                max_workers=min(32, os.cpu_count() or 1 + 4, self.concurrency),
                thread_name_prefix="asyncpg-queue",
                initializer=self._thread_initializer,
            ) as executor:
                log.debug("Worker beginning processing tasks")

                housekeeping_task = asyncio.create_task(
                    self._housekeeping(pool), name="asyncpg-queue-housekeeping"
                )
                self._worker_tasks.add(housekeeping_task)
                housekeeping_task.add_done_callback(self._task_done_callback)

                while not self._shutdown_event.is_set():
                    try:
                        if len(self._worker_tasks) >= self.concurrency:
                            done, pending = await asyncio.wait(
                                self._worker_tasks,
                                timeout=self._wait_for_seconds,
                                return_when=asyncio.ALL_COMPLETED,
                            )
                            log.debug(
                                "Concurrency threshold of %s reached, waiting for %ss, "
                                "%s tasks completed, %s tasks pending",
                                self.concurrency,
                                self._wait_for_seconds,
                                len(done),
                                len(pending),
                            )
                            continue
                        if not self.awake():
                            log.debug("Worker is locked, waiting for tasks to appear")
                            if self.burst:
                                self.shutdown()
                                log.info(
                                    "Shutting down burst worker, queue exhausted",
                                )
                                break
                            await asyncio.sleep(1)
                        else:
                            task = asyncio.create_task(self._run(pool, executor))
                            self._worker_tasks.add(task)
                            task.add_done_callback(self._task_done_callback)
                            log.debug("Created worker task %s", task.get_name())
                    except asyncio.CancelledError:
                        self.shutdown()
                        break

                log.debug("Waiting on outstanding worker tasks to finish")

                await asyncio.gather(
                    *self._worker_tasks, housekeeping_task, return_exceptions=True
                )
            log.debug("Thread pool executor closed")
        log.debug("Database connection pool closed")
        log.info("Worker run finished (%s)", pid)

    async def _run(
        self: Self,
        pool: asyncpg.Pool,
        executor: ThreadPoolExecutor,
    ) -> None:
        async with pool.acquire() as connection:
            async with connection.transaction():
                task = await pop(connection, self.tasks.keys())
                if not task:
                    if not self.awake():
                        log.debug("Worker already paused by lock")
                        return
                    await self._awake.acquire()
                    log.debug(
                        "No tasks in queue, lock set to pause worker task creation"
                    )
                    return
                log.debug("Beginning task (%s) %s", task.name, task.id)
                dequeued = task.dequeued or datetime.utcnow()
                function = self.tasks[task.name]
                data = json.loads(task.data)
                if asyncio.iscoroutinefunction(function):
                    if data is None:
                        coro = function()
                    elif isinstance(data, list):
                        coro = function(*data)
                    elif isinstance(data, dict):
                        coro = function(**data)
                    else:
                        coro = function(data)
                else:
                    ctx = contextvars.copy_context()
                    if data is None:
                        coro = self._loop.run_in_executor(
                            executor,
                            functools.partial(ctx.run, function),
                        )
                    elif isinstance(data, list):
                        coro = self._loop.run_in_executor(
                            executor,
                            functools.partial(ctx.run, function, *data),
                        )
                    elif isinstance(data, dict):
                        coro = self._loop.run_in_executor(
                            executor,
                            functools.partial(ctx.run, function, **data),
                        )
                    else:
                        coro = self._loop.run_in_executor(
                            executor,
                            functools.partial(ctx.run, function, data),
                        )
                try:
                    result = await asyncio.wait_for(coro, timeout=self.default_timeout)
                    log.debug(
                        "Got result %s from job %s after %.3gs",
                        result,
                        task.id,
                        (datetime.utcnow() - dequeued).total_seconds(),
                    )
                except TimeoutError:
                    log.error(
                        "Task (%s) %s timed out (%ss)",
                        task.name,
                        task.id,
                        self.default_timeout,
                    )
                except Exception:
                    log.exception(
                        "Task (%s) %s failed (%ss)",
                        task.name,
                        task.id,
                        self.default_timeout,
                    )
                await connection.execute(
                    "DELETE FROM queue WHERE id = $1",
                    task.id,
                )

    def awake(self) -> bool:
        """
        Is the worker currently processing tasks?.

        An awake worker is one that is querying Postgres for enqueued tasks to pop
        from the queue and then attempt to process.
        """
        return not self._awake.locked()

    async def _wake(
        self: Self,
        conn: asyncpg.Connection,
        pid: int,
        channel: str,
        payload: Any,
    ) -> None:
        del conn
        if self.awake():
            return  # pragma: no cover
        self._awake.release()
        log.debug(
            "Received notification from Postgres channel to release lock and wake "
            "worker",
            extra={"pid": pid, "channel": channel, "payload": payload},
        )

    def shutdown(self) -> None:
        """
        Shutdown the worker and associated tasks.

        Cancels all outstanding worker tasks, usually invoked when handling signals.
        """
        log.debug("Beginning shutdown")
        self._shutdown_event.set()
        for task in self._worker_tasks:
            if not task.done():  # pragma: no cover
                task.cancel()
        log.debug("All worker tasks cancelled")

    async def _housekeeping(
        self: Self,
        pool: asyncpg.Pool,
    ) -> None:
        """
        Perform housekeeping tasks to keep the worker instance happy.

        Housekeeping is currently a single action: Open a long-lived Postgres
        connection to receive any notifications on the appropriate channel that are
        used to wake the worker so that it can begin processing tasks.
        """
        if self._shutdown_event.is_set():
            return  # pragma: no cover
        if pool._closed:
            return  # pragma: no cover
        try:
            async with pool.acquire() as connection:  # pragma: no cover
                try:
                    await connection.add_listener("asyncpg_queue_task", self._wake)
                    try:
                        await asyncio.sleep(self._housekeeping_seconds)
                    except asyncio.CancelledError:
                        return
                finally:
                    # Remove the listener before returning the connectionn to the
                    # pool to prevent asyncpg from raising a warning.
                    await connection.remove_listener(
                        "asyncpg_queue_task",
                        self._wake,
                    )
        except (
            asyncpg.InterfaceError,
            asyncpg.ConnectionDoesNotExistError,
        ):  # pragma: no cover
            # This can happen if the pool has been closed already. Handling the error
            # by ignoring it prevents some scary looking errors from propagating up.
            return
        except asyncio.CancelledError:
            return

        housekeeping_task = asyncio.create_task(
            self._housekeeping(pool), name="asyncpg-queue-housekeeping"
        )
        log.debug("Recreated Housekeeping")
        self._worker_tasks.add(housekeeping_task)
        housekeeping_task.add_done_callback(self._task_done_callback)
        log.debug("Finished housekeeping task")

    def _thread_initializer(self) -> None:
        log.debug(
            "Initializing worker thread", extra={"thread_name": current_thread().name}
        )

    def _task_done_callback(self: Self, t: asyncio.Future[Any]) -> None:
        self._worker_tasks.remove(t)
        try:
            t.exception()
        except asyncio.CancelledError:
            log.exception("Task cancelled during processing")

    def _install_signal_handlers(self: Self) -> None:
        self._loop.add_signal_handler(signal.SIGHUP, self.shutdown)
        self._loop.add_signal_handler(signal.SIGTERM, self.shutdown)
