import asyncio
import json
import time
from typing import Any, Callable, Mapping
from uuid import uuid4

import asyncpg
import pytest

import asyncpg_queue
from asyncpg_queue.queue import put
from asyncpg_queue.worker import Worker


@pytest.fixture
def worker_run(  # type: ignore[no-untyped-def]
    event_loop: asyncio.AbstractEventLoop,
    postgres: str,
):
    worker = None
    worker_task = None

    def _worker_run(  # type: ignore[no-untyped-def]
        tasks: Mapping[str, Callable[..., Any]],
        burst: bool = True,
        **kwargs,
    ):
        nonlocal worker
        nonlocal worker_task

        # This will implicitly test that the housekeeping task recreates itself
        # by forcing the length of time the task holds its Postgres connection open
        # to be very short, thereby getting to the next iteration quickly.
        if "_housekeeping_seconds" not in kwargs:
            kwargs["_housekeeping_seconds"] = 0.1

        worker = Worker(
            postgres,
            tasks=tasks,
            burst=burst,
            **kwargs,
        )
        worker_task = event_loop.create_task(worker.run())
        event_loop.run_until_complete(worker_task)
        return worker

    yield _worker_run

    if worker:
        worker.shutdown()

    if worker_task and not worker_task.cancelled():
        event_loop.run_until_complete(worker_task)


@pytest.fixture
def put_task(  # type: ignore[no-untyped-def]
    db: asyncpg.Connection,
    event_loop: asyncio.AbstractEventLoop,
):
    def _put(
        func: Callable[..., Any], data: Any | None = None
    ) -> asyncpg_queue.queue.Task:
        task = event_loop.run_until_complete(put(db, func, data))
        return task

    yield _put


def test_worker_async_task_input_none(  # type: ignore[no-untyped-def]
    mocker,
    put_task,
    worker_run,
) -> None:
    f = mocker.AsyncMock()
    _ = put_task("f", None)
    worker_run({"f": f}, burst=True)
    f.assert_awaited_once()


_single_input_data = ("x", 1, True)
_array_input_data = (  # type: ignore[var-annotated]
    [],
    list(range(1000)),
    [True, "X", (1, 2, False)],
    tuple(range(1000)),
)
_map_input_data = (  # type: ignore[var-annotated]
    {},
    {"X": "x", 1: list(range(999))},
    {"Y": {"Z": [1, 2, None], "a": None}},
)


@pytest.mark.parametrize("data", _single_input_data)
def test_worker_async_task_single_input(  # type: ignore[no-untyped-def]
    data,
    mocker,
    put_task,
    worker_run,
) -> None:
    f = mocker.AsyncMock()
    _ = put_task("test_worker_async_task", data)
    worker_run({"test_worker_async_task": f}, burst=True)
    f.assert_awaited_once_with(data)


async def test_worker_default_timeout_int(postgres: str) -> None:
    w = Worker(postgres, tasks={}, timeout=1)
    assert float("inf") > w.default_timeout > 0


@pytest.mark.parametrize("data", _array_input_data)
def test_worker_async_task_input_list(  # type: ignore[no-untyped-def]
    data: list[Any],
    mocker,
    put_task,
    worker_run,
) -> None:
    f = mocker.AsyncMock()
    _ = put_task("test_worker_async_task", data)
    worker_run({"test_worker_async_task": f})
    f.assert_awaited_once_with(*json.loads(json.dumps(data)))


@pytest.mark.parametrize("data", _map_input_data)
def test_worker_async_task_input_map(  # type: ignore[no-untyped-def]
    data: dict[Any, Any],
    db: asyncpg.Connection,
    mocker,
    put_task,
    worker_run,
) -> None:
    f = mocker.AsyncMock()
    _ = put_task("test_worker_async_task", data)
    worker_run({"test_worker_async_task": f})
    f.assert_called_once_with(**json.loads(json.dumps(data)))


def test_worker_sync_task_none_input(  # type: ignore[no-untyped-def]
    mocker, put_task, worker_run
) -> None:
    f = mocker.MagicMock()
    _ = put_task("test_worker_async_task", None)
    worker_run({"test_worker_async_task": f})
    f.assert_called_once_with()


@pytest.mark.parametrize("data", _single_input_data)
def test_worker_sync_task_single_input(  # type: ignore[no-untyped-def]
    data: Any, mocker, put_task, worker_run
) -> None:
    f = mocker.MagicMock()
    _ = put_task("f", data)
    worker_run({"f": f})
    f.assert_called_once_with(data)


@pytest.mark.parametrize("data", _array_input_data)
def test_worker_sync_task_list_input(  # type: ignore[no-untyped-def]
    data: list[Any],
    mocker,
    put_task,
    worker_run,
) -> None:
    f = mocker.MagicMock()
    _ = put_task("f", data)
    worker_run({"f": f})
    f.assert_called_once_with(*json.loads(json.dumps(data)))


@pytest.mark.parametrize("data", _map_input_data)
def test_worker_sync_task_map_input(  # type: ignore[no-untyped-def]
    data: dict[Any, Any],
    db: asyncpg.Connection,
    mocker,
    put_task,
    worker_run,
) -> None:
    f = mocker.MagicMock()
    _ = put_task("test_worker_async_task", data)
    worker_run({"test_worker_async_task": f})
    f.assert_called_once_with(**json.loads(json.dumps(data)))


def test_worker_async_task_throws_exception(  # type:ignore[no-untyped-def]
    caplog,
    db: asyncpg.Connection,
    put_task,
    worker_run,
) -> None:
    async def f() -> None:
        raise Exception()

    caplog.set_level("ERROR")
    _ = put_task("test_worker_task_throws_exception")
    worker_run({"test_worker_task_throws_exception": f})
    assert "failed" in caplog.text


async def test_worker_sleeps(  # type: ignore[no-untyped-def]
    db: asyncpg.Connection,
    event_loop,
    mocker,
    postgres: str,
) -> None:
    f = mocker.MagicMock()
    await put(db, "test_worker_sleeps", None)
    worker = Worker(postgres, tasks={"test_worker_sleeps": f}, burst=False)
    worker_task = event_loop.create_task(worker.run())
    while not worker._awake.locked():
        await asyncio.sleep(0.01)
        continue
    assert worker._awake.locked()
    worker.shutdown()
    await asyncio.gather(worker_task)


async def test_worker_wakes_from_sleep(  # type: ignore[no-untyped-def]
    db: asyncpg.Connection, postgres: str, event_loop: asyncio.AbstractEventLoop, mocker
) -> None:
    f = mocker.MagicMock()
    worker = Worker(postgres, tasks={"test_worker_sleeps": f}, burst=False)
    worker_task = event_loop.create_task(worker.run())
    while not worker._awake.locked():
        await asyncio.sleep(0.01)
    await put(db, "test_worker_sleeps", None)
    while not f.call_count:
        await asyncio.sleep(0.01)
        continue
    assert f.called_once_with()
    worker.shutdown()
    await asyncio.gather(worker_task)


async def test_multiple_workers_single_call(  # type: ignore[no-untyped-def]
    db: asyncpg.Connection,
    event_loop: asyncio.AbstractEventLoop,
    postgres: str,
    mocker,
    tmp_path,
) -> None:
    def f() -> None:
        p = tmp_path / uuid4().hex
        p.touch()
        print(p)

    worker1 = Worker(
        postgres,
        tasks={"test_multiple_workers_single_call": f},
        concurrency=2,
        burst=False,
    )
    worker2 = Worker(
        postgres,
        tasks={"test_multiple_workers_single_call": f},
        concurrency=2,
        burst=False,
    )
    await put(db, "test_multiple_workers_single_call", None)
    worker_task1 = event_loop.create_task(worker1.run())
    worker_task2 = event_loop.create_task(worker2.run())
    while worker1.awake() and worker2.awake():
        await asyncio.sleep(0.1)
        continue
    assert len(list(tmp_path.iterdir())) == 1
    worker1.shutdown()
    worker2.shutdown()
    await asyncio.gather(worker_task2, worker_task1)


async def test_worker_cancellation_task_resume(  # type: ignore[no-untyped-def]
    db: asyncpg.Connection,
    postgres: str,
    event_loop: asyncio.AbstractEventLoop,
    mocker,
) -> None:
    started = False
    finished = False

    def f(t: int) -> None:
        nonlocal started
        nonlocal finished
        started = True

        time.sleep(t)
        finished = True

    spy = mocker.spy(f, "__call__")
    worker1 = Worker(
        postgres, tasks={"test_worker_cancellation_task_resume": spy}, burst=False
    )
    worker2 = Worker(
        postgres, tasks={"test_worker_cancellation_task_resume": spy}, burst=False
    )
    worker_task1 = event_loop.create_task(worker1.run())
    sleep_for = 5
    await put(db, "test_worker_cancellation_task_resume", sleep_for)
    while not started:
        await asyncio.sleep(0.1)
    worker1.shutdown()
    assert not finished
    worker_task2 = event_loop.create_task(worker2.run())
    while worker2.awake():
        await asyncio.sleep(0.1)
    assert finished
    worker2.shutdown()
    await asyncio.gather(worker_task1, worker_task2)


async def test_worker_handles_cancellation_error(  # type: ignore[no-untyped-def]
    event_loop: asyncio.AbstractEventLoop, mocker, postgres: str
) -> None:
    spy = mocker.spy(Worker, "shutdown")
    worker_task = event_loop.create_task(Worker(postgres, tasks={}).run())
    await asyncio.sleep(1)
    worker_task.cancel()
    await asyncio.gather(worker_task)
    spy.assert_called_once()


async def test_worker_shutdown(  # type: ignore[no-untyped-def]
    db: asyncpg.Connection,
    event_loop: asyncio.AbstractEventLoop,
    mocker,
    postgres: str,
    put_task,
) -> None:
    async def f(t: int) -> None:
        await asyncio.sleep(t)

    concurrency = 10
    worker = Worker(postgres, tasks={"test_worker_shutdown": f}, concurrency=10)
    worker_task = event_loop.create_task(worker.run())
    for _ in range(concurrency):
        await put(db, "test_worker_shutdown", 300)
    while len(worker._worker_tasks) < concurrency:
        await asyncio.sleep(0.1)
    worker.shutdown()
    await asyncio.gather(worker_task)
    assert len(worker._worker_tasks) == 0
