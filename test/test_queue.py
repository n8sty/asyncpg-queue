import asyncpg
import pytest

from asyncpg_queue.queue import Task, pop, put


@pytest.fixture(scope="function")
async def task(db: asyncpg.Connection) -> Task:
    return await put(db, "func", {"x": 1, "y": "qwerty", "z": True})


@pytest.mark.asyncio
async def test_put(task: Task) -> None:
    assert task


async def test_put_callable(db: asyncpg.Connection) -> None:
    def tasky() -> None:
        pass

    task = await put(db, tasky, None)
    assert task.name == "test_put_callable.<locals>.tasky"


@pytest.mark.asyncio
async def test_pop(db: asyncpg.Connection, task: Task) -> None:
    popped_task = await pop(db, ("func",))
    assert popped_task


@pytest.mark.asyncio
async def test_pop_empty_queue(db: asyncpg.Connection) -> None:
    popped_task = await pop(db, ("nothing to see here",))
    assert popped_task is None
