import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Iterable
from uuid import UUID

import asyncpg


@dataclass(kw_only=True)
class Task:
    """
    Container for task data.

    :param id:
        The primary key identifier of the task stored in Postgres.
    :param name:
        The queue that the task is part of. Used to determine the processing
        function.
    :param enqueued:
        Time at which the task was written to Postgres.
    :param dequeued:
        Time at which the task was popped from Postgres to begin processing.
    :param data:
        Input(s) to the processing function.
    """

    id: UUID
    name: str
    enqueued: datetime
    dequeued: datetime | None = None
    data: Any = ...


async def put(
    db: asyncpg.Connection, queue: str | Callable[..., Any], data: Any
) -> Task:
    """
    Add a test to the queue.

    This method should be used within a Postgres transaction otherwise it will
    may enqueue work that should not be upon some other process upon which the
    queued work depends failing.

    :param db:
        A database connection, ideally with a previously opened transaction.

    :param queue:
        The name of the queue to add this task to. This name will later be used
        by a :class:`asyncpg_queue.worker.Worker` instance to map functions that
        will perform the work to tasks.

    :param data:
        A JSON serializable object or `None` that will be stored in Postgres and
        passed as input to the processing function.
    """
    task = await db.fetchrow(
        """
        INSERT INTO queue (name, data)
        VALUES ($1, $2)
        RETURNING id, name, enqueued, data
        """,
        queue if isinstance(queue, str) else queue.__qualname__,
        json.dumps(data),
    )
    return Task(**task)


async def pop(db: asyncpg.Connection, queues: Iterable[str]) -> Task | None:
    """
    Remove a task from the queue.

    This method should likely be used within a Postgres transaction otherwise it will
    instantly mark the given task as dequeued. While not particularly a problem, it may
    be unexpected.

    Tasks are popped in the order in which they were enqueud, filitered by the
    specified queues. Tasks that are currently locked for processing will not be
    popped until their lock is released.

    :param db:
        A database connection, ideally with a previously opened transaction.

    :param queues:
        Which queues should be considered for getting the next task for processing.
    """
    task = await db.fetchrow(
        """
        WITH task AS (
            SELECT id
            FROM queue
            WHERE dequeued IS NULL
                AND name = ANY($1)
            ORDER BY enqueued
            FOR UPDATE
            SKIP LOCKED
            LIMIT 1
        )
        UPDATE queue
        SET dequeued = NOW()
        FROM task
        WHERE queue.id = task.id
        RETURNING queue.id, queue.name, queue.enqueued, queue.dequeued, queue.data
        """,
        queues,
    )
    if not task:
        return None
    return Task(**task)
