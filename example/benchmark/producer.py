import asyncio
import random
import sys
import time
from datetime import timedelta

import asyncpg
import uvloop

from asyncpg_queue import bootstrap, queue

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


async def f() -> None:
    pass


async def main(i: int) -> None:
    db = "postgresql://postgres@0.0.0.0:5433/postgres"
    await bootstrap(db)
    connection = await asyncpg.connect(db)
    start = time.monotonic()
    for _ in range(i):
        await queue.put(connection, f, None)
    end = time.monotonic()
    time_spent = end - start
    print(f"Enqueued {i} tasks in {time_spent:.4}s ({(time_spent / i):.2}s/task)")


if __name__ == "__main__":
    asyncio.run(main(int(sys.argv[1])))
