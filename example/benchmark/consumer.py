import asyncio
import logging
import sys
import time
from datetime import timedelta

from asyncpg_queue import bootstrap, Worker

logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s %(name)s %(levelname)s %(message)s"
)
logging.captureWarnings(True)


async def main(concurrency: int) -> None:
    counter = 0

    async def f() -> None:
        nonlocal counter
        counter += 1

    db = "postgresql://postgres@0.0.0.0:5433/postgres"

    await bootstrap(db)

    worker = Worker(
        db,
        tasks={"f": f},
        burst=True,
        concurrency=concurrency,
    )
    start = time.monotonic()
    await worker.run()
    end = time.monotonic()
    time_spent = end - start
    try:
        rate = time_spent / counter
    except ZeroDivisionError:
        rate = 0
    print(f"Processed {counter} tasks in {time_spent:.4}s ({rate:.2}s/task)")


if __name__ == "__main__":
    asyncio.run(main(int(sys.argv[1])))
