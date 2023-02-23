# asyncpg-queue

<h2 align="center">Postgres (asynchronous) queues</h1>

<p align="center">
<a href="https://github.com/n8sty/asyncpg-queue/blob/main/LICENSE"><img alt="License: MIT" src="https://black.readthedocs.io/en/stable/_static/license.svg"></a>
<a href="https://github.com/psf/black"><img alt="Code style: black" src="https://img.shields.io/badge/code%20style-black-000000.svg"></a>
<a href="https://github.com/charliermarsh/ruff"><img alt="Linter: ruff" src="https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/charliermarsh/ruff/main/assets/badge/v1.json">
</p>

Use Postgres to manage Python workloads asynchronously, powered by [asyncpg](https://github.com/MagicStack/asyncpg). asyncpg-queue is a simple library whose features include: 

* Ability to run both synchronous and asynchronous Python callables
* At-least-once execution of queued tasks
* Scales with the number of database connections available
* Dependency free apart from `asyncpg`
* Uses Postgres notification channels to not thrash the database with unnecessary polling

## Usage

To get started using asyncpg-queue, initialize the Postgres objects that it relies on:

```Python
import asyncpg
from asyncpg_queue import bootstrap

db = asyncpg.connect("postgresql://postgres@127.0.0.1:5432/postgres")
await bootstrap(db)
```

Now tasks can be enqueued for future processing. The `queue.put` method is naive and
should, in most cases be used within a transaction like in the following contrived
example:

```Python
from asyncpg_queue import queue

db = asyncpg.connect("postgresql://postgres@127.0.0.1:5432/postgres")

async with db.transaction():
    await db.execute(
        "INSERT INTO users (name, email) VALUES ($1, $2)",
        "Someone Like a User",
        "someone@example.com",
    )
    await queue.put(
        db,
        "send-welcome-email",
        data={
            "email": "someone@example.com",
            "name": "Someone Like a User",
            "stuff": "more of it"},
    )
```

The utility of using `put` within a transaction is that often tasks that are meant to
be processed asynchronously should only be enqueued if the generating process succeeds.
The above relies on the database transaction successfully being committed as a strong
indicator that the user was successfully created and therefore should receive a welcome
email. However, there is no requirement that `put` must be called within a transaction.

Processing tasks entails creating and running a worker process.

```Python
from asyncpg_queue import Worker


def send_welcome_email(email, name, **kwargs):
    print("sending a welcome email!")


worker = Worker(
    "postgresqlL//postgres@127.0.0.1:5432/postgres",
    tasks={
        "send-welcome-email": send_welcome_email,
    }
)
await worker.run()
```

Notice the `tasks` parameter passed as part of `Worker`'s initialization. This map
instructs the worker to process the "send-welcome-email" queue of tasks with the
specified function.

## Contributing

asyncpg-queue uses [Poetry](https://github.com/python-poetry/poetry) to manage its
dependencies, development tooling, and buiild.

```sh
poetry install
```

### Testing

A Docker container running Postgres is used during testing. Assuming that `docker` is
available on your system path at the time of running tests, the appropriate image(s)
will be pulled.

Tests are invoked by Pytest:

```sh
poetry run pytest test/
```

Alternatively, if you have a running Postgres instance and do not want to rely on
Docker, pass the DSN of a running Postgres instance that can be used during testing:

```sh
poetry run pytest  --postgres-dsn=postgresql://postgres@localhost:5433/postgres test/
```

### Formatting

Code formatting is enforced by [Black](https://github.com/psf/black):

```sh
poetry run black .
```

### Static analysis

Linting (and auto-fixing where possible) is done by [Ruff](https://github.com/charliermarsh/ruff/):
    
```sh
poetry run ruff check --fix .
```
    
Types are checked with [Mypy](https://github.com/python/mypy):

```sh
poetry run mypy --install-types ./asyncpg_queue/ ./test/
```

Unused code is checked by [Vulture](https://github.com/jendrikseipp/vulture):

```sh
poetry run vulture asyncpg_queue/ test/
```

## Motivation

Keep your project simple as long as possible! While simplicity is in the eye of the
beholder, the definition used here amounts to, refrain from adding additional tools
until necessary.

Many projects begin simply with a server and a data-store. Eventually, as the project
gains users and gathers complexity there may be a need for doing _something_ in a
separate process so as to not impede the main line. This something could be sending
emails by poking some email SaaS provider's API or calculating the total number of new
users of a particular feature at the end of the day. asyncpg-queue is meant for this
moment in an application's history.

asyncpg-queue and similar implementations have been successfully used to prolong or
forestall implementing queues and background workers with Redis, Celery or a variety
of other data stores. While many of these tools are not difficult to operate and PaaS
vendors often have a managed version, there is always an additional complexity cost
from introducing a new tool. asyncpg-queue should keep your toolset consistent since
it only relies on Python and Postgres.

## When _not_ to use

The primary caveat of this library is that if the database is the bottleneck in an
application deployment then using this tool will only add to the pressure on Postgres.
There will be more connections opened, more queries, and some additional data stored.
If any of those areas are problems, they will almost undoubtedly get worse with the
introduction of asyncpg-queue.

While fast enough, asyncpg-queue has little ability to ramp the performance of
producers (ie: adding to the queue) or consumers (ie: popping from the queue) because
of its reliance on Postgres. Only so much data can be written or read given a network
configuration and the server instance running Postgres. To play around with this idea
consult [`example/benchmark/producer.py`](example/benchmark/producer.py) and
[`example/benchmark/consumer.py`](example/benchmark/consumer.py) which should provide
estimates of the maximum read and write throughput of your setup.

asyncpg-queue is well suited for workloads that are mostly I/O. An example would be
calculating an end-of-day rollup table in Postgres that takes a long time to run.
However, asyncpg-queue is ill suited for running many CPU intensive tasks, like
training a neural network or performing the same end-of-day rollup in memory. In these
cases it's necessary to pay attention to the `concurrency` parameter of `Worker`.
