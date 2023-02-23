import asyncio
import contextlib
import socket
import time
from typing import Generator
from uuid import uuid4

import asyncpg
import docker
import pytest

from asyncpg_queue.bootstrap import bootstrap


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--postgres-version",
        action="store",
        default="15",
        help="Major version of Postgres to use.",
    )
    parser.addoption(
        "--postgres-dsn",
        action="store",
        default=None,
        help=(
            "Connection string for already existing Postgres. Useful when running in "
            "CI or another environment where a Postgres instance is already running "
            "and available for using during test."
        ),
    )


@pytest.fixture
def event_loop() -> Generator[asyncio.AbstractEventLoop, None, None]:
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def postgres_version(request):  # type: ignore[no-untyped-def]
    return request.config.getoption("--postgres-version")


@pytest.fixture(scope="session")
def postgres_dsn(request):  # type: ignore[no-untyped-def]
    return request.config.getoption("--postgres-dsn")


@pytest.fixture(scope="session")
def docker_client() -> docker.DockerClient:
    return docker.from_env()


@pytest.fixture(scope="session")
def open_port() -> int:
    with contextlib.closing(socket.socket(type=socket.SOCK_STREAM)) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]  # type: ignore[no-any-return]


@pytest.fixture(scope="session", autouse=True)
def postgres(
    docker_client: docker.DockerClient,
    open_port: int,
    postgres_dsn: str | None,
    postgres_version: str,
) -> Generator[str, None, None]:
    if not postgres_dsn:
        container = docker_client.containers.run(
            f"postgres:{postgres_version}-alpine",
            detach=True,
            name=f"asyncpg-queue-test-{uuid4().hex}",
            environment={"POSTGRES_HOST_AUTH_METHOD": "trust"},
            ports={
                "5432/tcp": open_port,
            },
            remove=True,
        )
        postgres_dsn = f"postgresql://postgres@0.0.0.0:{open_port}/postgres"
    while True:
        time.sleep(0.1)
        try:
            asyncio.run(bootstrap(postgres_dsn))
            break
        except Exception:  # noqa: S112
            continue
    try:
        yield postgres_dsn
    finally:
        try:
            container.stop()
        # NameError will be thrown if the container wasn't created for some reason
        # so don't try to stop a container that hasn't been created ie: doesn'that
        # exist.
        except NameError:
            pass


@pytest.fixture
async def db(postgres: str) -> asyncpg.Connection:
    return await asyncpg.connect(postgres)
