import asyncpg
import pytest

from asyncpg_queue.bootstrap import VersionDoesNotExistException, bootstrap


@pytest.mark.asyncio
async def test_bootstrap_queue_table_exists(db: asyncpg.Connection) -> None:
    assert await db.fetchval(
        "SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name = 'queue')"  # noqa: E501
    )


@pytest.mark.asyncio
async def test_bootstrap_bad_version(db: asyncpg.Connection) -> None:
    with pytest.raises(VersionDoesNotExistException):
        await bootstrap(db, version_target="does not exist")


async def test_bootstrap_target_already_applied(  # type: ignore[no-untyped-def]
    db: asyncpg.Connection,
    caplog,
) -> None:
    caplog.set_level("INFO")
    await bootstrap(db)
    log_message = (
        "asyncpg-queue's database schema is up to date and does not require "
        "upgrading or bootstrap"
    )
    assert log_message in caplog.text
