from logging import getLogger
from pathlib import Path

import asyncpg
from asyncpg.exceptions import SyntaxOrAccessError

log = getLogger(__name__)


class VersionDoesNotExistException(ValueError):  # noqa: D101
    def __init__(self, *args: object) -> None:  # noqa: D107
        super().__init__(*args)


async def _bootstrap(
    connection: asyncpg.Connection,
    *,
    version_target: str,
) -> None:
    try:
        current_version = await connection.fetchval(
            "SELECT version FROM _asyncpg_queue_schema_version WHERE latest"
        )
    except SyntaxOrAccessError:
        current_version = None

    if current_version == version_target.strip(".sql"):
        log.info(
            "asyncpg-queue's database schema is up to date and does not require "
            "upgrading or bootstrap"
        )
        return
    versions_dir = Path(__file__).parent / "version"
    target = versions_dir / version_target
    if not target.exists():
        raise VersionDoesNotExistException(
            f"{target} does not exist, check the file path"
        )
    schema = target.read_text()
    async with connection.transaction():
        await connection.execute(schema)


async def bootstrap(
    db: asyncpg.Connection | str, *, version_target: str = "20230111.sql"
) -> None:
    """
    Create the Postgres objects that asyncpg-queue relies upon.

    Bootstrapping asyncpg-queue is a simple database migration that is executed within
    a transaction. Future version upgrades will rely on previous versions having been
    previously applied. If previous versions have not been applied, bootstrapping will
    apply them.

    :param db:
        Specifies a :class:`asyncpg.Connection` object or Postgres DSN to use for
        bootstrapping the database.
    :param version_target:
        Specifies the SQL file from the `version` directory to run.
    """
    if isinstance(db, asyncpg.Connection):
        await _bootstrap(db, version_target=version_target)
    else:
        try:
            connection = await asyncpg.connect(db)
            await _bootstrap(connection, version_target=version_target)
        finally:
            await connection.close()
