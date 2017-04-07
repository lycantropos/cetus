from asyncio import AbstractEventLoop
from typing import Optional

import aiomysql.sa
import asyncpg
from asyncio_extras import async_contextmanager
from cetus.types import (ConnectionType,
                         MySQLConnectionType,
                         PostgresConnectionType)
from sqlalchemy.engine.url import URL

DEFAULT_MYSQL_PORT = 3306
DEFAULT_POSTGRES_PORT = 5432

DEFAULT_MIN_CONNECTIONS_LIMIT = 10
DEFAULT_CONNECTION_TIMEOUT = 60


@async_contextmanager
async def get_connection_pool(
        *, db_uri: URL,
        is_mysql: bool,
        timeout: float = DEFAULT_CONNECTION_TIMEOUT,
        min_size: int = DEFAULT_MIN_CONNECTIONS_LIMIT,
        max_size: int,
        loop: AbstractEventLoop):
    if is_mysql:
        async with get_mysql_connection_pool(
                db_uri,
                timeout=timeout,
                min_size=min_size,
                max_size=max_size,
                loop=loop) as connection_pool:
            yield connection_pool
    else:
        async with get_postgres_connection_pool(
                db_uri,
                timeout=timeout,
                min_size=min_size,
                max_size=max_size,
                loop=loop) as connection_pool:
            yield connection_pool


@async_contextmanager
async def get_mysql_connection_pool(
        db_uri: URL, *,
        timeout: float = DEFAULT_CONNECTION_TIMEOUT,
        min_size: int = DEFAULT_MIN_CONNECTIONS_LIMIT,
        max_size: int,
        loop: AbstractEventLoop):
    # `None` port causes exceptions
    port = db_uri.port or DEFAULT_MYSQL_PORT
    # we use engine instead of plain connection pool
    # because `aiomysql` has transactions API
    # only for engine-based connections
    async with aiomysql.sa.create_engine(
            host=db_uri.host,
            port=port,
            user=db_uri.username,
            password=db_uri.password,
            db=db_uri.database,
            charset='utf8',
            connect_timeout=timeout,
            # TODO: check if `asyncpg` connections
            # are autocommit by default
            autocommit=True,
            minsize=min_size,
            maxsize=max_size,
            loop=loop) as engine:
        yield engine


@async_contextmanager
async def get_postgres_connection_pool(
        db_uri: URL, *,
        timeout: float = DEFAULT_CONNECTION_TIMEOUT,
        min_size: int = DEFAULT_MIN_CONNECTIONS_LIMIT,
        max_size: int,
        loop: AbstractEventLoop):
    # for symmetry with MySQL case
    port = db_uri.port or DEFAULT_POSTGRES_PORT
    async with asyncpg.create_pool(
            host=db_uri.host,
            port=port,
            user=db_uri.username,
            password=db_uri.password,
            database=db_uri.database,
            timeout=timeout,
            min_size=min_size,
            max_size=max_size,
            loop=loop) as pool:
        yield pool


@async_contextmanager
async def begin_transaction(
        *, connection: ConnectionType,
        is_mysql: bool):
    if is_mysql:
        async with begin_mysql_transaction(connection):
            yield
    else:
        async with begin_postgres_transaction(connection):
            yield


@async_contextmanager
async def begin_mysql_transaction(
        connection: MySQLConnectionType):
    transaction = connection.begin()
    async with transaction:
        yield


@async_contextmanager
async def begin_postgres_transaction(
        connection: PostgresConnectionType,
        *, isolation: str = 'read_committed',
        read_only: bool = False,
        deferrable: bool = False):
    transaction = connection.transaction(
        isolation=isolation,
        readonly=read_only,
        deferrable=deferrable)
    async with transaction:
        yield


@async_contextmanager
async def get_connection(
        *, db_uri: URL,
        is_mysql: bool,
        timeout: float = DEFAULT_CONNECTION_TIMEOUT,
        loop: AbstractEventLoop):
    if is_mysql:
        async with get_mysql_connection(
                db_uri,
                timeout=timeout,
                loop=loop) as connection:
            yield connection
    else:
        async with get_postgres_connection(
                db_uri,
                timeout=timeout,
                loop=loop) as connection:
            yield connection


@async_contextmanager
async def get_mysql_connection(
        db_uri: URL, *,
        timeout: float = DEFAULT_CONNECTION_TIMEOUT,
        loop: AbstractEventLoop):
    # `None` port causes exceptions
    port = db_uri.port or DEFAULT_MYSQL_PORT
    # we use engine-based connection
    # instead of plain connection
    # because `aiomysql` has transactions API
    # only for engine-based connections
    async with aiomysql.sa.create_engine(
            host=db_uri.host,
            port=port,
            user=db_uri.username,
            password=db_uri.password,
            db=db_uri.database,
            charset='utf8',
            connect_timeout=timeout,
            # TODO: check if `asyncpg` connections
            # are autocommit by default
            autocommit=True,
            minsize=1,
            maxsize=1,
            loop=loop) as engine:
        async with engine.acquire() as connection:
            yield connection


@async_contextmanager
async def get_postgres_connection(
        db_uri: URL, *,
        timeout: float = DEFAULT_CONNECTION_TIMEOUT,
        loop: AbstractEventLoop):
    # for symmetry with MySQL case
    port = db_uri.port or DEFAULT_POSTGRES_PORT
    connection = await asyncpg.connect(
        host=db_uri.host,
        port=port,
        user=db_uri.username,
        password=db_uri.password,
        database=db_uri.database,
        timeout=timeout,
        loop=loop)
    try:
        yield connection
    finally:
        await connection.close()
