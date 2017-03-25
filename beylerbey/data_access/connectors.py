from asyncio import AbstractEventLoop
from typing import Optional

import aiomysql
import asyncpg
from asyncio_extras import async_contextmanager
from sqlalchemy.engine.url import URL

DEFAULT_MYSQL_PORT = 3306
DEFAULT_POSTGRES_PORT = 5432


@async_contextmanager
async def get_connection_pool(*, db_uri: URL,
                              is_mysql: bool,
                              timeout: Optional[float] = None,
                              max_size: int,
                              loop: AbstractEventLoop):
    if is_mysql:
        async with get_mysql_connection_pool(db_uri,
                                             timeout=timeout,
                                             max_size=max_size,
                                             loop=loop) as connection_pool:
            yield connection_pool
    else:
        async with get_postgres_connection_pool(db_uri,
                                                timeout=timeout,
                                                max_size=max_size,
                                                loop=loop) as connection_pool:
            yield connection_pool


@async_contextmanager
async def get_mysql_connection_pool(db_uri: URL, *,
                                    timeout: Optional[float],
                                    max_size: int,
                                    loop: AbstractEventLoop):
    # `None` port causes exceptions
    port = db_uri.port or DEFAULT_MYSQL_PORT
    async with aiomysql.create_pool(host=db_uri.host,
                                    port=port,
                                    user=db_uri.username,
                                    password=db_uri.password,
                                    db=db_uri.database,
                                    charset='utf8',
                                    connect_timeout=timeout,
                                    # TODO: check if `asyncpg` connections
                                    # are autocommit by default
                                    autocommit=True,
                                    maxsize=max_size,
                                    loop=loop) as pool:
        yield pool


@async_contextmanager
async def get_postgres_connection_pool(db_uri: URL, *,
                                       timeout: Optional[float],
                                       max_size: int,
                                       loop: AbstractEventLoop):
    # for symmetry with MySQL case
    port = db_uri.port or DEFAULT_POSTGRES_PORT
    async with asyncpg.create_pool(host=db_uri.host,
                                   port=port,
                                   user=db_uri.username,
                                   password=db_uri.password,
                                   database=db_uri.database,
                                   timeout=timeout,
                                   max_size=max_size,
                                   loop=loop) as pool:
        yield pool


@async_contextmanager
async def get_connection(*, db_uri: URL,
                         is_mysql: bool,
                         timeout: Optional[float] = None,
                         loop: AbstractEventLoop):
    if is_mysql:
        async with get_mysql_connection(db_uri,
                                        timeout=timeout,
                                        loop=loop) as connection:
            yield connection
    else:
        async with get_postgres_connection(db_uri,
                                           timeout=timeout,
                                           loop=loop) as connection:
            yield connection


@async_contextmanager
async def get_mysql_connection(db_uri: URL, *,
                               timeout: Optional[float],
                               loop: AbstractEventLoop):
    # `None` port causes exceptions
    port = db_uri.port or DEFAULT_MYSQL_PORT
    connection = await aiomysql.connect(host=db_uri.host,
                                        port=port,
                                        user=db_uri.username,
                                        password=db_uri.password,
                                        db=db_uri.database,
                                        charset='utf8',
                                        connect_timeout=timeout,
                                        # TODO: check if `asyncpg` connections
                                        # are autocommit by default
                                        autocommit=True,
                                        loop=loop)
    try:
        yield connection
    finally:
        connection.close()


@async_contextmanager
async def get_postgres_connection(db_uri: URL, *,
                                  timeout: Optional[float],
                                  loop: AbstractEventLoop):
    # for symmetry with MySQL case
    port = db_uri.port or DEFAULT_POSTGRES_PORT
    connection = await asyncpg.connect(host=db_uri.host,
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
