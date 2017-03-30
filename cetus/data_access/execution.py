from functools import wraps
from typing import (Union,
                    Callable,
                    Coroutine,
                    List)

from asyncpg import PostgresError
from pymysql import Error

from cetus.types import (ConnectionType,
                         RecordType)


def handle_exceptions(function: Callable[..., Coroutine]):
    @wraps(function)
    async def decorated(query: str, *args, **kwargs):
        try:
            res = await function(query, *args, **kwargs)
            return res
        except (Error, PostgresError) as err:
            err_msg = ('Error while processing '
                       f'query: "{query}".')
            raise IOError(err_msg) from err

    return decorated


@handle_exceptions
async def execute(query: str, *args: RecordType,
                  is_mysql: bool,
                  connection: ConnectionType) -> Union[int, str]:
    if is_mysql:
        async with connection.cursor() as cursor:
            resp = await cursor.execute(query, args=args)
    else:
        resp = await connection.execute(query, *args)
    return resp


@handle_exceptions
async def execute_many(query: str, *,
                       args: List[RecordType],
                       is_mysql: bool,
                       connection: ConnectionType) -> None:
    if is_mysql:
        async with connection.cursor() as cursor:
            await cursor.executemany(query, args=args)
    else:
        await connection.executemany(query, args=args)
