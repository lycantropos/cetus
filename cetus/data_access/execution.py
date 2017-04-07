from typing import (Union,
                    Iterable,
                    Tuple, List)

from cetus.types import (ConnectionType,
                         RecordType,
                         ColumnValueType)

from .utils import handle_exceptions


@handle_exceptions
async def execute(query: str,
                  *args: Tuple[RecordType],
                  is_mysql: bool,
                  connection: ConnectionType
                  ) -> Union[int, str]:
    if is_mysql:
        async with connection.connection.cursor() as cursor:
            resp = await cursor.execute(query, args=args)
    else:
        resp = await connection.execute(query, *args)
    return resp


@handle_exceptions
async def execute_many(query: str, *,
                       args: Iterable[RecordType],
                       is_mysql: bool,
                       connection: ConnectionType
                       ) -> None:
    if is_mysql:
        async with connection.connection.cursor() as cursor:
            await cursor.executemany(query, args=args)
    else:
        await connection.executemany(query, args=args)


@handle_exceptions
async def fetch_row(query: str, *,
                    is_mysql: bool,
                    connection: ConnectionType
                    ) -> RecordType:
    if is_mysql:
        async with connection.connection.cursor() as cursor:
            await cursor.execute(query)
            resp = await cursor.fetchone()
            return resp
    else:
        resp = await connection.fetchrow(query)
        if resp is not None:
            return tuple(resp.values())


@handle_exceptions
async def fetch_rows(query: str,
                     *args: Tuple[ColumnValueType],
                     is_mysql: bool,
                     connection: ConnectionType
                     ) -> List[RecordType]:
    if is_mysql:
        async with connection.connection.cursor() as cursor:
            await cursor.execute(query, args=args)
            return [row async for row in cursor]
    else:
        resp = await connection.fetch(query, *args)
        return [tuple(row.values()) for row in resp]
