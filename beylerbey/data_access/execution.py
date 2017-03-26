from typing import (Union,
                    List)

from beylerbey.types import (ConnectionType,
                             RecordType)


async def execute(query: str, *args: RecordType,
                  is_mysql: bool,
                  connection: ConnectionType) -> Union[int, str]:
    if is_mysql:
        async with connection.cursor() as cursor:
            resp = await cursor.execute(query, args=args)
    else:
        resp = await connection.execute(query, *args)
    return resp


async def execute_many(query: str, *,
                       args: List[RecordType],
                       is_mysql: bool,
                       connection: ConnectionType) -> None:
    if is_mysql:
        async with connection.cursor() as cursor:
            await cursor.executemany(query, args=args)
    else:
        await connection.executemany(query, args=args)
