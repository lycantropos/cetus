from typing import (Union,
                    Iterable,
                    Tuple)

from cetus.data_access.utils import handle_exceptions
from cetus.types import (ConnectionType,
                         RecordType)


@handle_exceptions
async def execute(query: str, *args: Tuple[RecordType],
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
                       args: Iterable[RecordType],
                       is_mysql: bool,
                       connection: ConnectionType) -> None:
    if is_mysql:
        async with connection.cursor() as cursor:
            await cursor.executemany(query, args=args)
    else:
        await connection.executemany(query, args=args)
