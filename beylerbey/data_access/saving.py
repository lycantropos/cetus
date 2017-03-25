from typing import List

from beylerbey.queries import generate_insert_query
from beylerbey.types import (ConnectionType,
                             RecordType)
from .execution import execute_many


async def insert(*, table_name: str,
                 columns_names: List[str],
                 unique_columns_names: List[str],
                 records: List[RecordType],
                 merge: bool = False,
                 connection: ConnectionType,
                 is_mysql: bool) -> None:
    query = await generate_insert_query(
        table_name=table_name,
        columns_names=columns_names,
        unique_columns_names=unique_columns_names,
        merge=merge,
        is_mysql=is_mysql)
    await execute_many(query,
                       args=records,
                       is_mysql=is_mysql,
                       connection=connection)
