from typing import Optional

from cetus.data_access.execution import execute
from cetus.queries import generate_update_query
from cetus.types import (ConnectionType,
                         ColumnValueType,
                         UpdatesType,
                         FiltersType)


async def update(
        *,
        table_name: str,
        updates: UpdatesType,
        filters: Optional[FiltersType] = None,
        is_mysql: bool,
        connection: ConnectionType) -> Optional[ColumnValueType]:
    query = generate_update_query(
        table_name=table_name,
        updates=updates,
        filters=filters,
        is_mysql=is_mysql)

    await execute(query,
                  is_mysql=is_mysql,
                  connection=connection)
