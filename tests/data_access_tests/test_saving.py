from typing import List

from sqlalchemy import Table
from sqlalchemy.engine.url import URL

from beylerbey.data_access import (get_connection,
                                   insert)
from beylerbey.types import RecordType
from tests.conftest import loop
from tests.utils import (sync,
                         fetch, loop)


@sync
async def test_insert(table: Table,
                      table_columns_names: List[str],
                      table_primary_key: str,
                      table_records: List[RecordType],
                      is_mysql: bool,
                      db_uri: URL) -> None:
    table_name = table.name
    unique_columns_names = [table_primary_key]

    async with get_connection(db_uri=db_uri,
                              is_mysql=is_mysql,
                              loop=loop) as connection:
        await insert(table_name=table_name,
                     columns_names=table_columns_names,
                     unique_columns_names=unique_columns_names,
                     records=table_records,
                     is_mysql=is_mysql,
                     connection=connection)

    records = await fetch(table=table,
                          db_uri=db_uri)

    assert all(table_record in records
               for table_record in table_records)
