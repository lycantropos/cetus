from typing import List

from sqlalchemy import Table
from sqlalchemy.engine.url import URL

from beylerbey.data_access import (get_connection,
                                   delete)
from beylerbey.types import RecordType
from tests.conftest import loop
from tests.utils import (sync,
                         insert,
                         fetch, records_to_dicts, loop)


@sync
async def test_delete(table: Table,
                      table_records: List[RecordType],
                      is_mysql: bool,
                      db_uri: URL) -> None:
    table_name = table.name

    table_records_dicts = await records_to_dicts(
        records=table_records,
        table=table)
    await insert(records_dicts=table_records_dicts,
                 table=table,
                 db_uri=db_uri)

    async with get_connection(db_uri=db_uri,
                              is_mysql=is_mysql,
                              loop=loop) as connection:
        await delete(table_name=table_name,
                     is_mysql=is_mysql,
                     connection=connection)

    records = await fetch(table=table,
                          db_uri=db_uri)

    assert not records
