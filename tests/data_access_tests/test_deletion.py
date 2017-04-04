from asyncio import AbstractEventLoop
from typing import List

import pytest
from sqlalchemy import Table
from sqlalchemy.engine.url import URL

from cetus.data_access import (get_connection,
                               delete)
from cetus.types import RecordType
from tests.utils import (insert,
                         fetch,
                         records_to_dicts)


@pytest.mark.asyncio
async def test_delete(table: Table,
                      table_name: str,
                      table_records: List[RecordType],
                      is_mysql: bool,
                      db_uri: URL,
                      event_loop: AbstractEventLoop
                      ) -> None:
    table_records_dicts = records_to_dicts(
        records=table_records,
        table=table)
    insert(records_dicts=table_records_dicts,
           table=table,
           db_uri=db_uri)

    async with get_connection(db_uri=db_uri,
                              is_mysql=is_mysql,
                              loop=event_loop) as connection:
        await delete(table_name=table_name,
                     is_mysql=is_mysql,
                     connection=connection)

    records = fetch(table=table,
                    db_uri=db_uri)

    assert not records
