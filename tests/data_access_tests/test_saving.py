from asyncio import AbstractEventLoop
from typing import List

import pytest

from cetus.data_access import (get_connection,
                               insert,
                               insert_returning)
from cetus.types import RecordType
from sqlalchemy import Table
from sqlalchemy.engine.url import URL
from tests.utils import fetch


@pytest.mark.asyncio
async def test_insert(table: Table,
                      table_name: str,
                      table_columns_names: List[str],
                      table_records: List[RecordType],
                      is_mysql: bool,
                      db_uri: URL,
                      event_loop: AbstractEventLoop
                      ) -> None:
    async with get_connection(db_uri=db_uri,
                              is_mysql=is_mysql,
                              loop=event_loop) as connection:
        await insert(table_name=table_name,
                     columns_names=table_columns_names,
                     records=table_records,
                     is_mysql=is_mysql,
                     connection=connection)

    records = fetch(table=table,
                    db_uri=db_uri)

    assert all(table_record in records
               for table_record in table_records)


@pytest.mark.asyncio
async def test_insert_returning(table_name: str,
                                table_columns_names: List[str],
                                table_primary_key: str,
                                table_records: List[RecordType],
                                is_mysql: bool,
                                db_uri: URL,
                                event_loop: AbstractEventLoop
                                ) -> None:
    async with get_connection(db_uri=db_uri,
                              is_mysql=is_mysql,
                              loop=event_loop) as connection:
        records = await insert_returning(
            table_name=table_name,
            columns_names=table_columns_names,
            returning_columns_names=table_columns_names,
            primary_key=table_primary_key,
            records=table_records,
            connection=connection,
            is_mysql=is_mysql)

    assert all(table_record in records
               for table_record in table_records)
