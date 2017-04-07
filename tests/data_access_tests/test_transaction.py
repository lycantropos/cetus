from asyncio import AbstractEventLoop
from contextlib import suppress
from typing import List

import pytest
from cetus.data_access import (get_connection,
                               begin_transaction,
                               insert)
from cetus.types import RecordType
from sqlalchemy.engine.url import URL
from sqlalchemy.schema import Table
from tests.utils import fetch


@pytest.mark.asyncio
async def test_transactions(table: Table,
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
        with suppress(Exception):
            async with begin_transaction(connection=connection,
                                         is_mysql=is_mysql):
                await insert(
                    table_name=table_name,
                    columns_names=table_columns_names,
                    records=table_records,
                    is_mysql=is_mysql,
                    connection=connection)
                raise Exception()

        records = fetch(table=table,
                        db_uri=db_uri)
        assert not records
