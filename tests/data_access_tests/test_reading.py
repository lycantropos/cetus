from typing import List

import pytest
from beylerbey.data_access import (get_connection,
                                   fetch,
                                   group_wise_fetch)
from beylerbey.types import RecordType
from hypothesis import strategies
from sqlalchemy import Table
from sqlalchemy.engine.url import URL
from tests.utils import (sync,
                         loop,
                         insert,
                         records_to_dicts)


@sync
async def test_fetch(table: Table,
                     table_columns_names: List[str],
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
        records = await fetch(table_name=table_name,
                              columns_names=table_columns_names,
                              is_mysql=is_mysql,
                              connection=connection)

    assert all(table_record in records
               for table_record in table_records)

    with pytest.raises(ValueError):
        async with get_connection(db_uri=db_uri,
                                  is_mysql=is_mysql,
                                  loop=loop) as connection:
            await fetch(table_name=table_name,
                        columns_names=[],
                        is_mysql=is_mysql,
                        connection=connection)


@pytest.fixture(scope='function')
def is_group_wise_maximum():
    return strategies.booleans().example()


@sync
async def test_group_wise_fetch(table: Table,
                                table_columns_names: List[str],
                                table_primary_key: str,
                                table_similar_records: List[RecordType],
                                is_group_wise_maximum: bool,
                                is_mysql: bool,
                                db_uri: URL) -> None:
    table_name = table.name
    non_unique_columns_names = [column.name
                                for column in table.columns
                                if not (column.primary_key or column.unique)]
    target_function = max if is_group_wise_maximum else min
    target_primary_key_value = target_function(record[0]
                                               for record in table_similar_records)

    table_similar_records_dicts = await records_to_dicts(
        records=table_similar_records,
        table=table)
    await insert(records_dicts=table_similar_records_dicts,
                 table=table,
                 db_uri=db_uri)

    async with get_connection(db_uri=db_uri,
                              is_mysql=is_mysql,
                              loop=loop) as connection:
        records = await group_wise_fetch(table_name=table_name,
                                         columns_names=table_columns_names,
                                         target_column_name=table_primary_key,
                                         groupings=non_unique_columns_names,
                                         is_maximum=is_group_wise_maximum,
                                         is_mysql=is_mysql,
                                         connection=connection)

    assert all(record in table_similar_records
               for record in records)
    assert len(records) == 1
    group_wise_maximum_record, = records
    group_wise_maximum_record_primary_key_value = group_wise_maximum_record[0]
    assert group_wise_maximum_record_primary_key_value == target_primary_key_value

    with pytest.raises(ValueError):
        async with get_connection(db_uri=db_uri,
                                  is_mysql=is_mysql,
                                  loop=loop) as connection:
            await group_wise_fetch(table_name=table_name,
                                   columns_names=[],
                                   target_column_name=table_primary_key,
                                   groupings=non_unique_columns_names,
                                   is_mysql=is_mysql,
                                   connection=connection)

    with pytest.raises(ValueError):
        async with get_connection(db_uri=db_uri,
                                  is_mysql=is_mysql,
                                  loop=loop) as connection:
            await group_wise_fetch(table_name=table_name,
                                   columns_names=table_columns_names,
                                   target_column_name=table_primary_key,
                                   groupings=[],
                                   is_mysql=is_mysql,
                                   connection=connection)
