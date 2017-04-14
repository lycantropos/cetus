from asyncio import AbstractEventLoop
from collections import OrderedDict
from typing import List

import pytest
from cetus.data_access import (get_connection,
                               update)
from cetus.types import (RecordType,
                         UpdatesType)
from sqlalchemy.engine.url import URL
from sqlalchemy.schema import Table
from tests.strategies.utils import values_strategies_by_python_types
from tests.utils import (insert,
                         fetch,
                         records_to_dicts,
                         is_sub_dictionary)


@pytest.fixture(scope='function')
def table_records_updates(table: Table,
                          table_unique_columns: List[str],
                          table_records: List[RecordType]
                          ) -> UpdatesType:
    records_dicts = records_to_dicts(records=table_records,
                                     table=table)
    res = OrderedDict()
    record_dict = records_dicts[0]
    for key, value in record_dict.items():
        if key in table_unique_columns:
            continue
        res[key] = values_strategies_by_python_types[type(value)].example()
    return res


@pytest.mark.asyncio
async def test_update(table: Table,
                      table_name: str,
                      table_records_updates: UpdatesType,
                      table_records: List[RecordType],
                      is_mysql: bool,
                      db_uri: URL,
                      event_loop: AbstractEventLoop
                      ) -> None:
    table_records_dicts = records_to_dicts(records=table_records,
                                           table=table)
    insert(records_dicts=table_records_dicts,
           table=table,
           db_uri=db_uri)
    async with get_connection(db_uri=db_uri,
                              is_mysql=is_mysql,
                              loop=event_loop) as connection:
        await update(table_name=table_name,
                     updates=table_records_updates,
                     is_mysql=is_mysql,
                     connection=connection)

    records = fetch(table=table,
                    db_uri=db_uri)
    records_dicts = records_to_dicts(records=records,
                                     table=table)
    assert all(is_sub_dictionary(sub_dictionary=table_records_updates,
                                 super_dictionary=record_dict)
               for record_dict in records_dicts)
