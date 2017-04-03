from itertools import chain
from typing import (Optional,
                    Iterable,
                    List)

from cetus.queries import (generate_insert_query,
                           generate_select_query,
                           generate_postgres_insert_returning_query)
from cetus.types import (ConnectionType,
                         RecordType,
                         ColumnValueType)

from .execution import (execute_many,
                        execute)
from .reading import fetch_query


async def insert(
        *, table_name: str,
        columns_names: List[str],
        unique_columns_names: Optional[List[str]] = None,
        records: Iterable[RecordType],
        merge: bool = False,
        connection: ConnectionType,
        is_mysql: bool) -> Optional[ColumnValueType]:
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


async def insert_returning(
        *, table_name: str,
        columns_names: List[str],
        unique_columns_names: Optional[List[str]] = None,
        returning_columns_names: List[str],
        records: Iterable[RecordType],
        merge: bool = False,
        connection: ConnectionType,
        is_mysql: bool) -> Optional[ColumnValueType]:
    if is_mysql:
        insert_query = await generate_insert_query(
            table_name=table_name,
            columns_names=columns_names,
            unique_columns_names=unique_columns_names,
            merge=merge,
            is_mysql=is_mysql)

        for record in records:
            await execute(insert_query,
                          *record,
                          is_mysql=is_mysql,
                          connection=connection)
        primary_key = unique_columns_names[0]
        resp = await fetch_query(
            f'SELECT LAST_INSERT_ID({primary_key}) '
            f'FROM {table_name}',
            is_mysql=is_mysql,
            connection=connection)
        primary_key_values = [row[0] for row in resp]
        filters = 'IN', (primary_key, primary_key_values)
        query = await generate_select_query(table_name=table_name,
                                            columns_names=returning_columns_names,
                                            filters=filters)
        resp = await fetch_query(query,
                                 is_mysql=is_mysql,
                                 connection=connection)
    else:
        query = await generate_postgres_insert_returning_query(
            table_name=table_name,
            columns_names=columns_names,
            unique_columns_names=unique_columns_names,
            returning_columns_names=returning_columns_names,
            merge=merge)
        resp = list(chain.from_iterable(
            [await fetch_query(query,
                               *record,
                               is_mysql=is_mysql,
                               connection=connection)
             for record in records]))
    return resp
