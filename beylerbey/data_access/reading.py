from functools import partial
from typing import (Optional,
                    List, Tuple)

from beylerbey.queries import (ALL_COLUMNS_ALIAS,
                               generate_select_query,
                               generate_group_wise_query)
from beylerbey.types import (ConnectionType,
                             RecordType,
                             ColumnValueType,
                             FiltersType,
                             OrderingType)

from .utils import (normalize_pagination,
                    normalize_record)


async def fetch_column_function(
        *, table_name: str,
        column_name: str,
        filters: Optional[FiltersType] = None,
        orderings: Optional[List[OrderingType]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        column_function_name: str,
        is_mysql: bool,
        connection: ConnectionType,
        default: ColumnValueType = 0) -> int:
    limit, offset = await normalize_pagination(limit=limit,
                                               offset=offset,
                                               is_mysql=is_mysql)
    function_column = f'{column_function_name}({column_name})'
    query = await generate_select_query(
        table_name=table_name,
        columns_names=[function_column],
        filters=filters,
        orderings=orderings,
        limit=limit,
        offset=offset)
    resp = await fetch_row(query,
                           is_mysql=is_mysql,
                           connection=connection)
    return resp[0] if resp is not None else default


fetch_max_column_value = partial(fetch_column_function,
                                 column_function_name='MAX')
fetch_records_count = partial(fetch_column_function,
                              column_function_name='COUNT',
                              column_name=ALL_COLUMNS_ALIAS)


async def group_wise_fetch_column_function(
        *, table_name: str,
        column_name: str = ALL_COLUMNS_ALIAS,
        column_function_name: str,
        target_column_name: str,
        groupings: List[str],
        filters: Optional[FiltersType] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        is_maximum: bool = True,
        is_mysql: bool,
        connection: ConnectionType,
        default: ColumnValueType = 0) -> int:
    limit, offset = await normalize_pagination(limit=limit,
                                               offset=offset,
                                               is_mysql=is_mysql)

    function_column = f'{column_function_name}({column_name})'
    query = await generate_group_wise_query(
        table_name=table_name,
        columns_names=[function_column],
        target_column_name=target_column_name,
        filters=filters,
        groupings=groupings,
        limit=limit,
        offset=offset,
        is_maximum=is_maximum,
        is_mysql=is_mysql)

    resp = await fetch_row(query,
                           is_mysql=is_mysql,
                           connection=connection)
    return resp[0] if resp is not None else default


group_wise_fetch_max_column_value = partial(group_wise_fetch_column_function,
                                            column_function_name='MAX')
group_wise_fetch_records_count = partial(group_wise_fetch_column_function,
                                         column_function_name='COUNT')


async def fetch(*, table_name: str,
                columns_names: List[str],
                filters: Optional[FiltersType] = None,
                orderings: Optional[List[OrderingType]] = None,
                limit: Optional[int] = None,
                offset: Optional[int] = None,
                is_mysql: bool,
                connection: ConnectionType) -> List[RecordType]:
    limit, offset = await normalize_pagination(limit=limit,
                                               offset=offset,
                                               is_mysql=is_mysql)
    query = await generate_select_query(
        table_name=table_name,
        columns_names=columns_names,
        filters=filters,
        orderings=orderings,
        limit=limit,
        offset=offset)

    resp = await fetch_columns(query,
                               columns_names=columns_names,
                               is_mysql=is_mysql,
                               connection=connection)
    return resp


async def group_wise_fetch(
        *, table_name: str,
        columns_names: List[str],
        target_column_name: str,
        groupings: List[str],
        filters: Optional[FiltersType] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        orderings: Optional[List[OrderingType]] = None,
        is_maximum: bool = True,
        is_mysql: bool,
        connection: ConnectionType) -> List[RecordType]:
    limit, offset = await normalize_pagination(limit=limit,
                                               offset=offset,
                                               is_mysql=is_mysql)

    query = await generate_group_wise_query(
        table_name=table_name,
        columns_names=columns_names,
        target_column_name=target_column_name,
        filters=filters,
        groupings=groupings,
        limit=limit,
        offset=offset,
        orderings=orderings,
        is_maximum=is_maximum,
        is_mysql=is_mysql)

    resp = await fetch_columns(query,
                               columns_names=columns_names,
                               is_mysql=is_mysql,
                               connection=connection)
    return resp


async def fetch_row(query: str, *,
                    is_mysql: bool,
                    connection: ConnectionType) -> ColumnValueType:
    if is_mysql:
        async with connection.cursor() as cursor:
            await cursor.execute(query)
            resp = await cursor.fetchone()
            return resp
    else:
        resp = await connection.fetchrow(query)
        return resp


async def fetch_columns(query: str,
                        *args: Tuple[ColumnValueType],
                        columns_names: List[str],
                        is_mysql: bool,
                        connection: ConnectionType
                        ) -> List[RecordType]:
    if is_mysql:
        async with connection.cursor() as cursor:
            await cursor.execute(query, args=args)
            return [row async for row in cursor]
    else:
        resp = await connection.fetch(query, *args)
        return [await normalize_record(row,
                                       columns_names=columns_names)
                for row in resp]


async def fetch_max_connections(*, is_mysql: bool,
                                connection: ConnectionType) -> int:
    setting_name = 'max_connections'
    if is_mysql:
        resp = await fetch_mysql_setting(
            setting_name=setting_name,
            connection=connection)
    else:
        resp = await fetch_postgres_setting(
            setting_name=setting_name,
            connection=connection)
    return int(resp)


async def fetch_mysql_setting(*, setting_name: str,
                              connection: ConnectionType
                              ) -> ColumnValueType:
    resp = await fetch_row(
        f'SHOW VARIABLES LIKE \'{setting_name}\'',
        is_mysql=True,
        connection=connection)
    return resp[1]


async def fetch_postgres_setting(*, setting_name: str,
                                 connection: ConnectionType
                                 ) -> str:
    resp = await fetch_row(f'SHOW {setting_name}',
                           is_mysql=False,
                           connection=connection)
    return resp[setting_name]
