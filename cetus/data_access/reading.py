from functools import partial
from typing import (Optional,
                    List,
                    Dict)

import pydevd

from .execution import (fetch_row,
                        fetch_rows)
from cetus.queries import (ALL_COLUMNS_ALIAS,
                           generate_select_query,
                           generate_group_wise_query)
from cetus.types import (ConnectionType,
                         RecordType,
                         ColumnValueType,
                         FiltersType,
                         OrderingType)

from .utils import (normalize_pagination,
                    generate_table_columns_names,
                    generate_table_columns_aliases)


async def fetch_column_function(
        *,
        column_function_name: str,
        table_name: str,
        column_name: str,
        filters: Optional[FiltersType] = None,
        orderings: Optional[List[OrderingType]] = None,
        is_mysql: bool,
        connection: ConnectionType,
        default: ColumnValueType) -> int:
    column_alias = f'{column_function_name}_1'
    function_column = (f'{column_function_name}({column_name}) '
                       f'AS {column_alias}')
    query = await generate_select_query(
        table_name=table_name,
        columns_names=[function_column],
        filters=filters,
        orderings=orderings)
    resp = await fetch_row(query,
                           is_mysql=is_mysql,
                           connection=connection)
    return resp[0] if resp is not None else default


fetch_max_column_value = partial(fetch_column_function,
                                 column_function_name='MAX',
                                 default=None)
fetch_records_count = partial(fetch_column_function,
                              column_function_name='COUNT',
                              column_name=ALL_COLUMNS_ALIAS,
                              default=0)


async def group_wise_fetch_column_function(
        *,
        column_function_name: str,
        table_name: str,
        column_name: str = ALL_COLUMNS_ALIAS,
        target_column_name: str,
        groupings: List[str],
        filters: Optional[FiltersType] = None,
        is_maximum: bool = True,
        is_mysql: bool,
        connection: ConnectionType,
        default: ColumnValueType = 0) -> int:
    column_alias = f'{column_function_name}_1'
    function_column = (f'{column_function_name}({column_name}) '
                       f'AS {column_alias}')
    query = await generate_group_wise_query(
        table_name=table_name,
        columns_names=[function_column],

        target_column_name=target_column_name,
        filters=filters,
        groupings=groupings,
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


async def fetch(
        *, table_name: str,
        columns_names: List[str],
        columns_aliases: Optional[Dict[str, str]] = None,
        filters: Optional[FiltersType] = None,
        orderings: Optional[List[OrderingType]] = None,
        groupings: List[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        is_mysql: bool,
        connection: ConnectionType) -> List[RecordType]:
    limit, offset = await normalize_pagination(
        limit=limit,
        offset=offset,
        is_mysql=is_mysql)

    columns_aliases = await generate_table_columns_aliases(
        columns_names=columns_names,
        columns_aliases=columns_aliases)
    columns_names = await generate_table_columns_names(
        columns_names=columns_names,
        columns_aliases=columns_aliases)

    query = await generate_select_query(
        table_name=table_name,
        columns_names=columns_names,
        filters=filters,
        orderings=orderings,
        groupings=groupings,
        limit=limit,
        offset=offset)

    resp = await fetch_rows(
        query,
        is_mysql=is_mysql,
        connection=connection)
    return resp


async def group_wise_fetch(
        *, table_name: str,
        columns_names: List[str],
        columns_aliases: Optional[Dict[str, str]] = None,
        target_column_name: str,
        groupings: List[str],
        filters: Optional[FiltersType] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        orderings: Optional[List[OrderingType]] = None,
        is_maximum: bool = True,
        is_mysql: bool,
        connection: ConnectionType) -> List[RecordType]:
    limit, offset = await normalize_pagination(
        limit=limit,
        offset=offset,
        is_mysql=is_mysql)

    columns_aliases = await generate_table_columns_aliases(
        columns_names=columns_names,
        columns_aliases=columns_aliases)
    columns_names = await generate_table_columns_names(
        columns_names=columns_names,
        columns_aliases=columns_aliases)

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

    resp = await fetch_rows(query,
                            is_mysql=is_mysql,
                            connection=connection)
    return resp


async def fetch_max_connections(*, is_mysql: bool,
                                connection: ConnectionType
                                ) -> int:
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
    return resp[0]
