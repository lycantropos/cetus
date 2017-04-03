from typing import (Any, Optional,
                    List, Tuple)

from cetus.types import (FiltersType,
                         OrderingType)
from cetus.utils import join_str

from .utils import (ALL_COLUMNS_ALIAS,
                    ORDERS_ALIASES,
                    add_filters,
                    add_orderings,
                    add_groupings,
                    add_pagination,
                    check_query_parameters)


async def generate_select_query(
        *, table_name: str,
        columns_names: List[str],
        filters: Optional[FiltersType] = None,
        orderings: Optional[List[OrderingType]] = None,
        groupings: List[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None) -> str:
    await check_query_parameters(columns_names=columns_names)

    columns = join_str(columns_names)
    query = (f'SELECT {columns} '
             f'FROM {table_name} ')
    query = await add_filters(query,
                              filters=filters)
    query = await add_orderings(query,
                                orderings=orderings)
    query = await add_groupings(query,
                                groupings=groupings)
    query = await add_pagination(query,
                                 limit=limit,
                                 offset=offset)
    return query


async def generate_group_wise_query(
        *, table_name: str,
        columns_names: List[str],
        target_column_name: str,
        filters: Optional[FiltersType] = None,
        groupings: List[str],
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        orderings: Optional[List[OrderingType]] = None,
        is_maximum: bool,
        is_mysql: bool) -> str:
    await check_query_parameters(columns_names=columns_names,
                                 groupings=groupings)

    if is_mysql:
        query = await generate_mysql_group_wise_query(
            table_name=table_name,
            columns_names=columns_names,
            target_column_name=target_column_name,
            filters=filters,
            groupings=groupings,
            limit=limit,
            offset=offset,
            orderings=orderings,
            is_maximum=is_maximum)
    else:
        query = await generate_postgres_group_wise_query(
            table_name=table_name,
            columns_names=columns_names,
            target_column_name=target_column_name,
            groupings=groupings,
            limit=limit,
            offset=offset,
            orderings=orderings,
            is_maximum=is_maximum)
    return query


async def generate_mysql_group_wise_query(
        *, table_name: str,
        columns_names: List[str],
        target_column_name: str,
        filters: Optional[Tuple[str, Any]] = None,
        groupings: List[str],
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        orderings: Optional[List[OrderingType]] = None,
        is_maximum: bool) -> str:
    # based on article
    # http://mysql.rjweb.org/doc.php/groupwise_max
    columns = join_str(columns_names)
    groupings_str = join_str(groupings)
    operator = 'MAX' if is_maximum else 'MIN'
    query = (f'SELECT {groupings_str}, '
             f'{operator}({target_column_name}) AS {target_column_name} '
             f'FROM {table_name} ')
    query = await add_filters(query,
                              filters=filters)
    query = await add_pagination(query,
                                 limit=limit,
                                 offset=offset)
    query = await add_groupings(query,
                                groupings=groupings)
    query = (f'SELECT {columns} '
             f'FROM {table_name} '
             f'JOIN ({query}) as subquery '
             f'USING ({groupings_str}, {target_column_name}) ')
    query = await add_orderings(query,
                                orderings=orderings)
    return query


async def generate_postgres_group_wise_query(
        *, table_name: str,
        columns_names: List[str],
        target_column_name: str,
        filters: Optional[Tuple[str, Any]] = None,
        groupings: List[str],
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        orderings: Optional[List[OrderingType]] = None,
        is_maximum: bool) -> str:
    # based on article
    # https://explainextended.com/2009/11/26/postgresql-selecting-records-holding-group-wise-maximum/
    columns = join_str(columns_names)
    group_wise_orderings = await generate_group_wise_orderings(
        groupings=groupings,
        target_column_name=target_column_name,
        is_maximum=is_maximum)
    groupings_str = join_str(groupings)
    query = (f'SELECT DISTINCT ON ({groupings_str}) {ALL_COLUMNS_ALIAS} '
             f'FROM {table_name} ')
    query = await add_filters(query,
                              filters=filters)
    query = await add_orderings(query,
                                orderings=group_wise_orderings)
    query = (f'SELECT {columns} '
             f'FROM ({query}) AS subquery ')
    query = await add_orderings(query,
                                orderings=orderings)
    query = await add_pagination(query,
                                 limit=limit,
                                 offset=offset)
    return query


async def generate_group_wise_orderings(*,
                                        groupings: List[str],
                                        target_column_name: str,
                                        is_maximum: bool
                                        ) -> List[OrderingType]:
    ordering = (target_column_name,
                ORDERS_ALIASES['descending'] if is_maximum
                else ORDERS_ALIASES['ascending'])
    groupings_orderings = ((grouping, ORDERS_ALIASES['ascending'])
                           for grouping in groupings)
    return [*groupings_orderings, ordering]
