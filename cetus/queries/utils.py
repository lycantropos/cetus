from typing import (Any, Optional,
                    List, Tuple,
                    Dict)

from cetus.types import OrderingType
from cetus.utils import join_str
from .filters import filters_to_str

ALL_COLUMNS_ALIAS = '*'
ORDERS_ALIASES = dict(ascending='ASC',
                      descending='DESC')


async def add_filters(query: str, *,
                      filters: Optional[Tuple[str, Any]]
                      ) -> str:
    if filters:
        filters = await filters_to_str(filters)
        query += f'WHERE {filters} '
    return query


async def add_orderings(query: str, *,
                        orderings: List[OrderingType]
                        ) -> str:
    if orderings:
        orderings = join_str(map(' '.join, orderings))
        query += f'ORDER BY {orderings} '
    return query


async def add_pagination(query: str, *,
                         limit: Optional[int],
                         offset: Optional[int]
                         ) -> str:
    if limit is not None:
        query += f'LIMIT {limit} '
        if offset is not None:
            query += f'OFFSET {offset} '
    return query


async def check_query_parameters(**query_parameters: Dict[str, List[str]]
                                 ) -> None:
    for parameter_key, parameter_value in query_parameters.items():
        if not parameter_value:
            err_msg = ('Invalid query parameter: '
                       f'"{parameter_key}" should be '
                       'non-empty list of strings '
                       f'but found: "{parameter_value}".')
            raise ValueError(err_msg)


async def add_groupings(query: str, *,
                        groupings: Optional[List[str]] = None
                        ) -> str:
    if groupings:
        groupings = join_str(groupings)
        query += f'GROUP BY {groupings} '
    return query
