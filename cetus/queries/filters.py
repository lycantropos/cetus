from typing import Optional, Tuple, Any

from cetus.types import (FiltersType,
                         FilterType)
from cetus.utils import join_str

from .utils import normalize_value

LOGICAL_OPERATORS = {'AND', 'OR'}
INCLUSION_OPERATORS = {'IN', 'NOT IN'}
RANGE_OPERATORS = {'BETWEEN'}
COMPARISON_OPERATORS = {'=', '!=',
                        '<', '>',
                        '<=', '>=',
                        'IS', 'IS NOT',
                        'LIKE', 'NOT LIKE'}
PREDICATES = (INCLUSION_OPERATORS
              | RANGE_OPERATORS
              | COMPARISON_OPERATORS)


def filters_to_str(filters: FiltersType) -> str:
    operator, filter_ = filters
    if operator in LOGICAL_OPERATORS:
        sub_filters = [filters_to_str(sub_filter)
                       for sub_filter in filter_]
        return operator.join(f'({sub_filter})'
                             for sub_filter in sub_filters)
    elif operator in PREDICATES:
        res = predicate_to_str(predicate_name=operator,
                               filter_=filter_)
        return res
    else:
        err_msg = ('Invalid filters operator: '
                   f'"{operator}" is not found '
                   f'in logical operators '
                   f'and predicates lists.')
        raise ValueError(err_msg)


def predicate_to_str(
        *,
        predicate_name: str,
        filter_: FilterType) -> str:
    column_name, value = filter_
    if predicate_name in INCLUSION_OPERATORS:
        value = map(normalize_value, value)
        value = f'({join_str(value)})'
    elif predicate_name in RANGE_OPERATORS:
        value = map(normalize_value, value)
        value = ' AND '.join(value)
    else:
        value = normalize_value(value)
    return f'{column_name} {predicate_name} {value}'


def add_filters(query: str, *,
                filters: Optional[Tuple[str, Any]]
                ) -> str:
    if filters:
        filters = filters_to_str(filters)
        query += f'WHERE {filters} '
    return query
