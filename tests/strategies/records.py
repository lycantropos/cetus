from functools import partial
from itertools import permutations
from typing import (Iterable,
                    List)

from hypothesis import strategies
from hypothesis.searchstrategy import SearchStrategy
from sqlalchemy import Column

from cetus.types import RecordType
from .utils import values_strategies_by_python_types

MAX_RECORDS_COUNT = 100


def generate_records(columns: List[Column]) -> List[RecordType]:
    strategy = records_lists_strategy(columns)
    return strategy.example()


def generate_similar_records(columns: List[Column]
                             ) -> List[RecordType]:
    strategy = similar_records_lists_strategy(columns)
    return strategy.example()


def records_lists_strategy(columns: List[Column]
                           ) -> SearchStrategy:
    strategy = records_strategy(columns)
    return (strategies.lists(strategy,
                             min_size=1,
                             max_size=MAX_RECORDS_COUNT)
            .filter(partial(records_satisfy_constraints,
                            columns=columns)))


def similar_records_lists_strategy(columns: List[Column]
                                   ) -> SearchStrategy:
    strategy = records_strategy(columns)
    record = strategy.example()
    similar_records_strategy = strategies.tuples(*[
        strategies.just(record[ind])
        if not (column.primary_key or column.unique)
        else strategies.one_of(
            values_strategies_by_python_types[column.type.python_type],
            strategies.none())
        if column.nullable
        else values_strategies_by_python_types[column.type.python_type]
        for ind, column in enumerate(columns)
        ])
    return (strategies.lists(similar_records_strategy,
                             min_size=2,
                             max_size=MAX_RECORDS_COUNT)
            .filter(partial(records_satisfy_constraints,
                            columns=columns)))


def records_strategy(columns: Iterable[Column]
                     ) -> SearchStrategy:
    return strategies.tuples(*[
        strategies.one_of(
            values_strategies_by_python_types[column.type.python_type],
            strategies.none())
        if column.nullable
        else values_strategies_by_python_types[column.type.python_type]
        for column in columns])


def records_satisfy_constraints(records: List[RecordType],
                                columns: List[Column]
                                ) -> bool:
    for column_ind, column in enumerate(columns):
        if not (column.unique or column.primary_key):
            continue
        records_unique_column_fields_are_not_unique = any(
            record[column_ind] == other_record[column_ind]
            for record, other_record in permutations(records, r=2))
        if records_unique_column_fields_are_not_unique:
            return False
    return True
