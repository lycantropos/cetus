from typing import Tuple

from hypothesis import strategies

from cetus.queries.filters import (PREDICATES,
                                   LOGICAL_OPERATORS,
                                   INCLUSION_OPERATORS,
                                   RANGE_OPERATORS,
                                   COMPARISON_OPERATORS)
from cetus.types import ColumnValueType
from .utils import date_times_strategy
from tests.strategies.utils import identifiers_strategy

predicates_names_strategy = strategies.one_of(strategies.just(predicate)
                                              for predicate in PREDICATES)
values_strategy = strategies.one_of(date_times_strategy,
                                    strategies.booleans(),
                                    strategies.integers(),
                                    strategies.floats(allow_infinity=False,
                                                      allow_nan=False),
                                    strategies.text())
values_lists_strategy = strategies.lists(values_strategy)
values_range_strategy = strategies.tuples(values_strategy, values_strategy)
filter_strategy = strategies.tuples(identifiers_strategy,
                                    strategies.one_of(values_strategy,
                                                      values_lists_strategy,
                                                      values_range_strategy))


def predicates_has_correct_value(
        predicate: Tuple[str, Tuple[str, ColumnValueType]]) -> bool:
    predicate_name, filter_ = predicate
    _, value = filter_
    inclusion_operator_has_list_value = (
        predicate_name not in INCLUSION_OPERATORS or
        isinstance(value, list))
    range_operator_has_tuple_value = (
        predicate_name not in RANGE_OPERATORS or
        isinstance(value, tuple))
    comparison_operator_has_simple_value = (
        predicate_name not in COMPARISON_OPERATORS or
        not isinstance(value, (tuple, list)))
    return (inclusion_operator_has_list_value and
            range_operator_has_tuple_value and
            comparison_operator_has_simple_value)


predicates_strategy = (strategies.tuples(predicates_names_strategy, filter_strategy)
                       .filter(predicates_has_correct_value))
predicates_lists_strategy = strategies.lists(predicates_strategy)
logical_operators_names_strategy = strategies.one_of(strategies.just(operator)
                                                     for operator in LOGICAL_OPERATORS)
logical_operators_strategy = strategies.tuples(logical_operators_names_strategy,
                                               predicates_lists_strategy)
filters_strategy = strategies.recursive(
    predicates_strategy,
    lambda child: strategies.tuples(logical_operators_names_strategy,
                                    strategies.lists(child)))
