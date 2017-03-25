from datetime import datetime
from typing import Tuple

import hypothesis
import pytest
from hypothesis import strategies

from beylerbey.queries.filters import (LOGICAL_OPERATORS,
                                       PREDICATES,
                                       INCLUSION_OPERATORS,
                                       RANGE_OPERATORS,
                                       predicate_to_str,
                                       normalize_value,
                                       filters_to_str)
from beylerbey.types import (ColumnValueType,
                             FiltersType,
                             FilterType)
from tests.strategies import (date_times_strategy,
                              predicates_strategy,
                              filters_strategy)
from tests.utils import sync

invalid_filters_strategy = strategies.tuples(
    strategies.text().filter(lambda s: s not in LOGICAL_OPERATORS | PREDICATES),
    strategies.none())

values_strategy = strategies.one_of(strategies.none(),
                                    strategies.integers(),
                                    strategies.floats(allow_infinity=False,
                                                      allow_nan=False),
                                    strategies.text(),
                                    date_times_strategy)


@hypothesis.given(value=values_strategy)
def test_normalize_value(value: ColumnValueType) -> None:
    normalized_value = normalize_value(value)

    assert isinstance(normalized_value, str)

    if isinstance(value, (str, datetime)) or value is None:
        assert normalized_value != str(value)
    else:
        assert normalized_value == str(value)


@hypothesis.given(predicate=predicates_strategy)
@sync
async def test_predicate_to_str(predicate: Tuple[str, FilterType]) -> None:
    predicate_name, filter_ = predicate
    column_name, value = filter_
    predicate_str = await predicate_to_str(predicate_name=predicate_name,
                                           filter_=filter_)
    assert isinstance(predicate_str, str)
    assert predicate_name in predicate_str
    assert predicate_str.startswith(column_name)
    if predicate_name in INCLUSION_OPERATORS | RANGE_OPERATORS:
        for sub_value in value:
            normalized_sub_value = normalize_value(sub_value)
            assert normalized_sub_value in predicate_str


@hypothesis.given(filters=filters_strategy,
                  invalid_filters=invalid_filters_strategy)
@hypothesis.settings(perform_health_check=False)
@sync
async def test_filters_to_str(filters: FiltersType,
                              invalid_filters: FiltersType):
    operator, filter_ = filters
    filters_str = await filters_to_str(filters)

    assert isinstance(filters_str, str)
    if len(filter_) > 1:
        assert operator in filters_str
    if operator in LOGICAL_OPERATORS:
        for sub_filter in filter_:
            sub_filter_str = await filters_to_str(sub_filter)
            assert sub_filter_str in filters_str

    with pytest.raises(ValueError):
        await filters_to_str(invalid_filters)
