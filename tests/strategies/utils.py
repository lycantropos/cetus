import itertools
from datetime import datetime
from functools import partial
from typing import (Any,
                    Iterable,
                    List)

from hypothesis import strategies
from hypothesis.extra.datetime import datetimes
from sqlalchemy import MetaData

from cetus.utils import MAX_IDENTIFIER_LENGTH

MIN_YEAR = 1970
MIN_STRING_LENGTH = 10
# we're using integers as primary key values
# which are usually positive
MIN_INTEGER_VALUE = 1
MAX_MYSQL_INTEGER_VALUE = 2147483647

characters_strategy = strategies.characters(min_codepoint=ord('a'),
                                            max_codepoint=ord('z'))

names_strategy = strategies.text(characters_strategy,
                                 min_size=5,
                                 max_size=MAX_IDENTIFIER_LENGTH)

date_times_strategy = datetimes(timezones=[],
                                min_year=MIN_YEAR)

values_strategies_by_python_types = {
    bool: strategies.booleans(),
    int: strategies.integers(min_value=MIN_INTEGER_VALUE,
                             max_value=MAX_MYSQL_INTEGER_VALUE),
    str: strategies.text(alphabet=characters_strategy,
                         max_size=MIN_STRING_LENGTH - 1),
    datetime: date_times_strategy.map(partial(datetime.replace,
                                              microsecond=0))}


def concatenate_iterables(iterables: Iterable[Iterable[Any]]
                          ) -> List[Any]:
    return list(itertools.chain.from_iterable(iterables))


db_metadata = MetaData()
identifiers_strategy = strategies.text(alphabet=characters_strategy,
                                       min_size=8,
                                       max_size=MAX_IDENTIFIER_LENGTH)
