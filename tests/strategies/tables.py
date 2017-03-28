from typing import List

from hypothesis import strategies
from sqlalchemy import (Table, Column,
                        Boolean, Integer,
                        BigInteger,
                        String, DateTime)

from cetus.utils import MAX_IDENTIFIER_LENGTH
from .utils import (MIN_STRING_LENGTH,
                    db_metadata,
                    concatenate_iterables,
                    identifiers_strategy)

auto_incrementable_column_types = [Integer, BigInteger]
column_types = (auto_incrementable_column_types
                + [Boolean, DateTime])
string_column_types_strategy = strategies.builds(
    String,
    length=strategies.integers(min_value=MIN_STRING_LENGTH,
                               max_value=MAX_IDENTIFIER_LENGTH))
primary_key_column_types_strategy = strategies.one_of(
    strategies.just(column_type)
    for column_type in auto_incrementable_column_types)
column_types_strategy = strategies.one_of(
    string_column_types_strategy,
    *[strategies.just(column_type)
      for column_type in column_types])
primary_key_columns_strategy = strategies.builds(
    Column,
    name=identifiers_strategy,
    type_=primary_key_column_types_strategy,
    autoincrement=strategies.just(True),
    primary_key=strategies.just(True))
non_primary_key_columns_strategy = strategies.builds(
    Column,
    name=identifiers_strategy,
    type_=column_types_strategy,
    unique=strategies.booleans(),
    nullable=strategies.just(False),
    primary_key=strategies.just(False),
    index=strategies.booleans())


def column_unique_by(column: Column) -> int:
    return hash(column.name)


def extend_columns_lists_strategy(child):
    return (strategies.tuples(child,
                              strategies.lists(
                                  non_primary_key_columns_strategy,
                                  unique_by=column_unique_by))
            .map(concatenate_iterables))


columns_lists_strategy = strategies.recursive(
    strategies.tuples(primary_key_columns_strategy),
    extend_columns_lists_strategy)


def has_non_unique_column(columns):
    return any(not (column.primary_key or column.unique)
               for column in columns)


# to make proper testing of group wise fetching
# we should have records with some equal columns values
columns_lists_with_non_unique_column_strategy = (columns_lists_strategy
                                                 .filter(has_non_unique_column))


def generate_table_by_columns(columns: List[Column]) -> Table:
    return Table(identifiers_strategy.example(),
                 db_metadata,
                 *columns,
                 extend_existing=True)


tables_strategy = (columns_lists_with_non_unique_column_strategy
                   .map(generate_table_by_columns))
