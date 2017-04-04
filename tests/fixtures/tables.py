from typing import (Generator,
                    List)

import pytest
from sqlalchemy import Table
from sqlalchemy.engine import Engine
from tests.strategies import tables_strategy


@pytest.yield_fixture(scope='function')
def table(engine: Engine) -> Generator[Table, None, None]:
    res = tables_strategy.example()
    res.create(bind=engine)
    yield res
    res.drop(bind=engine, checkfirst=True)


@pytest.fixture(scope='function')
def table_name(table: Table) -> str:
    return table.name


@pytest.fixture(scope='function')
def table_columns_names(table: Table) -> List[str]:
    return [column.name for column in table.columns]


@pytest.fixture(scope='function')
def table_primary_key(table: Table) -> str:
    return next(column.name
                for column in table.columns
                if column.primary_key)
