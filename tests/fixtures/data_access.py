from typing import Generator

import pytest
from cetus.data_access import is_db_uri_mysql
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import URL
from sqlalchemy_utils import (create_database,
                              drop_database)
from tests.strategies import db_uris_strategy
from tests.utils import get_engine


@pytest.yield_fixture(scope='function')
def db_uri() -> Generator[URL, None, None]:
    res = db_uris_strategy.example()
    create_database(res)
    yield res
    drop_database(res)


@pytest.yield_fixture(scope='function')
def engine(db_uri: URL
           ) -> Generator[Engine, None, None]:
    with get_engine(db_uri) as res:
        yield res


@pytest.fixture(scope='function')
def is_mysql(db_uri: URL,
             ) -> bool:
    res = is_db_uri_mysql(db_uri)
    return res
