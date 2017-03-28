from asyncio import (AbstractEventLoop,
                     ensure_future)
from typing import Generator

import pytest
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import URL
from sqlalchemy_utils import (create_database,
                              drop_database)

from cetus.data_access import is_db_uri_mysql
from tests.strategies import db_uris_strategy
from tests.utils import get_engine


@pytest.yield_fixture(scope='function')
def db_uri() -> Generator[URL, None, None]:
    res = db_uris_strategy.example()
    create_database(res)
    yield res
    drop_database(res)


@pytest.yield_fixture(scope='function')
def engine(db_uri: URL) -> Generator[Engine, None, None]:
    with get_engine(db_uri) as res:
        yield res


@pytest.fixture(scope='function')
def is_mysql(db_uri: URL,
             event_loop: AbstractEventLoop
             ) -> bool:
    future = ensure_future(is_db_uri_mysql(db_uri),
                           loop=event_loop)
    return event_loop.run_until_complete(future)
