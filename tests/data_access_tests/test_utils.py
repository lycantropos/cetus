import pytest
from hypothesis import strategies
from hypothesis.searchstrategy import SearchStrategy
from sqlalchemy.engine.url import URL

from cetus.data_access import is_db_uri_mysql
from cetus.data_access.utils import MYSQL_DRIVER_NAME_PREFIX


def extend_mysql_db_uri_like_strings_strategy(
        child: SearchStrategy) -> SearchStrategy:
    res = (strategies
           .tuples(child, strategies.text(min_size=1)))
    return res.map(''.join)


mysql_empty_db_uri_strategy = strategies.builds(
    URL,
    drivername=strategies.just(MYSQL_DRIVER_NAME_PREFIX))
non_mysql_like_driver_names_strategy = strategies.text(
    alphabet=strategies.characters(
        blacklist_characters=set(MYSQL_DRIVER_NAME_PREFIX)))
non_mysql_empty_db_uri_strategy = strategies.builds(
    URL,
    drivername=non_mysql_like_driver_names_strategy)


@pytest.fixture(scope='function')
def mysql_empty_db_uri() -> URL:
    return mysql_empty_db_uri_strategy.example()


@pytest.fixture(scope='function')
def non_mysql_empty_db_uri() -> URL:
    return non_mysql_empty_db_uri_strategy.example()


@pytest.mark.asyncio
async def test_is_db_uri_mysql(mysql_empty_db_uri: URL,
                               non_mysql_empty_db_uri: URL) -> None:
    mysql_empty_db_uri_is_mysql = await is_db_uri_mysql(mysql_empty_db_uri)
    assert mysql_empty_db_uri_is_mysql
    non_mysql_empty_db_uri_is_mysql = await is_db_uri_mysql(non_mysql_empty_db_uri)
    assert not non_mysql_empty_db_uri_is_mysql
