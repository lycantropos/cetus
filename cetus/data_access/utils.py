import logging
from functools import wraps
from typing import (Any,
                    Optional,
                    Callable,
                    Coroutine,
                    Dict,
                    Tuple, List)

from asyncpg import PostgresError
from pymysql import Error
from sqlalchemy.engine.url import URL

MYSQL_DRIVER_NAME_PREFIX = 'mysql'
# to make pagination without limit
MYSQL_MAX_BIGINT_VALUE = 18_446_744_073_709_551_615

logger = logging.getLogger(__name__)


def handle_exceptions(function: Callable[..., Coroutine]
                      ) -> Callable[..., Coroutine]:
    @wraps(function)
    async def decorated(query: str,
                        *args: Tuple[Any, ...],
                        **kwargs: Dict[str, Any]):
        try:
            res = await function(query, *args, **kwargs)
            return res
        except (Error, PostgresError) as err:
            err_msg = ('Error while processing '
                       f'query: "{query}".')
            raise IOError(err_msg) from err

    return decorated


async def is_db_uri_mysql(db_uri: URL) -> bool:
    backend_name = db_uri.get_backend_name()
    return backend_name == MYSQL_DRIVER_NAME_PREFIX


async def normalize_pagination(*, limit: Optional[int],
                               offset: Optional[int],
                               is_mysql: bool
                               ) -> Tuple[Optional[int],
                                          Optional[int]]:
    if is_mysql:
        if limit is None and offset is not None:
            warn_msg = ('Incorrect pagination parameters: '
                        'in MySQL "offset" parameter '
                        'should be specified '
                        'along with "limit" parameter, '
                        'but "limit" parameter '
                        f'has value "{limit}". '
                        f'Assuming that table\'s primary key '
                        f'has "BIGINT" type '
                        f'and setting limit '
                        f'to {MYSQL_MAX_BIGINT_VALUE}.')
            logger.warning(warn_msg)
            return MYSQL_MAX_BIGINT_VALUE, offset
    return limit, offset


async def generate_table_columns_names(*,
                                       columns_names: List[str],
                                       columns_aliases: List[str]
                                       ) -> List[str]:
    return [f'{column_name} AS {column_alias}'
            for column_name, column_alias in zip(columns_names,
                                                 columns_aliases)]


async def generate_table_columns_aliases(
        *,
        columns_names: List[str],
        columns_aliases: Optional[Dict[str, str]] = None) -> List[str]:
    columns_aliases = columns_aliases or {}
    return [
        f'{columns_aliases.get(column_name, column_name)}'
        for column_name in columns_names]
