from asyncio import ensure_future, get_event_loop
from contextlib import (closing,
                        contextmanager)
from functools import wraps
from typing import (Any,
                    Callable,
                    Coroutine,
                    List, Dict)

import logging

import time
from sqlalchemy.exc import OperationalError

from cetus.types import (RecordType,
                         ColumnValueType)
from sqlalchemy import Table
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL

event_loop = get_event_loop()


# TODO: find a better way of running asynchronous test cases
def sync(function: Callable[..., Coroutine]
         ) -> Callable[..., Any]:
    @wraps(function)
    def decorated(*args, **kwargs) -> Any:
        coroutine = function(*args, **kwargs)
        future = ensure_future(coroutine)
        res = event_loop.run_until_complete(future)
        return res

    return decorated


async def fetch(*, table: Table,
                db_uri: URL) -> List[RecordType]:
    with get_engine(db_uri=db_uri) as engine:
        with closing(engine.execute(table.select())) as result_proxy:
            return list(map(tuple, result_proxy))


async def insert(*,
                 records_dicts: List[Dict[str, ColumnValueType]],
                 table: Table,
                 db_uri: URL) -> None:
    if not records_dicts:
        return
    with get_engine(db_uri) as engine:
        engine.execute(table.insert().values(records_dicts))


async def records_to_dicts(*, records: List[RecordType],
                           table: Table
                           ) -> List[Dict[str, ColumnValueType]]:
    columns_names = [column.name for column in table.columns]
    return [dict(zip(columns_names, table_record))
            for table_record in records]


@contextmanager
def get_engine(db_uri: URL):
    engine = create_engine(db_uri)
    try:
        yield engine
    finally:
        engine.dispose()


def check_connection(db_uri: URL, *,
                     retry_attempts: int = 10,
                     retry_interval: int = 2
                     ) -> None:
    db_uri_str = db_uri.__to_string__()
    logging.info('Establishing connection '
                 f'with "{db_uri_str}".')
    with get_engine(db_uri) as engine:
        for attempt_num in range(retry_attempts):
            try:
                connection = engine.connect()
                connection.close()
                break
            except OperationalError:
                err_msg = ('Connection attempt '
                           f'#{attempt_num + 1} failed.')
                logging.error(err_msg)
                time.sleep(retry_interval)
        else:
            err_message = ('Failed to establish connection '
                           f'with "{db_uri_str}" '
                           f'after {retry_attempts} attempts '
                           f'with {retry_interval} s. interval.')
            raise ConnectionError(err_message)

    logging.info(f'Connection established with "{db_uri_str}".')
