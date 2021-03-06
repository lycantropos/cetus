import logging
import time
from collections import OrderedDict
from contextlib import (closing,
                        contextmanager)
from typing import List

from cetus.types import (RecordType,
                         ColumnValueType,
                         OrderedDictType)
from sqlalchemy.engine import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import OperationalError
from sqlalchemy.schema import Table


def fetch(*,
          table: Table,
          db_uri: URL) -> List[RecordType]:
    with get_engine(db_uri=db_uri) as engine:
        with closing(engine.execute(table.select())) as result_proxy:
            return list(map(tuple, result_proxy))


def insert(*,
           records_dicts: List[OrderedDictType[str, ColumnValueType]],
           table: Table,
           db_uri: URL) -> None:
    if not records_dicts:
        return
    with get_engine(db_uri) as engine:
        engine.execute(table.insert().values(records_dicts))


def records_to_dicts(*,
                     records: List[RecordType],
                     table: Table
                     ) -> List[OrderedDictType[str, ColumnValueType]]:
    columns_names = [column.name for column in table.columns]
    return [OrderedDict(zip(columns_names, table_record))
            for table_record in records]


def is_sub_dictionary(*,
                      sub_dictionary: dict,
                      super_dictionary: dict) -> bool:
    return all(item in super_dictionary.items()
               for item in sub_dictionary.items())


@contextmanager
def get_engine(db_uri: URL):
    engine = create_engine(db_uri)
    try:
        yield engine
    finally:
        engine.dispose()


def check_connection(db_uri: URL,
                     *,
                     retry_attempts: int = 10,
                     retry_interval: int = 2
                     ) -> None:
    db_uri_str = db_uri.__to_string__()
    logging.info('Establishing connection '
                 f'with "{db_uri_str}".')
    with get_engine(db_uri) as engine:
        for attempt_num in range(retry_attempts):
            try:
                with engine.connect():
                    pass
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
