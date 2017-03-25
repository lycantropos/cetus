import os

from hypothesis import strategies
from sqlalchemy.engine.url import (URL,
                                   make_url)

from tests.strategies.utils import names_strategy

mysql_db_uri_sample = make_url(os.environ['MYSQL_DB_URI'])
postgres_db_uri_sample = make_url(os.environ['POSTGRES_DB_URI'])

mysql_db_uris_strategy = strategies.builds(
    URL,
    drivername=strategies.just(mysql_db_uri_sample.drivername),
    username=strategies.just(mysql_db_uri_sample.username),
    password=strategies.just(mysql_db_uri_sample.password),
    host=strategies.just(mysql_db_uri_sample.host),
    port=strategies.just(mysql_db_uri_sample.port),
    database=names_strategy,
    query=strategies.just(mysql_db_uri_sample.query))

postgres_db_uris_strategy = strategies.builds(
    URL,
    drivername=strategies.just(postgres_db_uri_sample.drivername),
    username=strategies.just(postgres_db_uri_sample.username),
    password=strategies.just(postgres_db_uri_sample.password),
    host=strategies.just(postgres_db_uri_sample.host),
    port=strategies.just(postgres_db_uri_sample.port),
    database=names_strategy,
    query=strategies.just(postgres_db_uri_sample.query))

db_uris_strategy = strategies.one_of(mysql_db_uris_strategy,
                                     postgres_db_uris_strategy)
