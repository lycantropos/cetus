from typing import Callable

from cetus.types import ColumnValueType
from sqlalchemy.dialects.mysql.mysqldb import MySQLDialect_mysqldb
from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.orm.query import Query
from sqlalchemy.sql.elements import ClauseElement
from sqlalchemy.sql.sqltypes import (String,
                                     Interval,
                                     Date,
                                     DateTime)


class StringLiteral(String):
    def literal_processor(self, dialect: DefaultDialect
                          ) -> Callable[[ColumnValueType], str]:
        super_processor = super().literal_processor(dialect)

        def process(value: ColumnValueType) -> str:
            if isinstance(value, int):
                return str(value)
            if not isinstance(value, str):
                value = str(value)
            res = super_processor(value)
            if isinstance(res, bytes):
                res = res.decode(dialect.encoding)
            return res

        return process


class LiteralDialect(DefaultDialect):
    colspecs = {
        DateTime: StringLiteral,
        Date: StringLiteral,
        Interval: StringLiteral,
        # prevent various encoding explosions
        String: StringLiteral,
    }


class MySQLDialect(LiteralDialect,
                   MySQLDialect_mysqldb):
    pass


class PostgreSQLDialect(LiteralDialect,
                        PGDialect_psycopg2):
    pass


def query_to_str(query: Query,
                 *,
                 is_mysql: bool) -> str:
    statement = query.statement
    return statement_to_str(statement,
                            is_mysql=is_mysql)


def statement_to_str(statement: ClauseElement,
                     *,
                     is_mysql: bool) -> str:
    dialect_cls = MySQLDialect if is_mysql else PostgreSQLDialect
    compiled_query = statement.compile(
        dialect=dialect_cls(),
        compile_kwargs={'literal_binds': True})
    return compiled_query.string
