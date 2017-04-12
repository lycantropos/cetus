from typing import Callable

from cetus.types import ColumnValueType
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


def query_to_str(query: Query
                 ) -> str:
    statement = query.statement
    return statement_to_str(statement)


def statement_to_str(statement: ClauseElement
                     ) -> str:
    compiled_query = statement.compile(
        dialect=LiteralDialect(),
        compile_kwargs={'literal_binds': True})
    return compiled_query.string
