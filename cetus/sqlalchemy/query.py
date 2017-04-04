from typing import (Union,
                    Callable)

from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.orm import Query
from sqlalchemy.sql import ClauseElement
from sqlalchemy.sql.sqltypes import (String,
                                     Interval,
                                     Date,
                                     DateTime)

from cetus.types import ColumnValueType


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


def query_to_str(statement_or_query: Union[ClauseElement, Query]
                 ) -> str:
    statement = get_statement(statement_or_query)
    compiled_query = statement.compile(
        dialect=LiteralDialect(),
        compile_kwargs={'literal_binds': True})
    return compiled_query.string


def get_statement(statement_or_query: Union[ClauseElement, Query]
                  ) -> ClauseElement:
    if isinstance(statement_or_query, Query):
        return statement_or_query.statement
    return statement_or_query
