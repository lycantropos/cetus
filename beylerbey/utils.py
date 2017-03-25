from typing import Iterable

# more info at:
# for PostgreSQL
# https://www.postgresql.org/docs/current/static/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
# for MySQL
# https://dev.mysql.com/doc/refman/5.5/en/identifiers.html
MAX_IDENTIFIER_LENGTH = 63


def join_str(items: Iterable, sep: str = ', ') -> str:
    return sep.join(map(str, items))
