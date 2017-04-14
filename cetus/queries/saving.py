from typing import (Optional,
                    List)

from cetus.types import (FiltersType,
                         UpdatesType)
from cetus.utils import join_str

from .utils import (add_updates,
                    check_query_parameters)
from cetus.queries.filters import add_filters

# does nothing, added for symmetry with `asyncpg` version
aiomysql_label_template = '%s'.format
asyncpg_label_template = '${}'.format


def generate_insert_query(
        *,
        table_name: str,
        columns_names: List[str],
        unique_columns_names: Optional[List[str]] = None,
        merge: bool,
        is_mysql: bool) -> str:
    check_query_parameters(columns_names=columns_names)

    if is_mysql:
        query = generate_mysql_insert_query(
            table_name=table_name,
            columns_names=columns_names,
            unique_columns_names=unique_columns_names,
            merge=merge)
    else:
        query = generate_postgres_insert_query(
            table_name=table_name,
            columns_names=columns_names,
            unique_columns_names=unique_columns_names,
            merge=merge)
    return query


def generate_mysql_insert_query(
        *,
        table_name: str,
        columns_names: List[str],
        unique_columns_names: Optional[List[str]] = None,
        merge: bool) -> str:
    columns = join_str(columns_names)
    columns_count = len(columns_names)
    labels = join_str(aiomysql_label_template(ind + 1)
                      for ind in range(columns_count))
    res = (f'INSERT INTO {table_name} ({columns}) '
           f'VALUES ({labels}) ')

    if not unique_columns_names:
        return res

    if merge:
        updates = join_str(f'{column_name} = VALUES({column_name})'
                           for column_name in unique_columns_names)
    else:
        updates = join_str(f'{column_name} = VALUES({column_name})'
                           for column_name in columns_names)
    res += f'ON DUPLICATE KEY UPDATE {updates} '
    return res


def generate_postgres_insert_query(
        *,
        table_name: str,
        columns_names: List[str],
        unique_columns_names: Optional[List[str]] = None,
        merge: bool) -> str:
    columns = join_str(columns_names)
    columns_count = len(columns_names)
    labels = join_str(asyncpg_label_template(ind + 1)
                      for ind in range(columns_count))
    res = (f'INSERT INTO {table_name} ({columns}) '
           f'VALUES ({labels}) ')

    if not unique_columns_names:
        return res

    if merge:
        updates = join_str(f'{column_name} = EXCLUDED.{column_name}'
                           for column_name in columns_names)
        on_conflict_action = f'UPDATE SET {updates}'
    else:
        on_conflict_action = 'NOTHING'
    unique_columns = join_str(unique_columns_names)
    # WARNING: in PostgreSQL you should define unique constraint
    # on all of columns passed to `ON CONFLICT`
    res += (f'ON CONFLICT ({unique_columns}) '
            f'DO {on_conflict_action} ')
    return res


def generate_postgres_insert_returning_query(
        *,
        table_name: str,
        columns_names: List[str],
        unique_columns_names: List[str] = None,
        returning_columns_names: List[str],
        merge: bool = False) -> str:
    check_query_parameters(
        columns_names=columns_names,
        returning_columns_names=returning_columns_names)

    res = generate_postgres_insert_query(
        table_name=table_name,
        columns_names=columns_names,
        unique_columns_names=unique_columns_names,
        merge=merge)
    returning_columns = join_str(returning_columns_names)
    res += f'RETURNING {returning_columns}'
    return res


def generate_update_query(
        *,
        table_name: str,
        updates: UpdatesType,
        filters: Optional[FiltersType] = None,
        is_mysql: bool) -> str:
    query = f'UPDATE {table_name} '
    query = add_updates(query,
                        updates=updates)
    query = add_filters(query,
                        filters=filters)
    return query
