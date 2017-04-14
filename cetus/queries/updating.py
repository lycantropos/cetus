from typing import Optional

from cetus.queries.filters import add_filters
from cetus.types import (UpdatesType,
                         FiltersType)

from .utils import add_updates


def generate_update_query(
        *,
        table_name: str,
        updates: UpdatesType,
        filters: Optional[FiltersType] = None) -> str:
    query = f'UPDATE {table_name} '
    query = add_updates(query,
                        updates=updates)
    query = add_filters(query,
                        filters=filters)
    return query
