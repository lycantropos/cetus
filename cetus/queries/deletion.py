from typing import Optional

from cetus.queries.utils import add_filters
from cetus.types import FiltersType


async def generate_delete_query(
        *, table_name: str,
        filters: Optional[FiltersType]) -> str:
    query = f'DELETE FROM {table_name} '
    query = await add_filters(query, filters=filters)
    return query
