from typing import Optional

from beylerbey.types import FiltersType
from .filters import filters_to_str


async def generate_delete_query(
        *, table_name: str,
        filters: Optional[FiltersType]) -> str:
    query = f'DELETE FROM {table_name} '
    if filters:
        filters_str = await filters_to_str(filters)
        query += f'WHERE {filters_str} '
    return query
