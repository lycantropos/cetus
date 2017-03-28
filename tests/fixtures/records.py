from typing import List

import pytest
from sqlalchemy import Table

from cetus.types import RecordType
from tests.strategies import (generate_records,
                              generate_similar_records)


@pytest.fixture(scope='function')
def table_records(table: Table) -> List[RecordType]:
    return generate_records(table.columns)


@pytest.fixture(scope='function')
def table_similar_records(table: Table) -> List[RecordType]:
    return generate_similar_records(table.columns)
