import sqlite3
from typing import Iterator

import pytest

from bitemporal import merge_logic


@pytest.fixture()
def conn() -> Iterator[sqlite3.Connection]:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    merge_logic.initialize_db(connection)
    yield connection
    connection.close()
