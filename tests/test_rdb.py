import pytest
from src.rdb import DuckDBBackend
import pyarrow as pa
@pytest.fixture
def duckdb():
    return DuckDBBackend()

def test_upload_download_core(duckdb):
    in_table = pa.Table.from_pydict(
        {'i': [1, 2, 3, 4],
        'j': ["one", "two", "three", "four"]})
    duckdb.register('test2', in_table)
    out_table = duckdb.execute('select * from test2').arrow()
    assert in_table == out_table
