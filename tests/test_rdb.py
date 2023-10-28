"""
TODO:
- [X] Add testing on dropbox-based persistent usage. 
"""
import pytest
from batch_framework.rdb import DuckDBBackend
from batch_framework.filesystem import LocalBackend, DropboxBackend
import pyarrow as pa
from duckdb.duckdb import ConnectionException, CatalogException
import os

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

def test_close():
    duckdb = DuckDBBackend()
    in_table = pa.Table.from_pydict(
        {'i': [1, 2, 3, 4],
         'j': ["one", "two", "three", "four"]})
    duckdb.register('test2', in_table)
    _ = duckdb.execute('select * from test2').arrow()
    duckdb.close()
    with pytest.raises(ConnectionException):
        _ = duckdb.execute('select * from test2').arrow()
    duckdb2 = DuckDBBackend()
    with pytest.raises(CatalogException):
        duckdb2.execute('select * from test2').arrow()

def test_local_persistent():
    duckdb = DuckDBBackend(persist_fs=LocalBackend(), db_name='data/test.duckdb')
    in_table = pa.Table.from_pydict(
        {'i': [1, 2, 3, 4],
         'j': ["one", "two", "three", "four"]})
    duckdb.register('test', in_table)
    duckdb.execute('CREATE TABLE test2 AS (SELECT * FROM test);')
    duckdb.close()
    duckdb2 = DuckDBBackend(persist_fs=LocalBackend(), db_name='data/test.duckdb')
    with pytest.raises(CatalogException):
        duckdb2.execute('select * from test1').arrow()
    duckdb2.execute('select * from test2').arrow()
    os.remove('./data/test.duckdb')

def test_dropbox_persistent():
    db_name = 'data/no_commit.duckdb'
    duckdb = DuckDBBackend(persist_fs=DropboxBackend(), db_name=db_name)
    in_table = pa.Table.from_pydict(
        {'i': [1, 2, 3, 4],
         'j': ["one", "two", "three", "four"]})
    duckdb.register('test', in_table)
    duckdb.execute('CREATE TABLE test2 AS (SELECT * FROM test);')
    duckdb.close()
    os.remove(f'./{db_name}')
    duckdb2 = DuckDBBackend(persist_fs=DropboxBackend(), db_name=db_name)
    with pytest.raises(CatalogException):
        duckdb2.execute('select * from test2').arrow()
    os.remove(f'./{db_name}')

def test_dropbox_persistent_commit():
    db_name = 'data/with_commit.duckdb'
    duckdb = DuckDBBackend(persist_fs=DropboxBackend(), db_name=db_name)
    in_table = pa.Table.from_pydict(
        {'i': [1, 2, 3, 4],
         'j': ["one", "two", "three", "four"]})
    duckdb.register('test', in_table)
    duckdb.execute('CREATE TABLE test2 AS (SELECT * FROM test);')
    duckdb.commit() # Adding Commit Here to Sync with Dropbox
    duckdb.close()
    os.remove(f'./{db_name}')
    duckdb2 = DuckDBBackend(persist_fs=DropboxBackend(), db_name=db_name)
    with pytest.raises(CatalogException):
        duckdb2.execute('select * from test2').arrow()
    os.remove(f'./{db_name}')