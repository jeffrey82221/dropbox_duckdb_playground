import pytest
import pandas as pd
from src.filesystem import DropboxBackend, LocalBackend
from src.rdb import DuckDBBackend
from src.storage import PandasStorage

@pytest.fixture
def backends():
    return [DuckDBBackend(), DropboxBackend(), LocalBackend('./data/')]

@pytest.fixture
def storages():
    return [PandasStorage]

def test_upload_download(backends, storages):
    for backend in backends:
        for Storage in storages:
            storage = Storage(backend)
            in_table = pd.DataFrame(
                [[1,2,3,4,5]], 
                columns=['a', 'b', 'c', 'd', 'e']
            )
            storage.upload(in_table, 'test')
            out_table = storage.download('test')
            pd.testing.assert_frame_equal(in_table, out_table)
            assert len(in_table) == len(out_table)
            assert len(in_table.columns) == len(out_table.columns)
            assert in_table.columns[1] == out_table.columns[1]
        

