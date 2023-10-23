import pytest
import pandas as pd
import vaex as vx
from src.filesystem import LocalBackend
from src.rdb import DuckDBBackend
from src.storage import PandasStorage, VaexStorage

@pytest.fixture
def backends():
    return [DuckDBBackend(), LocalBackend('./data/')] # , DropboxBackend(), 

@pytest.fixture
def storages():
    return [PandasStorage, VaexStorage]

def test_upload_download(backends, storages):
    for backend in backends:
        for Storage in storages:
            storage = Storage(backend)
            in_table = pd.DataFrame(
                [[1,2,3,4,5]], 
                columns=['a', 'b', 'c', 'd', 'e']
            )
            if Storage == VaexStorage:
                in_table = vx.from_pandas(in_table)
            storage.upload(in_table, 'test')
            out_table = storage.download('test')
            assert len(in_table) == len(out_table)
            assert len(in_table.columns) == len(out_table.columns)
            if Storage == PandasStorage:
                pd.testing.assert_frame_equal(in_table, out_table)
            else:
                pd.testing.assert_frame_equal(in_table.to_pandas_df(), out_table.to_pandas_df())
        

