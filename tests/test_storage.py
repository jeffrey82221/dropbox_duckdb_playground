import pytest
import pandas as pd
from src.filesystem import DropboxBackend, LocalBackend
from src.storage import PandasStorage

@pytest.fixture
def backends():
    return [DropboxBackend(), LocalBackend('./data/')]

@pytest.fixture
def storages():
    return [PandasStorage]

def test_upload_download(backends, storages):
    for backend in backends:
        for Storage in storages:
            storage = Storage(backend)
            in_table = pd.DataFrame([1,2,4,5])
            storage.upload(in_table, 'my-file.parquet')
            out_table = storage.download('my-file.parquet')
            pd.testing.assert_frame_equal(in_table, out_table)

