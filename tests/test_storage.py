import pytest
import pandas as pd
import vaex as vx
import pyarrow as pa
from batch_framework.filesystem import LocalBackend
from batch_framework.rdb import DuckDBBackend
from batch_framework.storage import PandasStorage, VaexStorage, PyArrowStorage, JsonStorage
from datetime import datetime


@pytest.fixture
def backends():
    return [DuckDBBackend(), LocalBackend('./data/')]  # , DropboxBackend(),


@pytest.fixture
def storages():
    return [PandasStorage, VaexStorage, PyArrowStorage]


def test_upload_download(backends, storages):
    for backend in backends:
        for Storage in storages:
            storage = Storage(backend)
            in_table = pd.DataFrame(
                [[1, 2, 3, 4, 5, datetime.now()]],
                columns=['a', 'b', 'c', 'd', 'e', 'current-time']
            )
            if Storage == VaexStorage:
                in_table = vx.from_pandas(in_table)
            elif Storage == PyArrowStorage:
                in_table = pa.Table.from_pandas(in_table)
            storage.upload(in_table, 'test')
            out_table = storage.download('test')
            assert len(in_table) == len(out_table)
            assert len(in_table.columns) == len(out_table.columns)
            if Storage == PandasStorage:
                pd.testing.assert_frame_equal(in_table, out_table)
            elif Storage == PyArrowStorage:
                pd.testing.assert_frame_equal(
                    in_table.to_pandas(), out_table.to_pandas())
            else:
                pd.testing.assert_frame_equal(
                    in_table.to_pandas_df(), out_table.to_pandas_df())


def test_json_storage():
    js = JsonStorage(LocalBackend('./data/'))
    data = {'a': [1, 2, 3]}
    js.upload(data, 'json_test.json')
    result = js.download('json_test.json')
    assert data == result
