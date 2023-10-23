import pytest
from typing import Dict
from src.etl import SQLExecutor
from src.rdb import DuckDBBackend
from src.storage import PyArrowStorage
import pandas as pd


class MyExecutor(SQLExecutor):
    @property
    def input_ids(self):
        return [
            'input1',
            'input2'
        ]

    @property
    def output_ids(self):
        return [
            'output'
        ]

    def sqls(self, **kwargs) -> Dict[str, str]:
        return {
            'output': 'SELECT * FROM input1'
        }


@pytest.fixture
def op():
    db = DuckDBBackend()
    in_table = pd.DataFrame(
        [[1, 2, 3]], columns=['a', 'b', 'c']
    )
    db.register('input1', in_table)
    db.register('input2', in_table)
    op = MyExecutor(
        rdb=db
    )
    return op

def test_execute(op):
    in_table = pd.DataFrame(
        [[1, 2, 3]], columns=['a', 'b', 'c']
    )
    op.execute()
    result = PyArrowStorage(op._rdb).download('output').to_pandas()
    pd.testing.assert_frame_equal(result, in_table)
