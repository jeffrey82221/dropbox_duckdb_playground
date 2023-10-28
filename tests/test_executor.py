import pytest
from typing import Dict
from batch_framework.etl import SQLExecutor
from batch_framework.rdb import DuckDBBackend
from batch_framework.storage import PyArrowStorage
import pandas as pd


class MyExecutor(SQLExecutor):
    @property
    def input_ids(self):
        return [
            'input5',
            'input6'
        ]

    @property
    def output_ids(self):
        return [
            'output3'
        ]

    def sqls(self, **kwargs) -> Dict[str, str]:
        return {
            'output3': '''
                SELECT * FROM input5
                UNION
                SELECT * FROM input6
            '''
        }


@pytest.fixture
def op():
    db = DuckDBBackend()
    in_table = pd.DataFrame(
        [[1, 2, 3]], columns=['a', 'b', 'c']
    )
    db.register('input5', in_table)
    db.register('input6', in_table)
    op = MyExecutor(
        rdb=db
    )
    return op

def test_execute(op):
    in_table = pd.DataFrame(
        [[1, 2, 3]], columns=['a', 'b', 'c']
    )
    op.execute()
    result = PyArrowStorage(op._rdb).download('output3').to_pandas()
    pd.testing.assert_frame_equal(result, in_table)
