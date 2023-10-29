import pytest
import os
from typing import Dict
from batch_framework.etl import SQLExecutor
from batch_framework.rdb import DuckDBBackend
from batch_framework.storage import PyArrowStorage, PandasStorage
from batch_framework.filesystem import LocalBackend
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

@pytest.fixture
def op_with_fs_input():
    db = DuckDBBackend()
    in_table = pd.DataFrame(
        [[1, 2, 3]], columns=['a', 'b', 'c']
    )
    input_fs = LocalBackend('./data/')
    PandasStorage(input_fs).upload(in_table, 'input5')
    PandasStorage(input_fs).upload(in_table, 'input6')
    op = MyExecutor(
        rdb=db,
        input_fs=input_fs
    )
    return op

@pytest.fixture
def op_with_fs_output():
    db = DuckDBBackend()
    in_table = pd.DataFrame(
        [[1, 2, 3]], columns=['a', 'b', 'c']
    )
    output_fs = LocalBackend('./data/')
    db.register('input5', in_table)
    db.register('input6', in_table)
    op = MyExecutor(
        rdb=db,
        output_fs=output_fs
    )
    return op

def test_execute(op, op_with_fs_input, op_with_fs_output):
    for operator in [op, op_with_fs_input, op_with_fs_output]:
        in_table = pd.DataFrame(
            [[1, 2, 3]], columns=['a', 'b', 'c']
        )
        operator.execute()
        result = PyArrowStorage(operator._rdb).download('output3').to_pandas()
        pd.testing.assert_frame_equal(result, in_table)
        if operator._output_storage is not None:
            assert os.path.exists('./data/output3')
            result = operator._output_storage.download('output3').to_pandas()
            pd.testing.assert_frame_equal(result, in_table)
            os.remove('./data/output3')


