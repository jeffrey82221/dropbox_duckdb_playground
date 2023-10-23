import pytest
from src.etl import ETL
from typing import List
import pandas as pd
import vaex as vx
from src.filesystem import LocalBackend
from src.storage import PandasStorage, VaexStorage


class PDOperator(ETL):
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

    def transform(self, inputs: List[pd.DataFrame]) -> List[pd.DataFrame]:
        return [inputs[0] + inputs[1]]


class VXOperator(ETL):
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

    def transform(self, inputs: List[vx.DataFrame]) -> List[vx.DataFrame]:
        return [vx.concat([inputs[0], inputs[1]])]


@pytest.fixture
def pd_op():
    storage = PandasStorage(LocalBackend())
    storage.upload(pd.DataFrame([1, 2, 3]), 'input1')
    storage.upload(pd.DataFrame([1, 1, 1]), 'input2')
    op = PDOperator(
        input_storage=storage,
        output_storage=storage
    )
    return op


@pytest.fixture
def vx_op():
    storage = VaexStorage(LocalBackend('./tmp/'))
    in_table = pd.DataFrame(
        [[1, 2, 3]], columns=['a', 'b', 'c']
    )
    in_table = vx.from_pandas(in_table)
    storage.upload(in_table, 'input1')
    in_table = pd.DataFrame(
        [[1, 1, 1]], columns=['a', 'b', 'c']
    )
    in_table = vx.from_pandas(in_table)
    storage.upload(in_table, 'input2')
    op = VXOperator(
        input_storage=storage,
        output_storage=storage
    )
    return op


def test_get_input_type(pd_op, vx_op):
    assert pd_op.get_input_type() == pd.DataFrame
    assert vx_op.get_input_type() == vx.DataFrame


def test_get_output_type(pd_op, vx_op):
    assert pd_op.get_output_type() == pd.DataFrame
    assert vx_op.get_output_type() == vx.DataFrame


def test_execute(pd_op, vx_op):
    pd_op.execute()
    output = pd_op._output_storage.download('output')
    pd.testing.assert_frame_equal(output, pd.DataFrame([2, 3, 4]))
    vx_op.execute()
    output = vx_op._output_storage.download('output')
    pd.testing.assert_frame_equal(
        output.to_pandas_df(),
        pd.DataFrame([[1, 2, 3], [1, 1, 1]], columns=['a', 'b', 'c'])
    )
