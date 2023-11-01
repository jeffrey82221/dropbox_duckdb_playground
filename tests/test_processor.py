"""
TODO:
- [ ] Allow passing only one storage, automatically let output_storage = input_storage
- [ ] Allow using dataframe of output_ids in transform (good for upsert/update operation)
"""

import pytest
from batch_framework.etl import ObjProcessor
from typing import List
import pandas as pd
import vaex as vx
import os
from batch_framework.filesystem import LocalBackend
from batch_framework.storage import PandasStorage, VaexStorage


class PDOperator(ObjProcessor):        
    @property
    def input_ids(self):
        return [
            'input1',
            'input2'
        ]

    @property
    def output_ids(self):
        return [
            'output1'
        ]

    def transform(self, inputs: List[pd.DataFrame], **kwargs) -> List[pd.DataFrame]:
        return [inputs[0] + inputs[1]]


class VXOperator(ObjProcessor):
    @property
    def input_ids(self):
        return [
            'input3',
            'input4'
        ]

    @property
    def output_ids(self):
        return [
            'output2'
        ]

    def transform(self, inputs: List[vx.DataFrame], **kwargs) -> List[vx.DataFrame]:
        return [vx.concat([inputs[0], inputs[1]])]


@pytest.fixture
def pd_op():
    storage = PandasStorage(LocalBackend('./data/'))
    storage.upload(pd.DataFrame([1, 2, 3]), 'input1')
    storage.upload(pd.DataFrame([1, 1, 1]), 'input2')
    storage.upload(pd.DataFrame([], columns=['a', 'b', 'c']), 'output1')
    op = PDOperator(
        input_storage=storage,
        feedback_ids=['output1']
    )
    return op


@pytest.fixture
def vx_op():
    storage = VaexStorage(LocalBackend('./data/'))
    in_table = pd.DataFrame(
        [[1, 2, 3]], columns=['a', 'b', 'c']
    )
    in_table = vx.from_pandas(in_table)
    storage.upload(in_table, 'input3')
    in_table = pd.DataFrame(
        [[1, 1, 1]], columns=['a', 'b', 'c']
    )
    in_table = vx.from_pandas(in_table)
    storage.upload(in_table, 'input4')
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
    pd_op.execute(time='time')
    output = pd_op._output_storage.download('output1')
    pd.testing.assert_frame_equal(output, pd.DataFrame([2, 3, 4]))
    vx_op.execute(time='time')
    output = vx_op._output_storage.download('output2')
    pd.testing.assert_frame_equal(
        output.to_pandas_df(),
        pd.DataFrame([[1, 2, 3], [1, 1, 1]], columns=['a', 'b', 'c'])
    )
    os.remove('./data/input1')
    os.remove('./data/input2')
    os.remove('./data/input3')
    os.remove('./data/input4')
    os.remove('./data/output1')
    os.remove('./data/output2')
