import pytest
from src.operator import Operator
from typing import List 
import pandas as pd
from src.filesystem import LocalBackend
from src.storage import PandasStorage
class MyOperator(Operator):
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


@pytest.fixture
def operator():
    storage = PandasStorage(LocalBackend())
    storage.upload(pd.DataFrame([1,2,3]), 'input1')
    storage.upload(pd.DataFrame([1,1,1]), 'input2')
    op = MyOperator(
        input_storage=storage,
        output_storage=storage
        )
    return op


def test_get_input_type(operator):
    assert operator.get_input_type() == pd.DataFrame

def test_get_output_type(operator):
    assert operator.get_output_type() == pd.DataFrame

def test_execute(operator):
    operator.execute()
    output = operator._output_storage.download('output')
    pd.testing.assert_frame_equal(output, pd.DataFrame([2,3,4]))