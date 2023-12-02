from typing import List
from parallize import MapReduce
from batch_framework.etl import ObjProcessor, ETLGroup
from batch_framework.storage import PandasStorage
from batch_framework.filesystem import LocalBackend
import pandas as pd

ENLARGE_RATE = 1000
SPLIT_COUNT = 5

class TestSmallToLargeProcess(ObjProcessor):
    __name__ = 'TestSmallToLargeProcess'
    @property
    def input_ids(self):
        return ['package']
    
    @property
    def output_ids(self):
        return ['test']
    
    def transform(self, inputs: List[pd.DataFrame], **kwargs) -> List[pd.DataFrame]:
        output = pd.concat([inputs[0]]*ENLARGE_RATE)
        return [output]

class TestLargeToSmall(ObjProcessor):
    __name__ = 'TestLargeToSmall'
    @property
    def input_ids(self):
        return ['test']
    
    @property
    def output_ids(self):
        return ['package_source']
    
    def transform(self, inputs: List[pd.DataFrame], **kwargs) -> List[pd.DataFrame]:
        size = len(inputs[0])
        output = inputs[0].head(size // ENLARGE_RATE)
        return [output]

class TestSimple(ObjProcessor):
    __name__ = 'TestSimple'
    @property
    def input_ids(self):
        return ['package']
    
    @property
    def output_ids(self):
        return ['test']
    
    def transform(self, inputs: List[pd.DataFrame], **kwargs) -> List[pd.DataFrame]:
        return inputs

class ExpectFlow(ObjProcessor):
    @property
    def input_ids(self):
        return ['package']
    
    @property
    def output_ids(self):
        return ['test_expect']
    
    def transform(self, inputs: List[pd.DataFrame], **kwargs) -> List[pd.DataFrame]:
        return inputs


class AssertEqual(ObjProcessor):
    @property
    def input_ids(self):
        return ['test', 'test_expect']
    
    @property
    def output_ids(self):
        return []
    
    def transform(self, inputs: List[pd.DataFrame], **kwargs) -> List[pd.DataFrame]:
        assert len(inputs[0]) == len(inputs[1])
        assert set(inputs[0].columns) == set(inputs[1].columns), f'columns1: {inputs[0].columns} ; columns2: {inputs[1].columns}'
        df1 = inputs[0].set_index('node_id')
        df2 = inputs[1].set_index('node_id')
        pd.testing.assert_frame_equal(df1, df2, check_like=True)
        return []

class TestFlow(ETLGroup):
    def __init__(self):
        input_storage = PandasStorage(LocalBackend('./data/subgraph/output/'))
        output_storage = PandasStorage(LocalBackend('./data/parallel/'))
        simple = MapReduce(TestSimple(
            input_storage,
            output_storage
        ), SPLIT_COUNT, 
            tmp_fs = LocalBackend('./data/parallel/partition/'),
            has_external_input=True
        )
        expect_flow = ExpectFlow(
            input_storage, output_storage
            )
        assert_flow = AssertEqual(output_storage, output_storage)
        super().__init__(simple, expect_flow, assert_flow)

    @property
    def input_ids(self):
        return ['package']
    
    @property
    def output_ids(self):
        return []
    


class MemoryIntenseFlow(ETLGroup):
    def __init__(self):
        src_storage = PandasStorage(LocalBackend('./data/subgraph/output/'))
        target_storage = PandasStorage(LocalBackend('./data/parallel/'))
        tmp_fs = LocalBackend('./data/parallel/partition/')
        process1 = MapReduce(TestSmallToLargeProcess(
            src_storage, 
            target_storage,
        ), SPLIT_COUNT, 
            tmp_fs = tmp_fs
        )

        process2 = MapReduce(TestLargeToSmall(
            target_storage,
            src_storage, 
        ), SPLIT_COUNT, 
            tmp_fs = tmp_fs
        )
        super().__init__(process1, process2)

    @property
    def input_ids(self):
        return ['package']
    
    @property
    def output_ids(self):
        return []
    
    @property
    def external_input_ids(self) -> List[str]:
        return self.input_ids

intense_flow = MemoryIntenseFlow()
if __name__ == '__main__':
    TestFlow().execute(sequential=True)
    TestFlow().execute()
    TestFlow().execute(max_active_run=1)
    TestFlow().execute(max_active_run=2)
    intense_flow.execute(max_active_run=1)