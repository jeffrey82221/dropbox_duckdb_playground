from typing import List
import pandas as pd
import numpy as np
from batch_framework.etl import ObjProcessor
from batch_framework.storage import Storage

__all__ = ['PandasDivide', 'PandasMerge']

class MapReduce(ObjProcessor):
    def __init__(self, obj_name: str, divide_count: int, input_storage: Storage):
        self._obj_name = obj_name
        self._divide_count = divide_count
        super().__init__(input_storage)

class PandasDivide(MapReduce):
    @property
    def input_ids(self):
        return [self._obj_name]
    
    @property
    def output_ids(self):
        return [self._obj_name + f'.{i}' for i in range(self._divide_count)]

    def transform(self, inputs: List[pd.DataFrame]) -> List[pd.DataFrame]:
        print('Size before divide:', len(inputs[0]))
        tables = np.array_split(inputs[0], self._divide_count)
        print('Size after divide:', len(tables[0]))
        return tables

class PandasMerge(MapReduce):
    @property
    def input_ids(self):
        return [self._obj_name + f'.{i}' for i in range(self._divide_count)]

    @property
    def output_ids(self):
        return [self._obj_name]

    def transform(self, inputs: List[pd.DataFrame]) -> List[pd.DataFrame]:
        print('Size before merge:', len(inputs[0]))
        merged_table = pd.concat(inputs)
        print('Size after merge:', len(merged_table))
        return [merged_table]
