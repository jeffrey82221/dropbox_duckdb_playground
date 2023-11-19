"""Divide and conquer module 
TODO:
    - [ ] making this a class decorator
"""
from typing import List
import pandas as pd
import numpy as np
import os
from batch_framework.etl import ObjProcessor, ETLGroup
from batch_framework.storage import Storage

__all__ = ['MapReduce']

class MapReduce(ETLGroup):
    def __init__(self, map: ObjProcessor, parallel_count: int, tmp_storage: Storage, has_external_input: bool=False):
        self._map = map
        self._tmp_storage = tmp_storage
        self._parallel_count = parallel_count
        self._has_external_input = has_external_input
        class MapClass(ObjProcessor):
            def __init__(self, input_storage: Storage, partition_id: int):
                self._partition_id = partition_id
                super().__init__(input_storage)
            
            @property
            def input_ids(self):
                return [f'{id}.{self._partition_id}' for id in map.input_ids]
            
            @property
            def output_ids(self):
                return [f'{id}.{self._partition_id}' for id in map.output_ids]
            
            def transform(self, inputs: List[pd.DataFrame], **kwargs) -> List[pd.DataFrame]:
                return map.transform(inputs, **kwargs)
            
            def start(self, **kwargs):
                return map.start(**kwargs)

            def end(self, **kwargs):
                result = map.end(**kwargs)
                for id in self.input_ids:
                    path = tmp_storage._backend._directory + id + f'.{self._partition_id}'
                    if os.path.exists(path):
                        os.remove(path)
                return result
            
        units = [
            Divide(
                self._map.input_ids,
                parallel_count, 
                self._map._input_storage, 
                tmp_storage
            )
        ] + [
            MapClass(tmp_storage, i) for i in range(parallel_count)
        ] + [
            Merge(
                self._map.output_ids,
                parallel_count, 
                tmp_storage,
                self._map._output_storage, 
            )
        ]
        super().__init__(*units)

    @property
    def external_input_ids(self) -> List[str]:
        if self._has_external_input:
            return self.input_ids
        else:
            return []
        
    @property
    def input_ids(self):
        return self._map.input_ids

    @property
    def output_ids(self):
        return self._map.output_ids

    def end(self, **kwargs):
        for id in self.input_ids:
            for i in range(self._parallel_count):
                path = self._tmp_storage._backend._directory + id + f'.{i}'
                MapReduce._drop_partition(path)
        for id in self.output_ids:
            for i in range(self._parallel_count):
                path = self._tmp_storage._backend._directory + id + f'.{i}'
                MapReduce._drop_partition(path)

    @staticmethod
    def _drop_partition(path):
        if os.path.exists(path):
            os.remove(path)
            print('[drop_partition]', path, 'removed!')
        else:
            print('[drop_partition]', path, 'not found!')

class Base(ObjProcessor):
    def __init__(self, obj_ids: List[str], divide_count: int, input_storage: Storage, output_storage: Storage):
        self._obj_ids = obj_ids
        self._divide_count = divide_count
        super().__init__(input_storage, output_storage)

class Divide(Base):
    @property
    def input_ids(self):
        return self._obj_ids
    
    @property
    def output_ids(self):
        results = []
        for id in self._obj_ids:
            results.extend([id + f'.{i}' for i in range(self._divide_count)])
        return results

    def transform(self, inputs: List[pd.DataFrame]) -> List[pd.DataFrame]:
        print('Size before divide:', len(inputs[0]))
        tables = np.array_split(inputs[0], self._divide_count)
        print('Size after divide:', len(tables[0]))
        return tables

class Merge(Base):
    @property
    def input_ids(self):
        results = []
        for id in self._obj_ids:
            results.extend([id + f'.{i}' for i in range(self._divide_count)])
        return results

    @property
    def output_ids(self):
        return self._obj_ids

    def transform(self, inputs: List[pd.DataFrame]) -> List[pd.DataFrame]:
        print('Size before merge:', len(inputs[0]))
        merged_table = pd.concat(inputs)
        print('Size after merge:', len(merged_table))
        return [merged_table]
