"""Divide and conquer module 
TODO:
    - [X] making this a class decorator
    - [ ] using vaex storage to enable zero copy (lower RAM usage)
"""
from typing import List
import pandas as pd
import numpy as np
import os
import vaex as vx
from batch_framework.etl import ObjProcessor, ETLGroup, SQLExecutor
from batch_framework.storage import Storage, VaexStorage
from batch_framework.filesystem import FileSystem
from batch_framework.rdb import DuckDBBackend
__all__ = ['MapReduce']

class MapReduce(ETLGroup):
    """
    Decorating an ETL object into MapReduced version
    """
    def __init__(self, map: ObjProcessor, parallel_count: int, tmp_fs: FileSystem, has_external_input: bool=False):
        self._map = map
        map_name = type(map).__name__
        self._tmp_fs = tmp_fs
        self._parallel_count = parallel_count
        self._has_external_input = has_external_input
        class MapClass(ObjProcessor):
            def __init__(self, input_storage: Storage, partition_id: int):
                self._partition_id = partition_id
                super().__init__(input_storage)
            
            @property
            def input_ids(self):
                return [f'{map_name}.{id}.{self._partition_id}.parquet' for id in map.input_ids]
            
            @property
            def output_ids(self):
                return [f'{map_name}.{id}.{self._partition_id}.parquet' for id in map.output_ids]
            
            def transform(self, inputs: List[pd.DataFrame], **kwargs) -> List[pd.DataFrame]:
                return map.transform(inputs, **kwargs)
            
            def start(self, **kwargs):
                return map.start(**kwargs)
            
        units = [
            DecomposedDivide(
                i,
                self._map.input_ids,
                parallel_count, 
                self._map._input_storage._backend, 
                tmp_fs,
                map_name=map_name
            ) for i in range(parallel_count)
        ] + [
            MapClass(type(self._map._input_storage)(tmp_fs), i) for i in range(parallel_count)
        ] + [
            Merge(
                id,
                parallel_count, 
                tmp_fs,
                self._map._output_storage._backend,
                map_name=map_name
            ) for id in self._map.output_ids
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
                path = self._tmp_fs._directory + f'{self._map.__name__}.{id}' + f'.{i}.parquet'
                MapReduce._drop_partition(path)
        for id in self.output_ids:
            for i in range(self._parallel_count):
                path = self._tmp_fs._directory + f'{self._map.__name__}.{id}' + f'.{i}.parquet'
                MapReduce._drop_partition(path)

    @staticmethod
    def _drop_partition(path):
        if os.path.exists(path):
            os.remove(path)
            print('[drop_partition]', path, 'removed!')
        else:
            print('[drop_partition]', path, 'not found!')

# Using Decomposed Divide to avoid large memory usage
class DecomposedDivide(ObjProcessor):
    def __init__(self, partition_id: int, obj_ids: List[str], divide_count: int, input_fs: FileSystem, output_fs: FileSystem, map_name: str=''):
        self._obj_ids = obj_ids
        self._partition_id = partition_id
        self._divide_count = divide_count
        self._map_name = map_name
        super().__init__(VaexStorage(input_fs), VaexStorage(output_fs))

    @property
    def input_ids(self):
        return self._obj_ids
    
    @property
    def output_ids(self):
        return [f'{self._map_name}.{id}.{self._partition_id}.parquet' for id in self._obj_ids]
    
    def transform(self, inputs: List[vx.DataFrame]) -> List[vx.DataFrame]:
        results = []
        for i, table in enumerate(inputs):
            size = len(table)
            print('generate table size:', size)
            batch_size = size // self._divide_count
            subtable = table[batch_size*self._partition_id:batch_size*(self._partition_id+1)]
            print(f'get subtable for {i}th input')
            results.append(subtable)
        return results
        

        # print('Size before merge:', len(inputs[0]))
        merged_table = vx.concat(inputs)
        # print('Size after merge:', len(merged_table))
        return [merged_table]

class Divide(SQLExecutor):
    def __init__(self, 
                 obj_ids: List[str], 
                 divide_count: int, 
                 input_fs: FileSystem, 
                 output_fs: FileSystem
                 ):
        self._obj_ids = obj_ids
        self._divide_count = divide_count
        super().__init__(rdb=DuckDBBackend(), input_fs=input_fs, output_fs=output_fs)

    @property
    def input_ids(self):
        return self._obj_ids
    
    @property
    def output_ids(self):
        results = []
        for id in self._obj_ids:
            results.extend([id + f'_{i}' for i in range(self._divide_count)])
        return results

    def sqls(self, **kwargs):
        results = dict()
        for i, id in enumerate(self.output_ids):
            results[id] = f"""
            WITH row_table AS (
                SELECT
                    *,
                    row_number() OVER () AS row_id
                FROM {self.input_ids[0]}
            )
            SELECT 
                * EXCLUDE(row_id)
            FROM row_table
            WHERE row_id % {self._divide_count} = {i}
            """
        return results


class Merge(ObjProcessor):
    def __init__(self, obj_id: str, divide_count: int, input_fs: FileSystem, output_fs: FileSystem, map_name: str=''):
        self._obj_id = obj_id
        self._divide_count = divide_count
        self._map_name = map_name
        super().__init__(VaexStorage(input_fs), VaexStorage(output_fs))

    @property
    def input_ids(self):
        return [f'{self._map_name}.{self._obj_id}.{i}.parquet' for i in range(self._divide_count)]

    @property
    def output_ids(self):
        return [self._obj_id]

    def transform(self, inputs: List[vx.DataFrame]) -> List[vx.DataFrame]:
        merged_table = vx.concat(inputs)
        return [merged_table]
