"""Divide and conquer module 
TODO:
    - [X] making this a class decorator
    - [X] using vaex storage to enable zero copy (lower RAM usage)
    - [X] Enable setting of max active parallel thread on group level & job level
    - [X] Allow duckdb to support multithread
"""
from typing import List
import os
import pandas as pd
import vaex as vx
import traceback
from dill.source import getsource
from batch_framework.etl import ObjProcessor, ETLGroup, SQLExecutor
from batch_framework.storage import Storage, VaexStorage, PandasStorage
from batch_framework.filesystem import FileSystem
from batch_framework.rdb import DuckDBBackend
__all__ = ['MapReduce']

class MapReduce(ETLGroup):
    """
    Decorating an ETL object into MapReduced version
    """
    def __init__(self, map: ObjProcessor, parallel_count: int, tmp_fs: FileSystem, has_external_input: bool=False):
        assert isinstance(map, ObjProcessor), f'map object for MapReduce should be ObjProcessor'
        self._map = map
        map_name = type(map).__name__
        self._map_name = map_name
        self._tmp_fs = tmp_fs
        self._parallel_count = parallel_count
        self._has_external_input = has_external_input
        input_type = self._map.get_input_type()
        output_type = self._map.get_output_type()
        class MapClass(ObjProcessor):
            def __init__(self, input_storage: Storage, partition_id: int):
                self._partition_id = partition_id
                super().__init__(input_storage)
            
            @property
            def input_ids(self):
                return [f'{map_name}.{id}.{self._partition_id}.parquet' for id in map.input_ids]
            
            @property
            def output_ids(self):
                return [f'{map_name}.{id}.{self._partition_id}.parquet'.replace('.', '_') for id in map.output_ids]
            
            def transform(self, inputs: List[input_type], **kwargs) -> List[output_type]:
                try:
                    assert len(inputs) == len(map.input_ids), f'inputs of transform does not equals input id count. inputs: {inputs}; input_ids: {self.input_ids}'
                    return map.transform(inputs, **kwargs)
                except BaseException as e:
                    content = getsource(map.transform)
                    raise ValueError(f'Error happened on {self._partition_id}th MapClass execution - transform of {map}:\n{content}')
            
            def start(self, **kwargs):
                return map.start(**kwargs)
        units = [
            AddPartitionKey(map_name, self._map.input_ids, self._map._input_storage._backend, tmp_fs, parallel_count)
        ] + [
            EfficientDivide(map_name, id, parallel_count, tmp_fs, tmp_fs) for id in self._map.input_ids
        ] + [
            MapClass(type(self._map._input_storage)(tmp_fs), i) for i in range(parallel_count)
        ] + [
            EfficientMerge(
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
        self._drop_input_tmps()
        self._drop_partitions(self.input_ids + self.output_ids)

    def _drop_input_tmps(self):
        for id in self.input_ids:
            path = self._tmp_fs._directory + f'{self._map_name}.{id}' + f'.full.parquet'
            MapReduce._drop_partition(path)
    
    def _drop_partitions(self, ids: List[str]):
        for id in ids:
            for i in range(self._parallel_count):
                path = self._tmp_fs._directory + f'{self._map_name}.{id}' + f'.{i}.parquet'
                MapReduce._drop_partition(path)

    @staticmethod
    def _drop_partition(path):
        if os.path.exists(path):
            os.remove(path)
            print('[drop_partition]', path, 'removed!')
        else:
            print('[drop_partition]', path, 'not found!')


class AddPartitionKey(SQLExecutor):
    def __init__(self, 
                 map_name: str,
                 obj_ids: List[str], 
                 input_fs: FileSystem, 
                 output_fs: FileSystem,
                 divide_count: int
                 ):
        self._map_name = map_name
        self._obj_ids = obj_ids
        self._divide_count = divide_count
        super().__init__(rdb=DuckDBBackend(), input_fs=input_fs, output_fs=output_fs)

    @property
    def input_ids(self):
        return self._obj_ids
    
    @property
    def output_ids(self):
        return [f'{self._map_name}.{id}.full.parquet' for id in self._obj_ids]
    
    def sqls(self, **kwargs):
        results = dict()
        for in_id, out_id in zip(self.input_ids, self.output_ids):
            results[out_id] = f"""
            WITH row_table AS (
                SELECT
                    *,
                    (row_number() OVER ()) AS row_id
                FROM {in_id}
            )
            SELECT 
                *, 
                row_id % {self._divide_count} AS partition
            FROM row_table
            """
        return results

class EfficientDivide(ObjProcessor):
    def __init__(self, 
                 map_name: str,
                 obj_id: str, 
                 divide_count: int, 
                 input_fs: FileSystem, 
                 output_fs: FileSystem
                 ):
        self._map_name = map_name
        self._obj_id = obj_id
        self._divide_count = divide_count
        super().__init__(VaexStorage(input_fs), VaexStorage(output_fs))
    
    @property
    def input_ids(self):
        return [f'{self._map_name}.{self._obj_id}.full.parquet']
    
    @property
    def output_ids(self):
        return [f'{self._map_name}.{self._obj_id}.{i}.parquet' for i in range(self._divide_count)]

    def transform(self, inputs: List[vx.DataFrame]) -> List[vx.DataFrame]:
        table = inputs[0]
        columns = table.get_column_names()
        columns.remove('partition')
        columns.remove('row_id')
        results = []
        for _, df in table.groupby('partition'):
            assert len(df) > 0, 'output dataframes of EfficientDivide should not be 0 size.'
            results.append(df[columns])
        return results


class EfficientMerge(SQLExecutor):
    def __init__(self, obj_id: str, divide_count: int, input_fs: FileSystem, output_fs: FileSystem, map_name: str=''):
        self._obj_id = obj_id
        self._divide_count = divide_count
        self._map_name = map_name
        super().__init__(DuckDBBackend(), input_fs, output_fs)
    
    @property
    def input_ids(self):
        return [f'{self._map_name}.{self._obj_id}.{i}.parquet'.replace('.', '_') for i in range(self._divide_count)]

    @property
    def output_ids(self):
        return [self._obj_id]

    def sqls(self, **kwargs):
        sql = '\nUNION\n'.join([f"(SELECT * FROM {id})" for id in self.input_ids])
        return {
            self._obj_id: sql
        }