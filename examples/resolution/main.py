"""
TODO:
- [ ] Add ID convertor / Links to Mapping Generator
    -> Build Entity Resolution Class
- [ ] Enable no-canon workflow in mapping generator
"""
from typing import List
import os
from batch_framework.storage import PandasStorage
from batch_framework.filesystem import FileSystem
from batch_framework.etl import SQLExecutor, ETLGroup
from batch_framework.rdb import RDB
from .meta import ERMeta
from .mapper import CanonMatcher
from .mapper import MessyMatcher
from .convertor import IDConvertor

class MappingGenerator(ETLGroup):
    def __init__(self, meta: ERMeta, subgraph_fs: FileSystem, mapping_fs: FileSystem, model_fs: FileSystem, rdb: RDB):
        self._mapping_fs = mapping_fs
        canon_matcher = CanonMatcher(meta,
            PandasStorage(subgraph_fs), 
            PandasStorage(mapping_fs),
            model_fs=model_fs,
            threshold=0.25
        )
        messy_matcher = MessyMatcher(
            meta,
            subgraph_fs,
            mapping_fs,
            model_fs,
            rdb,
            threshold=0.5
        )
        mapping_combiner = MappingCombiner(
            meta,
            rdb,
            workspace_fs=mapping_fs
        )
        etl_layers = [
            canon_matcher,
            messy_matcher,
            mapping_combiner
        ]
        for item, column in meta.id_convertion_messy_items:
            etl_layers.append(
                IDConvertor(
                    meta.messy_node,
                    item,
                    column,
                    rdb,
                    subgraph_fs,
                    subgraph_fs
                )
            )
        super().__init__(
            *etl_layers
        )
        self._meta = meta

    @property
    def input_ids(self):
        return self._meta.input_ids

    @property
    def external_input_ids(self) -> List[str]:
        return self.input_ids
    
    @property
    def output_ids(self):
        return self._meta.output_ids

    def end(self, **kwargs):
        for id in [
                f'mapper_{self._meta.messy_node}2{self._meta.canon_node}',
                f'mapper_{self._meta.messy_node}'
            ]:
            path = self._mapping_fs._directory + id
            MessyMatcher._drop_tmp(path)

    @staticmethod    
    def _drop_tmp(path):
        if os.path.exists(path):
            os.remove(path)
            print('[_drop_tmp]', path, 'dropped')
        else:
            print('[_drop_tmp]', path, 'not found')


class MappingCombiner(SQLExecutor):
    """
    Combine Messy2clean and Messy2Canon Mapping
    """
    def __init__(self, meta: ERMeta, rdb: RDB, workspace_fs: FileSystem):
        self._workspace_fs = workspace_fs
        self._meta = meta
        super().__init__(rdb, input_fs=workspace_fs)
        
    @property
    def input_ids(self):
        return [
            f'mapper_{self._meta.messy_node}2{self._meta.canon_node}',
            f'mapper_{self._meta.messy_node}'
        ]

    @property
    def output_ids(self):
        return [f'mapper_{self._meta.messy_node}' + '_clean']

    def sqls(self):
        return {
            self.output_ids[0]: f"""
                SELECT 
                    t1.messy_id,
                    COALESCE(t2.canon_id, t1.cluster_id) AS new_id
                FROM mapper_{self._meta.messy_node} AS t1
                LEFT JOIN mapper_{self._meta.messy_node}2{self._meta.canon_node} AS t2
                ON t1.messy_id = t2.messy_id
            """
        }