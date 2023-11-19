from typing import List
from batch_framework.storage import PandasStorage
from batch_framework.filesystem import FileSystem
from batch_framework.etl import SQLExecutor, ETLGroup
from batch_framework.rdb import RDB
from ..meta import ERMeta
from .canon import CanonMatcher
from .messy import MessyMatcher

class MappingGenerator(ETLGroup):
    def __init__(self, meta: ERMeta, subgraph_fs: FileSystem, mapping_fs: FileSystem, model_fs: FileSystem, rdb: RDB):
        canon_matcher = CanonMatcher(meta,
            PandasStorage(subgraph_fs), 
            PandasStorage(mapping_fs),
            model_fs=model_fs,
            threshold=0.25
        )
        messy_matcher = MessyMatcher(
            meta,
            PandasStorage(subgraph_fs), 
            PandasStorage(mapping_fs),
            model_fs=model_fs,
            threshold=0.5
        )
        mapping_combiner = MappingCombiner(
            meta,
            rdb,
            workspace_fs=mapping_fs
        )
        super().__init__(
            canon_matcher,
            messy_matcher,
            mapping_combiner
        )
        self._meta = meta

    @property
    def input_ids(self):
        return [
            self._meta.messy_node,
            self._meta.canon_node,
        ]

    @property
    def external_input_ids(self) -> List[str]:
        return self.input_ids
    
    @property
    def output_ids(self):
        return [
            f'mapper_{self._meta.messy_node}' + '_clean'
        ]


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