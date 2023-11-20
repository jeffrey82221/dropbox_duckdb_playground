"""
Build Messy Node Id Convertor

TODO:
- [X] In mapper.py, combine CanonMatcher & MessyMatcher & a new 
    MapCombiner to produce a Mapping Table for Messy Node Ids
- [ ] In convertor.py, allow class to takes `node` or `node_of_link`
    as input for ID convertion. 
"""
from batch_framework.etl import SQLExecutor
from batch_framework.filesystem import FileSystem
from batch_framework.rdb import RDB
from .meta import ERMeta

__all__ = ['IDConvertor']

class IDConvertor(SQLExecutor):
    """
    Converting ID of Messy Node to Cleaned Node ID
    """
    def __init__(self, meta: ERMeta, db: RDB, input_fs: FileSystem, output_fs: FileSystem):
        self._meta = meta
        super().__init__(db, input_fs=input_fs, output_fs=output_fs)
        
    @property
    def input_ids(self):
        return [
            self._meta.messy_node,
            f'mapper_{self._meta.messy_node}_clean'
        ]

    @property
    def output_ids(self):
        return [self._meta.messy_node + '_clean']
    
    
    def sqls(self):
        return {
            self._meta.messy_node + '_clean': f"""
                SELECT
                    t2.new_id AS node_id,
                    t1.* EXCLUDE (node_id)
                FROM {self._meta.messy_node} AS t1
                LEFT JOIN mapper_{self._meta.messy_node}_clean AS t2
                ON t1.node_id = t2.messy_id
            """
        }