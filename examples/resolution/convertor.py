"""
Build Messy Node Id Convertor

TODO:
- [ ] In mapper.py, combine CanonMatcher & MessyMatcher & a new 
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
    def __init__(self, meta: ERMeta, db: RDB, source_fs: FileSystem, target_fs: FileSystem, workspace_fs: FileSystem):
        self._source_fs = source_fs
        self._workspace_fs = workspace_fs
        self._meta = meta
        super().__init__(db, input_fs=workspace_fs, output_fs=target_fs)
        
    @property
    def input_ids(self):
        return [
            self._meta.messy_node,
            f'mapper_{self._meta.messy_node}2{self._meta.canon_node}',
            f'mapper_{self._meta.messy_node}'
        ]

    @property
    def output_ids(self):
        return [self._meta.messy_node + '_clean']
    
    def start(self):
        buff = self._source_fs.download_core(self._meta.messy_node)
        buff.seek(0)
        self._workspace_fs.upload_core(buff, self._meta.messy_node)

    def sqls(self):
        return {
            self._meta.messy_node + '_clean': f"""
                WITH mapping_table AS (
                    SELECT 
                        t1.messy_id,
                        COALESCE(t2.canon_id, t1.cluster_id) AS new_id
                    FROM mapper_{self._meta.messy_node} AS t1
                    LEFT JOIN mapper_{self._meta.messy_node}2{self._meta.canon_node} AS t2
                    ON t1.messy_id = t2.messy_id
                )
                SELECT
                    t2.messy_id AS node_id,
                    t1.* EXCLUDE (node_id)
                FROM {self._meta.messy_node} AS t1
                LEFT JOIN mapping_table AS t2
                ON t1.node_id = t2.messy_id
            """
        }
    
    def end(self):
        input_cnt = self._rdb.execute(
            f'''
            SELECT count(*) AS count FROM {self._meta.messy_node}
            '''
        ).df()['count'].values[0]
        output_cnt = self._rdb.execute(
            f'''
            SELECT count(*) AS count FROM {self._meta.messy_node}_clean
            '''
        ).df()['count'].values[0]
        assert input_cnt == output_cnt, 'Input table size != Output Table Size'
        print(f'Finish Converting ID of `{self._meta.messy_node}` Node')