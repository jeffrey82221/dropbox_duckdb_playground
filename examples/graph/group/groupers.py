from batch_framework.etl import SQLExecutor
from batch_framework.rdb import RDB
from batch_framework.filesystem import FileSystem
from .meta import GroupingMeta

class NodeGrouper(SQLExecutor):
    """
    Group nodes of subgraph and save node tables to target folder
    """
    def __init__(self, meta: GroupingMeta, rdb: RDB, input_fs: FileSystem, output_fs: FileSystem):
        self._meta = meta
        super().__init__(rdb, input_fs=input_fs, output_fs=output_fs)

    @property
    def input_ids(self):
        return self._meta.input_nodes
    
    @property
    def output_ids(self):
        return [f'node_{n}' for n in self._meta.output_nodes]
    
    def sqls(self, **kwargs):
        return self._meta.node_grouping_sqls
    

class LinkGrouper(SQLExecutor):
    """
    Group links of subgraph and save link tables to target folder
    """
    def __init__(self, meta: GroupingMeta, rdb: RDB, input_fs: FileSystem, output_fs: FileSystem):
        self._meta = meta
        super().__init__(rdb, input_fs=input_fs, output_fs=output_fs)

    @property
    def input_ids(self):
        return self._meta.input_links
    
    @property
    def output_ids(self):
        return [f'link_{n}' for n in self._meta.output_links]
    
    def sqls(self, **kwargs):
        return self._meta.link_grouping_sqls