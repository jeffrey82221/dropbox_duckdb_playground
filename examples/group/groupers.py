from batch_framework.etl import SQLExecutor
from batch_framework.rdb import RDB
from batch_framework.filesystem import FileSystem
from metagraph import MetaGraph

class NodeGrouper(SQLExecutor):
    """
    Group nodes of subgraph and save node tables to target folder
    """
    def __init__(self, metagraph: MetaGraph, rdb: RDB, input_fs: FileSystem, output_fs: FileSystem):
        self._metagraph = metagraph
        super().__init__(rdb, input_fs=input_fs, output_fs=output_fs)

    @property
    def input_ids(self):
        return self._metagraph.nodes
    
    @property
    def output_ids(self):
        return self._metagraph.final_nodes
    
    def sqls(self, **kwargs):
        return self._metagraph.node_grouping_sqls
    

class LinkGrouper(SQLExecutor):
    """
    Group links of subgraph and save link tables to target folder
    """
    def __init__(self, metagraph: MetaGraph, rdb: RDB, input_fs: FileSystem, output_fs: FileSystem):
        self._metagraph = metagraph
        super().__init__(rdb, input_fs=input_fs, output_fs=output_fs)

    @property
    def input_ids(self):
        return self._metagraph.links
    
    @property
    def output_ids(self):
        return self._metagraph.final_links
    
    def sqls(self, **kwargs):
        return self._metagraph.link_grouping_sqls