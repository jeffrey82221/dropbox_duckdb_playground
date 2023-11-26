from typing import List
from batch_framework.etl import SQLExecutor
from batch_framework.rdb import RDB
from batch_framework.filesystem import FileSystem
from metagraph import MetaGraph

__all__ = ['LinkExtractor', 'NodeExtractor']

class ExtractorBase(SQLExecutor):
    def __init__(self, metagraph: MetaGraph, rdb: RDB, input_fs: FileSystem, output_fs: FileSystem):
        self._metagraph = metagraph
        super().__init__(rdb, input_fs=input_fs, output_fs=output_fs)

    @property
    def input_ids(self):
        return self._metagraph.input_ids
    
class NodeExtractor(ExtractorBase):
    @property
    def output_ids(self):
        return self._metagraph.nodes
    
    def sqls(self, **kwargs):
        return self._metagraph.node_sqls

class LinkExtractor(ExtractorBase):
    @property
    def output_ids(self):
        return self._metagraph.links
    
    def sqls(self, **kwargs):
        return self._metagraph.link_sqls