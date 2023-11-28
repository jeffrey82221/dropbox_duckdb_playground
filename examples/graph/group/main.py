from typing import List
from batch_framework.filesystem import FileSystem
from batch_framework.etl import ETLGroup
from batch_framework.rdb import RDB
from .groupers import NodeGrouper, LinkGrouper
from .meta import GroupingMeta

class GraphGrouper(ETLGroup):
    def __init__(self, meta: GroupingMeta, rdb: RDB, input_fs: FileSystem, node_fs: FileSystem, link_fs: FileSystem):
        node_grouper = NodeGrouper(
            meta=meta,
            rdb=rdb,
            input_fs=input_fs,
            output_fs=node_fs
        )
        link_grouper = LinkGrouper(
            meta=meta,
            rdb=rdb,
            input_fs=input_fs,
            output_fs=link_fs
        )
        self._meta = meta
        self._inputs = meta.input_nodes + meta.input_links
        self._outputs = meta.output_nodes + meta.output_links
        super().__init__(node_grouper, link_grouper)

    @property
    def external_input_ids(self) -> List[str]:
        return self._inputs
    
    @property
    def input_ids(self):
        return self._inputs
    
    @property
    def output_ids(self):
        return self._outputs
