from typing import List
from batch_framework.filesystem import FileSystem
from batch_framework.etl import ETLGroup
from batch_framework.rdb import RDB
from .groupers import NodeGrouper, LinkGrouper
from .meta import GroupingMeta

class GraphGrouper(ETLGroup):
    def __init__(self, meta: GroupingMeta, rdb: RDB, input_fs: FileSystem, output_fs: FileSystem):
        node_grouper = NodeGrouper(
            meta=meta,
            rdb=rdb,
            input_fs=input_fs,
            output_fs=output_fs
        )
        link_grouper = LinkGrouper(
            meta=meta,
            rdb=rdb,
            input_fs=input_fs,
            output_fs=output_fs
        )
        self._meta = meta
        self._inputs = node_grouper.input_ids + link_grouper.input_ids
        self._outputs = node_grouper.output_ids + link_grouper.output_ids
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
