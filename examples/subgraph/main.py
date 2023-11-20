from typing import List
from batch_framework.rdb import RDB
from batch_framework.etl import ETLGroup
from batch_framework.storage import PandasStorage
from batch_framework.filesystem import FileSystem
from .links import LinkExtractor
from .nodes import NodeExtractor
from .validate import Validator
from .metagraph import MetaGraph

class SubgraphExtractor(ETLGroup):
    """
    Extract Link and Node from Raw Tabular Data
    """
    def __init__(self, metagraph: MetaGraph, db: RDB, input_fs: FileSystem, output_fs: FileSystem):
        self._metagraph = metagraph
        link_op = LinkExtractor(db, input_fs=input_fs, output_fs=output_fs)
        node_op = NodeExtractor(db, input_fs=input_fs, output_fs=output_fs)
        val_op = Validator(metagraph, PandasStorage(db))
        super().__init__(link_op, node_op, val_op)

    @property
    def input_ids(self) -> List[str]:
        return [
            'latest_package',
            'latest_requirement',
            'latest_url'
        ]
    
    @property
    def external_input_ids(self) -> List[str]:
        return self.input_ids
    
    @property
    def output_ids(self) -> List[str]:
        return self._metagraph.nodes + self._metagraph.links
    