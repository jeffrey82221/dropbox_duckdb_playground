"""
Validate ID(s) of a Subgraph
"""
from typing import List
import pandas as pd
from batch_framework.etl import DFProcessor
from batch_framework.rdb import DuckDBBackend
from batch_framework.filesystem import LocalBackend
from batch_framework.storage import PandasStorage
from subgraph.links import LinkExtractor
from subgraph.nodes import NodeExtractor

class IDValidator(DFProcessor):
    def __init__(self, link: str, src_node: str, target_node: str, input_storage: PandasStorage):
        self._link = link
        self._src_node = src_node
        self._target_node = target_node
        super().__init__(input_storage)


    @property
    def input_ids(self):
        return [
            self._link,
            self._src_node,
            self._target_node
        ]

    @property
    def output_ids(self):
        return []
    
    def transform(self, inputs: List[pd.DataFrame]) -> List[pd.DataFrame]:
        link_df = inputs[0]
        src_node_df = inputs[1]
        target_node_df = inputs[2]
        link_src_ids = set(link_df.from_id.tolist())
        link_target_ids = set(link_df.to_id.tolist())
        src_ids = set(src_node_df.node_id.tolist())
        target_ids = set(target_node_df.node_id.tolist())
        assert link_src_ids.issubset(src_ids), 'some source node in link is not in the source node table'
        assert link_target_ids.issubset(target_ids), 'some target node in link is not in the target node table'
        return []

class Validation:
    def __init__(self, storage: PandasStorage):
        self._storage = storage

    @property
    def subgraph_list(self):
        return {
            'has_requirement': ('package', 'requirement'),
            'has_author': ('package', 'author'), 
            'has_maintainer': ('package', 'maintainer'), 
            'has_license': ('package', 'license'), 
            'has_docs_url': ('package', 'docs_url'), 
            'has_home_page': ('package', 'home_page'), 
            'has_project_url': ('package', 'project_url')
        }

    @property
    def validator_list(self):
        results = []
        for link, (src_node, target_node) in self.subgraph_list.items():
            results.append(IDValidator(link, src_node, target_node, self._storage))
        return results
    
    def execute(self):
        for op in self.validator_list:
            print('Start validating', op._link)
            op.execute()
            print('End validating', op._link)


if __name__ == '__main__':
    input_fs = LocalBackend('./data/canon/output/')
    fs = LocalBackend('./data/subgraph/output/')
    db = DuckDBBackend()
    link_op = LinkExtractor(db, input_fs=input_fs)
    node_op = NodeExtractor(db, input_fs=input_fs)
    link_op.execute()
    node_op.execute()
    val_op = Validation(PandasStorage(db))
    val_op.execute()