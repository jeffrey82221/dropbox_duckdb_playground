"""
Validate ID(s) of a Subgraph
"""
from typing import List
import pandas as pd
from batch_framework.etl import ObjProcessor
from batch_framework.storage import PandasStorage

class IDValidator(ObjProcessor):
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
        print('subgraph', self._link, '- #Link:', len(link_df))
        print('subgraph', self._link, '- #Src Nodes:', len(src_node_df))
        print('subgraph', self._link, '- #Target Nodes:', len(target_node_df))
        link_src_ids = set(link_df.from_id.tolist())
        print('subgraph', self._link, '- #Link Src Nodes:', len(link_src_ids))
        link_target_ids = set(link_df.to_id.tolist())
        print('subgraph', self._link, '- #Link Target Nodes:', len(link_target_ids))
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

