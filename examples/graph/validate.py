
from typing import List
import pandas as pd
from batch_framework.etl import ObjProcessor
from batch_framework.storage import PandasStorage

class LinkIDValidator(ObjProcessor):
    """
    Check whether link source/target IDs are subset 
    of corresponding node IDs.
    """
    def __init__(self, link: str, node: str, id_type: str, input_storage: PandasStorage):
        assert id_type in ['from_id', 'to_id']
        self._link = link
        self._node = node
        self._id_type = id_type
        super().__init__(input_storage)


    @property
    def input_ids(self):
        return [
            self._link,
            self._node
        ]

    @property
    def output_ids(self):
        return []
    
    def transform(self, inputs: List[pd.DataFrame]) -> List[pd.DataFrame]:
        link_df = inputs[0]
        node_df = inputs[1]
        print('subgraph', self._link, '- #Link:', len(link_df))
        print('subgraph', self._node, '- #Nodes:', len(node_df))        
        link_ids = set(link_df[self._id_type].tolist())
        print('subgraph', self._link, '- #Link Nodes:', len(link_ids))
        node_ids = set(node_df.node_id.tolist())
        assert link_ids.issubset(node_ids), f'some {self._id_type} in link is not in the node table'
        return []