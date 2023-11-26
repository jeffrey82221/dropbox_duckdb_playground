"""
TODO:
    - [ ] Enable Not Provide of Canon Node & Canon Lambda 
    - [ ] Adding links / nodes property convertor to MetaGraph class
"""
from typing import List, Dict, Set, Callable, Optional, Tuple
from metagraph import MetaGraph

__all__ = ['ERMeta']

class ERMeta:
    """Data Class holding Metadata about Entity Resolution
    """
    def __init__(self, 
                 messy_node: str, 
                 dedupe_fields: List[Dict[str, str]], 
                 messy_lambda: Callable=lambda record: record, 
                 canon_node: Optional[str] = None, 
                 canon_lambda: Callable=lambda record: record
        ):
        self.messy_node = messy_node
        self.dedupe_fields = dedupe_fields
        self.messy_lambda = messy_lambda
        self.canon_node = canon_node
        self.canon_lambda = canon_lambda

    def combine_with_metagraph(self, metagraph: MetaGraph) -> MetaGraph:
        self._subgraphs = metagraph._subgraphs
        

    @property
    def source_columns(self) -> Set[Tuple[str, str]]:
        """
        Return:
            key: name of node or link that should be cleaned
            value: column name of the node/link table that should be cleaned
        """
        results = []
        for link, (src_node, target_node) in self._subgraphs.items():
            if src_node == self.messy_node:
                results.append((src_node, 'node_id'))
                results.append((link, 'from_id'))
            if target_node == self.messy_node:
                results.append((target_node, 'node_id'))
                results.append((link, 'to_id'))
        return set(results)
