"""
TODO:
- [X] Enable Not Provide of Canon Node & Canon Lambda 
- [X] Adding links / nodes property convertion to MetaGraph class
"""
from typing import List, Dict, Callable, Optional, Tuple, Set
from ..group import GroupingMeta

__all__ = ['ERMeta']

class ERMeta:
    """Data Class holding Metadata about Entity Resolution
    """
    def __init__(self, 
                 subgraphs: Dict[str, Tuple[str, str]], 
                 messy_node: str, 
                 dedupe_fields: List[Dict[str, str]], 
                 messy_lambda: Callable=lambda record: record, 
                 canon_node: Optional[str] = None, 
                 canon_lambda: Callable=lambda record: record
        ):
        self.subgraphs = subgraphs
        self.messy_node = messy_node
        self.messy_items = self.get_messy_items()
        self.dedupe_fields = dedupe_fields
        self.messy_lambda = messy_lambda
        self.canon_node = canon_node
        self.canon_lambda = canon_lambda
        self.id_convertion_messy_items: List[Tuple[str, str]] = []

    @property
    def has_canon(self) -> bool:
        return self.canon_node is not None

    def alter_grouping_way(self, meta: GroupingMeta) -> GroupingMeta:
        """
        Attach ER to Metagraph

        In detail, 
            1. Read subgraphs and determine which node or link and which columns should be ID converted.
            2. Add IDConvertor infos to a data variable.
            3. Change meta original links/nodes property to new links/nodes property.
        """
        assert len(self.id_convertion_messy_items) == 0, 'def alter_grouping_way should only be called once'
        self.repeated_items: Dict[str, str] = dict([(item[0], item[0]) for item in self.messy_items])
        for messy_item, column in self.messy_items:
            current_nm = self.repeated_items[messy_item]
            self.id_convertion_messy_items.append((current_nm, column))
            new_nm = current_nm + 'Q'
            if column == 'node_id':
                meta.alter_input_node(current_nm, new_nm)
            else:
                meta.alter_input_link(current_nm, new_nm)
            self.repeated_items[messy_item] = new_nm
        return meta
    
    @property
    def input_ids(self) -> List[str]:
        results = [self.messy_node]
        if self.has_canon:
            results.append(self.canon_node)
        for messy_item, _ in self.messy_items:
            results.append(messy_item)
        return list(set(results))
    
    @property
    def output_ids(self) -> List[str]:
        results = []
        for origin_name, new_name in self.repeated_items.items():
            if origin_name != new_name:
                results.append(new_name)
        return results

    def get_messy_items(self) -> Set[Tuple[str, str]]:
        results = []
        for link, (from_node, to_node) in self.subgraphs.items():
            if from_node == self.messy_node:
                results.append((from_node, 'node_id'))
                results.append((link, 'from_id'))
            if to_node == self.messy_node:
                results.append((to_node, 'node_id'))
                results.append((link, 'to_id'))
        return set(results)