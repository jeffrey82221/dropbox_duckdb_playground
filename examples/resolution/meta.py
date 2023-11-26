"""
TODO:
- [X] Enable Not Provide of Canon Node & Canon Lambda 
- [ ] Adding links / nodes property convertion to MetaGraph class
"""
from typing import List, Dict, Callable, Optional
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

    @property
    def has_canon(self) -> bool:
        return self.canon_node is not None

    def attach_to_metagraph(self, metagraph: MetaGraph) -> MetaGraph:
        """
        Attach ER to Metagraph

        In detail, 
            1. Read subgraphs and determine which node or link and which columns should be ID converted.
            2. Add IDConvertor to a class dictionary (key: output ids).
            3. Deep copy metagraph and change its original links/nodes property to new links/nodes property.
        """
        self._subgraphs = metagraph._subgraphs