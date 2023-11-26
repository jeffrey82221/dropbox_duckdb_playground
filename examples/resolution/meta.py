"""
TODO:
    - [ ] Enable Not Provide of Canon Node & Canon Lambda 
"""
from typing import List, Dict, Callable, Optional

__all__ = ['ERMeta']

class ERMeta:
    """Data Class holding Metadata about Entity Resolution
    """
    def __init__(self, messy_node: str, dedupe_fields: List[Dict[str, str]], messy_lambda: Callable=lambda record: record, canon_node: Optional[str] = None, canon_lambda: Callable=lambda record: record):
        self.messy_node = messy_node
        self.canon_node = canon_node
        self.dedupe_fields = dedupe_fields
        self.messy_lambda = messy_lambda
        self.canon_lambda = canon_lambda