from typing import List, Dict, Callable

__all__ = ['ERMeta']

class ERMeta:
    """Data Class holding Metadata about Entity Resolution
    """
    def __init__(self, messy_node: str, canon_node: str, dedupe_fields: List[Dict[str, str]], messy_lambda: Callable=lambda record: record, canon_lambda: Callable=lambda record: record):
        self.messy_node = messy_node
        self.canon_node = canon_node
        self.dedupe_fields = dedupe_fields
        self.messy_lambda = messy_lambda
        self.canon_lambda = canon_lambda