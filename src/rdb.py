import duckdb
from .backend import Backend
class RDB(Backend):
    """RDB backend for storing tabular data
    """
    def __init__(self, schema: str=''):
        super().__init__(schema)

class DuckDBBackend(RDB):
    def __init__(self):
        self._db = duckdb