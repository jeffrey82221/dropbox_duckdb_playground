import duckdb
from .backend import Backend
class RDB(Backend):
    """RDB backend for storing tabular data
    """
    def __init__(self, schema, conn):
        self._schema = schema
        self._conn = conn
        super().__init__()
        
class DuckDBBackend(RDB):
    def __init__(self):
        super().__init__('', duckdb.connect())

    def register(self, table_name: str, table: object):
        self._conn.register(table_name, table)

    def execute(self, sql):
        return self._conn.execute(sql)