"""
RDB classes: 
Can register table, execute sql, and extract table object.
"""
import abc
import duckdb
from .backend import Backend


class RDB(Backend):
    """RDB backend for storing tabular data
    """

    def __init__(self, schema, conn):
        self._schema = schema
        self._conn = conn
        super().__init__()

    @abc.abstractmethod
    def register(self, table_name: str, table: object):
        """
        Register a table object into RDB by a table_name
        Args:
            - table_name: The table name.
            - table: The table object.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def execute(self, sql: str) -> object:
        """
        Execute a sql and extract selected table as a table object.

        Args:
            - sql: The sql to be executed.
        Returns:
            - object: The table object.
        """
        raise NotImplementedError
        

class DuckDBBackend(RDB):
    def __init__(self):
        super().__init__('', duckdb.connect())

    def register(self, table_name: str, table: object):
        self._conn.register(table_name, table)

    def execute(self, sql: str) -> object:
        return self._conn.execute(sql)
