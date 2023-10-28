"""
RDB classes: 
Can register table, execute sql, and extract table object.
"""
from typing import Optional
import abc
import duckdb
import os
from .backend import Backend
from .filesystem import FileSystem, LocalBackend

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

    def close(self):
        """
        Close db connection
        """
        self._conn.close()

class DuckDBBackend(RDB):
    def __init__(self, persist_fs: Optional[FileSystem]=None, db_name: Optional[str]=None):
        self._persist_fs = persist_fs
        self._db_name = db_name
        if persist_fs:
            if not isinstance(persist_fs, LocalBackend):
                if persist_fs.check_exists(db_name):
                    # download data to local from remote file system
                    buff = persist_fs.download_core(db_name)
                    lb = LocalBackend()
                    lb.upload_core(buff, self._db_name)
                    assert os.path.exists('./' + db_name), f'db_name: {db_name} does not exist'
            super().__init__('', duckdb.connect(database='./' + db_name))
        else:
            super().__init__('', duckdb.connect(database=':memory:'))

    def register(self, table_name: str, table: object):
        self._conn.register(table_name, table)

    def execute(self, sql: str) -> object:
        return self._conn.execute(sql)

    def close(self):
        self._conn.close()
    
    def commit(self):
        """
        Upload current status of duckdb to remote file system
        """
        assert not isinstance(self._persist_fs, LocalBackend), 'No need to commit for local duckdb dump storage.'
        lb = LocalBackend()
        buff = lb.download_core(self._db_name)
        self._persist_fs.upload_core(buff, self._db_name)

