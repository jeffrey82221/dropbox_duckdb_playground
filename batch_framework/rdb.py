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

    def __init__(self, db_name: str=''):
        self._db_name = db_name
        self._conn = None
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

    def get_conn(self):
        """
        Create New DB connection object
        """
        raise NotImplementedError

class DuckDBBackend(RDB):
    def __init__(self, persist_fs: Optional[FileSystem]=None, db_name: Optional[str]=None):
        if persist_fs:
            if not isinstance(persist_fs, LocalBackend):
                if persist_fs.check_exists(db_name):
                    # download data to local from remote file system
                    buff = persist_fs.download_core(db_name)
                    lb = LocalBackend()
                    lb.upload_core(buff, self._db_name)
                    assert os.path.exists('./' + db_name), f'db_name: {db_name} does not exist'
        self._persist_fs = persist_fs
        super().__init__(db_name)

    @property
    def conn(self):
        """
        Get thread local used connection.
        """
        if self._conn is None:
            if self._persist_fs is None:
                conn = duckdb.connect(database=':memory:')
            else:
                conn = duckdb.connect(database=self._persist_fs._directory + self._db_name)
            self._conn = conn
        return self._conn
        
    def get_conn(self):
        return self.conn.cursor()

    def register(self, table_name: str, table: object):
        conn = self.conn
        try:
            conn.register(table_name, table)
        except BaseException as e:
            raise ValueError(f'table_name: {table_name}') from e
        

    def execute(self, sql: str) -> object:
        conn = self.conn
        try:
            return conn.execute(sql)
        except BaseException as e:
            raise ValueError(sql) from e

    def commit(self):
        """
        Upload current status of duckdb to remote file system
        """
        assert not isinstance(self._persist_fs, LocalBackend), 'No need to commit for local duckdb dump storage.'
        lb = LocalBackend()
        buff = lb.download_core(self._db_name)
        self._persist_fs.upload_core(buff, self._db_name)

