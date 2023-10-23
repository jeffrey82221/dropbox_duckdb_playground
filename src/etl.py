"""
SQL Process
NOTE:
How to process with SQL?
If: input_storage and output_storage are the storage and all using the same RDB backend.
    -> Run SQL on the same RDB. Use Jinja to stitch input_id, output_id, and select sql together.
Else If: input_storage is RDB and output_storage is not the same storage.
    -> Run SQL on the input_storage RDB. Use Jinja to stitch input_id, and select sql.
    -> Extract the result of SQL as python object supported by output_storage and save them into output_storage.
Else:
    -> Extract input tables as save them into DuckDB
    -> Run SQL in DuckDB
    -> Take output from DuckDB and save to output storage.
"""
from typing import List, Dict
import abc
from .storage import Storage
from .rdb import RDB

__all__ = [
    'DFProcessor',
    'SQLExecutor'
]
class ETL:
    """
    Basic Interface for defining a unit of ETL flow.
    """
    @abc.abstractproperty
    def input_ids(self) -> List[str]:
        """
        Returns:
            List[str]: a list of input object ids
        """
        raise NotImplementedError

    @abc.abstractproperty
    def output_ids(self) -> List[str]:
        """
        Args:
            **kwargs: some additional variable passed from scheduling engine (e.g., Airflow)

        Returns:
            List[str]: a list of output object ids
        """
        raise NotImplementedError
    
    @abc.abstractmethod
    def execute(self, **kwargs):
        """Execute ETL 
        """
        raise NotImplementedError

class SQLExecutor(ETL):
    """Basic interface for SQL executor
    """
    def __init__(self, rdb: RDB):
        self._rdb = rdb

    @abc.abstractmethod
    def sqls(self, **kwargs) -> Dict[str, str]:
        """Select SQL for transforming the input tables.
        
        Args:
            **kwargs: some additional variable passed from scheduling engine (e.g., Airflow)

        Returns:
            Dict[str, str]: The transformation SQLs. The key 
            is the output_id to be insert into. The value is 
            the corresponding SQL. 
        """
        raise NotImplementedError

    def execute(self, **kwargs):
        """
        Args:
            **kwargs: some additional variable passed from scheduling engine (e.g., Airflow)
        """
        assert all([id in self.sqls(**kwargs) for id in self.output_ids]), 'sqls key should corresponds to the output_ids'
        for output_id, sql in self.sqls(**kwargs).items():
            self._rdb.execute(f'''
                  CREATE TABLE {output_id} AS ({sql});
            ''')
    
class DFProcessor(ETL):
    """
    Basic Interface for defining a dataframe processing unit of ETL flow.
    """
    def __init__(self, input_storage: Storage, output_storage: Storage):
        self._input_storage = input_storage
        self._output_storage = output_storage

    @abc.abstractmethod
    def transform(self, inputs: List[object], **kwargs) -> List[object]:
        """
        Args:
            Inputs: List of object to be load from storage.
        Returns:
            List[object]: List of object to be saved to storage.
        """
        raise NotImplementedError


    def execute(self, **kwargs):
        """
        Args:
            **kwargs: some additional variable passed from scheduling engine (e.g., Airflow)
        Run ETL (extract, transform, and load)
        """
        input_objs = self._extract(**kwargs)
        assert isinstance(
            input_objs, list), 'Input of transform should be a list of object'
        assert all([isinstance(obj, self.get_input_type()) for obj in input_objs]
                ), f'One of the input_obj is not {self.get_input_type()}'
        output_objs = self.transform(input_objs, **kwargs)
        assert isinstance(
            output_objs, list), 'Output of transform should be a list of object'
        assert all([isinstance(obj, self.get_output_type()) for obj in output_objs]
                ), f'One of the output_obj is not {self.get_output_type()}'
        self._load(output_objs, **kwargs)

    def get_input_type(self):
        return self.transform.__annotations__['inputs'].__args__[0]

    def get_output_type(self):
        return self.transform.__annotations__['return'].__args__[0]

    def _extract(self, **kwargs) -> List[object]:
        """
        Args:
            **kwargs: some additional variable passed from scheduling engine (e.g., Airflow)
        Returns:
            List[object]: List of dataframe object to be passed to `transform`.
        """
        input_tables = [self._input_storage.download(
            id) for id in self.input_ids]
        return input_tables

    def _load(self, output_tables: List[object], **kwargs):
        """
        Args:
            output_tables: List[object]: List of dataframe object passed from `transform`.
            **kwargs: some additional variable passed from scheduling engine (e.g., Airflow)
        """
        for id, table in zip(self.output_ids, output_tables):
            self._output_storage.upload(table, id)
