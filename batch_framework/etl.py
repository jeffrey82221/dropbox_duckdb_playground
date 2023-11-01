"""
ETL Class
TODO:
- [X] Split SQL executor and Processor
- [ ] Rename DFProcessor as Object Processor
"""
from paradag import DAG
from paradag import dag_run
from typing import List, Dict, Optional
import abc
from .storage import Storage, PyArrowStorage
from .filesystem import FileSystem
from .rdb import RDB

__all__ = [
    'DFProcessor',
    'SQLExecutor'
]
class ETL:
    """
    Basic Interface for defining a unit of ETL flow.
    """
    def __init__(self):
        assert isinstance(self.input_ids, list), 'def input_ids should be a property returning a list of str'
        assert isinstance(self.output_ids, list), 'def output_ids should be a property returning a list of str'
        assert len(set(self.input_ids) & set(self.output_ids)) == 0, 'There should not be an object_id on both input_ids and output_ids'
        assert len(self.input_ids) == len(set(self.input_ids)), 'There should no be repeated id in self.input_ids'
        assert len(self.output_ids) == len(set(self.output_ids)), 'There should no be repeated id in self.output_ids'

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
    
    def execute(self, **kwargs):
        self.start(**kwargs)
        self._execute(**kwargs)
        self.end(**kwargs)
        
    @abc.abstractmethod
    def start(self, **kwargs) -> None:
        """Define some action before execute start
        e.g., creating output table if not exists
        """
        pass

    @abc.abstractmethod
    def end(self, **kwargs) -> None:
        """Define some action after execute end
        e.g., validate the ouput data
        """
        pass

    @abc.abstractmethod
    def _execute(self, **kwargs):
        """Execute ETL 
        """
        raise NotImplementedError

    def build(self, dag: DAG):
        """Connecting input_ids, output_ids and execution method
        as nodes into dag.
        """
        # Step1: add execute to dag
        dag.add_vertex(self)
        # Step2: connect input_id to execute
        for input_id in self.input_ids:
            dag.add_edge(input_id, self)
        # Step3: add all output_ids into dag
        for output_id in self.output_ids:
            dag.add_vertex(output_id)
        # Step4: connect execute to ouput_id
        for output_id in self.output_ids:
            dag.add_edge(self, output_id)

class DagExecutor:
    def param(self, vertex):
        return vertex

    def execute(self, param):
        if isinstance(param, str):
            print(f'Passing Object: {param}')
        elif isinstance(param, ETL):
            print('Start Executing', type(param), param)
            param.execute()
        else:
            raise TypeError

class ETLGroup(ETL):
    """Interface for connecting multiple ETL units
    """
    def __init__(self, *etl_units: List[ETL]):
        self.etl_units = etl_units
    
    def _execute(self, **kwargs):
        """Execute ETL units
        """
        from paradag import SequentialProcessor
        
        dag = DAG()
        self.build(dag)
        dag_run(dag, processor=SequentialProcessor(), 
                executor=DagExecutor()
        )

    def build(self, dag: DAG):
        for etl_unit in self.etl_units:
            etl_unit.build(dag)
        for _id in self.output_ids:
            assert _id in dag.vertices(), f'output_id {_id} is not in dag input vertices'

class SQLExecutor(ETL):
    """Basic interface for SQL executor
    """
    def __init__(self, rdb: RDB, input_fs: Optional[FileSystem]=None, output_fs: Optional[FileSystem]=None):
        assert isinstance(rdb, RDB), 'rdb is not RDB type'
        self._rdb = rdb
        if input_fs is not None:
            assert isinstance(input_fs, FileSystem), 'input_storage of SQLExecutor should be FileSystem'
            self._input_storage = PyArrowStorage(input_fs)
        else:
            self._input_storage = None
        if output_fs is not None:
            assert isinstance(output_fs, FileSystem), 'output_storage of SQLExecutor should be FileSystem'
            self._output_storage = PyArrowStorage(output_fs)
        else:
            self._output_storage = None
        super().__init__()

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

    def _execute(self, **kwargs):
        """
        Args:
            **kwargs: some additional variable passed from scheduling engine (e.g., Airflow)
        """
        assert set(self.sqls(**kwargs).keys()) == set(self.output_ids), 'sqls key should corresponds to the output_ids'
        # Extract Table and Load into RDB from FileSystem
        if self._input_storage is not None:
            [self._rdb.register(id, self._input_storage.download(id)) for id in self.input_ids]
        # Do transform using SQL on RDB
        for output_id, sql in self.sqls(**kwargs).items():
            self._rdb.execute(f'''
                  CREATE TABLE {output_id} AS ({sql});
            ''')
        # Load Table into FileSystem from RDB
        if self._output_storage is not None:
            for id in self.output_ids:
                table = self._rdb.execute(f'SELECT * FROM {id}').arrow()
                self._output_storage.upload(table, id)

class DFProcessor(ETL):
    """
    Basic Interface for defining a dataframe processing unit of ETL flow.
    """
    def __init__(self, input_storage: Storage, output_storage: Optional[Storage]=None, feedback_ids: List[str]=[]):
        self._input_storage = input_storage
        if output_storage is None:
            self._output_storage = input_storage
        else:
            self._output_storage = output_storage
        assert isinstance(self._input_storage, Storage), f'input_storage should be Storage rather than: {type(self._input_storage)}'
        assert isinstance(self._output_storage, Storage), f'output_storage should be Storage rather than: {type(self._output_storage)}'
        for id in feedback_ids:
            assert id in self.output_ids, f'Each of feedback_ids should be one of output_ids, but {id} is not'
        self._feedback_ids = feedback_ids
        super().__init__()

    @abc.abstractmethod
    def transform(self, inputs: List[object], **kwargs) -> List[object]:
        """
        Args:
            Inputs: List of object to be load from storage.
        Returns:
            List[object]: List of object to be saved to storage.
        """
        raise NotImplementedError


    def _execute(self, **kwargs):
        """
        Args:
            **kwargs: some additional variable passed from scheduling engine (e.g., Airflow)
        Run ETL (extract, transform, and load)
        """
        # Extraction Step 
        if len(self.input_ids):
            input_objs = self._extract_inputs()
        else:
            input_objs = []
        # Extract outputs and load into kwargs
        feedback_objs = self._extract_feedback()
        # Output Post-Validation
        kwargs.update(feedback_objs)
        # Transformation Step
        output_objs = self.transform(input_objs, **kwargs)
        # Output Validation
        assert isinstance(
            output_objs, list), 'Output of transform should be a list of object'
        assert all([isinstance(obj, self.get_output_type()) for obj in output_objs]
                ), f'One of the output_obj is not {self.get_output_type()}'
        # Load Step
        self._load(output_objs)

    def get_input_type(self):
        return self.transform.__annotations__['inputs'].__args__[0]

    def get_output_type(self):
        return self.transform.__annotations__['return'].__args__[0]

    def _extract_inputs(self) -> List[object]:
        """
        Returns:
            List[object]: List of dataframe object to be passed to `transform`.
        """
        input_tables = [self._input_storage.download(
            id) for id in self.input_ids]
        assert all([isinstance(obj, self.get_input_type()) for obj in input_tables]
                    ), f'One of the input_obj is not {self.get_input_type()}'
        return input_tables

    def _extract_feedback(self) -> Dict[str, object]:
        """extract table from output to feedback to transform method

        Returns:
            Dict[str, object]: contains dataframe object to be passed to `transform` in
            **kwargs
                - key: The feedback output id
                - value: The downloaded dataframe object
        """
        if len(self._feedback_ids):
            output_tables = [(id, self._output_storage.download(id)) for id in self._feedback_ids]
            assert all([isinstance(obj, self.get_input_type()) for id, obj in output_tables]
                        ), f'One of the input_obj is not {self.get_input_type()}'
            return dict(output_tables)
        else:
            return dict()

    def _load(self, output_tables: List[object]):
        """
        Args:
            output_tables: List[object]: List of dataframe object passed from `transform`.
        """
        for id, table in zip(self.output_ids, output_tables):
            self._output_storage.upload(table, id)
