"""
ETL Class
TODO:
- [X] Split SQL executor and Processor
- [X] Rename DFProcessor as Object Processor
- [ ] Add multi-threading to SQLExecutor
"""
from paradag import DAG
from paradag import dag_run
from paradag import MultiThreadProcessor, SequentialProcessor
from typing import List, Dict, Optional
from threading import Semaphore
from dill.source import getsource
import traceback
import abc
from .storage import Storage, PyArrowStorage, VaexStorage
from .filesystem import FileSystem
from .rdb import RDB

__all__ = [
    'ObjProcessor',
    'SQLExecutor',
    'ETLGroup'
]
class ETL:
    """
    Basic Interface for defining a unit of ETL flow.
    """
    def __init__(self):
        assert isinstance(self.input_ids, list), f'property input_ids is not a list of string but {type(self.input_ids)} on {self}'
        assert isinstance(self.output_ids, list), f'property output_ids is not a list of string but {type(self.output_ids)} on {self}'
        assert len(set(self.input_ids) & set(self.output_ids)) == 0, 'There should not be an object_id on both input_ids and output_ids'
        assert len(self.input_ids) == len(set(self.input_ids)), 'There should no be repeated id in self.input_ids'
        assert len(self.output_ids) == len(set(self.output_ids)), 'There should no be repeated id in self.output_ids'
        assert all([id in self.input_ids for id in self.external_input_ids]), 'external input ids should be defined in input ids'

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
        Returns:
            List[str]: a list of output object ids
        """
        raise NotImplementedError
    
    @abc.abstractproperty
    def external_input_ids(self) -> List[str]:
        """
        Returns:
            List[str]: a list of input object ids passed from external scope
        """
        return []
    
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
        try:
            # Step0: add external_ids to dag
            for id in self.external_input_ids:
                dag.add_vertex(id)
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
        except BaseException as e:
            raise ValueError(f'Dag Build Error on {self}') from e

    @abc.abstractmethod
    def drop_inputs(self):
        pass
    
    @abc.abstractmethod
    def drop_outputs(self):
        pass

    @abc.abstractmethod
    def drop_input(self, obj_id: str):
        pass
    
    @abc.abstractmethod
    def drop_output(self, obj_id: str):
        pass

class DagExecutor:
    """Executing Unit for Tasks in the Dag"""
    def __init__(self, limit_pool: Optional[Semaphore]=None):
        self._limit_pool = limit_pool

    def param(self, vertex):
        return vertex

    def execute(self, param):
        if self._limit_pool is not None:
            self._limit_pool.acquire()
        try:
            if isinstance(param, str):
                print(f'@Passing Object: {param}')
            elif isinstance(param, ETL):
                print('@Start:', type(param), 'inputs:', param.input_ids, 'outputs:', param.output_ids)
                param.execute()
                print('@End:', type(param), 'inputs:', param.input_ids, 'outputs:', param.output_ids)
            elif callable(param):
                print('@Start:', param, 'of', type(param))
                param()
                print('@End:', param, 'of', type(param))
            else:
                raise ValueError(f'param of DagExecutor should be str, ETL, or callable, but it is: {type(param)}')
        except Exception as e:
            traceback_str = traceback.format_exc()
            if isinstance(param, ETL):
                if isinstance(param, ObjProcessor):
                    content = getsource(param.transform) + f'\n{traceback_str}'
                elif isinstance(param, SQLExecutor):
                    content = getsource(param.sqls) + f'\n{traceback_str}'
                raise ValueError(f'something wrong on transform/sql of {param}: \n{content}') from e
            elif callable(param):
                content = getsource(param) + f'\n{traceback_str}'
                raise ValueError(f'something wrong on {param}: \n{content}') from e
            else:
                raise e 
        finally:
            if self._limit_pool is not None:
                self._limit_pool.release()


class ETLGroup(ETL):
    """Interface for connecting multiple ETL units
    """
    def __init__(self, *etl_units: List[ETL]):
        self.etl_units = etl_units
    
    def execute(self, **kwargs):
        self._execute(**kwargs)

    def _execute(self, **kwargs):
        """Execute ETL units
        """
        dag = DAG()
        self.build(dag)
        if 'sequential' in kwargs and kwargs['sequential']:
            dag_run(dag, processor=SequentialProcessor(), 
                    executor=DagExecutor()
                    )
        elif 'max_active_run' in kwargs:
            limit_pool = Semaphore(value=kwargs['max_active_run'])
            dag_run(dag, processor=MultiThreadProcessor(), 
                    executor=DagExecutor(limit_pool=limit_pool)
                    )
        else:
            dag_run(dag, processor=MultiThreadProcessor(), 
                    executor=DagExecutor()
            )

    def build(self, dag: DAG):
        # Step0: add external_ids to dag
        for id in self.external_input_ids:
            dag.add_vertex(id)
        # Step1: connecting dag with all etl units
        for etl_unit in self.etl_units:
            etl_unit.build(dag)
        # Step2: make sure all output ids are already in the dag
        for _id in self.output_ids:
            assert _id in dag.vertices(), f'output_id {_id} is not in dag input vertices'
        # Step3: Add start and end to dag
        dag.add_vertex(self.start)
        dag.add_vertex(self.end)
        # Step4: Connect end to all output_ids
        for id in self.input_ids:
            dag.add_edge(self.start, id)
        # Step5: connect execute to ouput_id
        for id in self.output_ids:
            dag.add_edge(id, self.end)

    @property
    def internal_inputs(self) -> Dict[str, ETL]:
        """
        Get internal inputs ids and its located ETL units
        """
        results = dict()
        for etl_unit in self.etl_units:
            for id in etl_unit.input_ids:
                results[id] = etl_unit
        for id in self.input_ids:
            del results[id]
        return results
    
    @property
    def internal_outputs(self) -> Dict[str, ETL]:
        """
        Get internal inputs ids and its located ETL units
        """
        results = dict()
        for etl_unit in self.etl_units:
            for id in etl_unit.output_ids:
                results[id] = etl_unit
        for id in self.output_ids:
            del results[id]
        return results
    
    def drop_internal_objs(self):
        for input_id, etl_unit in self.internal_inputs.items():
            try:
                etl_unit._drop_input(input_id)
            except:
                print(f'input_id: {input_id} drop skipped')


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

        assert all(['.' not in id for id in self.input_ids]), f'using . in SQLExecutor input id is not allowed. See: {self.input_ids}'
        assert all(['.' not in id for id in self.output_ids]), f'using . in SQLExecutor output id is not allowed. See: {self.output_ids}'
        for id in self.output_ids:
            assert id in self.sqls(), f'output_id {id} does not have corresponding sql'
        for key in self.sqls():
            assert key in self.output_ids, f'sql of field {key} does not have corresponding output_id'
        super().__init__()

    def drop_inputs(self):
        for id in self.input_ids:
            self._drop_input(id)

    def drop_outputs(self):
        for id in self.output_ids:
            self._drop_input(id)

    def _drop_input(self, obj_id: str):
        assert obj_id in self.input_ids, 'For drop_input on obj_id, it should be in input_ids'
        if self._input_storage is not None:
            self._input_storage.drop(obj_id)
        else:
            self._rdb.drop(id)

    def _drop_output(self, obj_id: str):
        assert obj_id in self.output_ids, 'For drop_output on obj_id, it should be in output_ids'
        if self._output_storage is not None:
            self._output_storage.drop(obj_id)
        else:
            self._rdb.drop(id)

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
        cursor = self._rdb.get_conn()
        try:
            if self._input_storage is not None:
                for id in self.input_ids:
                    if self._input_storage.check_exists(id):
                        print(f'@{self} Start Registering Input: {id}')
                        cursor.register(id, self._input_storage.download(id))
                        print(f'@{self} End Registering Input: {id}')
                    else:
                        raise ValueError(f'{id} does not exists')
            if self._output_storage is not None:
                for output_id, sql in self.sqls(**kwargs).items():
                    print(f'@{self} Start Uploading Output: {output_id}')
                    table = cursor.execute(f'SELECT * FROM ({sql})').arrow()
                    self._output_storage.upload(table, output_id)
                    print(f'@{self} End Uploading Output: {output_id}')
            else:
                for output_id, sql in self.sqls(**kwargs).items():
                    cursor.execute(f'''
                    CREATE TABLE {output_id} AS ({sql});
                    ''')

        finally:
            cursor.close()


class ObjProcessor(ETL):
    """
    Basic Interface for defining an object processing unit of ETL flow.
    """
    def __init__(self, input_storage: Storage, output_storage: Optional[Storage]=None, feedback_ids: List[str]=[]):
        self._input_storage = input_storage
        if output_storage is None:
            self._output_storage = input_storage
        else:
            self._output_storage = output_storage
        assert isinstance(self._input_storage, Storage), f'input_storage should be Storage rather than: {type(self._input_storage)}'
        assert isinstance(self._output_storage, Storage), f'output_storage should be Storage rather than: {type(self._output_storage)}'
        assert self.get_input_type() == self._input_storage.get_download_type(), f'storage download type: {self._input_storage.get_download_type()} != transform input type: {self.get_input_type()}'
        assert self.get_output_type() == self._output_storage.get_upload_type(), f'storage upload type: {self._output_storage.get_upload_type()} != transform output type: {self.get_output_type()}'
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
            assert all([isinstance(obj, self.get_input_type()) for obj in input_objs]
                    ), f'One of the input_obj {input_objs} is not {self.get_input_type()}'
        else:
            input_objs = []
        # Transformation Step
        output_objs = self.transform(input_objs, **kwargs)
        # Output Validation
        assert isinstance(
            output_objs, list), 'Output of transform should be a list of object'
        assert all([isinstance(obj, self.get_output_type()) for obj in output_objs]
                ), f'One of the output_obj {output_objs} is not {self.get_output_type()}'
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
        input_tables = []
        for id in self.input_ids:
            print(f'@{self} Start Extracting Input: {id}')
            table = self._input_storage.download(id)
            input_tables.append(table)
            print(f'@{self} End Extracting Input: {id}')
        return input_tables

    def _load(self, output_tables: List[object]):
        """
        Args:
            output_tables: List[object]: List of dataframe object passed from `transform`.
        """
        for id, table in zip(self.output_ids, output_tables):
            print(f'@{self} Start Loading Output: {id}')
            self._output_storage.upload(table, id)
            print(f'@{self} End Loading Output: {id}')

    def drop_inputs(self):
        for id in self.input_ids:
            self._input_storage.drop(id)
        
    def drop_outputs(self):
        for id in self.output_ids:
            self._output_storage.drop(id)

    def _drop_input(self, obj_id: str):
        assert obj_id in self.input_ids, 'For drop_input on obj_id, it should be in input_ids'
        self._input_storage.drop(obj_id)

    def _drop_output(self, obj_id: str):
        assert obj_id in self.output_ids, 'For drop_output on obj_id, it should be in output_ids'
        self._output_storage.drop(obj_id)