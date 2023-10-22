"""
Class Operator

1) Define inputs, outputs, and a `transform` method for defining the process method. 
2) A execute method to run the ETL (extract, transform, load).
3) Interface connecting `extract` and `load` method with `Storage` class.
4) Variable defining the interaction between `extract` and `transform` and `transform` and `load`. 
"""
from typing import List
import abc
from .storage import Storage

class Operator:
    """
    Basic Interface for defining a unit of ETL flow.
    """
    def __init__(self, input_storage: Storage, output_storage: Storage):
        self._input_storage = input_storage
        self._output_storage = output_storage

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
        assert isinstance(input_objs, list), 'Input of transform should be a list of object'
        assert all([isinstance(obj, self.get_input_type()) for obj in input_objs]), f'One of the input_obj is not {self.get_input_type()}'
        output_objs = self.transform(input_objs, **kwargs)
        assert isinstance(output_objs, list), 'Output of transform should be a list of object'
        assert all([isinstance(obj, self.get_output_type()) for obj in output_objs]), f'One of the output_obj is not {self.get_output_type()}'
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
        input_tables = [self._input_storage.download(id) for id in self.input_ids]
        return input_tables

    def _load(self, output_tables: List[object], **kwargs):
        """
        Args:
            output_tables: List[object]: List of dataframe object passed from `transform`.
            **kwargs: some additional variable passed from scheduling engine (e.g., Airflow)
        """
        for id, table in zip(self.output_ids, output_tables):
            self._output_storage.upload(table, id)

