import abc
import pandas as pd
import vaex as vx
import pyarrow as pa
import pyarrow.parquet as pq
import io
from typing import Dict, List, Union
import json
from .backend import Backend
from .filesystem import FileSystem
from .filesystem import LocalBackend
from .rdb import RDB


class Storage:
    """
    A python object storage with various backend assigned.
    """

    def __init__(self, backend: Backend):
        """
        Args:
            backend (Backend): A remote storage backend
            to be use for the python object storage.
        """
        self._backend = backend

    @abc.abstractmethod
    def upload(self, obj: object, obj_id: str):
        """Upload of obj as a file at remote path

        Args:
            obj (object): object to be upload
            obj_id (str): path to upload
        """
        raise NotImplementedError

    @abc.abstractmethod
    def download(self, obj_id: str) -> object:
        """Download obj from remote path
        Args:
            obj_id (str): The path to be download from.
        Returns:
            object: The downloaded file.
        """
        raise NotImplementedError

class JsonStorage(Storage):
    """
    Storage of JSON Python Diction 
    """
    def __init__(self, filesystem: FileSystem):
        assert isinstance(filesystem, FileSystem), 'Json Storage should have FileSystem backend'
        super().__init__(backend=filesystem)

    def upload(self, json_obj: Union[Dict, List], obj_id: str):
        """Upload a python object as json on a filesystem

        Args:
            json (Union[Dict, List]): A json parsable python object
            obj_id (str): The id of the object
        """
        buff = io.BytesIO(json.dumps(json_obj).encode())
        self._backend.upload_core(buff, obj_id)
    
    def download(self, obj_id: str) -> Union[Dict, List]:
        buff = self._backend.download_core(obj_id)
        buff.seek(0)
        return json.loads(buff.read().decode())

class PandasStorage(Storage):
    """
    Storage of Pandas DataFrame
    """

    def __init__(self, backend: Backend):
        super().__init__(backend=backend)

    def upload(self, pandas: pd.DataFrame, obj_id: str):
        if isinstance(self._backend, FileSystem):
            buff = io.BytesIO()
            pandas.to_parquet(buff)
            self._backend.upload_core(buff, obj_id)
        elif isinstance(self._backend, RDB):
            assert all([isinstance(col, str) for col in pandas.columns]
                       ), 'all columns should be string for RDB backend'
            self._backend.register(obj_id, pandas)
        else:
            raise TypeError('backend should be RDB or FileSystem')

    def download(self, obj_id: str) -> pd.DataFrame:
        if isinstance(self._backend, FileSystem):
            buff = self._backend.download_core(obj_id)
            result = pd.read_parquet(buff, engine='pyarrow')
            return result
        elif isinstance(self._backend, RDB):
            return self._backend.execute(f"SELECT * FROM {obj_id}").df()
        else:
            raise TypeError('backend should be RDB or FileSystem')

class PyArrowStorage(Storage):
    """Storage of pyarrow Table
    """
    def __init__(self, backend: Backend):
        super().__init__(backend=backend)

    def upload(self, pyarrow: pa.Table, obj_id: str):
        if isinstance(self._backend, LocalBackend):
            buff = io.BytesIO()
            pq.write_table(pyarrow, buff)
            self._backend.upload_core(buff, obj_id)
        elif isinstance(self._backend, RDB):
            self._backend.register(obj_id, pyarrow)
        else:
            raise TypeError('backend should be RDB or FileSystem')

    def download(self, obj_id: str) -> pa.Table:
        if isinstance(self._backend, FileSystem):
            buff = self._backend.download_core(obj_id)
            return pq.read_table(buff)
        elif isinstance(self._backend, RDB):
            return self._backend.execute(
                f"SELECT * FROM {obj_id}").arrow()
        else:
            raise TypeError('backend should be RDB or FileSystem')


class VaexStorage(Storage):
    """Storage of Polars DataFrame
    """

    def __init__(self, backend: Backend):
        super().__init__(backend=backend)

    def upload(self, vaex: vx.DataFrame, obj_id: str):
        if isinstance(self._backend, LocalBackend):
            buff = io.BytesIO()
            vaex.export_parquet(buff)
            self._backend.upload_core(buff, obj_id)
        elif isinstance(self._backend, RDB):
            assert all([isinstance(col, str) for col in vaex.columns]
                       ), 'all columns should be string for RDB backend'
            self._backend.register(obj_id, vaex.to_arrow_table())
        else:
            raise TypeError('backend should be RDB or LocalBackend')

    def download(self, obj_id: str) -> vx.DataFrame:
        if isinstance(self._backend, LocalBackend):
            result = vx.open(f'{self._backend._directory}{obj_id}')
            return result
        elif isinstance(self._backend, RDB):
            return vx.from_arrow_table(self._backend.execute(
                f"SELECT * FROM {obj_id}").arrow())
        else:
            raise TypeError('backend should be RDB or LocalBackend')
