import abc
import pandas as pd
import io
from .backend import Backend


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
    def upload(self, obj: object, remote_path: str):
        """Upload of obj as a file at remote path 

        Args:
            obj (object): object to be upload
            remote_path (str): path to upload
        """
        raise NotImplementedError

    @abc.abstractmethod
    def download(self, remote_path: str) -> object:
        """Download obj from remote path
        Args:
            remote_path (str): The path to be download from.
        Returns:
            object: The downloaded file. 
        """
        raise NotImplementedError

class PandasStorage(Storage):
    """
    Storage object with IO interface implemented
    """
    def __init__(self, backend):
        super().__init__(backend=backend)

    def upload(self, pandas: pd.DataFrame, remote_path: str):
        buff = io.BytesIO()
        pandas.to_parquet(buff)
        self._backend.upload_core(buff, remote_path)

    def download(self, remote_path: str) -> pd.DataFrame:
        buff = self._backend.download_core(remote_path)
        result = pd.read_parquet(buff, engine='pyarrow')
        return result