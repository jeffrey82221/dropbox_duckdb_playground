import abc
import os
import sys
import dropbox
from dropbox.files import WriteMode
from dropbox.exceptions import ApiError
import io
import pandas as pd
class Backend:
    """
    Backend for storing python objects. 

    Example: DropBoxStorage
    """
    @abc.abstractmethod
    def _upload_core(self, file_obj: io.BytesIO, remote_path: str):
        """Upload file object to a remote storage

        Args:
            file_obj (io.BytesIO): file to be upload 
            remote_path (str): remote file path 
        """
        raise NotImplementedError

    @abc.abstractmethod
    def _download_core(self, remote_path: str) -> io.BytesIO:
        """Download file from remote storage

        Args:
            remote_path (str): remote file path

        Returns:
            io.BytesIO: downloaded file
        """
        raise NotImplementedError
    
class DropboxBackend(Backend):
    """
    Storage object with IO interface left abstract
    """
    def __init__(self):
        token = os.getenv('DROPBOX_TOKEN')
        self._dbx = dropbox.Dropbox(token)

    def upload_core(self, file_obj: io.BytesIO, remote_path: str):
        try:
            if not remote_path.startswith('/'):
                remote_path = '/' + remote_path
            file_obj.seek(0)
            self._dbx.files_upload(file_obj.read(), remote_path, mode=WriteMode('overwrite'))
        except ApiError as err:
            # This checks for the specific error where a user doesn't have
            # enough Dropbox space quota to upload this file
            if (err.error.is_path() and
                    err.error.get_path().reason.is_insufficient_space()):
                sys.exit("ERROR: Cannot back up; insufficient space.")
            elif err.user_message_text:
                print(err.user_message_text)
                sys.exit()
            else:
                print(err)
                sys.exit()

    def download_core(self, remote_path: str) -> io.BytesIO:
        try:
            if not remote_path.startswith('/'):
                remote_path = '/' + remote_path
            buff = io.BytesIO(self._dbx.files_download(remote_path)[-1].content)
            buff.seek(0)
            return buff
        except ApiError as err:
            # This checks for the specific error where a user doesn't have
            # enough Dropbox space quota to upload this file
            if (err.error.is_path() and
                    err.error.get_path().reason.is_insufficient_space()):
                sys.exit("ERROR: Cannot back up; insufficient space.")
            elif err.user_message_text:
                print(err.user_message_text)
                sys.exit()
            else:
                print(err)
                sys.exit()

    def _select_revision(self, remote_path: str) -> str:
        # Get the revisions for a file (and sort by the datetime object, "server_modified")
        print("Finding available revisions on Dropbox...")
        entries = self._dbx.files_list_revisions(remote_path, limit=30).entries
        revisions = sorted(entries, key=lambda entry: entry.server_modified)
        # Return the newest revision (last entry, because revisions was sorted oldest:newest)
       
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