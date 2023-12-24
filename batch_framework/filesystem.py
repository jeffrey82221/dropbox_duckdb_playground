"""
TODO:
- [X] Add check exists on DropBox Backend
"""
import abc
import os
import sys
import io
import dropbox
from dropbox.files import WriteMode
from dropbox.exceptions import ApiError, AuthError
from .backend import Backend


class FileSystem(Backend):
    """
    FileSystem Backend for storing python objects.

    Example: DropBoxStorage
    """

    def __init__(self, directory: str = '/'):
        assert directory.endswith('/')
        super().__init__(directory)

    @abc.abstractmethod
    def upload_core(self, file_obj: io.BytesIO, remote_path: str):
        """Upload file object to a remote storage

        Args:
            file_obj (io.BytesIO): file to be upload
            remote_path (str): remote file path
        """
        raise NotImplementedError

    @abc.abstractmethod
    def download_core(self, remote_path: str) -> io.BytesIO:
        """Download file from remote storage

        Args:
            remote_path (str): remote file path

        Returns:
            io.BytesIO: downloaded file
        """
        raise NotImplementedError

    @abc.abstractmethod
    def check_exists(self, remote_path: str) -> bool:
        """Check whether a remote file exists or not.
        Args:
            remote_path (str): remote file path

        Returns:
            bool: exists or not
        """
        raise NotImplementedError

    @abc.abstractmethod
    def drop_file(self, remote_path: str):
        """
        Delete a remote file
        Args:
            remote_path (str): remote file path
        """
        raise NotImplementedError


class DropboxBackend(FileSystem):
    """
    Storage object with IO interface left abstract
    """

    def __init__(self, directory='/'):
        self._dbx = dropbox.Dropbox(
            app_key=os.getenv('DROPBOX_APP_KEY'),
            app_secret=os.getenv('DROPBOX_APP_SECRET'),
            oauth2_refresh_token=os.getenv('DROPBOX_TOKEN'))
        super().__init__(directory)

    def upload_core(self, file_obj: io.BytesIO, remote_path: str):
        try:
            file_obj.seek(0)
            self._dbx.files_upload(
                file_obj.read(),
                self._directory + remote_path,
                mode=WriteMode('overwrite'))
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
        except AuthError as err:
            raise err

    def download_core(self, remote_path: str) -> io.BytesIO:
        try:
            buff = io.BytesIO(self._dbx.files_download(
                self._directory + remote_path)[-1].content)
            buff.seek(0)
            return buff
        except ApiError as err:
            raise err
        except AuthError as err:
            raise err

    def check_exists(self, remote_path: str) -> bool:
        """Check whether a remote file exists or not.
        Args:
            remote_path (str): remote file path

        Returns:
            bool: exists or not
        """
        try:
            self._dbx.files_get_metadata(self._directory + remote_path)
            return True
        except ApiError:
            return False


class LocalBackend(FileSystem):
    def __init__(self, directory='./'):
        self._directory = directory
        assert self._directory.endswith(
            '/'), 'Please make sure directory endswith /'

    def upload_core(self, file_obj: io.BytesIO, remote_path: str):
        """Upload file object to local storage

        Args:
            file_obj (io.BytesIO): file to be upload
            remote_path (str): remote file path
        """
        file_obj.seek(0)
        with open(self._directory + remote_path, 'wb') as f:
            f.write(file_obj.getbuffer())

    def download_core(self, remote_path: str) -> io.BytesIO:
        """Download file from remote storage

        Args:
            remote_path (str): remote file path

        Returns:
            io.BytesIO: downloaded file
        """
        with open(self._directory + remote_path, 'rb') as f:
            result = io.BytesIO(f.read())
        return result

    def check_exists(self, remote_path: str) -> bool:
        return os.path.exists(self._directory + remote_path)

    def drop_file(self, remote_path: str):
        return os.remove(self._directory + remote_path)
