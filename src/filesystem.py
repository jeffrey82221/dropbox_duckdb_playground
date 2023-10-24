import abc
import os
import sys
import io
import dropbox
from dropbox.files import WriteMode
from dropbox.exceptions import ApiError
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


class DropboxBackend(FileSystem):
    """
    Storage object with IO interface left abstract
    """

    def __init__(self, directory='/'):
        token = os.getenv('DROPBOX_TOKEN')
        self._dbx = dropbox.Dropbox(token)
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

    def download_core(self, remote_path: str) -> io.BytesIO:
        try:
            buff = io.BytesIO(self._dbx.files_download(
                self._directory + remote_path)[-1].content)
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
        # Get the revisions for a file (and sort by the datetime object,
        # "server_modified")
        print("Finding available revisions on Dropbox...")
        entries = self._dbx.files_list_revisions(
            self._directory + remote_path, limit=30).entries
        revisions = sorted(entries, key=lambda entry: entry.server_modified)
        # Return the newest revision (last entry, because revisions was sorted
        # oldest:newest)


class LocalBackend(FileSystem):
    def __init__(self, directory='./'):
        self._directory = directory

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
