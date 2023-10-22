import os
import sys
import dropbox
from dropbox.files import WriteMode
from dropbox.exceptions import ApiError
import io
class Storage:
    """
    Storage object with IO interface implemented
    """
    def __init__(self):
        token = os.getenv('DROPBOX_TOKEN')
        self._dbx = dropbox.Dropbox(token)

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

    def _select_revision(self, remote_path: str) -> str:
        # Get the revisions for a file (and sort by the datetime object, "server_modified")
        print("Finding available revisions on Dropbox...")
        entries = self._dbx.files_list_revisions(remote_path, limit=30).entries
        revisions = sorted(entries, key=lambda entry: entry.server_modified)
        # Return the newest revision (last entry, because revisions was sorted oldest:newest)
        return revisions[-1].rev