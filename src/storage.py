import os
import sys
import dropbox
from dropbox.files import WriteMode
from dropbox.exceptions import ApiError

class Storage:
    """
    Storage object with IO interface implemented
    """
    def __init__(self):
        token = os.getenv('DROPBOX_TOKEN')
        self._dbx = dropbox.Dropbox(token)

    def upload_file(self, local_path, remote_path):
        try:
            if not remote_path.startswith('/'):
                remote_path = '/' + remote_path
            with open(local_path, 'rb') as f:
                self._dbx.files_upload(f.read(), remote_path, mode=WriteMode('overwrite'))
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