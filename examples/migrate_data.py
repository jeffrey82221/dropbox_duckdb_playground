from batch_framework.filesystem import LocalBackend
from batch_framework.filesystem import DropboxBackend

import os
local_fs = LocalBackend('./data/canon/raw/')
dropbox_fs = DropboxBackend('/data/canon/raw/')
for file in os.listdir('./data/canon/raw/'):
    buff = local_fs.download_core(file)
    buff.seek(0)
    dropbox_fs.upload_core(buff, file)
    print('file:', file, 'uploaded')