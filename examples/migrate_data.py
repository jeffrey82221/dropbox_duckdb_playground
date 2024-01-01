from batch_framework.filesystem import LocalBackend
from batch_framework.filesystem import DropboxBackend


local_fs = LocalBackend('./data/model/')
dropbox_fs = DropboxBackend('/data/model/')

if __name__ == '__main__':
    for model_file in ['license.model', 'requirement.model',
                       'requirement2package.model']:
        buff = local_fs.download_core(model_file)
        buff.seek(0)
        dropbox_fs.upload_core(buff, model_file)
        print('model:', model_file, 'uploaded')
