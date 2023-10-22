import pandas as pd
import os
from src.storage import Storage
def test_upload_file():
    storage = Storage()
    with open('my-file.txt', 'wb') as f:
        f.write(b'test data')        
    storage.upload_file('my-file.txt', 'my-file.txt')
    os.remove('my-file.txt')