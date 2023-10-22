import pandas as pd
import os
import io
from src.storage import Storage

def test_upload_download_core():
    storage = Storage()
    data = io.BytesIO()
    in_table = pd.DataFrame([1,2,3])
    in_table.to_parquet(data)
    storage.upload_core(data, 'my-file.parquet')
    download = storage.download_core('/my-file.parquet')
    out_table = pd.read_parquet(download, engine='pyarrow')
    pd.testing.assert_frame_equal(in_table, out_table)