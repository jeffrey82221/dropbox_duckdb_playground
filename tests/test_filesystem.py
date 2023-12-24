import pytest
import pandas as pd
import io
from batch_framework.filesystem import DropboxBackend, LocalBackend
from dropbox.exceptions import ApiError


@pytest.fixture
def dropbox():
    return DropboxBackend()


@pytest.fixture
def local():
    return LocalBackend('./data/')


def test_upload_download_core(dropbox, local):
    for backend in [dropbox, local]:
        data = io.BytesIO()
        in_table = pd.DataFrame([1, 2, 3])
        in_table.to_parquet(data)
        backend.upload_core(data, 'my-file.parquet')
        download = backend.download_core('my-file.parquet')
        out_table = pd.read_parquet(download, engine='pyarrow')
        pd.testing.assert_frame_equal(in_table, out_table)


def test_dropbox_file_not_exists(dropbox):
    with pytest.raises(ApiError):
        dropbox.download_core('something-does-not-exist')
    assert not dropbox.check_exists('something-does-not-exist')
    dropbox.upload_core(io.BytesIO(), '123')
    assert dropbox.check_exists('123')
