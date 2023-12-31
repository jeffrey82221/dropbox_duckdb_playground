from canon import SimplePyPiCanonicalize
from batch_framework.filesystem import LocalBackend, DropboxBackend
pypi_table_loader = SimplePyPiCanonicalize(
    raw_df=DropboxBackend('/data/canon/raw/'),
    tmp_fs=DropboxBackend('/data/canon/tmp/'),
    output_fs=DropboxBackend('/data/canon/output/'),
    partition_fs=DropboxBackend('/data/canon/partition/'),
    download_worker_count=800,  # 800
    update_worker_count=16,
    # test_count=1000,
    do_update=False
)
if __name__ == '__main__':
    pypi_table_loader.execute(max_active_run=16)
    # pypi_table_loader.updator.execute()
