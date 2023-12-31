from canon import SimplePyPiCanonicalize
from batch_framework.filesystem import LocalBackend
pypi_table_loader = SimplePyPiCanonicalize(
    raw_df=LocalBackend('./data/canon/raw/'),
    tmp_fs=LocalBackend('./data/canon/tmp/'),
    output_fs=LocalBackend('./data/canon/output/'),
    partition_fs=LocalBackend('./data/canon/partition/'),
    download_worker_count=1,  # 800
    update_worker_count=16,
    do_update=True
)
if __name__ == '__main__':
    # pypi_table_loader.execute(max_active_run=10)
    pypi_table_loader.updator.execute()
