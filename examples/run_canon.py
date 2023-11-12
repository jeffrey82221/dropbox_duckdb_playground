from canon import SimplePyPiCanonicalize
from batch_framework.filesystem import LocalBackend
pypi_table_loader = SimplePyPiCanonicalize(
    raw_df=LocalBackend('./data/canon/raw/'),
    tmp_fs=LocalBackend('./data/canon/tmp/'),
    output_fs=LocalBackend('./data/canon/output/'),
    partition_fs=LocalBackend('./data/canon/partition/'),
    parallel_count=100,
    test_count=1000
)
if __name__ == '__main__':
    pypi_table_loader.execute()
