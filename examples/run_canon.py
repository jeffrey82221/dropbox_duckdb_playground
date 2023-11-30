from canon import SimplePyPiCanonicalize
from batch_framework.filesystem import LocalBackend
pypi_table_loader = SimplePyPiCanonicalize(
    raw_df=LocalBackend('./data/canon/raw/'),
    tmp_fs=LocalBackend('./data/canon/tmp/'),
    output_fs=LocalBackend('./data/canon/output/'),
    partition_fs=LocalBackend('./data/canon/partition/'),
    parallel_count=10,
    test_count=100
)
if __name__ == '__main__':
    # Total Package Count: 496743
    # for i in range(50):
    pypi_table_loader.execute()
