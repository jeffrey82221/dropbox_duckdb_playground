from canon import PyPiCanonicalize
from batch_framework.filesystem import LocalBackend
pypi_table_loader = PyPiCanonicalize(
    tmp_fs=LocalBackend('./data/canon/tmp/'),
    output_fs=LocalBackend('./data/canon/output/'),
    parallel_cnt=100,
    test_count=100
)
if __name__ == '__main__':
    pypi_table_loader.execute()
