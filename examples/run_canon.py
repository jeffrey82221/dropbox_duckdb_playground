from canon import SimplePyPiCanonicalize
from batch_framework.filesystem import LocalBackend
pypi_table_loader = SimplePyPiCanonicalize(
    tmp_fs=LocalBackend('./data/canon/tmp/'),
    output_fs=LocalBackend('./data/canon/output/'),
    test_count=10
)
if __name__ == '__main__':
    pypi_table_loader.execute()
