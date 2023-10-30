from canon.trigger import PyPiNameTrigger
from canon.crawl import LatestCrawler
from canon.tabularize import LatestTabularize
from batch_framework.filesystem import LocalBackend
from batch_framework.storage import PandasStorage
tmp_fs = PandasStorage(LocalBackend('./data/canon/tmp/'))
output_fs = PandasStorage(LocalBackend('./data/canon/output/'))
op1 = PyPiNameTrigger(tmp_fs)
op2 = LatestCrawler(tmp_fs, test_count=10000)
op3 = LatestTabularize(input_storage=tmp_fs, output_storage=output_fs)

if __name__ == '__main__':
    op1.execute()
    op2.execute()
    op3.execute()
