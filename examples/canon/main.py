from batch_framework.filesystem import LocalBackend
from batch_framework.storage import PandasStorage
from batch_framework.etl import ETLGroup
from .trigger import PyPiNameTrigger
from .crawl import LatestCrawler
from .tabularize import LatestTabularize
from .map_reduce import PandasDivide, PandasMerge

class PyPiCanonicalize(ETLGroup):
    """Make Data Crawled from PyPi Tabularized
    """
    def __init__(self, tmp_fs: LocalBackend, output_fs: LocalBackend, parallel_cnt = 50, test_count=None):
        units = [
            PyPiNameTrigger(PandasStorage(tmp_fs)),
            PandasDivide('name_trigger', parallel_cnt, PandasStorage(tmp_fs))
        ] + [
            LatestCrawler(PandasStorage(tmp_fs), test_count=test_count, partition_id=i) 
            for i in range(parallel_cnt)
        ] + [
            PandasMerge('latest', parallel_cnt, PandasStorage(tmp_fs)),
            LatestTabularize(
                input_storage=PandasStorage(tmp_fs), 
                output_storage=PandasStorage(output_fs)
            )
        ]
        super().__init__(*units)

    @property
    def input_ids(self):
        return []
    
    @property
    def output_ids(self):
        return ['latest_package', 'latest_requirement', 'latest_url']


if __name__ == '__main__':
    tmp_fs = LocalBackend('./data/canon/tmp/')
    output_fs = LocalBackend('./data/canon/output/')
    pypi_canonicalization = PyPiCanonicalize(tmp_fs, output_fs)
    pypi_canonicalization.execute()
