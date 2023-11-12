from typing import Optional
from batch_framework.filesystem import LocalBackend
from batch_framework.storage import PandasStorage, VaexStorage
from batch_framework.etl import ETLGroup
import os
from parallize import MapReduce
from .trigger import PyPiNameTrigger
from .crawl import (
    LatestFeedback,
    NewPackageExtractor,
    LatestDownloader,
    LatestUpdator,
    Combine
)
from .tabularize import LatestTabularize

class SimplePyPiCanonicalize(ETLGroup):
    def __init__(self, raw_df: LocalBackend, tmp_fs: LocalBackend, output_fs: LocalBackend, partition_fs: LocalBackend, parallel_count: int, test_count: Optional[int]=None):
        self._tmp_fs = tmp_fs
        tmp_s = PandasStorage(tmp_fs)
        units = [
            # Crawl Start
            LatestFeedback(input_storage=VaexStorage(raw_df), output_storage=VaexStorage(tmp_fs)),
            PyPiNameTrigger(tmp_s),
            NewPackageExtractor(VaexStorage(tmp_fs), test_count=test_count),
            MapReduce(LatestDownloader(tmp_s), parallel_count, PandasStorage(partition_fs)),
            MapReduce(LatestUpdator(tmp_s), parallel_count, PandasStorage(partition_fs)),
            Combine(input_storage=tmp_s, output_storage=PandasStorage(raw_df)),
            # Crawl End
            LatestTabularize(
                input_storage=PandasStorage(raw_df), 
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


class PyPiCanonicalize(ETLGroup):
    """Make Data Crawled from PyPi Tabularized
    """
    def __init__(self, tmp_fs: LocalBackend, output_fs: LocalBackend, parallel_cnt: Optional[int] = 50, test_count: Optional[int]=None):
        self._tmp_fs = tmp_fs
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

    def end(self):
        os.remove(f'{self._input_storage._directory}name_trigger.*')
        os.remove(f'{self._input_storage._directory}latest.*')
        print('Drop partition files')

if __name__ == '__main__':
    tmp_fs = LocalBackend('./data/canon/tmp/')
    output_fs = LocalBackend('./data/canon/output/')
    pypi_canonicalization = PyPiCanonicalize(tmp_fs, output_fs)
    pypi_canonicalization.execute()
