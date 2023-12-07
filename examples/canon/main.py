"""
TODO: 
- [X] Add name, etag selection temp for less memory usage on latest update
- [ ] Build Polars Storage and Polars support on ObjProcessor for zero copy combine
"""
from typing import Optional
from batch_framework.filesystem import LocalBackend
from batch_framework.storage import PandasStorage, VaexStorage
from batch_framework.etl import ETLGroup
from batch_framework.parallize import MapReduce
from .trigger import PyPiNameTrigger
from .crawl import (
    LatestFeedback,
    NewPackageExtractor,
    LatestDownloader,
    LatestUpdatorInputReduce,
    LatestUpdator,
    Combine,
    Pass
)
from .tabularize import LatestTabularize

class SimplePyPiCanonicalize(ETLGroup):
    def __init__(self, raw_df: LocalBackend, 
                 tmp_fs: LocalBackend, 
                 output_fs: LocalBackend, 
                 partition_fs: LocalBackend, 
                 download_worker_count: int,
                 upload_worker_count: int, 
                 test_count: Optional[int]=None, 
                 do_update: bool=True):
        self._tmp_fs = tmp_fs
        units = [
            # Crawl Start
            LatestFeedback(input_storage=VaexStorage(raw_df), output_storage=VaexStorage(tmp_fs)),
            PyPiNameTrigger(PandasStorage(tmp_fs)),
            NewPackageExtractor(VaexStorage(tmp_fs), test_count=test_count),
            MapReduce(
                LatestDownloader(PandasStorage(tmp_fs)), 
                    download_worker_count, 
                    partition_fs
            )
        ]
        if do_update:
            units.extend([
                LatestUpdatorInputReduce(VaexStorage(tmp_fs)),
                MapReduce(LatestUpdator(PandasStorage(tmp_fs)), 
                          upload_worker_count, 
                          partition_fs
                ),
                Combine(input_storage=PandasStorage(tmp_fs), output_storage=PandasStorage(raw_df))
            ])
        else:
            units.append(
                Pass(input_storage=VaexStorage(tmp_fs), output_storage=VaexStorage(raw_df))
            )
        units.append(
            LatestTabularize(
                input_storage=PandasStorage(raw_df), 
                output_storage=PandasStorage(output_fs)
            )
        )
        super().__init__(*units)
    
    @property
    def input_ids(self):
        return []
    
    @property
    def output_ids(self):
        return ['latest_package', 'latest_requirement', 'latest_url']