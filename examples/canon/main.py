"""
TODO:
- [X] Add name, etag selection temp for less memory usage on latest update
- [ ] Build Polars Storage and Polars support on ObjProcessor for zero copy combine
    - [ ] Refactor: 
        - [ ] `NewPackageExtractor` (output cache) -> `GetNew` (no cache) -> `Update` (output cache)
            - [X] `NewPackageExtractor` get a list of package names
                - [X] It select new packages 
                - [X] First time, it takes no previous data, so all input becomes new data
                - [X] Second time and later, it takes previous cache and filter output 
                    the old data to get new data. 
            - [X] `LatestDownloader` using map reduce to download package json. 
                It produce new data with download json
            - [ ] `Update` takes output of GetNew as input and PyPiNameTrigger as input
                - [ ] Step 1: Load output cache and update the Json.
                - [ ] Step 2: append new json data to the updated cache. 
                - [ ] Step 3: Save output. 
"""
from typing import Optional, List
import vaex as vx
import pandas as pd
from batch_framework.filesystem import LocalBackend
from batch_framework.storage import PandasStorage, VaexStorage
from batch_framework.etl import ETLGroup, ObjProcessor
from batch_framework.parallize import MapReduce
from .trigger import PyPiNameTrigger
from .crawl import (
    LatestDownloader,
    LatestUpdator
)
from .tabularize import LatestTabularize


class NewPackageExtractor(ObjProcessor):
    def __init__(self, *args, **kwargs):
        kwargs['make_cache'] = True
        super().__init__(*args, **kwargs)

    @property
    def input_ids(self):
        return ['name_trigger']

    @property
    def output_ids(self):
        return ['name_trigger_new']

    def transform(self, inputs: List[vx.DataFrame],
                  **kwargs) -> List[vx.DataFrame]:
        pkg_name_df = inputs[0]
        print('Size of pkg_name:', len(pkg_name_df))
        if self.exists_cache:
            pkg_name_cache_df = self.load_cache(self.input_ids[0])
            print('Size of cached pkg_name:', len(pkg_name_cache_df))
            new_pkg_names = self._get_new_package_names(pkg_name_df, pkg_name_cache_df)
            assert len(new_pkg_names) > 0, 'Should download new package'
            print('number of new packages:', len(new_pkg_names))
            return [vx.from_pandas(pd.DataFrame(new_pkg_names, columns=['name']))]
        else:
            return inputs

    def _get_new_package_names(
            self, pkg_name_df: vx.DataFrame, cache_pkg_name_df: vx.DataFrame) -> List[str]:
        pkg_names = pkg_name_df[['name']].to_arrays(array_type='list')[0]
        cache_pkg_names = cache_pkg_name_df[['name']].to_arrays(array_type='list')[0]
        new_names = list(set(pkg_names) - set(cache_pkg_names))
        return new_names
    
class SimplePyPiCanonicalize(ETLGroup):
    def __init__(self, raw_df: LocalBackend,
                 tmp_fs: LocalBackend,
                 output_fs: LocalBackend,
                 partition_fs: LocalBackend,
                 download_worker_count: int,
                 test_count: Optional[int] = None,
                 do_update: bool = True):
        self._tmp_fs = tmp_fs
        units = [
            PyPiNameTrigger(PandasStorage(tmp_fs), test_count=test_count),
            NewPackageExtractor(VaexStorage(tmp_fs)),
            MapReduce(
                LatestDownloader(PandasStorage(tmp_fs)),
                download_worker_count,
                partition_fs
            )
        ]
        units.extend([
            LatestUpdator(VaexStorage(tmp_fs), VaexStorage(raw_df), do_update=do_update)
        ])
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
