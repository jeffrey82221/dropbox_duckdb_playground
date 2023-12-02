
"""
Download Latest Json for all package on PyPi
TODO:
- [X] Try to devide and conquer the download for speed up the process. 
    - [X] Add `name_trigger` partitioning task between `trigger` and `crawl`
    - [X] Add partition_id selection to `crawl`
    - [X] Add `latest` merge task between crawl and tabularize 
- [X] Decompose Crawl Class  
    - [X] Get new package names 
    - [X] Download new package records -> Decorate with MapReduce 
    - [X] Update package records -> Decorate with MapReduce
    - [X] Combine package records 
- [ ] Reduce RAM usage:
    - [-] LatestFeedback -> Use streaming for copying 
    - [-] Load Two kinds of feedback: 1) with latest json 2) without latest json
    - [ ] `Combine` use DuckDB to leverage zero-copy capability
    - [ ] `Combine` use polaris to leverage zero-copy with drop_duplicate functionality
"""
from typing import List, Dict, Tuple, Optional
import requests
import pandas as pd
import vaex as vx
import tqdm
from batch_framework.etl import ObjProcessor
from batch_framework.storage import PandasStorage

class LatestFeedback(ObjProcessor):
    @property
    def input_ids(self):
        return []
    
    @property
    def output_ids(self):
        return ['latest_feedback']
    
    def transform(self, inputs: List[vx.DataFrame], **kwargs) -> List[vx.DataFrame]:
        if not self._input_storage._backend.check_exists('latest'):
            latest_df = vx.from_pandas(pd.DataFrame.from_records([], columns=['name', 'latest', 'etag']))
            print('latest_feedback created')
        else:
            latest_df = self._input_storage.download('latest')
            print('latest_feedback loaded')
        print("Done loading latest for latest_feedback: ", len(latest_df))
        return [latest_df]
    
class NewPackageExtractor(ObjProcessor):
    def __init__(self, input_storage: PandasStorage, test_count: Optional[int]=None):
        self._test_count = test_count
        super().__init__(input_storage=input_storage)

    @property
    def input_ids(self):
        return ['name_trigger', 'latest_feedback']
    
    @property
    def output_ids(self):
        return ['name_trigger_new']

    def transform(self, inputs: List[vx.DataFrame], **kwargs) -> List[vx.DataFrame]:
        pkg_name_df = inputs[0]
        latest_df = inputs[1]
        print('Size of pkg_name:', len(pkg_name_df))
        print('Size of latest_feedback:', len(latest_df))
        new_pkg_names = self._get_new_package_names(pkg_name_df, latest_df)
        assert len(new_pkg_names) > 0, 'Should download new package'
        print('number of new packages:', len(new_pkg_names))
        return [vx.from_pandas(pd.DataFrame(new_pkg_names, columns=['name']))]

    def _get_new_package_names(self, pkg_name_df: pd.DataFrame, latest_df: pd.DataFrame) -> List[str]:
        pkg_names = pkg_name_df[['name']].to_arrays(array_type='list')[0]
        latest_names = latest_df[['name']].to_arrays(array_type='list')[0]
        new_names = list(set(pkg_names) - set(latest_names))
        if self._test_count is None:
            return new_names
        else:
            return new_names[:self._test_count]

class LatestProcessor:
    def process_latest(self, data: Dict) -> Dict:
        results = dict()
        results['info'] = data['info']
        results['info']['num_releases'] = len(data['releases'])
        return results
    
class LatestDownloader(ObjProcessor, LatestProcessor):
    @property
    def input_ids(self):
        return ['name_trigger_new']
    
    @property
    def output_ids(self):
        return ['latest_new']

    def transform(self, inputs: List[pd.DataFrame], **kwargs) -> List[pd.DataFrame]:
        assert len(inputs[0]) > 0, 'input table should have size > 0'
        new_df = self._get_new_package_records(inputs[0].name.tolist())
        assert 'name' in new_df.columns
        assert 'latest' in new_df.columns
        assert 'etag' in new_df.columns
        assert len(new_df.columns) == 3
        return [new_df]
    
    def _get_new_package_records(self, names: List[str]) -> pd.DataFrame:
        """Download new latest json data for a list of package names
        Args:
            names: Names of packages

        Returns:
            DataFrame with columns
                - name: Name of package
                - latest: Latest Json
                - etag: etag
        """
        results = []
        for i, name in enumerate(tqdm.tqdm(names, desc='get_new_package_records')):
            url = f"https://pypi.org/pypi/{name}/json"
            res = requests.get(url)
            if res.status_code == 404:
                continue
            assert res.status_code == 200, f'response status code is {res.status_code}'
            latest = res.json()
            latest = self.process_latest(latest)
            etag = res.headers["ETag"]
            results.append((name, latest, etag))
        return pd.DataFrame.from_records(results, columns=['name', 'latest', 'etag'])

class LatestUpdatorInputReduce(ObjProcessor):
    @property
    def input_ids(self):
        return ['latest_feedback']

    @property
    def output_ids(self):
        return ['latest_feedback_reduced']

    def transform(self, inputs: List[vx.DataFrame], **kwargs) -> List[vx.DataFrame]:
        return [inputs[0]['name', 'etag']]
    
class LatestUpdator(ObjProcessor, LatestProcessor):
    @property
    def input_ids(self):
        return ['latest_feedback_reduced']
    
    @property
    def output_ids(self):
        return ['latest_updated']

    def transform(self, inputs: List[pd.DataFrame], **kwargs) -> List[pd.DataFrame]:
        assert len(inputs) == 1, 'LatestUpdator should have 1 input from latest_feedback'
        try:
            df = inputs[0]
            for col in df.columns:
                if col not in ['name', 'etag']:
                    del df[col]
            new_df = self._get_updated_package_records(df)
            return [new_df]
        except Exception as e:
            raise e

    def _get_updated_package_records(self, latest_df: pd.DataFrame) -> pd.DataFrame:
        """Get the update latest records
        Args:
            latest_df (DataFrame with columns):
                - name: Name of package
                - latest: Latest Json
                - etag: etag
        Returns:
            new_df (Schema same as latest_df but only holds name of updated records)
        """
        results = []
        total = len(latest_df)
        name_etag_pipe = zip(latest_df.name.tolist(), latest_df.etag.tolist())
        name_etag_pipe = tqdm.tqdm(name_etag_pipe, total=total, desc='get_updated_package_records')
        for i, (name, etag) in enumerate(name_etag_pipe):
            result = self._update_with_etag(name, etag)
            if not isinstance(result, str):
                latest, etag = result
                latest = self.process_latest(latest)
                results.append((name, latest, etag))
                # print(f'[_get_updated_package_records] {i+1}/{total} {name} Download')
            # else:
                # print(f'[_get_updated_package_records] {i}/{total} {name} skipped due to {result}')
        return pd.DataFrame.from_records(results, columns=['name', 'latest', 'etag'])
    
    def _update_with_etag(self, name: str, etag: str) -> Optional[Tuple[Dict, str]]:
        """Update latest json data given package name and etag.
        (reduce repeat crawling of old data)

        Args:
            name (str): Name of package
            etag (str): Etag of the API call

        Returns:
            Optional[Tuple[Dict, str]]: 
                - Dict: The resulting latest json
                - str: The etag of the API call
                (Not None if there is data difference)
        """
        url = f"https://pypi.org/pypi/{name}/json"
        res = requests.get(url, headers={"If-None-Match": etag})
        if res.status_code == 404:
            return '404'
        assert res.status_code in [200, 304], f'response status code is {res.status_code}'
        if res.status_code == 200:
            latest = res.json()
            etag = res.headers["ETag"]
            return latest, etag
        else:
            return '304'

class Combine(ObjProcessor):
    @property
    def input_ids(self):
        return ['latest_new', 'latest_updated', 'latest_feedback']
    
    @property
    def output_ids(self):
        return ['latest']

    def transform(self, inputs: List[pd.DataFrame], **kwargs) -> List[pd.DataFrame]:
        print(f'Combine latest_new: {len(inputs[0])}, latest_updated: {len(inputs[1])}, latest_feedback: {len(inputs[2])}')
        result_df = pd.concat(inputs, ignore_index=True)
        result_df.drop_duplicates(subset=['name'], keep='first', inplace=True)
        print(f'Combined Size: {len(result_df)}')
        return [result_df]
    
