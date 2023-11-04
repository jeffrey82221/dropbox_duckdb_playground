
"""
Download Latest Json for all package on PyPi
TODO:
- [X] Try to devide and conquer the download for speed up the process. 
    - [X] Add `name_trigger` partitioning task between `trigger` and `crawl`
    - [X] Add partition_id selection to `crawl`
    - [X] Add `latest` merge task between crawl and tabularize 
- [ ] Decompose Crawl into 
    - [ ] Get new package names 
    - [ ] Download new package records -> Decorate with MapReduce 
    - [ ] Update package records -> Decorate with MapReduce
    - [ ] Combine package records 
"""
from typing import List, Dict, Tuple, Optional
import requests
import pandas as pd
from batch_framework.etl import ObjProcessor
from batch_framework.storage import PandasStorage

class LatestCrawler(ObjProcessor):
    def __init__(self, input_storage: PandasStorage, test_count: Optional[int]=None, partition_id: Optional[int]=None):
        self._partition_id = partition_id
        super().__init__(input_storage=input_storage)
        self._test_count = test_count

    def start(self, **kwargs):
        if not self._input_storage._backend.check_exists('latest_feedback'):
            latest_df = pd.DataFrame.from_records([], columns=['name', 'latest', 'etag'])
            self._input_storage.upload(latest_df, 'latest_feedback')

    @property
    def input_ids(self):
        if self._partition_id is not None:
            return [f'name_trigger.{self._partition_id}', 'latest_feedback']
        else:
            return ['name_trigger', 'latest_feedback']

    @property
    def external_input_ids(self) -> List[str]:
        return ['latest_feedback']
    
    @property
    def output_ids(self):
        if self._partition_id is not None:
            return [f'latest.{self._partition_id}']
        else:
            return ['latest']
        
    def transform(self, inputs: List[pd.DataFrame], **kwargs) -> List[pd.DataFrame]:
        if len(inputs) == 2:
            pkg_name_df = inputs[0]
            latest_df = inputs[1]
            print(f'Package Name Count ({self._partition_id}):', len(pkg_name_df))
            new_pkg_names = self._get_new_package_names(pkg_name_df, latest_df)
            new_df = self._get_new_package_records(new_pkg_names)
            print(f'New Packages Count ({self._partition_id}):', len(new_df))
            update_df = self._get_updated_package_records(latest_df)
            print(f'Updated Packages Count ({self._partition_id}):', len(update_df))
            result_df = pd.concat([new_df, update_df, latest_df], ignore_index=True)
            print(f'Total Output Package Count Before Drop Duplicate ({self._partition_id}):', len(result_df))
            result_df.drop_duplicates(subset=['name'], keep='first', inplace=True)
            print(f'Total Output Package Count ({self._partition_id}):', len(result_df))
            return [result_df]
        else:
            pkg_name_df = inputs[0]
            print(f'Package Name Count ({self._partition_id}):', len(pkg_name_df))
            new_pkg_names = pkg_name_df.name.tolist()
            new_df = self._get_new_package_records(new_pkg_names)
            print(f'New Packages Count ({self._partition_id}):', len(new_df))
            return [new_df]

    def _get_new_package_names(self, pkg_name_df: pd.DataFrame, latest_df: pd.DataFrame) -> List[str]:
        new_names = list(set(pkg_name_df.name) - set(latest_df.name))
        if self._test_count is None:
            return new_names
        else:
            return new_names[:self._test_count]

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
        for i, name in enumerate(names):
            url = f"https://pypi.org/pypi/{name}/json"
            res = requests.get(url)
            if res.status_code == 404:
                print(f'[_get_new_package_records] ({self._partition_id}) {name} latest skipped due to 404')
                continue
            assert res.status_code == 200, f'response status code is {res.status_code}'
            latest = res.json()
            etag = res.headers["ETag"]
            print(f'[_get_new_package_records] ({self._partition_id}) {i+1}/{len(names)} {name} latest downloaded.')
            results.append((name, latest, etag))
        return pd.DataFrame.from_records(results, columns=['name', 'latest', 'etag'])

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
        for i, (name, etag) in enumerate(zip(latest_df.name.tolist(), latest_df.etag.tolist())):
            result = self._update_with_etag(name, etag)
            if not isinstance(result, str):
                latest, etag = result
                results.append((name, latest, etag))
                print(f'[_get_updated_package_records] ({self._partition_id}) {i+1}/{total} {name} Download')
            else:
                print(f'[_get_updated_package_records] ({self._partition_id}) {i}/{total} {name} skipped due to {result}')
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
