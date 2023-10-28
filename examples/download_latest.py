
"""
TODO:
- [X] Create empty dataframe if not exists
- [ ] Enable Multi-Thread Download for Update Mode
"""
from typing import List, Dict, Tuple, Optional
import requests
import pandas as pd
from batch_framework.filesystem import LocalBackend
from batch_framework.storage import PandasStorage
from batch_framework.etl import DFProcessor

class LatestDownload(DFProcessor):
    def __init__(self, input_storage, output_storage, test_count: Optional[int]=None):
        assert input_storage == output_storage, 'Should using the same storage here'
        super().__init__(input_storage=input_storage, output_storage=output_storage)
        self._test_count = test_count
        if not input_storage._backend.check_exists('latest.parquet'):
            latest_df = pd.DataFrame.from_records([], columns=['name', 'latest', 'etag'])
            input_storage.upload(latest_df, 'latest.parquet')

    @property
    def input_ids(self):
        return ['package_names.parquet', 'latest.parquet']

    @property
    def output_ids(self):
        return ['latest.parquet']
    
    def transform(self, inputs: List[pd.DataFrame]) -> List[pd.DataFrame]:
        pkg_name_df, latest_df = inputs
        print('Package Name Count:', len(pkg_name_df))
        print('Latest Count:', len(latest_df))
        new_pkg_names = self._get_new_package_names(pkg_name_df, latest_df)
        new_df = self._get_new_package_records(new_pkg_names)
        print('New Packages Count:', len(new_df))
        update_df = self._get_updated_package_records(latest_df)
        print('Updated Packages Count:', len(update_df))
        result_df = pd.concat([new_df, update_df, latest_df], ignore_index=True)
        print('Total Output Package Count Before Drop Duplicate:', len(result_df))
        result_df.drop_duplicates(subset=['name'], keep='first', inplace=True)
        print('Total Output Package Count:', len(result_df))
        return [result_df]

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
        for name in names:
            url = f"https://pypi.org/pypi/{name}/json"
            res = requests.get(url)
            if res.status_code == 404:
                print(f'[_get_new_package_records] {name} latest skipped due to 404')
                continue
            assert res.status_code == 200, f'response status code is {res.status_code}'
            latest = res.json()
            etag = res.headers["ETag"]
            print(f'[_get_new_package_records] {name} latest downloaded.')
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
        for name, etag in zip(latest_df.name.tolist(), latest_df.etag.tolist()):
            result = self._update_with_etag(name, etag)
            if result is not None:
                latest, etag = result
                results.append((name, latest, etag))
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
            print(f'[_update_with_etag] {name} latest skipped due to 404')
            return None
        assert res.status_code in [200, 304], f'response status code is {res.status_code}'
        if res.status_code == 200:
            latest = res.json()
            etag = res.headers["ETag"]
            print(f'[_update_with_etag] {name} latest updated')
            return latest, etag
        else:
            print(f'[_update_with_etag] {name} latest skipped due to 304')
            return None
        
if __name__ == '__main__':
    storage = PandasStorage(LocalBackend('./data/'))
    op2 = LatestDownload(storage, storage, test_count=1000)
    op2.execute()