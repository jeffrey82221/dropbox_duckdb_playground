from typing import List
import requests
from bs4 import BeautifulSoup
import pandas as pd
from batch_framework.filesystem import LocalBackend
from batch_framework.storage import PandasStorage
from batch_framework.etl import DFProcessor

class PyPiNameDownload(DFProcessor):
    def __init__(self, input_storage, output_storage):
        super().__init__(input_storage, output_storage)
        self._url = "https://pypi.python.org/simple/"
    
    @property
    def input_ids(self):
        return []
    
    @property
    def output_ids(self):
        return ['package_names.parquet']

    def transform(self, inputs: List[pd.DataFrame]) -> List[pd.DataFrame]:
        names = self._download_from_pypi()
        print('number of packages:', len(names))
        return [pd.DataFrame(names, columns=['name'])]

    def _download_from_pypi(self):
        print(f"GET list of packages from {self._url}")
        try:
            resp = requests.get(self._url, timeout=5)
        except requests.exceptions.RequestException:
            print("ERROR: Could not GET the pypi index. Check your internet connection.")
            exit(1)

        print(f"NOW parsing the HTML (this could take a couple of seconds...)")
        try:
            soup = BeautifulSoup(resp.text, "html.parser")
            body = soup.find("body")
            links = (pkg for pkg in body.find_all("a"))
            pkg_names = [link["href"].split("/")[-2] for link in list(links)]
        except BaseException:
            print("ERROR: Could not parse pypi HTML.")
            exit(1)
        return pkg_names
        
if __name__ == '__main__':
    storage = PandasStorage(LocalBackend('./data/'))
    op1 = PyPiNameDownload(storage, storage)
    op1.execute()