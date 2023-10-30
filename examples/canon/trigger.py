"""
Get PyPi Name 
"""
from typing import List
import requests
from bs4 import BeautifulSoup
import pandas as pd
from batch_framework.etl import DFProcessor

URL = "https://pypi.python.org/simple/"

class PyPiNameTrigger(DFProcessor):
    @property
    def input_ids(self):
        return []
    
    @property
    def output_ids(self):
        return ['name_trigger']

    def transform(self, inputs: List[pd.DataFrame]) -> List[pd.DataFrame]:
        names = self._download_from_pypi()
        print('number of packages:', len(names))
        return [pd.DataFrame(names, columns=['name'])]

    def _download_from_pypi(self):
        print(f"GET list of packages from {URL}")
        try:
            resp = requests.get(URL, timeout=5)
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