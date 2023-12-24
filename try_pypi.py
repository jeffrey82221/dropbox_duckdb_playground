import pprint
import requests
pkg_name = 'apache-airflow'
url = f'https://pypi.org/pypi/{pkg_name}/json'
data = requests.get(url).json()

versions = list(data['releases'].keys())
print(data['info']['platform'])
"""
for version in versions[:10]:
    url = f"https://pypi.org/pypi/{pkg_name}/{version}/json"
    result = requests.get(url).json()
    pprint.pprint(result)
"""
