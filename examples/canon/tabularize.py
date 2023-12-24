
"""
Convert pandas with JSON column to plain pandas dataframe
"""
from typing import List, Dict, Union
import pandas as pd
import json
from batch_framework.etl import ObjProcessor


class LatestTabularize(ObjProcessor):
    @property
    def input_ids(self):
        return ['latest']

    @property
    def output_ids(self):
        return ['latest_package', 'latest_requirement', 'latest_url']

    def transform(self, inputs: List[pd.DataFrame]) -> List[pd.DataFrame]:
        infos = []
        reqs = []
        urls = []
        for record in inputs[0].to_dict('records'):
            record['latest'] = json.loads(record['latest'])
            info = LatestTabularize.simplify_record(record)
            _reqs = LatestTabularize.simplify_requires_dist(record)
            _urls = LatestTabularize.simplify_project_urls(record)
            infos.append(info)
            reqs.extend(_reqs)
            urls.extend(_urls)

        package_df = pd.DataFrame(infos)
        requirement_df = pd.DataFrame(reqs)
        urls_df = pd.DataFrame(urls)
        assert len(package_df) > 0
        assert len(requirement_df) > 0
        assert len(urls_df) > 0
        print('Package Table Size:', len(package_df))
        print('Requirement Table Size:', len(requirement_df))
        print('Url Table Size:', len(urls_df))
        return [package_df, requirement_df, urls_df]

    @staticmethod
    def simplify_record(
            record: Dict) -> Dict[str, Union[str, int, float, None]]:
        """Simplify the nestest record dictionary

        Args:
            record (Dict): A nested dictionary

        Returns:
            Dict: The simplified dictionary that is not nested
        """
        return {
            'pkg_name': record['name'],
            'name': record['latest']['info']['name'],
            'package_url': record['latest']['info']['package_url'],
            'project_url': record['latest']['info']['project_url'],
            'requires_python': record['latest']['info']['requires_python'],
            'version': record['latest']['info']['version'],
            'keywords': record['latest']['info']['keywords'],
            'num_releases': record['latest']['info']['num_releases'],
            'author': record['latest']['info']['author'],
            'author_email': record['latest']['info']['author_email'],
            'maintainer': record['latest']['info']['maintainer'],
            'maintainer_email': record['latest']['info']['maintainer_email'],
            'license': record['latest']['info']['license'],
            'docs_url': record['latest']['info']['docs_url'],
            'home_page': record['latest']['info']['home_page']
        }

    @staticmethod
    def simplify_requires_dist(
            record: Dict) -> List[Dict[str, Union[str, int, float, None]]]:
        """Simply nested componenet - requires_dict in record

        Args:
            record (Dict): A nested dictionary

        Returns:
            List[Dict]:  List of the simplified dictionary that is not nested
        """
        data = record['latest']['info']['requires_dist']
        if data is not None:
            return [
                {
                    'pkg_name': record['name'],
                    'requirement': e
                } for e in data
            ]
        else:
            return []

    @staticmethod
    def simplify_project_urls(
            record: Dict) -> List[Dict[str, Union[str, int, float, None]]]:
        """Simply nested componenet - project_urls in record

        Args:
            record (Dict): A nested dictionary

        Returns:
            List[Dict]:  List of the simplified dictionary that is not nested
        """
        if isinstance(record['latest']['info']['project_urls'], dict):
            return [
                {
                    'pkg_name': record['name'],
                    'url_type': key,
                    'url': value
                } for key, value in record['latest']['info']['project_urls'].items() if value is not None
            ]
        else:
            return []
