"""
Extract Link Table using DUCKDB from filesystem
"""
from batch_framework.etl import SQLExecutor
from batch_framework.rdb import DuckDBBackend
from batch_framework.filesystem import LocalBackend


class LinkExtractor(SQLExecutor):
    @property
    def input_ids(self):
        return [
            'latest_package',
            'latest_requirement',
            'latest_url'
        ]

    @property
    def output_ids(self):
        return [ 
            'has_requirement', 
            'has_author', 
            'has_maintainer', 
            'has_license', 
            'has_docs_url', 
            'has_home_page', 
            'has_project_url'
        ]
    
    
    def sqls(self, **kwargs):
        return {
            # Has Requirement Link
            'has_requirement': """
            SELECT
                DISTINCT ON (pkg_name, requirement)
                HASH(pkg_name) AS from_id,
                HASH(requirement) AS to_id
            FROM latest_requirement
            """,
            # Has Author Link
            'has_author': """
            SELECT
                DISTINCT ON (author, author_email, pkg_name)
                HASH(pkg_name) AS from_id,
                HASH(CONCAT(author, '|', author_email)) AS to_id
            FROM latest_package
            WHERE author IS NOT NULL AND author_email IS NOT NULL
            AND author_email <> ''
            """,
            # Has Maintainer Link
            'has_maintainer': """
            SELECT
                DISTINCT ON (maintainer, maintainer_email, pkg_name)
                HASH(pkg_name) AS from_id,
                HASH(CONCAT(maintainer, '|', maintainer_email)) AS to_id
            FROM latest_package
            WHERE maintainer IS NOT NULL AND maintainer_email IS NOT NULL
            AND maintainer_email <> ''
            """,
            # Has License Link
            'has_license': """
            SELECT
                DISTINCT ON (license, pkg_name)
                HASH(pkg_name) AS from_id,
                HASH(license) AS to_id
            FROM latest_package
            WHERE license IS NOT NULL 
                AND license <> 'UNKNOWN'
                AND license <> 'LICENSE.txt'
            """,
            # Docs URL Node
            'has_docs_url': """
            SELECT
                DISTINCT ON (docs_url, pkg_name)
                HASH(pkg_name) AS from_id,
                HASH(docs_url) AS to_id
            FROM latest_package
            WHERE docs_url IS NOT NULL
            """,
            # Home Page URL Node
            'has_home_page': """
            SELECT
                DISTINCT ON (home_page, pkg_name)
                HASH(pkg_name) AS from_id,
                HASH(home_page) AS to_id
            FROM latest_package
            WHERE home_page IS NOT NULL
            """,
            # Project URL Node
            'has_project_url': """
            SELECT
                DISTINCT ON (url, pkg_name)
                HASH(pkg_name) AS from_id,
                HASH(url) AS to_id,
                url_type
            FROM latest_url
            WHERE url IS NOT NULL
            AND url <> 'UNKNOWN'
            """
        }


