"""
Extract Node Table using DUCKDB from filesystem
"""
from batch_framework.etl import SQLExecutor
from batch_framework.rdb import DuckDBBackend
from batch_framework.filesystem import LocalBackend


class NodeExtractor(SQLExecutor):
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
            'package', 
            'requirement', 
            'author', 
            'maintainer', 
            'license', 
            'docs_url', 
            'home_page', 
            'project_url'
        ]
    
    
    def sqls(self, **kwargs):
        return {
            # Main Package Node
            'package': """
            SELECT
                DISTINCT ON (pkg_name)
                HASH(pkg_name) AS node_id,
                name,
                package_url,
                project_url,
                requires_python,
                version,
                keywords,
                num_releases
            FROM latest_package
            """,
            # Requirement Package Node
            'requirement': """
            SELECT
                DISTINCT ON (requirement)
                HASH(requirement) AS node_id,
                requirement AS name
            FROM latest_requirement
            """,
            # Author Person Node
            'author': """
            SELECT
                DISTINCT ON (author, author_email)
                HASH(CONCAT(author, '|', author_email)) AS node_id,
                author AS name,
                author_email AS email
            FROM latest_package
            WHERE author IS NOT NULL AND author_email IS NOT NULL
            AND author_email <> ''
            """,
            # Maintainer Person Node
            'maintainer': """
            SELECT
                DISTINCT ON (maintainer, maintainer_email)
                HASH(CONCAT(maintainer, '|', maintainer_email)) AS node_id,
                maintainer AS name,
                maintainer_email AS email
            FROM latest_package
            WHERE maintainer IS NOT NULL AND maintainer_email IS NOT NULL
            AND maintainer_email <> ''
            """,
            # License Node
            'license': """
            SELECT
                DISTINCT ON (license)
                HASH(license) AS node_id,
                license AS name
            FROM latest_package
            WHERE license IS NOT NULL 
                AND license <> 'UNKNOWN'
                AND license <> 'LICENSE.txt'
            """,
            # Docs URL Node
            'docs_url': """
            SELECT
                DISTINCT ON (docs_url)
                HASH(docs_url) AS node_id,
                docs_url AS url
            FROM latest_package
            WHERE docs_url IS NOT NULL
            """,
            # Home Page URL Node
            'home_page': """
            SELECT
                DISTINCT ON (home_page)
                HASH(home_page) AS node_id,
                home_page AS url
            FROM latest_package
            WHERE home_page IS NOT NULL
            """,
            # Project URL Node
            'project_url': """
            SELECT
                DISTINCT ON (url)
                HASH(url) AS node_id,
                url
            FROM latest_url
            WHERE url IS NOT NULL
            AND url <> 'UNKNOWN'
            """
        }

