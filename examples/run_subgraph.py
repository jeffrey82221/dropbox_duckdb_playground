"""
Build Flow of:
- [ ] Subgraph Extraction
    - Define relation between link & node of subgraph
- [ ] Entity Resolution
    - Define mapping of nodes between two subgraph
    - Take subgraph as input such that related links ID can also be clean up. 
- [ ] Graph Merging
    - Define nodes that should be merged. 
    - Include ERMeta to build a hidden flow start from cleaned nodes. 
"""

from batch_framework.rdb import DuckDBBackend
from batch_framework.filesystem import LocalBackend
from subgraph.main import SubgraphExtractor
from subgraph.metagraph import MetaGraph

metagraph = MetaGraph({
        'has_requirement': ('package', 'requirement'),
        'has_author': ('package', 'author'), 
        'has_maintainer': ('package', 'maintainer'), 
        'has_license': ('package', 'license'), 
        'has_docs_url': ('package', 'docs_url'), 
        'has_home_page': ('package', 'home_page'), 
        'has_project_url': ('package', 'project_url')
    },  
    input_ids=[
        'latest_package',
        'latest_requirement',
        'latest_url'
    ],
    node_sqls={
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
    },
    link_sqls={
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
)

input_fs = LocalBackend('./data/canon/output/')
output_fs = LocalBackend('./data/subgraph/output/')
db = DuckDBBackend()
subgraph_extractor = SubgraphExtractor(
    metagraph=metagraph, 
    rdb=db, 
    input_fs=input_fs, 
    output_fs=output_fs
)

if __name__ == '__main__':
    subgraph_extractor.execute(sequential=True)