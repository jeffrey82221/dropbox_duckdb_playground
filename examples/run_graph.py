"""
Build Flow of:

- [X] Subgraph Extraction
    - Define relation between link & node of subgraph
- [X] Entity Resolution
    - Define mapping of nodes between two subgraph
    - Take subgraph as input such that related links ID can also be clean up. 
- [X] Graph Merging
    - Define nodes that should be merged. 
    - Include ERMeta to build a hidden flow start from cleaned nodes. 
- [X] Decompose GroupingMeta and MetaGraph. 
- [X] ERMeta(s) should take subgraphs as __init__ input for finding messy link/node.
- [X] ERMeta when attach with a GroupingMeta, should generate related IDConvertor(s).
    and produce a revised GroupingMeta.
"""
from batch_framework.rdb import DuckDBBackend
from batch_framework.filesystem import LocalBackend
from subgraph import SubgraphExtractor
from metagraph import MetaGraph
from group import NodeGrouper, LinkGrouper
from resolution import (
    ERMeta,
    MappingGenerator
)
from meta import subgraphs
metagraph = MetaGraph(
    subgraphs=subgraphs,
    node_grouping={
        'package': ['package', 'requirement'],
        'person': ['author', 'maintainer'],
        'url': ['docs_url', 'home_page', 'project_url']
    },
    node_grouping_sqls={
        'package': """
            t0.node_id,
            COALESCE(t1.name, t2.name) AS name,
            t1.requires_python,
            t1.version,
            t1.keywords,
            t1.num_releases
        """,
        'person': """
            t0.node_id,
            COALESCE(t1.name, t2.name) AS name,
            COALESCE(t1.email, t2.email) AS email
        """,
        "url": """
            t0.node_id,
            COALESCE(t1.url, t2.url, t3.url) AS url
        """
    },
    link_grouping={
        'has_url': ['has_project_url', 'has_docs_url',  'has_home_page']
    },
    link_grouping_sqls = {
        'has_url': """
            t0.from_id,
            t0.to_id,
            COALESCE(t1.url_type, t2.url_type, t3.url_type) AS url_type
        """
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
            HASH(docs_url) AS to_id,
            'Documentation' AS url_type
        FROM latest_package
        WHERE docs_url IS NOT NULL
        """,
        # Home Page URL Node
        'has_home_page': """
        SELECT
            DISTINCT ON (home_page, pkg_name)
            HASH(pkg_name) AS from_id,
            HASH(home_page) AS to_id,
            'Homepage' AS url_type
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
er_meta = ERMeta(
    subgraphs=subgraphs,
    messy_node='requirement',
    dedupe_fields=[
        {'field': 'full_name', 'type': 'String'},
        {'field': 'before_whitespace', 'type': 'Exact'},
        {'field': 'before_upper_bracket', 'type': 'Exact'},
        {'field': 'before_marks', 'type': 'Exact'}
    ],
    messy_lambda=lambda record: {
        'full_name': record['name'],
        'before_whitespace': record['name'].split(' ')[0].split(';')[0],
        'before_upper_bracket': record['name'].split('[')[0].split('(')[0],
        'before_marks': record['name'].split('<')[0].split('>')[0].split('=')[0].split('~')[0]
    },
    canon_node='package',
    canon_lambda=lambda record: {
        'full_name': record['name'],
        'before_whitespace': record['name'],
        'before_upper_bracket': record['name'],
        'before_marks': record['name']
    }
)
grouping_meta = metagraph.grouping_meta
er_meta.alter_grouping_way(grouping_meta)

subgraph_extractor = SubgraphExtractor(
    metagraph=metagraph, 
    rdb=DuckDBBackend(), 
    input_fs=LocalBackend('./data/canon/output/'), 
    output_fs=LocalBackend('./data/subgraph/output/')
)

mapping = MappingGenerator(
    er_meta,
    LocalBackend('./data/subgraph/output/'),
    LocalBackend('./data/mapping/'),
    LocalBackend('./data/model/'),
    DuckDBBackend()
)

node_grouper = NodeGrouper(
    meta=grouping_meta,
    rdb=DuckDBBackend(),
    input_fs=LocalBackend('./data/subgraph/output/'),
    output_fs=LocalBackend('./data/graph/nodes/')
)
link_grouper = LinkGrouper(
    meta=grouping_meta,
    rdb=DuckDBBackend(),
    input_fs=LocalBackend('./data/subgraph/output/'),
    output_fs=LocalBackend('./data/graph/links/')
)
if __name__ == '__main__':
    # subgraph_extractor.execute(sequential=True)
    # mapping.execute(sequential=True)
    node_grouper.execute()
    link_grouper.execute()