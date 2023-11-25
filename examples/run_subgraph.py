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