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
from batch_framework.filesystem import LocalBackend
from batch_framework.rdb import DuckDBBackend
from graph import GraphDataPlatform
from meta import metagraph, er_meta_license, er_meta_requirement

gdp = GraphDataPlatform(
    metagraph=metagraph,
    canon_fs = LocalBackend('./data/canon/output/'),
    subgraph_fs = LocalBackend('./data/subgraph/output/'),
    node_fs = LocalBackend('./data/graph/nodes/'),
    link_fs = LocalBackend('./data/graph/links/'),
    er_meta_list=[er_meta_license, er_meta_requirement],
    mapping_fs = LocalBackend('./data/mapping/'),
    model_fs = LocalBackend('./data/model/'),
    rdb=DuckDBBackend(),
    messy_pairing_worker_cnt=100
)

if __name__ == '__main__':
    gdp.execute(max_active_run=2)