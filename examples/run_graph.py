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
- [-] Another way of entity resolution is to do it between canon and graph grouping layer
    ( Canon Layer -> ER Layer -> Graph Aggregation )
- [X] Allow Entity Resolution to Store Process Time of Messy Nodes 
    and to pairing only the incoming new nodes.

- [ ] Passing to redisgraph
    - docker run -it -p 9001:6379 --rm redislabs/redisgraph
redisgraph-bulk-insert PYPI --enforce-schema --port 9001 --nodes node_package.csv 
    
    
    node_person.csv node_url.csv node_license.csv \
    --relations link_has_author.csv link_has_license.csv link_has_maintainer.csv \
        link_has_requirement.csv link_has_url.csv
"""
from batch_framework.filesystem import LocalBackend
from batch_framework.rdb import DuckDBBackend
from graph import GraphDataPlatform
from meta import metagraph, er_meta_license, er_meta_requirement

gdp = GraphDataPlatform(
    metagraph=metagraph,
    canon_fs = LocalBackend('./data/canon/output/'),
    subgraph_fs = LocalBackend('./data/subgraph/output/'),
    output_fs = LocalBackend('./data/graph/'),
    redisgraph_fs = LocalBackend('./data/redisgraph/'),
    er_meta_list=[er_meta_license, er_meta_requirement],
    mapping_fs = LocalBackend('./data/mapping/'),
    model_fs = LocalBackend('./data/model/'),
    rdb=DuckDBBackend(),
    messy_pairing_worker_cnt=10
)


if __name__ == '__main__':
    gdp._grouper.execute(max_active_run=1)
    import os
    url = 'redis://localhost:9001'
    graph_name = 'PYPI'
    cmd = f'redisgraph-bulk-insert -u {url} {graph_name} --enforce-schema '
    cmd += ' --skip-invalid-nodes '
    cmd += ' --skip-invalid-edges '
    for fn in os.listdir('data/redisgraph/'):
        print(fn)
        if fn.startswith('node_'):
            cmd += '--nodes ' + 'data/redisgraph/' + fn + ' '
        if fn.startswith('link_'):
            cmd += '--relations ' + 'data/redisgraph/' + fn + ' '
    os.system(cmd)