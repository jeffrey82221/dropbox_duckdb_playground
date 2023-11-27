"""
Mapping Similar Nodes. 

TODO:
- [ ] Build Threshold Tunner
- [X] Build Learning ETL
- [X] Build Mapping Table ETL
- [X] Get mapping 1 ( messy node -> canon node )
- [X] Get mapping 2 ( messy node -> cluster id)
- [X] Combine mapping 1 & mapping 2
- [X] Do mapping ( messy node -> canon / cluster node )
- [X] Decompose messy_matcher
- [X] Build merging layer
"""
from batch_framework.storage import PandasStorage, JsonStorage
from batch_framework.filesystem import LocalBackend
from batch_framework.rdb import DuckDBBackend
from resolution import (
    ERMeta,
    CanonMatchLearner, MessyMatchLearner, 
    MappingGenerator
)
from meta import subgraphs

meta = ERMeta(
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

subgraph_fs = LocalBackend('./data/subgraph/output/')
train_fs = LocalBackend('./data/train/')
model_fs = LocalBackend('./data/model/')
mapping_fs = LocalBackend('./data/mapping/')

canon_learner = CanonMatchLearner(
    meta,
    PandasStorage(subgraph_fs), 
    JsonStorage(train_fs),
    model_fs=model_fs
)
messy_learner = MessyMatchLearner(
    meta, 
    PandasStorage(subgraph_fs), 
    JsonStorage(train_fs),
    model_fs=model_fs
)
# meta: ERMeta, subgraph_fs: FileSystem, mapping_fs: FileSystem, model_fs: FileSystem, rdb: RDB
mapping = MappingGenerator(
    meta,
    subgraph_fs,
    mapping_fs,
    model_fs,
    DuckDBBackend()
)


if __name__ == '__main__':
    # canon_learner.execute()
    # messy_learner.execute()
    mapping.execute(sequential=True)