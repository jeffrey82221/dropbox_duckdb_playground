"""
Mapping Similar Nodes. 

TODO:
- [ ] Build Threshold Tunner
- [X] Build Learning ETL
- [X] Build Mapping Table ETL
- [X] Get mapping 1 ( messy node -> canon node )
- [ ] Get remain messy node 
- [X] Get mapping 2 ( messy node -> cluster id)
- [ ] Combine mapping 1 & mapping 2
- [ ] Do mapping ( messy node -> canon / cluster node )
- [ ] Build merging layer
"""
from batch_framework.storage import PandasStorage, JsonStorage
from batch_framework.filesystem import LocalBackend
from resolution import (
    ERMeta, 
    CanonMatchLearner, MessyMatchLearner, 
    CanonMatcher, MessyMatcher
)

input_fs = LocalBackend('./data/subgraph/output/')
train_fs = LocalBackend('./data/train/')
model_fs = LocalBackend('./data/model/')
mapping_fs = LocalBackend('./data/mapping/')
meta = ERMeta(
    messy_node='requirement',
    canon_node='package',
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
    canon_lambda=lambda record: {
        'full_name': record['name'],
        'before_whitespace': record['name'],
        'before_upper_bracket': record['name'],
        'before_marks': record['name']
    }
)
op1 = CanonMatchLearner(meta, 
        PandasStorage(input_fs), 
        JsonStorage(train_fs),
        model_fs=model_fs
    )
op2 = MessyMatchLearner(
    meta, 
    PandasStorage(input_fs), 
    JsonStorage(train_fs),
    model_fs=model_fs
)
op3 = CanonMatcher(meta,
    PandasStorage(input_fs), 
    PandasStorage(mapping_fs),
    model_fs=model_fs,
    threshold=0.25
)
op4 = MessyMatcher(
    meta,
    PandasStorage(input_fs), 
    PandasStorage(mapping_fs),
    model_fs=model_fs,
    threshold=0.5
)

if __name__ == '__main__':
    op4.execute()