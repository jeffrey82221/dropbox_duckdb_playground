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
from graph.resolution import (
    CanonMatchLearner, MessyMatchLearner
)
from meta import er_meta


subgraph_fs = LocalBackend('./data/subgraph/output/')
train_fs = LocalBackend('./data/train/')
model_fs = LocalBackend('./data/model/')
mapping_fs = LocalBackend('./data/mapping/')

canon_learner = CanonMatchLearner(
    er_meta,
    PandasStorage(subgraph_fs), 
    JsonStorage(train_fs),
    model_fs=model_fs
)
messy_learner = MessyMatchLearner(
    er_meta, 
    PandasStorage(subgraph_fs), 
    JsonStorage(train_fs),
    model_fs=model_fs
)

if __name__ == '__main__':
    canon_learner.execute()
    messy_learner.execute()
    