from batch_framework.rdb import DuckDBBackend
from batch_framework.filesystem import LocalBackend
from batch_framework.storage import PandasStorage
from subgraph.links import LinkExtractor
from subgraph.nodes import NodeExtractor
from subgraph.validate import Validation

if __name__ == '__main__':
    input_fs = LocalBackend('./data/canon/output/')
    output_fs = LocalBackend('./data/subgraph/output/')
    db = DuckDBBackend()
    link_op = LinkExtractor(db, input_fs=input_fs, output_fs=output_fs)
    node_op = NodeExtractor(db, input_fs=input_fs, output_fs=output_fs)
    val_op = Validation(PandasStorage(db))
    link_op.execute()
    node_op.execute()
    val_op.execute()