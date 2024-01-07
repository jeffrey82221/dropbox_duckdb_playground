import copy
from typing import Optional
from canon import SimplePyPiCanonicalize
from batch_framework.filesystem import DropboxBackend
from batch_framework.etl import ETLGroup
from batch_framework.filesystem import LocalBackend, DropboxBackend
from batch_framework.rdb import DuckDBBackend
from graph import GraphDataPlatform
from meta import metagraph, er_meta_license, er_meta_requirement

class GraphConstructor(ETLGroup):
    def __init__(self, test_count: Optional[int] = None):
        self.pypi_table_loader = SimplePyPiCanonicalize(
            raw_df=DropboxBackend('/data/canon/raw/'),
            tmp_fs=DropboxBackend('/data/canon/tmp/'),
            output_fs=DropboxBackend('/data/canon/output/'),
            partition_fs=LocalBackend('./data/canon/partition/'),
            download_worker_count=16,
            update_worker_count=16,
            test_count=test_count,
            do_update=True
        )
        self.table_to_graph_transformer = GraphDataPlatform(
            metagraph=copy.deepcopy(metagraph),
            canon_fs=DropboxBackend('/data/canon/output/'),
            subgraph_fs=DropboxBackend('/data/subgraph/'),
            output_fs=DropboxBackend('/data/graph/'),
            redisgraph_fs=LocalBackend('./data/redisgraph/'),
            er_meta_list=[copy.deepcopy(er_meta_license), copy.deepcopy(er_meta_requirement)],
            mapping_fs=DropboxBackend('/data/mapping/'),
            model_fs=DropboxBackend('/data/model/'),
            rdb=DuckDBBackend()
        )
        super().__init__(
            self.pypi_table_loader,
            self.table_to_graph_transformer
        )

    @property
    def input_ids(self):
        return self.pypi_table_loader.input_ids

    @property
    def output_ids(self):
        return self.table_to_graph_transformer.output_ids


if __name__ == '__main__':
    # for i in [257]:
    #     with open('index.log', 'w') as f:
    #         f.write('start:' + str(i))
    gc = GraphConstructor()
    gc.execute(max_active_run=16)