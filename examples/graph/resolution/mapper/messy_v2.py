from typing import Iterator, Tuple, List, Dict
import pandas as pd
import dedupe
import tqdm
import io
import csv
import itertools
import igraph as ig
from batch_framework.filesystem import FileSystem, LocalBackend
from batch_framework.storage import PandasStorage
from batch_framework.etl import SQLExecutor, ETLGroup
from batch_framework.rdb import RDB
from batch_framework.parallize import MapReduce
from .base import MatcherBase
from ..base import MessyOnly
from ..meta import ERMeta


class MessyMatcher(ETLGroup):
    """
    Input Messy Node Table 
    Output a Messy->Cluster Mapping Table
    """
    def __init__(self, meta: ERMeta, subgraph_fs: FileSystem, mapping_fs: FileSystem, model_fs: FileSystem, rdb: RDB, pairing_worker_count: int = 10, threshold=0.5, take_filtered=True):
        self._mapping_fs = mapping_fs
        self._take_filtered = take_filtered
        self._partition_fs = LocalBackend(f'{self._mapping_fs._directory}partition/')
        messy_feature_engineer = MessyFeatureEngineer(
            meta,
            PandasStorage(subgraph_fs), 
            PandasStorage(mapping_fs),
            model_fs=None,
            take_filtered=self._take_filtered
        )
        messy_blocker = MessyBlocker(
            meta,
            PandasStorage(mapping_fs), 
            PandasStorage(mapping_fs),
            model_fs=model_fs
        )
        messy_entity_map = MessyEntityPairer(
            meta,
            rdb,
            mapping_fs
        )
        messy_pair_selector = MapReduce(MessyPairSelector(
            meta,
            PandasStorage(mapping_fs), 
            PandasStorage(mapping_fs),
            model_fs=model_fs,
            threshold=threshold
        ), pairing_worker_count, 
            tmp_fs = self._partition_fs
        )
        messy_cluster = MessyClusterer(
            meta,
            PandasStorage(mapping_fs), 
            PandasStorage(mapping_fs),
            model_fs=None
        )
        self._meta = meta
        super().__init__(
            messy_feature_engineer,
            messy_blocker,
            messy_entity_map,
            messy_pair_selector,
            messy_cluster
        )
        

    @property
    def input_ids(self):
        if self._take_filtered:
            return [self._meta.messy_node + '_filtered']
        else:
            return [self._meta.messy_node]
    
    @property
    def output_ids(self):
        return [f'mapper_{self._meta.messy_node}']
    
    @property
    def label(self):
        return self._meta.messy_node

    def start(self, **kwargs):
        from pathlib import Path
        path = Path(self._partition_fs._directory).mkdir(parents=True, exist_ok=True)
        print(path, 'created')
        
    def end(self, **kwargs):
        self.drop_internal_objs()

class Readable(object):
    """
    Convert iterator to in-memory CSV file
    """
    def __init__(self, iterator):

        self.output = io.StringIO()
        self.writer = csv.writer(self.output)
        self.iterator = iterator

    def read(self, size):

        self.writer.writerows(itertools.islice(self.iterator, size))

        chunk = self.output.getvalue()
        self.output.seek(0)
        self.output.truncate(0)

        return chunk

class MessyFeatureEngineer(MessyOnly, MatcherBase):
    """
    Input node table 
    Output node feature table
    """
    def __init__(self, mapping_meta: ERMeta, input_storage: PandasStorage, output_storage: PandasStorage, model_fs: FileSystem, threshold: float=0.25, take_filtered=True):
        self._take_filtered = take_filtered
        super().__init__(mapping_meta, input_storage, output_storage, model_fs=model_fs)
        self._threshold = threshold
        
    
    def transform(self, inputs: List[pd.DataFrame], **kwargs) -> List[pd.DataFrame]:
        table = pd.DataFrame.from_dict(self.feature_generation(inputs[0].to_dict('records')))
        print(table)
        assert len(table) == len(inputs[0])
        origin_node_ids = set(inputs[0].node_id.tolist())
        processed_node_ids = set(table.node_id.tolist())
        print('common node ids:', len(origin_node_ids & processed_node_ids))
        return [table]

    def feature_generation(self, records: Iterator[Dict]) -> Iterator[Dict]:
        for record in records:
            result = self._meta.messy_lambda(record)
            result['node_id'] = record['node_id']
            yield result

    @property
    def input_ids(self):
        if self._take_filtered:
            return [self._meta.messy_node + '_filtered']
        else:
            return [self._meta.messy_node]
    
    @property
    def output_ids(self):
        return [f'{self.label}_feature']
    
class MessyBlocker(MessyOnly, MatcherBase):
    """Produce Node Block Table using Dedupe Package
    Input: messy node data, model
    Output: Blocking table
    """
    def start(self):
        """Load model setting in the beginning
        """
        print('Start Loading Model to MessyMatcher')
        buff = self._model_fs.download_core(self.model_file_name)
        buff.seek(0)
        self._deduper = dedupe.StaticDedupe(buff)
        print('Finish Creating dedupe.StaticDedupe of MessyMatcher')

    def transform(self, inputs: List[pd.DataFrame], **kwargs) -> List[pd.DataFrame]:
        table = inputs[0]
        print('input size of messy blocker:', len(table))
        fields = [field for field in self._deduper.fingerprinter.index_fields]
        assert len(fields) > 0, 'index fields list is empty'
        for field in fields:
            print('fingerprinting field:', field)
            field_data = tuple(table[field].unique().tolist())
            self._deduper.fingerprinter.index(field_data, field)
        b_data = self._deduper.fingerprinter(self.to_fingerprinter(table))
        result = pd.read_csv(
            Readable(b_data), 
            names = ['block_key', 'messy_id'], 
            header=None
        )
        print('result size of messy blocker:', len(result))
        assert len(result) > 0, 'messy block result size = 0'
        origin_node_ids = set(inputs[0].node_id.tolist())
        processed_node_ids = set(result.messy_id.tolist())
        print('common node ids:', len(origin_node_ids & processed_node_ids))
        assert len(origin_node_ids & processed_node_ids) > 0, 'messy block size = 0'
        return [result]

    def to_fingerprinter(self, table: pd.DataFrame) -> List[Dict]:
        for record in table.to_dict('records'):
            id = record['node_id']
            del record['node_id']
            yield (id, record)

    @property
    def input_ids(self):
        return [f'{self.label}_feature']
    
    @property
    def output_ids(self):
        return [f'{self.label}_block']
    
class MessyEntityPairer(SQLExecutor, MessyOnly):
    """Generate Entity Map from Block Table
    """
    def __init__(self, meta: ERMeta, rdb: RDB, fs: FileSystem):
        self._meta = meta
        self.messy_node = meta.messy_node
        super().__init__(rdb, input_fs=fs, output_fs=fs)

    @property
    def input_ids(self):
        return [f'{self.label}_feature', f'{self.label}_block']

    @property
    def output_ids(self):
        return [f'{self.label}_entity_map']
    
    def sqls(self):
        return {
            self.output_ids[0]: f"""
            SELECT 
                a.node_id AS a_node_id,
                {self.get_column_str('a')},
                b.node_id AS b_node_id,
                {self.get_column_str('b')}
            FROM (SELECT DISTINCT l.messy_id AS east, r.messy_id AS west
                    FROM {self.label}_block AS l
                    INNER JOIN {self.label}_block AS r
                    USING (block_key)
                    WHERE l.messy_id < r.messy_id) ids
            INNER JOIN {self.label}_feature a ON ids.east=a.node_id
            INNER JOIN {self.label}_feature b ON ids.west=b.node_id
            """
        }

    def get_column_str(self, prefix: str) -> str:
        return ",".join([f'{prefix}.{field} AS {prefix}_{field}' for field in self.fields])

    @property
    def fields(self) -> List[str]:
        return [field['field'] for field in self._meta.dedupe_fields]

class MessyEntityMapValidate(MessyOnly, MatcherBase):
    """
    Validate the ID consistency of input tables
    """
    @property
    def input_ids(self):
        return [f'{self.label}_feature', f'{self.label}_entity_map', f'{self.label}_id_pairs']

    @property
    def output_ids(self):
        return []
    
    def transform(self, inputs: List[pd.DataFrame], **kwargs) -> List[pd.DataFrame]:
        feature_table = inputs[0]
        entity_map_table = inputs[1]
        id_pairs_table = inputs[2]
        feature_node_ids = set(feature_table.node_id.tolist())
        block_table_nodes = set(entity_map_table.a_node_id.tolist()) | set(entity_map_table.b_node_id.tolist())
        print('common node ids:', len(feature_node_ids & block_table_nodes))
        id_pairs_table['from'] = id_pairs_table['from'].map(int)
        id_pairs_table['to'] = id_pairs_table['to'].map(int)
        id_pairs_nodes = set(id_pairs_table['from'].tolist()) | set(id_pairs_table['to'].tolist())
        print('common node ids:', len(id_pairs_nodes & block_table_nodes))
        return []

class MessyPairSelector(MessyOnly, MatcherBase):
    """
    Input Entity Mapping Table
    Output Id-Id Pairing Table
    """
    def start(self):
        """Load model setting in the beginning
        """
        print('Start Loading Model to MessyMatcher')
        buff = self._model_fs.download_core(self.model_file_name)
        buff.seek(0)
        self._deduper = dedupe.StaticDedupe(buff, num_cores=0, in_memory=True)
        print('Finish Creating dedupe.StaticDedupe of MessyMatcher')
    
    def transform(self, inputs: List[pd.DataFrame], **kwargs) -> List[pd.DataFrame]:
        table = inputs[0]
        print('[MessyPairSelector] table size:', len(table))
        scores = list(map(
            lambda x: [x[0][0], x[0][1], x[1]], 
            filter(
                lambda x: x[1] > self._threshold, 
                    tqdm.tqdm(
                        self._deduper.score(
                            self.organize_pairs(
                                table.to_dict('records')
                            )
                        ),
                        total=len(table), 
                        desc='scoring'
                    )
                )
            )
        )
        print('Finish Score Calculation of Size:', len(scores))
        result = pd.DataFrame(scores, columns=['from', 'to', 'score'])
        return [result]
    
        
    def organize_pairs(self, records: Iterator[Dict]) -> Iterator[Tuple[Dict, Dict]]:
        _a_fields = self.a_fields
        _b_fields = self.b_fields
        for record in records:
            a_json = dict([(key.replace('a_', ''), value) for key, value in record.items() if key in _a_fields])
            b_json = dict([(key.replace('b_', ''), value) for key, value in record.items() if key in _b_fields])
            record_a = (str(record['a_node_id']), a_json)
            record_b = (str(record['b_node_id']), b_json)
            yield record_a, record_b

    @property
    def a_fields(self) -> List[str]:
        return [f'a_{fi}' for fi in self.fields]

    @property
    def b_fields(self) -> List[str]:
        return [f'b_{fi}' for fi in self.fields]
    
    @property
    def fields(self) -> List[str]:
        return [field['field'] for field in self._meta.dedupe_fields]
    
    @property
    def input_ids(self):    
        return [f'{self.label}_entity_map']
    
    @property
    def output_ids(self):
        return [f'{self.label}_id_pairs']

class MessyClusterer(MessyOnly, MatcherBase):
    """Find Connected Components"""
    @property
    def input_ids(self):
        return [f'{self.label}_id_pairs']
    
    def transform(self, inputs: List[pd.DataFrame], **kwargs) -> List[pd.DataFrame]:
        id_pair_df = inputs[0]
        id_pair_df['from'] = id_pair_df['from'].map(int)
        id_pair_df['to'] = id_pair_df['to'].map(int)
        node_set_in_pair = set(id_pair_df['from'].tolist()) | set(id_pair_df['to'].tolist())
        print('node size in pairs:', len(node_set_in_pair))
        g = ig.Graph.TupleList(
            id_pair_df.itertuples(index=False), directed=True, weights=False, edge_attrs="score")
        components = g.connected_components(mode='weak')
        messy2cluster_mapping = []
        for cluster_id, cluster_group in enumerate(components):
            for id in cluster_group:
                messy2cluster_mapping.append(
                    (g.vs[id]['name'], MessyClusterer.do_hash(cluster_id))
                )
        table = pd.DataFrame(messy2cluster_mapping, 
                        columns=['messy_id', 'cluster_id'])
        print('# of Cluster:', cluster_id + 1)
        return [table]
    
    @staticmethod
    def do_hash(cluster_id: int) -> int:
        import ctypes
        return ctypes.c_size_t(hash(f'cluster*id*v1*{cluster_id}')).value