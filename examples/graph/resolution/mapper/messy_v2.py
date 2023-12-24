from typing import Iterator, Tuple, List, Dict
import pandas as pd
import dedupe
import tqdm
import io
import csv
import itertools
import igraph as ig
from pathos.multiprocessing import Pool
from batch_framework.filesystem import FileSystem
from batch_framework.storage import PandasStorage
from batch_framework.etl import SQLExecutor, ETLGroup
from batch_framework.rdb import RDB
from .base import MatcherBase
from ..base import MessyOnly
from ..meta import ERMeta


class MessyMatcher(ETLGroup):
    """
    Input Messy Node Table
    Output a Messy->Cluster Mapping Table
    """

    def __init__(self, meta: ERMeta, subgraph_fs: FileSystem, mapping_fs: FileSystem,
                 model_fs: FileSystem, rdb: RDB, threshold=0.5, take_filtered=True):
        self._mapping_fs = mapping_fs
        self._take_filtered = take_filtered
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
        messy_pair_selector = MessyPairSelector(
            meta,
            PandasStorage(mapping_fs),
            PandasStorage(mapping_fs),
            model_fs=model_fs,
            threshold=threshold
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

    def __init__(self, mapping_meta: ERMeta, input_storage: PandasStorage, output_storage: PandasStorage,
                 model_fs: FileSystem, threshold: float = 0.25, take_filtered=True):
        self._take_filtered = take_filtered
        super().__init__(mapping_meta, input_storage, output_storage, model_fs=model_fs)
        self._threshold = threshold

    def transform(self, inputs: List[pd.DataFrame],
                  **kwargs) -> List[pd.DataFrame]:
        table = pd.DataFrame.from_dict(
            self.feature_generation(
                inputs[0].to_dict('records')))
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

    def end(self):
        self._deduper.fingerprinter.reset_indices()

    def transform(self, inputs: List[pd.DataFrame],
                  **kwargs) -> List[pd.DataFrame]:
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
            names=['block_key', 'messy_id'],
            header=None
        )
        print('result size of messy blocker:', len(result))
        assert len(result) > 0, 'messy block result size = 0'
        origin_node_ids = set(inputs[0].node_id.tolist())
        processed_node_ids = set(result.messy_id.tolist())
        print('common node ids:', len(origin_node_ids & processed_node_ids))
        assert len(
            origin_node_ids & processed_node_ids) > 0, 'messy block size = 0'
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
        return ",".join(
            [f'{prefix}.{field} AS {prefix}_{field}' for field in self.fields])

    @property
    def fields(self) -> List[str]:
        return [field['field'] for field in self._meta.dedupe_fields]


class MessyEntityMapValidate(MessyOnly, MatcherBase):
    """
    Validate the ID consistency of input tables
    """
    @property
    def input_ids(self):
        return [f'{self.label}_feature',
                f'{self.label}_entity_map', f'{self.label}_id_pairs']

    @property
    def output_ids(self):
        return []

    def transform(self, inputs: List[pd.DataFrame],
                  **kwargs) -> List[pd.DataFrame]:
        feature_table = inputs[0]
        entity_map_table = inputs[1]
        id_pairs_table = inputs[2]
        feature_node_ids = set(feature_table.node_id.tolist())
        block_table_nodes = set(
            entity_map_table.a_node_id.tolist()) | set(
            entity_map_table.b_node_id.tolist())
        print('common node ids:', len(feature_node_ids & block_table_nodes))
        id_pairs_table['from'] = id_pairs_table['from'].map(int)
        id_pairs_table['to'] = id_pairs_table['to'].map(int)
        id_pairs_nodes = set(
            id_pairs_table['from'].tolist()) | set(
            id_pairs_table['to'].tolist())
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
        self._deduper = dedupe.StaticDedupe(buff, num_cores=4, in_memory=True)
        print('Finish Creating dedupe.StaticDedupe of MessyMatcher')

    def transform(self, inputs: List[pd.DataFrame],
                  **kwargs) -> List[pd.DataFrame]:
        """
        TODO:
            - [X] Convert table.to_dict() to batch by batch output
            - [X] Do self.organize_pairs, score, filter, map batch by batch
            - [X] Collect Result Batch by Batch
            - [X] Add Cache of Input
            - [X] Add Cache of Output
            - [X] Do Data Split by Input Cache
                - [X] FIXME:
                    - [X] Old Pairing Is Defined by Prvious a_node_id, b_node_id combination
                    - [X] New Pairing Are Those not in Old Pairing
            - [X] Do Multiple Stage of Pairing
            - [X] Combine Pairing Result with Output Cache
        """
        table = inputs[0]
        print('[MessyPairSelector] table size:', len(table))
        if self.exists_cache:
            feedback_input = self.load_cache(self.input_ids[0])
            feedback_input['id_pairs'] = feedback_input.a_node_id.map(
                str) + feedback_input.b_node_id.map(str)
            table['id_pairs'] = table.a_node_id.map(
                str) + table.b_node_id.map(str)
            old_id_pairs = set(feedback_input['id_pairs'].tolist())
            print('# old_id_pairs:', len(old_id_pairs))
            old_condi = table['id_pairs'].map(lambda x: x in old_id_pairs)
            new_table = table[~old_condi]
            print('# new_table:', len(new_table))
            del new_table['id_pairs']
            if len(new_table) > 0:
                new_result = self.do_pairing(new_table)
                feedback_result = self.load_cache(self.output_ids[0])
                print('# feedback_result:', len(feedback_result))
                print('# new_result:', len(new_result))
                result = pd.concat([feedback_result, new_result])
                print('Done Append New Pairs')
            else:
                result = self.load_cache(self.output_ids[0])
                print('Done Loading New Pairs')
        else:
            result = self.do_pairing(table)
            print('Done Creating Pairs')
        result.sort_values('score', ascending=False, inplace=True)
        result.drop_duplicates(subset=['from', 'to'], inplace=True)
        print('# Final Result:', len(result))
        return [result]

    def do_pairing(self, table: pd.DataFrame) -> pd.DataFrame:
        pairs_with_score = self.calculate_scores(
            table, batch_size=100, worker_cnt=8)
        pairs = list(
            filter(lambda x: x[-1] > self._threshold, pairs_with_score))
        print('Finish Pairs:', len(pairs))
        result = pd.DataFrame(pairs, columns=['from', 'to', 'score'])
        return result

    def save_cache(self):
        for input_id in self.input_ids:
            self._input_storage.upload(
                self._input_storage.download(input_id),
                input_id + '_cache')
            print(input_id + '_cache', 'saved')
        for output_id in self.output_ids:
            self._output_storage.upload(
                self._output_storage.download(output_id),
                output_id + '_cache')
            print(output_id + '_cache', 'saved')

    def load_cache(self, id: str) -> pd.DataFrame:
        if id in self.input_ids:
            return self._input_storage.download(id + '_cache')
        elif id in self.output_ids:
            return self._output_storage.download(id + '_cache')
        else:
            raise ValueError(
                'id to be loaded in load_cache should be in self.input_ids or self.output_ids')

    @property
    def exists_cache(self) -> bool:
        for id in self.input_ids:
            if not self._input_storage.check_exists(id + '_cache'):
                return False
        for id in self.output_ids:
            if not self._output_storage.check_exists(id + '_cache'):
                return False
        return True

    def end(self):
        self.save_cache()

    def calculate_scores(self, table: pd.DataFrame, batch_size: int,
                         worker_cnt: int) -> Iterator[Tuple[str, str, float]]:
        total = len(table)
        batch_generator = MessyPairSelector.batch(
            table.to_dict('records'), n=batch_size)
        batch_pipe = tqdm.tqdm(
            batch_generator,
            desc='calculate scores between messy items',
            total=total // batch_size)
        with Pool(worker_cnt) as pool:
            batch_pipe = pool.imap(self.process_batchwise, batch_pipe)
            for batch in batch_pipe:
                for item in batch:
                    yield item
            pool.close()
            pool.join()

    def process_batchwise(self, batch: List) -> List:
        record_pairs = self.organize_pairs(batch)
        record_ids, records = zip(*(zip(*record_pair)
                                  for record_pair in record_pairs))
        featurizer = self._deduper.data_model.distances
        classifier = self._deduper.classifier
        features = featurizer(records)
        scores = classifier.predict_proba(features)[:, -1]
        output = [(ids[0], ids[1], score)
                  for ids, score in zip(record_ids, scores)]
        return output

    @staticmethod
    def batch(iterable: Iterator, n=1):
        l = len(iterable)
        for ndx in range(0, l, n):
            yield iterable[ndx:min(ndx + n, l)]

    def organize_pairs(
            self, records: Iterator[Dict]) -> Iterator[Tuple[Dict, Dict]]:
        _a_fields = self.a_fields
        _b_fields = self.b_fields
        for record in records:
            a_json = dict([(key.replace('a_', ''), value)
                          for key, value in record.items() if key in _a_fields])
            b_json = dict([(key.replace('b_', ''), value)
                          for key, value in record.items() if key in _b_fields])
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

    def transform(self, inputs: List[pd.DataFrame],
                  **kwargs) -> List[pd.DataFrame]:
        id_pair_df = inputs[0]
        id_pair_df['from'] = id_pair_df['from'].map(int)
        id_pair_df['to'] = id_pair_df['to'].map(int)
        node_set_in_pair = set(
            id_pair_df['from'].tolist()) | set(
            id_pair_df['to'].tolist())
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
