from typing import List, Iterator, Dict, Tuple
import pandas as pd
import dedupe
from batch_framework.storage import PandasStorage
from batch_framework.filesystem import FileSystem
from batch_framework.etl import SQLExecutor, ETLGroup
from batch_framework.rdb import RDB
from .base import ERBase, Messy2Canon, MessyOnly
from .meta import ERMeta
import tqdm

__all__ = ['MappingGenerator', 
           'MessyFeatureEngineer', 
           'MessyEntityMapGenerator',
           'MessyPairSelector'
           ]

class MappingGenerator(ETLGroup):
    def __init__(self, meta: ERMeta, subgraph_fs: FileSystem, mapping_fs: FileSystem, model_fs: FileSystem, rdb: RDB):
        canon_matcher = CanonMatcher(meta,
            PandasStorage(subgraph_fs), 
            PandasStorage(mapping_fs),
            model_fs=model_fs,
            threshold=0.25
        )
        messy_matcher = MessyMatcher(
            meta,
            PandasStorage(subgraph_fs), 
            PandasStorage(mapping_fs),
            model_fs=model_fs,
            threshold=0.5
        )
        mapping_combiner = MappingCombiner(
            meta,
            rdb,
            workspace_fs=mapping_fs
        )
        super().__init__(
            canon_matcher,
            messy_matcher,
            mapping_combiner
        )
        self._meta = meta

    @property
    def input_ids(self):
        return [
            self._meta.messy_node,
            self._meta.canon_node,
        ]

    @property
    def external_input_ids(self) -> List[str]:
        return self.input_ids
    
    @property
    def output_ids(self):
        return [
            f'mapper_{self._meta.messy_node}' + '_clean'
        ]


class MappingCombiner(SQLExecutor):
    """
    Combine Messy2clean and Messy2Canon Mapping
    """
    def __init__(self, meta: ERMeta, rdb: RDB, workspace_fs: FileSystem):
        self._workspace_fs = workspace_fs
        self._meta = meta
        super().__init__(rdb, input_fs=workspace_fs)
        
    @property
    def input_ids(self):
        return [
            f'mapper_{self._meta.messy_node}2{self._meta.canon_node}',
            f'mapper_{self._meta.messy_node}'
        ]

    @property
    def output_ids(self):
        return [f'mapper_{self._meta.messy_node}' + '_clean']

    def sqls(self):
        return {
            self.output_ids[0]: f"""
                SELECT 
                    t1.messy_id,
                    COALESCE(t2.canon_id, t1.cluster_id) AS new_id
                FROM mapper_{self._meta.messy_node} AS t1
                LEFT JOIN mapper_{self._meta.messy_node}2{self._meta.canon_node} AS t2
                ON t1.messy_id = t2.messy_id
            """
        }


class MatcherBase(ERBase):
    def __init__(self, mapping_meta: ERMeta, input_storage: PandasStorage, output_storage: PandasStorage, model_fs: FileSystem, threshold: float=0.25):
        super().__init__(mapping_meta, input_storage, output_storage, model_fs=model_fs)
        self._threshold = threshold

    @property
    def output_ids(self):
        return [self.mapper_file_name]

class CanonMatcher(Messy2Canon, MatcherBase):
    """Produce Node Mapping using Dedupe Package
    
    Input: messy node data, canon node data, model
    Output: mapping table
    """
    def start(self):
        """Load model setting in the beginning
        """
        print('Start Loading Model to CanonMatcher')
        buff = self._model_fs.download_core(self.model_file_name)
        buff.seek(0)
        self._gazetteer = dedupe.StaticGazetteer(buff)
        print('Finish Creating dedupe.StaticGazetteer of CanonMatcher')

    def transform(self, inputs: List[pd.DataFrame], **kwargs) -> List[pd.DataFrame]:
        # Do Feature Engineering on Input DataFrame
        print('Start Extracting Messy and Canon Feature...')
        messy = self._extract_messy_feature(inputs[0])
        canonical = self._extract_canonical_feature(inputs[1])
        print('Finish Extracting Messy and Canon Feature...')
        self._gazetteer.index(canonical)
        print('Finish Indexing...')
        match_generator = self._gazetteer.search(messy, n_matches=2, generator=True)
        print('Finish Search...')
        messy2canon_mapping = []
        for messy_id, matches in match_generator:
            for canon_id, score in matches:
                if score > self._threshold:
                    messy2canon_mapping.append((messy_id, canon_id, score))
                    # print('matching:', messy[messy_id]['full_name'], '->', canonical[canon_id]['full_name'])
        table = pd.DataFrame(messy2canon_mapping, columns=['messy_id', 'canon_id', 'score'])
        print('# of Messy Data:', len(messy))
        print('# of Canonical Data:', len(canonical))
        print('# Match:', len(table))
        return [table]

import io
import csv
import itertools
class Readable(object):

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
    def transform(self, inputs: List[pd.DataFrame], **kwargs) -> List[pd.DataFrame]:
        table = pd.DataFrame.from_dict(self.feature_generation(inputs[0].to_dict('records')))
        print(table)
        assert len(table) == len(inputs[0])
        return [table]

    def feature_generation(self, records: Iterator[Dict]) -> Iterator[Dict]:
        for record in records:
            result = self._meta.messy_lambda(record)
            result['node_id'] = record['node_id']
            yield result

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
        for field in self._deduper.fingerprinter.index_fields:
            field_data = tuple(table[field].unique().tolist())
            self._deduper.fingerprinter.index(field_data, field)
        b_data = self._deduper.fingerprinter(self.to_fingerprinter(table))
        result = pd.read_csv(Readable(b_data), names = ['block_key', 'messy_id'], header=None)
        print(result)
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
        self._messy_node = meta.messy_node
        super().__init__(rdb, input_fs=fs, output_fs=fs)

    @property
    def input_ids(self):
        return [f'{self._messy_node}_feature', f'{self._messy_node}_block']

    @property
    def output_ids(self):
        return [f'{self._messy_node}_entity_map']
    
    def sqls(self):
        return {
            self.output_ids[0]: f"""
            SELECT 
                a.node_id AS a_node_id,
                {self.get_column_str('a')},
                b.node_id AS b_node_id,
                {self.get_column_str('b')}
            FROM (SELECT DISTINCT l.messy_id AS east, r.messy_id AS west
                    FROM {self._messy_node}_block AS l
                    INNER JOIN {self._messy_node}_block AS r
                    USING (block_key)
                    WHERE l.messy_id < r.messy_id) ids
            INNER JOIN {self._messy_node}_feature a ON ids.east=a.node_id
            INNER JOIN {self._messy_node}_feature b ON ids.west=b.node_id
            """
        }

    def get_column_str(self, prefix: str) -> str:
        return ",".join([f'{prefix}.{field} AS {prefix}_{field}' for field in self.fields])

    @property
    def fields(self) -> List[str]:
        return [field['field'] for field in self._meta.dedupe_fields]


class MessyPairSelector(MessyOnly, MatcherBase):
    def start(self):
        """Load model setting in the beginning
        """
        print('Start Loading Model to MessyMatcher')
        buff = self._model_fs.download_core(self.model_file_name)
        buff.seek(0)
        self._deduper = dedupe.StaticDedupe(buff, num_cores=0, in_memory=True)
        print('Finish Creating dedupe.StaticDedupe of MessyMatcher')
    
    def transform(self, inputs: List[pd.DataFrame], **kwargs) -> List[pd.DataFrame]:
        table = inputs[0] #.head(1000)
        scores = list(map(lambda x: [x[0][0], x[0][1], x[1]], filter(lambda x: x[1] > self._threshold, 
                                                 tqdm.tqdm(self._deduper.score(self.organize_pairs(table.to_dict('records'))), 
                                                           total=len(table), desc='scoring'))))
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
    
"""Find Connected Components: 
# find connected_components
print(scores[0])
raise
clustered_dupes = self._deduper.cluster(
    scores,
    threshold=self._threshold
)
print('Finish Clustering...')
messy2cluster_mapping = []
for cluster_id, (records, scores) in tqdm.tqdm(enumerate(clustered_dupes), total=len(clustered_dupes), desc='cluster_mapping'):
    for messy_id, score in zip(records, scores):
        messy2cluster_mapping.append(
            (messy_id, f'c{cluster_id}', score)
        )
table = pd.DataFrame(messy2cluster_mapping, 
                        columns=['messy_id', 'cluster_id', 'score'])
print('# of Cluster:', len(clustered_dupes))
return [table]
"""


class MessyMatcher(MessyOnly, MatcherBase):
    """Produce Node Mapping using Dedupe Package
    
    Input: messy node data, model
    Output: mapping table
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
        # Do Feature Engineering on Input DataFrame
        print('Start Extracting Messy Feature...')
        messy = self._extract_messy_feature(inputs[0])
        print(f'Finish Extracting {len(messy)} Messy Feature...')
        clustered_dupes = self._deduper.partition(messy, self._threshold)
        print('Finish Clustering...')
        messy2cluster_mapping = []
        for cluster_id, (records, scores) in enumerate(clustered_dupes):
            for messy_id, score in zip(records, scores):
                messy2cluster_mapping.append((messy_id, f'c{cluster_id}', score))
                if len(records) >= 2:
                    print('matching:', messy[messy_id]['full_name'], '->', cluster_id)
        table = pd.DataFrame(messy2cluster_mapping, columns=['messy_id', 'cluster_id', 'score'])
        print('# of Messy Data:', len(messy))
        print('# of Cluster:', len(clustered_dupes))
        return [table]