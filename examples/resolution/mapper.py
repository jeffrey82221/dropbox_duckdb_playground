from typing import List
import pandas as pd
import dedupe
from batch_framework.storage import PandasStorage
from batch_framework.filesystem import FileSystem
from batch_framework.etl import SQLExecutor, ETLGroup
from batch_framework.rdb import RDB
from .base import ERBase, Messy2Canon, MessyOnly
from .meta import ERMeta

__all__ = ['MappingGenerator']

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