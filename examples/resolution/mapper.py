from typing import List
import pandas as pd
import dedupe
from batch_framework.storage import PandasStorage
from batch_framework.filesystem import FileSystem
from .base import ERBase, Messy2Canon, MessyOnly
from .meta import ERMeta

__all__ = ['CanonMatcher', 'MessyMatcher']


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
        buff = self._model_fs.download_core(self.model_file_name)
        buff.seek(0)
        self._gazetteer = dedupe.StaticGazetteer(buff)

    def transform(self, inputs: List[pd.DataFrame], **kwargs) -> List[pd.DataFrame]:
        # Do Feature Engineering on Input DataFrame
        messy = self._extract_messy_feature(inputs[0])
        canonical = self._extract_canonical_feature(inputs[1])
        self._gazetteer.index(canonical)
        match_generator = self._gazetteer.search(messy, n_matches=2, generator=True)
        messy2canon_mapping = []
        for messy_id, matches in match_generator:
            for canon_id, score in matches:
                if score > self._threshold:
                    messy2canon_mapping.append((messy_id, canon_id, score))
                    print('matching:', messy[messy_id]['full_name'], '->', canonical[canon_id]['full_name'])
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
        buff = self._model_fs.download_core(self.model_file_name)
        buff.seek(0)
        self._deduper = dedupe.StaticDedupe(buff)

    def transform(self, inputs: List[pd.DataFrame], **kwargs) -> List[pd.DataFrame]:
        # Do Feature Engineering on Input DataFrame
        messy = self._extract_messy_feature(inputs[0])
        clustered_dupes = self._deduper.partition(messy, self._threshold)
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