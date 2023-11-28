from typing import List
import pandas as pd
import dedupe
from ..base import MessyOnly
from .base import MatcherBase

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