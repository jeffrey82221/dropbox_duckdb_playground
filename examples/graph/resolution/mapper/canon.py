"""
TODO:
    - [ ] Understand and Tune n_matches in gazeteer.search
    - [ ] Allow Map Reduce to reduce memory usage.
"""
from typing import List
import pandas as pd
import dedupe
from ..base import Messy2Canon
from .base import MatcherBase

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
        match_generator = self._gazetteer.search(messy, n_matches=2, 
                                                 generator=True)
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