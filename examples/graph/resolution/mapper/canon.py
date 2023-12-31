"""
TODO:
    - [X] Understand and Tune n_matches in gazeteer.search
    - [ ] Reduce Daily Operation
        - [ ] Seperate Operation into
            - [ ] 1. Seperate Input:
                - [ ] (1) OldMessy + NewCanon
                - [ ] (2) OldCanon + NewMessy
                - [ ] (3) NewCanon + OldCanon
            - [ ] 2. Run CanonMatcher on (1-3)
            - [ ] 3. Append result of (1-3) and distinct on (messy_id) select one with largest similarity
            - [ ] 4. Feedback the final table

    - [ ] Refactor:
        - [X] Decompose CanonMatcher into MessyFeatureEngineer / CanonFeatureEngineer / Pairing
        - [ ] Add FeatureFeedback class to pass old canon and old messy features
        - [ ] Add Final mapping table Feedback Module
        - [ ] Add Divide class to seperate old canon and new canon, old messy and new messy
        - [ ] Build up old-canon-new-messy, old-messy-new-canon, new-canon-new-messy Pairing Flow
        - [ ] Add Merge class to merge the output of these three flow.

"""
from typing import List, Dict, Iterator
import pandas as pd
import dedupe
from batch_framework.etl import ETLGroup
from batch_framework.storage import PandasStorage
from batch_framework.filesystem import FileSystem
from ..base import Messy2Canon
from .base import MatcherBase
from ..meta import ERMeta

__all__ = ['CanonMatcher']


class MessyFeatureEngineer(Messy2Canon, MatcherBase):
    @property
    def input_ids(self):
        return [self._meta.messy_node]

    @property
    def output_ids(self):
        return [self._meta.messy_node + '_m2c_feature']

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


class CanonFeatureEngineer(Messy2Canon, MatcherBase):
    @property
    def input_ids(self):
        return [self._meta.canon_node]

    @property
    def output_ids(self):
        return [self._meta.canon_node + '_m2c_feature']

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
            result = self._meta.canon_lambda(record)
            result['node_id'] = record['node_id']
            yield result


class Pairer(Messy2Canon, MatcherBase):
    def __init__(self, *args, **kwargs):
        kwargs['make_cache'] = True
        super().__init__(*args, **kwargs)

    @property
    def input_ids(self):
        return [self._meta.messy_node + '_m2c_feature',
                self._meta.canon_node + '_m2c_feature']

    def start(self):
        """Load model setting in the beginning
        """
        print('Start Loading Model to CanonMatcher')
        buff = self._model_fs.download_core(self.model_file_name)
        buff.seek(0)
        self._gazetteer = dedupe.StaticGazetteer(buff)
        print('Finish Creating dedupe.StaticGazetteer of CanonMatcher')

    @staticmethod
    def dict_to_input(input_item):
        node_id = input_item['node_id']
        del input_item['node_id']
        return str(node_id), input_item

    def transform(self, inputs: List[pd.DataFrame],
                  **kwargs) -> List[pd.DataFrame]:
        if self.exists_cache:
            # Load Cache
            feedback_messy_ids = set(self.load_cache(
                self.input_ids[0])['node_id'].tolist())
            feedback_canon_ids = set(self.load_cache(
                self.input_ids[1])['node_id'].tolist())
            feedback_table = self.load_cache(self.output_ids[0])
            print('Cache Loaded')
            messy_df = inputs[0]
            canon_df = inputs[1]
            messy_ids = set(messy_df.node_id.tolist())
            canon_ids = set(canon_df.node_id.tolist())
            new_messy_ids = messy_ids - feedback_messy_ids
            new_canon_ids = canon_ids - feedback_canon_ids
            old_messy_ids = messy_ids & feedback_messy_ids
            old_canon_ids = canon_ids & feedback_canon_ids
            old_messy_df = messy_df[messy_df.node_id.map(
                lambda x: x in old_messy_ids)]
            new_messy_df = messy_df[messy_df.node_id.map(
                lambda x: x in new_messy_ids)]
            old_canon_df = canon_df[canon_df.node_id.map(
                lambda x: x in old_canon_ids)]
            new_canon_df = canon_df[canon_df.node_id.map(
                lambda x: x in new_canon_ids)]
            print('old_messy_df:', len(old_messy_df))
            print('new_messy_df:', len(new_messy_df))
            print('old_canon_df:', len(old_canon_df))
            print('new_canon_df:', len(new_canon_df))
            print('updated feedback table:', len(feedback_table))
            tables = [feedback_table]
            if len(new_canon_df):
                print('Start Pairing Old Messy to New Canon...')
                old_new = self.match_tables([old_messy_df, new_canon_df])[0]
                tables.append(old_new)
            if len(new_messy_df):
                print('Start Pairing New Messy to Old Canon...')
                new_old = self.match_tables([new_messy_df, old_canon_df])[0]
                tables.append(new_old)
            if len(new_canon_df) > 0 and len(new_messy_df) > 0:
                print('Start Pairing New Messy to New Canon...')
                new_new = self.match_tables([new_messy_df, new_canon_df])[0]
                tables.append(new_new)
            final_table = pd.concat(tables, axis=0)
            print('# final_table (before groupby):', len(final_table))
            final_table.sort_values('score', ascending=False, inplace=True)
            final_table.drop_duplicates(
                subset=['messy_id'], keep='first', inplace=True)
            print('# final_table (after groupby):', len(final_table))
            results = [final_table]
            return results
        else:
            messy_df = inputs[0]
            canon_df = inputs[1]
            messy_ids = set(messy_df.node_id.tolist())
            canon_ids = set(canon_df.node_id.tolist())
            results = self.match_tables([messy_df, canon_df])
            return results

    def match_tables(
            self, inputs: List[pd.DataFrame], **kwargs) -> List[pd.DataFrame]:
        messy = dict([Pairer.dict_to_input(item)
                     for item in inputs[0].to_dict('records')])
        canonical = dict([Pairer.dict_to_input(item)
                         for item in inputs[1].to_dict('records')])
        print(
            f'Finish Extracting Messy ({len(messy)}) and Canon ({len(canonical)}) Feature...')
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
        final_table = pd.DataFrame(
            messy2canon_mapping, columns=[
                'messy_id', 'canon_id', 'score'])
        print('# final_table (before groupby):', len(final_table))
        final_table.sort_values('score', ascending=False, inplace=True)
        final_table.drop_duplicates(
            subset=['messy_id'], keep='first', inplace=True)
        print('# final_table (after groupby):', len(final_table))
        print('Finish Matching...')
        self.start()
        return [final_table]


class CanonMatcher(ETLGroup, Messy2Canon, MatcherBase):
    def __init__(self, mapping_meta: ERMeta, input_storage: PandasStorage,
                 output_storage: PandasStorage, model_fs: FileSystem, threshold: float = 0.25):
        tmp_storage = PandasStorage(input_storage._backend)
        mfe = MessyFeatureEngineer(
            mapping_meta, input_storage, tmp_storage, model_fs=model_fs, threshold=threshold
        )
        cfe = CanonFeatureEngineer(
            mapping_meta, input_storage, tmp_storage, model_fs=model_fs, threshold=threshold
        )
        pair = Pairer(
            mapping_meta,
            tmp_storage,
            output_storage,
            model_fs=model_fs,
            threshold=threshold)
        super().__init__(mfe, cfe, pair)

    @property
    def input_ids(self):
        return self.etl_units[0].input_ids + self.etl_units[1].input_ids

    @property
    def output_ids(self):
        return self.etl_units[-1].output_ids
