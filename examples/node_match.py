"""
Mapping Similar Nodes. 

TODO:
- [ ] Build Threshold Tunner
- [X] Build Learning ETL
- [X] Build Mapping Table ETL
"""
import abc
from typing import List, Dict, Callable
import pandas as pd
import dedupe
import io
import json
from batch_framework.etl import DFProcessor
from batch_framework.storage import PandasStorage, JsonStorage
from batch_framework.filesystem import LocalBackend, FileSystem

class MappingMeta:
    def __init__(self, messy_node: str, canon_node: str, dedupe_fields: List[Dict[str, str]], messy_lambda: Callable=lambda record: record, canon_lambda: Callable=lambda record: record):
        self.messy_node = messy_node
        self.canon_node = canon_node
        self.dedupe_fields = dedupe_fields
        self.messy_lambda = messy_lambda
        self.canon_lambda = canon_lambda

class NodeMappingBase(DFProcessor):
    """Base class for building NodeMappingLearner & NodeMappingProducer
    """
    def __init__(self, mapping_meta: MappingMeta, input_storage: PandasStorage, output_storage: JsonStorage, model_fs: FileSystem):
        self._mapping_meta = mapping_meta
        self._model_fs = model_fs
        super().__init__(input_storage, output_storage)

    @property
    def messy_node(self):
        return self._mapping_meta.messy_node
    
    @property
    def canon_node(self):
        return self._mapping_meta.canon_node

    @property
    def dedupe_fields(self):
        return self._mapping_meta.dedupe_fields

    def _extract_messy_feature(self, df: pd.DataFrame) -> Dict:
        results = []
        for record in df.to_dict('records'):
            results.append((str(record['node_id']), self._mapping_meta.messy_lambda(record)))
        return dict(results)

    def _extract_canonical_feature(self, df: pd.DataFrame) -> Dict:
        results = []
        for record in df.to_dict('records'):
            results.append((str(record['node_id']), self._mapping_meta.canon_lambda(record)))
        return dict(results)

    @property
    def input_ids(self):
        return [
            self.messy_node,
            self.canon_node
        ]

    @property
    def model_file_name(self):
        return f'{self._label}.model'
    
    @property
    def _label(self):
        return f'{self.messy_node}2{self.canon_node}'

class MatchLearnerBase(NodeMappingBase):
    """
    Base class of NodeMappingLearner and MessyMatchingLearner
    """
    @property
    def output_ids(self):
        return [self.train_file_name]

    def start(self):
        """
        Load old training data in the beginning
        """
        if self._output_storage._backend.check_exists(self.train_file_name):
            self._training_file = self._output_storage._backend.download_core(self.train_file_name)
            self._training_file.seek(0)
            print(f'Training Data Loaded from {self.train_file_name}')
        else:
            self._training_file = None

    def transform(self, inputs: List[pd.DataFrame], **kwargs) -> List[Dict]:
        self.prepare_training(inputs)
        dedupe.console_label(self._deduper)
        self._deduper.train()
        # Generate JSON format Training Data
        updated_train = io.StringIO()
        self._deduper.write_training(updated_train)
        updated_train.seek(0)
        train_data = json.loads(updated_train.read())
        return [train_data]
    
    @abc.abstractmethod
    def prepare_training(self, inputs: List[pd.DataFrame]):
        raise NotImplementedError
    
    def end(self):
        """
        Saving Model in the end
        """
        model_file = io.BytesIO()
        self._deduper.write_settings(model_file)
        model_file.seek(0)
        self._model_fs.upload_core(model_file, self.model_file_name)
        print(f'Model Uploaded As {self.model_file_name}')
    
class MessyMatchingLearner(MatchLearnerBase):
    """Learning how to custer messy node

    Input: messy node data, previous training data
    Output: model, updated training data
    """
    def __init__(self, mapping_meta: MappingMeta, input_storage: PandasStorage, output_storage: JsonStorage, model_fs: FileSystem):
        super().__init__(mapping_meta, input_storage, output_storage, model_fs=model_fs)
        self._deduper = dedupe.Dedupe(self._mapping_meta.dedupe_fields)
    
    @property
    def train_file_name(self):
        return f'{self.messy_node}.json'

    def prepare_training(self, inputs: List[pd.DataFrame]):
        """Prepare Active Learning with Old Training Data

        Args:
            - inputs[0]: messy node dataframe
        """
        messy = self._extract_messy_feature(inputs[0])
        print('# of Messy Data:', len(messy))
        self._deduper.prepare_training(messy, training_file=self._training_file)

class NodeMappingLearner(MatchLearnerBase):
    """Learning Mapping Rule using Dedupe Package

    Input: messy node data, canon node data, previous training data
    Output: model, updated training data
    """
    def __init__(self, mapping_meta: MappingMeta, input_storage: PandasStorage, output_storage: JsonStorage, model_fs: FileSystem):
        super().__init__(mapping_meta, input_storage, output_storage, model_fs=model_fs)
        self._deduper = dedupe.Gazetteer(self._mapping_meta.dedupe_fields)

    @property
    def train_file_name(self):
        return f'{self._label}.json'
    
    def prepare_training(self, inputs: List[pd.DataFrame]):
        """Prepare Active Learning with Old Training Data
        Args:
            inputs[0]: messy node dataframe 
            inputs[1]: canonical node dataframe
        """
        messy = self._extract_messy_feature(inputs[0])
        canonical = self._extract_canonical_feature(inputs[1])
        print('# of Messy Data:', len(messy))
        print('# of Canonical Data:', len(canonical))
        self._deduper.prepare_training(messy, canonical, training_file=self._training_file)

class NodeMappingProducer(NodeMappingBase):
    """Produce Node Mapping using Dedupe Package
    
    Input: messy node data, canon node data, model
    Output: mapping table
    """
    def __init__(self, mapping_meta: MappingMeta, input_storage: PandasStorage, output_storage: JsonStorage, model_fs: FileSystem, threshold: float=0.25):
        super().__init__(mapping_meta, input_storage, output_storage, model_fs=model_fs)
        self._threshold = threshold

    @property
    def output_ids(self):
        return [self._label]
    
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

if __name__ == '__main__':
    input_fs = LocalBackend('./data/subgraph/output/')
    train_fs = LocalBackend('./data/train/')
    model_fs = LocalBackend('./data/model/')
    mapping_fs = LocalBackend('./data/mapping/')
    meta = MappingMeta(
        messy_node='requirement',
        canon_node='package',
        dedupe_fields=[
            {'field': 'full_name', 'type': 'String'},
            {'field': 'before_whitespace', 'type': 'Exact'},
            {'field': 'before_upper_bracket', 'type': 'Exact'},
            {'field': 'before_marks', 'type': 'Exact'}
        ],
        messy_lambda=lambda record: {
            'full_name': record['name'],
            'before_whitespace': record['name'].split(' ')[0].split(';')[0],
            'before_upper_bracket': record['name'].split('[')[0].split('(')[0],
            'before_marks': record['name'].split('<')[0].split('>')[0].split('=')[0].split('~')[0]
        },
        canon_lambda=lambda record: {
            'full_name': record['name'],
            'before_whitespace': record['name'],
            'before_upper_bracket': record['name'],
            'before_marks': record['name']
        }
    )
    op1 = NodeMappingLearner(meta, 
            PandasStorage(input_fs), 
            JsonStorage(train_fs),
            model_fs=model_fs
        )
    op2 = MessyMatchingLearner(
        meta, 
        PandasStorage(input_fs), 
        JsonStorage(train_fs),
        model_fs=model_fs
    )
    op3 = NodeMappingProducer(meta,
        PandasStorage(input_fs), 
        PandasStorage(mapping_fs),
        model_fs=model_fs
    )
    # TODO:
    # - [ ] Get mapping 1 ( messy node -> canon node )
    # - [ ] Get remain messy node 
    # - [ ] Get mapping 2 ( messy node -> cluster id)
    # - [ ] Combine mapping 1 & mapping 2
    # - [ ] Do mapping ( messy node -> canon / cluster node )
    # - [ ] Build merging layer