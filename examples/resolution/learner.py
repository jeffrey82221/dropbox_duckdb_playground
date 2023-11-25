import abc
from typing import List, Dict
import pandas as pd
import dedupe
import json
import io
from batch_framework.storage import PandasStorage, JsonStorage
from batch_framework.filesystem import FileSystem
from .base import ERBase, Messy2Canon, MessyOnly
from .meta import ERMeta

__all__ = ['CanonMatchLearner', 'MessyMatchLearner']

class MatchLearnerBase(ERBase):
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
        while True:
            dedupe.console_label(self._deduper)
            self._deduper.train(recall=0.9, index_predicates=True)
            print('Finish Training')
            fields = [field for field in self._deduper.fingerprinter.index_fields]
            print('FingerPrinting Fields:', fields)
            if len(fields) > 0:
                break
            else:
                continue
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

class CanonMatchLearner(Messy2Canon, MatchLearnerBase):
    """Learning Mapping Rule using Dedupe Package

    Input: messy node data, canon node data, previous training data
    Output: model, updated training data
    """
    def __init__(self, meta: ERMeta, input_storage: PandasStorage, output_storage: JsonStorage, model_fs: FileSystem):
        super().__init__(meta, input_storage, output_storage, model_fs=model_fs)
        self._deduper = dedupe.Gazetteer(self._meta.dedupe_fields)
    
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

class MessyMatchLearner(MessyOnly, MatchLearnerBase):
    """Learning how to custer messy node

    Input: messy node data, previous training data
    Output: model, updated training data
    """
    def __init__(self, meta: ERMeta, input_storage: PandasStorage, output_storage: JsonStorage, model_fs: FileSystem):
        super().__init__(meta, input_storage, output_storage, model_fs=model_fs)
        self._deduper = dedupe.Dedupe(self._meta.dedupe_fields)
    
    def prepare_training(self, inputs: List[pd.DataFrame]):
        """Prepare Active Learning with Old Training Data

        Args:
            - inputs[0]: messy node dataframe
        """
        messy = self._extract_messy_feature(inputs[0])
        print('# of Messy Data:', len(messy))
        self._deduper.prepare_training(messy, training_file=self._training_file)

    
