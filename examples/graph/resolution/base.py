import abc
from typing import Dict
import pandas as pd
from batch_framework.etl import ObjProcessor
from batch_framework.storage import PandasStorage, JsonStorage
from batch_framework.filesystem import FileSystem
from .meta import ERMeta

__all__ = ['ERBase', 'Messy2Canon', 'MessyOnly']


class ERBase(ObjProcessor):
    """
    Base class for building
    NodeMappingLearner & NodeMappingProducer
    """

    def __init__(self, meta: ERMeta, input_storage: PandasStorage,
                 output_storage: JsonStorage, model_fs: FileSystem, make_cache: bool=False):
        self._meta = meta
        self._model_fs = model_fs
        super().__init__(input_storage, output_storage, make_cache=make_cache)

    @property
    def messy_node(self):
        return self._meta.messy_node

    @property
    def canon_node(self):
        return self._meta.canon_node

    @property
    def dedupe_fields(self):
        return self._meta.dedupe_fields

    def _extract_messy_feature(self, df: pd.DataFrame) -> Dict[str, Dict]:
        results = []
        for record in df.to_dict('records'):
            results.append(
                (str(
                    record['node_id']),
                    self._meta.messy_lambda(record)))
        return dict(results)

    def _extract_canonical_feature(self, df: pd.DataFrame) -> Dict:
        results = []
        for record in df.to_dict('records'):
            results.append(
                (str(
                    record['node_id']),
                    self._meta.canon_lambda(record)))
        return dict(results)

    @property
    def train_file_name(self):
        return f'{self.label}_train'

    @property
    def model_file_name(self):
        return f'{self.label}.model'

    @property
    def mapper_file_name(self):
        return f'mapper_{self.label}'

    @abc.abstractproperty
    def label(self):
        raise NotImplementedError


class Messy2Canon:
    @property
    def input_ids(self):
        return [
            self.messy_node,
            self.canon_node
        ]

    @property
    def label(self):
        return f'{self.messy_node}2{self.canon_node}'


class MessyOnly:
    @property
    def input_ids(self):
        return [
            self.messy_node
        ]

    @property
    def label(self):
        return self.messy_node
