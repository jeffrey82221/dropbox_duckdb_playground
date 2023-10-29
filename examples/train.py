"""
Active Learning How two node merge or not. 

- inputs: messy data, canonical data, training_data, console input
- outputs: training_data, model_setting 
"""
from typing import List, Dict
import pandas as pd
import dedupe
import io
import json
from batch_framework.etl import DFProcessor
from batch_framework.storage import PandasStorage, JsonStorage
from batch_framework.filesystem import LocalBackend

class MappingTrainer(DFProcessor):
    def __init__(self, input_storage: PandasStorage, output_storage: JsonStorage):
        super().__init__(input_storage, output_storage)
        self._gazetteer = dedupe.Gazetteer([
            {'field': 'name', 'type': 'String'}
        ])

    @property
    def input_ids(self):
        return [
            'requirement',
            'package'
        ]

    @property
    def output_ids(self):
        return ['train.json']
    
    def start(self):
        if self._output_storage._backend.check_exists('train.json'):
            self._training_file = self._output_storage._backend.download_core('train.json')
            self._training_file.seek(0)
        else:
            self._training_file = None

    def transform(self, inputs: List[pd.DataFrame], **kwargs) -> List[Dict]:
        messy = self._convert_df(inputs[0])
        canonical = self._convert_df(inputs[1])
        self._gazetteer.prepare_training(messy, canonical, training_file=self._training_file)
        dedupe.console_label(self._gazetteer)
        self._gazetteer.train()
        updated_train = io.StringIO()
        self._gazetteer.write_training(updated_train)
        updated_train.seek(0)
        train_data = json.loads(updated_train.read())
        print('Generated Training Data Have Type:', type(train_data))
        self._model_file = io.BytesIO()
        self._gazetteer.write_settings(self._model_file)
        return [train_data]

    def end(self):
        self._model_file.seek(0)
        self._output_storage._backend.upload_core(self._model_file, 'model')
        print('Model Uploaded')
        
    
    def _convert_df(self, df: pd.DataFrame) -> Dict:
        results = []
        for record in df.to_dict('records'):
            results.append((record['node_id'], {'name': record['name']}))
        return dict(results)

if __name__ == '__main__':
    input_fs = LocalBackend('./data/subgraph/output/')
    output_fs = LocalBackend('./data/model/')
    op = MappingTrainer(PandasStorage(input_fs), JsonStorage(output_fs))
    op.execute()