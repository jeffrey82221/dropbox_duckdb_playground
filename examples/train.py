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
            {'field': 'full_name', 'type': 'String'},
            {'field': 'before_whitespace', 'type': 'Exact'},
            {'field': 'before_upper_bracket', 'type': 'Exact'},
            {'field': 'before_marks', 'type': 'Exact'}
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
            print('Training Data Loaded')
        else:
            self._training_file = None

    def transform(self, inputs: List[pd.DataFrame], **kwargs) -> List[Dict]:
        messy = self._convert_messy(inputs[0])
        canonical = self._convert_canonical(inputs[1])
        print('# of Messy Data:', len(messy))
        print('# of Canonical Data:', len(canonical))
        self._gazetteer.prepare_training(messy, canonical, training_file=self._training_file)
        dedupe.console_label(self._gazetteer)
        self._gazetteer.train()
        updated_train = io.StringIO()
        self._gazetteer.write_training(updated_train)
        updated_train.seek(0)
        train_data = json.loads(updated_train.read())
        self._model_file = io.BytesIO()
        self._gazetteer.write_settings(self._model_file)
        return [train_data]

    def end(self):
        self._model_file.seek(0)
        self._output_storage._backend.upload_core(self._model_file, 'model')
        print('Model Uploaded')
        
    
    def _convert_messy(self, df: pd.DataFrame) -> Dict:
        results = []
        for record in df.to_dict('records'):
            results.append((record['node_id'], {
                'full_name': record['name'],
                'before_whitespace': record['name'].split(' ')[0].split(';')[0],
                'before_upper_bracket': record['name'].split('[')[0],
                'before_marks': record['name'].split('<')[0].split('>')[0].split('=')[0]
            }))
        return dict(results)

    def _convert_canonical(self, df: pd.DataFrame) -> Dict:
        results = []
        for record in df.to_dict('records'):
            results.append((record['node_id'], {
                'full_name': record['name'],
                'before_whitespace': record['name'],
                'before_upper_bracket': record['name'],
                'before_marks': record['name']
            }))
        return dict(results)

if __name__ == '__main__':
    input_fs = LocalBackend('./data/subgraph/output/')
    output_fs = LocalBackend('./data/model/')
    op = MappingTrainer(PandasStorage(input_fs), JsonStorage(output_fs))
    op.execute()