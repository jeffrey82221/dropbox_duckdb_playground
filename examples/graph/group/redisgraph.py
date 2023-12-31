"""
Building CSV to be load into RedisGraph
"""
from typing import List
import pandas as pd
import csv
from batch_framework.etl import ObjProcessor
from batch_framework.storage import PandasStorage
from batch_framework.filesystem import FileSystem

TYPE_CONVERT = {
    'object': 'STRING',
    'float64': 'FLOAT'
}


class FormatNode(ObjProcessor):
    def __init__(self, node_name: str, input_storage: PandasStorage,
                 output_fs: FileSystem):
        self._node_name = node_name
        self._output_fs = output_fs
        super().__init__(input_storage)

    @property
    def external_input_ids(self) -> List[str]:
        return self.input_ids

    @property
    def input_ids(self):
        return [f'node_{self._node_name}_final']

    @property
    def output_ids(self):
        return []

    def transform(self, inputs: List[pd.DataFrame],
                  **kwargs) -> List[pd.DataFrame]:
        df = inputs[0]
        # df['node_id'] = df['node_id'].map(lambda x: f'{self._node_name}_{x}')
        rename_dict = dict([(key,
                             f'{key}:{TYPE_CONVERT[str(value)]}') for key,
                            value in df.dtypes.to_dict().items() if key != 'node_id'])
        rename_dict.update(
            {'node_id': f':ID({self._node_name})'}
        )
        df = df.rename(columns=rename_dict)
        print(df)
        df.to_csv(self._output_fs._fs.path + '/node_' + self._node_name + '.csv',
                  index=False, header=True, quoting=csv.QUOTE_NONNUMERIC, escapechar='\\')
        return []


class FormatLink(ObjProcessor):
    def __init__(self, link_name: str, from_node: str, to_node: str,
                 input_storage: PandasStorage, output_fs: FileSystem):
        self._link_name = link_name
        self._from_node = from_node
        self._to_node = to_node
        self._output_fs = output_fs
        super().__init__(input_storage)

    @property
    def external_input_ids(self) -> List[str]:
        return self.input_ids

    @property
    def input_ids(self):
        return [f'link_{self._link_name}_final']

    @property
    def output_ids(self):
        return []

    def transform(self, inputs: List[pd.DataFrame],
                  **kwargs) -> List[pd.DataFrame]:
        df = inputs[0]
        # df['from_id'] = df['from_id'].map(lambda x: f'{self._from_node}_{x}')
        # df['to_id'] = df['to_id'].map(lambda x: f'{self._to_node}_{x}')
        rename_dict = dict(
            [
                (key,
                 f'{key}:{TYPE_CONVERT[str(value)]}') for key,
                value in df.dtypes.to_dict().items() if key != 'from_id' and key != 'to_id'])
        rename_dict.update(
            {
                'from_id': f':START_ID({self._from_node})',
                'to_id': f':END_ID({self._to_node})'
            }
        )
        df = df.rename(columns=rename_dict)
        print(df)
        df.to_csv(self._output_fs._fs.path + '/link_' + self._link_name + '.csv',
                  index=False, header=True, quoting=csv.QUOTE_NONNUMERIC, escapechar='\\')
        return []
