
from ..base import ERBase
from ..meta import ERMeta
from batch_framework.storage import PandasStorage
from batch_framework.filesystem import FileSystem


class MatcherBase(ERBase):
    def __init__(self, mapping_meta: ERMeta, input_storage: PandasStorage,
                 output_storage: PandasStorage, model_fs: FileSystem, threshold: float = 0.25):
        super().__init__(mapping_meta, input_storage, output_storage, model_fs=model_fs)
        self._threshold = threshold

    @property
    def output_ids(self):
        return [self.mapper_file_name]
