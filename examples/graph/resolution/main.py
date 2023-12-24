"""
TODO:
- [X] Add ID convertor / Links to Mapping Generator
    -> Build Entity Resolution Class
- [X] Enable no-canon workflow in mapping generator
"""
from batch_framework.storage import PandasStorage
from batch_framework.filesystem import FileSystem
from batch_framework.etl import SQLExecutor, ETLGroup
from batch_framework.rdb import RDB
from .meta import ERMeta
from .mapper import CanonMatcher
from .mapper import MessyMatcher
from .convertor import IDConvertor


class MappingGenerator(ETLGroup):
    def __init__(self, meta: ERMeta, subgraph_fs: FileSystem,
                 mapping_fs: FileSystem, model_fs: FileSystem, rdb: RDB):
        self._mapping_fs = mapping_fs
        etl_layers = []
        if meta.has_canon:
            etl_layers.append(
                CanonMatcher(meta,
                             PandasStorage(subgraph_fs),
                             PandasStorage(mapping_fs),
                             model_fs=model_fs,
                             threshold=0.25
                             )
            )
            etl_layers.append(
                MessyInput(
                    meta,
                    rdb,
                    input_fs=subgraph_fs,
                    output_fs=mapping_fs)
            )
            etl_layers.append(
                MessyFilter(meta, rdb, workspace_fs=mapping_fs)
            )
            etl_layers.append(
                MessyMatcher(
                    meta,
                    mapping_fs,
                    mapping_fs,
                    model_fs,
                    rdb,
                    threshold=0.5,
                    take_filtered=True
                )
            )
            etl_layers.append(
                MappingCombiner(
                    meta,
                    rdb,
                    workspace_fs=mapping_fs
                )
            )
        else:
            etl_layers.append(
                MessyMatcher(
                    meta,
                    subgraph_fs,
                    mapping_fs,
                    model_fs,
                    rdb,
                    threshold=0.5,
                    take_filtered=False
                )
            )
            etl_layers.append(
                MessyMappingPassor(
                    meta,
                    rdb,
                    workspace_fs=mapping_fs
                )
            )
        for item, column in meta.id_convertion_messy_items:
            etl_layers.append(
                IDConvertor(
                    meta.messy_node,
                    item,
                    column,
                    rdb,
                    subgraph_fs=subgraph_fs,
                    mapping_fs=mapping_fs
                )
            )
        super().__init__(
            *etl_layers
        )
        self._meta = meta

    @property
    def input_ids(self):
        return self._meta.input_ids

    @property
    def output_ids(self):
        return self._meta.output_ids

    def end(self, **kwargs):
        self.drop_internal_objs()


class MessyInput(SQLExecutor):
    """
    Passing messy input to mapping_fs
    """

    def __init__(self, meta: ERMeta, rdb: RDB,
                 input_fs: FileSystem, output_fs: FileSystem):
        self._meta = meta
        super().__init__(rdb, input_fs=input_fs, output_fs=output_fs)

    @property
    def input_ids(self):
        return [
            f'{self._meta.messy_node}'
        ]

    @property
    def output_ids(self):
        return [
            f'{self._meta.messy_node}' + '_input'
        ]

    def sqls(self):
        return {
            self.output_ids[0]: f"""
                SELECT
                    *
                FROM {self.input_ids[0]}
            """
        }


class MessyFilter(SQLExecutor):
    """
    Filter Messy Node by using canon mapper
    """

    def __init__(self, meta: ERMeta, rdb: RDB, workspace_fs: FileSystem):
        self._workspace_fs = workspace_fs
        self._meta = meta
        super().__init__(rdb, input_fs=workspace_fs, output_fs=workspace_fs)

    @property
    def input_ids(self):
        return [
            f'{self._meta.messy_node}_input',
            f'mapper_{self._meta.messy_node}2{self._meta.canon_node}'
        ]

    @property
    def output_ids(self):
        return [
            f'{self._meta.messy_node}' + '_filtered'
        ]

    def sqls(self):
        return {
            self.output_ids[0]: f"""
                SELECT
                    *
                FROM {self.input_ids[0]} AS messy_table
                WHERE NOT EXISTS (
                    SELECT
                        *
                    FROM {self.input_ids[1]} AS mapping_table
                    WHERE mapping_table.messy_id = messy_table.node_id
                )
            """
        }


class MessyMappingPassor(SQLExecutor):
    """
    Passing Messy2clean to Next Stage
    """

    def __init__(self, meta: ERMeta, rdb: RDB, workspace_fs: FileSystem):
        self._workspace_fs = workspace_fs
        self._meta = meta
        super().__init__(rdb, input_fs=workspace_fs, output_fs=workspace_fs)

    @property
    def input_ids(self):
        return [
            f'mapper_{self._meta.messy_node}'
        ]

    @property
    def output_ids(self):
        return [
            f'mapper_{self._meta.messy_node}' + '_clean'
        ]

    def sqls(self):
        return {
            self.output_ids[0]: f"""
                SELECT
                    messy_id,
                    cluster_id AS new_id
                FROM mapper_{self._meta.messy_node}
            """
        }


class MappingCombiner(SQLExecutor):
    """
    Combine Messy2clean and Messy2Canon Mapping
    """

    def __init__(self, meta: ERMeta, rdb: RDB, workspace_fs: FileSystem):
        self._workspace_fs = workspace_fs
        self._meta = meta
        super().__init__(rdb, input_fs=workspace_fs, output_fs=workspace_fs)

    @property
    def input_ids(self):
        return [
            f'mapper_{self._meta.messy_node}2{self._meta.canon_node}',
            f'mapper_{self._meta.messy_node}'
        ]

    @property
    def output_ids(self):
        return [f'mapper_{self._meta.messy_node}' + '_clean']

    def sqls(self):
        return {
            self.output_ids[0]: f"""
                SELECT
                    messy_id,
                    canon_id AS new_id
                FROM mapper_{self._meta.messy_node}2{self._meta.canon_node}
                UNION ALL
                SELECT
                    messy_id,
                    cluster_id AS new_id
                FROM mapper_{self._meta.messy_node}
            """
        }
