"""
Build Messy Node Id Convertor

TODO:
- [X] In mapper.py, combine CanonMatcher & MessyMatcher & a new 
    MapCombiner to produce a Mapping Table for Messy Node Ids
- [ ] In convertor.py, allow class to takes `node` or `node_of_link`
    as input for ID convertion.
"""
from batch_framework.etl import SQLExecutor, ETLGroup
from batch_framework.filesystem import FileSystem
from batch_framework.rdb import RDB

__all__ = ['IDConvertor']

class IDConvertor(ETLGroup):
    """
    Converting ID of Messy Node to Cleaned Node ID
    """
    def __init__(self, messy_node: str, source_item: str, target_column: str, db: RDB, subgraph_fs: FileSystem, mapping_fs: FileSystem):
        self._source_item = source_item
        self._messy_node = messy_node
        units = [
            _TablePassing(source_item, db, input_fs=subgraph_fs, output_fs=mapping_fs),
            _IDConvertor(messy_node, source_item, target_column, db, input_fs=mapping_fs, output_fs=subgraph_fs)
        ]
        super().__init__(*units)

    @property
    def input_ids(self):
        return [
            self._source_item,
            f'mapper_{self._messy_node}_clean'
        ]
    
    @property
    def output_ids(self):
        return [self._source_item + 'Q']
    
class _TablePassing(SQLExecutor):
    """
    Passing Input Table to mapping_fs
    """
    def __init__(self, source_item: str, db: RDB, input_fs: FileSystem, output_fs: FileSystem):
        self._source_item = source_item
        super().__init__(db, input_fs=input_fs, output_fs=output_fs)
    
    @property
    def input_ids(self):
        return [self._source_item]
    
    @property
    def output_ids(self):
        return [self._source_item + '_tmp']
    
    def sqls(self):
        return {
            self.output_ids[0]: f"""
                SELECT
                    *
                FROM {self.input_ids[0]}
            """
        }

class _IDConvertor(SQLExecutor):
    """
    Converting ID of Messy Node to Cleaned Node ID
    """
    def __init__(self, messy_node: str, source_item: str, target_column: str, db: RDB, input_fs: FileSystem, output_fs: FileSystem):
        self._messy_node = messy_node
        self._source_item = source_item
        self._target_column = target_column
        super().__init__(db, input_fs=input_fs, output_fs=output_fs)
        
    @property
    def input_ids(self):
        return [
            self._source_item + '_tmp',
            f'mapper_{self._messy_node}_clean'
        ]

    @property
    def output_ids(self):
        return [self._source_item + 'Q']
    
    def sqls(self):
        return {
            self.output_ids[0]: f"""
                SELECT
                    t2.new_id AS {self._target_column},
                    t1.* EXCLUDE ({self._target_column})
                FROM {self._source_item} AS t1
                LEFT JOIN mapper_{self._messy_node}_clean AS t2
                ON t1.{self._target_column} = t2.messy_id
            """
        }