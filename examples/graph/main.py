from typing import List, Optional
from batch_framework.rdb import DuckDBBackend
from batch_framework.filesystem import FileSystem
from batch_framework.etl import ETLGroup
from batch_framework.rdb import RDB
from .subgraph import SubgraphExtractor
from .group import GraphGrouper
from .resolution import (
    MappingGenerator, ERMeta
)
from graph.metagraph import MetaGraph

class GraphDataPlatform(ETLGroup):
    """
    Data Flow: 
        1. canonicalize data 
        2. extract subgraphs
        3. do entity resolution
        4. group subgraph
    """
    def __init__(self, metagraph: MetaGraph, 
                 canon_fs: FileSystem, subgraph_fs: FileSystem, 
                 output_fs: FileSystem,
                 redisgraph_fs: FileSystem,
                 er_meta_list: List[ERMeta]=[],
                 mapping_fs: Optional[FileSystem]=None, 
                 model_fs: Optional[FileSystem]=None, 
                 rdb: RDB=DuckDBBackend(), 
                 messy_pairing_worker_cnt: int=10
                ):
        # Connecting MetaGraph with Entity Resolution Meta
        grouping_meta = metagraph.grouping_meta
        for er_meta in er_meta_list:
            er_meta.alter_grouping_way(grouping_meta)
        # Basic ETL components        
        # 1. Extract Subgraphs from Canonicalized Tables
        subgraph_extractor = SubgraphExtractor(
            metagraph=metagraph, 
            rdb=rdb,
            input_fs=canon_fs, 
            output_fs=subgraph_fs
        )
        args = [subgraph_extractor]
        # Insert Entity Resolutions to the DataFlow
        for er_meta in er_meta_list:
            mapping = MappingGenerator(
                er_meta,
                subgraph_fs=subgraph_fs,
                mapping_fs=mapping_fs,
                model_fs=model_fs,
                rdb=rdb,
                messy_pairing_worker_cnt=messy_pairing_worker_cnt
            )
            args.append(mapping)
        # 2. Group Subgraphs into Final Graph
        self._grouper = GraphGrouper(
            meta=grouping_meta,
            rdb=rdb,
            input_fs=subgraph_fs,
            output_fs=output_fs,
            redisgraph_fs=redisgraph_fs
        )
        args.append(self._grouper)
        self._input_ids = subgraph_extractor.input_ids
        self._output_ids = self._grouper.output_ids
        super().__init__(*args)

    @property
    def input_ids(self):
        return self._input_ids
    
    @property
    def external_input_ids(self) -> List[str]:
        return self.input_ids
    
    @property
    def output_ids(self):
        return self._output_ids
