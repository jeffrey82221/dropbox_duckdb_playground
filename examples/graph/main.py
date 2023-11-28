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
                 node_fs: FileSystem, link_fs: FileSystem, 
                 er_meta_list: List[ERMeta]=[],
                 mapping_fs: Optional[FileSystem]=None, model_fs: Optional[FileSystem]=None, rdb: RDB=DuckDBBackend()):
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
        # 2. Group Subgraphs into Final Graph
        grouper = GraphGrouper(
            meta=grouping_meta,
            rdb=rdb,
            input_fs=subgraph_fs,
            node_fs=node_fs,
            link_fs=link_fs
        )
        args = [subgraph_extractor, grouper]
        self._input_ids = subgraph_extractor.input_ids
        self._output_ids = grouper.output_ids
        # Insert Entity Resolutions to the DataFlow
        for er_meta in er_meta_list:
            mapping = MappingGenerator(
                er_meta,
                subgraph_fs=subgraph_fs,
                mapping_fs=mapping_fs,
                model_fs=model_fs,
                rdb=rdb
            )
            args.append(mapping)
        super().__init__(*args)

    @property
    def input_ids(self):
        return self._input_ids
    
    @property
    def output_ids(self):
        return self._output_ids
