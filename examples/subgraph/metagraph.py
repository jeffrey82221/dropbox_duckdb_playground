from typing import Dict, Tuple, List

class MetaGraph:
    """
    Data Class holding extraction logic of subgraphs

    Args:
        - metagraph: describe the relationship between subgraph nodes and links
            - key: link name
            - value (tuple)
                - value[0]: name of the source node for the link
                - value[1]: name of the target node for the link
        - input_ids: name of tables from which the subgraph is extracted.
        - node_sqls: define how node tables are extracted from the tables of `input_ids`
        - link_sqls: define how link tables are extracted from the tables of `input_ids`
    """
    def __init__(self, metagraph: Dict[str, Tuple[str, str]], input_ids: List[str], node_sqls: Dict[str, str], link_sqls: Dict[str, str]):
        self._metagraph = metagraph
        self.input_ids = input_ids
        self.node_sqls = node_sqls
        self.link_sqls = link_sqls

    @property
    def nodes(self) -> List[str]:
        results = []
        for from_node, to_node in self._metagraph.values():
            results.append(from_node)
            results.append(to_node)
        return list(set(results))
    
    @property
    def links(self) -> List[str]:
        return list(set([link for link in self._metagraph.keys()]))