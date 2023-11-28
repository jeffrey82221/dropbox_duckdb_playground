import copy
from typing import Dict, Tuple, List
from .group import GroupingMeta


class MetaGraph:
    """
    Data Class holding extraction logic of subgraphs

    Args:
        - subgraph: describe the relationship between subgraph nodes and links
            - key: link name
            - value (tuple)
                - value[0]: name of the source node for the link
                - value[1]: name of the target node for the link
        - node_grouping: describe how the nodes of subgraphs should be integrated.
            - key: node name
            - value (list): list of subgraph nodes that should be grouped
        - link_grouping: describe how the links of subgraphs should be integrated.
            - key: link name
            - value (list): list of subgraph links that should be grouped
        - input_ids: name of tables from which the subgraph is extracted.
        - node_sqls: define how node tables are extracted from the tables of `input_ids`
        - link_sqls: define how link tables are extracted from the tables of `input_ids`
    """
    def __init__(self, 
                 subgraphs: Dict[str, Tuple[str, str]], 
                 node_grouping: Dict[str, List[str]], 
                 link_grouping: Dict[str, List[str]], 
                 input_ids: List[str], 
                 node_sqls: Dict[str, str],
                 link_sqls: Dict[str, str],
                 node_grouping_sqls: Dict[str, str]=dict(), 
                 link_grouping_sqls: Dict[str, str]=dict(),
                ):
        self._subgraphs = subgraphs
        self._node_grouping = node_grouping
        self.__check_subgraph_nodes()
        self._link_grouping = link_grouping
        self.__check_subgraph_links()
        self.input_ids = input_ids
        self.node_sqls = node_sqls
        self.__check_node_sqls()
        self.link_sqls = link_sqls
        self.__check_link_sqls()
        self.__node_grouping_sqls = node_grouping_sqls
        self.__link_grouping_sqls = link_grouping_sqls

    def subgraphs(self):
        return self._subgraphs
    
    def __check_subgraph_nodes(self):
        subgraph_nodes = self.nodes
        for _, nodes in self._node_grouping.items():
            for node in nodes:
                assert node in subgraph_nodes, f'node `{node}` is not defined in nodes of subgraphs ({subgraph_nodes})'
    
    def __check_subgraph_links(self):
        subgraph_links = self.links
        for _, links in self._link_grouping.items():
            for link in links:
                assert link in subgraph_links, f'link `{link}` is not defined in links of subgraphs ({subgraph_links})'
    
    def __check_node_sqls(self):
        subgraph_nodes = self.nodes
        for node in self.node_sqls:
            assert node in subgraph_nodes, f'node `{node}` of node_sqls is not defined in nodes of subgraphs ({subgraph_nodes})'
        for node in subgraph_nodes:
            assert node in self.node_sqls, f'sql of subgraph node `{node}` is not provided'
    
    def __check_link_sqls(self):
        subgraph_links = self.links
        for link in self.link_sqls:
            assert link in subgraph_links, f'link `{link}` of link_sqls is not defined in links of subgraphs ({subgraph_links})'
        for link in subgraph_links:
            assert link in self.link_sqls, f'sql of subgraph link `{link}` is not provided'
    
    @property
    def nodes(self) -> List[str]:
        results = []
        for from_node, to_node in self._subgraphs.values():
            results.append(from_node)
            results.append(to_node)
        return list(set(results))
    
    @property
    def links(self) -> List[str]:
        return list(set([link for link in self._subgraphs.keys()]))
    
    @property
    def grouping_meta(self) -> GroupingMeta:
        return GroupingMeta(
            self.node_grouping,
            self.link_grouping,
            self.__node_grouping_sqls,
            self.__link_grouping_sqls
        )
    
    @property
    def node_grouping(self) -> Dict[str, List[str]]:
        result = copy.copy(self._node_grouping)
        exist_subgraph_nodes = []
        for nodes in self._node_grouping.values():
            exist_subgraph_nodes.extend(nodes)
        for node in self.nodes:
            if node not in exist_subgraph_nodes:
                result.update({node: [node]})
        return result
    
    @property
    def link_grouping(self) -> Dict[str, List[str]]:
        result = copy.copy(self._link_grouping)
        exist_subgraph_links = []
        for links in self._link_grouping.values():
            exist_subgraph_links.extend(links)
        for link in self.links:
            if link not in exist_subgraph_links:
                result.update({link: [link]})
        return result
    
    