import copy
from typing import Dict, Tuple, List

class SqlBuilder:
    """
    Build Common Sql patterns for Node / Link Grouping
    """
    @staticmethod
    def build_node_join_sql(column_sql: str, node_names: List[str]) -> str:
        pop_sql = SqlBuilder.build_node_pop_sql(node_names)
        left_join_sql = '\n'.join([
            f'LEFT JOIN {node} AS t{i+1} ON t0.node_id = t{i+1}.node_id'
            for i, node in enumerate(node_names)
        ])
        result = f"""
        WITH pop AS (
            {pop_sql}
        )
        SELECT 
            {column_sql}
        FROM pop AS t0
            {left_join_sql}
        """
        return result

    @staticmethod
    def build_node_pop_sql(node_names: List[str]) -> str:
        union_table = 'UNION\n'.join([f'SELECT node_id FROM {nm}\n' for nm in node_names])
        result = f"""
        SELECT DISTINCT ON (node_id)
            node_id
        FROM (
            {union_table}
        )
        """
        return result

    @staticmethod
    def build_link_join_sql(column_sql: str, link_names: List[str]) -> str:
        pop_sql = SqlBuilder.build_link_pop_sql(link_names)
        left_join_sql = '\n'.join([
            f'LEFT JOIN {link} AS t{i+1} ON t0.from_id = t{i+1}.from_id AND t0.to_id = t{i+1}.to_id'
            for i, link in enumerate(link_names)
        ])
        result = f"""
        WITH pop AS (
            {pop_sql}
        )
        SELECT 
            {column_sql}
        FROM pop AS t0
            {left_join_sql}
        """
        return result

    @staticmethod
    def build_link_pop_sql(link_names: List[str]) -> str:
        union_table = 'UNION\n'.join([f'SELECT from_id, to_id FROM {nm}\n' for nm in link_names])
        result = f"""
        SELECT DISTINCT ON (from_id, to_id)
            from_id, to_id
        FROM (
            {union_table}
        )
        """
        return result

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
    def final_nodes(self) -> List[str]:
        return [key for key in self.node_grouping_sqls]
    
    @property
    def node_grouping_sqls(self) -> Dict[str, str]:
        result = dict()
        for key in self.node_grouping:
            if key in self.__node_grouping_sqls:
                result[f'{key}_final'] = SqlBuilder.build_node_join_sql(self.__node_grouping_sqls[key], self.node_grouping[key])
            else:
                assert len(self.node_grouping[key]) == 1, 'default node grouping should be 1-1 mapping'
                assert self.node_grouping[key][0] == key, 'default node grouping source node should equals target node'
                result[f'{key}_final'] = f"SELECT * FROM {key}"
        return result
    
        
    @property
    def final_links(self) -> List[str]:
        return [key for key in self.link_grouping_sqls]
    
    @property
    def link_grouping_sqls(self) -> Dict[str, str]:
        result = dict()
        for key in self.link_grouping:
            if key in self.__link_grouping_sqls:
                result[f'{key}_final'] = SqlBuilder.build_link_join_sql(self.__link_grouping_sqls[key], self.link_grouping[key])
            else:
                assert len(self.link_grouping[key]) == 1, 'default link grouping should be 1-1 mapping'
                assert self.link_grouping[key][0] == key, 'default link grouping source link should equals target link'
                result[f'{key}_final'] = f"SELECT * FROM {key}"
        return result
    
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