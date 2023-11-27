from typing import List, Dict


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
    
class GroupingMeta:
    """
    Data Class describing how subgraphs are merged
    """
    def __init__(self, 
            node_grouping: Dict[str, List[str]], 
            link_grouping: Dict[str, List[str]], 
            node_grouping_sqls: Dict[str, str]=dict(), 
            link_grouping_sqls: Dict[str, str]=dict()
        ):
            self.node_grouping = node_grouping
            self.link_grouping = link_grouping
            self.__node_grouping_sqls = node_grouping_sqls
            self.__link_grouping_sqls = link_grouping_sqls
    
    @property
    def input_nodes(self) -> List[str]:
        result = []
        for _, nodes in self.node_grouping.items():
            result.extend(nodes)
        return list(set(result))
    
    @property
    def input_links(self) -> List[str]:
        result = []
        for _, links in self.link_grouping.items():
            result.extend(links)
        return list(set(result))
    
    @property
    def output_nodes(self) -> List[str]:
        return [f'{n}_final' for n in self.node_grouping]
    
    @property
    def output_links(self) -> List[str]:
        return [f'{n}_final' for n in self.link_grouping]
    
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