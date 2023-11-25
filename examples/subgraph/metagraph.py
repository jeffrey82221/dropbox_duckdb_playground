from typing import Dict, Tuple, List

class MetaGraph:
    def __init__(self, metagraph: Dict[str, Tuple[str, str]], input_ids: List[str]):
        self._metagraph = metagraph
        self.input_ids = input_ids

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