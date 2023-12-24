"""
Validate ID(s) of the Final Graph
"""
from batch_framework.storage import PandasStorage
from batch_framework.etl import ETLGroup
from .meta import GroupingMeta
from ..validate import LinkIDValidator

__all__ = ['Validator']


class Validator(ETLGroup):
    def __init__(self, metagraph: GroupingMeta, storage: PandasStorage):
        self._storage = storage
        self.metagraph = metagraph
        super().__init__(*self.validator_list)

    @property
    def input_ids(self):
        results = []
        results.extend(self.metagraph.output_nodes)
        results.extend(self.metagraph.output_links)
        return results

    @property
    def output_ids(self):
        return []

    @property
    def validator_list(self):
        results = []
        for link, (src_node, dest_node) in self.metagraph.triplets.items():
            results.append(LinkIDValidator(f'link_{link}_final',
                                           f'node_{src_node}_final',
                                           'from_id', self._storage))
            results.append(LinkIDValidator(f'link_{link}_final',
                                           f'node_{dest_node}_final',
                                           'to_id', self._storage))
        return results
