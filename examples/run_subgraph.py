from batch_framework.rdb import DuckDBBackend
from batch_framework.filesystem import LocalBackend
from batch_framework.storage import PandasStorage
from subgraph.main import SubgraphExtractor
from subgraph.metagraph import MetaGraph

metagraph = MetaGraph({
    'has_requirement': ('package', 'requirement'),
    'has_author': ('package', 'author'), 
    'has_maintainer': ('package', 'maintainer'), 
    'has_license': ('package', 'license'), 
    'has_docs_url': ('package', 'docs_url'), 
    'has_home_page': ('package', 'home_page'), 
    'has_project_url': ('package', 'project_url')
})

input_fs = LocalBackend('./data/canon/output/')
output_fs = LocalBackend('./data/subgraph/output/')
db = DuckDBBackend()
subgraph_extractor = SubgraphExtractor(metagraph, db, input_fs, output_fs)

if __name__ == '__main__':
    subgraph_extractor.execute(sequential=True)