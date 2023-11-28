from graph.resolution import ERMeta

subgraphs = {
        'has_requirement': ('package', 'requirement'),
        'has_author': ('package', 'author'), 
        'has_maintainer': ('package', 'maintainer'), 
        'has_license': ('package', 'license'), 
        'has_docs_url': ('package', 'docs_url'), 
        'has_home_page': ('package', 'home_page'), 
        'has_project_url': ('package', 'project_url')
    }

er_meta = ERMeta(
    subgraphs=subgraphs,
    messy_node='requirement',
    dedupe_fields=[
        {'field': 'full_name', 'type': 'String'},
        {'field': 'before_whitespace', 'type': 'Exact'},
        {'field': 'before_upper_bracket', 'type': 'Exact'},
        {'field': 'before_marks', 'type': 'Exact'}
    ],
    messy_lambda=lambda record: {
        'full_name': record['name'],
        'before_whitespace': record['name'].split(' ')[0].split(';')[0],
        'before_upper_bracket': record['name'].split('[')[0].split('(')[0],
        'before_marks': record['name'].split('<')[0].split('>')[0].split('=')[0].split('~')[0]
    },
    canon_node='package',
    canon_lambda=lambda record: {
        'full_name': record['name'],
        'before_whitespace': record['name'],
        'before_upper_bracket': record['name'],
        'before_marks': record['name']
    }
)