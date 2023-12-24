import os


def ingest_to_redisgraph():
    url = 'redis://localhost:9001'
    graph_name = 'PYPI'
    cmd = f'redisgraph-bulk-insert -u {url} {graph_name} --enforce-schema '
    cmd += ' --skip-invalid-nodes '
    cmd += ' --skip-invalid-edges '
    for fn in os.listdir('data/redisgraph/'):
        print(fn)
        if fn.startswith('node_'):
            cmd += '--nodes ' + 'data/redisgraph/' + fn + ' '
        if fn.startswith('link_'):
            cmd += '--relations ' + 'data/redisgraph/' + fn + ' '
    os.system(cmd)


if __name__ == '__main__':
    ingest_to_redisgraph()
