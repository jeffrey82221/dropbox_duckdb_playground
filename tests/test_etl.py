import pytest
from batch_framework.etl import ETL
from paradag import DAG
from paradag import DAGVertexNotFoundError

class ETL1(ETL):
    @property
    def input_ids(self):
        return []

    @property
    def output_ids(self):
        return ['a', 'b']

    def execute(self):
        return 'etl1_execute'

class ETL2(ETL):
    @property
    def input_ids(self):
        return ['a', 'b']

    @property
    def output_ids(self):
        return ['c']
    
    def execute(self):
        return 'etl2_execute'


@pytest.fixture
def etl1():
    return ETL1()

@pytest.fixture
def etl2():
    return ETL2()

def test_build(etl1, etl2):
    dag = DAG()
    etl1.build(dag)
    etl2.build(dag)
    assert list(dag.predecessors('c'))[0]() == 'etl2_execute'
    assert 'a' in dag.predecessors(list(dag.predecessors('c'))[0])
    assert 'b' in dag.predecessors(list(dag.predecessors('c'))[0])
    assert list(dag.predecessors('a'))[0]() == 'etl1_execute'
    assert list(dag.predecessors('b'))[0]() == 'etl1_execute'


def test_build_fail(etl2):
    dag = DAG()
    with pytest.raises(DAGVertexNotFoundError):
        etl2.build(dag)