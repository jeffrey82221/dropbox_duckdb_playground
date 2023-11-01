import pytest
from batch_framework.etl import ETL, ETLGroup
from paradag import DAG
from paradag import DAGVertexNotFoundError
from datetime import datetime
import os
import time
class ETL1(ETL):
    @property
    def input_ids(self):
        return []

    @property
    def output_ids(self):
        return ['a', 'b']

    def execute(self):
        with open(f'tests/{str(datetime.now().date())}.test1', 'wb') as f:
            f.write(b'hello')
        f.close()
        time.sleep(1)
        return 'etl1'

class ETL2(ETL):
    @property
    def input_ids(self):
        return ['a', 'b']

    @property
    def output_ids(self):
        return ['c']
    
    def execute(self):
        print('ETL2 Executed')
        with open(f'tests/{str(datetime.now().date())}.test1', 'rb') as f:
            ans = f.read()
        f.close()
        assert ans == b'hello'
        os.remove(f'tests/{str(datetime.now().date())}.test1')
        return 'etl2'

class MyGroup(ETLGroup):
    @property
    def input_ids(self):
        return []

    @property
    def output_ids(self):
        return ['c']

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
    assert list(dag.predecessors('c'))[0].execute() == 'etl2'
    assert 'a' in dag.predecessors(list(dag.predecessors('c'))[0])
    assert 'b' in dag.predecessors(list(dag.predecessors('c'))[0])
    assert list(dag.predecessors('a'))[0].execute() == 'etl1'
    assert list(dag.predecessors('b'))[0].execute() == 'etl1'


def test_build_fail(etl2):
    dag = DAG()
    with pytest.raises(DAGVertexNotFoundError):
        etl2.build(dag)

def test_group(etl1, etl2):
    etl_group = MyGroup(etl1, etl2)
    dag = DAG()
    etl_group.build(dag)

def test_group_fail(etl1, etl2):
    etl_group = MyGroup(etl2, etl1)
    dag = DAG()
    with pytest.raises(DAGVertexNotFoundError):
        etl_group.build(dag)

def test_etl_hash(etl1, etl2):
    assert etl1 in set([etl1, etl2])
    assert etl1 in set([etl1, etl1])
    assert len(set([etl1, etl1])) == 1

def test_group_execute(etl1, etl2):
    etl_group = MyGroup(etl1, etl2)
    etl_group.execute()