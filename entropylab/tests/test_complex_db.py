from entropylab import Script, Graph
from entropylab.api.data_reader import ExperimentReader
from entropylab.api.execution import EntropyContext
from entropylab.graph_experiment import PyNode
from entropylab.instruments.lab_topology import ExperimentResources, LabResources
from entropylab.results_backend.sqlalchemy.db import SqlAlchemyDB
import numpy as np 
import subprocess
from pathlib import Path
import os
import pytest
from distutils.dir_util import copy_tree
from os.path import abspath

def abs_path_to(rel_path: str) -> str:
    source_path = Path(__file__).resolve()
    source_dir = source_path.parent
    return os.path.join(source_dir, rel_path)

@pytest.fixture(scope="session")
def db_file():
    db_file=abs_path_to(r'tests_cache\test_entropy_autoplot')

@pytest.fixture(scope="session")
def create_and_populate_db(tmpdir_factory):
    temp_dir = tmpdir_factory.mktemp("data")
    db=SqlAlchemyDB(path=temp_dir)
    labResources = ExperimentResources(db)    

    ## 1d array, single node
    def node_operation():
        return {'res':[1,2,3,4]}

    node1 = PyNode(label="first_node", program=node_operation,output_vars={'res'})
    experiment = Graph(resources=labResources, graph={node1}, story="run_a") 
    handle = experiment.run()

    
    ## 2d array, single node
    def node_operation():
        return {'res':np.random.randn(5,5).tolist()}

    node1 = PyNode(label="2d array", program=node_operation,output_vars={'res'})
    experiment = Graph(resources=labResources, graph={node1}, story="run_a") 
    handle = experiment.run()


    ## 3d array, single node
    def node_operation():
        return {'res':np.random.randn(5,5,3)}

    node1 = PyNode(label="3d array", program=node_operation,output_vars={'res'})
    experiment = Graph(resources=labResources, graph={node1}, story="run_a") 
    handle = experiment.run()

    ## 2 1d arrays, 1 2d array , single node
    def node_operation():
        return {'x':np.random.randn(10),'y':np.random.randn(10),'res3':np.random.randn(10,10)}

    node1 = PyNode(label="multiple res", program=node_operation,output_vars={'res'})
    experiment = Graph(resources=labResources, graph={node1}, story="run_a") 
    handle = experiment.run()


    ##2 nodes
    def node_operation1():
        return {'x':np.random.randn(10),'y':np.random.randn(10),'res3':np.random.randn(10,10)}

    def node_operation2():
        return {'x':np.random.randn(10),'y':np.random.randn(10),'res3':np.random.randn(10,10)}

    node1 = PyNode("the first node", node_operation1, output_vars={"x"})
    node2 = PyNode("the second node", node_operation2, input_vars={"x": node1.outputs["x"]})
    experiment = Graph(resources=labResources, graph={node1,node2}, story="multi-node") 
    graph_handle = experiment.run(db)
    return temp_dir

@pytest.fixture(scope="function")
def complex_db(create_and_populate_db,tmpdir_factory):
    local_temp = abspath(tmpdir_factory.mktemp('local_temp'))
    copy_tree(abspath(create_and_populate_db) , abspath(local_temp))
    return local_temp

def test_fetch_res_by_name(complex_db):
    db=SqlAlchemyDB(complex_db)
    assert len(db.get_all_results_with_label(1,'res'))!=0 

def test_fetch_2d_array(complex_db):
    db=SqlAlchemyDB(complex_db)
    data = db.get_results_from_node("2d array")[0]
    assert data.results[0].label == "2d array"

