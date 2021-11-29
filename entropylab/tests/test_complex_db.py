from entropylab import Script, Graph
from entropylab.api.data_reader import ExperimentReader
from entropylab.api.execution import EntropyContext
from entropylab.graph_experiment import PyNode
from entropylab.instruments.lab_topology import ExperimentResources, LabResources
from entropylab.results_backend.sqlalchemy.db import SqlAlchemyDB
import numpy as np 
import subprocess

db_file=r'C:\repos\entropy\entropylab\tests\myproj'
db=SqlAlchemyDB(path=db_file)
er = ExperimentResources(db)    

##
def node_operation():
    return {'res':np.array([1,2,3,4])}

node1 = PyNode(label="first_node", program=node_operation,output_vars={'res'})
experiment = Graph(resources=er, graph={node1}, story="run_a") 
handle = experiment.run()


def node_operation():
    return {'res':np.random.randn(5,5)}

node1 = PyNode(label="2d array", program=node_operation,output_vars={'res'})
experiment = Graph(resources=er, graph={node1}, story="run_a") 
handle = experiment.run()



def node_operation():
    return {'res':np.random.randn(5,5,3)}

node1 = PyNode(label="3d array", program=node_operation,output_vars={'res'})
experiment = Graph(resources=er, graph={node1}, story="run_a") 
handle = experiment.run()



def node_operation():
    return {'x':np.random.randn(10),'y':np.random.randn(10),'res3':np.random.randn(10,10)}

node1 = PyNode(label="multiple res", program=node_operation,output_vars={'res'})
experiment = Graph(resources=er, graph={node1}, story="run_a") 
handle = experiment.run()



def node_operation1():
    return {'x':np.random.randn(10),'y':np.random.randn(10),'res3':np.random.randn(10,10)}

def node_operation2():
    return {'x':np.random.randn(10),'y':np.random.randn(10),'res3':np.random.randn(10,10)}

node1 = PyNode(label="multiple res", program=node1_operation,output_vars={'res'})
node2 = PyNode(label="multiple res", program=node2_operation,output_vars={'res'})
experiment = Graph(resources=er, graph={node1,node2}, story="multi-node") 
handle = experiment.run()
