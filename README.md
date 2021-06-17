![PyPI](https://img.shields.io/pypi/v/entropylab)
[![discord](https://img.shields.io/discord/806244683403100171?label=QUA&logo=Discord&style=plastic)](https://discord.gg/7FfhhpswbP)

[![License](https://img.shields.io/badge/License-BSD%203--Clause-blue.svg)](https://opensource.org/licenses/BSD-3-Clause)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.4785097.svg)](https://doi.org/10.5281/zenodo.4785097)

# Entropy

Entropy is a lab workflow managment package built for, but not limitied-to, streamlining the process of running quantum information processing experiments. 

Entropy is built to solve a few major hurdles in experiment design: 

1. Building, maintaining and executing complex experiments
2. Data collection
3. Device management
4. Calibration automation

To tackle these problems, Entropy is built around the central concept of a graph strucutre. The nodes of a graph give us a convenient way 
to brake down experiments into stages and to automate some of the tasks required in each node. For example data collection is automated, at least in part, 
by saving node data and code to a persistant database. 

Device managment is the challange of managing the state and control of a variety of different resources. These include, but are not limited to, lab instrumnets. 
They can also be computational resources, software resources or others. Entropy is built with tools to save such resources to a shared database and give nodes access to 
the resources needed during an experiment. 

Performing automatic calibration is an important reason why we built Entropy. This could be though of as the usecase most clearly benefiting from shared resources, persistant 
storage of different pieced of information and the graph structure. If the final node in a graph is the target experiment, then all the nodes between the root and that node are often 
calibration steps. The documentation section will show how this can be done. 

The Entropy system is built with concrete implemnetations of the various parts (database backend, resource managment and others) but is meant to be completely customizable. Any or every part of the system can be tailored by end users. 

## Versioning and the Alpha release 

The current release of Entropy is version 0.1.0. You can learn more about the Entropy versioning scheme in the versioning
document. This means this version is a work in progress in several important ways: 

1. It is not fully tested
2. There are important features missing, such as the results GUI which will enable visual results viewing and automatic plotting
3. There will more than likely be breaking changes to the API for a while until we learn how things should be done. 

Keep this in mind as you start your journey. 

## Installation

Installation is done from pypi using the following command

```shell
pip install entropylab
```

## Testing your installation

import the library from `entropylab`

```python
from entropylab import *

def my_func():
    return {'res': 1}

node1 = PyNode("first_node", my_func, output_vars={'res'})
experiment = Graph(None, {node1}, "run_a")  # No resources used here
handle = experiment.run()
```

## Usage

See [docs](docs) folder in this repository for all the dirty details.


## Extensions

Entropy can and will be extended via custom extensions. An example is `entropylab-qpudb`, an extension built to keep track of the calibration parameters of a mutli-qubit Quantum Processing Unit (QPU). This extension is useful when writing an automatic calibration graph. 



