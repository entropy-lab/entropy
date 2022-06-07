![PyPI](https://img.shields.io/pypi/v/entropylab)
[![discord](https://img.shields.io/discord/806244683403100171?label=QUA&logo=Discord&style=plastic)](https://discord.gg/7FfhhpswbP)

[![License](https://img.shields.io/badge/License-BSD%203--Clause-blue.svg)](https://opensource.org/licenses/BSD-3-Clause)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.4785097.svg)](https://doi.org/10.5281/zenodo.4785097)

# Entropy

Entropy is a lab workflow management package built for, but not limited-to, streamlining the process of running quantum information processing experiments. 

Check out our [docs](https://docs.entropy-lab.io) for more information

Entropy is built to solve a few major hurdles in experiment design: 

1. Building, maintaining and executing complex experiments
2. Data collection
3. Device management
4. Calibration automation

To tackle these problems, Entropy is built around the central concept of a graph structure. The nodes of a graph give us a convenient way 
to brake down experiments into stages and to automate some tasks required in each node. For example data collection is automated, at least in part, 
by saving node data and code to a persistent database. 

Device management is the challenge of managing the state and control of a variety of different resources. These include, but are not limited to, lab instruments. 
They can also be computational resources, software resources or others. Entropy is built with tools to save such resources to a shared database and give nodes access to 
the resources needed during an experiment. 

Performing automatic calibration is an important reason why we built Entropy. This could be though of as the use case most clearly benefiting from shared resources, persistent 
storage of different pieced of information and the graph structure. If the final node in a graph is the target experiment, then all the nodes between the root and that node are often 
calibration steps. The documentation section will show how this can be done. 

The Entropy system is built with concrete implementations of the various parts (database backend, resource management and others) but is meant to be completely customizable. Any or every part of the system can be tailored by end users. 

## Modules 

- ***Pipeline*** : A simple execution engine for a collection of nodes. Allows passing data between nodes and saving results to a database. Also includes a dashboard for viewing results. 
- ***Flame*** : An actor model execution engine 
- ***QuAM*** : The Quantum Abstract Machine. An abstraction layer above QPU to simplify experiment authoring and parameter management.
 

## Installation

Installation is done from pypi using the following command

```shell
pip install entropylab
```

## Versioning and the Alpha release 

The current release of Entropy is version 0.x.x. You can learn more about the Entropy versioning scheme in the versioning
document. There will more than likely be breaking changes to the API for a while until we learn how things should be done.

