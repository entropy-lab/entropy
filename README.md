[![discord](https://img.shields.io/discord/806244683403100171?label=QUA&logo=Discord&style=plastic)](https://discord.gg/7FfhhpswbP)

[![License](https://img.shields.io/badge/License-BSD%203--Clause-blue.svg)](https://opensource.org/licenses/BSD-3-Clause)

# entropy

Entropy is a lab workflow managment package built for, but not limitied-to, streamlining the process of running quantum information processing experiments. 

Entropy is built to solve three major problems: data collection, device managment and calibration automation. 

The device managment problem is handled via shared resources. 

Collecting and arranging experimental data is automated and via the graph node structure. The current release includes a database implemetation but 
the system is built with customization in mind, allowing deploy a host of different backends.


## Installation

Installation is done from pypi using the following command

`pip install entropylab`

## Testing your installation

import the library by running `from entropylab import *`

```
def my_func():
    return {'res':1}

node1 = PyNode("first_node", my_func,output_vars={'res'})
experiment = Graph(None, {node1}, "run_a") #No resources used here
handle = experiment.run()
```

## Usage

See jupyter notebooks in this repository



