## Pipeline

Entropy revolves one central structure: an execution graph. 
The nodes of a graph give us a convenient way to brake down experiments into stages and to automate some repetitive tasks.
Pipeline is an execution engine and api for writing it.

## Hello world

```python
from entropylab import *

def my_func():
    return {'res': 1}

node1 = PyNode("first_node", my_func, output_vars={'res'})
experiment = Graph(None, {node1}, "run_a")  # No resources used here
handle = experiment.run()
```