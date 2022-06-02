# Flame framework

Next version of `entropylab` to run with Flame executor.

For more information refer to official documentation at [entropy-lab.io](https://entropy-lab.io)

## Key requirements

- natively distributed, connects heterogenous asynchronous systems, no
  bottlenecks
- low latency between nodes, bare metal access
- allows modular building of experiment, and good observability

## Realisation

User uses just two main ingredients.

- **Node I/O library**, that is used within the code of individual node,
  provided for given programming language of choice. Client library provides
  definition of node interface thorugh inputs `entorpylab.flame.inputs`, outputs
  `entropylab.flame.outputs`. Cleint library will use runtime context to "magically"
  provide correct bindings. When used in node development context, client libary
  will generate schema JSON for the node specifying inputs and outputs.

- **Workflow library** `entropylab.flame.workflow` written only in Python. It uses
  JSON schema produced by cleint library for each node to automatically generate
  Python class API helps to autocomplete node when writing workflow
  specification as a code.


Away from the user are two more software elements that take care of actual
optimal execution.diment, and if
used with fully saved runs in timeseries database, can allow node development on
"synthetic" experiment (so far as node emulates feedback response for signals it
generates). Note that some nodes might run always (e.g. temperature
controllers). Current state is saved in local key-value database, that also
allows system state recovery if executor crashes, or if whole system suffers
power loss.

## Input/Output magic

Inputs can be classified

- based on _behaviour_ on

  - **state** variable, set temperature on air-conditioning. This input blocks
    until the first resolution. After that user can check what was last time it
    was updated.
  - **flow** variable, like image on processing pipeline. This input blocks
    until resolution is received, and then that resolutoin is consumed. When
    trying to fetch new value of the input, input will block until new value is
    available.

- based on _resolution_ on
  - **runtime** variable, that becomes known only during the run of the
    experiment. It can be for example measurement result of one node that is
    provided to another node.
  - **parameter** variable, that is resolved before running workflow, using
    values entered by the user or queried from the existing variables in the
    Data Store.

Outputs can be classified

- based on _persistance_ on

  - **runtime-only** : these are available only to other node during workflow
    run. All variables between nodes are passed using ZeroMQ.
  - **finite-life** : these persist during single experimental run.
    They are streamed to the timeseries database.
  - **permanent** : these are streamed to the time series database, and after
    run these are saved into persistant storage

## Single addressing space

All addressing is done through EUI. It can be

- **absoulte** `#/jobID/nodeID/outputID` `#/jobID/nodeID/outputID`
- **relative** `#nodeID/outputID` which is within current run

EUI resolution has logic to provide from required place data. Even if system
might be physically distributed, all resources are effectively files, in
hirearchical file system, and there is single coherent name space (this is
inspired by Plan9 and Inferno distributed OS). This simply maps also to keys of
timeseries database, as well as to folder structure of HDF5 files.

## Executing on distributed runtime

Initial project will execute nodoes on single machine, and require individual
nodes to handle connections and similar if they are using extra resources (e.g.
cloud). However, architecture of the system has to be such that it can include
trully distributed execution environment, including multiple bare-metal
instances, cloud infrastructure integration etc. Workflow for this will be same
as single machine deployment, with all code written on one machine using code
server, except that nodes will have specified target runtimes.
