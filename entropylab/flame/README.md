# Entropylab ++

Next version of `entropylab` to run with Flame executor. Everything written here
refers to the planned new version.

## Key requirements

- natively distributed, connects heterogenous asynchronous systems, no
  bottlenecks
- low latency between nodes, bare metal access
- allows modular building of experiment, and good observability

## Realisation

User uses just two main ingredients.

- **Node I/O library**, that is used within the code of individual node,
  provided for given programming language of choice. Client library provides
  definition of node interface thorugh inputs `entorpylab.inputs`, outputs
  `entropylab.outputs`. Cleint library will use runtime context to "magically"
  provide correct bindings. When used in node development context, client libary
  will generate schema JSON for the node specifying inputs and outputs.

- **Workflow library** `entropylab.workflow` written only in Python. It uses
  JSON schema produced by cleint library for each node to automatically generate
  Python class API helps to autocomplete node when writing workflow
  specification as a code.

We will also provide template for writting the **node as a state machine**.

Away from the user are two more software elements that take care of actual
optimal execution.

- **Flame executor** runs the actual workflow specified by the user. It provides
  runtime context for each node which is run as indenpendant process, and
  resolves inputs and outputs as parameters, [ZeroMQ](https://zeromq.org/)
  communicated values, serialized with
  [MessagePack](https://msgpack.org/index.html). Executor sets up paths, and
  lets the system run once everything is set up. It also provides service
  discovery for any dynamically added node, or dynamically specified (runtime
  generated) node interdependancies.

- **Flame compiler** analyses QUA nodes, and tries to optimize them, using
  precompilation, run-time config completion on paused machines, optimisation of
  execution seqences of connected QUA nodes. Uses draft _Graph execution API_
  (to be developed later; specification for this exists already elsewhere).
  Added note: This should be merged as part of nodeio and workflow logic, and
  abstracted away from user.

This architecture allows easy debugging, though looking on zeromq
inputs/outputs, sandboxed node development next to running experiment, and if
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
  - **finite-life** : these have defined persistance between 0 and infinity.
    They are streamed to the timeseries database.
  - **permanent** : these are streamed to the time series database, and after
    run these are saved into persistant storage

**Permanent** data can be further classified

- based on _data reduction_ (taken care by time series database) on

  - **last-value** : only last time series value is saved
  - **downsampled** : downsampling is done in time series database
  - **all-values** : full time-series are saved

- based on _indexability_
  - **indexable**: simple numbers, strings etc: they are saved both in HDF5 and
    in PostgreSQL to allow later on relational queries.
  - **non-indexable** : large datasets, etc, they are saved only in HDF5.

By default all outputs should be non-indexable (until user learns how to use
advanced relational queries).

## Single addressing space

All addressing is done through EUI. It can be

- **absoulte** `#/runtimeId/nodeId/inputId` `#/runtimeId/nodeId/outputId`
- **relative** `#node/input` which is within current run
- **queried** `#/last/nodeId/input` `#/last[date]/nodeId/input`

EUI resolution has logic to provide from required place data. Even if system
might be physically distributed, all resources are effectively files, in
hirearchical file system, and there is single coherent name space (this is
inspired by Plan9 and Inferno distributed OS). This simply maps also to keys of
timeseries database, as well as to folder structure of HDF5 files.

## Executing on distributed runtime

Initial project will execute ndoes on single machine, and require individual
nodes to handle connections and similar if they are using extra resources (e.g.
cloud). However, architecture of the system has to be such that it can include
trully distributed execution environment, including multiple bare-metal
instances, cloud infrastructure integration etc. Workflow for this will be same
as single machine deployment, with all code written on one machine using code
server, except that nodes will have specified target runtimes.

How it can work? For authoring, VS from runtime terminal can be used with
RemoteSSH extension to author nodes on all relevant parts of the runtime. When
authoring nodes, their python descriptions will be all copied also to central
repo and runtime terminal environment. Alternatively, one can set up just that
node should be executed on another hardware and entropy executor should setup
and run node there.

For running, only one processon runtime terminal machine is listening to the job
queue. That process triggers executor on that hardware. If executor finds that
workflow includes nodes runnning on additional hardware, and this is the master
executor process, it will use signaling/ssh to trigger executor processes on
every additional runtime. All executor processes will wait after they have added
their own part of playbook for all other executors to finish writting the
playbook. This is signaled also on the playbook locaiton (key-value server).
Once this is resolved, individual nodes will be executed by respective
executors.

Simplest and cleanest implementaiton is that all peaces of hardware have
Ansimble playbook installation of very thin client that will listen to signals
from key-value database.
