# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
## [Unreleased]
### Changed

### Added
* Flame: Support for saving single node dry-runs into EntropyHub


## [0.12.0]
### Added
* Single node dry run in Flame

## [0.11.2]
### Fixed
* Resolved high CPU issue when running the dashboard in Linux (#282)

## [0.11.1]
### Fixed
* Entropy serve CLI command now reports the serve path on the STD-IO 

## [0.11.0]
### Added
* spinner displayed while plots are loading for a selected experiment
* Caching of the `FigureRecords` in memory, Reducing the time to re-render plot tabs significantly.

## [0.10.0]
### Added
* Expiration field for parameters in paramstore (prep for calibration automation)
* Project size and remaining HDD space is now shown in header row


## [0.9.0]
### Changed
* Readme files pushed into modules folders
* Results dashboard layout changed to column view
### Added
* Support for Jupyter notebook node development in NodeIO

## [0.8.0]

### Added
* Added Flame execution engine with NodeIO library

### Changed
* Updated QuAM names

## [0.7.0] - 2022-05-31

### Added
* Added a --debug flag to CLI
* Added migration for paramStore v0.2

## [0.6.0] - 2022-05-25

### Added
* QuAM - Quantum Abstract Machine. An abstraction layer above qubits to help to build and run experiments. This is
a first implementation of this tool. 

## [0.5.6] - 2022-05-23

### Fixed
* ParamStore GUI now looks for param.db file in .entropy folder
* ParamStore GUI tags are now editable and correctly displayed

## [0.5.5] - 2022-05-16

### Added 
* ParamStore viewer added to the GUI
* Added Quantum Abstraction Machine (QUAM)
### Changed

* Success/Failure filter in results table is broken. A bug was issued


## [0.5.4] - 2022-05-08
### Changed
* Data now save in HDF5 file per experiment


## [0.5.3] - 2022-05-02
### Added
* Added methods to change key name and list tags per key in param store

## [0.5.2] - 2022-04-25
### Fixed
* Resolved a problem with logging errors occurring in entropy nodes

## [0.5.1] - 2022-04-13
### Changed
* Dashboard: changed plotting interface 
* DB: retrieving data based on node ID is fixed

## [0.5.0] - 2022-03-01
### Added
* add retry behavior for nodes - recovery from errors
* When serving the Results Dashboard, Dash's "dev tools" feature is always enabled. 
* added a paramStore: a mechanism for saving system state with a git-like interface

## [0.4.1] - 2022-01-23
### Changed
* Dashboard: Fixed auto plot to support Dict results

## [0.4.0] - 2022-01-11
### Added
* New results dashboard web application
* Dashboard: Auto plot last result
* Dashboard: Serve using production-grade WSGI server (waitress 2.0.0)
* Dashboard: Experiments table is refreshed from disk every 3 seconds

## [0.3.0] - 2021-10-11
### Added
* Entropy now is also a CLI utility see doc [here](entropylab/cli/cli.md).
* CLI utility to help upgrade the entropy project. 
### Changed
* Entropy project is now a directory with `.entropy` subdirectory with all entropy specific information.

## [0.2.0] - 2021-09-30
### Added
* New HDF5 support for data persistence

## [0.1.2] - 2021-06-07
### Added
* Pass "save results" as parameter for node, enabling to toggle data saving 

## [0.1.1] - 2021-05-11
### Fixed
* Package metadata

## [0.1.0] - 2021-05-10
### Added
* Directed Graph for experiment authoring - This is the hallmark feature of Entropy which enables breaking 
down an experiment into a graph of nodes which can run arbitrary code and pass data from node to node.
* Resource management - nodes in a graph share resourced which can be, but are not limited to, lab instruments.
This allows easy usage of resources and sharing of information as well as persistence of state and automated saving
of metadata representing the state of the entire lab.
* Data saving backend - Node data, and resource state data, is saved to a DB backend. 
This release support saving to a SQLite DB. The backend is extendable and additional persistence targets can be added.
* QCodes driver library adapter - an adapter for the drivers provided by QCodes to be easily used with the
Entropy persistence backend.
* QPU-DB - This is an extension to entropy which is built to save and manage the pieced of information describing 
a Quantum Processing Unit. If multiple nodes are set up to calibrate the QPU, measuring decoherence times and resonant frequencies for example, then that data can be saved to a centralized store and subsequently used in the target application. Warning: this module will be replaced in future releases but a migration path will be provided. 

[Unreleased]: https://github.com/entropy-lab/entropy/compare/v0.12.0...HEAD
[0.12.0]: https://github.com/entropy-lab/entropy/compare/v0.11.1...v0.12.0
[0.11.2]: https://github.com/entropy-lab/entropy/compare/v0.11.1...v0.11.2
[0.11.1]: https://github.com/entropy-lab/entropy/compare/v0.11.0...v0.11.1
[0.11.0]: https://github.com/entropy-lab/entropy/compare/v0.10.0...v0.11.0
[0.10.0]: https://github.com/entropy-lab/entropy/compare/v0.9.0...v0.10.0
[0.9.0]: https://github.com/entropy-lab/entropy/compare/v0.8.0...v0.9.0
[0.8.0]: https://github.com/entropy-lab/entropy/compare/v0.7.0...v0.8.0
[0.7.0]: https://github.com/entropy-lab/entropy/compare/v0.6.0...v0.7.0
[0.5.7]: https://github.com/entropy-lab/entropy/compare/v0.5.6...v0.6.0
[0.5.6]: https://github.com/entropy-lab/entropy/compare/v0.5.5...v0.5.6
[0.5.5]: https://github.com/entropy-lab/entropy/compare/v0.5.4...v0.5.5
[0.5.4]: https://github.com/entropy-lab/entropy/compare/v0.5.3...v0.5.4
[0.5.3]: https://github.com/entropy-lab/entropy/compare/v0.5.2...v0.5.3
[0.5.2]: https://github.com/entropy-lab/entropy/compare/v0.5.1...v0.5.2
[0.5.1]: https://github.com/entropy-lab/entropy/compare/v0.5.0...v0.5.1
[0.5.0]: https://github.com/entropy-lab/entropy/compare/v0.4.1...v0.5.0
[0.4.1]: https://github.com/entropy-lab/entropy/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/entropy-lab/entropy/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/entropy-lab/entropy/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/entropy-lab/entropy/compare/v0.1.2...v0.2.0
[0.1.2]: https://github.com/entropy-lab/entropy/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/entropy-lab/entropy/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/entropy-lab/entropy/releases/tag/v0.1.0
