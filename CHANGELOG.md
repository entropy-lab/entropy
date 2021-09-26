# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Added
* New HDF5 support

## [0.1.2]
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

[Unreleased]: https://github.com/entropy-lab/entropy/compare/v0.1.2...HEAD
[0.1.2]: https://github.com/entropy-lab/entropy/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/entropy-lab/entropy/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/entropy-lab/entropy/releases/tag/v0.1.0
