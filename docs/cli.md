# Entropy projects and the CLI 

When running multiple experiments in the lab, it's good to have a way to divide and conquer. Entropy allows you to divides such work packages into `project`s. 

## Starting a project

When working from the command prompt, initializing a project is similar to initializing a git repository.
You need to create a new folder for your project, change the working directory to that folder and then use
the entropy Command Line Interface (CLI) to initialize the project.

```shell
mkdir my_proj
cd my_proj
entropy init
```

The directory will now contain a `.entropy` sub-directory where the sqlite database file and the hdf5 data
file are stored. The project directory's name is effectively the project's name.

```
📁 my_proj    <--- project directory
  📁 .entropy
    📄 entropy.db
    📄 entropy.hdf5
```

You can then place the experiment files under `my_proj`: 
```
📁 my_proj    <--- project directory
  📁 .entropy
    📄 entropy.db
    📄 entropy.hdf5
  📄 experiment1.py
  📄 experiment2.py
```

## The CLI

The entropy CLI in installed when you install entropy with `pip`.
```shell
pip install entropylab
```
The CLI currently support two commands: `init` and `upgrade`.

### `init`

```shell
entropy init <path to project directory>
```

Initializes a new project in the given directory (as described above)

### `upgrade`

```shell
entropy upgrade <path to entropy project directory>
```
Takes an entropy db that predates the project structure (before version 0.3.0) and updates it as needed.

These are the steps executed:
1. Moves the `.db` file (and corresponding `.hdf5` file, if it exists) to a new project directory. 
The directory name will be the original `.db` file's name.
2. Upgrades the `.db` file to the latest version of Entropy (if needed).
3. Migrates experiment results and metadata from the `.db` file to `.hdf5` (if needed).