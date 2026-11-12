[![GitHub](https://img.shields.io/github/license/soren-n/tickle)](https://github.com/soren-n/tickle/blob/main/LICENSE)
[![PyPI](https://img.shields.io/pypi/v/tickle-soren-n)](https://pypi.org/project/tickle-soren-n/)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/tickle-soren-n)](https://pypi.org/project/tickle-soren-n/)
[![Discord](https://img.shields.io/discord/931473325543268373?label=discord)](https://discord.gg/bddF43Vk2q)
[![GitHub Sponsors](https://img.shields.io/github/sponsors/soren-n)](https://github.com/sponsors/soren-n)

# Tickle
A command line workflow automation tool which performs task graph scheduling and concurrent task evaluation.

Think of tickle as a generalised version of [ninja](https://github.com/ninja-build/ninja), i.e. not just for compiling native code projects, but for arbitrary concurrent evaluation of command line tasks. Tickle was initially conceived as a general backend for build systems, but can be used as a backend for any system that needs to schedule and evaluate command line tasks that produce and consume files concurrently.

Tickle takes as input a description of tasks to be performed and their dependencies; this is in the form of two files: _agenda_ and _depend_. It then compiles an evaluation schedule for these tasks; checking their input and output files for changes against a persistent cache, as well as checking status of task dependencies. I.e. only tasks that need to or can be re/evaluated will be scheduled.

# Install
Tickle requires Python >=3.11. It is distributed with pip and can be installed with the following example command:
```
$ python3 -m pip install tickle-soren-n
```

The pip install above will also install the following project dependencies:

- [PyYAML](https://github.com/yaml/pyyaml)
- [Watchdog](https://github.com/gorakhargosh/watchdog)

If you love Tickle, then please go give the author/maintainers of these projects some love too!

# Modes
Tickle has two evaluation modes:

- The __offline mode__ builds the task graph once and will schedule and generally evaluate once from there. However it will watch the _depend_ file for dynamic dependency changes, and reschedule as necessary.
- The __online mode__ will watch the _agenda_ file and the initial input files for changes, as well as the _depend_ file for dynamic dependency changes. It will then dynamically schedule and evaluate as the task graph changes.

In build system terms, offline mode is like a regular build, and online mode is like a watch/dev/live build. Both are incremental.

# Assumptions
Tickle assumes that it has full control over the input and output files described in the agenda. The only files that tickle supports modification to during runtime are: the agenda file, the depend file and the initial input files.

As such if you need to interface/overlap tickle with other systems in a workflow, it is safest that you do so modally; i.e. before tickle is run, and after it has terminated. However it should be safe to overlap your system's runtime with tickle's, iff your system only reads the generated files.

# Usage
Tickle can be used through two interfaces; the CLI and the API.

## CLI
```
usage: tickle [-h] [--debug] [-w WORKERS] [-a AGENDA] [-d DEPEND] [-c CACHE] [-l LOG]
              {offline,online,clean,version}

Task graph scheduling with asynchronous evaluation.

positional arguments:
  {offline,online,clean,version}
                        Offline mode for an inattentive evaluation mode where file
                        modifications are ignored once tasks have been scheduled. Online
                        mode for an attentive evaluation mode where file creations or
                        modifications trigger a rescheduling of the task graph. Clean
                        mode will delete all files and folders generated during offline
                        or online evaluation. Version mode will print the tool version.

options:
  -h, --help            show this help message and exit
  --debug               Sets debug logging level for tool messages (default: False)
  -w, --workers WORKERS
                        The number of concurrent workers; defaults to the number of
                        logical cores minus one for the main thread (default: e.g. 7)
  -a, --agenda AGENDA   Agenda YAML file location; contains the procedure and task
                        definitions, file path must be relative to current working
                        directory (default: agenda.yaml)
  -d, --depend DEPEND   Depend YAML file location; contains a map of dynamic task
                        dependencies, this file is optional, file path must be relative
                        to current working directory (default: depend.yaml)
  -c, --cache CACHE     Binary cache file location; contains inter-run persistent data,
                        file path must be relative to current working directory
                        (default: tickle.cache)
  -l, --log LOG         Log file location; contains runtime messages, file path must be
                        relative to current working directory (default: tickle.log)
```
The CLI has two additional modes to offline/online:

- The __clean mode__ will delete any files or folders generated during a previous offline or online evaluation.
- The __version mode__ will print the installed version of tickle.

The clean mode will only delete generated folders if they are empty after generated files are deleted; i.e. if there are leftover files in the folders, e.g. generated from other processes not within the control of tickle; these files and the host folders are then left untouched.

If you stick to the default paths and file names, then running tickle should be as simple as:
```
$ cd my_workflow
$ tickle MODE
```
Where `MODE` is one of offline, online, clean or version.

## API
The API is accessible in case you wish to run one of the evaluation modes as part of your workflow scripts; rather than spawning a subprocess for tickle.
The basic setup is that your script generates an agenda, stores it, and then runs tickle through the api.

All paths given to the API must be absolute, and the agenda, depend and cache paths must lie within the target directory. Each mode also takes an optional `worker_count`, which defaults to the number of logical cores minus one.

For example running tickle in offline mode could look something like this:
```Python
from pathlib import Path

import tickle.api as tickle_api
from tickle import agenda

def my_workflow(target_dir: Path) -> bool:

    # Paths
    agenda_path = tickle_api.default_agenda_path(target_dir)
    depend_path = tickle_api.default_depend_path(target_dir)
    cache_path = tickle_api.default_cache_path(target_dir)

    # Make and store agenda
    agenda_data = make_agenda(target_dir)
    agenda.store(agenda_path, agenda_data)

    # Run tickle offline; returns whether every task succeeded
    return tickle_api.offline(
        target_dir,
        agenda_path,
        depend_path,
        cache_path
    )
```

In online mode, `tickle_api.online` returns an evaluator instead of running it. Its `start` method blocks until `stop` is called, so run it in a thread of its own:
```Python
from pathlib import Path
from threading import Thread

import tickle.api as tickle_api
from tickle import agenda

def my_workflow(target_dir: Path) -> None:

    # Paths
    agenda_path = tickle_api.default_agenda_path(target_dir)
    depend_path = tickle_api.default_depend_path(target_dir)
    cache_path = tickle_api.default_cache_path(target_dir)

    # Make and store agenda
    agenda_data = make_agenda(target_dir)
    agenda.store(agenda_path, agenda_data)

    # Make the online evaluator; None if the agenda file is missing
    evaluator = tickle_api.online(
        target_dir,
        agenda_path,
        depend_path,
        cache_path
    )
    if evaluator is None: return

    # Run it in the background
    runner = Thread(target = evaluator.start)
    runner.start()

    # Do other stuff, e.g. modify the agenda or the input files
    ...

    # Stop the evaluator
    evaluator.stop()
    runner.join()
```

To remove the generated files again, call `tickle_api.clean(target_dir, cache_path)`.

TODO: Make full documentation for API

## The agenda file
The agenda file is a YAML file with the follow grammar:
```
procs:
  <proc name>:
    - <command word 1>
    - <command word 2>
    ...
  ...
flows:
  <flow name>:
    - [ <proc name 1>, <proc name 2>, ... ]
    ...
  ...
tasks:
  - desc: <task description>
    proc: <proc name>
    args:
      <param name>:
        - <arg value 1>
        - <arg value 2>
        ...
      ...
    flows: [ <flow name>, ... ]
    inputs:
      - <file path 1>
      - <file path 2>
      ...
    outputs:
      - <file path 1>
      - <file path 2>
      ...
  ...
```
The procs section defines a dictionary of procedures.
A proc is defined with a name and it's implementation is a command as a list of string words.
A command word is a parameter if it is prefixed with $.

The flows section defines a list of workflows.
A workflow is a sequence of stages. A stage is a list of procs, defining which procs are allowed to be evaluated in parallel.
This is useful when you have a clear separation in the evaluation order between groups of tasks; you could achieve the same ordering without stages, by having a many-to-many dependency between the task groups that need separating, which however would be costly on the scheduler. So stages were added as both a semantic convenience as well as an optimisation.

Most likely you will only need to define one workflow; however there are cases where you would wish to evaluate two agendas in parallel, or with some overlap. In this case it is more beneficial to keep these two agenda in the same evaluation context; i.e. defining multiple workflows allows you to do that.

The tasks section defines a list of tasks.
A task is an instantiation of a proc.
The task description is set with the desc field.
The proc is selected with the proc field.
The proc parameters are given arguments via the args field.
An arg is a list of string values to be inserted into the proc's command.
The input and output files that the task ranges over are set with the inputs and outputs fields.

You should think of the agenda file as describing the explicit dependencies between files for a task, e.g. source file to object file in a code project build workflow.

For more context please check out the examples directory.

## The depend file
The depend file is a YAML file with the following grammar:
```
<file path>:
  - <file path 1>
  - <file path 2>
  ...
...
```
The file defines a dictionary of file path to list of file paths, i.e. a file path dependency graph.

You should think of the depend file as describing the implicit dependencies between files for a task, i.e. uncovered by scanning the content of the files; e.g. source file to header file in a code project build workflow; the source file to header file dependencies change more often during development than the task dependencies mentioned earlier, and as such should be defined in the depend file and not the agenda file.

For more context please check out the examples directory.

# Examples
The example project named hello_world is a simple C++ build example. To build the project in watch/dev mode; run the following command line:
```
$ cd tickle/examples/hello_world
$ tickle online
```

# Development
The project is managed with [uv](https://docs.astral.sh/uv/). The tests are property-based, written with [Minigun](https://github.com/soren-n/minigun-py), and require Python >=3.12:
```
$ uv sync
$ uv run minigun --time-budget 60
```
