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
                        mode will delete the files and folders generated during offline
                        or online evaluation, keeping files that are now sources or that
                        changed since they were generated. Version mode will print the
                        tool version.

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
Clean also keeps a generated file that the current agenda or depend file now reads as a source, a file that changed after tickle last wrote it, and anything outside the current directory.

The exit code is 0 on success and 1 on failure: a task failed, the agenda or depend file is invalid, or offline mode skipped tasks that could not run. Interrupting offline mode with Ctrl-C exits with 130; the tasks that did not finish run again next time.

While evaluating, tickle prints each task as it starts. A task that succeeds but writes to stderr, such as a compiler warning, has that output printed as a warning, as does a task that succeeds without writing all of its outputs. A failing task's stderr, or its stdout if stderr is empty, is printed with the failure.

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

To remove the generated files again, call `tickle_api.clean(target_dir, cache_path, agenda_path, depend_path)`; the agenda and depend paths are optional, and let clean keep generated files they now read as sources.

See [docs/api.md](https://github.com/soren-n/tickle/blob/main/docs/api.md) for the full API reference.

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
A proc is defined with a name and its implementation is a command as a list of string words.
A command word is a parameter if it is prefixed with $; every other word is passed to the command exactly as written, spaces and all.
Commands are run directly, not through a shell, so shell syntax such as pipes, redirection or globs has no special meaning; use a proc like `[ sh, -c, ... ]` if you need it.

The flows section defines a list of workflows.
A workflow is a sequence of stages. A stage is a list of procs, defining which procs are allowed to be evaluated in parallel.
Within a workflow, every task of a stage is started only after the tasks of the earlier stages, and a task may not depend on a task of a later stage. A proc can be part of only one stage per workflow.
This is useful when you have a clear separation in the evaluation order between groups of tasks; you could achieve the same ordering without stages, by having a many-to-many dependency between the task groups that need separating, which however would be costly on the scheduler. So stages were added as both a semantic convenience as well as an optimisation.

Most likely you will only need to define one workflow; however there are cases where you would wish to evaluate two agendas in parallel, or with some overlap. In this case it is more beneficial to keep these two agenda in the same evaluation context; i.e. defining multiple workflows allows you to do that.

The tasks section defines a list of tasks.
A task is an instantiation of a proc.
The task description is set with the desc field.
The proc is selected with the proc field.
The proc parameters are given arguments via the args field.
An arg is a list of string values to be inserted into the proc's command; each value becomes one word of the command, in place of the parameter.
The input and output files that the task ranges over are set with the inputs and outputs fields; paths are relative to the agenda file's folder.

A task is rerun when:

- it changed, i.e. its proc's command, its args, flows, inputs or outputs differ from when it last succeeded;
- the content of an input, or of a file an input depends on through the depend file, differs from when the task last succeeded;
- one of its outputs is missing;
- a task it depends on is rerun.

A task records its inputs only when it succeeds, so a task that failed or was interrupted runs again on the next run, even if nothing changed in between.

A task with an input that does not exist and that no task produces is skipped, together with the tasks that depend on it. The rest of the agenda is still evaluated, and offline mode then fails. Inputs must be files; a task with a folder as an input is skipped the same way.

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
The file defines a dictionary of file path to list of file paths, i.e. a file path dependency graph; paths are relative to the depend file's folder.
The dependencies must not form a cycle, neither among themselves nor together with the tasks of the agenda.
When a task's input depends, directly or not, on a file that another task produces, the task runs after that task, as if the file were one of its inputs; the stage rules of flows apply to such dependencies too.

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

The code is checked with ruff, for bugs only since the project keeps its own formatting, and with pyright:
```
$ uv run ruff check .
$ uv run pyright
```

CI runs both, and the tests on Linux, macOS and Windows.

See [docs/architecture.md](https://github.com/soren-n/tickle/blob/main/docs/architecture.md) for how tickle schedules and evaluates tasks, and [docs/releasing.md](https://github.com/soren-n/tickle/blob/main/docs/releasing.md) for how to cut a release.
