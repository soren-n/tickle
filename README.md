[![GitHub](https://img.shields.io/github/license/soren-n/tickle)](https://github.com/soren-n/tickle/blob/main/LICENSE)
[![PyPI](https://img.shields.io/pypi/v/tickle-soren-n)](https://pypi.org/project/tickle-soren-n/)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/tickle-soren-n)](https://pypi.org/project/tickle-soren-n/)
[![Discord](https://img.shields.io/discord/931473325543268373?label=discord)](https://discord.gg/bddF43Vk2q)
[![GitHub Sponsors](https://img.shields.io/github/sponsors/soren-n)](https://github.com/sponsors/soren-n)

# Tickle
A command line workflow automation tool which performs task graph scheduling and concurrent task evaluation.

Tickle is made for pipelines that a script generates: long running tasks over large files, such as converting, rendering or encoding many files in several steps. The script writes an agenda of tasks, each a command with the files it reads and writes, and tickle runs them in dependency order, as many at a time as you allow, and on the next run only the tasks whose command or input files changed, together with the tasks after them. Like [ninja](https://github.com/ninja-build/ninja), it is a backend that executes a task graph rather than a language for writing one; unlike ninja, it is not tuned for builds of many small, fast compile steps.

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

Online mode applies a change once it has settled for a moment, without waiting for running tasks: a running task whose definition and inputs did not change carries on, and one whose did is stopped and started again. A generated file that is deleted is built again at once.

Each task starts as soon as the tasks it reads from have finished, and those of the earlier stages of its flows. When several tasks are ready, the one heading the longest chain of work starts first, judged by how long the tasks took last time, so long chains do not end up last. Only one run can use a project at a time; a second one fails at once, naming the run that holds it.

# Assumptions
Tickle assumes that it has full control over the input and output files described in the agenda. The only files that tickle supports modification to during runtime are: the agenda file, the depend file and the initial input files.

As such if you need to interface/overlap tickle with other systems in a workflow, it is safest that you do so modally; i.e. before tickle is run, and after it has terminated. However it should be safe to overlap your system's runtime with tickle's, iff your system only reads the generated files.

# Usage
Tickle can be used through two interfaces; the CLI and the API.

## CLI
```
usage: tickle [-h] [-k] [--stop-on-failure] [-n] [--log-dir LOG_DIR] [--debug]
              [-w WORKERS] [-a AGENDA] [-d DEPEND] [-c CACHE] [-l LOG]
              {offline,online,clean,version} [TARGET ...]

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
  TARGET                Run only what these need: task descriptions or output files; all
                        tasks if none are given (default: None)

options:
  -h, --help            show this help message and exit
  -k, --keep-going      Offline mode: after a task fails, keep running the tasks that do
                        not read from it (default: False)
  --stop-on-failure     Online mode: after a task fails, start nothing more until the
                        next change (default: False)
  -n, --dry-run         Offline mode: print the tasks that would run, without running
                        them (default: False)
  --log-dir LOG_DIR     Folder for the output of each task, one log file per task; must
                        be inside the current working directory (default: tickle-logs)
  --debug               Sets debug logging level for tool messages (default: False)
  -w, --workers WORKERS
                        The number of concurrent workers; defaults to the number of
                        logical cores minus one for the main thread (default: e.g. 7)
  -a, --agenda AGENDA   Agenda YAML file location; contains the procedure and task
                        definitions, must be inside the current working directory
                        (default: agenda.yaml)
  -d, --depend DEPEND   Depend YAML file location; contains a map of dynamic task
                        dependencies, this file is optional, must be inside the current
                        working directory (default: depend.yaml)
  -c, --cache CACHE     Binary cache file location; contains inter-run persistent data,
                        must be inside the current working directory (default:
                        tickle.cache)
  -l, --log LOG         Log file location; contains runtime messages, must be inside the
                        current working directory (default: tickle.log)
```
The CLI has two additional modes to offline/online:

- The __clean mode__ will delete any files or folders generated during a previous offline or online evaluation.
- The __version mode__ will print the installed version of tickle.

The clean mode will only delete generated folders if they are empty after generated files are deleted; i.e. if there are leftover files in the folders, e.g. generated from other processes not within the control of tickle; these files and the host folders are then left untouched.
Clean also keeps a generated file that the current agenda or depend file now reads as a source, a file that changed after tickle last wrote it, and anything outside the current directory.

The exit code is 0 on success and 1 on failure: a task failed, the agenda or depend file is invalid, offline mode skipped tasks that could not run, or another run is using the project. An invalid command line, such as a cache file outside the current directory or in a folder that does not exist, exits with 2. Interrupting offline mode with Ctrl-C exits with 130, and SIGTERM with 143; either way the running tasks are stopped, and the tasks that did not finish run again next time. Online mode stops on Ctrl-C or SIGTERM, stopping its running tasks.

Tasks run in a process group of their own, so stopping a task, at a timeout or an interrupt, stops the processes it started too: tickle asks them to end and kills them after five seconds.

Without `--keep-going`, nothing more starts after a task fails; the tasks already running finish and are recorded. With it, every task that does not read from the failed task, directly or through other tasks, still runs; a stage counts a failed task as finished, so the later stages of its flows go on.

Give targets after the mode, as task descriptions or output files, to run only them and the tasks they need: `tickle offline out/final.mp4`. `--dry-run` prints the tasks that would run, in an order they could run in, and why each would run (new or changed, an input changed, an output is missing, or it reads from a task that runs), without running them. A task that only reads from tasks that run may still be left alone, if they write what they wrote before.

While evaluating, tickle prints each task as it starts, and when it finishes, with how long it took and how many of the tasks to run have finished; offline mode ends with how many tasks ran and how long it took; the lines are flushed at once, so they also appear as they happen when the output goes to a pipe or a file. Each task's output, stdout and stderr together, goes to a log file of its own in `tickle-logs` (set with `--log-dir`), replaced each time the task runs. When a task fails, the end of its output and the path of its log are printed as soon as it fails. A task that succeeds without writing all of its outputs is reported.

If you stick to the default paths and file names, then running tickle should be as simple as:
```
$ cd my_workflow
$ tickle MODE
```
Where `MODE` is one of offline, online, clean or version.

## API
The API is accessible in case you wish to run one of the evaluation modes as part of your workflow scripts; rather than spawning a subprocess for tickle.
The basic setup is that your script generates an agenda, stores it, and then runs tickle through the api.

All paths given to the API must be absolute, and the agenda, depend and cache paths must lie within the target directory; paths left out take the defaults in the target directory. Each mode also takes an optional `worker_count`, which defaults to the number of logical cores minus one, and keyword options such as `keep_going`, `targets` and `dry_run`; `offline` takes the agenda as a path or as an `Agenda` object.

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

    # Run tickle offline; the result says what ran, failed or was
    # skipped, and is true when every task succeeded
    result = tickle_api.offline(
        target_dir,
        agenda_path,
        depend_path,
        cache_path,
        keep_going = True
    )
    return bool(result)
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

    # Make the online evaluator; None if the agenda file is missing or
    # another run is using the project
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
    depfile: <file path>
    pool: <pool name>
    timeout: <seconds>
    retries: <count>
    always: <true or false>
  ...
pools:
  <pool name>: <how many of its tasks may run at once>
  ...
```
Every section and every task field but desc and proc is optional.

The procs section defines a dictionary of procedures.
A proc is defined with a name and its implementation is a command as a list of string words.
A command word is a parameter if it is prefixed with $; every other word is passed to the command exactly as written, spaces and all.
Commands are run directly, not through a shell, so shell syntax such as pipes, redirection or globs has no special meaning; use a proc like `[ sh, -c, ... ]` if you need it.

The flows section defines a list of workflows.
A workflow is a sequence of stages. A stage is a list of procs, defining which procs are allowed to be evaluated in parallel.
Within a workflow, every task of a stage is started only after the tasks of the earlier stages, and a task may not depend on a task of a later stage. A proc can be part of only one stage per workflow. Dependencies must also not form a cycle together with the stage order of several workflows; for example, with two workflows of two stages each, a task in the first stage of one workflow reading the output of a task in the second stage of the other, and the other way around. Such an agenda is rejected.
Stages only order tasks: a task does not rerun because a task of an earlier stage did, unless it reads one of that task's outputs, and changing a task's flows does not make it rerun.
This is useful when you have a clear separation in the evaluation order between groups of tasks; you could achieve the same ordering without stages, by having a many-to-many dependency between the task groups that need separating, which however would be costly on the scheduler. So stages were added as both a semantic convenience as well as an optimisation.

Most likely you will only need to define one workflow; however there are cases where you would wish to evaluate two agendas in parallel, or with some overlap. In this case it is more beneficial to keep these two agenda in the same evaluation context; i.e. defining multiple workflows allows you to do that.

The tasks section defines a list of tasks.
A task is an instantiation of a proc.
The task description is set with the desc field.
The proc is selected with the proc field.
The proc parameters are given arguments via the args field.
An arg is a list of string values to be inserted into the proc's command; each value becomes one word of the command, in place of the parameter.
The input and output files that the task ranges over are set with the inputs and outputs fields; paths are relative to the agenda file's folder.
A task in no flow is ordered by its inputs alone.

A path that ends with `/` names a folder. A task may read a folder, such as a folder of frames, and is then rerun when any file in it, at any depth, changes, appears or goes. A task may write a folder: tickle creates it before the task runs, records the files the task left in it, and before the task runs again removes the files it recorded there that nobody has changed since, so no stale file of an earlier run is left behind. A task that reads a file inside another task's output folder runs after that task, and no task may write a path inside another task's output folder.

A task with a depfile, such as the `.d` file `gcc -MD` writes, has the files that depfile names added to its inputs once the task has succeeded: changing one of them reruns the task. The depfile is read in the make syntax compilers write, relative to the target directory. Files named only in a depfile do not order tasks; declare a generated header as an input, or in the depend file, so its task runs first.

The pools section limits how many tasks of a kind run at once, below the worker count, such as one task at a time on the GPU or two large encodes: a task names its pool with the pool field.
A task with a timeout is stopped and fails when it runs longer than that many seconds.
A task with retries runs again when it fails, up to that many times, before it counts as failed; use it for tasks that fail now and then for reasons outside them, such as a network share.
None of these fields change what a task is, so changing them does not make it rerun.

A task is rerun when:

- it changed, i.e. its proc's command, its args, inputs or outputs differ from when it last succeeded (its flows do not count);
- the content of an input, or of a file an input depends on through the depend file, differs from when the task last succeeded;
- one of its outputs is missing;
- a task it depends on reran and wrote something different.

When a task reruns and writes exactly what it wrote before, the tasks after it are left alone; this is checked when each of them is about to start.

A task records its inputs only when it succeeds, so a task that failed or was interrupted runs again on the next run, even if nothing changed in between.

A task with an input that does not exist and that no task produces is skipped, together with the tasks that depend on it. The rest of the agenda is still evaluated, and offline mode then fails. Inputs must be files; a task with a folder as an input is skipped the same way.

A task must have at least one output; a task without outputs is skipped the same way, unless it sets `always: true`, which makes it run on every run; nothing can depend on such a task. For a task that only has an effect elsewhere, such as uploading a file, that should run only when its inputs change, let its command also write a small stamp file and list that as its output.

To decide whether an input changed, tickle compares its size and modification time with those it saw when it last hashed it, and only hashes it again when they differ, or when the file was modified within two seconds of being hashed. A tool that changes a file's content but keeps its size and modification time, such as some that restore timestamps, goes unnoticed; touch the file after such a tool.

Paths may be written in any equivalent way: `out/x.txt`, `./out/x.txt`, `out/../out/x.txt` and the absolute path name the same file, and changing how a path is written does not make a task rerun. Tickle compares paths as written, without resolving symbolic links or ignoring case, so on a file system that ignores case `out/x.txt` and `OUT/X.txt` are one file to the system but two to tickle; spell each path one way.

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

An entry's key is a file that tasks read, and the entry makes those tasks rerun when the listed files change. Key entries by the source files that tasks take as inputs, not by the files they produce: the dependency output of a compiler such as `gcc -MD` is keyed by the object file, and an entry keyed by `main.o` would rerun the task that reads `main.o`, not the task that compiles it.

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

A run takes about four minutes. Most properties run the test task in-process; those about processes themselves start real ones, and their attempt counts are not limited by the time budget. Set `TICKLE_TEST_PROCESSES=1` to start a real process for every task, as the nightly CI run does (about eight minutes). Set `TICKLE_TEST_ATTEMPT_SCALE` (for example to `0.2`) to make fewer attempts, as CI does on Windows, and use `--select` with part of a property's description to run only matching properties.

The code is checked with ruff, for bugs only since the project keeps its own formatting, and with pyright:
```
$ uv run ruff check .
$ uv run pyright
```

CI runs both, and the tests on Linux, macOS and Windows.

See [docs/architecture.md](https://github.com/soren-n/tickle/blob/main/docs/architecture.md) for how tickle schedules and evaluates tasks, and [docs/releasing.md](https://github.com/soren-n/tickle/blob/main/docs/releasing.md) for how to cut a release.
