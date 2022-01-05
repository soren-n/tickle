# tickle
A command line workflow automation tool which performs task graph scheduling and concurrent task evaluation.

Think of tickle as a generalised version of [ninja](https://github.com/ninja-build/ninja), i.e. not just for compiling native code projects, but for arbitrary concurrent evaluation of command line tasks. Tickle was initially conceived as a general backend for build systems, but can be used as a backend for any system that needs to schedule and evaluate command line tasks that produce and consume files concurrently.

Tickle takes as input a description of tasks to be performed and their dependencies; this is in the form of two files: _agenda_ and _depend_. It then compiles an evaluation schedule for these tasks; checking their input and output files for changes against a persistent cache, as well as checking status of task dependencies. I.e. only tasks that need to be re/evaluated will be scheduled.

# Install
Tickle is currently only supported for Python >=3.9, although it might work with older versions. It is distributed with pip and can be installed with the following command:
```
$ python3 -m pip install tickle-soren-n
```

The pip install above will also install the following project dependencies:

- [PyYAML](https://github.com/yaml/pyyaml)
- [Watchdog](https://github.com/gorakhargosh/watchdog)

# Modes
Tickle has three modes:

- The __static mode__ builds the task graph once and will schedule and evaluate from this. However it will also watch the depend file for dynamic dependency changes, and reschedule as necessary.
- The __dynamic mode__ will watch the agenda file and initial input files for changes, as well as the depend file for dynamic dependency changes. It will then dynamically schedule and evaluate as the task graph changes.
- The __clean mode__ will delete any files or folders generated during a previous static or dynamic evaluation.

In build system terms, static mode is like a regular full build, and dynamic mode is like a watch/dev/live build. Both are incremental.

The clean mode will only delete generated folders if they are empty after generated files are deleted; i.e. if there are leftover files in the folders, e.g. generated from other processes not within the control of tickle; these files and the host folders are then left untouched.

# Assumptions
Tickle assumes that it has full control over the input and output files described in the agenda. The only files that tickle supports modification to during runtime are: the agenda file, the depend file and the initial input files.

As such if you need to interface/overlap tickle with other systems in a workflow, it is safest that you do so modally; i.e. before tickle is run, and after it has terminated. However it should be safe to overlap your system's runtime with tickle's, iff your system only reads the generated files.

# Usage
```
usage: tickle [-h] [--debug] [-w WORKERS] [-a AGENDA] [-d DEPEND] [-c CACHE] [-l LOG]
              {static,dynamic,clean,version}

Task graph scheduling with asynchronous evaluation.

positional arguments:
  {static,dynamic,clean,version}
                        static for an inattentive evaluation mode where file modifications are ignored
                        once tasks have been scheduled, dynamic for an attentive evaluation mode where
                        file creations or modifications trigger a rescheduling of the task graph; clean
                        mode will delete all files and folders generated during static or dynamic
                        evaluation; version mode will print the tool version

optional arguments:
  -h, --help            show this help message and exit
  --debug               Sets debug logging level for tool messages (default: False)
  -w WORKERS, --workers WORKERS
                        The number of concurrent workers; defaults to the number of logical cores minus
                        one for the main thread (default: <logical core count minus one>)
  -a AGENDA, --agenda AGENDA
                        Agenda YAML file location; contains the procedure and task definitions, file path
                        must be relative to current working directory (default: agenda.yaml)
  -d DEPEND, --depend DEPEND
                        Depend YAML file location; contains a map of dynamic task dependencies, this file
                        is optional, file path must be relative to current working directory (default:
                        depend.yaml)
  -c CACHE, --cache CACHE
                        Binary cache file location; contains inter-run persistent data, file path must be
                        relative to current working directory (default: tickle.cache)
  -l LOG, --log LOG     Log file location; contains runtime messages, file path must be relative to
                        current working directory (default: tickle.log)
```
If you stick to the default paths and file names, then running tickle should be as simple as:
```
$ cd my_workflow
$ tickle MODE
```
Where `MODE` is one of static, dynamic or clean.

## The agenda file
The agenda file is a YAML file with the follow grammar:
```
procs:
  <proc name>:
    - <command word 1>
    - <command word 2>
    ...
stages:
  - [ <proc name 1>, <proc name 2>, ... ]
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

The stages section defines a list of stages.
A stage is a list of procs, defining which procs are allowed to be evaluated in parallel. This is useful when you have clear separation in the evaluation order of tasks; you could achieve the same ordering without stages, by having a many-to-many dependency between the tasks that need separating, which is costly on the scheduler. So stages where added both as a semantic convenience as well as an optimisation.

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

You should think of the depend file as describing the implicit dependencies between files for a task, i.e. uncovered by scanning the content of the files; e.g. source file to header file in a code project build workflow; the source file to header file dependencies change more often during development than the static dependencies mentioned earlier, and as such should be defined in the depend file and not the agenda file.

For more context please check out the examples directory.

# Examples
The example project named hello_world is a simple C++ build example. To build the project in watch/dev mode; run the following command line:
```
$ cd tickle/examples/hello_world
$ tickle dynamic
```
