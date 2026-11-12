# Architecture

This document describes how tickle turns an agenda into work and evaluates it. For the file formats see the README; for the Python entry points see [api.md](api.md).

## Overview

```
agenda.yaml --load/compile--> CompiledAgenda --_make_graph--> Task graph
depend.yaml --load/compile--> CompiledDepend --_update_depend--> input closures
                                                                     |
              tickle.cache <--> _make_schedule (validity, activity) <+
                                        |
                                 graph.compile --> Program --> Evaluator --> Workers
```

Every mode runs the same pipeline. Paths in the agenda are relative to the agenda file's directory, and paths in the depend file to the depend file's directory. Offline mode runs it once (and again if the depend file changes); online mode reruns parts of it whenever a watched file changes.

## Compiling the agenda

`agenda.load` reads the YAML into the `Agenda` dataclasses through `dataspec`. `agenda.compile` then produces a `CompiledTask` per task:

- **Command**: the task's proc is a list of words; words starting with `$` are parameters. The argument vector is built word by word: a literal word is kept exactly (spaces, quotes and `%` included), and a parameter word is replaced by its argument values, one word per value. A missing argument is an error.
- **Flows**: each flow is a list of stages, and each stage a list of procs. A task's `flows` map becomes `{flow: stage index}` for the flows it belongs to. A proc may appear in only one stage per flow.
- **Paths**: inputs and outputs are made absolute against the agenda file's directory.
- **Hash**: an MD5 of the task's proc, args, flows, inputs and outputs. It identifies the task across runs, independently of its position.

## Building the task graph

`_make_graph` creates an `evaluate.Task` per compiled task, whose work runs the command with `subprocess.run` in the target directory. Two tasks may not write the same output. A task depends on the task that produces each of its inputs; `Task.add_dependency` refuses a dependency on a task in a later stage of a shared flow.

## Depend closures

The depend file lists implicit dependencies between files, such as a source file including a header. `_update_depend` joins them with the agenda's input/output edges into one file graph, rejects cycles (searching from every file, so a cycle without an entry point is found too), and computes for each file the transitive closure of files it depends on. A task's input closure is its inputs plus their closures; a change to any file in it makes the task out of date.

It also returns the implicit files (in the closures but not named by the agenda), which online mode watches.

## Deciding what to run

`_make_schedule` marks each task valid (up to date) and active (can run), using the cache:

1. **Recovery.** Cached input hashes are keyed by task name (`task0`, `task1`, ...). The `recover` map from task hash to name carries hashes over when tasks are reordered, so an edit that only moves a task does not rerun it. A task whose hash is new (added, or with a changed proc, args, flows, inputs or outputs) starts without hashes and is added to `pending`; a pending task is out of date until it has run successfully.
2. **Impossible tasks.** In topological order, a task with no outputs, or with an input that neither exists nor is produced by an earlier task, is made inactive and skipped with an error.
3. **Input changes.** If the task's input closure differs from the cached one, or any file in it has a different MD5 than cached, the task is invalid.
4. **Missing outputs.** If any output is missing, the task is invalid.
5. **Propagation.** `graph.propagate` walks the graph in topological order; a task depending on an invalid task becomes invalid, and one depending on an inactive task becomes inactive.

After a task runs successfully, its cached input hashes are refreshed and it is removed from `pending`.

Because each input closure is transitive, a changed file invalidates every task downstream of it through that task's own hashes. Propagation matters for tasks that are out of date without any file changing, such as a pending task: the tasks after it rerun in the same run, since its outputs may change.

## Compiling a program

`graph.compile` reduces the invalid, active tasks to a `Program`:

- A **sequence** is a chain of tasks where each has exactly one pending dependency, that dependency has exactly one pending dependent, and both are in the same stages. A sequence runs on one worker, in order.
- A **batch** is a set of sequences that can run in parallel: their dependencies all lie in earlier batches.
- Batches are computed per flow and stage, then combined into one list.

## Evaluation

`Evaluator.start` starts the worker threads and then loops: it takes the next batch, puts each sequence on a queue, and waits for the queue to drain before moving on. Workers take sequences off the queue and run their tasks. A failing command, or one that cannot be started, raises `TaskError` in the worker, which passes it to the main loop; any other exception in a worker is reported the same way, so a batch always drains. The main loop calls `on_task_error`, clears the remaining program and continues.

`pause`/`resume` take the write side of a reader/writer lock. Workers hold the read side while they take and run a sequence, and the main loop while it queues a batch, so pausing waits for running sequences to finish and reprogramming never races with them. `stop` may be called from any thread, more than once; it ends the loop, and the workers are then stopped and joined.

When there is no program the main loop waits on an event that reprogramming and stopping set, with a one-second timeout as a fallback; idle workers check for work every 50 milliseconds.

## Offline mode

`OfflineEvaluator` builds the graph once and adds a forced terminating task that depends on every other task and calls `stop`. Its `on_task_error` re-raises, so `offline()` returns `False` on the first failing task. It watches the depend file and reschedules if it changes during the run; an invalid depend file is reported and the current schedule kept. The watcher is stopped by `start` after the main loop ends, not by `stop`, which runs on the worker performing the terminating task.

## Online mode

`OnlineEvaluator` watches the agenda, the depend file, every explicit input that no task produces, and every implicit file. On a change it compares the file's content hash with the last one seen and, if it differs, pauses, rebuilds what the change affects, reprograms and resumes:

- agenda: recompile the agenda, rebuild the graph, the closures and the schedule;
- depend: recompute the closures and the schedule;
- input file: recompute the schedule.

A change is applied only if everything rebuilds: an agenda or depend file that does not load, or that introduces a cycle, is reported and the current schedule kept. A removed depend file counts as an empty one; a removed agenda is reported and ignored.

A failing task is logged and the rest of the schedule dropped; the next change schedules again. Online mode runs until `stop` is called (the CLI does so on Ctrl-C).

## The cache

`tickle.cache` is a pickled dict. Workers update it under its lock, and it is written to `tickle.cache.tmp` and renamed into place, so a crash never leaves a truncated cache; an unreadable cache is reported and treated as empty.

| Key | Contents |
| --- | --- |
| `files` | Output files tickle has generated |
| `folders` | Directories tickle created for outputs |
| `recover` | Task hash to task name |
| `hashes` | Task name to `{input path: MD5}` for its input closure |
| `pending` | Hashes of tasks that have not yet run successfully since they last changed |

`clean` deletes the generated files, then the generated folders that are empty, then the cache.

## Threads

- The caller's thread runs the evaluator's main loop.
- Worker threads run tasks; their count defaults to the number of logical cores minus one.
- The watchdog observer thread delivers file events and runs the reschedule callbacks, which pause the evaluator while they reprogram it.
