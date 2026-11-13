# Architecture

This document describes how tickle turns an agenda into work and evaluates it. For the file formats see the README; for the Python entry points see [api.md](api.md).

## Overview

```
agenda.yaml --load/compile--> CompiledAgenda --+
                                               +--_make_graph--> Task graph + input closures
depend.yaml --load/compile--> depend closures -+                         |
                                                                         |
              tickle.cache <--> _make_schedule (validity, activity) <----+
                                        |
                                 graph.compile --> Program --> Evaluator --> Workers
```

Every mode runs the same pipeline. Paths in the agenda are relative to the agenda file's directory, and paths in the depend file to the depend file's directory. Offline mode runs it once (and again if the depend file changes); online mode reruns parts of it whenever a watched file changes.

## Compiling the agenda

`agenda.load` reads the YAML into the `Agenda` dataclasses through `dataspec`, whose errors say where in the file a value does not fit, such as `tasks[2].inputs`. `agenda.compile` then produces a `CompiledTask` per task:

- **Command**: the task's proc is a list of words; words starting with `$` are parameters. The argument vector is built word by word: a literal word is kept exactly (spaces, quotes and `%` included), and a parameter word is replaced by its argument values, one word per value. A missing argument is an error.
- **Flows**: each flow is a list of stages, and each stage a list of procs. A task's `flows` map becomes `{flow: stage index}` for the flows it belongs to. A flow must have a non-empty name, a proc may appear in only one stage per flow, and a task's proc must be in a stage of each of its flows.
- **Paths**: inputs and outputs are made absolute against the agenda file's directory and normalised (`os.path.normpath`), so every spelling of a path names the same file; depend file paths likewise.
- **Hash**: an MD5 of the task's command, args, and normalised inputs and outputs. It identifies the task across runs, independently of its position. Flows are left out, since they only order tasks, so changing them reruns nothing.

## Depend closures

The depend file lists implicit dependencies between files, such as a source file including a header. `_depend_closures` joins them with the agenda's input/output edges into one file graph to reject cycles (searching from every file, so a cycle without an entry point is found too), and then computes for each file in the depend file the transitive closure of the files it depends on through the depend file alone.

A task's input closure is its inputs plus their depend closures. It does not reach through other tasks to their inputs: a change there makes the producing task rerun, and propagation (below) reruns the tasks after it. Keeping closures to a task's own inputs keeps the cache and the hashing per run linear in the size of the agenda.

## Building the task graph

`_make_graph` creates an `evaluate.Task` per compiled task, whose work runs the command with `subprocess.run` in the target directory, with stdin from the null device so a task cannot wait on the terminal. Two tasks may not write the same output. A task depends on the task producing each file in its input closure: its explicit inputs, and files its inputs depend on through the depend file, such as a generated header. `Task.add_dependency` refuses a dependency on a task in a later stage of a shared flow.

Because depend entries add dependencies, a change to the depend file builds the graph again rather than only the closures.

Online mode also watches the implicit files: those in some input closure that the agenda names neither as an input nor an output.

## Deciding what to run

`_make_schedule` marks each task valid (up to date) and active (can run). It reads the cache but never changes the recorded input hashes:

1. **Impossible tasks.** In topological order, a task with no outputs, with an input that is a folder, or with an input that neither exists nor is produced by an earlier task, is made inactive and skipped with an error.
2. **Changed tasks and inputs.** The cache records, per task hash, the input closure with the MD5 of each file as they were when the task last started and then succeeded. A task without a record (new, changed, or never succeeded since) is invalid, as is one whose closure has different files or any file with a different MD5. Each file is hashed at most once per schedule.
3. **Missing outputs.** If any output is missing, the task is invalid.
4. **Propagation.** `graph.propagate` walks the graph in topological order; a task depending on an invalid task becomes invalid, and one depending on an inactive task becomes inactive.

It returns the program and the descriptions of the inactive (skipped) tasks. Before any task runs, the outputs of the tasks about to run are recorded as generated files, so clean finds them even if tickle is interrupted.

When a task starts, its worker hashes the input closure as it is at that moment; only if the command succeeds are those hashes recorded under the task's hash. A task that fails or is interrupted therefore keeps its previous record, which no longer matches, and runs again. A file edited while its task runs counts as changed afterwards, because the record holds the content from before the edit.

## Compiling a program

`graph.compile` reduces the invalid, active tasks to a `Program`:

- The tasks to run are all invalid, active tasks. Propagation has already made every task after an invalid task invalid, so no search of the graph is needed; in particular a task that only a skipped task needs still runs.
- A **sequence** is a list of tasks run in order by one worker. Each sequence currently holds one task; joining chains of tasks into longer sequences broke the stage order across flows and is left to the planned ready-queue scheduler.
- Within each flow, one barrier node is placed between consecutive stages, depending on every task of the earlier stage and depended on by every task of the later one. This keeps the graph linear in size, where edges from every task of a stage to every task of the next would be quadratic.
- A **batch** is a set of sequences that can run in parallel: a task's batch is one more than the highest batch of its dependencies (Kahn's algorithm). A barrier passes its level on without adding one, so a task starts in the earliest batch its dependencies and stages allow; barriers are then left out.
- Each dependency respects the stages of the flows its two tasks share, but dependencies can still form a cycle with the stage order of other flows. No task of such a cycle can start, so the ordering raises `RuntimeError` naming them rather than leave them out. `_make_graph` runs the same check (`graph.check_stages`) over every task when it builds the graph, so whether an agenda is accepted does not depend on which tasks are up to date.

## Evaluation

`Evaluator.start` starts the worker threads and then loops: it takes the next batch, puts each sequence on a queue, and waits for the queue to drain before moving on. Workers take sequences off the queue and run their tasks. A failing command, or one that cannot be started, raises `TaskError` in the worker, which passes it to the main loop; any other exception in a task is reported the same way under the task's description, so a batch always drains.

Each program gets a generation number, and errors carry the generation of the program their task belonged to. The main loop reports every queued error through `on_task_error`, and clears the remaining program only if an error came from the current one, so a failure in an old program never cancels the program that replaced it. It checks the generation again after pausing, since a new program may have been installed while it waited for the lock. When the main loop has nothing to queue it calls `on_idle`, which the evaluators use to write the cache.

`pause`/`resume` take the write side of a reader/writer lock. Workers hold the read side while they take and run a sequence, and the main loop while it queues a batch, so pausing waits for running sequences to finish and reprogramming never races with them. `stop` may be called from any thread, more than once, also before `start`; from then on workers take queued sequences off the queue without running them, so stopping waits only for the tasks already running. It ends the loop, and the workers are then stopped and joined.

When there is no program the main loop waits on an event that reprogramming and stopping set, with a one-second timeout as a fallback; idle workers check for work every 50 milliseconds.

## Offline mode

`OfflineEvaluator` builds the graph once and adds a forced terminating task that depends on every other task and calls `stop`. A failure is printed as soon as it happens (`on_task_failed`, called on the worker), but the main loop learns of it only once the batch has drained; its `on_task_error` then re-raises, so `offline()` returns `False` after the tasks of that batch have finished; skipped tasks make it return `False` after the rest has run. It watches the depend file and builds the graph and schedule again if it changes during the run; an invalid depend file is reported and the current schedule kept. The watcher is stopped by `start` after the main loop ends, not by `stop`, which runs on the worker performing the terminating task. `start` writes any pending cache changes on the way out, also when interrupted.

## Online mode

`OnlineEvaluator` watches the agenda, the depend file, every explicit input that no task produces, and every implicit file. On a change it compares the file's content hash with the last one seen and, if it differs, pauses, rebuilds what the change affects, reprograms and resumes:

- agenda or depend file: recompile, rebuild the closures, the graph and the schedule;
- input file: recompute the schedule.

A change is applied only if everything rebuilds: an agenda or depend file that does not load, or that introduces a cycle, is reported and the current schedule kept. A removed depend file counts as an empty one; a removed agenda is reported and ignored. An unexpected error while rescheduling is reported the same way, so the watcher keeps running.

A failing task is logged as it fails and the rest of the schedule dropped; the next change schedules again. Online mode runs until `stop` is called (the CLI does so on Ctrl-C).

## Watching files

`FileWatcher` subscribes callbacks to individual files by watching their folders. A file whose folder does not exist yet is watched through the nearest existing folder above it; when folders are created there the watch moves down, and a file that already exists by then is reported as created. Both paths of a move are checked, since saving by renaming a new file over a watched one is reported only as a move on some platforms. A callback that raises is logged rather than allowed to end the observer thread. A watched folder that is deleted and created again, or replaced by a rename, is not watched again; that is left to the planned watcher rework.

## The cache

`tickle.cache` is a pickled dict. Workers update it under its lock; changes are written at most every half second while tasks finish, when the evaluator is idle, and when it stops. A write that is lost only makes the tasks it would have recorded run again. Each write goes to `tickle.cache.tmp` and is renamed into place, so a crash never leaves a truncated cache; an unreadable cache is reported and treated as empty. Paths are relative to the target directory, so a project keeps its cache when it is moved or copied.

| Key | Contents |
| --- | --- |
| `version` | Cache format version, currently 2; a cache with another version is discarded and every task runs once |
| `tasks` | Task hash to `{input path: MD5}`: the input closure when the task last started and then succeeded |
| `files` | Output path to `(mtime_ns, size)` after tickle last ran the task writing it, or `None` if it has not yet |
| `folders` | Directories tickle created for outputs |

Records of tasks no longer in the agenda are dropped, so a task changed back to an earlier definition runs again rather than trusting outputs the other definition wrote.

`clean` deletes the generated files, then the generated folders that are empty, then the cache. It keeps a generated file that the agenda or depend file now reads as a source, one whose size or modification time differs from what tickle recorded (it changed after tickle wrote it), folders, and anything outside the target directory. It also reads caches from before version 2, which held absolute paths and no file stats.

## Threads

- The caller's thread runs the evaluator's main loop.
- Worker threads run tasks; their count defaults to the number of logical cores minus one.
- The watchdog observer thread delivers file events and runs the reschedule callbacks, which pause the evaluator while they reprogram it.
