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
                              Schedule (ready queue) --> Evaluator --> Workers --> Process
```

Every mode runs the same pipeline. Paths in the agenda are relative to the agenda file's directory, and paths in the depend file to the depend file's directory. Offline mode runs it once (and again if the depend file changes); online mode reruns parts of it whenever a watched file changes. Each mode first takes the project lock (`lock.ProjectLock`, an OS lock on the file next to the cache), so two runs never share a cache.

## Compiling the agenda

`agenda.load` reads the YAML into the `Agenda` dataclasses through `dataspec`, whose errors say where in the file a value does not fit, such as `tasks[2].inputs`. `agenda.compile` then produces a `CompiledTask` per task:

- **Command**: the task's proc is a list of words; words starting with `$` are parameters. The argument vector is built word by word: a literal word is kept exactly (spaces, quotes and `%` included), and a parameter word is replaced by its argument values, one word per value. A missing argument is an error.
- **Flows**: each flow is a list of stages, and each stage a list of procs. A task's `flows` map becomes `{flow: stage index}` for the flows it belongs to. A flow must have a non-empty name, a proc may appear in only one stage per flow, and a task's proc must be in a stage of each of its flows.
- **Paths**: inputs and outputs are made absolute against the agenda file's directory and normalised (`os.path.normpath`), so every spelling of a path names the same file; depend file paths likewise.
- **Hash**: an MD5 of the task's command, args, and its inputs and outputs spelled relative to the agenda's directory with `/` separators. It identifies the task across runs, independently of its position and of how its paths are written. Flows, pools, timeouts, retries and `always` are left out, since they change when and how a task runs, not what it writes, so changing them reruns nothing.
- **Folders and depfiles**: a path written with a trailing `/` is a folder; `folders` holds them. A depfile path is resolved like the others. The hash keeps a folder's trailing `/` and covers the depfile path only when one is set, so tasks without folders or depfiles keep their hash.
- **Options**: pools must be named in the agenda's `pools`, with a depth of at least one; timeouts and retries must not be negative; an always-run task must have no outputs.

## Depend closures

The depend file lists implicit dependencies between files, such as a source file including a header. `_depend_closures` joins them with the agenda's input/output edges into one file graph to reject cycles (searching from every file, so a cycle without an entry point is found too), and then computes for each file in the depend file the transitive closure of the files it depends on through the depend file alone.

A task's input closure is its inputs plus their depend closures; a folder input is kept as its path with a trailing separator, and hashed as a tree (the names of everything in it and the hashes of its files, each through the stat cache). A task with a depfile also reads the files its depfile named when it last succeeded (`implicit` in its record), for validity and watching but not for ordering. It does not reach through other tasks to their inputs: a change there makes the producing task rerun, and propagation (below) reruns the tasks after it. Keeping closures to a task's own inputs keeps the cache and the hashing per run linear in the size of the agenda.

## Building the task graph

`_make_graph` creates an `evaluate.Task` per compiled task, carrying its pool, retries, its hash as a key, and as weight the duration the cache recorded for it (or the average of the recorded ones). Its work is `_eval_cmd` (below). Two tasks may not write the same output, and no task may write inside another's output folder. A task depends on the task writing a file it reads, or the folder that file lies in. A task depends on the task producing each file in its input closure: its explicit inputs, and files its inputs depend on through the depend file, such as a generated header. `Task.add_dependency` refuses a dependency on a task in a later stage of a shared flow.

Because depend entries add dependencies, a change to the depend file builds the graph again rather than only the closures. Once the graph is built, it is checked for cycles, and `graph.check_stages` checks over every task that the dependencies do not form a cycle with the stage order of flows, so whether an agenda is accepted does not depend on what is up to date.

Online mode also watches the implicit files: those in some input closure that the agenda names neither as an input nor an output.

## Deciding what to run

`_make_schedule` marks each task valid (up to date) and active (can run). It reads the cache but never changes the recorded input hashes:

1. **Impossible tasks.** In topological order, a task with no outputs (unless it runs always), with an input that is a folder, or with an input that neither exists nor is produced by an earlier task, is made inactive and skipped with an error.
2. **Changed tasks and inputs.** The cache records, per task hash, the input closure with the MD5 of each file as they were when the task last started and then succeeded. A task without a record (new, changed, or never succeeded since) is invalid, as is one whose closure has different files or any file with a different MD5. A file's MD5 comes from the cache's `stats` when its size and modification time match those recorded when it was last hashed, and that hash was taken more than two seconds after the modification; otherwise the file is hashed and the stat recorded. Each file is looked at at most once per schedule. An always-run task is never valid.
3. **Missing outputs.** If any output is missing, the task is invalid.
4. **Propagation.** `graph.propagate` walks the graph in topological order; a task depending on an invalid task becomes invalid, and one depending on an inactive task becomes inactive.

Each task gets a `reason`, shown by a dry run: always, new or changed, reading other files, an input changed, an output missing, or reading from a task that runs (preferred for a changed input that a running task writes). Reasons and other messages name paths relative to the target directory with `/` on every platform, through `_name`, rather than as cache keys, which use the platform's separator. It returns the descriptions of the inactive (skipped) tasks; with targets, only the wanted tasks (the targets and everything they depend on) count. Before any task runs, the outputs of the tasks about to run are recorded as generated files, so clean finds them even if tickle is interrupted; a dry run records nothing.

When a task starts, its worker hashes the input closure as it is at that moment. If that matches the task's record and its outputs exist, the task is up to date after all (it was scheduled only because a task it reads from ran, and that one wrote what it wrote before): it does nothing and is marked `cut_off`, which is early cutoff. Otherwise it runs, and only if the command succeeds are those hashes recorded under the task's hash, with its duration. A task that fails or is interrupted therefore keeps its previous record, which no longer matches, and runs again. A file edited while its task runs counts as changed afterwards, because the record holds the content from before the edit.

## Scheduling

`schedule.Schedule` is a state machine without threads. It takes the invalid, active (and wanted) tasks, and between consecutive stages of each flow one barrier node that depends on every task of the earlier stage and is depended on by every task of the later one, keeping the graph linear in size. A node waits until its dependencies and barriers have finished; a barrier then completes at once, and a task becomes ready. Dependencies and stage order that form a cycle are refused.

Ready tasks are kept in a heap by priority: the longest chain of task weights (expected durations) from the task to the end of the graph, then agenda order. `next()` hands out the first ready task whose pool has room, within the worker count; `finish()` records the outcome. A failure with retries left makes the task ready again. Otherwise, without keep going the schedule stops handing out tasks; with keep going, the tasks that read from the failed one, directly or not, are blocked, while barriers count failed and blocked tasks as finished, so later stages go on. `adopt()` marks a task as running that started under an earlier schedule, and `order()` lists the tasks in an order one worker would run them, for a dry run.

## Evaluation

`Evaluator.install(schedule, keep)` makes a schedule current, at any time. Each running task is either adopted by it, when the new schedule would start a task with the same key now and `keep` accepts the run, or cancelled. The modes pass a `keep` that accepts a run whose input closure still hashes as it did when the run started.

Worker threads loop: under the evaluator's condition, take the next task from the current schedule (or wait), run it outside the lock, and record the outcome in the schedule that owns the run, which after adoption is the new one. A failure is reported at once on the worker (`on_task_failed`), and so is a success (`on_task_done`, with the time it took and the schedule's progress), which the modes print. A cancelled run's outcome is dropped. Any exception from a task's work becomes a `TaskError`, so a schedule always learns that a task ended.

The main loop waits on the condition until the current schedule has finished (nothing running, nothing more to start) and calls `on_finished`, which offline mode uses to stop. `stop()` may be called from any thread, more than once, also before `start`; nothing more starts, and with `cancel` the running tasks are cancelled too. An exception in the main loop, such as `KeyboardInterrupt`, cancels the running tasks before it propagates, and the workers are joined in every case.

## Running a task

`_eval_cmd` hashes the input closure, creates the output and log folders, and runs the command through `process.Process`: in a process group of its own (a new session on POSIX, a new process group on Windows), with no stdin, and with stdout and stderr going to the task's log file in the log folder, named after its description and hash. Cancelling the run, or a timeout, stops the group: SIGTERM (CTRL_BREAK on Windows), then SIGKILL (`taskkill /T /F`) after five seconds. A failure's message holds the exit code or the timeout, the last 20 lines of the log and its path. Before an output folder's task runs, the files its last run left there whose stat still matches are removed, and the folder is created; afterwards every file in it is recorded for clean. On success the input hashes, the duration and the files its depfile names are recorded; a depfile entry hashed at the start keeps that hash, a new one is hashed then.

## Offline mode

`OfflineEvaluator` builds the graph and installs a schedule, and stops when it has finished. It watches the depend file and builds the graph and schedule again if it changes during the run, adopting the running tasks it still needs; an invalid depend file is reported and the current schedule kept. `offline()` returns a `Result` from the schedule's states: succeeded, failed, blocked (also the tasks not started after a failure without keep going) and skipped. A `KeyboardInterrupt`, which the CLI also raises on SIGTERM, cancels the running tasks and propagates. The watcher is stopped by `start` after the main loop ends, and `start` writes any pending cache changes on the way out, also when interrupted.

## Online mode

`OnlineEvaluator` watches the agenda, the depend file, every explicit input that no task produces, and every implicit file. Events are noted and applied once no new one has arrived for 0.3 seconds, one update at a time, judged by the files' content hashes:

- agenda or depend file: recompile, rebuild the closures, the graph and the schedule;
- input file: recompute validity and the schedule;
- output: if one is missing, recompute validity and the schedule, so a deleted output is built again at once (writing outputs also raises events, which then change nothing).

The new schedule is installed without waiting for running tasks, which are adopted or cancelled as above. A change is applied only if everything rebuilds: an agenda or depend file that does not load, or that introduces a cycle, is reported and the current schedule kept. A removed depend file counts as an empty one; a removed agenda is reported and ignored.

Online mode keeps going after a failure by default, so the tasks that do not read from a failed task still run; the failed one runs again on the next change. It runs until `stop` is called (the CLI does so on Ctrl-C and SIGTERM), which cancels running tasks unless asked not to.

## Watching files

`FileWatcher` subscribes callbacks to individual files by watching their folders. A file whose folder does not exist yet is watched through the nearest existing folder above it; when folders are created there the watch moves down, and a file that already exists by then is reported as created. Both paths of a move are checked, since saving by renaming a new file over a watched one is reported only as a move on some platforms. Watchdog calls the watcher on its observer thread holding the observer's lock, which scheduling a watch also takes, while subscribing takes the watcher's lock and then the observer's; so the watcher only queues the event there, and its own thread handles events and calls the callbacks, or a subscription during an event would deadlock both. A callback that raises is logged rather than allowed to end that thread. When folders are deleted, created or moved, every watch is checked again: a file whose nearest existing folder changed moves its watch up or down, and a watched folder that was replaced, by being deleted and created again or by a rename, is watched again, since the old watch followed the folder that went away. A folder reported as modified moves watches the same way. A folder is reported as modified whenever a file in it changes (on its own watch on Linux, on its parent's on Windows), and the project folder whenever the cache is written, so a modification renews an unchanged watch only when another folder now has its path, by device and inode: renewing a watch can lose the events its old one had not read, such as a file's deletion. (macOS reports a folder created again only as its parent being modified; the watch then moves down because the nearest existing folder changed.) A watch that cannot be made because its folder went away meanwhile moves up to the nearest folder that exists. After moving or renewing a watch, each file is reported as created if it exists and as deleted if not; callbacks judge by content.

The watchdog observer exists only from `start` to `stop`. On Linux an observer takes an inotify instance and a thread as soon as it is made and gives them back only once started and stopped, and evaluators that never start (dry runs, constructors that raise, online evaluators closed unstarted) still make a watcher, so the watcher schedules its watches when it starts.

## The cache

`tickle.cache` is a pickled dict. Workers update it under its lock; changes are written at most every half second while tasks finish, when the evaluator is idle or finished, and when it stops. A write that is lost only makes the tasks it would have recorded run again. Each write goes to `tickle.cache.tmp` and is renamed into place, so a crash never leaves a truncated cache; an unreadable cache is reported and treated as empty. Paths are relative to the target directory, so a project keeps its cache when it is moved or copied.

| Key | Contents |
| --- | --- |
| `version` | Cache format version, currently 3 |
| `tasks` | Task hash to a record: `inputs`, the input closure with the MD5 of each file when the task last started and then succeeded, and `duration`. Records take optional fields, so new fields need no new version |
| `stats` | Input path to `(size, mtime_ns, checked_ns, MD5)`, to skip hashing unchanged files |
| `files` | Output path to `(mtime_ns, size)` after tickle last ran the task writing it, or `None` if it has not yet |
| `folders` | Directories tickle created for outputs and logs |
| `logs` | Log files tickle wrote |

A cache of version 2 (tickle 0.5.0) is migrated: its records become `{'inputs': ...}`, and stats start empty, so upgrading reruns nothing and hashes every input once. A cache from a newer version of tickle is refused, by every mode, and left alone. Older caches are discarded, so every task runs once.

Records of tasks no longer in the agenda are dropped, so a task changed back to an earlier definition runs again rather than trusting outputs the other definition wrote. Stats of files no task reads are dropped too.

`clean` deletes the generated files, the logs, then the generated folders that are empty, then the cache and the lock file. It keeps a generated file that the agenda or depend file now reads as a source, one whose size or modification time differs from what tickle recorded (it changed after tickle wrote it), folders, and anything outside the target directory. It also reads caches from before version 2, which held absolute paths and no file stats.

## Threads

- The caller's thread runs the evaluator's main loop.
- Worker threads run tasks; their count defaults to the number of logical cores minus one.
- The watchdog observer thread delivers file events to a queue, and the watcher's thread handles them; online mode notes them and a timer thread applies them once they settle, installing new schedules while tasks run.
- A timer thread per stopped task kills its process group after the grace period.
