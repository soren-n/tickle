# Changelog

## Unreleased

### Changed

- Requires Python 3.11 or later, PyYAML 6.0.3 or later and watchdog 7.1.0 or later.
- Task identity now covers the command a task runs, so the first run after upgrading reruns every task once. It no longer covers the task's flows, which only order tasks, so changing a task's flows or stages does not rerun it.
- Commands are built word by word: literal proc words are passed exactly as written, and each argument value becomes one word. Empty argument values are passed on instead of being dropped.
- Evaluation no longer waits on fixed one-second polls, so a run takes about 0.1 seconds of overhead instead of about 2.
- Packaging moved from `setup.py` to `pyproject.toml`; the project is developed with uv.
- The cache has a new format (version 2): records are keyed by task rather than position and use paths relative to the project, so a moved or copied project keeps its cache. An older cache is discarded once, so every task runs once.
- Offline mode fails (exit code 1, `offline()` returns `False`) when tasks are skipped because an input does not exist and no task produces it. The other tasks still run first.
- A depend file entry naming a file that another task produces now makes the task depend on that task, so it runs after it; before, the two could run in parallel.
- `tickle clean` keeps generated files that the agenda or depend file now reads as sources, files changed after tickle wrote them, folders, and anything outside the project. `api.clean` takes optional `agenda_path` and `depend_path` arguments for this; the CLI passes them.
- A succeeding task's stderr, such as compiler warnings, is printed as a warning instead of being discarded, and a task that succeeds without writing all of its outputs is reported. A failing task with an empty stderr shows its stdout.
- The CLI exits with 1 on failure instead of 255, and with 130 when interrupted.
- Agenda and depend errors say where the problem is, such as `tasks[2].inputs: ...`.
- Agendas are parsed with PyYAML's C loader when it is available, which is several times faster for large agendas.
- Paths in the agenda and depend file are normalised: `out/x.txt`, `./out/x.txt` and `out/../out/x.txt` name the same file, so a task reading one spelling depends on the task writing another, and two tasks writing one file under different spellings are rejected.
- A flow named by the empty string is rejected.
- Tasks run with no stdin, so a task that reads its input from stdin gets an empty one instead of waiting on the terminal.
- Tickle logs to the `tickle` logger instead of the root logger, so it no longer configures logging for a script that calls the API. Progress lines are flushed as they are printed, so they show up as they happen when the output goes to a pipe or a file.
- Within a stage, a task now waits only for the tasks it depends on, not for every task scheduled before it, and every task starts in the earliest step its dependencies and stages allow.
- An agenda whose dependencies, including those from the depend file, form a cycle together with the stage order of flows is rejected, naming the tasks involved; for example, with two flows of two stages each, a task in the first stage of one flow reading the output of a task in the second stage of the other, and the other way around.
- The CLI rejects agenda, depend, cache and log paths outside the current directory, and cache and log paths in folders that do not exist, with exit code 2, instead of failing with a traceback.

### Fixed

- Offline evaluation could wait forever when a depend entry made a task depend on a task of another flow, because the scheduler left tasks out of the schedule; it also silently left out the tasks of a cycle through stage order.
- In online mode, a failure could still cancel the schedule built for a change saved just after it, when that schedule was installed while the failure was being reported.
- Online mode never ran an out-of-date task whose only dependents were skipped because one of their inputs was missing; offline mode did.
- Scheduling took time and memory quadratic to cubic in the number of tasks per step: two steps of 4000 tasks took 396 seconds to schedule, and now take 0.04; two steps of 20000 take 0.4 seconds and 114 MB.
- A task that failed or was interrupted counted as up to date on the next run when its output was left from an earlier run, because its input hashes were recorded before it ran.
- In online mode, an input edited while its task was running was recorded as built, so the output stayed stale.
- `tickle clean` could delete a hand-written file that an earlier version of the agenda generated, and crashed in a copied project or with an unreadable cache.
- Online mode could start a task before the earlier stages of its flow had finished, when it followed a task of another flow.
- In online mode, a failure reported after the agenda or an input changed could cancel the new schedule.
- Ctrl-C in offline mode printed a traceback.
- A task whose proc is in no stage of one of its flows, a folder used as an input, or an unreadable agenda or depend file crashed with a traceback; they are now reported.
- A succeeding task that printed bytes that are not valid text was reported as failed, under the name "unknown".
- Online mode never noticed inputs in folders that did not exist when it started, and on Linux missed atomic saves that are reported only as moves. A failing change handler no longer stops all watching.
- Large agendas were slow: the cache was rewritten three times per task, shared files were hashed once per task reading them, and scheduling took quadratic time. A build of 2000 small tasks took 35 seconds instead of 4.5, and a run with nothing to do 3.3 seconds instead of 0.5.
- The depend file was resolved against the wrong folder and silently ignored.
- Changing a proc's command or a task's args did not rerun the task when its inputs were unchanged.
- An agenda whose tasks form a cycle crashed with a `KeyError` instead of being rejected.
- Offline evaluation could hang when it finished before every worker had started.
- A task whose command could not be started, such as a program that is not installed, hung offline evaluation.
- Online mode ignored agenda saves reported as created or moved files, which editors that save atomically produce.
- Pausing the evaluator to reschedule did not wait for running tasks.
- An invalid agenda or depend file, or one removed in online mode, raised a traceback; it is now reported, and online mode keeps its current schedule.
- The cache could be corrupted by concurrent updates or an interrupted write, and a damaged cache crashed every later run; it is now written atomically and rebuilt when unreadable.
- Literal proc words containing spaces were split, words containing `%` failed to compile, and argument values containing quotes swallowed the following words.
- Re-hashing the inputs of a finished task waited forever if an input had been deleted.
- The error message for an undefined flow crashed while being formatted.
- On a single core machine the default worker count was zero.
- Stopping online mode twice raised an error.

## 0.4.1 and earlier

See the git history and the `v0.1.1` to `v0.4.1` tags.
