# Changelog

## Unreleased

### Added

- Tickle prints a line when a task finishes, with how long it took and how many of the tasks to run have finished, and offline mode ends with how many tasks ran and how long that took (`Result.seconds`).

### Fixed

- Online mode built a deleted output again only on the next change; it now does so at once.

## 0.7.0

### Added

- Folders as inputs and outputs: a path ending with `/` names a folder. A task reading one reruns when a file in it changes, appears or goes; a task writing one gets it created, and before it runs again the files its last run left there, and that nobody changed, are removed. A task reading a file inside another task's output folder runs after it.
- Early cutoff: a task that reruns and writes what it wrote before no longer reruns the tasks after it.
- Per-task depfiles (`depfile` on a task): after the task succeeds, the files its make-syntax depfile names, such as the headers `gcc -MD` lists, count as its inputs.
- A dry run says why each task would run: new or changed, which input changed, which output is missing, or which task it reads from. `Result.reasons` holds the same.

### Changed

- A task out of date only because a task it reads from runs is checked again when it is about to start, and left alone if its inputs came out unchanged; such a task is neither printed nor listed as succeeded.

## 0.6.0

### Upgrading

The cache moves to format 3; a cache written by 0.5.0 is migrated, so upgrading reruns nothing (every input is hashed once). A cache written by a newer tickle is refused and left alone. A task's output now goes to a log file in `tickle-logs` instead of being printed. `api.offline` returns a `Result`, which is true when the run succeeded, so `if offline(...)` keeps working; code comparing the result with `True` must use `bool(...)`.

### Added

- Tasks start as soon as the tasks they read from, and those of the earlier stages of their flows, have finished, instead of in batches that waited for their slowest task. When several tasks are ready, the one heading the longest chain of work starts first, judged by the durations recorded last time.
- `--keep-going` (`keep_going` in the API): after a task fails, every task that does not read from it still runs. Online mode keeps going by default (`--stop-on-failure` turns it off).
- Targets: `tickle offline out/final.mp4` runs only the tasks a target needs, given as task descriptions or output files.
- `--dry-run` prints the tasks that would run, without running them.
- Pools (`pools` in the agenda, `pool` on a task) limit how many tasks of a kind run at once.
- `timeout` stops a task that runs too long and fails it; `retries` runs a failing task again.
- `always: true` makes a task without outputs run on every run.
- Each task's output goes to a log file of its own in `tickle-logs` (`--log-dir`); a failure prints the end of it and its path.
- A lock keeps two runs out of one project; the second fails at once, naming the first.
- An input whose size and modification time are unchanged is not hashed again, so a run with nothing to do no longer reads every input in full.
- `api.offline` takes an `Agenda` object as well as a path, and every path argument after the target directory is optional; the options are keyword-only. The package ships type information.

### Changed

- Tasks run in a process group of their own. Interrupting tickle, with Ctrl-C or now also SIGTERM (exit code 143), or a timeout, stops a task's processes: tickle asks them to end and kills them after five seconds.
- Online mode applies a change without waiting for running tasks: a running task whose definition and inputs did not change carries on, and one whose did is stopped and started again. Changes are applied once they settle for 0.3 seconds. Stopping online mode stops its running tasks.
- A task's `flows` field is optional, and so is every agenda section.
- A succeeding task's output, stderr included, goes to its log instead of being printed.

### Fixed

- Online mode stopped noticing changes to files in a folder that was deleted and created again, or replaced by a rename.
- A change made between creating an evaluator and starting it was missed.

## 0.5.0

## 0.5.0

### Upgrading

The first run after upgrading reruns every task once: the cache has a new format and tasks are identified differently. `tickle clean` still reads the old cache.

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
- A succeeding task's stderr, such as compiler warnings, is reported instead of being discarded: the warning shows its last five lines and the log file all of it. A task that succeeds without writing all of its outputs is reported. A failing task with an empty stderr shows its stdout.
- A failing task is reported as soon as it fails, not after the other tasks running alongside it have finished.
- The CLI exits with 1 on failure instead of 255, and with 130 when interrupted.
- Agenda and depend errors say where the problem is, such as `tasks[2].inputs: ...`.
- Agendas are parsed with PyYAML's C loader when it is available, which is several times faster for large agendas.
- Paths in the agenda and depend file are normalised: `out/x.txt`, `./out/x.txt`, `out/../out/x.txt` and the absolute path name the same file, so a task reading one spelling depends on the task writing another, and two tasks writing one file under different spellings are rejected.
- A flow named by the empty string is rejected.
- Tasks run with no stdin, so a task that reads its input from stdin gets an empty one instead of waiting on the terminal.
- Tickle logs to the `tickle` logger instead of the root logger, so it no longer configures logging for a script that calls the API. Progress lines are flushed as they are printed, so they show up as they happen when the output goes to a pipe or a file.
- Every task is placed in the earliest batch its dependencies and stages allow. Tasks still run in batches, so a task waits for every task of the batches before it; the scheduler planned for 0.6.0 removes that wait.
- An agenda whose dependencies, including those from the depend file, form a cycle together with the stage order of flows is rejected, naming the tasks involved, also when every task is up to date; for example, with two flows of two stages each, a task in the first stage of one flow reading the output of a task in the second stage of the other, and the other way around.
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
- Stopping online mode with Ctrl-C or SIGTERM kept starting the remaining tasks of the current batch; it now waits only for the running tasks.
- Checking a large agenda for cycles took time quadratic in its number of tasks in offline mode: 200000 tasks spent about 33 seconds in the graph checks, now about 5.
- Loading a large agenda took twice as long as needed.
- A cache or log path naming a folder failed with a traceback; the CLI now reports it.
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
