# Python API

Tickle can be driven from Python instead of the CLI, typically by a script that generates the agenda and then evaluates it. The README has short examples; this page lists the public functions and types. The package ships type information (`py.typed`).

All paths passed to the API must be absolute `pathlib.Path` objects, and the agenda, depend and cache paths and the log folder must lie within the target directory. Tasks run with the target directory as their working directory, with no stdin, in a process group of their own, and with their output going to a log file per task.

Only one run can use a project at a time: each mode takes a lock next to the cache (`tickle.lock` for `tickle.cache`), which the operating system releases when the process ends, however it ends. A second run fails at once with a message naming the process that holds the lock.

## `tickle.api`

### Defaults

```python
default_agenda_path(dir_path: Path = Path('./')) -> Path   # dir_path / 'agenda.yaml'
default_depend_path(dir_path: Path = Path('./')) -> Path   # dir_path / 'depend.yaml'
default_cache_path(dir_path: Path = Path('./')) -> Path    # dir_path / 'tickle.cache'
default_log_dir(dir_path: Path = Path('./')) -> Path       # dir_path / 'tickle-logs'
default_worker_count() -> int                              # logical cores minus one, at least 1
```

### `offline`

```python
offline(
    target_dir, agenda_path=None, depend_path=None, cache_path=None, worker_count=None,
    *, keep_going=False, targets=None, dry_run=False, log_dir=None
) -> Result
```

Evaluates every out-of-date task once and returns when they are done. Arguments left out or `None` take the defaults above, in `target_dir`. `agenda_path` may also be an `Agenda` object, whose paths are then relative to `target_dir`; no agenda file is read or written. The depend file is optional.

- `keep_going`: after a task fails, keep running every task that does not read from it, directly or through other tasks. Without it, nothing more starts after a failure; running tasks finish and are recorded.
- `targets`: run only these tasks and the tasks they need. Each target is a task description or one of a task's outputs, relative to `target_dir`; an unknown target is an error.
- `dry_run`: decide what would run, without running it or writing the cache; `Result.planned` lists the tasks in an order they could run in.
- `log_dir`: where each task's output goes, one log file per task, replaced on each run of the task.

```python
@dataclass
class Result:
    succeeded: list[str]   # descriptions of the tasks that ran and succeeded
    failed: list[str]      # ran and failed, with no retries left
    skipped: list[str]     # could not run: an input is missing and no task makes it, or no outputs
    blocked: list[str]     # did not run because a task failed
    planned: list[str]     # a dry run's tasks
    reasons: dict[str, str]  # a dry run's reason for each planned task, such as 'its output out/x is missing'
    error: str | None      # why the run could not start: no agenda, an invalid agenda or depend file, the lock is held, ...
```

A `Result` is true when the run did everything it had to: no task failed, was skipped or was blocked, and `error` is `None`. So `if offline(...)` keeps working as it did when `offline` returned a `bool`.

A `KeyboardInterrupt` raised while evaluating stops the running tasks and their processes (asking them to end, and killing them after five seconds) and then propagates to the caller; tasks that did not finish are not recorded as done and run again next time.

### `online`

```python
online(
    target_dir, agenda_path=None, depend_path=None, cache_path=None, worker_count=None,
    *, keep_going=True, targets=None, log_dir=None
) -> OnlineEvaluator | None
```

Returns an evaluator without running it, or `None` if the agenda file is missing or invalid, or another run holds the project. The evaluator holds the project lock from then on; call `start()` to evaluate, which blocks, rescheduling as the agenda, the depend file and the input files change, until `stop()` is called from another thread, and releases the lock when it returns. An evaluator that will not be started should be closed with `close()`.

```python
evaluator = tickle_api.online(target_dir)
runner = Thread(target = evaluator.start)
runner.start()
...
evaluator.stop()
runner.join()
```

Changes are applied once they have settled for a moment, without waiting for running tasks: a running task whose definition and input files are unchanged carries on and counts in the new schedule, and any other running task is stopped. A failing task is logged; with `keep_going` (the default) the tasks that do not read from it carry on, and without it nothing more starts until the next change. A change that makes the agenda or depend file invalid is logged and the current schedule kept until the file is fixed. `stop()` stops running tasks too, unless called as `stop(cancel = False)`; it may be called more than once.

### `clean`

```python
clean(target_dir, cache_path=None, agenda_path=None, depend_path=None) -> bool
```

Deletes the files a previous run generated, the task logs, then the directories it created if they are empty, then the cache and the lock file. It keeps a generated file that the agenda or depend file now reads as a source (when their paths are given), a file that changed after tickle last wrote it, a folder, and anything outside `target_dir`. Returns `True`, also when there is no cache, and `False` without deleting anything if the agenda or depend file is invalid, the cache was written by a newer version of tickle, or another run holds the project.

## `tickle.agenda`

```python
@dataclass
class Task:
    desc: str                     # shown when the task runs
    proc: str                     # name of a proc in Agenda.procs
    flows: list[str] = []         # flows the task belongs to
    args: dict[str, list[str]] = {}  # values for the proc's $parameters
    inputs: list[str] = []        # paths relative to the agenda file; a trailing / names a folder
    outputs: list[str] = []
    depfile: str = ''             # a make-syntax depfile the task writes, naming more inputs
    pool: str = ''                # a pool in Agenda.pools, or '' for none
    timeout: float = 0.0          # seconds before the task is stopped and fails; 0 for none
    retries: int = 0              # how often a failing task runs again
    always: bool = False          # run on every run; only for tasks without outputs

@dataclass
class Agenda:
    procs: dict[str, list[str]] = {}        # proc name to command words; '$name' is a parameter
    flows: dict[str, list[list[str]]] = {}  # flow name to stages, each a list of proc names
    tasks: list[Task] = []
    pools: dict[str, int] = {}              # pool name to how many of its tasks run at once

load(agenda_path: Path) -> Agenda
store(agenda_path: Path, agenda_data: Agenda) -> None
compile(target_dir: Path, agenda_data: Agenda) -> list[CompiledTask]
```

(The defaults are dataclass fields; the lists and dicts are new for each object.) `store` leaves `depfile`, `pool`, `timeout`, `retries`, `always` and `pools` out of the file while they hold their defaults.

`load` raises `RuntimeError` for a file that cannot be read, malformed YAML, or data that does not fit the types above; the message says where, as in `tasks[2].inputs: ...` (`tickle.depend.load` likewise).

`compile` validates an agenda without running it; it raises `RuntimeError` for an undefined proc, flow or pool, a flow or pool named by the empty string, a pool of depth less than one, a negative timeout or retry count, an always-run task with outputs, a proc used in two stages of one flow, or a task whose proc is in no stage of one of its flows. Input and output paths are normalised, so equivalent spellings of a path name the same file. Problems between tasks, such as two tasks writing the same output, a dependency on a later stage, or dependencies forming a cycle with the stage order of flows, are found when an evaluator builds the task graph.

## `tickle.depend`

```python
Depend = dict[str, list[str]]   # file path to the files it depends on

empty() -> Depend
load(depend_path: Path) -> Depend
store(depend_path: Path, depend_data: Depend) -> None
```

Paths in the depend file are relative to the depend file's directory, like paths in the agenda.

A generator that discovers implicit dependencies, such as a compiler's header dependency output, can write the depend file while tickle runs; both modes pick up the change.

## Logging

Tickle prints progress as `[tickle] ...` lines on stdout, flushed as they are printed, and also logs to the `tickle` logger of the `logging` module. It leaves the root logger and the process's logging setup alone; records propagate to the root logger, so a handler configured there (the CLI uses `logging.basicConfig` with a file) keeps the log, and without one nothing extra is printed. A task's own output goes to its log file in the log folder; when it fails, the end of that output and the log's path are printed with the failure.
