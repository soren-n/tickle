# Python API

Tickle can be driven from Python instead of the CLI, typically by a script that generates the agenda and then evaluates it. The README has short examples; this page lists the public functions and types.

All paths passed to the API must be absolute `pathlib.Path` objects, and the agenda, depend and cache paths must lie within the target directory. Tasks run with the target directory as their working directory.

## `tickle.api`

### Defaults

```python
default_agenda_path(dir_path: Path = Path('./')) -> Path   # dir_path / 'agenda.yaml'
default_depend_path(dir_path: Path = Path('./')) -> Path   # dir_path / 'depend.yaml'
default_cache_path(dir_path: Path = Path('./')) -> Path    # dir_path / 'tickle.cache'
default_worker_count() -> int                              # logical cores minus one, at least 1
```

### `offline`

```python
offline(target_dir, agenda_path, depend_path, cache_path, worker_count=default_worker_count()) -> bool
```

Evaluates every out-of-date task once and returns when they are done. Returns `True` on success, and `False` if the agenda file is missing, the agenda or depend file is invalid (malformed YAML, an undefined proc or flow, two tasks writing the same output, or a dependency cycle), a task fails or its command cannot be started, or tasks were skipped because an input does not exist and no task produces it. Skipped tasks do not stop the others; the result is `False` once everything else is done. The depend file is optional.

A `KeyboardInterrupt` raised while evaluating propagates to the caller; tasks that did not finish are not recorded as done and run again next time.

### `online`

```python
online(target_dir, agenda_path, depend_path, cache_path, worker_count=default_worker_count()) -> OnlineEvaluator | None
```

Returns an evaluator without running it, or `None` if the agenda file is missing or invalid. Call `start()` to evaluate; it blocks, rescheduling as the agenda, the depend file and the input files change, until `stop()` is called from another thread:

```python
evaluator = tickle_api.online(target_dir, agenda_path, depend_path, cache_path)
runner = Thread(target = evaluator.start)
runner.start()
...
evaluator.stop()
runner.join()
```

A failing task is logged and does not stop the evaluator. A change that makes the agenda or depend file invalid is logged and the current schedule kept until the file is fixed. `stop()` may be called more than once.

### `clean`

```python
clean(target_dir, cache_path, agenda_path=None, depend_path=None) -> bool
```

Deletes the files a previous run generated, then the directories it created if they are empty, then the cache. It keeps a generated file that the agenda or depend file now reads as a source (when their paths are given), a file that changed after tickle last wrote it, a folder, and anything outside `target_dir`. Returns `True`, also when there is no cache, and `False` without deleting anything if the agenda or depend file is invalid.

## `tickle.agenda`

```python
@dataclass
class Task:
    desc: str                     # shown when the task runs
    proc: str                     # name of a proc in Agenda.procs
    flows: list[str]              # flows the task belongs to
    args: dict[str, list[str]]    # values for the proc's $parameters
    inputs: list[str]             # paths relative to the agenda file
    outputs: list[str]

@dataclass
class Agenda:
    procs: dict[str, list[str]]       # proc name to command words; '$name' is a parameter
    flows: dict[str, list[list[str]]] # flow name to stages, each a list of proc names
    tasks: list[Task]

load(agenda_path: Path) -> Agenda
store(agenda_path: Path, agenda_data: Agenda) -> None
compile(target_dir: Path, agenda_data: Agenda) -> list[CompiledTask]
```

`load` raises `RuntimeError` for a file that cannot be read, malformed YAML, or data that does not fit the types above; the message says where, as in `tasks[2].inputs: ...` (`tickle.depend.load` likewise).

`compile` validates an agenda without running it; it raises `RuntimeError` for an undefined proc or flow, a proc used in two stages of one flow, or a task whose proc is in no stage of one of its flows. Problems between tasks, such as two tasks writing the same output or a dependency on a later stage, are found when an evaluator builds the task graph.

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

Tickle prints progress as `[tickle] ...` lines on stdout and also logs through the `logging` root logger; configure a handler (the CLI uses `logging.basicConfig` with a file) to keep the log. Warnings, such as a succeeding task's stderr, are logged at `WARNING`.
