# Tickle

A command line workflow automation tool: it reads an agenda of tasks (commands over input and output files), builds a task graph, decides which tasks are out of date from a persistent cache, and evaluates them concurrently. Published on PyPI as `tickle-soren-n`; the CLI entry point is `tickle`.

User documentation is in `README.md`. Further docs:

- `docs/architecture.md`: how an agenda becomes a schedule and how it is evaluated.
- `docs/api.md`: the Python API.
- `docs/releasing.md`: how to cut a release.

## Commands

```
uv sync                                  # create .venv with runtime and dev dependencies
uv run minigun --time-budget 60          # run the property tests (about 6 minutes)
uv run minigun --time-budget 10 --select "encode"   # run the properties whose description matches
uv run ruff check . && uv run pyright   # lint (bug rules only) and type check
uv run tickle offline                    # run the CLI in the current directory
uv build                                 # build sdist and wheel into dist/
```

To try the CLI end to end, copy `examples/hello_world` somewhere outside the repo and run `tickle offline`, `tickle online` or `tickle clean` in it (needs `g++`).

## Layout

- `tickle/cli.py`: argparse front end; resolves paths against the working directory and calls the API.
- `tickle/api.py`: the three modes (`offline`, `online`, `clean`), the `OfflineEvaluator` and `OnlineEvaluator`, the cache layout, depend closures (`_depend_closures`), graph construction (`_make_graph`), task evaluation (`_eval_cmd`) and invalidation (`_make_schedule`).
- `tickle/agenda.py`: agenda dataclasses, YAML load/store, and `compile` (procs to commands, flows to stages, paths made absolute).
- `tickle/depend.py`: the depend file (implicit file dependencies such as source to header).
- `tickle/graph.py`: cycle checks, invalidity propagation, and `compile`, which turns the task graph into a `Program` of batches.
- `tickle/evaluate.py`: `Task`, `Worker` threads and the base `Evaluator` loop.
- `tickle/cache.py`: pickled dict persisted to `tickle.cache`, with batched writes.
- `tickle/watch.py`: watchdog wrapper subscribing callbacks to individual files, also in folders that do not exist yet.
- `tickle/dataspec.py`: typed encode/decode between dataclasses and plain YAML data.
- `tickle/log.py`: logs to the `logging` root logger and prints `[tickle] ...` to stdout.
- `tests/main.py`: the property tests; `tests/_agenda.py` generates random acyclic agendas; `tests/tasks/` holds the `success.py` and `failure.py` scripts generated agendas run.

## Conventions

- Match the existing style: `name : Type` annotations with spaces around the colon, `typing` generics (`List`, `Dict`, `Set`), `%` string formatting, and `####` banner comments separating sections. No formatter is configured; do not reformat unrelated code. `ruff check` (bug rules only) and pyright in basic mode must pass; CI runs them.
- Runtime dependencies are only `pyyaml` and `watchdog`; keep it that way unless there is a strong reason.
- The version lives in `tickle/__init__.py` (`__version__`) and is read by hatchling.
- Record user-visible changes and fixes under "Unreleased" in `CHANGELOG.md`.
- Commit messages: imperative summary line without a type prefix (for example "Fix evaluator hang when ..."), with a body explaining why for anything non-trivial.

## Testing

- CI (`.github/workflows/ci.yml`) runs the lint and type check, the property tests on Linux, macOS and Windows with Python 3.12 and 3.14, and a small end-to-end agenda with Python 3.11.
- Tests use [minigun](https://github.com/soren-n/minigun-py) (property-based, a sibling project by the same author), which requires Python 3.12+, although tickle itself supports 3.11+.
- The minigun CLI discovers modules in `tests/` that export `spec`; helper modules start with `_` and are imported relatively (`from ._agenda import ...`).
- Properties that evaluate agendas start real processes, so they declare `attempts=100` and use agendas of at most 12 tasks; keep new ones similarly bounded. `_run_offline` returns the descriptions of the tasks a run performed, for properties about what reruns.
- Properties get scratch directories from `minigun.fixture.temporary_path()` (under `.minigun/`, gitignored, removed after the run). Paths passed to the API must be absolute, so call `.resolve()` on them.
- Wrap API calls in tests with `_quiet()` or go through `_run_offline`; tickle prints to stdout and logs failures at CRITICAL. `_run_offline` collects lines starting with `[tickle] Task `, so tasks in properties about what runs need descriptions starting with `Task `.
- `tests/tasks/success.py` writes each output as a digest of its arguments, its inputs and the files they depend on through `depend.yaml` (as a compiler reads headers), so a stale output differs from a clean build's. It fails when an input's content is exactly `fail` (`_add_trigger` uses that to make a task fail without changing its definition), ignores `--salt` words (a way to change a command), and appends to `--append` files after reading its inputs (an edit during the run).
- The model-based property ("matches a clean build") applies random changes, failures and moves with a run after each, then compares the outputs with a clean build. It is the main guard of incremental correctness; extend its actions when adding behaviour that affects what reruns.
- Evaluation properties take a worker count from `worker_counts` (1 to 8) and pass it to `_run_offline`; do the same in new ones, so the serial case is covered.
- Process-free properties build `evaluate.Task` graphs directly and call `graph.compile` or drive an `Evaluator` with fake work; prefer them for scheduler behaviour, since they are fast and deterministic.
- A new property should fail on the code it guards. Check by running it against the previous commit in a detached worktree, or against a copy of the repo with the guarded logic broken (for example propagation in `graph.propagate` removed). Generated agendas give tasks random stage bumps so stage order is not implied by dependency order; keep generators able to tell such behaviours apart.
- Online mode has one property (outputs built, agenda edits followed, stops); check other changes to it, such as input and depend edits, by hand with the example project.

## Gotchas

- All API paths must be absolute, and the agenda, depend and cache paths must lie inside the target directory (asserted). Paths inside the agenda are relative to the agenda file's directory.
- The cache (format `version` 2, see the layout comment in `api.py`) records, per task hash, the input closure hashes taken when the task started; they are written only when the task succeeds, in `_eval_cmd`. `_make_schedule` must only read them: recording hashes before a task has succeeded makes a failed or interrupted task look up to date. Paths in the cache are relative to the target directory. Changing what goes into the task hash in `agenda.compile`, what goes into input closures, or the cache layout reruns every task for every user; bump `_CACHE_VERSION` for layout changes.
- Input closures stop at task outputs: a task hashes its inputs and their depend-file closures, not the inputs of the tasks producing them. Upstream changes reach it through `graph.propagate`.
- Depend entries that name a task's output add a dependency on that task, so a depend change rebuilds the graph, not only the closures.
- Clean must never delete user data: it keeps current sources, files whose recorded `(mtime_ns, size)` no longer match, folders, and paths outside the target directory.
- Concurrency: `Worker.stop()` can run before the thread is scheduled, so stop state is a `threading.Event` set from outside. Any exception in a worker must end up as a `TaskError` with `task_done()` called, or the main loop waits for the batch forever. `pause()` takes the write side of the lock and waits for running sequences, so never call it from a worker. Cache updates from workers go under `cache.lock`.
- Rescheduling callbacks run on the watchdog thread; they must build new state before committing it and always `resume()` (see `OnlineEvaluator._reschedule`). Stop the watcher in `start()` after the loop, never in `stop()`.
- Watchdog events arrive on the observer thread. Saves may show up as created or moved rather than modified, so handlers compare content hashes rather than trusting the event type.
- Offline mode stops through a forced terminating task that depends on every other task; on a task error it raises `TaskError` out of `start()`, and skipped tasks make `offline()` return False after the rest has run. Online mode logs the error and drops the current schedule until the next file change.
- Task errors carry the generation of the program they came from; only an error from the current program clears it. Keep that when touching the main loop, or a stale error cancels a fresh schedule.
- Sequences currently hold one task each (joining broke stage order across flows); the ready-queue scheduler planned for 0.6.0 replaces batches and sequences.
