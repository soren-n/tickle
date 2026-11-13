# Tickle

A command line workflow automation tool: it reads an agenda of tasks (commands over input and output files), builds a task graph, decides which tasks are out of date from a persistent cache, and evaluates them concurrently. Published on PyPI as `tickle-soren-n`; the CLI entry point is `tickle`.

User documentation is in `README.md`. Further docs:

- `docs/architecture.md`: how an agenda becomes a schedule and how it is evaluated.
- `docs/api.md`: the Python API.
- `docs/releasing.md`: how to cut a release.

## Commands

```
uv sync                                  # create .venv with runtime and dev dependencies
uv run minigun --time-budget 60          # run the property tests (about 10 minutes; see Testing)
uv run minigun --time-budget 10 --select "encode"   # run the properties whose description matches
uv run ruff check . && uv run pyright   # lint (bug rules only) and type check
uv run tickle offline                    # run the CLI in the current directory
uv build                                 # build sdist and wheel into dist/
```

To try the CLI end to end, copy `examples/hello_world` somewhere outside the repo and run `tickle offline`, `tickle online` or `tickle clean` in it (needs `g++`).

## Layout

- `tickle/cli.py`: argparse front end; resolves paths against the working directory and calls the API.
- `tickle/api.py`: the three modes (`offline`, `online`, `clean`) and `Result`, the `OfflineEvaluator` and `OnlineEvaluator`, the cache layout and migration, stat-based hashing (`_file_hash`), depend closures (`_depend_closures`), graph construction (`_make_graph`), targets (`_wanted`), task evaluation (`_eval_cmd`) and invalidation (`_make_schedule`).
- `tickle/agenda.py`: agenda dataclasses, YAML load/store, and `compile` (procs to commands, flows to stages, paths made absolute).
- `tickle/depend.py`: the depend file (implicit file dependencies such as source to header).
- `tickle/graph.py`: cycle checks, invalidity propagation, and `check_stages` (dependencies against the stage order of flows, over the whole graph).
- `tickle/schedule.py`: `Schedule`, a thread-free state machine over the tasks to run: one barrier node between consecutive stages of a flow, a ready heap ordered by the longest chain of task weights, pools, retries, keep going, adoption, and `order()` for dry runs.
- `tickle/evaluate.py`: `Task`, `Run` (one attempt, with cancel callbacks) and the base `Evaluator`: worker threads that take the next ready task, `install()` for replacing the schedule while tasks run, and the main loop.
- `tickle/process.py`: runs a task's command in its own process group with output to its log file; timeouts and stopping (SIGTERM, then SIGKILL after a grace period).
- `tickle/lock.py`: the project lock, an OS file lock next to the cache.
- `tickle/cache.py`: pickled dict persisted to `tickle.cache`, with batched writes.
- `tickle/watch.py`: watchdog wrapper subscribing callbacks to individual files, also in folders that do not exist yet.
- `tickle/dataspec.py`: typed encode/decode between dataclasses and plain YAML data.
- `tickle/log.py`: logs to the `tickle` logger (with a `NullHandler`, never the root logger) and prints flushed `[tickle] ...` lines to stdout.
- `tests/main.py`: the property tests; `tests/_agenda.py` generates random acyclic agendas; `tests/tasks/` holds the `success.py` and `failure.py` scripts generated agendas run (`success.run` can also be called in-process).

## Conventions

- Match the existing style: `name : Type` annotations with spaces around the colon, builtin generics (`list`, `dict`, `set`) and `X | None`, `%` string formatting, and `####` banner comments separating sections. No formatter is configured; do not reformat unrelated code. `ruff check` (bug rules only) and pyright in basic mode must pass; CI runs them.
- Runtime dependencies are only `pyyaml` and `watchdog`; keep it that way unless there is a strong reason.
- The version lives in `tickle/__init__.py` (`__version__`) and is read by hatchling.
- Record user-visible changes and fixes under "Unreleased" in `CHANGELOG.md`.
- Commit messages: imperative summary line without a type prefix (for example "Fix evaluator hang when ..."), with a body explaining why for anything non-trivial.

## Testing

- CI (`.github/workflows/ci.yml`) runs the lint and type check, the property tests on Linux with Python 3.12 and 3.14 and on macOS and Windows with 3.14, and a small end-to-end agenda with Python 3.11. A nightly run (and a manual dispatch) covers the whole matrix with `TICKLE_TEST_PROCESSES=1`.
- Tests use [minigun](https://github.com/soren-n/minigun-py) (property-based, a sibling project by the same author), which requires Python 3.12+, although tickle itself supports 3.11+.
- The minigun CLI discovers modules in `tests/` that export `spec`; helper modules start with `_` and are imported relatively (`from ._agenda import ...`).
- Properties that evaluate agendas go through `_run_offline` or `_offline_result`, which run the success script in-process (`_InProcess` stands in for `tickle.process.Process`) unless called with `processes = True` or `TICKLE_TEST_PROCESSES=1` is set; any other command still starts a process. Use real processes for properties about processes themselves (signals, timeouts, process groups, logs, stdin). They declare `attempts=_attempts(100)` and use agendas of at most 12 tasks; keep new ones similarly bounded. Declared attempts are not limited by minigun's `--time-budget`, so they set the suite's length. `_attempts` scales them by `TICKLE_TEST_ATTEMPT_SCALE` (default 1); CI sets 0.2 on Windows, where starting processes is slow. `_run_offline` returns the descriptions of the tasks a run performed, for properties about what reruns; `_offline_result` also returns the `Result` and the printed text. Options such as `keep_going`, `targets` and `dry_run` pass through.
- Properties get scratch directories from `minigun.fixture.temporary_path()` (under `.minigun/`, gitignored, removed after the run). Paths passed to the API must be absolute, so call `.resolve()` on them.
- Wrap API calls in tests with `_quiet()` or go through `_run_offline`; tickle prints to stdout and logs failures at CRITICAL. `_run_offline` collects lines starting with `[tickle] Task `, so tasks in properties about what runs need descriptions starting with `Task `.
- `tests/tasks/success.py` writes each output as a digest of its arguments, its inputs and the files they depend on through `depend.yaml` (as a compiler reads headers), so a stale output differs from a clean build's. It fails when an input's content is exactly `fail` (`_add_trigger` uses that to make a task fail without changing its definition), ignores `--salt` words (a way to change a command), and appends to `--append` files after reading its inputs (an edit during the run).
- The model-based property ("matches a clean build") applies random changes, failures and moves with a run after each (some runs keep going, run only targets or follow a dry run), then compares the outputs with a clean build. It is the main guard of incremental correctness; extend its actions when adding behaviour that affects what reruns.
- Evaluation properties take a worker count from `worker_counts` (1 to 8) and pass it to `_run_offline`; do the same in new ones, so the serial case is covered.
- Generated agendas spell the paths in their `inputs` and `outputs` fields in one of several equivalent ways (`respell` in `tests/_agenda.py`; the args keep the plain spelling). Compare or open those paths through `_norm` in test code, never as raw strings.
- Agenda data properties fill the optional task and agenda fields at random through `_annotate`, so new optional fields belong there too.
- Process-free properties build `evaluate.Task` graphs directly and drive a `schedule.Schedule` with `_simulate` (random finish order, checking worker and pool limits), or an `Evaluator` with fake work (`_install`, `_sleep`, `_running`); prefer them for scheduler behaviour, since they are fast and deterministic. Work takes the `Run` as its argument and should end early when `run.cancelled()`.
- A new property should fail on the code it guards. Check by running it against the previous commit in a detached worktree, or against a copy of the repo with the guarded logic broken (for example propagation in `graph.propagate` removed). Generated agendas give tasks random stage bumps so stage order is not implied by dependency order; keep generators able to tell such behaviours apart.
- Online mode has a few properties (outputs built and agenda edits followed, producers of skipped tasks built, running tasks kept or restarted, folders and deleted outputs followed) and a model-based one: random source, header, depend and output edits while it runs must end in a clean build's outputs, with nothing left for offline mode to do. Check other changes to it, such as agenda edits under load, by hand with the example project.

## Gotchas

- All API paths must be absolute, and the agenda, depend and cache paths must lie inside the target directory (asserted). Paths inside the agenda are relative to the agenda file's directory.
- The cache (format `version` 3, see the layout comment in `api.py`) records, per task hash, a record whose `inputs` are the input closure hashes taken when the task started; they are written only when the task succeeds, in `_eval_cmd`, with its `duration`. Records take optional fields, so new fields need no version bump. Version 2 is migrated in `_prepare_cache`; never discard a cache again, and refuse a newer one (`CacheVersionError`) without writing it. File hashes go through `_file_hash`, which trusts the `stats` entry when size and mtime match and the hash was taken more than two seconds after the modification. `_make_schedule` must only read them: recording hashes before a task has succeeded makes a failed or interrupted task look up to date. Paths in the cache are relative to the target directory. The task hash covers the command, args and inputs and outputs relative to the agenda folder, not flows, pools, timeouts, retries or `always` (they change when a task runs, not what it writes). Changing what goes into the task hash in `agenda.compile`, what goes into input closures, or the cache layout reruns every task for every user; bump `_CACHE_VERSION` for layout changes.
- Early cutoff: `_eval_cmd` skips a task whose input closure matches its record and whose outputs exist, so a property expecting the tasks after a rerun task to run must make that task write something different (change a trigger input, or a proc word the script puts in its output); an unused arg or a deleted output reruns only the task itself.
- Closure entries for folders end with `os.sep` (`_folder_key`); the cache names them with a trailing `/` (`_key`). Go through those helpers rather than `_rel` for closure keys.
- Input closures stop at task outputs: a task hashes its inputs and their depend-file closures, not the inputs of the tasks producing them. Upstream changes reach it through `graph.propagate`.
- Depend entries that name a task's output add a dependency on that task, so a depend change rebuilds the graph, not only the closures.
- Clean must never delete user data: it keeps current sources, files whose recorded `(mtime_ns, size)` no longer match, folders, and paths outside the target directory.
- Concurrency: the evaluator's state (current schedule, running runs, stopping) lives under one condition; `stop()` may run before `start()`. Workers run tasks outside it. Any exception from a task's work must end up recorded in the schedule that owns the run (`finish` or, when cancelled, `cancel`), or the schedule never finishes. `install()` may be called at any time, from any thread; it adopts or cancels running runs, and `keep` is called under the condition, so compute expensive decisions before (as `_ProjectEvaluator._keep` does). Cache updates from workers go under `cache.lock`.
- Online changes are noted on the watchdog thread and applied on a timer thread once they settle (`_request`, `_apply`, one at a time); updates build new state before committing it. Stop the watcher in `start()` after the loop, never in `stop()`. `start()` compares every watched file once after the watcher starts, since a change made before has no event.
- Tasks run in their own process group, so Ctrl-C in a terminal does not reach them: tickle must stop them (`Run.cancel` calls `Process.stop`). The CLI turns SIGTERM into a `KeyboardInterrupt` subclass in offline mode.
- Watchdog events arrive on the observer thread. Saves may show up as created or moved rather than modified, so handlers compare content hashes rather than trusting the event type.
- Offline mode stops when its schedule has finished (`on_finished`); `offline()` builds the `Result` from the schedule's states. Without keep going, a failure stops the schedule handing out tasks; running ones finish. Online mode keeps going by default.
- A run's outcome goes to the schedule that owns it, which changes when a new schedule adopts it; an outcome of a cancelled run is dropped. "The evaluator runs each new schedule once ..." and "A running task that a new schedule would start ..." guard this.
