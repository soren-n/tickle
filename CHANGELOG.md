# Changelog

## Unreleased

### Changed

- Requires Python 3.11 or later, PyYAML 6.0.3 or later and watchdog 7.1.0 or later.
- Task identity now covers the command a task runs, so the first run after upgrading reruns every task once.
- Commands are built word by word: literal proc words are passed exactly as written, and each argument value becomes one word. Empty argument values are passed on instead of being dropped.
- Evaluation no longer waits on fixed one-second polls, so a run takes about 0.1 seconds of overhead instead of about 2.
- Packaging moved from `setup.py` to `pyproject.toml`; the project is developed with uv.

### Fixed

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
