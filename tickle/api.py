# External module dependencies
from typing import Any, Callable, Optional, Tuple, List, Dict, Set
from multiprocessing import cpu_count
from functools import partial
from pathlib import Path
import subprocess
import hashlib
import os

# Internal module dependencies
from .evaluate import Task, TaskError, Evaluator, Program
from .watch import Event, FileWatcher
from .cache import Cache
from . import agenda
from . import depend
from . import graph
from . import log

###############################################################################
# Defaults
###############################################################################
def default_agenda_path(dir_path : Path = Path('./')) -> Path:
    return Path(dir_path, 'agenda.yaml')

def default_depend_path(dir_path : Path = Path('./')) -> Path:
    return Path(dir_path, 'depend.yaml')

def default_cache_path(dir_path : Path = Path('./')) -> Path:
    return Path(dir_path, 'tickle.cache')

def default_worker_count() -> int:
    return max(1, cpu_count() - 1)

###############################################################################
# Files
###############################################################################
Stat = Tuple[int, int]

def _hash(file_path : Path) -> Optional[str]:
    if file_path.is_dir():
        raise RuntimeError(
            '%s is a folder; tickle can only track files' % file_path
        )
    try:
        with file_path.open('rb') as file:
            return hashlib.file_digest(file, 'md5').hexdigest()
    except FileNotFoundError:
        return None

def _stat(file_path : Path) -> Optional[Stat]:
    try: stat = os.stat(file_path)
    except FileNotFoundError: return None
    return stat.st_mtime_ns, stat.st_size

def _rel(target_dir : Path, file_path : Any) -> str:
    # The cache records paths relative to the target directory, so a
    # project keeps its cache when it is moved or copied
    return os.path.relpath(file_path, target_dir)

def _abs(target_dir : Path, rel_path : str) -> Path:
    return Path(os.path.normpath(os.path.join(target_dir, rel_path)))

def _is_inside(rel_path : str) -> bool:
    if os.path.isabs(rel_path): return False
    return rel_path != os.pardir and not rel_path.startswith(os.pardir + os.sep)

###############################################################################
# Cache
###############################################################################
# Layout of the cache:
#   version: _CACHE_VERSION
#   tasks:   task hash -> {input path: MD5}, the input closure as it was when
#            the task last started and then succeeded
#   files:   output path -> (mtime_ns, size) after tickle last ran the task
#            writing it, or None if tickle has not run it yet; clean only
#            removes files that still match
#   folders: folders tickle created for outputs
# Paths are relative to the target directory.
_CACHE_VERSION = 2

def _prepare_cache(cache : Cache):
    with cache.lock:
        if cache.get('version') == _CACHE_VERSION: return
        if len(list(cache)) != 0:
            log.info('The cache was written by an earlier version of tickle; '
                'every task runs once')
        cache.clear()
        cache['version'] = _CACHE_VERSION
        cache['tasks'] = dict()
        cache['files'] = dict()
        cache['folders'] = set()

def _prune_cache(cache : Cache, agenda_data : agenda.CompiledAgenda):
    # Forget tasks that are no longer in the agenda. Keeping them would let
    # a task that is changed back to an earlier definition count as up to
    # date, although its outputs were last written by the other definition.
    current = { task_data.hash for task_data in agenda_data }
    with cache.lock:
        tasks = cache['tasks']
        for task_hash in list(tasks.keys()):
            if task_hash in current: continue
            del tasks[task_hash]
        cache.changed()

###############################################################################
# Task graph util
###############################################################################
def _load_depend(depend_path : Path) -> depend.CompiledDepend:
    # The depend file is optional and may be removed while running
    return depend.compile(
        depend_path.parent,
        depend.load(depend_path)
        if depend_path.exists() else
        depend.empty()
    )

def _depend_closures(
    agenda_data : agenda.CompiledAgenda,
    depend_data : depend.CompiledDepend
    ) -> Dict[str, Set[str]]:
    """For each file named in the depend file, every file it depends on
    through the depend file, directly or not."""

    def _join_graphs() -> Dict[str, Set[str]]:
        # Tasks as edges from their outputs to their inputs, joined with the
        # depend file, to check for cycles between the two
        result : Dict[str, Set[str]] = dict()
        for task_data in agenda_data:
            dsts = { str(file_path) for file_path in task_data.inputs }
            for src in task_data.outputs:
                result[str(src)] = set(dsts)
        for src_path, dst_paths in depend_data.items():
            src = str(src_path)
            if src not in result: result[src] = set()
            result[src].update(str(dst_path) for dst_path in dst_paths)
        return result

    def _has_cycle(graph : Dict[str, Set[str]]) -> bool:
        # Depth-first search from every node; a dependency found on the
        # current path closes a cycle.
        checked : Set[str] = set()
        for root_node in graph.keys():
            if root_node in checked: continue
            path : List[str] = [root_node]
            on_path : Set[str] = {root_node}
            stack : List[List[str]] = [list(graph.get(root_node, ()))]
            while len(stack) != 0:
                node_deps = stack[-1]
                if len(node_deps) == 0:
                    node = path.pop(-1)
                    on_path.remove(node)
                    checked.add(node)
                    stack.pop(-1)
                    continue
                node_dep = node_deps.pop()
                if node_dep in checked: continue
                if node_dep in on_path: return True
                path.append(node_dep)
                on_path.add(node_dep)
                stack.append(list(graph.get(node_dep, ())))
        return False

    if _has_cycle(_join_graphs()):
        raise RuntimeError('Cycle found between the files of the agenda and depend')

    # Closures over the depend file alone: the inputs of the task producing
    # a file are that task's business, and a change to them reaches the
    # tasks after it through propagation
    deps : Dict[str, Set[str]] = {
        str(src_path) : { str(dst_path) for dst_path in dst_paths }
        for src_path, dst_paths in depend_data.items()
    }
    closures : Dict[str, Set[str]] = dict()
    for root in deps:
        if root in closures: continue
        stack = [(root, iter(deps[root]))]
        while len(stack) != 0:
            node, children = stack[-1]
            child = next(children, None)
            if child is None:
                stack.pop()
                closure : Set[str] = set()
                for dep in deps[node]:
                    closure.add(dep)
                    closure.update(closures.get(dep, ()))
                closures[node] = closure
                continue
            if child in closures or child not in deps: continue
            stack.append((child, iter(deps[child])))
    return closures

def _task_closure(
    task_data : agenda.CompiledTask,
    closures : Dict[str, Set[str]]
    ) -> Set[str]:
    inputs = { str(input) for input in task_data.inputs }
    result = set(inputs)
    for input in inputs: result.update(closures.get(input, ()))
    return result

def _make_dirs(target_dir : Path, cache : Cache, file_path : Path):
    created : List[Path] = list()
    for parent_path in reversed(file_path.parents):
        if parent_path.exists(): continue
        try: os.mkdir(parent_path)
        except FileExistsError: continue
        created.append(parent_path)
    if len(created) == 0: return
    with cache.lock:
        cache['folders'].update(_rel(target_dir, path) for path in created)
        cache.changed()

# How many of the last lines of a succeeding task's stderr are printed
_STDERR_TAIL = 5

def _eval_cmd(
    target_dir : Path,
    cache : Cache,
    task_data : agenda.CompiledTask,
    closure : Set[str]
    ) -> Optional[str]:
    log.debug('%s: %s' % (
        task_data.description,
        ' '.join(task_data.command)
    ))
    log.info(task_data.description)

    # The inputs as they are now, before the command reads them; a change
    # made while the task runs then still counts as a change afterwards
    input_hashes = {
        _rel(target_dir, file_path) : _hash(Path(file_path))
        for file_path in closure
    }

    # Ensure output folders exists
    for output_path in task_data.outputs:
        _make_dirs(target_dir, cache, output_path)

    # Evaluate task command
    try:
        # Tasks get no stdin: one waiting for input would hang the run, and
        # tasks running at the same time would compete for the terminal
        result = subprocess.run(
            task_data.command,
            stdin = subprocess.DEVNULL,
            capture_output = True,
            text = True,
            errors = 'replace',
            cwd = target_dir
        )
    except OSError as error:
        # For example a command that is not installed
        raise TaskError(task_data.description, str(error)) from error
    finally:
        # Whatever the command left behind is what clean may remove
        with cache.lock:
            for output_path in task_data.outputs:
                cache['files'][_rel(target_dir, output_path)] = _stat(output_path)
            cache.changed()

    # Check for failed evaluation
    if result.returncode != 0:
        message = result.stderr if result.stderr.strip() else result.stdout
        raise TaskError(task_data.description, message)

    # Report what the task said and did not do
    # Tools such as ffmpeg log every run on stderr, so only its end is
    # printed; the log file keeps all of it
    if result.stderr.strip():
        log.warning(
            '"%s" wrote to stderr' % task_data.description,
            result.stderr, tail = _STDERR_TAIL
        )
    for output_path in sorted(task_data.outputs):
        if output_path.exists(): continue
        log.warning('"%s" succeeded without writing %s' % (
            task_data.description, _rel(target_dir, output_path)
        ))

    # Record the inputs it ran with
    with cache.lock:
        cache['tasks'][task_data.hash] = input_hashes
        cache.changed()

    # Done
    if len(result.stdout) == 0: return None
    return result.stdout

def _make_graph(
    target_dir : Path,
    agenda_data : agenda.CompiledAgenda,
    closures : Dict[str, Set[str]],
    cache : Cache
    ) -> Tuple[List[Task], List[Set[str]]]:
    """The tasks of the agenda with their dependencies, and the input closure
    of each task."""

    # Create tasks
    tasks : List[Task] = list()
    task_closures : List[Set[str]] = list()
    output_map : Dict[str, int] = dict()
    for index, task_data in enumerate(agenda_data):
        closure = _task_closure(task_data, closures)
        task_closures.append(closure)
        tasks.append(Task(
            task_data.flows,
            partial(_eval_cmd, target_dir, cache, task_data, closure),
            description = task_data.description
        ))
        for file_path in task_data.outputs:
            output = str(file_path)
            if output in output_map:
                raise RuntimeError('Multiple tasks output to %s' % output)
            output_map[output] = index

    # A task depends on the tasks producing its inputs, and the files its
    # inputs depend on through the depend file
    for index, dst_node in enumerate(tasks):
        dst_data = agenda_data[index]
        for input in sorted(task_closures[index]):
            if input not in output_map: continue
            src_index = output_map[input]
            if src_index == index: continue
            src_data = agenda_data[src_index]
            try: dst_node.add_dependency(tasks[src_index])
            except RuntimeError as e:
                raise RuntimeError('%s:\n\t\"%s\" -> \"%s\"' % (
                    str(e), dst_data.description, src_data.description
                )) from e

    # Reject a cycle through the stage order of flows now, over every task;
    # compiling a program only sees the tasks about to run, so an agenda
    # would otherwise be accepted or not depending on what is up to date
    if graph.has_cycle(tasks):
        raise RuntimeError('Cycle detected in agenda!')
    graph.check_stages(tasks)

    # Done
    return tasks, task_closures

def _implicits(
    agenda_data : agenda.CompiledAgenda,
    task_closures : List[Set[str]]
    ) -> Set[str]:
    """Files the tasks read only through the depend file."""
    explicits : Set[str] = set()
    for task_data in agenda_data:
        explicits.update(str(input) for input in task_data.inputs)
        explicits.update(str(output) for output in task_data.outputs)
    result : Set[str] = set()
    for closure in task_closures: result.update(closure)
    return result.difference(explicits)

def _make_schedule(
    target_dir : Path,
    tasks : List[Task],
    agenda_data : agenda.CompiledAgenda,
    task_closures : List[Set[str]],
    cache : Cache
    ) -> Tuple[Program, List[str]]:
    """Decide which tasks are out of date and which cannot run, and compile
    the program performing the first; also returns the descriptions of the
    tasks that cannot run. Reads the recorded input hashes but never changes
    them: only a task that succeeds records what it ran with."""
    _prepare_cache(cache)
    task_count = len(agenda_data)
    for task in tasks:
        task.set_valid(True)
        task.set_active(True)

    # Disable impossible tasks
    position : Dict[Task, int] = {
        task : index for index, task in enumerate(tasks)
    }
    outputs : Set[str] = set()
    for task in graph.topological_order(tasks[:task_count]):
        task_data = agenda_data[position[task]]

        # Skip tasks without outputs
        if len(task_data.outputs) == 0:
            log.error('Skipping task \"%s\"' % task_data.description)
            log.debug('Task have no outputs')
            task.set_active(False)
        else:
            # Check for file existance and output generation
            for input_path in sorted(task_data.inputs):
                reason = None
                if input_path.is_dir():
                    reason = 'is a folder; tickle can only track files'
                elif not input_path.exists() and str(input_path) not in outputs:
                    reason = ('does not exist and will not be generated '
                        'during task graph evaluation')
                if reason is None: continue
                log.error('Skipping task \"%s\": input %s %s' % (
                    task_data.description,
                    _rel(target_dir, input_path),
                    reason
                ))
                task.set_active(False)
                break

        # Task is possible collect ouputs
        outputs.update(str(output) for output in task_data.outputs)

    # Compare the input closures with those recorded when each task last
    # succeeded; a task without a record has not succeeded since it last
    # changed
    recorded : Dict[str, Dict[str, Optional[str]]] = cache['tasks']
    hashes : Dict[str, Optional[str]] = dict()
    def _current_hash(file_path : str) -> Optional[str]:
        if file_path not in hashes: hashes[file_path] = _hash(Path(file_path))
        return hashes[file_path]
    for index in range(task_count):
        task = tasks[index]
        if not task.get_active(): continue
        task_data = agenda_data[index]
        previous = recorded.get(task_data.hash)
        if previous is None:
            task.set_valid(False)
            continue
        closure = {
            _rel(target_dir, file_path) : file_path
            for file_path in task_closures[index]
        }
        if set(closure.keys()) != set(previous.keys()):
            task.set_valid(False)
            continue
        for rel_path, file_path in closure.items():
            if _current_hash(file_path) == previous[rel_path]: continue
            task.set_valid(False)
            break

    # Check output files
    for index in range(task_count):
        task_data = agenda_data[index]
        if all(output.exists() for output in task_data.outputs): continue
        tasks[index].set_valid(False)

    # Propagate invalidity
    graph.propagate(tasks)
    skipped = [
        agenda_data[index].description
        for index in range(task_count)
        if not tasks[index].get_active()
    ]

    # Record the outputs the scheduled tasks may write before any of them
    # runs, so clean finds them even if tickle is interrupted
    with cache.lock:
        files = cache['files']
        for index in range(task_count):
            task = tasks[index]
            if task.get_valid() or not task.get_active(): continue
            for output_path in agenda_data[index].outputs:
                files.setdefault(_rel(target_dir, output_path), None)
        cache.store()

    # Compile schedule
    return graph.compile(tasks), skipped

def _report_failure(error : TaskError):
    log.error('Task \"%s\" failed with message:\n%s' % (
        error.description, error.message.rstrip()
    ))

###############################################################################
# Offline evaluation mode
###############################################################################
class OfflineEvaluator(Evaluator):
    def __init__(self,
        target_dir : Path,
        agenda_path : Path,
        depend_path : Path,
        cache_path : Path,
        worker_count : int
        ):
        super().__init__(worker_count)
        self._target_dir = target_dir
        self._depend_path = depend_path
        self.skipped : List[str] = list()

        # Setup cache and file watcher
        self._cache = Cache(cache_path)
        self._watcher = FileWatcher()

        # Initial load of agenda and depend
        self._agenda_data = agenda.compile(
            agenda_path.parent,
            agenda.load(agenda_path)
        )
        self._depend_hash = _hash(self._depend_path)
        self._rebuild()
        _prune_cache(self._cache, self._agenda_data)

        # Online reload of depend
        self._watcher.subscribe(depend_path, self._event_depend)

    def _event_depend(self, event : Event):
        depend_hash = _hash(self._depend_path)
        if self._depend_hash == depend_hash: return
        self._depend_hash = depend_hash
        log.info('%s was modified, rescheduling' % (
            self._depend_path.relative_to(self._target_dir)
        ))
        self.pause()
        try: self._rebuild()
        except Exception as error:
            log.error('Keeping the current schedule: %s' % error)
        finally: self.resume()

    def _rebuild(self):
        # The depend file adds dependencies between tasks, so the graph is
        # built again whenever it changes
        closures = _depend_closures(
            self._agenda_data,
            _load_depend(self._depend_path)
        )
        tasks, task_closures = _make_graph(
            self._target_dir,
            self._agenda_data,
            closures,
            self._cache
        )

        # Add a terminating task to graph
        terminate_task = Task({'': 0}, self.stop, True)
        terminate_task.add_dependencies(tasks)
        tasks.append(terminate_task)

        program, skipped = _make_schedule(
            self._target_dir,
            tasks,
            self._agenda_data,
            task_closures,
            self._cache
        )
        self._tasks = tasks
        self.skipped = skipped
        self.reprogram(program)

    def on_idle(self):
        self._cache.flush()

    def on_task_failed(self, error : TaskError):
        _report_failure(error)

    def start(self):
        log.info('Beginning of evaluation in offline mode')
        self._watcher.start()
        try: super().start()
        except Exception:
            log.info('Failed evaluation in offline mode')
            raise
        finally:
            # Stopped here rather than in stop(), which runs on a worker:
            # joining the watcher there could wait on a reschedule that
            # waits on that worker.
            self._watcher.stop()
            self._cache.flush()

    def stop(self):
        super().stop()
        log.info('End of evaluation in offline mode')

def offline(
    target_dir : Path,
    agenda_path : Path,
    depend_path : Path,
    cache_path : Path,
    worker_count : int = default_worker_count()
    ) -> bool:

    assert target_dir.is_absolute()
    assert agenda_path.is_relative_to(target_dir)
    assert depend_path.is_relative_to(target_dir)
    assert cache_path.is_relative_to(target_dir)

    # Check agenda file path
    if not agenda_path.is_file():
        log.critical('Agenda file not found: %s' % (
            agenda_path.relative_to(target_dir)
        ))
        return False

    # Run offline evaluator
    try:
        evaluator = OfflineEvaluator(
            target_dir,
            agenda_path,
            depend_path,
            cache_path,
            worker_count
        )
        evaluator.start()
    except TaskError as error:
        # Its message was reported when it failed
        log.critical('Task \"%s\" failed' % error.description)
        return False
    except RuntimeError as e:
        log.critical(str(e))
        return False

    # Tasks that could not run fail the evaluation, after everything else
    # has been done
    if len(evaluator.skipped) != 0:
        log.critical('Skipped %d task(s) that could not run: %s' % (
            len(evaluator.skipped),
            ', '.join('"%s"' % desc for desc in evaluator.skipped)
        ))
        return False

    # Done
    return True

###############################################################################
# Online evaluation mode
###############################################################################
class OnlineEvaluator(Evaluator):
    def __init__(self,
        target_dir : Path,
        agenda_path : Path,
        depend_path : Path,
        cache_path : Path,
        worker_count : int
        ):
        super().__init__(worker_count)
        self._target_dir = target_dir
        self._agenda_path = agenda_path
        self._depend_path = depend_path

        # Setup cache and file watcher
        self._cache = Cache(cache_path)
        self._watcher = FileWatcher()

        # Initial load of agenda and depend
        self._explicits : Set[str] = set()
        self._implicits : Set[str] = set()
        self._tasks : List[Task] = list()
        self._task_closures : List[Set[str]] = list()
        self._agenda_hash : Optional[str] = _hash(agenda_path)
        self._depend_hash : Optional[str] = _hash(depend_path)
        self._source_hashes : Dict[str, Optional[str]] = dict()
        self._update_agenda()

        # Setup file watching
        self._watcher.subscribe(agenda_path, self._event_agenda)
        self._watcher.subscribe(depend_path, self._event_depend)

    def _event_agenda(self, event : Event):
        # Editors that save by writing a new file and renaming it over the
        # agenda report creations and moves rather than modifications, so
        # every event is judged by the agenda's content.
        agenda_hash = _hash(self._agenda_path)
        if self._agenda_hash == agenda_hash: return
        if agenda_hash is None:
            log.error('%s was removed, keeping the current schedule' % (
                self._agenda_path.relative_to(self._target_dir)
            ))
            return
        self._agenda_hash = agenda_hash
        log.info('%s was modified, rescheduling' % (
            self._agenda_path.relative_to(self._target_dir)
        ))
        self._reschedule(self._update_agenda)

    def _event_depend(self, event : Event):
        depend_hash = _hash(self._depend_path)
        if self._depend_hash == depend_hash: return
        self._depend_hash = depend_hash
        log.info('%s was modified, rescheduling' % (
            self._depend_path.relative_to(self._target_dir)
        ))
        self._reschedule(self._update_depend)

    def _event_source(self,
        source_path : Path,
        event : Event
        ):
        _source_path = str(source_path)
        if _source_path in self._source_hashes:
            try: source_hash = _hash(source_path)
            except RuntimeError: return
            if self._source_hashes[_source_path] == source_hash: return
            self._source_hashes[_source_path] = source_hash
        log.info('%s was modified, rescheduling' % (
            source_path.relative_to(self._target_dir)
        ))
        self._reschedule(self._update_source)

    def _reschedule(self, update : Callable[[], None]):
        # Runs on the watcher thread. An invalid agenda or depend file is
        # reported and the current schedule kept; the evaluator must be
        # resumed whatever happens, or it would stay paused for good.
        self.pause()
        try: update()
        except RuntimeError as error:
            log.error('Keeping the current schedule: %s' % error)
        except Exception as error:
            log.error('Keeping the current schedule after an unexpected '
                'error: %s: %s' % (type(error).__name__, error))
        finally: self.resume()

    def _watch(self, current : Set[str], files : Set[str]):
        for file_path in current.difference(files):
            self._watcher.unsubscribe(Path(file_path))
            self._source_hashes.pop(file_path, None)
        for file_path in files.difference(current):
            _file_path = Path(file_path)
            self._watcher.subscribe(
                _file_path, partial(self._event_source, _file_path)
            )
            try: self._source_hashes[file_path] = _hash(_file_path)
            except RuntimeError: self._source_hashes[file_path] = None

    def _update_agenda(self):
        agenda_data = agenda.compile(
            self._agenda_path.parent,
            agenda.load(self._agenda_path)
        )
        self._rebuild(agenda_data)

    def _update_depend(self):
        self._rebuild(self._agenda_data)

    def _rebuild(self, agenda_data : agenda.CompiledAgenda):
        def _explicits() -> Set[str]:
            inputs : Set[str] = set()
            outputs : Set[str] = set()
            for task_data in agenda_data:
                inputs.update(str(input) for input in task_data.inputs)
                outputs.update(str(output) for output in task_data.outputs)
            return inputs.difference(outputs)

        # Build everything before changing any state, so an invalid agenda
        # or depend file leaves the current ones in place
        closures = _depend_closures(
            agenda_data,
            _load_depend(self._depend_path)
        )
        tasks, task_closures = _make_graph(
            self._target_dir, agenda_data, closures, self._cache
        )
        program, _ = _make_schedule(
            self._target_dir, tasks, agenda_data, task_closures, self._cache
        )

        # Commit and reschedule
        explicits = _explicits()
        implicits = _implicits(agenda_data, task_closures)
        self._watch(self._explicits, explicits)
        self._explicits = explicits
        self._watch(self._implicits, implicits.difference(explicits))
        self._implicits = implicits.difference(explicits)
        self._agenda_data = agenda_data
        self._tasks = tasks
        self._task_closures = task_closures
        _prune_cache(self._cache, agenda_data)
        self.reprogram(program)

    def _update_source(self):
        program, _ = _make_schedule(
            self._target_dir,
            self._tasks,
            self._agenda_data,
            self._task_closures,
            self._cache
        )
        self.reprogram(program)

    def on_idle(self):
        self._cache.flush()

    def start(self):
        log.info('Beginning of evaluation in online mode')
        self._watcher.start()
        try: super().start()
        finally:
            self._watcher.stop()
            self._cache.flush()
        log.info('End of evaluation in online mode')

    def stop(self):
        super().stop()

    def on_task_failed(self, error : TaskError):
        _report_failure(error)

    def on_task_error(self, error : TaskError):
        # Reported when it failed; the rest of its program is dropped
        pass

def online(
    target_dir : Path,
    agenda_path : Path,
    depend_path : Path,
    cache_path : Path,
    worker_count : int = default_worker_count()
    ) -> Optional[OnlineEvaluator]:

    assert target_dir.is_absolute()
    assert agenda_path.is_relative_to(target_dir)
    assert depend_path.is_relative_to(target_dir)
    assert cache_path.is_relative_to(target_dir)

    # Check agenda file path
    if not agenda_path.is_file():
        log.critical('Agenda file not found: %s' % (
            agenda_path.relative_to(target_dir)
        ))
        return None

    # Make online evaluator
    try:
        return OnlineEvaluator(
            target_dir,
            agenda_path,
            depend_path,
            cache_path,
            worker_count
        )
    except RuntimeError as error:
        log.critical(str(error))
        return None

###############################################################################
# Clean mode
###############################################################################
def _sources(
    agenda_path : Optional[Path],
    depend_path : Optional[Path]
    ) -> Set[str]:
    """Files the agenda or depend file read that no task produces."""
    agenda_data : agenda.CompiledAgenda = list()
    if agenda_path is not None and agenda_path.exists():
        agenda_data = agenda.compile(agenda_path.parent, agenda.load(agenda_path))
    depend_data : depend.CompiledDepend = dict()
    if depend_path is not None and depend_path.exists():
        depend_data = _load_depend(depend_path)
    outputs : Set[str] = set()
    result : Set[str] = set()
    for task_data in agenda_data:
        outputs.update(str(output) for output in task_data.outputs)
        result.update(str(input) for input in task_data.inputs)
    for src_path, dst_paths in depend_data.items():
        result.add(str(src_path))
        result.update(str(dst_path) for dst_path in dst_paths)
    return result.difference(outputs)

def clean(
    target_dir : Path,
    cache_path : Path,
    agenda_path : Optional[Path] = None,
    depend_path : Optional[Path] = None
    ) -> bool:

    assert target_dir.is_absolute()
    assert cache_path.is_relative_to(target_dir)

    def _empty_dir(dir_path : Path) -> bool:
        return len(os.listdir(dir_path)) == 0

    # Load cache
    if not cache_path.exists(): return True
    try: sources = _sources(agenda_path, depend_path)
    except RuntimeError as error:
        log.critical('Not cleaning: %s' % error)
        return False
    cache = Cache(cache_path)

    # Generated files and folders as absolute paths; a cache written before
    # version 2 recorded absolute paths and no file stats
    files : Dict[str, Any] = dict()
    folders : Set[str] = set()
    if cache.get('version') == _CACHE_VERSION:
        files = {
            str(_abs(target_dir, rel_path)) : stat
            for rel_path, stat in cache['files'].items()
        }
        folders = {
            str(_abs(target_dir, rel_path))
            for rel_path in cache['folders']
        }
    elif isinstance(cache.get('files'), set):
        files = { str(file_path) : '' for file_path in cache['files'] }
        folders = { str(dir_path) for dir_path in cache.get('folders', ()) }

    log.info('Beginning of clean mode')

    # Remove generated files, unless they are sources now, lie outside the
    # target directory, or were changed after tickle wrote them
    for file_path in sorted(files.keys(), reverse = True):
        rel_path = _rel(target_dir, file_path)
        if not os.path.lexists(file_path): continue
        if not _is_inside(rel_path):
            log.warning('Not removing %s: it is outside %s' % (file_path, target_dir))
            continue
        if file_path in sources:
            log.info('Keeping %s: the agenda reads it as a source' % rel_path)
            continue
        if os.path.isdir(file_path):
            log.warning('Not removing %s: it is a folder' % rel_path)
            continue
        stat = files[file_path]
        if stat != '' and stat != _stat(Path(file_path)):
            log.info('Keeping %s: it changed after tickle wrote it' % rel_path)
            continue
        log.info('Removing %s' % rel_path)
        os.remove(file_path)

    # Remove empty generated folders
    for dir_path in sorted(folders, reverse = True):
        rel_path = _rel(target_dir, dir_path)
        if not _is_inside(rel_path): continue
        if not os.path.isdir(dir_path): continue
        if not _empty_dir(Path(dir_path)): continue
        log.info('Removing %s' % rel_path)
        os.rmdir(dir_path)

    # Remove cache, and any write of it that was interrupted
    log.info('Removing %s' % cache_path.relative_to(target_dir))
    os.remove(cache_path)
    temp_path = cache_path.with_name(cache_path.name + '.tmp')
    if temp_path.exists(): os.remove(temp_path)

    # Done
    log.info('End of clean mode')
    return True
