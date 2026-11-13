# External module dependencies
from typing import Any
from collections.abc import Callable
from dataclasses import dataclass, field
from multiprocessing import cpu_count
from functools import partial
from threading import Lock, Timer
from pathlib import Path
import hashlib
import json
import stat
import time
import re
import os

# Internal module dependencies
from .evaluate import Task, TaskError, Evaluator, Run
from .schedule import Schedule, DONE, FAILED, BLOCKED, WAITING, READY
from .watch import Event, FileWatcher
from .lock import ProjectLock
from .process import Process, tail
from .cache import Cache
from . import agenda
from . import depend
from . import depfile
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

def default_log_dir(dir_path : Path = Path('./')) -> Path:
    return Path(dir_path, 'tickle-logs')

def default_worker_count() -> int:
    return max(1, cpu_count() - 1)

def lock_path(cache_path : Path) -> Path:
    """The lock file that keeps two runs out of the project of a cache."""
    return cache_path.with_name(cache_path.stem + '.lock')

###############################################################################
# Results
###############################################################################
@dataclass
class Result:
    """What an offline run did, by task description. True when the run did
    everything it had to: no task failed, was skipped or was blocked, and
    the agenda was valid."""
    succeeded : list[str] = field(default_factory = list)
    failed : list[str] = field(default_factory = list)
    # Could not run: an input does not exist and no task makes it, or the
    # task has no outputs
    skipped : list[str] = field(default_factory = list)
    # Did not run because a task they read from failed (keep going), or
    # because the run stopped after a failure
    blocked : list[str] = field(default_factory = list)
    # The tasks a dry run would run, in an order they could run in, and why
    planned : list[str] = field(default_factory = list)
    reasons : dict[str, str] = field(default_factory = dict)
    # Why the run could not start, such as an invalid agenda
    error : str | None = None

    def __bool__(self) -> bool:
        return (
            self.error is None
            and len(self.failed) == 0
            and len(self.skipped) == 0
            and len(self.blocked) == 0
        )

###############################################################################
# Files
###############################################################################
Stat = tuple[int, int]

def _md5(file_path : Path) -> str | None:
    try:
        with file_path.open('rb') as file:
            return hashlib.file_digest(file, 'md5').hexdigest()
    except FileNotFoundError:
        return None

def _hash(file_path : Path) -> str | None:
    """The MD5 of a file, or None if it does not exist."""
    if file_path.is_dir():
        raise RuntimeError(
            '%s is a folder; tickle can only track files' % file_path
        )
    return _md5(file_path)

def _stat(file_path : Path) -> Stat | None:
    try: file_stat = os.stat(file_path)
    except FileNotFoundError: return None
    return file_stat.st_mtime_ns, file_stat.st_size

def _rel(target_dir : Path, file_path : Any) -> str:
    # The cache records paths relative to the target directory, so a
    # project keeps its cache when it is moved or copied
    return os.path.relpath(file_path, target_dir)

def _abs(target_dir : Path, rel_path : str) -> Path:
    return Path(os.path.normpath(os.path.join(target_dir, rel_path)))

def _folder_key(path : Path) -> str:
    # In input closures a folder is its path with a trailing separator, so
    # it is not taken for a file of that name
    return str(path) + os.sep

def _is_folder_key(path : str) -> bool:
    return path.endswith(os.sep)

def _key(target_dir : Path, path : str) -> str:
    """How the cache names a closure entry: relative to the target
    directory, with a trailing / for a folder."""
    rel_path = _rel(target_dir, path)
    return rel_path + '/' if _is_folder_key(path) else rel_path

def _is_inside(rel_path : str) -> bool:
    if os.path.isabs(rel_path): return False
    return rel_path != os.pardir and not rel_path.startswith(os.pardir + os.sep)

###############################################################################
# Cache
###############################################################################
# Layout of the cache:
#   version: _CACHE_VERSION
#   tasks:   task hash -> record, a dict with
#              inputs:   {input path: MD5}, the input closure as it was when
#                        the task last started and then succeeded
#              duration: seconds the task took then
#            Records take optional fields, so new ones need no new version.
#   stats:   input path -> (size, mtime_ns, checked_ns, MD5): the MD5 of the
#            file when it had that size and modification time, hashed at
#            checked_ns; lets an unchanged file go without hashing
#   files:   output path -> (mtime_ns, size) after tickle last ran the task
#            writing it, or None if tickle has not run it yet; clean only
#            removes files that still match
#   folders: folders tickle created, for outputs and logs
#   logs:    log files tickle wrote
# Paths are relative to the target directory. Version 2 had task records of
# only {input path: MD5}, and no stats or logs; it is migrated, so upgrading
# reruns nothing. Earlier versions are discarded.
_CACHE_VERSION = 3

# A file modified this close to when it was hashed may have changed again
# within the resolution of its modification time, which is coarse on some
# file systems; such a file is hashed again rather than trusted by its stat
_RACY_NS = 2 * 10**9

class CacheVersionError(RuntimeError):
    pass

def _prepare_cache(cache : Cache):
    """Bring the cache to the current format. A cache from a newer version
    of tickle is refused and left alone, rather than thrown away."""
    with cache.lock:
        version = cache.get('version')
        if isinstance(version, int) and version > _CACHE_VERSION:
            raise CacheVersionError(
                'The cache was written by a newer version of tickle (cache '
                'format %d, this version reads %d); upgrade tickle, or remove '
                'the cache to run every task again' % (version, _CACHE_VERSION)
            )
        if version == _CACHE_VERSION:
            cache['stats'] = cache.get('stats', dict())
            cache['logs'] = cache.get('logs', set())
            return
        if version == 2:
            cache['tasks'] = {
                task_hash : { 'inputs' : inputs }
                for task_hash, inputs in cache['tasks'].items()
            }
            cache['stats'] = dict()
            cache['logs'] = set()
            cache['version'] = _CACHE_VERSION
            cache.mark()
            return
        if len(list(cache)) != 0:
            log.info('The cache was written by an earlier version of tickle; '
                'every task runs once')
        cache.clear()
        cache['version'] = _CACHE_VERSION
        cache['tasks'] = dict()
        cache['stats'] = dict()
        cache['files'] = dict()
        cache['folders'] = set()
        cache['logs'] = set()

def _prune_cache(
    cache : Cache,
    target_dir : Path,
    agenda_data : agenda.CompiledAgenda,
    task_closures : list[set[str]]
    ):
    # Forget tasks that are no longer in the agenda. Keeping them would let
    # a task that is changed back to an earlier definition count as up to
    # date, although its outputs were last written by the other definition.
    # Stats of files no task reads any more go too.
    current = { task_data.hash for task_data in agenda_data }
    read : set[str] = set()
    folders : list[str] = []
    for task_data, closure in zip(agenda_data, task_closures, strict = True):
        for file_path in _full_closure(target_dir, cache, task_data, closure):
            if _is_folder_key(file_path):
                folders.append(_rel(target_dir, file_path) + os.sep)
            else:
                read.add(_rel(target_dir, file_path))
    with cache.lock:
        tasks = cache['tasks']
        for task_hash in list(tasks.keys()):
            if task_hash in current: continue
            del tasks[task_hash]
        stats = cache['stats']
        for rel_path in list(stats.keys()):
            if rel_path in read: continue
            if any(rel_path.startswith(folder) for folder in folders): continue
            del stats[rel_path]
        cache.changed()

def _tree_hash(target_dir : Path, cache : Cache, dir_key : str) -> str | None:
    """A hash of a folder: of the names of everything in it, at any depth,
    and the hashes of its files; None if it does not exist."""
    dir_path = dir_key.rstrip(os.sep)
    if not os.path.isdir(dir_path):
        if os.path.exists(dir_path):
            raise RuntimeError('%s is a file, not a folder' % dir_path)
        return None
    entries : list[list[str | None]] = []
    for root, dirs, files in os.walk(dir_path):
        dirs.sort()
        rel_root = Path(os.path.relpath(root, dir_path))
        for name in dirs:
            entries.append([(rel_root / name).as_posix() + '/', None])
        for name in sorted(files):
            entries.append([
                (rel_root / name).as_posix(),
                _file_hash(target_dir, cache, os.path.join(root, name))
            ])
    return hashlib.md5(json.dumps(entries).encode('utf-8')).hexdigest()

def _file_hash(target_dir : Path, cache : Cache, file_path : str) -> str | None:
    """The MD5 of a file, or None if it does not exist; taken from the
    cache when the file's size and modification time match those recorded
    with it, and the record is not too close to the modification. A folder
    key gives the hash of the folder."""
    if _is_folder_key(file_path): return _tree_hash(target_dir, cache, file_path)
    try: file_stat = os.stat(file_path)
    except FileNotFoundError: return None
    if stat.S_ISDIR(file_stat.st_mode):
        raise RuntimeError(
            '%s is a folder; write it with a trailing / to read a folder' % file_path
        )
    rel_path = _rel(target_dir, file_path)
    with cache.lock: record = cache['stats'].get(rel_path)
    if (record is not None
        and record[0] == file_stat.st_size
        and record[1] == file_stat.st_mtime_ns
        and file_stat.st_mtime_ns < record[2] - _RACY_NS):
        return record[3]
    checked_ns = time.time_ns()
    md5 = _md5(Path(file_path))
    with cache.lock:
        # The stat from before hashing: if the file changes while it is
        # hashed, the next look sees a different stat and hashes again
        cache['stats'][rel_path] = (
            file_stat.st_size, file_stat.st_mtime_ns, checked_ns, md5
        )
        cache.mark()
    return md5

def _record(cache : Cache, task_hash : str) -> dict[str, Any] | None:
    return cache['tasks'].get(task_hash)

def _recorded_implicits(
    target_dir : Path,
    cache : Cache,
    task_data : agenda.CompiledTask
    ) -> set[str]:
    """The files the task's depfile named when it last succeeded."""
    with cache.lock: record = _record(cache, task_data.hash)
    if record is None: return set()
    return { str(_abs(target_dir, rel_path)) for rel_path in record.get('implicit', ()) }

def _full_closure(
    target_dir : Path,
    cache : Cache,
    task_data : agenda.CompiledTask,
    closure : set[str]
    ) -> set[str]:
    """A task's input closure with the files its depfile named."""
    if task_data.depfile is None: return closure
    return closure | _recorded_implicits(target_dir, cache, task_data)

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
    ) -> dict[str, set[str]]:
    """For each file named in the depend file, every file it depends on
    through the depend file, directly or not."""

    def _join_graphs() -> dict[str, set[str]]:
        # Tasks as edges from their outputs to their inputs, joined with the
        # depend file, to check for cycles between the two
        result : dict[str, set[str]] = dict()
        for task_data in agenda_data:
            dsts = { str(file_path) for file_path in task_data.inputs }
            for src in task_data.outputs:
                result[str(src)] = set(dsts)
        for src_path, dst_paths in depend_data.items():
            src = str(src_path)
            if src not in result: result[src] = set()
            result[src].update(str(dst_path) for dst_path in dst_paths)
        return result

    def _has_cycle(graph : dict[str, set[str]]) -> bool:
        # Depth-first search from every node; a dependency found on the
        # current path closes a cycle.
        checked : set[str] = set()
        for root_node in graph.keys():
            if root_node in checked: continue
            path : list[str] = [root_node]
            on_path : set[str] = {root_node}
            stack : list[list[str]] = [list(graph.get(root_node, ()))]
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
    deps : dict[str, set[str]] = {
        str(src_path) : { str(dst_path) for dst_path in dst_paths }
        for src_path, dst_paths in depend_data.items()
    }
    closures : dict[str, set[str]] = dict()
    for root in deps:
        if root in closures: continue
        stack = [(root, iter(deps[root]))]
        while len(stack) != 0:
            node, children = stack[-1]
            child = next(children, None)
            if child is None:
                stack.pop()
                closure : set[str] = set()
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
    closures : dict[str, set[str]]
    ) -> set[str]:
    result : set[str] = set()
    for input in task_data.inputs:
        if input in task_data.folders:
            result.add(_folder_key(input))
            continue
        result.add(str(input))
        result.update(closures.get(str(input), ()))
    return result

def _make_dirs(target_dir : Path, cache : Cache, file_path : Path):
    created : list[Path] = list()
    for parent_path in reversed(file_path.parents):
        if parent_path.exists(): continue
        try: os.mkdir(parent_path)
        except FileExistsError: continue
        created.append(parent_path)
    if len(created) == 0: return
    with cache.lock:
        cache['folders'].update(_rel(target_dir, path) for path in created)
        cache.changed()

###############################################################################
# Running tasks
###############################################################################
# How many of the last lines of a failing task's log are printed
_FAILURE_TAIL = 20

def _log_path(log_dir : Path, task_data : agenda.CompiledTask) -> Path:
    # Named after the task, and its hash, so two tasks with one description
    # keep separate logs; each run of a task replaces its last log
    slug = re.sub(r'[^a-z0-9]+', '-', task_data.description.lower()).strip('-')
    return log_dir / ('%s-%s.log' % (slug[:60] or 'task', task_data.hash[:8]))

def _outputs_exist(task_data : agenda.CompiledTask) -> bool:
    return all(
        output.is_dir() if output in task_data.folders else output.is_file()
        for output in task_data.outputs
    )

def _clear_folder(target_dir : Path, cache : Cache, folder : Path):
    # Remove the files the last run of a task left in its output folder,
    # those nobody changed since, so no stale file outlives a new run
    prefix = _rel(target_dir, folder) + os.sep
    with cache.lock:
        recorded = [
            (rel_path, file_stat)
            for rel_path, file_stat in cache['files'].items()
            if rel_path.startswith(prefix)
        ]
    for rel_path, file_stat in recorded:
        file_path = _abs(target_dir, rel_path)
        if file_stat is None or _stat(file_path) != file_stat: continue
        try: os.remove(file_path)
        except OSError: continue
        with cache.lock: cache['files'].pop(rel_path, None)

def _record_folder(target_dir : Path, cache : Cache, folder : Path):
    # What the task left in its output folder is what clean may remove
    if not folder.is_dir(): return
    with cache.lock:
        for root, dirs, files in os.walk(folder):
            for name in dirs:
                cache['folders'].add(_rel(target_dir, os.path.join(root, name)))
            for name in files:
                file_path = Path(root, name)
                cache['files'][_rel(target_dir, file_path)] = _stat(file_path)

def _eval_cmd(
    target_dir : Path,
    cache : Cache,
    log_dir : Path,
    task_data : agenda.CompiledTask,
    closure : set[str],
    run : Run
    ) -> str | None:

    # The inputs as they are now, before the command reads them; a change
    # made while the task runs then still counts as a change afterwards
    full_closure = _full_closure(target_dir, cache, task_data, closure)
    input_hashes = {
        _key(target_dir, file_path) : _file_hash(target_dir, cache, file_path)
        for file_path in full_closure
    }
    run.info['inputs'] = input_hashes

    # Early cutoff: a task scheduled only because a task it reads from ran
    # is up to date after all if that task wrote what it wrote before
    with cache.lock: record = _record(cache, task_data.hash)
    if (not task_data.always
        and record is not None
        and record.get('inputs') == input_hashes
        and _outputs_exist(task_data)):
        log.debug('%s: up to date, its inputs came out unchanged' % (
            task_data.description
        ))
        run.info['cutoff'] = True
        return None

    log.debug('%s: %s' % (
        task_data.description,
        ' '.join(task_data.command)
    ))
    log.info(task_data.description)

    # Ensure output and log folders exist; an output folder is emptied of
    # what the last run left in it
    for output_path in task_data.outputs:
        if output_path in task_data.folders:
            _clear_folder(target_dir, cache, output_path)
            _make_dirs(target_dir, cache, output_path / 'file')
        else:
            _make_dirs(target_dir, cache, output_path)
    log_path = _log_path(log_dir, task_data)
    _make_dirs(target_dir, cache, log_path)
    with cache.lock:
        cache['logs'].add(_rel(target_dir, log_path))
        cache.changed()

    # Evaluate task command
    try:
        try: process = Process(task_data.command, target_dir, log_path)
        except OSError as error:
            # For example a command that is not installed
            raise TaskError(task_data.description, str(error)) from error
        run.on_cancel(process.stop)
        outcome = process.wait(task_data.timeout)
    finally:
        # Whatever the command left behind is what clean may remove
        for output_path in task_data.outputs:
            if output_path in task_data.folders:
                _record_folder(target_dir, cache, output_path)
        with cache.lock:
            for output_path in task_data.outputs:
                if output_path in task_data.folders: continue
                cache['files'][_rel(target_dir, output_path)] = _stat(output_path)
            if task_data.depfile is not None:
                cache['files'][_rel(target_dir, task_data.depfile)] = _stat(task_data.depfile)
            cache.changed()

    # Check for failed evaluation
    if outcome.stopped:
        raise TaskError(task_data.description, 'stopped')
    if outcome.timed_out or outcome.returncode != 0:
        lines, count = tail(log_path, _FAILURE_TAIL)
        reason = (
            'timed out after %g seconds' % task_data.timeout
            if outcome.timed_out else
            'exited with code %s' % outcome.returncode
        )
        message = reason
        if count != 0:
            message += '; %s of its output:\n%s' % (
                'the last %d lines' % len(lines) if count > len(lines) else 'all',
                '\n'.join(lines)
            )
        message += '\n(log: %s)' % _rel(target_dir, log_path)
        raise TaskError(task_data.description, message)

    # Report what the task did not do
    for output_path in sorted(task_data.outputs):
        if output_path.exists(): continue
        log.warning('"%s" succeeded without writing %s' % (
            task_data.description, _rel(target_dir, output_path)
        ))

    # The files its depfile names now count as its inputs: those it named
    # before were hashed when it started, new ones are hashed now
    implicits : list[str] = []
    if task_data.depfile is not None:
        deps = depfile.load(task_data.depfile)
        if deps is None:
            log.warning('"%s" succeeded without writing its depfile %s' % (
                task_data.description, _rel(target_dir, task_data.depfile)
            ))
        else:
            own = { str(path) for path in task_data.outputs }
            own.add(str(task_data.depfile))
            for dep in deps:
                dep_path = str(agenda.resolve_path(target_dir, dep))
                if dep_path in closure or dep_path in own: continue
                if dep_path not in implicits: implicits.append(dep_path)
    inputs = {
        _key(target_dir, file_path) : input_hashes[_key(target_dir, file_path)]
        for file_path in closure
    }
    tracked : list[str] = []
    for dep_path in implicits:
        key = _key(target_dir, dep_path)
        if key in input_hashes: inputs[key] = input_hashes[key]
        else:
            # A folder named in a depfile is not tracked
            try: inputs[key] = _file_hash(target_dir, cache, dep_path)
            except RuntimeError: continue
        tracked.append(dep_path)

    # Record the inputs it ran with
    with cache.lock:
        cache['tasks'][task_data.hash] = {
            'inputs' : inputs,
            'duration' : outcome.duration,
            'implicit' : [ _rel(target_dir, dep_path) for dep_path in tracked ]
        }
        cache.changed()
    return None

def _report_failure(error : TaskError, retrying : bool = False):
    if retrying:
        log.warning('Task \"%s\" failed and runs again: %s' % (
            error.description, error.message.rstrip()
        ))
        return
    log.error('Task \"%s\" failed: %s' % (
        error.description, error.message.rstrip()
    ))

###############################################################################
# Graphs and schedules
###############################################################################
def _weights(cache : Cache, agenda_data : agenda.CompiledAgenda) -> list[float]:
    """Each task's expected duration: the last one recorded, or for a task
    without one the average of those recorded."""
    with cache.lock:
        recorded = [
            (_record(cache, task_data.hash) or {}).get('duration')
            for task_data in agenda_data
        ]
    known = [ value for value in recorded if isinstance(value, (int, float)) ]
    default = sum(known) / len(known) if len(known) != 0 else 1.0
    return [
        float(value) if isinstance(value, (int, float)) else default
        for value in recorded
    ]

def _make_graph(
    target_dir : Path,
    agenda_data : agenda.CompiledAgenda,
    closures : dict[str, set[str]],
    cache : Cache,
    log_dir : Path
    ) -> tuple[list[Task], list[set[str]]]:
    """The tasks of the agenda with their dependencies, and the input closure
    of each task."""

    # Create tasks
    tasks : list[Task] = list()
    task_closures : list[set[str]] = list()
    output_map : dict[str, int] = dict()
    folder_map : dict[str, int] = dict()
    weights = _weights(cache, agenda_data)
    for index, task_data in enumerate(agenda_data):
        closure = _task_closure(task_data, closures)
        task_closures.append(closure)
        tasks.append(Task(
            task_data.flows,
            partial(_eval_cmd, target_dir, cache, log_dir, task_data, closure),
            force = task_data.always,
            description = task_data.description,
            key = task_data.hash,
            pool = task_data.pool,
            retries = task_data.retries,
            weight = weights[index]
        ))
        for file_path in task_data.outputs:
            output = str(file_path)
            if output in output_map:
                raise RuntimeError('Multiple tasks output to %s' % output)
            output_map[output] = index
            if file_path in task_data.folders: folder_map[output] = index

    # No task writes inside another task's output folder
    for output, index in output_map.items():
        for parent in Path(output).parents:
            owner = folder_map.get(str(parent))
            if owner is None or owner == index: continue
            raise RuntimeError(
                'Task \"%s\" writes %s, inside the output folder of task \"%s\"' % (
                    agenda_data[index].description, output,
                    agenda_data[owner].description
                )
            )

    def _producer(path : str) -> int | None:
        # The task writing a file or folder, or the folder it lies in
        path = path.rstrip(os.sep)
        if path in output_map: return output_map[path]
        for parent in Path(path).parents:
            if str(parent) in folder_map: return folder_map[str(parent)]
        return None

    # A task depends on the tasks producing its inputs, and the files its
    # inputs depend on through the depend file
    for index, dst_node in enumerate(tasks):
        dst_data = agenda_data[index]
        for input in sorted(task_closures[index]):
            src_index = _producer(input)
            if src_index is None or src_index == index: continue
            src_data = agenda_data[src_index]
            try: dst_node.add_dependency(tasks[src_index])
            except RuntimeError as e:
                raise RuntimeError('%s:\n\t\"%s\" -> \"%s\"' % (
                    str(e), dst_data.description, src_data.description
                )) from e

    # Reject a cycle through the stage order of flows now, over every task;
    # a schedule only sees the tasks about to run, so an agenda would
    # otherwise be accepted or not depending on what is up to date
    if graph.has_cycle(tasks):
        raise RuntimeError('Cycle detected in agenda!')
    graph.check_stages(tasks)

    # Done
    return tasks, task_closures

def _implicits(
    target_dir : Path,
    cache : Cache,
    agenda_data : agenda.CompiledAgenda,
    task_closures : list[set[str]]
    ) -> set[str]:
    """Files the tasks read only through the depend file or their
    depfiles."""
    explicits : set[str] = set()
    for task_data in agenda_data:
        explicits.update(str(input) for input in task_data.inputs)
        explicits.update(str(output) for output in task_data.outputs)
    result : set[str] = set()
    for task_data, closure in zip(agenda_data, task_closures, strict = True):
        result.update(
            file_path
            for file_path in _full_closure(target_dir, cache, task_data, closure)
            if not _is_folder_key(file_path)
        )
    return result.difference(explicits)

def _wanted(
    target_dir : Path,
    tasks : list[Task],
    agenda_data : agenda.CompiledAgenda,
    targets : list[str] | None
    ) -> set[Task] | None:
    """The tasks the targets need: the tasks they name, by description or
    by an output, and every task those depend on; None for all."""
    if targets is None: return None
    by_desc : dict[str, list[Task]] = dict()
    by_output : dict[str, Task] = dict()
    for task, task_data in zip(tasks, agenda_data, strict = True):
        by_desc.setdefault(task_data.description, []).append(task)
        for output in task_data.outputs: by_output[str(output)] = task
    roots : list[Task] = []
    for target in targets:
        if target in by_desc:
            roots.extend(by_desc[target])
            continue
        path = str(agenda.resolve_path(target_dir, target))
        if path in by_output:
            roots.append(by_output[path])
            continue
        raise RuntimeError(
            'Unknown target %s: no task has that description or output' % target
        )
    result : set[Task] = set()
    worklist = list(roots)
    while len(worklist) != 0:
        task = worklist.pop()
        if task in result: continue
        result.add(task)
        worklist.extend(task.get_deps())
    return result

def _make_schedule(
    target_dir : Path,
    tasks : list[Task],
    agenda_data : agenda.CompiledAgenda,
    task_closures : list[set[str]],
    cache : Cache,
    wanted : set[Task] | None = None,
    record : bool = True
    ) -> list[str]:
    """Decide which tasks are out of date and which cannot run, marking
    them; returns the descriptions of the tasks that cannot run. Reads the
    recorded input hashes but never changes them: only a task that
    succeeds records what it ran with."""
    for task in tasks:
        task.set_valid(True)
        task.set_active(True)

    # Disable impossible tasks
    position : dict[Task, int] = {
        task : index for index, task in enumerate(tasks)
    }
    outputs : set[str] = set()
    folders : set[str] = set()
    def _produced(path : Path) -> bool:
        if str(path) in outputs: return True
        return any(str(parent) in folders for parent in path.parents)
    for task in graph.topological_order(tasks):
        task_data = agenda_data[position[task]]

        # Skip tasks without outputs, unless they run always
        if len(task_data.outputs) == 0 and not task_data.always:
            log.error('Skipping task \"%s\": it has no outputs' % (
                task_data.description
            ))
            task.set_active(False)
        else:
            # Check for file existance and output generation
            for input_path in sorted(task_data.inputs):
                reason = None
                if input_path in task_data.folders:
                    if input_path.exists() and not input_path.is_dir():
                        reason = 'is a file, not a folder'
                    elif not input_path.exists() and not _produced(input_path):
                        reason = ('does not exist and will not be generated '
                            'during task graph evaluation')
                elif input_path.is_dir():
                    reason = 'is a folder; write it with a trailing / to read a folder'
                elif not input_path.exists() and not _produced(input_path):
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

        # Collect the outputs tasks so far would write
        outputs.update(str(output) for output in task_data.outputs)
        folders.update(str(output) for output in task_data.folders if output in task_data.outputs)

    # Compare the input closures, with the files their depfiles named, with
    # those recorded when each task last succeeded; a task without a record
    # has not succeeded since it last changed. Files whose stat matches the
    # cache are not hashed again.
    fallback : dict[Task, str] = dict()
    hashes : dict[str, str | None] = dict()
    def _current_hash(file_path : str) -> str | None:
        if file_path not in hashes:
            hashes[file_path] = _file_hash(target_dir, cache, file_path)
        return hashes[file_path]
    for index, task in enumerate(tasks):
        task.reason = ''
        if not task.get_active(): continue
        task_data = agenda_data[index]
        if task_data.always:
            task.reason = 'it runs always'
            continue
        with cache.lock: record_data = _record(cache, task_data.hash)
        previous = record_data.get('inputs') if record_data is not None else None
        if previous is None:
            task.set_valid(False)
            task.reason = 'it is new or changed'
            continue
        closure = {
            _key(target_dir, file_path) : file_path
            for file_path in _full_closure(
                target_dir, cache, task_data, task_closures[index]
            )
        }
        if set(closure.keys()) != set(previous.keys()):
            task.set_valid(False)
            task.reason = 'it reads other files than when it last ran'
            continue
        for key, file_path in sorted(closure.items()):
            if _current_hash(file_path) == previous[key]: continue
            task.set_valid(False)
            # A generated input is better explained by the task writing it,
            # when that one runs; see below
            if _produced(Path(file_path.rstrip(os.sep))):
                fallback[task] = 'its input %s changed' % key
            else:
                task.reason = 'its input %s changed' % key
            break

    # Check output files
    for index, task_data in enumerate(agenda_data):
        for output in sorted(task_data.outputs):
            if output.is_dir() if output in task_data.folders else output.is_file():
                continue
            tasks[index].set_valid(False)
            if tasks[index].reason == '':
                tasks[index].reason = 'its output %s is missing' % (
                    _key(target_dir, _folder_key(output))
                    if output in task_data.folders else
                    _rel(target_dir, output)
                )
            break

    # Propagate invalidity; a task out of date only because a task it reads
    # from runs says so
    graph.propagate(tasks)
    for task in graph.topological_order(tasks):
        if task.get_valid() or task.reason != '': continue
        for dep in sorted(task.get_deps(), key = lambda dep: position.get(dep, 0)):
            if dep.get_valid(): continue
            task.reason = 'it reads from \"%s\", which runs' % dep.description
            break
        if task.reason == '': task.reason = fallback.get(task, '')
    skipped = [
        agenda_data[index].description
        for index, task in enumerate(tasks)
        if not task.get_active() and (wanted is None or task in wanted)
    ]

    # Record the outputs the scheduled tasks may write before any of them
    # runs, so clean finds them even if tickle is interrupted
    if record:
        with cache.lock:
            files = cache['files']
            for index, task in enumerate(tasks):
                if task.get_valid() or not task.get_active(): continue
                if wanted is not None and task not in wanted: continue
                for output_path in agenda_data[index].outputs:
                    if output_path in agenda_data[index].folders: continue
                    files.setdefault(_rel(target_dir, output_path), None)
            cache.store()
    return skipped

###############################################################################
# Evaluators
###############################################################################
class _ProjectEvaluator(Evaluator):
    """What both modes share: the project's files, the cache, building the
    graph and installing schedules."""

    def __init__(self,
        target_dir : Path,
        depend_path : Path,
        cache_path : Path,
        worker_count : int,
        keep_going : bool,
        log_dir : Path,
        targets : list[str] | None
        ):
        super().__init__(worker_count, keep_going = keep_going)
        self._target_dir = target_dir
        self._depend_path = depend_path
        self._log_dir = log_dir
        self._targets = targets
        self._cache = Cache(cache_path)
        _prepare_cache(self._cache)
        self._watcher = FileWatcher()
        self._agenda_data : agenda.CompiledAgenda = []
        self._tasks : list[Task] = []
        self._task_closures : list[set[str]] = []
        self._wanted : set[Task] | None = None
        self.skipped : list[str] = []

    def _build(self,
        agenda_data : agenda.CompiledAgenda,
        pools : dict[str, int],
        record : bool = True
        ) -> tuple[list[Task], list[set[str]], set[Task] | None, list[str], Schedule]:
        closures = _depend_closures(agenda_data, _load_depend(self._depend_path))
        tasks, task_closures = _make_graph(
            self._target_dir, agenda_data, closures, self._cache, self._log_dir
        )
        wanted = _wanted(self._target_dir, tasks, agenda_data, self._targets)
        skipped = _make_schedule(
            self._target_dir, tasks, agenda_data, task_closures, self._cache,
            wanted, record
        )
        self.set_pools(pools)
        schedule = self.make_schedule(tasks, wanted)
        return tasks, task_closures, wanted, skipped, schedule

    def _keep(self,
        agenda_data : agenda.CompiledAgenda,
        task_closures : list[set[str]]
        ) -> Callable[[Run, Task], bool]:
        """Which running tasks may carry on under a new schedule: those whose
        input closure is what it was when they started."""
        closure_by_key = {
            task_data.hash : _full_closure(
                self._target_dir, self._cache, task_data, task_closures[index]
            )
            for index, task_data in enumerate(agenda_data)
        }
        keep : set[Run] = set()
        for run in self.runs():
            started = run.info.get('inputs')
            closure = closure_by_key.get(run.task.key)
            if started is None or closure is None: continue
            try:
                current = {
                    _key(self._target_dir, file_path) :
                        _file_hash(self._target_dir, self._cache, file_path)
                    for file_path in closure
                }
            except RuntimeError: continue
            if current == started: keep.add(run)
        return lambda run, task: run in keep

    def on_idle(self):
        self._cache.flush()

    def on_finished(self, schedule : Schedule):
        self._cache.flush()

class OfflineEvaluator(_ProjectEvaluator):
    def __init__(self,
        target_dir : Path,
        agenda_source : 'Path | agenda.Agenda',
        depend_path : Path,
        cache_path : Path,
        worker_count : int,
        keep_going : bool = False,
        log_dir : Path | None = None,
        targets : list[str] | None = None,
        dry_run : bool = False
        ):
        super().__init__(
            target_dir, depend_path, cache_path, worker_count, keep_going,
            log_dir or default_log_dir(target_dir), targets
        )
        self._dry_run = dry_run

        # Initial load of agenda and depend
        if isinstance(agenda_source, agenda.Agenda):
            agenda_data = agenda_source
            agenda_dir = target_dir
        else:
            agenda_data = agenda.load(agenda_source)
            agenda_dir = agenda_source.parent
        self._pools = dict(agenda_data.pools)
        self._agenda_data = agenda.compile(agenda_dir, agenda_data)
        self._depend_hash = _hash(self._depend_path)
        self._rebuild()
        if not dry_run:
            _prune_cache(
                self._cache, self._target_dir,
                self._agenda_data, self._task_closures
            )

            # Online reload of depend
            self._watcher.subscribe(depend_path, self._event_depend)

    def planned(self) -> list[tuple[str, str]]:
        """The tasks to run, in an order they could run in, and why."""
        schedule = self.schedule()
        if schedule is None: return []
        return [ (task.description, task.reason) for task in schedule.order() ]

    def _event_depend(self, event : Event):
        depend_hash = _hash(self._depend_path)
        if self._depend_hash == depend_hash: return
        self._depend_hash = depend_hash
        log.info('%s was modified, rescheduling' % (
            self._depend_path.relative_to(self._target_dir)
        ))
        try: self._rebuild()
        except Exception as error:
            log.error('Keeping the current schedule: %s' % error)

    def _rebuild(self):
        # The depend file adds dependencies between tasks, so the graph is
        # built again whenever it changes
        tasks, task_closures, wanted, skipped, schedule = self._build(
            self._agenda_data, self._pools, record = not self._dry_run
        )
        keep = self._keep(self._agenda_data, task_closures)
        self._tasks = tasks
        self._task_closures = task_closures
        self._wanted = wanted
        self.skipped = skipped
        self.install(schedule, keep)

    def on_task_failed(self, error : TaskError, retrying : bool):
        _report_failure(error, retrying)

    def on_finished(self, schedule : Schedule):
        super().on_finished(schedule)
        self.stop()

    def result(self) -> Result:
        result = Result(skipped = list(self.skipped))
        schedule = self.schedule()
        if schedule is None: return result
        # A task found up to date when it was about to start did not run
        result.succeeded = [
            task.description for task in schedule.with_state(DONE)
            if not task.cut_off
        ]
        result.failed = [ task.description for task in schedule.with_state(FAILED) ]
        result.blocked = [
            task.description for task in schedule.tasks
            if schedule.state[task] in (BLOCKED, WAITING, READY)
        ]
        return result

    def start(self):
        log.info('Beginning of evaluation in offline mode')
        self._watcher.start()

        # A change made before the watcher started has no event
        self._event_depend(Event.Modified)
        try: super().start()
        finally:
            # Stopped here rather than in stop(), which may run on a
            # worker: joining the watcher there could wait on a reschedule
            # that waits on that worker
            self._watcher.stop()
            self._cache.flush()
        log.info('End of evaluation in offline mode')

def _check_paths(target_dir : Path, *paths : Path | None):
    assert target_dir.is_absolute()
    for path in paths:
        if path is None: continue
        assert path.is_absolute()
        assert path.is_relative_to(target_dir)

def _summarise(result : Result):
    for desc in result.failed:
        log.critical('Task \"%s\" failed' % desc)
    if len(result.blocked) != 0:
        log.critical('%d task(s) did not run because a task failed: %s' % (
            len(result.blocked),
            ', '.join('"%s"' % desc for desc in result.blocked)
        ))
    if len(result.skipped) != 0:
        log.critical('Skipped %d task(s) that could not run: %s' % (
            len(result.skipped),
            ', '.join('"%s"' % desc for desc in result.skipped)
        ))

def offline(
    target_dir : Path,
    agenda_path : 'Path | agenda.Agenda | None' = None,
    depend_path : Path | None = None,
    cache_path : Path | None = None,
    worker_count : int | None = None,
    *,
    keep_going : bool = False,
    targets : list[str] | None = None,
    dry_run : bool = False,
    log_dir : Path | None = None
    ) -> Result:
    """Run every out-of-date task once; see docs/api.md."""
    agenda_source = agenda_path or default_agenda_path(target_dir)
    depend_path = depend_path or default_depend_path(target_dir)
    cache_path = cache_path or default_cache_path(target_dir)
    log_dir = log_dir or default_log_dir(target_dir)
    _check_paths(
        target_dir,
        agenda_source if isinstance(agenda_source, Path) else None,
        depend_path, cache_path, log_dir
    )

    # Check agenda file path
    if isinstance(agenda_source, Path) and not agenda_source.is_file():
        message = 'Agenda file not found: %s' % (
            agenda_source.relative_to(target_dir)
        )
        log.critical(message)
        return Result(error = message)

    # Run offline evaluator
    lock = ProjectLock(lock_path(cache_path))
    try:
        lock.acquire('offline')
        evaluator = OfflineEvaluator(
            target_dir, agenda_source, depend_path, cache_path,
            worker_count or default_worker_count(),
            keep_going = keep_going, log_dir = log_dir,
            targets = targets, dry_run = dry_run
        )
        if dry_run:
            planned = evaluator.planned()
            for desc, reason in planned:
                log.info('Would run \"%s\": %s' % (desc, reason))
            return Result(
                skipped = list(evaluator.skipped),
                planned = [ desc for desc, _ in planned ],
                reasons = dict(planned)
            )
        evaluator.start()
    except RuntimeError as error:
        log.critical(str(error))
        return Result(error = str(error))
    finally:
        lock.release()

    # Tasks that failed, were blocked or could not run fail the evaluation,
    # after everything else has been done
    result = evaluator.result()
    _summarise(result)
    return result

###############################################################################
# Online evaluation mode
###############################################################################
# How long online mode waits for a burst of changes to settle before it
# reschedules, such as an editor's save or a file being copied in
_SETTLE = 0.3

class OnlineEvaluator(_ProjectEvaluator):
    def __init__(self,
        target_dir : Path,
        agenda_path : Path,
        depend_path : Path,
        cache_path : Path,
        worker_count : int,
        keep_going : bool = True,
        log_dir : Path | None = None,
        targets : list[str] | None = None
        ):
        self._lock = ProjectLock(lock_path(cache_path))
        self._lock.acquire('online')
        try:
            super().__init__(
                target_dir, depend_path, cache_path, worker_count, keep_going,
                log_dir or default_log_dir(target_dir), targets
            )
            self._agenda_path = agenda_path

            # Initial load of agenda and depend
            self._explicits : set[str] = set()
            self._implicits : set[str] = set()
            self._pools : dict[str, int] = dict()
            self._agenda_hash : str | None = _hash(agenda_path)
            self._depend_hash : str | None = _hash(depend_path)
            self._source_hashes : dict[str, str | None] = dict()
            self._pending : set[str] = set()
            self._pending_lock = Lock()
            self._update_lock = Lock()
            self._timer : Timer | None = None
            self._update_agenda()

            # Setup file watching
            self._watcher.subscribe(agenda_path, self._event_agenda)
            self._watcher.subscribe(depend_path, self._event_depend)
        except BaseException:
            self._lock.release()
            raise

    ###########################################################################
    # Events: noted at once, applied once they settle
    ###########################################################################
    def _event_agenda(self, event : Event):
        # Editors that save by writing a new file and renaming it over the
        # agenda report creations and moves rather than modifications, so
        # every event is judged by the agenda's content, once it settles
        self._request('agenda')

    def _event_depend(self, event : Event):
        self._request('depend')

    def _event_source(self,
        source_key : str,
        event : Event
        ):
        self._request('source:%s' % source_key)

    def _request(self, kind : str):
        with self._pending_lock:
            self._pending.add(kind)
            if self._timer is not None: self._timer.cancel()
            self._timer = Timer(_SETTLE, self._apply)
            self._timer.daemon = True
            self._timer.start()

    def _apply(self):
        # One update at a time; it builds the new state before committing
        # it, and a failure keeps the current schedule
        with self._update_lock:
            with self._pending_lock:
                pending = self._pending
                self._pending = set()
            try: self._update(pending)
            except RuntimeError as error:
                log.error('Keeping the current schedule: %s' % error)
            except Exception as error:
                log.error('Keeping the current schedule after an unexpected '
                    'error: %s: %s' % (type(error).__name__, error))

    def _update(self, pending : set[str]):
        rebuild = False
        if 'agenda' in pending:
            agenda_hash = _hash(self._agenda_path)
            if agenda_hash is None:
                log.error('%s was removed, keeping the current schedule' % (
                    self._agenda_path.relative_to(self._target_dir)
                ))
            elif agenda_hash != self._agenda_hash:
                self._agenda_hash = agenda_hash
                log.info('%s was modified, rescheduling' % (
                    self._agenda_path.relative_to(self._target_dir)
                ))
                self._update_agenda()
                return
        if 'depend' in pending:
            depend_hash = _hash(self._depend_path)
            if depend_hash != self._depend_hash:
                self._depend_hash = depend_hash
                log.info('%s was modified, rescheduling' % (
                    self._depend_path.relative_to(self._target_dir)
                ))
                rebuild = True
        changed : list[str] = []
        for kind in sorted(pending):
            if not kind.startswith('source:'): continue
            source_key = kind[len('source:'):]
            if source_key in self._source_hashes:
                try: source_hash = self._source_hash(source_key)
                except RuntimeError: continue
                if self._source_hashes[source_key] == source_hash: continue
                self._source_hashes[source_key] = source_hash
            changed.append(source_key)
        for source_key in changed:
            log.info('%s was modified, rescheduling' % (
                _key(self._target_dir, source_key)
            ))
        if rebuild:
            self._rebuild(self._agenda_data, self._pools)
        elif len(changed) != 0:
            self._update_source()

    def _source_hash(self, source_key : str) -> str | None:
        if _is_folder_key(source_key):
            return _tree_hash(self._target_dir, self._cache, source_key)
        return _hash(Path(source_key))

    def _watch(self, current : set[str], files : set[str]):
        # Files are watched alone, folders with everything in them
        for file_path in current.difference(files):
            self._watcher.unsubscribe(Path(file_path))
            self._source_hashes.pop(file_path, None)
        for file_path in files.difference(current):
            self._watcher.subscribe(
                Path(file_path), partial(self._event_source, file_path),
                tree = _is_folder_key(file_path)
            )
            try: self._source_hashes[file_path] = self._source_hash(file_path)
            except RuntimeError: self._source_hashes[file_path] = None

    ###########################################################################
    # Rescheduling, without waiting for running tasks
    ###########################################################################
    def _update_agenda(self):
        agenda_data = agenda.load(self._agenda_path)
        compiled = agenda.compile(self._agenda_path.parent, agenda_data)
        self._rebuild(compiled, dict(agenda_data.pools))

    def _rebuild(self,
        agenda_data : agenda.CompiledAgenda,
        pools : dict[str, int]
        ):
        def _explicits() -> set[str]:
            # The inputs no task writes, folders as folder keys
            inputs : set[str] = set()
            outputs : set[str] = set()
            for task_data in agenda_data:
                inputs.update(
                    _folder_key(input) if input in task_data.folders else str(input)
                    for input in task_data.inputs
                )
                outputs.update(str(output) for output in task_data.outputs)
            def _generated(input : str) -> bool:
                path = Path(input.rstrip(os.sep))
                if str(path) in outputs: return True
                return any(str(parent) in outputs for parent in path.parents)
            return { input for input in inputs if not _generated(input) }

        # Build everything before changing any state, so an invalid agenda
        # or depend file leaves the current ones in place
        tasks, task_closures, wanted, skipped, schedule = self._build(
            agenda_data, pools
        )
        keep = self._keep(agenda_data, task_closures)

        # Commit and reschedule
        explicits = _explicits()
        implicits = _implicits(self._target_dir, self._cache, agenda_data, task_closures)
        self._watch(self._explicits, explicits)
        self._explicits = explicits
        self._watch(self._implicits, implicits.difference(explicits))
        self._implicits = implicits.difference(explicits)
        self._agenda_data = agenda_data
        self._pools = pools
        self._tasks = tasks
        self._task_closures = task_closures
        self._wanted = wanted
        self.skipped = skipped
        _prune_cache(self._cache, self._target_dir, agenda_data, task_closures)
        self.install(schedule, keep)

    def _update_source(self):
        self.skipped = _make_schedule(
            self._target_dir, self._tasks, self._agenda_data,
            self._task_closures, self._cache, self._wanted
        )
        keep = self._keep(self._agenda_data, self._task_closures)
        self.install(self.make_schedule(self._tasks, self._wanted), keep)

    ###########################################################################
    # Callbacks
    ###########################################################################
    def on_task_failed(self, error : TaskError, retrying : bool):
        _report_failure(error, retrying)

    def start(self):
        log.info('Beginning of evaluation in online mode')
        self._watcher.start()

        # A change made before the watcher started has no event; compare
        # every watched file with what was seen when it was loaded
        self._request('agenda')
        self._request('depend')
        for file_path in self._explicits | self._implicits:
            self._request('source:%s' % file_path)
        try: super().start()
        finally:
            self._watcher.stop()
            with self._pending_lock:
                if self._timer is not None: self._timer.cancel()
            self._cache.flush()
            self._lock.release()
        log.info('End of evaluation in online mode')

    def stop(self, cancel : bool = True):
        """End online mode, from any thread; running tasks are stopped
        unless cancel is False, in which case they finish first."""
        super().stop(cancel)

    def close(self):
        """Release the project without starting, for an evaluator that
        will not run."""
        self._lock.release()

def online(
    target_dir : Path,
    agenda_path : Path | None = None,
    depend_path : Path | None = None,
    cache_path : Path | None = None,
    worker_count : int | None = None,
    *,
    keep_going : bool = True,
    targets : list[str] | None = None,
    log_dir : Path | None = None
    ) -> OnlineEvaluator | None:
    """Make an online evaluator without running it; see docs/api.md."""
    agenda_path = agenda_path or default_agenda_path(target_dir)
    depend_path = depend_path or default_depend_path(target_dir)
    cache_path = cache_path or default_cache_path(target_dir)
    log_dir = log_dir or default_log_dir(target_dir)
    _check_paths(target_dir, agenda_path, depend_path, cache_path, log_dir)

    # Check agenda file path
    if not agenda_path.is_file():
        log.critical('Agenda file not found: %s' % (
            agenda_path.relative_to(target_dir)
        ))
        return None

    # Make online evaluator
    try:
        return OnlineEvaluator(
            target_dir, agenda_path, depend_path, cache_path,
            worker_count or default_worker_count(),
            keep_going = keep_going, log_dir = log_dir, targets = targets
        )
    except RuntimeError as error:
        log.critical(str(error))
        return None

###############################################################################
# Clean mode
###############################################################################
def _sources(
    agenda_path : Path | None,
    depend_path : Path | None
    ) -> set[str]:
    """Files the agenda or depend file read that no task produces."""
    agenda_data : agenda.CompiledAgenda = list()
    if agenda_path is not None and agenda_path.exists():
        agenda_data = agenda.compile(agenda_path.parent, agenda.load(agenda_path))
    depend_data : depend.CompiledDepend = dict()
    if depend_path is not None and depend_path.exists():
        depend_data = _load_depend(depend_path)
    outputs : set[str] = set()
    result : set[str] = set()
    for task_data in agenda_data:
        outputs.update(str(output) for output in task_data.outputs)
        result.update(str(input) for input in task_data.inputs)
    for src_path, dst_paths in depend_data.items():
        result.add(str(src_path))
        result.update(str(dst_path) for dst_path in dst_paths)
    return result.difference(outputs)

def clean(
    target_dir : Path,
    cache_path : Path | None = None,
    agenda_path : Path | None = None,
    depend_path : Path | None = None
    ) -> bool:
    """Delete what earlier runs generated; see docs/api.md."""
    cache_path = cache_path or default_cache_path(target_dir)
    _check_paths(target_dir, cache_path, agenda_path, depend_path)
    if not cache_path.exists(): return True
    lock = ProjectLock(lock_path(cache_path))
    try: lock.acquire('clean')
    except RuntimeError as error:
        log.critical('Not cleaning: %s' % error)
        return False
    try: return _clean(target_dir, cache_path, agenda_path, depend_path)
    finally: lock.release(remove = True)

def _clean(
    target_dir : Path,
    cache_path : Path,
    agenda_path : Path | None,
    depend_path : Path | None
    ) -> bool:

    def _empty_dir(dir_path : Path) -> bool:
        return len(os.listdir(dir_path)) == 0

    # Load cache
    try: sources = _sources(agenda_path, depend_path)
    except RuntimeError as error:
        log.critical('Not cleaning: %s' % error)
        return False
    cache = Cache(cache_path)
    version = cache.get('version')
    if isinstance(version, int) and version > _CACHE_VERSION:
        log.critical('Not cleaning: the cache was written by a newer version '
            'of tickle (cache format %d); upgrade tickle' % version)
        return False

    # Generated files, folders and logs as absolute paths; a cache written
    # before version 2 recorded absolute paths and no file stats
    files : dict[str, Any] = dict()
    folders : set[str] = set()
    logs : set[str] = set()
    if version in (2, _CACHE_VERSION):
        files = {
            str(_abs(target_dir, rel_path)) : file_stat
            for rel_path, file_stat in cache['files'].items()
        }
        folders = {
            str(_abs(target_dir, rel_path))
            for rel_path in cache['folders']
        }
        logs = {
            str(_abs(target_dir, rel_path))
            for rel_path in cache.get('logs', set())
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
        file_stat = files[file_path]
        if file_stat != '' and file_stat != _stat(Path(file_path)):
            log.info('Keeping %s: it changed after tickle wrote it' % rel_path)
            continue
        log.info('Removing %s' % rel_path)
        os.remove(file_path)

    # Remove the logs tickle wrote
    for file_path in sorted(logs):
        rel_path = _rel(target_dir, file_path)
        if not _is_inside(rel_path) or not os.path.isfile(file_path): continue
        if file_path in sources: continue
        log.debug('Removing %s' % rel_path)
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
