# External module dependencies
from typing import Optional, Tuple, List, Dict, Set
from multiprocessing import cpu_count
from functools import partial
from pathlib import Path
from time import sleep
import subprocess
import hashlib
import os

# Internal module dependencies
from .evaluate import Task, TaskError, Evaluator
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
    return cpu_count() - 1

###############################################################################
# Task graph util
###############################################################################
def _hash_wait(file_path : Path) -> str:
    while not file_path.exists(): sleep(1)
    with file_path.open('rb') as file:
        return hashlib.md5(file.read()).hexdigest()

def _hash(file_path : Path) -> Optional[str]:
    if not file_path.exists(): return
    with file_path.open('rb') as file:
        return hashlib.md5(file.read()).hexdigest()

def _make_graph(
    cwd_path : Path,
    agenda_data : agenda.CompiledAgenda,
    cache : Cache
    ) -> List[Task]:
    def _make_dirs(dir_path : Path):
        for parent_path in reversed(dir_path.parents):
            try:
                os.mkdir(parent_path)
                cache['folders'].add(parent_path)
            except: continue
        cache.flush()

    def _eval_cmd(task_name : str, task_data : agenda.CompiledTask):
        log.debug('%s: %s' % (
            task_data.description,
            ' '.join(task_data.command)
        ))
        log.info(task_data.description)

        # Ensure output folders exists
        for output_path in task_data.outputs:
            _make_dirs(output_path)

        # Track generated files
        cache['files'] = Set[Path].union(cache['files'], {
            str(output) for output in task_data.outputs
        })

        # Evaluate task command
        result = subprocess.run(
            task_data.command,
            capture_output = True,
            text = True,
            cwd = cwd_path
        )

        # Check for failed evaluation
        if result.returncode != 0:
            cache.flush()
            raise TaskError(task_data.description, result.stderr)

        # Update cached hashes
        for input in list(cache['hashes'][task_name].keys()):
            cache['hashes'][task_name][input] = _hash_wait(Path(input))
        cache.flush()

        # Done
        if len(result.stdout) == 0: return
        return result.stdout

    # Create tasks
    tasks : List[Task] = list()
    output_map : Dict[str, Tuple[agenda.CompiledTask, Task]] = dict()
    for index, task_data in enumerate(agenda_data):
        task_name = 'task%d' % index
        task = Task(
            task_data.flows,
            partial(_eval_cmd, task_name, task_data)
        )
        tasks.append(task)
        for file_path in task_data.outputs:
            output = str(file_path)
            if output in output_map:
                raise RuntimeError('Multiple tasks output to %s' % output)
            output_map[output] = (task_data, task)

    # Define explicit dependencies
    for index, dst_node in enumerate(tasks):
        dst_data = agenda_data[index]
        for file_path in dst_data.inputs:
            input = str(file_path)
            if input not in output_map: continue
            src_data, src_node = output_map[input]
            try: dst_node.add_dependency(src_node)
            except Exception as e:
                raise RuntimeError('%s:\n\t\"%s\" -> \"%s\"' % (
                    str(e), dst_data.description, src_data.description
                ))

    # Done
    return tasks

def _update_depend(
    agenda_data : agenda.CompiledAgenda,
    depend_data : depend.CompiledDepend,
    watcher : FileWatcher,
    cache : Cache
    ) -> Tuple[Set[str], Dict[str, Set[str]]]:

    def _outputs(
        agenda_data : agenda.CompiledAgenda,
        depend_data : depend.CompiledDepend
        ) -> Set[str]:
        return Set[str].union(*[
            { str(output) for output in task_data.outputs }
            for task_data in agenda_data
        ]).union({
            str(src_path)
            for src_path in depend_data.keys()
        })

    def _explicits(
        agenda_data : agenda.CompiledAgenda
        ) -> Set[str]:
        return Set[str].union(*[
            Set[str].union(
                { str(input) for input in task_data.inputs },
                { str(input) for input in task_data.outputs }
            )
            for task_data in agenda_data
        ])

    def _join_graphs(
        agenda_data : agenda.CompiledAgenda,
        depend_data : depend.CompiledDepend
        ) -> Dict[str, Set[str]]:
        result : Dict[str, Set[str]] = dict()
        for task_data in agenda_data:
            dsts = { str(file_path) for file_path in task_data.inputs }
            for src in task_data.outputs:
                result[str(src)] = dsts
        for src_path, dst_paths in depend_data.items():
            src = str(src_path)
            dsts = { str(dst_path) for dst_path in dst_paths }
            if src not in result: result[src] = set()
            result[src] = result[src].union(dsts)
        return result

    def _has_cycle(
        worklist : List[str],
        graph : Dict[str, Set[str]]
        ) -> bool:
        def _visit(
            node : str,
            path : List[str],
            visited : Set[str]
            ) -> bool:
            if node in visited: return False
            if node in path: return True
            _path = path.copy()
            _path.append(node)
            _visited = visited.copy()
            _visited.add(node)
            if node not in graph: return False
            for dep in graph[node]:
                if _visit(dep, _path, _visited): return True
            return False

        visited : Set[str] = set()
        for node in worklist:
            if _visit(node, [], visited): return True
            visited.add(node)
        return False

    def _reachable(
        worklist : List[str],
        deps : Dict[str, Set[str]]
        ) -> List[str]:
        result : List[str] = list()
        while len(worklist) != 0:
            node = worklist.pop(0)
            if node in result: continue
            result.append(node)
            if node not in deps: continue
            worklist += deps[node]
        return result

    def _leafs(
        nodes : List[str],
        graph : Dict[str, Set[str]]
        ) -> List[str]:
        def _is_leaf(node : str):
            if node not in graph: return True
            if len(graph[node]) == 0: return True
            return False
        return list(filter(_is_leaf, nodes))

    def _inverse_graph_alive(
        alive : List[str],
        graph : Dict[str, Set[str]]
        ) -> Dict[str, Set[str]]:
        result : Dict[str, Set[str]] = dict()
        for src, dsts in graph.items():
            if src not in alive: continue
            for dst in dsts:
                if dst not in alive: continue
                if dst not in result: result[dst] = set()
                result[dst].add(src)
        return result

    def _topological_order(
        worklist : List[str],
        deps : Dict[str, Set[str]],
        refs : Dict[str, Set[str]]
        ) -> List[str]:
        result : List[str] = list()
        while len(worklist) != 0:
            node = worklist.pop(0)
            if node in result: continue
            node_deps : Set[str] = deps[node] if node in deps else set()
            if len(node_deps.difference(set(result))) != 0: continue
            result.append(node)
            worklist += refs[node] if node in refs else set()
        return result

    # Find agenda sources and check for cycles against depend
    nodes : List[str] = list(_outputs(agenda_data, depend_data))
    deps : Dict[str, Set[str]] = _join_graphs(agenda_data, depend_data)
    if _has_cycle(nodes[:], deps):
        raise RuntimeError('Cycle found in depend!')

    # Find dependency closures
    depend_closures : Dict[str, Set[str]] = dict()
    alive : List[str] = _reachable(nodes[:], deps)
    ordered_depends = _topological_order(
        _leafs(alive, deps), deps,
        _inverse_graph_alive(alive, deps)
    )
    for src_file in ordered_depends:
        if src_file not in deps:
            depend_closures[src_file] = set()
            continue
        src_deps = deps[src_file]
        depend_closures[src_file] = Set[str].union(src_deps, *[
            depend_closures[dst_file] for dst_file in src_deps
        ])

    # Done
    implicits = set(alive).difference(_explicits(agenda_data))
    return implicits, depend_closures

def _make_schedule(
    target_dir : Path,
    tasks : List[Task],
    agenda_data : agenda.CompiledAgenda,
    depend_closures : Dict[str, Set[str]],
    cache : Cache
    ):
    def _task_index_graph(
        nodes : List[Task],
        node_count : int
        ) -> Dict[int, Set[int]]:
        result : Dict[int, Set[int]] = dict()
        for index, node in enumerate(nodes):
            if index >= node_count: break
            result[index] = set()
            for dep in node.get_deps():
                dep_index = nodes.index(dep)
                if dep_index >= node_count: continue
                result[index].add(dep_index)
        return result

    def _index_graph_inverse(
        graph : Dict[int, Set[int]],
        node_count : int
        ) -> Dict[int, Set[int]]:
        result : Dict[int, Set[int]] = {
            dep : set() for dep in range(node_count)
        }
        for node, deps in graph.items():
            for dep in deps:
                result[dep].add(node)
        return result

    def _index_graph_leafs(
        deps : Dict[int, Set[int]]
        ) -> List[int]:
        result : List[int] = list()
        for node, _deps in deps.items():
            if len(_deps) != 0: continue
            result.append(node)
        return result

    def _topological_order(
        worklist : List[int],
        deps : Dict[int, Set[int]],
        refs : Dict[int, Set[int]]
        ) -> List[int]:
        result : List[int] = list()
        while len(worklist) != 0:
            node = worklist.pop(0)
            result_set = set(result)
            if node in result_set: continue
            if len(deps[node].difference(result_set)) != 0: continue
            worklist += refs[node].difference(result_set)
            result.append(node)
        return result

    # Clear graph progress and prepare cache
    if 'files' not in cache:
        cache['files'] = set()
        cache['folders'] = set()
        cache['recover'] = dict()
        cache['hashes'] = dict()
    for index, task in enumerate(tasks):
        task_name = 'task%d' % index
        task.set_valid(True)
        task.set_active(True)
        if task_name in cache['hashes']: continue
        cache['hashes'][task_name] = {}

    # Task recovery
    hashes : Dict[str, Dict[str, str]] = dict()
    recover = cache['recover']
    cache['recover'] = dict()
    for index, task in enumerate(agenda_data):
        new_task_name = 'task%d' % index
        cache['recover'][task.hash] = new_task_name
        if task.hash in recover:
            old_task_name = recover[task.hash]
            hashes[new_task_name] = cache['hashes'][old_task_name]
        else:
            hashes[new_task_name] = cache['hashes'][new_task_name]
    cache['hashes'] = hashes

    # Disable impossible tasks
    outputs : Set[str] = set()
    task_count = len(agenda_data)
    deps = _task_index_graph(tasks, task_count)
    refs = _index_graph_inverse(deps, task_count)
    leafs = _index_graph_leafs(deps)
    for index in _topological_order(leafs, deps, refs):
        task_data = agenda_data[index]

        # Check for file existance and output generation
        for input in task_data.inputs:
            if input.exists(): continue
            if str(input) in outputs: continue
            log.error('Skipping task \"%s\"' % task_data.description)
            log.debug(
                'Task input \"%s\" does not exist and will not '
                'be generated during task graph evaluation.' % (
                    input.relative_to(target_dir)
                )
            )
            tasks[index].set_active(False)
            break

        # Task is possible collect ouputs
        outputs = Set[str].union(outputs, {
            str(output) for output in task_data.outputs
        })

    # Check input closure
    for index, task in enumerate(tasks):
        if index >= task_count: continue
        task_name = 'task%d' % index
        task_data = agenda_data[index]
        prev_hashes = cache['hashes'][task_name]

        # Check for depend closure change
        inputs = { str(input) for input in task_data.inputs }
        curr_closure = Set[str].union(inputs, *[
            depend_closures[input] for input in inputs
        ])
        prev_closure = { input for input in prev_hashes.keys() }
        if prev_closure != curr_closure:
            task.set_valid(False)
            for file_path in prev_closure.difference(curr_closure):
                del prev_hashes[file_path]
            for file_path in curr_closure.difference(prev_closure):
                prev_hashes[file_path] = _hash(Path(file_path))
            continue

        # Check for file changes
        curr_hashes = {
            file_path : _hash(Path(file_path))
            for file_path in curr_closure
        }
        equal_hashes = {
            file_path : curr_hash == prev_hashes[file_path]
            for file_path, curr_hash in curr_hashes.items()
        }
        if all(equal_hashes.values()): continue
        task.set_valid(False)
        cache['hashes'][task_name] = curr_hashes

    # Flush any cache changes
    cache.flush()

    # Check output files
    for index, task in enumerate(tasks):
        if index >= task_count: continue
        task_data = agenda_data[index]

        # Check for file existence
        if all([ output.exists() for output in task_data.outputs ]): continue
        task.set_valid(False)

    # Propagate invalidity
    graph.propagate(tasks)

    # Compile schedule
    return graph.compile(tasks)

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

        # Setup cache and file watcher
        self._cache = Cache(cache_path)
        self._watcher = FileWatcher()

        # Initial load of agenda
        self._agenda_data = agenda.compile(
            agenda_path.parent,
            agenda.load(agenda_path)
        )
        self._tasks = _make_graph(
            self._target_dir,
            self._agenda_data,
            self._cache
        )

        # Add a terminating task to graph
        terminate_task = Task({'': 0}, self.stop, True)
        terminate_task.add_dependencies(self._tasks)
        self._tasks.append(terminate_task)

        # Initial load of depend
        self._depend_hash = _hash(self._depend_path)
        self._update_depend()

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
        self._update_depend()
        self.resume()

    def _update_depend(self):
        depend_data = (
            depend.compile(
                self._depend_path,
                depend.load(self._depend_path)
            )
            if self._depend_path.exists() else {}
        )
        _, closures = _update_depend(
            self._agenda_data,
            depend_data,
            self._watcher,
            self._cache
        )
        self.reprogram(_make_schedule(
            self._target_dir,
            self._tasks,
            self._agenda_data,
            closures,
            self._cache
        ))

    def start(self):
        log.info('Beginning of evaluation in offline mode')
        self._watcher.start()
        try: super().start()
        except Exception as e:
            log.info('Failed evaluation in offline mode')
            self._watcher.stop()
            raise e

    def stop(self):
        self._watcher.stop()
        super().stop()
        log.info('End of evaluation in offline mode')

def offline(
    target_dir : Path,
    agenda_path : Path,
    depend_path : Path,
    cache_path : Path,
    worker_count : int = default_worker_count()
    ):

    assert target_dir.is_absolute()
    assert agenda_path.is_relative_to(target_dir)
    assert depend_path.is_relative_to(target_dir)
    assert cache_path.is_relative_to(target_dir)

    # Check agenda file path
    if not agenda_path.exists() and not agenda_path.is_file():
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
        log.critical('Task \"%s\" failed with message:\n%s' % (
            error.description, error.message
        ))
        return False
    except RuntimeError as e:
        log.critical(str(e))
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
        self._closures : Dict[str, Set[str]] = dict()
        self._agenda_hash : Optional[str] = _hash(agenda_path)
        self._depend_hash : Optional[str] = _hash(depend_path)
        self._source_hashes : Dict[str, Optional[str]] = dict()
        self._update_agenda()

        # Setup file watching
        self._watcher.subscribe(agenda_path, self._event_agenda)
        self._watcher.subscribe(depend_path, self._event_depend)

    def _event_agenda(self, event : Event):
        if event != Event.Modified:
            raise RuntimeError('Unexpected file event %s' % event)
        agenda_hash = _hash(self._agenda_path)
        if self._agenda_hash == agenda_hash: return
        self._agenda_hash = agenda_hash
        log.info('%s was modified, rescheduling' % (
            self._agenda_path.relative_to(self._target_dir)
        ))
        self.pause()
        self._update_agenda()
        self.resume()

    def _event_depend(self, event : Event):
        depend_hash = _hash(self._depend_path)
        if self._depend_hash == depend_hash: return
        self._depend_hash = depend_hash
        log.info('%s was modified, rescheduling' % (
            self._depend_path.relative_to(self._target_dir)
        ))
        self.pause()
        self._update_depend()
        self.resume()

    def _event_source(self,
        source_path : Path,
        event : Event
        ):
        _source_path = str(source_path)
        if _source_path in self._source_hashes:
            source_hash = _hash(source_path)
            if self._source_hashes[_source_path] == source_hash: return
            self._source_hashes[_source_path] = source_hash
        log.info('%s was modified, rescheduling' % (
            source_path.relative_to(self._target_dir)
        ))
        self.pause()
        self._update_source()
        self.resume()

    def _update_explicits(self, explicits : Set[str]):
        for file_path in self._explicits.difference(explicits):
            _file_path = Path(self._target_dir, file_path)
            self._watcher.unsubscribe(_file_path)
            del self._source_hashes[file_path]
        for file_path in explicits.difference(self._explicits):
            _file_path = Path(self._target_dir, file_path)
            self._watcher.subscribe(
                _file_path, partial(self._event_source, _file_path)
            )
            self._source_hashes[file_path] = _hash(_file_path)
        self._explicits = explicits

    def _update_implicits(self, implicits : Set[str]):
        for file_path in self._implicits.difference(implicits):
            _file_path = Path(self._target_dir, file_path)
            self._watcher.unsubscribe(_file_path)
            del self._source_hashes[file_path]
        for file_path in implicits.difference(self._implicits):
            _file_path = Path(self._target_dir, file_path)
            self._watcher.subscribe(
                _file_path, partial(self._event_source, _file_path)
            )
            self._source_hashes[file_path] = _hash(_file_path)
        self._implicits = implicits

    def _update_agenda(self):
        def _agenda_explicits(
            agenda_data : agenda.CompiledAgenda
            ) -> Set[str]:
            return Set[str].union(*[
                { str(input) for input in task_data.inputs }
                for task_data in agenda_data
            ]).difference(Set[str].union(*[
                { str(output) for output in task_data.outputs }
                for task_data in agenda_data
            ]))

        # Load descriptions
        self._agenda_data = agenda.compile(
            self._agenda_path.parent,
            agenda.load(self._agenda_path)
        )
        self._update_explicits(_agenda_explicits(self._agenda_data))
        self._depend_data = (
            depend.compile(
                self._depend_path,
                depend.load(self._depend_path)
            )
            if self._depend_path.exists() else {}
        )

        # Remake graph and schedule
        self._tasks = _make_graph(
            self._target_dir,
            self._agenda_data,
            self._cache
        )
        implicits, self._closures = _update_depend(
            self._agenda_data,
            self._depend_data,
            self._watcher,
            self._cache
        )
        self._update_implicits(implicits)
        self.reprogram(_make_schedule(
            self._target_dir,
            self._tasks,
            self._agenda_data,
            self._closures,
            self._cache
        ))

    def _update_depend(self):
        self._depend_data = depend.compile(
            self._depend_path,
            depend.load(self._depend_path)
        )
        implicits, self._closures = _update_depend(
            self._agenda_data,
            self._depend_data,
            self._watcher,
            self._cache
        )
        self._update_implicits(implicits)
        self.reprogram(_make_schedule(
            self._target_dir,
            self._tasks,
            self._agenda_data,
            self._closures,
            self._cache
        ))

    def _update_source(self):
        self.reprogram(_make_schedule(
            self._target_dir,
            self._tasks,
            self._agenda_data,
            self._closures,
            self._cache
        ))

    def start(self):
        log.info('Beginning of evaluation in online mode')
        self._watcher.start()
        super().start()

    def stop(self):
        self._watcher.stop()
        super().stop()
        log.info('End of evaluation in online mode')

    def on_task_error(self, error : TaskError):
        log.info('Task \"%s\" failed with message:\n%s' % (
            error.description, error.message
        ))

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
    if not agenda_path.exists() and not agenda_path.is_file():
        log.critical('Agenda file not found: %s' % (
            agenda_path.relative_to(target_dir)
        ))
        return

    # Make online evaluator
    return OnlineEvaluator(
        target_dir,
        agenda_path,
        depend_path,
        cache_path,
        worker_count
    )

###############################################################################
# Clean mode
###############################################################################
def clean(
    target_dir : Path,
    cache_path : Path
    ) -> bool:

    assert target_dir.is_absolute()
    assert cache_path.is_relative_to(target_dir)

    def _empty_dir(dir_path : Path) -> bool:
        return len(os.listdir(dir_path)) == 0

    # Load cache
    if not cache_path.exists(): return True
    cache = Cache(cache_path)

    log.info('Beginning of clean mode')

    # Remove generated files
    for file_path in reversed(sorted(cache['files'])):
        if not os.path.exists(file_path): continue
        log.info('Removing %s' % Path(file_path).relative_to(target_dir))
        os.remove(file_path)

    # Remove empty generated folders
    for dir_path in reversed(sorted(cache['folders'])):
        if not os.path.exists(dir_path): continue
        if not _empty_dir(dir_path): continue
        log.info('Removing %s' % Path(dir_path).relative_to(target_dir))
        os.rmdir(dir_path)

    # Remove cache
    log.info('Removing %s' % cache_path.relative_to(target_dir))
    cache = None
    os.remove(cache_path)

    # Done
    log.info('End of clean mode')
    return True
