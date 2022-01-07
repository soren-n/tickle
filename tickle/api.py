# External module dependencies
from multiprocessing import cpu_count
from functools import partial
from pathlib import Path
from time import sleep
import subprocess
import hashlib
import logging
import os

# Internal module dependencies
from .evaluate import Task, TaskError, Evaluator
from .watch import Event, FileWatcher
from .cache import Cache
from . import agenda
from . import depend
from . import graph

###############################################################################
# Logging helpers
###############################################################################
def _info(msg):
    logging.info(msg)
    print('[tickle] %s' % msg)

def _error(msg):
    logging.error(msg)
    print('[tickle] Error: %s' % msg)

def _critical(msg):
    logging.critical(msg)
    print('[tickle] Critical: %s' % msg)

###############################################################################
# Defaults
###############################################################################
def default_agenda_path(dir_path = Path('./')):
    return Path(dir_path, 'agenda.yaml')

def default_depend_path(dir_path = Path('./')):
    return Path(dir_path, 'depend.yaml')

def default_cache_path(dir_path = Path('./')):
    return Path(dir_path, 'tickle.cache')

def default_log_path(dir_path = Path('./')):
    return Path(dir_path, 'tickle.log')

def default_worker_count():
    return cpu_count() - 1

###############################################################################
# Task graph util
###############################################################################
def _hash_wait(file_path):
    while not file_path.exists(): sleep(1)
    with file_path.open('rb') as file:
        return hashlib.md5(file.read()).hexdigest()

def _hash(file_path):
    if not file_path.exists(): return
    with file_path.open('rb') as file:
        return hashlib.md5(file.read()).hexdigest()

def _make_graph(agenda_data, cache):
    def _make_dirs(dir_path):
        for parent_path in reversed(dir_path.parents):
            try:
                os.mkdir(parent_path)
                cache['folders'].add(parent_path)
            except: continue
        cache.flush()

    def _eval_cmd(task_name, task_data):
        logging.debug('%s: %s' % (
            task_data.description,
            ' '.join(task_data.command)
        ))
        _info(task_data.description)

        # Ensure output folders exists
        for output_path in task_data.outputs:
            _make_dirs(output_path)

        # Evaluate task command
        result = subprocess.run(
            task_data.command,
            capture_output = True,
            text = True
        )

        # Check for failed evaluation
        if result.returncode != 0:
            raise TaskError(task_data.description, result.stderr)

        # Update cached hashes
        for input in cache['hashes'][task_name].keys():
            cache['hashes'][task_name][input] = _hash_wait(Path(input))
        cache['files'] = set.union(cache['files'], {
            str(output) for output in task_data.outputs
        })
        cache.flush()

        # Done
        if len(result.stdout) == 0: return
        return result.stdout

    # Create tasks
    tasks = list()
    output_map = dict()
    for index, task_data in enumerate(agenda_data):
        task_name = 'task%d' % index
        task = Task(task_data.stage, partial(_eval_cmd, task_name, task_data))
        tasks.append(task)
        for file_path in task_data.outputs:
            output = str(file_path)
            if output in output_map:
                raise RuntimeError('Multiple tasks output to %s' % output)
            output_map[output] = task

    # Define explicit dependencies
    for index, dst_node in enumerate(tasks):
        for file_path in agenda_data[index].inputs:
            input = str(file_path)
            if input not in output_map: continue
            src_node = output_map[input]
            dst_node.add_dependency(src_node)

    # Done
    return tasks

def _update_depend(
    agenda_data,
    depend_data,
    watcher,
    cache
    ):

    def _outputs(agenda_data, depend_data):
        return set.union(*[
            { str(output) for output in task_data.outputs }
            for task_data in agenda_data
        ]).union({
            str(src_path)
            for src_path in depend_data.keys()
        })

    def _explicits(agenda_data):
        return set.union(*[
            set.union(
                { str(input) for input in task_data.inputs },
                { str(input) for input in task_data.outputs }
            )
            for task_data in agenda_data
        ])

    def _join_graphs(agenda_data, depend_data):
        result = dict()
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

    def _has_cycle(worklist, graph):
        def _visit(node, path, visited):
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

        visited = set()
        for node in worklist:
            if _visit(node, [], visited): return True
            visited.add(node)
        return False

    def _reachable(worklist, deps):
        result = list()
        while len(worklist) != 0:
            node = worklist.pop(0)
            if node in result: continue
            result.append(node)
            if node not in deps: continue
            worklist += deps[node]
        return result

    def _leafs(nodes, graph):
        return list(filter(lambda node: node not in graph, nodes))

    def _inverse_graph_alive(alive, graph):
        result = dict()
        for src, dsts in graph.items():
            if src not in alive: continue
            for dst in dsts:
                if dst not in alive: continue
                if dst not in result: result[dst] = set()
                result[dst].add(src)
        return result

    def _topological_order(worklist, deps, refs):
        result = list()
        while len(worklist) != 0:
            node = worklist.pop(0)
            if node in result: continue
            node_deps = deps[node] if node in deps else set()
            if len(node_deps.difference(set(result))) != 0: continue
            result.append(node)
            worklist += refs[node] if node in refs else set()
        return result

    # Find agenda sources and check for cycles against depend
    nodes = list(_outputs(agenda_data, depend_data))
    deps = _join_graphs(agenda_data, depend_data)
    if _has_cycle(nodes[:], deps):
        raise RuntimeError('Cycle found in depend!')

    # Find dependency closures
    depend_closures = dict()
    alive = _reachable(nodes[:], deps)
    ordered_depends = _topological_order(
        _leafs(alive, deps), deps,
        _inverse_graph_alive(alive, deps)
    )
    for src_file in ordered_depends:
        if src_file not in deps:
            depend_closures[src_file] = set()
            continue
        src_deps = deps[src_file]
        depend_closures[src_file] = set.union(src_deps, *[
            depend_closures[dst_file] for dst_file in src_deps
        ])

    # Done
    implicits = set(alive).difference(_explicits(agenda_data))
    return implicits, depend_closures

def _make_schedule(tasks, agenda_data, depend_closures, cache):

    # Clear graph progress and prepare cache
    if 'files' not in cache:
        cache['files'] = set()
        cache['folders'] = set()
        cache['recover'] = dict()
        cache['hashes'] = dict()
    for index, task in enumerate(tasks):
        task_name = 'task%d' % index
        task.set_valid(True)
        if task_name in cache['hashes']: continue
        cache['hashes'][task_name] = {}

    # Task recovery
    hashes = dict()
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

    # Check input files
    for index, task in enumerate(tasks):
        if index >= len(agenda_data): continue
        task_name = 'task%d' % index
        task_data = agenda_data[index]
        prev_hashes = cache['hashes'][task_name]

        # Check for depend closure change
        inputs = { str(input) for input in task_data.inputs }
        curr_closure = set.union(inputs, *[
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
        if index >= len(agenda_data): continue
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
    def __init__(self, agenda_path, depend_path, cache_path, worker_count):
        super().__init__(worker_count)
        self._depend_path = depend_path

        # Setup cache and file watcher
        self._cache = Cache(cache_path)
        self._watcher = FileWatcher()

        # Initial load of agenda
        self._agenda_data = agenda.load(agenda_path)
        self._tasks = _make_graph(self._agenda_data, self._cache)

        # Add a terminating task to graph
        end_stage = max(task_data.stage for task_data in self._agenda_data) + 1
        terminate_task = Task(end_stage, self.stop, True)
        terminate_task.add_dependencies(self._tasks)
        self._tasks.append(terminate_task)

        # Initial load of depend
        self._depend_hash = _hash(self._depend_path)
        self._update_depend()

        # Online reload of depend
        self._watcher.subscribe(depend_path, self._event_depend)

    def _event_depend(self, event):
        depend_hash = _hash(self._depend_path)
        if self._depend_hash == depend_hash: return
        self._depend_hash = depend_hash
        _info('%s was modified, rescheduling' % self._depend_path)
        self.pause()
        self._update_depend()
        self.resume()

    def _update_depend(self):
        depend_data = (
            depend.load(self._depend_path)
            if self._depend_path.exists() else {}
        )
        _, closures = _update_depend(
            self._agenda_data,
            depend_data,
            self._watcher,
            self._cache
        )
        self.reprogram(_make_schedule(
            self._tasks,
            self._agenda_data,
            closures,
            self._cache
        ))

    def start(self):
        self._watcher.start()
        super().start()

    def stop(self):
        self._watcher.stop()
        super().stop()

def offline(
    agenda_path = default_agenda_path(),
    depend_path = default_depend_path(),
    cache_path = default_cache_path(),
    log_path = default_log_path(),
    worker_count = default_worker_count(),
    debug = False
    ):

    # Handle logging
    logging.basicConfig(
        filename = log_path,
        encoding = 'utf-8',
        level = 'DEBUG' if debug else 'INFO',
        format = '%(asctime)s | %(levelname)s | %(message)s'
    )

    # Check agenda file path
    if not agenda_path.exists() and not agenda_path.is_file():
        _critical('Agenda file not found: ./%s' % agenda_path)
        return False

    # Run offline evaluator
    _info('Beginning of evaluation in offline mode')
    evaluator = OfflineEvaluator(
        agenda_path,
        depend_path,
        cache_path,
        worker_count
    )
    try: evaluator.start()
    except TaskError as error:
        _critical('Task \"%s\" failed with message:\n%s' % (
            error.description, error.message
        ))
    finally:
        _info('End of evaluation in offline mode')

    # Done
    return True

###############################################################################
# Online evaluation mode
###############################################################################
class OnlineEvaluator(Evaluator):
    def __init__(self, agenda_path, depend_path, cache_path, worker_count):
        super().__init__(worker_count)
        self._agenda_path = agenda_path
        self._depend_path = depend_path

        # Setup cache and file watcher
        self._cache = Cache(cache_path)
        self._watcher = FileWatcher()

        # Initial load of agenda and depend
        self._explicits = set()
        self._implicits = set()
        self._closures = dict()
        self._agenda_hash = _hash(agenda_path)
        self._depend_hash = _hash(depend_path)
        self._source_hashes = dict()
        self._update_agenda()

        # Setup file watching
        self._watcher.subscribe(agenda_path, self._event_agenda)
        self._watcher.subscribe(depend_path, self._event_depend)

    def _event_agenda(self, event):
        if event != Event.Modified:
            raise RuntimeError('Unexpected file event %s' % event)
        agenda_hash = _hash(self._agenda_path)
        if self._agenda_hash == agenda_hash: return
        self._agenda_hash = agenda_hash
        _info('%s was modified, rescheduling' % self._agenda_path)
        self.pause()
        self._update_agenda()
        self.resume()

    def _event_depend(self, event):
        depend_hash = _hash(self._depend_path)
        if self._depend_hash == depend_hash: return
        self._depend_hash = depend_hash
        _info('%s was modified, rescheduling' % self._depend_path)
        self.pause()
        self._update_depend()
        self.resume()

    def _event_source(self, source_path, event):
        _source_path = str(source_path)
        if _source_path in self._source_hashes:
            source_hash = _hash(source_path)
            if self._source_hashes[_source_path] == source_hash: return
            self._source_hashes[_source_path] = source_hash
        _info('%s was modified, rescheduling' % source_path)
        self.pause()
        self._update_source()
        self.resume()

    def _update_explicits(self, explicits):
        for file_path in self._explicits.difference(explicits):
            self._watcher.unsubscribe(Path(file_path))
            del self._source_hashes[file_path]
        for file_path in explicits.difference(self._explicits):
            _file_path = Path(file_path)
            self._watcher.subscribe(
                _file_path, partial(self._event_source, _file_path)
            )
            self._source_hashes[file_path] = _hash(_file_path)
        self._explicits = explicits

    def _update_implicits(self, implicits):
        for file_path in self._implicits.difference(implicits):
            self._watcher.unsubscribe(Path(file_path))
            del self._source_hashes[file_path]
        for file_path in implicits.difference(self._implicits):
            _file_path = Path(file_path)
            self._watcher.subscribe(
                _file_path, partial(self._event_source, _file_path)
            )
            self._source_hashes[file_path] = _hash(_file_path)
        self._implicits = implicits

    def _update_agenda(self):
        def _agenda_explicits(agenda_data):
            return set.union(*[
                { str(input) for input in task_data.inputs }
                for task_data in agenda_data
            ]).difference(set.union(*[
                { str(output) for output in task_data.outputs }
                for task_data in agenda_data
            ]))

        # Load descriptions
        self._agenda_data = agenda.load(self._agenda_path)
        self._update_explicits(_agenda_explicits(self._agenda_data))
        self._depend_data = (
            depend.load(self._depend_path)
            if self._depend_path.exists() else {}
        )

        # Remake graph and schedule
        self._tasks = _make_graph(self._agenda_data, self._cache)
        implicits, self._closures = _update_depend(
            self._agenda_data,
            self._depend_data,
            self._watcher,
            self._cache
        )
        self._update_implicits(implicits)
        self.reprogram(_make_schedule(
            self._tasks,
            self._agenda_data,
            self._closures,
            self._cache
        ))

    def _update_depend(self):
        self._depend_data = depend.load(self._depend_path)
        implicits, self._closures = _update_depend(
            self._agenda_data,
            self._depend_data,
            self._watcher,
            self._cache
        )
        self._update_implicits(implicits)
        self.reprogram(_make_schedule(
            self._tasks,
            self._agenda_data,
            self._closures,
            self._cache
        ))

    def _update_source(self):
        self.reprogram(_make_schedule(
            self._tasks,
            self._agenda_data,
            self._closures,
            self._cache
        ))

    def start(self):
        self._watcher.start()
        super().start()

    def stop(self):
        self._watcher.stop()
        super().stop()

    def on_task_error(self, error):
        _error('Task \"%s\" failed with message:\n%s' % (
            error.description, error.message
        ))

def online(
    agenda_path = default_agenda_path(),
    depend_path = default_depend_path(),
    cache_path = default_cache_path(),
    log_path = default_log_path(),
    worker_count = default_worker_count(),
    debug = False
    ):

    # Handle logging
    logging.basicConfig(
        filename = log_path,
        encoding = 'utf-8',
        level = 'DEBUG' if debug else 'INFO',
        format = '%(asctime)s | %(levelname)s | %(message)s'
    )

    # Check agenda file path
    if not agenda_path.exists() and not agenda_path.is_file():
        _critical('Agenda file not found: %s' % agenda_path)
        return False

    # Run online evaluator
    _info('Beginning of evaluation in online mode')
    evaluator = OnlineEvaluator(
        agenda_path,
        depend_path,
        cache_path,
        worker_count
    ).start()
    _info('End of evaluation in online mode')

    # Done
    return True

###############################################################################
# Clean mode
###############################################################################
def clean(
    cache_path = default_cache_path(),
    log_path = default_log_path(),
    debug = False
    ):

    def _empty_dir(dir_path):
        return len(os.listdir(dir_path)) == 0

    # Handle logging
    logging.basicConfig(
        filename = log_path,
        encoding = 'utf-8',
        level = 'DEBUG' if debug else 'INFO',
        format = '%(asctime)s | %(levelname)s | %(message)s'
    )

    # Load cache
    if not cache_path.exists(): return True
    cache = Cache(cache_path)

    _info('Beginning of clean mode')

    # Remove generated files
    for file_path in reversed(sorted(cache['files'])):
        _info('Removing %s' % file_path)
        os.remove(file_path)

    # Remove empty generated folders
    for dir_path in reversed(sorted(cache['folders'])):
        if not _empty_dir(dir_path): continue
        _info('Removing %s' % dir_path)
        os.rmdir(dir_path)

    # Remove cache
    _info('Removing %s' % cache_path)
    cache = None
    os.remove(cache_path)

    # Done
    _info('End of clean mode')
    return True