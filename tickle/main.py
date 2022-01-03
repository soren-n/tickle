# External module dependencies
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
def _msg(msg):
    logging.info(msg)
    print('[tickle] %s' % msg)

def _error(msg):
    logging.error(msg)
    print('[tickle] Error: %s' % msg)

def _critical(msg):
    logging.critical(msg)
    print('[tickle] Critical: %s' % msg)

###############################################################################
# Task graph util
###############################################################################
cwd = str(Path.cwd())

def _cwd_relative(path):
    _path = str(path)
    if not _path.startswith(cwd): return path
    return Path('.%s' % _path[len(cwd):])

def _hash_wait(file_path):
    while not file_path.exists(): sleep(0)
    with file_path.open('rb') as file:
        return hashlib.md5(file.read()).hexdigest()

def _hash(file_path):
    if not file_path.exists(): return
    with file_path.open('rb') as file:
        return hashlib.md5(file.read()).hexdigest()

def _make_graph(agenda_data, cache):
    def _make_dirs(dir_path):
        _dir_path = _cwd_relative(dir_path)
        for parent_path in reversed(_dir_path.parents):
            _parent_path = str(Path(cwd, parent_path))
            try:
                os.mkdir(_parent_path)
                cache['folders'].add(_parent_path)
            except: continue
        cache.flush()

    def _eval_cmd(task_name, task_data):
        logging.debug('%s: %s' % (
            task_data.description,
            ' '.join(task_data.command)
        ))
        _msg(task_data.description)

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
        cache[task_name].update({
            str(input) : _hash_wait(input)
            for input in task_data.inputs
        })
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
        task = Task(partial(_eval_cmd, task_name, task_data))
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

    def _agenda_sources(agenda_data, depend_data):
        result = set.union(*[
            { str(input) for input in task_data.inputs }
            for task_data in agenda_data
        ])
        return set.union(result, *[
            { str(dst_path) for dst_path in dst_paths }
            for dst_paths in depend_data.values()
        ])

    def _agenda_files(agenda_data):
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
            result[src] = set.union(result[src], dsts)
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

    def _reachable(worklist, graph):
        result = list()
        while len(worklist) != 0:
            node = worklist.pop(0)
            if node in result: continue
            result.append(node)
            if node not in graph: continue
            worklist += graph[node]
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

    # Find agenda sources and check for cycles against depend
    nodes = list(_agenda_sources(agenda_data, depend_data))
    graph = _join_graphs(agenda_data, depend_data)
    if _has_cycle(nodes[:], graph):
        raise RuntimeError('Cycle found in depend!')

    # Find dependency closures
    depend_closures = dict()
    alive = _reachable(nodes[:], graph)
    ordered_depends = _reachable(
        _leafs(alive, graph),
        _inverse_graph_alive(alive, graph)
    )
    for src_file in ordered_depends:
        if src_file not in graph:
            depend_closures[src_file] = set()
            continue
        deps = graph[src_file]
        depend_closures[src_file] = set.union(deps, *[
            depend_closures[dst_file] for dst_file in deps
        ])

    # Done
    implicits = set(alive).difference(_agenda_files(agenda_data))
    return implicits, depend_closures

def _make_schedule(tasks, agenda_data, depend_closures, cache):

    # Clear graph progress and prepare cache
    if 'files' not in cache: cache['files'] = set()
    if 'folders' not in cache: cache['folders'] = set()
    for index, task in enumerate(tasks):
        task_name = 'task%d' % index
        task.set_valid(True)
        if task_name in cache: continue
        cache[task_name] = {}

    # Check input files
    for index, task in enumerate(tasks):
        if index >= len(agenda_data): continue
        task_name = 'task%d' % index
        task_data = agenda_data[index]
        prev_hashes = cache[task_name]

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
        diff_hashes = {
            file_path : curr_hash == prev_hashes[file_path]
            for file_path, curr_hash in curr_hashes.items()
        }
        if all(diff_hashes.values()): continue
        task.set_valid(False)
        cache[task_name] = curr_hashes

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
# Static evaluation mode
###############################################################################
class StaticEvaluator(Evaluator):
    def __init__(self, agenda_path, depend_path, cache_path):
        super().__init__()
        self._depend_path = depend_path

        # Setup cache and file watcher
        self._cache = Cache(cache_path)
        self._watcher = FileWatcher()

        # Initial load of agenda
        self._agenda_data = agenda.load(agenda_path)
        self._tasks = _make_graph(self._agenda_data, self._cache)

        # Add a terminating task to graph
        terminate_task = Task(self.stop, True)
        terminate_task.add_dependencies(self._tasks)
        self._tasks.append(terminate_task)

        # Initial load of depend
        self._depend_hash = _hash(self._depend_path)
        self._update_depend()

        # Dynamic reload of depend
        self._watcher.subscribe(depend_path, self._event_depend)

    def _event_depend(self, event):
        depend_hash = _hash(self._depend_path)
        if self._depend_hash == depend_hash: return
        self._depend_hash = depend_hash
        _msg('./%s was modified, rescheduling' % (
            _cwd_relative(self._depend_path)
        ))
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

def _static(agenda_path, depend_path, cache_path):
    _msg('Beginning of evaluation in static mode')

    # Run static evaluator
    evaluator = StaticEvaluator(
        agenda_path,
        depend_path,
        cache_path
    )
    try: evaluator.start()
    except TaskError as error:
        _critical('Task \"%s\" failed with message:\n%s' % (
            error.description, error.message
        ))
    finally:
        _msg('End of evaluation in static mode')

    # Done
    return True

###############################################################################
# Dynamic evaluation mode
###############################################################################
class DynamicEvaluator(Evaluator):
    def __init__(self, agenda_path, depend_path, cache_path):
        super().__init__()
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
        _msg('./%s was modified, rescheduling' % (
            _cwd_relative(agenda_path)
        ))
        self.pause()
        self._update_agenda()
        self.resume()

    def _event_depend(self, event):
        depend_hash = _hash(self._depend_path)
        if self._depend_hash == depend_hash: return
        self._depend_hash = depend_hash
        _msg('./%s was modified, rescheduling' % (
            _cwd_relative(depend_path)
        ))
        self.pause()
        self._update_depend()
        self.resume()

    def _event_source(self, source_path, event):
        _source_path = str(source_path)
        if _source_path in self._source_hashes:
            source_hash = _hash(source_path)
            if self._source_hashes[_source_path] == source_hash: return
            self._source_hashes[_source_path] = source_hash
        _msg('./%s was modified, rescheduling' % (
            _cwd_relative(source_path)
        ))
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

def _dynamic(agenda_path, depend_path, cache_path):
    _msg('Beginning of evaluation in dynamic mode')

    # Run dynamic evaluator
    evaluator = DynamicEvaluator(
        agenda_path,
        depend_path,
        cache_path
    ).start()

    # Done
    _msg('End of evaluation in dynamic mode')
    return True

###############################################################################
# Dynamic evaluation mode
###############################################################################
def _clean(cache_path):
    def _empty_dir(dir_path):
        return len(os.listdir(dir_path)) == 0

    _msg('Beginning of clean mode')

    # Load cache
    cache = Cache(cache_path)

    # Remove generated files
    for file_path in reversed(sorted(cache['files'])):
        _msg('Removing ./%s' % _cwd_relative(file_path))
        os.remove(file_path)

    # Remove empty generated folders
    for dir_path in reversed(sorted(cache['folders'])):
        if not _empty_dir(dir_path): continue
        _msg('Removing ./%s' % _cwd_relative(dir_path))
        os.rmdir(dir_path)

    # Remove cache
    _msg('Removing ./%s' % _cwd_relative(cache_path))
    cache = None
    os.remove(cache_path)

    # Done
    _msg('End of clean mode')
    return True

###############################################################################
# Main entry
###############################################################################
def main(args):

    # Check if in version mode
    if args.mode == 'version':
        from . import __version__
        print(__version__)
        return True

    # Check agenda file path
    agenda_path = Path(cwd, args.agenda)
    if not agenda_path.exists() and not agenda_path.is_file():
        _critical('Agenda file not found: ./%s' % (
            _cwd_relative(agenda_path)
        ))
        return False

    # Depend and cache file path
    depend_path = Path(cwd, args.depend)
    cache_path = Path(cwd, args.cache)

    # Handle logging
    logging.basicConfig(
        filename = args.log,
        encoding = 'utf-8',
        level = 'DEBUG' if args.debug else 'INFO',
        format = '%(asctime)s | %(levelname)s | %(message)s'
    )

    # Run specified mode
    if args.mode == 'static':
        return _static(agenda_path, depend_path, cache_path)
    if args.mode == 'dynamic':
        return _dynamic(agenda_path, depend_path, cache_path)
    if args.mode == 'clean':
        return _clean(cache_path)

def cli():
    import sys
    import argparse

    parser = argparse.ArgumentParser(
        prog = 'tickle',
        description = 'Task graph scheduling with asynchronous evaluation.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        'mode', type = str,
        choices = ['static', 'dynamic', 'clean', 'version'],
        help = 'static for an inattentive evaluation mode where file modifications are ignored once tasks have been scheduled, dynamic for an attentive evaluation mode where file creations or modifications trigger a rescheduling of the task graph; clean mode will delete all files and folders generated during static or dynamic evaluation; version mode will print the tool version'
    )
    parser.add_argument(
        '--debug',
        dest = 'debug',
        action = 'store_true',
        help = 'Sets debug logging level for tool messages'
    )
    parser.add_argument(
        '-a', '--agenda',
        type = str,
        dest = 'agenda',
        default = './agenda.yaml',
        help = 'Agenda YAML file location; contains the procedure and task definitions, file path must be relative to current working directory'
    )
    parser.add_argument(
        '-d', '--depend',
        type = str,
        dest = 'depend',
        default = './depend.yaml',
        help = 'Depend YAML file location; contains a map of dynamic task dependencies, this file is optional, file path must be relative to current working directory'
    )
    parser.add_argument(
        '-c', '--cache',
        type = str,
        dest = 'cache',
        default = './cache.bin',
        help = 'Binary cache file location; contains inter-run persistent data, file path must be relative to current working directory'
    )
    parser.add_argument(
        '-l', '--log',
        type = str,
        dest = 'log',
        default = './tickle.log',
        help = 'Log file location; contains runtime messages, file path must be relative to current working directory'
    )
    success = main(parser.parse_args())
    if not success: parser.print_help()
    sys.exit(0 if success else -1)
