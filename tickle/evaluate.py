# External module dependencies
from typing import Callable, Optional, List, Dict, Set
from threading import Thread
from queue import Queue, Empty
from time import sleep

# Internal module dependencies
from .util import SingleWriteMultipleReadLock
from . import log

###############################################################################
# Exceptions
###############################################################################
class TaskError(Exception):
    def __init__(self, description : str, message : str):
        super().__init__()
        self.description = description
        self.message = message

###############################################################################
# Classes
###############################################################################
Work = Callable[[], Optional[str]]
class Task:
    def __init__(self,
        flows : Dict[str, int],
        work : Work,
        force : bool = False
        ):
        self._valid : bool = False
        self._active : bool = True
        self._force : bool = force
        self._deps : Set['Task'] = set()
        self._refs : Set['Task'] = set()
        self._flows : Dict[str, int] = flows
        self._work : Work = work

    def __hash__(self):
        return hash(id(self))

    def get_deps(self) -> Set['Task']:
        return self._deps

    def get_refs(self) -> Set['Task']:
        return self._refs

    def get_flows(self):
        return self._flows

    def get_valid(self):
        if self._force: return False
        return self._valid

    def set_valid(self, valid : bool):
        self._valid = valid

    def get_active(self):
        if self._force: return True
        return self._active

    def set_active(self, active : bool):
        self._active = active

    def add_dependency(self, other : 'Task'):
        for flow, stage in self._flows.items():
            if flow not in other._flows: continue
            if stage >= other._flows[flow]: continue
            raise RuntimeError(
                'Tasks can not depend on tasks of '
                'later stages in the same workflow!'
            )
        self._deps.add(other)
        other._refs.add(self)

    def add_dependencies(self, others : List['Task']):
        for other in others: self.add_dependency(other)

    def remove_dependency(self, other : 'Task'):
        if other not in self._deps: return
        self._deps.remove(other)
        other._refs.remove(self)

    def remove_dependencies(self, others : List['Task']):
        for other in others:
            if other not in self._deps: continue
            self._deps.remove(other)
            other._refs.remove(self)

    def clear_dependencies(self):
        for other in self._deps:
            other._refs.remove(self)
        self._deps = set()

    def perform(self) -> Optional[str]:
        return self._work()

class Worker(Thread):
    def __init__(self,
        dequeue : Callable[[Callable[[List[Task]], None]], None],
        index : int
        ):
        super().__init__()
        self._dequeue = dequeue
        self._index = index
        self._running = False

    def _work(self, sequence : List[Task]):
        for task in sequence:
            output = task.perform()
            if output: log.debug('Worker %d: %s' % (self._index, output))

    def run(self):
        self._running = True
        while self._running:
            self._dequeue(self._work)

    def stop(self):
        self._running = False

Batch = List[Task]
Program = List[List[Batch]]

class Evaluator:
    def __init__(self, worker_count : int):
        self._running : bool = False
        self._paused : bool = False
        self._program : Program = []
        self._queue : Queue[List[Task]] = Queue()
        self._exception : Queue[TaskError] = Queue()
        self._pause = SingleWriteMultipleReadLock()
        self._workers = [
            Worker(self._dequeue, index + 1)
            for index in range(worker_count)
        ]

    def _dequeue(self, perform : Callable[[List[Task]], None]):
        self._pause.acquire_read()
        try:
            work = self._queue.get(timeout=1)
            perform(work)
            self._queue.task_done()
        except Empty: sleep(1)
        except TaskError as e:
            self._exception.put(e)
            self._queue.task_done()
        finally:
            self._pause.release_read()

    def _check(self) -> bool:
        try:
            error = self._exception.get(block = False)
            self._exception.task_done()
            self.on_task_error(error)
            return False
        except Empty: return True

    def on_task_error(self, error : TaskError) -> None:
        raise error

    def start(self):
        log.debug('Evaluator.start()')
        if self._running:
            raise RuntimeError('Evaluator can not start if already running!')
        self._running = True

        # Main loop
        for worker in self._workers: worker.start()
        try:
            while self._running:
                if not self._check() and len(self._program) != 0:
                    self.pause()
                    self.deprogram()
                    self.resume()
                if len(self._program) == 0: sleep(1); continue
                batch = self._program.pop(0)
                for sequence in batch: self._queue.put(sequence)
                self._queue.join()
        finally:
            self._running = False
            for worker in self._workers: worker.stop()
            for worker in self._workers: worker.join()

    def pause(self):
        log.debug('Evaluator.pause()')
        if self._paused:
            raise RuntimeError('Evaluator can not pause if already paused!')
        self._pause.acquire_read()
        self._paused = True

    def reprogram(self, program : Program):
        log.debug('Evaluator.reprogram()')
        if self._running and not self._paused:
            raise RuntimeError('Evaluator must be paused prior to reprogramming!')

        # Clear existing program
        while not self._queue.empty():
            try: self._queue.get()
            except Empty: break
            self._queue.task_done()

        # Set the new schedule
        self._program = program

    def deprogram(self):
        log.debug('Evaluator.deprogram()')
        if self._running and not self._paused:
            raise RuntimeError('Evaluator must be paused prior to deprogramming!')

        # Clear existing program
        while not self._queue.empty():
            try: self._queue.get()
            except Empty: break
            self._queue.task_done()

        # Clear schedule
        self._program = list()

    def resume(self):
        log.debug('Evaluator.resume()')
        if not self._paused:
            raise RuntimeError('Evaluator can not resume if not paused!')
        self._paused = False
        self._pause.release_read()

    def stop(self):
        log.debug('Evaluator.stop()')
        if not self._running:
            raise RuntimeError('Evaluator can not stop if already not running!')
        self._running = False
