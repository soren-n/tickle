# External module dependencies
from typing import Callable, Optional, Tuple, List, Dict, Set
from threading import Event, Thread
from queue import Queue, Empty

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
        force : bool = False,
        description : str = ''
        ):
        self.description : str = description
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
        # Set by stop(), which may be called before run() is scheduled; a
        # flag that run() initialised itself could then overwrite the stop
        # and leave the worker looping forever.
        self._stopped = Event()

    def _work(self, sequence : List[Task]):
        for task in sequence:
            try: output = task.perform()
            except TaskError: raise
            except Exception as error:
                # Report the failure against the task that raised it
                raise TaskError(
                    task.description or 'unknown',
                    '%s: %s' % (type(error).__name__, error)
                ) from error
            if output: log.debug('Worker %d: %s' % (self._index, output))

    def run(self):
        while not self._stopped.is_set():
            self._dequeue(self._work)

    def stop(self):
        self._stopped.set()

Batch = List[Task]
Program = List[List[Batch]]

# How long an idle worker waits for work before checking whether it was
# stopped, and how long the idle main loop waits before checking again;
# reprogramming and stopping wake the main loop at once.
_POLL_INTERVAL = 0.05
_IDLE_INTERVAL = 1.0

class Evaluator:
    def __init__(self, worker_count : int):
        self._running : bool = False
        self._paused : bool = False
        self._program : Program = []
        # Each program gets a new generation; an error is tagged with the
        # generation of the program its task belonged to, so a failure in
        # an old program never cancels the program that replaced it.
        self._generation : int = 0
        self._queue : Queue[Tuple[int, List[Task]]] = Queue()
        self._exception : Queue[Tuple[int, TaskError]] = Queue()
        self._pause = SingleWriteMultipleReadLock()
        self._wake = Event()
        self._workers = [
            Worker(self._dequeue, index + 1)
            for index in range(max(1, worker_count))
        ]

    def _dequeue(self, perform : Callable[[List[Task]], None]):
        # Workers hold the read side of the pause lock while they take and
        # perform a sequence, so pause() waits for running sequences.
        self._pause.acquire_read()
        try:
            try: generation, work = self._queue.get(timeout = _POLL_INTERVAL)
            except Empty: return
            try: perform(work)
            except TaskError as error:
                self._exception.put((generation, error))
            except Exception as error:
                # Any other failure must still be reported and accounted
                # for, or the main loop would wait for the batch forever.
                self._exception.put((generation, TaskError(
                    'unknown', '%s: %s' % (type(error).__name__, error)
                )))
            finally:
                self._queue.task_done()
                self._wake.set()
        finally:
            self._pause.release_read()

    def _check(self) -> Set[int]:
        # Report every error that arrived, and return the generations of the
        # programs they came from; those programs must be dropped.
        failed : Set[int] = set()
        while True:
            try: generation, error = self._exception.get(block = False)
            except Empty: return failed
            self._exception.task_done()
            failed.add(generation)
            self.on_task_error(error)

    def on_task_error(self, error : TaskError) -> None:
        raise error

    def on_idle(self) -> None:
        """Called by the main loop when it has nothing to queue."""
        pass

    def _next_batch(self) -> Optional[List[Batch]]:
        # Reprogramming happens under the write side of the pause lock, so
        # taking the batch under the read side never queues a stale one.
        self._pause.acquire_read()
        try:
            if len(self._program) == 0: return None
            batch = self._program.pop(0)
            for sequence in batch:
                self._queue.put((self._generation, sequence))
            return batch
        finally:
            self._pause.release_read()

    def start(self):
        log.debug('Evaluator.start()')
        if self._running:
            raise RuntimeError('Evaluator can not start if already running!')
        self._running = True

        # Main loop
        for worker in self._workers: worker.start()
        try:
            while self._running:
                self._wake.clear()
                failed = self._check()
                if self._generation in failed and len(self._program) != 0:
                    # Check again once paused: a new program may have been
                    # installed while waiting, and must not be dropped
                    self.pause()
                    try:
                        if self._generation in failed: self.deprogram()
                    finally: self.resume()
                if self._next_batch() is None:
                    self.on_idle()
                    self._wake.wait(_IDLE_INTERVAL)
                    continue
                self._queue.join()
        finally:
            self._running = False
            for worker in self._workers: worker.stop()
            for worker in self._workers: worker.join()

    def pause(self):
        log.debug('Evaluator.pause()')
        self._pause.acquire_write()
        self._paused = True

    def _clear_queue(self):
        while True:
            try: self._queue.get(block = False)
            except Empty: break
            self._queue.task_done()

    def reprogram(self, program : Program):
        log.debug('Evaluator.reprogram()')
        if self._running and not self._paused:
            raise RuntimeError('Evaluator must be paused prior to reprogramming!')
        self._clear_queue()
        self._generation += 1
        self._program = program
        self._wake.set()

    def deprogram(self):
        log.debug('Evaluator.deprogram()')
        if self._running and not self._paused:
            raise RuntimeError('Evaluator must be paused prior to deprogramming!')
        self._clear_queue()
        self._program = list()

    def resume(self):
        log.debug('Evaluator.resume()')
        if not self._paused:
            raise RuntimeError('Evaluator can not resume if not paused!')
        self._paused = False
        self._pause.release_write()

    def stop(self):
        # Safe to call more than once, and from any thread, including a
        # worker performing the task that ends the evaluation.
        log.debug('Evaluator.stop()')
        self._running = False
        self._wake.set()
