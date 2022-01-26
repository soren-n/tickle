# External module dependencies
from threading import Thread, Event
from queue import Queue, Empty
from time import sleep

# Internal module dependencies
from .util import SingleWriteMultipleReadLock
from . import log

###############################################################################
# Exceptions
###############################################################################
class TaskError(Exception):
    def __init__(self, description, message):
        super().__init__()
        self.description = description
        self.message = message

###############################################################################
# Classes
###############################################################################
class Task:
    def __init__(self, flows, work, force = False):
        self._valid = False
        self._active = True
        self._force = force
        self._deps = set()
        self._refs = set()
        self._flows = flows
        self._work = work

    def __hash__(self):
        return hash(id(self))

    def get_flows(self):
        return self._flows

    def get_valid(self):
        if self._force: return False
        return self._valid

    def set_valid(self, valid):
        self._valid = valid

    def get_active(self):
        if self._force: return True
        return self._active

    def set_active(self, active):
        self._active = active

    def add_dependency(self, other):
        for flow, stage in self._flows.items():
            if flow not in other._flows: continue
            if stage >= other._flows[flow]: continue
            raise RuntimeError(
                'Tasks can not depend on tasks of '
                'later stages in the same workflow!'
            )
        self._deps.add(other)
        other._refs.add(self)

    def add_dependencies(self, others):
        for other in others: self.add_dependency(other)

    def remove_dependency(self, other):
        if other not in self._deps: return
        self._deps.remove(other)
        other._refs.remove(self)

    def remove_dependencies(self, others):
        for other in others:
            if other not in self._deps: continue
            self._deps.remove(other)
            other._refs.remove(self)

    def clear_dependencies(self):
        for other in self._deps:
            other._refs.remove(self)
        self._deps = set()

    def perform(self):
        self._work()

class Worker(Thread):
    def __init__(self, dequeue, index):
        super().__init__()
        self._dequeue = dequeue
        self._index = index
        self._running = False

    def _work(self, sequence):
        for task in sequence:
            output = task.perform()
            if output: logging.debug('Worker %d: %s' % (self._index, output))

    def run(self):
        self._running = True
        while self._running:
            self._dequeue(self._work)

    def stop(self):
        self._running = False

class Evaluator:
    def __init__(self, worker_count):
        self._running = False
        self._paused = False
        self._program = []
        self._queue = Queue()
        self._exception = Queue()
        self._pause = SingleWriteMultipleReadLock()
        self._workers = [
            Worker(self._dequeue, index + 1)
            for index in range(worker_count)
        ]

    def _dequeue(self, perform):
        self._pause.acquire_read()
        try:
            work = self._queue.get(timeout=1)
            perform(work)
            self._queue.task_done()
        except Empty: sleep(1)
        except Exception as e:
            self._exception.put(e)
            self._queue.task_done()
        finally:
            self._pause.release_read()

    def _check(self):
        try:
            error = self._exception.get(block = False)
            self._exception.task_done()
            self.on_task_error(error)
        except Empty: return

    def on_task_error(self, error):
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
                if len(self._program) == 0: sleep(1); continue
                batch = self._program.pop(0)
                for sequence in batch: self._queue.put(sequence)
                self._queue.join()
                self._check()
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

    def reprogram(self, program):
        log.debug('Evaluator.reprogram()')
        if self._running and not self._paused:
            raise RuntimeError('Evaluator must be paused prior to reprogramming!')

        # Clear existing program
        while not self._queue.empty():
            try: self._queue.get()
            except Empty: break
            self._queue.task_done()

        # Set the new program
        self._program = program

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
