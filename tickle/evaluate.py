# External module dependencies
from typing import Any
from collections.abc import Callable
from threading import Condition, RLock, Thread

# Internal module dependencies
from .schedule import Schedule, READY
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
# Runs
###############################################################################
class Run:
    """One attempt at performing a task. Work registers how to stop what it
    started, such as killing a process, and checks whether it was
    cancelled; info carries what the work wants to keep about the run."""

    def __init__(self, task : 'Task'):
        self.task = task
        self.info : dict[str, Any] = {}
        self._condition = Condition()
        self._cancelled = False
        self._on_cancel : list[Callable[[], None]] = []

    def cancelled(self) -> bool:
        with self._condition: return self._cancelled

    def on_cancel(self, callback : Callable[[], None]):
        """Call callback when the run is cancelled, at once if it was."""
        with self._condition:
            if not self._cancelled:
                self._on_cancel.append(callback)
                return
        callback()

    def cancel(self):
        with self._condition:
            if self._cancelled: return
            self._cancelled = True
            callbacks = list(self._on_cancel)
            self._on_cancel = []
        for callback in callbacks:
            try: callback()
            except Exception as error:
                log.error('Failed stopping "%s": %s: %s' % (
                    self.task.description, type(error).__name__, error
                ))

###############################################################################
# Tasks
###############################################################################
Work = Callable[[Run], str | None]

class Task:
    def __init__(self,
        flows : dict[str, int],
        work : Work,
        force : bool = False,
        description : str = '',
        key : str = '',
        pool : str = '',
        retries : int = 0,
        weight : float = 1.0
        ):
        self.description : str = description
        # Identifies the task across graphs, so a run started under one
        # schedule can count in the next
        self.key : str = key
        self._valid : bool = False
        self._active : bool = True
        self._force : bool = force
        self._deps : set['Task'] = set()
        self._refs : set['Task'] = set()
        self._flows : dict[str, int] = flows
        self._work : Work = work
        self._pool = pool
        self._retries = retries
        self._weight = weight
        # Why the task is to run, for a dry run
        self.reason : str = ''
        # Whether its work found it up to date after all and did nothing
        self.cut_off : bool = False

    def __hash__(self):
        return hash(id(self))

    def get_deps(self) -> set['Task']:
        return self._deps

    def get_refs(self) -> set['Task']:
        return self._refs

    def get_flows(self):
        return self._flows

    def get_pool(self) -> str:
        return self._pool

    def get_retries(self) -> int:
        return self._retries

    def get_weight(self) -> float:
        return self._weight

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

    def add_dependencies(self, others : list['Task']):
        for other in others: self.add_dependency(other)

    def perform(self, run : Run) -> str | None:
        return self._work(run)

###############################################################################
# Evaluator
###############################################################################
# How long the main loop and idle workers wait before checking again; every
# change of state wakes them at once, so this only bounds how late Ctrl-C is
# noticed where waiting cannot be interrupted (Windows)
_IDLE_INTERVAL = 0.25

Keep = Callable[[Run, Task], bool]

class Evaluator:
    """Runs the tasks of a schedule on worker threads. Workers take the next
    ready task as soon as one is free, so a task starts the moment what it
    waits for has finished. install() replaces the schedule at any time,
    without waiting for running tasks: a running task that the new schedule
    would start now, and that keep() accepts, carries on and counts in it;
    any other running task is cancelled."""

    def __init__(self,
        worker_count : int,
        pools : dict[str, int] | None = None,
        keep_going : bool = False
        ):
        self._worker_count = max(1, worker_count)
        self._pools : dict[str, int] = dict(pools or {})
        self._keep_going = keep_going
        self._condition = Condition(RLock())
        self._schedule : Schedule | None = None
        self._finished : Schedule | None = None
        self._running : dict[Run, tuple[Schedule | None, Task]] = {}
        self._started = False
        self._stopping = False
        self._workers = [
            Thread(target = self._work, name = 'tickle-worker-%d' % (index + 1))
            for index in range(self._worker_count)
        ]

    ###########################################################################
    # Configuration
    ###########################################################################
    def set_pools(self, pools : dict[str, int]):
        with self._condition: self._pools = dict(pools)

    def make_schedule(self,
        tasks : list[Task],
        wanted : set[Task] | None = None
        ) -> Schedule:
        with self._condition: pools = dict(self._pools)
        return Schedule(
            tasks, self._worker_count, pools, self._keep_going, wanted
        )

    def install(self, schedule : Schedule, keep : Keep | None = None):
        """Make schedule the current one; see the class description."""
        with self._condition:
            by_key = { task.key : task for task in schedule.tasks if task.key }
            for run, (owner, task) in list(self._running.items()):
                if owner is None: continue
                successor = by_key.get(task.key) if task.key else None
                if (successor is not None
                    and schedule.can_adopt(successor)
                    and (keep is None or keep(run, successor))):
                    schedule.adopt(successor)
                    self._running[run] = (schedule, successor)
                    continue
                self._running[run] = (None, task)
                run.cancel()
            self._schedule = schedule
            self._condition.notify_all()

    def schedule(self) -> Schedule | None:
        with self._condition: return self._schedule

    def runs(self) -> list[Run]:
        """The runs of the current tasks that are running now."""
        with self._condition:
            return [
                run for run, (owner, _) in self._running.items()
                if owner is not None
            ]

    ###########################################################################
    # Callbacks
    ###########################################################################
    def on_task_failed(self, error : TaskError, retrying : bool) -> None:
        """Called on the worker thread as soon as a task fails."""
        pass

    def on_finished(self, schedule : Schedule) -> None:
        """Called by the main loop once a schedule has nothing running and
        nothing more to start."""
        pass

    ###########################################################################
    # Workers
    ###########################################################################
    def _next(self) -> tuple[Run, Schedule, Task] | None:
        with self._condition:
            while not self._stopping:
                schedule = self._schedule
                task = schedule.next() if schedule is not None else None
                if schedule is not None and task is not None:
                    run = Run(task)
                    self._running[run] = (schedule, task)
                    return run, schedule, task
                self._condition.wait(_IDLE_INTERVAL)
            return None

    def _work(self):
        while True:
            taken = self._next()
            if taken is None: return
            run, _, task = taken
            error : TaskError | None = None
            try:
                output = task.perform(run)
                if output: log.debug('%s: %s' % (task.description, output))
            except TaskError as task_error:
                error = task_error
            except Exception as other_error:
                # Any failure must end up recorded, or the schedule would
                # wait for this task forever
                error = TaskError(
                    task.description or 'unknown',
                    '%s: %s' % (type(other_error).__name__, other_error)
                )
            state = None
            with self._condition:
                owner, owned = self._running.pop(run)
                if error is None and run.info.get('cutoff'): owned.cut_off = True
                if owner is not None and run.cancelled():
                    owner.cancel(owned)
                elif owner is not None:
                    state = owner.finish(owned, error is None)
                self._condition.notify_all()
            if error is not None and state is not None:
                try: self.on_task_failed(error, state == READY)
                except Exception as report_error:
                    log.error('Failed reporting a task error: %s: %s' % (
                        type(report_error).__name__, report_error
                    ))

    ###########################################################################
    # Main loop
    ###########################################################################
    def start(self):
        log.debug('Evaluator.start()')
        with self._condition:
            if self._started:
                raise RuntimeError('Evaluator can not start twice!')
            self._started = True
        for worker in self._workers: worker.start()
        try:
            while True:
                finished : Schedule | None = None
                with self._condition:
                    if self._stopping: break
                    schedule = self._schedule
                    if (schedule is not None
                        and schedule is not self._finished
                        and schedule.finished()):
                        self._finished = schedule
                        finished = schedule
                    else:
                        self._condition.wait(_IDLE_INTERVAL)
                if finished is not None: self.on_finished(finished)
        except BaseException:
            # Interrupted: stop what runs rather than wait for it
            self.cancel()
            raise
        finally:
            with self._condition:
                self._stopping = True
                self._condition.notify_all()
            for worker in self._workers: worker.join()

    def cancel(self):
        """Cancel every running task, and start nothing more."""
        with self._condition:
            if self._schedule is not None: self._schedule.stopped = True
            runs = list(self._running.keys())
            self._condition.notify_all()
        for run in runs: run.cancel()

    def stop(self, cancel : bool = False):
        """End the evaluation, from any thread, more than once, also before
        start(); nothing more starts, and running tasks finish first unless
        cancel is set."""
        log.debug('Evaluator.stop()')
        if cancel: self.cancel()
        with self._condition:
            self._stopping = True
            self._condition.notify_all()
