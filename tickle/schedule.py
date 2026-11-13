# Annotations name Task, which imports this module
from __future__ import annotations

# External module dependencies
from typing import TYPE_CHECKING, cast
import heapq

# Internal module dependencies
from . import graph

if TYPE_CHECKING:
    from .evaluate import Task

###############################################################################
# Schedule
###############################################################################
# States of a node in a schedule
WAITING = 'waiting'   # dependencies or earlier stages have not finished
READY = 'ready'       # may start once its pool has room
RUNNING = 'running'
DONE = 'done'         # succeeded
FAILED = 'failed'     # failed, and has no retries left
BLOCKED = 'blocked'   # never started, because a task it reads from failed

class _Barrier:
    """Orders the stages of a flow: depends on every task of a stage and is
    depended on by every task of the next one, so the graph stays linear in
    size. It completes as soon as everything before it has finished."""
    __slots__ = ('index',)

    def __init__(self, index : int):
        self.index = index

Node = object

class Schedule:
    """Which tasks of a graph run, in what order, and how far they are; a
    plain state machine without threads, so it can be tested on its own.

    The tasks to run are those out of date and able to run, limited to the
    wanted ones if given. A task becomes ready once the tasks it reads from
    have succeeded and the tasks of the earlier stages of its flows have
    finished. Ready tasks start by priority: the longest chain of estimated
    durations from the task to the end of the graph, so long chains start
    first. A pool limits how many of its tasks run at once, and the worker
    count limits all of them.

    When a task fails it is retried while it has retries left. Without keep
    going, nothing more starts after a failure; with it, only the tasks that
    read from the failed task, directly or not, are blocked, and stages
    count a failed or blocked task as finished."""

    def __init__(self,
        tasks : list[Task],
        worker_count : int,
        pools : dict[str, int] | None = None,
        keep_going : bool = False,
        wanted : set[Task] | None = None
        ):
        self._worker_count = max(1, worker_count)
        self._pools : dict[str, int] = dict(pools or {})
        self._keep_going = keep_going
        self.stopped = False

        # Tasks to run, in graph order
        self.tasks : list[Task] = [
            task for task in tasks
            if not task.get_valid() and task.get_active()
            and (wanted is None or task in wanted)
        ]
        members = set(self.tasks)
        self._order : dict[Node, int] = {
            task : index for index, task in enumerate(self.tasks)
        }

        # Data dependencies between tasks to run, and ordering edges through
        # one barrier per pair of consecutive stages in a flow
        self._data_refs : dict[Node, set[Node]] = {}
        self._order_refs : dict[Node, set[Node]] = {}
        self._pending : dict[Node, int] = {}
        for task in self.tasks:
            self._data_refs[task] = {
                ref for ref in task.get_refs() if ref in members
            }
            self._order_refs[task] = set()
            self._pending[task] = len([
                dep for dep in task.get_deps() if dep in members
            ])
        stages_by_flow : dict[str, dict[int, list[Task]]] = {}
        for task in self.tasks:
            for flow, stage in task.get_flows().items():
                stages_by_flow.setdefault(flow, {}).setdefault(stage, []).append(task)
        self._barriers : list[_Barrier] = []
        for flow in sorted(stages_by_flow):
            stages = stages_by_flow[flow]
            previous : list[Task] | None = None
            for stage in sorted(stages):
                current = stages[stage]
                if previous is not None:
                    barrier = _Barrier(len(self._barriers))
                    self._barriers.append(barrier)
                    self._data_refs[barrier] = set()
                    self._order_refs[barrier] = set(current)
                    self._pending[barrier] = len(previous)
                    for task in previous: self._order_refs[task].add(barrier)
                    for task in current: self._pending[task] += 1
                previous = current

        # Refuse an order no task of which could start
        self._check_order()

        # Priorities: the longest chain of durations to the end of the graph
        self._priority : dict[Node, float] = {}
        for node in reversed(self._topological()):
            weight = 0.0 if isinstance(node, _Barrier) else cast('Task', node).get_weight()
            after = [
                self._priority[ref]
                for ref in self._data_refs[node] | self._order_refs[node]
            ]
            self._priority[node] = weight + max(after, default = 0.0)

        # Initial states
        self.state : dict[Node, str] = {}
        self._attempts : dict[Task, int] = { task : 0 for task in self.tasks }
        self._ready : list[tuple[float, int, int]] = []
        self._ready_nodes : dict[int, Task] = {}
        self._running_by_pool : dict[str, int] = {}
        self._running_count = 0
        for node in list(self._pending):
            self.state[node] = WAITING
        for node in list(self._pending):
            if self._pending[node] == 0 and self.state[node] == WAITING:
                self._release(node)

    ###########################################################################
    # Construction helpers
    ###########################################################################
    def _topological(self) -> list[Node]:
        waiting = dict(self._pending)
        result : list[Node] = [
            node for node, count in waiting.items() if count == 0
        ]
        index = 0
        while index < len(result):
            node = result[index]
            index += 1
            for ref in self._data_refs[node] | self._order_refs[node]:
                waiting[ref] -= 1
                if waiting[ref] == 0: result.append(ref)
        return result

    def _check_order(self):
        if len(self._topological()) == len(self._pending): return
        # A cycle through dependencies and stage order; name its tasks
        # the same way graph does
        graph.check_stages(self.tasks)
        raise RuntimeError('Cycle detected in agenda!')

    ###########################################################################
    # State changes
    ###########################################################################
    def _release(self, node : Node):
        # Every dependency and earlier stage has finished
        if isinstance(node, _Barrier):
            self.state[node] = DONE
            self._finished(node, True)
            return
        self.state[node] = READY
        key = (-self._priority[node], self._order[node], id(node))
        heapq.heappush(self._ready, key)
        self._ready_nodes[id(node)] = cast('Task', node)

    def _finished(self, node : Node, succeeded : bool):
        # Let the nodes after a finished node go: a failed or blocked task
        # blocks the tasks reading from it, and lets stages go on
        for ref in self._data_refs[node]:
            if succeeded: self._decrement(ref)
            else: self._block(ref)
        for ref in self._order_refs[node]:
            self._decrement(ref)

    def _decrement(self, node : Node):
        self._pending[node] -= 1
        if self._pending[node] == 0 and self.state[node] == WAITING:
            self._release(node)

    def _block(self, node : Node):
        if self.state[node] in (BLOCKED, DONE, FAILED, RUNNING): return
        if self.state[node] == READY:
            self._ready_nodes.pop(id(node), None)
        self.state[node] = BLOCKED
        self._finished(node, False)

    def _pool(self, task : Task) -> str:
        return task.get_pool()

    def _has_room(self, task : Task) -> bool:
        if self._running_count >= self._worker_count: return False
        pool = self._pool(task)
        if pool == '': return True
        return self._running_by_pool.get(pool, 0) < self._pools.get(pool, 1)

    ###########################################################################
    # Interface
    ###########################################################################
    def next(self) -> Task | None:
        """The ready task to start next, marked running, or None if no
        ready task may start now."""
        if self.stopped: return None
        if self._running_count >= self._worker_count: return None
        skipped : list[tuple[float, int, int]] = []
        result : Task | None = None
        while len(self._ready) != 0:
            key = heapq.heappop(self._ready)
            task = self._ready_nodes.get(key[2])
            if task is None or self.state[task] != READY: continue
            if not self._has_room(task):
                skipped.append(key)
                continue
            result = task
            break
        for key in skipped: heapq.heappush(self._ready, key)
        if result is None: return None
        del self._ready_nodes[id(result)]
        self._start(result)
        return result

    def _start(self, task : Task):
        self.state[task] = RUNNING
        self._attempts[task] += 1
        pool = self._pool(task)
        self._running_by_pool[pool] = self._running_by_pool.get(pool, 0) + 1
        self._running_count += 1

    def adopt(self, task : Task):
        """Mark a task as running that started under an earlier schedule
        and still counts in this one."""
        if self.state.get(task) != READY:
            raise RuntimeError('Only a ready task can be adopted')
        del self._ready_nodes[id(task)]
        self._start(task)

    def can_adopt(self, task : Task) -> bool:
        return self.state.get(task) == READY

    def finish(self, task : Task, succeeded : bool) -> str:
        """Record that a running task ended; returns its new state, READY
        when it failed and is retried."""
        if self.state.get(task) != RUNNING:
            raise RuntimeError('Only a running task can finish')
        pool = self._pool(task)
        self._running_by_pool[pool] -= 1
        self._running_count -= 1
        if succeeded:
            self.state[task] = DONE
            self._finished(task, True)
            return DONE
        if self._attempts[task] <= task.get_retries() and not self.stopped:
            self._release(task)
            return READY
        self.state[task] = FAILED
        if self._keep_going: self._finished(task, False)
        else: self.stopped = True
        return FAILED

    def cancel(self, task : Task):
        """Forget a running task that was cancelled; it counts as not run,
        and nothing after it starts."""
        if self.state.get(task) != RUNNING: return
        pool = self._pool(task)
        self._running_by_pool[pool] -= 1
        self._running_count -= 1
        self.state[task] = WAITING
        self.stopped = True

    def attempts(self, task : Task) -> int:
        return self._attempts.get(task, 0)

    def running(self) -> list[Task]:
        return [ task for task in self.tasks if self.state[task] == RUNNING ]

    def finished(self) -> bool:
        """Whether nothing runs and nothing more will start."""
        if self._running_count != 0: return False
        if self.stopped: return True
        return not any(
            self.state[task] in (WAITING, READY) for task in self.tasks
        )

    def with_state(self, state : str) -> list[Task]:
        return [ task for task in self.tasks if self.state[task] == state ]

    def order(self) -> list[Task]:
        """The tasks in an order a single worker would run them, ignoring
        failures; for a dry run."""
        result : list[Task] = []
        trial = Schedule.__new__(Schedule)
        trial.__dict__.update(self.__dict__)
        trial.state = dict(self.state)
        trial._pending = dict(self._pending)
        trial._ready = list(self._ready)
        trial._ready_nodes = dict(self._ready_nodes)
        trial._attempts = dict(self._attempts)
        trial._running_by_pool = dict(self._running_by_pool)
        trial._worker_count = 1
        trial._running_count = 0
        trial._pools = {}
        trial.stopped = False
        while True:
            task = trial.next()
            if task is None: break
            result.append(task)
            trial.finish(task, True)
        return result
