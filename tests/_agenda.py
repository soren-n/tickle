# External module dependencies
from collections.abc import Iterator
from pathlib import Path
import math
import sys

from minigun import generate as g

from tickle.agenda import Agenda, Task

###############################################################################
# Constants
###############################################################################
# Tasks run the success script with the interpreter running the tests, by
# absolute path, so the agenda works from any fixture directory.
SUCCESS_SCRIPT = Path(__file__).parent.joinpath('tasks', 'success.py').resolve()
FAILURE_SCRIPT = Path(__file__).parent.joinpath('tasks', 'failure.py').resolve()

###############################################################################
# Agenda generator
###############################################################################
Graph = list[list[bool]]


def _directed_acyclic_graph(task_count: int) -> g.Generator[Graph]:
    # Row i lists the tasks that task i depends on; keeping only entries
    # below the diagonal makes every edge point to an earlier task.
    def _lower_triangle(matrix: Graph) -> Graph:
        return [
            [column if column_index < row_index else False
             for column_index, column in enumerate(row)]
            for row_index, row in enumerate(matrix)
        ]
    return g.map(
        _lower_triangle,
        g.bounded_lists(
            0, task_count,
            g.bounded_lists(0, task_count, g.bools())
        )
    )


def _topological_order(
    deps: dict[int, set[int]],
    refs: dict[int, set[int]]
) -> Iterator[int]:
    visited: set[int] = set()
    worklist: list[int] = [node for node in refs if len(deps[node]) == 0]
    while len(worklist) != 0:
        node = worklist.pop(0)
        if node in visited: continue
        if len(deps[node].difference(visited)) != 0: continue
        visited.add(node)
        worklist += refs[node]
        yield node


def _convert(graph_data: Graph, flows_data: list[list[bool]]) -> Agenda:

    # Decode graph
    deps: dict[int, set[int]] = {}
    refs: dict[int, set[int]] = {}
    for row_index, row in enumerate(graph_data):
        deps[row_index] = set()
        refs[row_index] = set()
        for column_index, column in enumerate(row):
            if not column: continue
            deps[row_index].add(column_index)
            refs[column_index].add(row_index)

    # Decode task flows; a task without flows joins flow 0
    task_flows: dict[int, set[int]] = {}
    for task_index in deps:
        flow_data = flows_data[task_index] if task_index < len(flows_data) else []
        task_flows[task_index] = {
            flow_index
            for flow_index, flow_datum in enumerate(flow_data)
            if flow_datum
        } or {0}

    # Compute task depths
    task_depth: dict[int, int] = {}
    for task_index in _topological_order(deps, refs):
        depth = max([-1] + [task_depth[dep] for dep in deps[task_index]]) + 1
        task_depth[task_index] = depth

    def _proc_name(task_index: int) -> str:
        return 'proc_%d_%d_%d' % (
            len(deps[task_index]),
            len(refs[task_index]),
            task_depth[task_index]
        )

    def _file_path(file_index: int) -> str:
        return 'temp/file_%d.txt' % file_index

    # Convert task graph to agenda
    result = Agenda()

    # Add agenda procs
    for task_index in deps:
        proc_name = _proc_name(task_index)
        if proc_name in result.procs: continue
        result.procs[proc_name] = [
            sys.executable,
            str(SUCCESS_SCRIPT),
            '-i', '$inputs',
            '-o', '$outputs'
        ]

    # Map agenda flows
    flows: dict[int, dict[int, set[str]]] = {}
    for task_index in deps:
        proc_name = _proc_name(task_index)
        stage = task_depth[task_index]
        for flow in task_flows[task_index]:
            flows.setdefault(flow, {}).setdefault(stage, set()).add(proc_name)

    # Add agenda flows
    for flow in sorted(flows):
        result.flows['flow_%d' % flow] = [
            sorted(flows[flow][stage])
            for stage in sorted(flows[flow])
        ]

    # Add tasks
    for task_index in deps:
        _inputs = [_file_path(dep) for dep in sorted(deps[task_index])]
        _outputs = [_file_path(task_index)]
        result.tasks.append(Task(
            desc = 'Task %d' % task_index,
            proc = _proc_name(task_index),
            flows = ['flow_%d' % flow for flow in sorted(task_flows[task_index])],
            args = {
                'inputs': _inputs,
                'outputs': _outputs
            },
            inputs = _inputs,
            outputs = _outputs
        ))

    # Done
    return result


def agendas(task_counts: g.Generator[int]) -> g.Generator[Agenda]:
    def _sized(task_count: int) -> g.Generator[Agenda]:
        flow_count = int(math.log2(max(1, task_count)))
        return g.map(
            _convert,
            _directed_acyclic_graph(task_count),
            g.bounded_lists(
                0, task_count,
                g.bounded_lists(0, flow_count, g.bools())
            )
        )
    return g.bind(_sized, task_counts)
