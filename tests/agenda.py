# External module dependencies
from typing import List, Dict, Set, Generator
from tickle.agenda import Agenda, Task
import minigun.quantify as q
import math

###############################################################################
# Agenda strategy
###############################################################################
Graph = List[List[bool]]
def _directed_acyclic_graph(task_count : int) -> q.Sampler[Graph]:
    def _zero_diagonal(matrix : Graph) -> Graph:
        result : Graph = []
        for row_index, row in enumerate(matrix):
            _row : List[bool] = []
            for column_index, column in enumerate(row):
                if column_index < row_index: _row.append(column)
                else: _row.append(False)
            result.append(_row)
        return result
    return q.map(
        _zero_diagonal,
        q.bounded_list_of(
            0, task_count,
            q.bounded_list_of(
                0, task_count,
                q.boolean()
            )
        )
    )

def _topological_order(
    deps : Dict[int, Set[int]],
    refs : Dict[int, Set[int]]
    ) -> Generator[int, None, None]:
    visited : Set[int] = set()
    worklist : List[int] = list(filter(
        lambda node: len(deps[node]) == 0,
        refs.keys()
    ))
    while len(worklist) != 0:
        node = worklist.pop(0)
        if node in visited: continue
        if len(deps[node].difference(visited)) != 0: continue
        visited.add(node)
        worklist += refs[node]
        yield node

def agenda() -> q.Sampler[Agenda]:
    def _convert(
        graph_data : Graph,
        flows_data : List[List[bool]]
        ) -> Agenda:

        # Decode graph
        deps : Dict[int, Set[int]] = {}
        refs : Dict[int, Set[int]] = {}
        for row_index, row in enumerate(graph_data):
            deps[row_index] = set()
            refs[row_index] = set()
            for column_index, column in enumerate(row):
                if not column: continue
                deps[row_index].add(column_index)
                refs[column_index].add(row_index)

        # Decode task flows
        task_flows : Dict[int, Set[int]] = {}
        for task_index, flow_data in enumerate(flows_data):
            task_flows[task_index] = set()
            for flow_index, flow_datum in enumerate(flow_data):
                if not flow_datum: continue
                task_flows[task_index].add(flow_index)
            if len(task_flows[task_index]) != 0: continue
            task_flows[task_index].add(0)

        # Compute task depths
        task_depth : Dict[int, int] = {}
        for task_index in _topological_order(deps, refs):
            depth = max([-1]+[task_depth[dep] for dep in deps[task_index]]) + 1
            task_depth[task_index] = depth

        def _proc_name(task_index : int) -> str:
            return 'proc_%d_%d_%d' % (
                len(deps[task_index]),
                len(refs[task_index]),
                task_depth[task_index]
            )

        def _file_path(file_index : int) -> str:
            return 'temp/file_%d.txt' % file_index

        # Convert task graph to agenda
        result = Agenda()

        # Add agenda procs
        for task_index in deps.keys():
            proc_name = _proc_name(task_index)
            if proc_name in result.procs: continue
            result.procs[proc_name] = [
                'python3',
                '../../tasks/success.py',
                '-i', '$inputs',
                '-o', '$outputs'
            ]

        # Map agenda flows
        flows : Dict[int, Dict[int, Set[str]]] = dict()
        for task_index in deps.keys():
            proc_name = _proc_name(task_index)
            stage = task_depth[task_index]
            for flow in task_flows[task_index]:
                if flow not in flows: flows[flow] = dict()
                if stage not in flows[flow]: flows[flow][stage] = set()
                flows[flow][stage].add(proc_name)

        # Add agenda flows
        for flow in sorted(flows.keys()):
            result.flows['flow_%d' % flow] = [
                list(flows[flow][stage])
                for stage in sorted(flows[flow].keys())
            ]

        # Add tasks
        for task_index in deps.keys():
            _inputs = [ _file_path(dep) for dep in deps[task_index] ]
            _outputs = [ _file_path(task_index) ]
            result.tasks.append(Task(
                desc = 'Task %d' % task_index,
                proc = _proc_name(task_index),
                flows = [
                    'flow_%d' % flow
                    for flow in task_flows[task_index]
                ],
                args = {
                    'inputs': _inputs,
                    'outputs': _outputs
                },
                inputs = _inputs,
                outputs = _outputs
            ))

        # Done
        return result

    def _impl(task_count : int) -> q.Sampler[Agenda]:
        flow_count = int(math.log2(max(1, task_count)))
        return q.map(
            _convert,
            _directed_acyclic_graph(task_count),
            q.bounded_list_of(
                0, task_count,
                q.bounded_list_of(
                    0, flow_count,
                    q.boolean()
                )
            )
        )

    return q.bind(_impl, q.small_natural())