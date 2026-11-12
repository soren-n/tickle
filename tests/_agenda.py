# External module dependencies
from collections.abc import Iterator
from pathlib import Path
import math
import sys

from minigun import generate as g

from tickle.agenda import Agenda, Task
from tickle.depend import Depend

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


def _convert(
    graph_data: Graph,
    flows_data: list[list[bool]],
    stage_bumps: list[int]
) -> Agenda:

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

    # Compute task stages: after every dependency's stage, sometimes one
    # further, so independent tasks also end up in different stages and
    # stage order is not implied by dependency order alone
    task_depth: dict[int, int] = {}
    for task_index in _topological_order(deps, refs):
        depth = max([-1] + [task_depth[dep] for dep in deps[task_index]]) + 1
        if task_index < len(stage_bumps): depth += stage_bumps[task_index]
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
            ),
            g.bounded_lists(0, task_count, g.int_range(0, 1))
        )
    return g.bind(_sized, task_counts)


def cyclic_agendas() -> g.Generator[Agenda]:
    """Agendas whose tasks form a ring: each task reads the previous task's
    output, and the first reads the last's (a single task reads its own)."""
    def _ring(task_count: int) -> Agenda:
        def _file_path(index: int) -> str:
            return 'temp/file_%d.txt' % (index % task_count)
        result = Agenda()
        result.procs['proc'] = [
            sys.executable,
            str(SUCCESS_SCRIPT),
            '-i', '$inputs',
            '-o', '$outputs'
        ]
        result.flows['flow'] = [['proc']]
        for index in range(task_count):
            _inputs = [_file_path(index - 1)]
            _outputs = [_file_path(index)]
            result.tasks.append(Task(
                desc = 'Task %d' % index,
                proc = 'proc',
                flows = ['flow'],
                args = {'inputs': _inputs, 'outputs': _outputs},
                inputs = _inputs,
                outputs = _outputs
            ))
        return result
    return g.map(_ring, g.int_range(1, 6))


###############################################################################
# Arbitrary text
###############################################################################
# Any printable text, for checking that files keep exactly what was stored
texts = g.strings()
text_lists = g.lists(texts)


def text_agendas() -> g.Generator[Agenda]:
    """Agendas whose every name, path and value is arbitrary text; they are
    well typed but not meant to compile."""
    tasks = g.map(
        lambda desc, proc, flows, args, inputs, outputs: Task(
            desc = desc, proc = proc, flows = flows, args = args,
            inputs = inputs, outputs = outputs
        ),
        texts, texts, text_lists, g.dicts(texts, text_lists),
        text_lists, text_lists
    )
    return g.map(
        lambda procs, flows, tasks: Agenda(
            procs = procs, flows = flows, tasks = tasks
        ),
        g.dicts(texts, text_lists),
        g.dicts(texts, g.lists(text_lists)),
        g.lists(tasks)
    )


def text_depends() -> g.Generator[Depend]:
    return g.dicts(texts, text_lists)

###############################################################################
# Command templates
###############################################################################
# A proc word is a literal or one of a few parameters
Word = tuple[str, str]
PARAMETERS = ['p0', 'p1', 'p2']


def command_templates() -> g.Generator[tuple[list[Word], dict[str, list[str]]]]:
    """A proc as words tagged literal or parameter, with arguments for every
    parameter; literal words never start with $, which marks parameters."""
    literal = g.map(lambda text: ('literal', text.lstrip('$')), texts)
    parameter = g.map(lambda name: ('parameter', name), g.one_of(PARAMETERS))
    arguments = g.map_dict({name: text_lists for name in PARAMETERS})
    return g.tuples(g.lists(g.choice(literal, parameter)), arguments)

###############################################################################
# Depend files
###############################################################################
def cyclic_depends() -> g.Generator[Depend]:
    """Depend files whose files form a ring of one to four files."""
    def _ring(file_count: int) -> Depend:
        def _file_path(index: int) -> str:
            return 'deps/file_%d.txt' % (index % file_count)
        return {
            _file_path(index): [_file_path(index + 1)]
            for index in range(file_count)
        }
    return g.map(_ring, g.int_range(1, 4))
