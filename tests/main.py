# External module dependencies
from typing import List, Dict, Set
from minigun.testing import prop, domain, check, conj
import minigun.quantify as q
from pathlib import Path

import tickle.dataspec as td
import tickle.agenda as ta
import tickle.api as tapi

# Internal module dependencies
from agenda import agenda

###############################################################################
# Test helpers
###############################################################################
def _has_cycle(graph : Dict[int, Set[int]]) -> bool:
    worklist : List[int] = list(filter(
        lambda node: len(graph[node]) == 0,
        graph.keys()
    ))
    checked : Set[int] = set()
    for root_node in worklist:
        path : List[int] = [root_node]
        stack : List[List[int]] = [list(graph[root_node])]
        while len(stack) != 0:
            node_deps = stack[-1]
            if len(node_deps) == 0:
                checked.add(path.pop(-1))
                stack.pop(-1)
                continue
            node_dep = node_deps.pop(0)
            if node_dep in checked: continue
            if node_dep in path: return True
            path.append(node_dep)
            stack.append(list(graph[node_dep]))
    return False

###############################################################################
# Agenda strategy tests
###############################################################################
@prop('Agenda strategy tasks have unique outputs', 100)
@domain(agenda())
def _agenda_strategy_unique_outputs(agenda : ta.Agenda):
    outputs : Dict[str, ta.Task] = {}
    for task in agenda.tasks:
        for output in task.outputs:
            if output in outputs: return False
            outputs[output] = task
    return True

@prop('Agenda strategy tasks is member of flow', 100)
@domain(agenda())
def _agenda_strategy_is_member_of_flow(agenda : ta.Agenda):
    for task in agenda.tasks:
        if 0 < len(task.flows): continue
        return False
    return True

@prop('Agenda strategy tasks have no cycles', 100)
@domain(agenda())
def _agenda_strategy_acyclic(agenda : ta.Agenda):

    # Map tasks from output
    file_to_task : Dict[str, int] = {}
    for task_index, task in enumerate(agenda.tasks):
        for output_file in task.outputs:
            file_to_task[output_file] = task_index

    # Build task graph
    graph : Dict[int, Set[int]] = {}
    for task_index, task in enumerate(agenda.tasks):
        graph[task_index] = set()
        for input_file in task.inputs:
            if input_file not in file_to_task: continue
            graph[task_index].add(file_to_task[input_file])

    # Check for cycles
    return not _has_cycle(graph)

###############################################################################
# Agenda tests
###############################################################################
@prop('Agenda encode and decode are inverse', 100)
@domain(agenda())
def _agenda_encode_decode_inverse(agenda : ta.Agenda):
    return td.decode(ta.Agenda, td.encode(ta.Agenda, agenda)) == agenda

@prop('Agenda store and load are inverse', 100)
@domain(q.directory(), agenda())
def _agenda_store_load_inverse(test_path : Path, agenda : ta.Agenda):
    agenda_path = Path(test_path, 'agenda.yaml')
    ta.store(agenda_path, agenda)
    return agenda == ta.load(agenda_path)

@prop('Agenda compiles', 100)
@domain(q.directory(), agenda())
def _agenda_compiles(test_path : Path, agenda : ta.Agenda):
    try:
        _ = ta.compile(test_path, agenda)
        return True
    except:
        return False

@prop('Agenda evaluates in offline mode', 100)
@domain(q.directory(), agenda())
def _agenda_evaluate_offline(test_path : Path, agenda : ta.Agenda):
    _test_path = test_path.resolve()
    agenda_path = Path(_test_path, 'agenda.yaml')
    depend_path = Path(_test_path, 'depend.yaml')
    cache_path = Path(_test_path, 'tickle.cache')
    ta.store(agenda_path, agenda)
    return tapi.offline(
        _test_path,
        agenda_path,
        depend_path,
        cache_path
    )

###############################################################################
# Main entry
###############################################################################
if __name__ == '__main__':
    import sys
    success = check(conj(
        _agenda_strategy_unique_outputs,
        _agenda_strategy_is_member_of_flow,
        _agenda_strategy_acyclic,
        _agenda_encode_decode_inverse,
        _agenda_store_load_inverse,
        _agenda_compiles,
        _agenda_evaluate_offline
    ))
    sys.exit(0 if success else -1)