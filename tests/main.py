# External module dependencies
from collections.abc import Iterator
from contextlib import contextmanager, redirect_stdout
from pathlib import Path
import io
import logging
import random

from minigun import assume, conj, context, prop
from minigun import generate as g
from minigun.fixture import temporary_path

import tickle.dataspec as td
import tickle.agenda as ta
import tickle.api as tapi

# Internal module dependencies
from ._agenda import FAILURE_SCRIPT, agendas

###############################################################################
# Generators
###############################################################################
# Agendas with up to 100 tasks for the properties over agenda data, and
# smaller ones for the properties that evaluate them, since every task runs
# a Python process.
any_agendas = agendas(g.small_nats())
evaluated_agendas = agendas(g.int_range(0, 12))

###############################################################################
# Test helpers
###############################################################################
def _has_cycle(graph: dict[int, set[int]]) -> bool:
    worklist: list[int] = [node for node in graph if len(graph[node]) == 0]
    checked: set[int] = set()
    for root_node in worklist:
        path: list[int] = [root_node]
        stack: list[list[int]] = [list(graph[root_node])]
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


def _fixture_path() -> Path:
    return temporary_path().resolve()


@contextmanager
def _quiet() -> Iterator[None]:
    # Tickle reports progress on stdout and through the root logger; the
    # properties check outcomes, so keep the test report readable.
    logging.disable(logging.CRITICAL)
    try:
        with redirect_stdout(io.StringIO()):
            yield
    finally:
        logging.disable(logging.NOTSET)


def _offline(test_path: Path, agenda: ta.Agenda) -> bool:
    agenda_path = test_path / 'agenda.yaml'
    ta.store(agenda_path, agenda)
    with _quiet():
        return tapi.offline(
            test_path,
            agenda_path,
            test_path / 'depend.yaml',
            test_path / 'tickle.cache'
        )


def _outputs(test_path: Path, agenda: ta.Agenda) -> list[Path]:
    return [
        test_path / output
        for task in agenda.tasks
        for output in task.outputs
    ]

###############################################################################
# Agenda generator tests
###############################################################################
@context(any_agendas)
@prop('Agenda generator tasks have unique outputs')
def _agenda_generator_unique_outputs(agenda: ta.Agenda) -> bool:
    outputs: set[str] = set()
    for task in agenda.tasks:
        for output in task.outputs:
            if output in outputs: return False
            outputs.add(output)
    return True


@context(any_agendas)
@prop('Agenda generator tasks are members of a flow')
def _agenda_generator_is_member_of_flow(agenda: ta.Agenda) -> bool:
    return all(0 < len(task.flows) for task in agenda.tasks)


@context(any_agendas)
@prop('Agenda generator tasks have no cycles')
def _agenda_generator_acyclic(agenda: ta.Agenda) -> bool:

    # Map tasks from output
    file_to_task: dict[str, int] = {}
    for task_index, task in enumerate(agenda.tasks):
        for output_file in task.outputs:
            file_to_task[output_file] = task_index

    # Build task graph
    graph: dict[int, set[int]] = {}
    for task_index, task in enumerate(agenda.tasks):
        graph[task_index] = {
            file_to_task[input_file]
            for input_file in task.inputs
            if input_file in file_to_task
        }

    # Check for cycles
    return not _has_cycle(graph)

###############################################################################
# Agenda tests
###############################################################################
@context(any_agendas)
@prop('Agenda encode and decode are inverse')
def _agenda_encode_decode_inverse(agenda: ta.Agenda) -> bool:
    return td.decode(ta.Agenda, td.encode(ta.Agenda, agenda)) == agenda


@context(any_agendas)
@prop('Agenda store and load are inverse')
def _agenda_store_load_inverse(agenda: ta.Agenda) -> bool:
    agenda_path = _fixture_path() / 'agenda.yaml'
    ta.store(agenda_path, agenda)
    return agenda == ta.load(agenda_path)


@context(any_agendas)
@prop('Agenda compiles')
def _agenda_compiles(agenda: ta.Agenda) -> bool:
    compiled = ta.compile(_fixture_path(), agenda)
    return len(compiled) == len(agenda.tasks)

###############################################################################
# Evaluation tests
###############################################################################
@context(evaluated_agendas)
@prop('Offline evaluation succeeds and creates every output', attempts=20)
def _agenda_evaluate_offline(agenda: ta.Agenda) -> bool:
    test_path = _fixture_path()
    if not _offline(test_path, agenda): return False
    return all(path.is_file() for path in _outputs(test_path, agenda))


@context(evaluated_agendas)
@prop('Offline evaluation reports a failing task', attempts=20)
def _agenda_evaluate_offline_failure(
    agenda: ta.Agenda,
    rng: random.Random
) -> bool:
    assume(len(agenda.tasks) != 0)
    failing_task = rng.choice(agenda.tasks)
    agenda.procs[failing_task.proc] = [
        agenda.procs[failing_task.proc][0],
        str(FAILURE_SCRIPT),
        '-i', '$inputs',
        '-o', '$outputs'
    ]
    return not _offline(_fixture_path(), agenda)


@context(evaluated_agendas)
@prop('Clean removes every output and the cache', attempts=20)
def _agenda_clean(agenda: ta.Agenda) -> bool:
    test_path = _fixture_path()
    if not _offline(test_path, agenda): return False
    cache_path = test_path / 'tickle.cache'
    with _quiet():
        if not tapi.clean(test_path, cache_path): return False
    if cache_path.exists(): return False
    return not any(path.exists() for path in _outputs(test_path, agenda))

###############################################################################
# Specification
###############################################################################
spec = conj(
    _agenda_generator_unique_outputs,
    _agenda_generator_is_member_of_flow,
    _agenda_generator_acyclic,
    _agenda_encode_decode_inverse,
    _agenda_store_load_inverse,
    _agenda_compiles,
    _agenda_evaluate_offline,
    _agenda_evaluate_offline_failure,
    _agenda_clean,
)
