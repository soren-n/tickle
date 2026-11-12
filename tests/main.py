# External module dependencies
from collections.abc import Iterator
from contextlib import contextmanager, redirect_stdout
from pathlib import Path
from threading import Thread
import io
import os
import time
import logging
import random
import sys

from minigun import assume, conj, context, prop
from minigun import generate as g
from minigun.fixture import temporary_path

import tickle.dataspec as td
import tickle.agenda as ta
import tickle.api as tapi
import tickle.depend as tdep

# Internal module dependencies
from ._agenda import (
    FAILURE_SCRIPT,
    SUCCESS_SCRIPT,
    Word,
    agendas,
    command_templates,
    cyclic_agendas,
    cyclic_depends,
    text_agendas,
    text_depends,
)

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
    # Depth-first search from every node, independent of tickle's own check
    checked: set[int] = set()

    def _visit(node: int, path: set[int]) -> bool:
        if node in checked: return False
        if node in path: return True
        path.add(node)
        if any(_visit(dep, path) for dep in graph[node]): return True
        path.remove(node)
        checked.add(node)
        return False

    return any(_visit(node, set()) for node in graph)


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


def _run_offline(
    test_path: Path,
    agenda: ta.Agenda | None = None
) -> tuple[bool, list[str]]:
    """Run offline mode, storing the agenda first if one is given; returns
    whether it succeeded and the descriptions of the tasks it ran, in the
    order they started."""
    agenda_path = test_path / 'agenda.yaml'
    if agenda is not None: ta.store(agenda_path, agenda)
    output = io.StringIO()
    logging.disable(logging.CRITICAL)
    try:
        with redirect_stdout(output):
            success = tapi.offline(
                test_path,
                agenda_path,
                test_path / 'depend.yaml',
                test_path / 'tickle.cache'
            )
    finally:
        logging.disable(logging.NOTSET)
    performed = [
        line.removeprefix('[tickle] ')
        for line in output.getvalue().splitlines()
        if line.startswith('[tickle] Task ')
    ]
    return success, performed


def _offline(test_path: Path, agenda: ta.Agenda) -> bool:
    success, _ = _run_offline(test_path, agenda)
    return success


def _dependents(agenda: ta.Agenda, task: ta.Task) -> set[str]:
    """The descriptions of the task and every task downstream of it."""
    producer = {
        output: other.desc
        for other in agenda.tasks
        for output in other.outputs
    }
    result = {task.desc}
    changed = True
    while changed:
        changed = False
        for other in agenda.tasks:
            if other.desc in result: continue
            if any(producer.get(input) in result for input in other.inputs):
                result.add(other.desc)
                changed = True
    return result


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

@context(text_agendas())
@prop('Agenda store and load are inverse for any text')
def _agenda_store_load_inverse_text(agenda: ta.Agenda) -> bool:
    agenda_path = _fixture_path() / 'agenda.yaml'
    ta.store(agenda_path, agenda)
    return agenda == ta.load(agenda_path)


@context(text_depends())
@prop('Depend store and load are inverse for any text')
def _depend_store_load_inverse(depend: tdep.Depend) -> bool:
    depend_path = _fixture_path() / 'depend.yaml'
    tdep.store(depend_path, depend)
    return depend == tdep.load(depend_path)


@context(command_templates())
@prop('Agenda commands keep literal words and give each argument a word')
def _agenda_command_expansion(
    template: tuple[list[Word], dict[str, list[str]]]
) -> bool:
    words, args = template
    proc = [
        text if kind == 'literal' else '$' + text
        for kind, text in words
    ]
    expected: list[str] = []
    for kind, text in words:
        if kind == 'literal': expected.append(text)
        else: expected.extend(args[text])
    agenda = ta.Agenda(
        procs = {'proc': proc},
        flows = {'flow': [['proc']]},
        tasks = [ta.Task(
            desc = 'Task', proc = 'proc', flows = ['flow'], args = args,
            inputs = [], outputs = ['output']
        )]
    )
    return ta.compile(Path('/target'), agenda)[0].command == expected

###############################################################################
# Evaluation tests
###############################################################################
@context(evaluated_agendas)
@prop('Offline evaluation succeeds and creates every output', attempts=100)
def _agenda_evaluate_offline(agenda: ta.Agenda) -> bool:
    test_path = _fixture_path()
    if not _offline(test_path, agenda): return False
    return all(path.is_file() for path in _outputs(test_path, agenda))


@context(evaluated_agendas)
@prop('Offline evaluation reports a failing task', attempts=100)
def _agenda_evaluate_offline_failure(
    agenda: ta.Agenda,
    missing_command: bool,
    rng: random.Random
) -> bool:
    assume(len(agenda.tasks) != 0)
    failing_task = rng.choice(agenda.tasks)
    if missing_command:
        # A command that cannot be started, rather than one that fails
        agenda.procs[failing_task.proc] = ['tickle-test-missing-command']
    else:
        agenda.procs[failing_task.proc] = [
            agenda.procs[failing_task.proc][0],
            str(FAILURE_SCRIPT),
            '-i', '$inputs',
            '-o', '$outputs'
        ]
    return not _offline(_fixture_path(), agenda)


@context(cyclic_agendas())
@prop('Offline evaluation rejects a cyclic agenda')
def _agenda_evaluate_offline_cycle(agenda: ta.Agenda) -> bool:
    return not _offline(_fixture_path(), agenda)


@context(evaluated_agendas)
@prop('Offline evaluation without changes runs no tasks', attempts=100)
def _agenda_evaluate_offline_incremental(agenda: ta.Agenda) -> bool:
    test_path = _fixture_path()
    if not _offline(test_path, agenda): return False
    success, performed = _run_offline(test_path)
    return success and len(performed) == 0


@context(evaluated_agendas)
@prop('Offline evaluation reruns a task when a depend file changes', attempts=100)
def _agenda_evaluate_offline_depend(
    agenda: ta.Agenda,
    rng: random.Random
) -> bool:
    candidates = [task for task in agenda.tasks if len(task.inputs) != 0]
    assume(len(candidates) != 0)
    task = rng.choice(candidates)

    # Make the task's first input depend on a file only the depend file names
    test_path = _fixture_path()
    header_path = test_path / 'temp' / 'header.txt'
    header_path.parent.mkdir(parents = True, exist_ok = True)
    header_path.write_text('before')
    tdep.store(test_path / 'depend.yaml', {
        task.inputs[0]: ['temp/header.txt']
    })
    if not _offline(test_path, agenda): return False

    # Changing it must rerun the task
    header_path.write_text('after')
    success, performed = _run_offline(test_path)
    return success and task.desc in performed


@context(evaluated_agendas)
@prop('Offline evaluation recovers from a damaged cache', attempts=100)
def _agenda_evaluate_offline_damaged_cache(agenda: ta.Agenda) -> bool:
    test_path = _fixture_path()
    if not _offline(test_path, agenda): return False
    (test_path / 'tickle.cache').write_bytes(b'not a pickle')
    success, _ = _run_offline(test_path)
    return success and all(
        path.is_file() for path in _outputs(test_path, agenda)
    )


@context(evaluated_agendas)
@prop('Offline evaluation starts tasks after their dependencies and earlier stages', attempts=100)
def _agenda_evaluate_offline_order(agenda: ta.Agenda) -> bool:
    success, performed = _run_offline(_fixture_path(), agenda)
    if not success or len(performed) != len(agenda.tasks): return False
    started = {desc: index for index, desc in enumerate(performed)}
    tasks = {task.desc: task for task in agenda.tasks}
    producer = {
        output: task.desc
        for task in agenda.tasks
        for output in task.outputs
    }

    # Dependencies start first
    for task in agenda.tasks:
        for input in task.inputs:
            if input not in producer: continue
            if started[producer[input]] > started[task.desc]: return False

    # Within a flow, every task of a stage starts before any of a later one
    stage_of = {
        (flow, proc): index
        for flow, stages in agenda.flows.items()
        for index, stage in enumerate(stages)
        for proc in stage
    }
    for flow in agenda.flows:
        stages = [
            stage_of[(flow, tasks[desc].proc)]
            for desc in performed
            if flow in tasks[desc].flows
        ]
        if stages != sorted(stages): return False
    return True


@context(evaluated_agendas)
@prop('Offline evaluation after reordering the tasks runs no tasks', attempts=100)
def _agenda_evaluate_offline_reorder(
    agenda: ta.Agenda,
    rng: random.Random
) -> bool:
    test_path = _fixture_path()
    if not _offline(test_path, agenda): return False
    rng.shuffle(agenda.tasks)
    success, performed = _run_offline(test_path, agenda)
    return success and len(performed) == 0


@context(evaluated_agendas)
@prop('Offline evaluation reruns a task with a missing output and the tasks after it', attempts=100)
def _agenda_evaluate_offline_missing_output(
    agenda: ta.Agenda,
    rng: random.Random
) -> bool:
    assume(len(agenda.tasks) != 0)
    test_path = _fixture_path()
    if not _offline(test_path, agenda): return False
    task = rng.choice(agenda.tasks)
    (test_path / task.outputs[0]).unlink()
    success, performed = _run_offline(test_path)
    return success and set(performed) == _dependents(agenda, task)


@context(evaluated_agendas)
@prop('Offline evaluation reruns changed tasks and the tasks after them', attempts=100)
def _agenda_evaluate_offline_changed_task(
    agenda: ta.Agenda,
    change_proc: bool,
    rng: random.Random
) -> bool:
    assume(len(agenda.tasks) != 0)
    test_path = _fixture_path()
    if not _offline(test_path, agenda): return False

    # Change a task without changing any file, so only the change itself and
    # propagation can make tasks rerun
    task = rng.choice(agenda.tasks)
    if change_proc:
        # A trailing '-i' resets the script's inputs: same effect, new
        # command, for every task using the proc
        agenda.procs[task.proc] = agenda.procs[task.proc] + ['-i']
        changed = [other for other in agenda.tasks if other.proc == task.proc]
    else:
        # An argument the command does not use
        task.args['unused'] = ['changed']
        changed = [task]
    expected = set().union(*(_dependents(agenda, other) for other in changed))
    success, performed = _run_offline(test_path, agenda)
    return success and set(performed) == expected


@context(evaluated_agendas)
@prop('Offline evaluation skips a task with an impossible input and the tasks after it', attempts=100)
def _agenda_evaluate_offline_skip(
    agenda: ta.Agenda,
    rng: random.Random
) -> bool:
    assume(len(agenda.tasks) != 0)
    task = rng.choice(agenda.tasks)
    task.inputs.append('missing/never.txt')
    skipped = _dependents(agenda, task)
    test_path = _fixture_path()
    success, performed = _run_offline(test_path, agenda)
    if not success: return False
    if set(performed) != {t.desc for t in agenda.tasks} - skipped: return False
    return all(
        (test_path / output).is_file() == (other.desc not in skipped)
        for other in agenda.tasks
        for output in other.outputs
    )


@context(evaluated_agendas, cyclic_depends())
@prop('Offline evaluation rejects a cyclic depend file', attempts=50)
def _agenda_evaluate_offline_cyclic_depend(
    agenda: ta.Agenda,
    depend: tdep.Depend
) -> bool:
    test_path = _fixture_path()
    tdep.store(test_path / 'depend.yaml', depend)
    return not _offline(test_path, agenda)


def _wait_for(condition, timeout: float = 20.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition(): return True
        time.sleep(0.05)
    return False


@context(evaluated_agendas)
@prop('Online evaluation creates every output, follows the agenda and stops', attempts=20)
def _agenda_evaluate_online(agenda: ta.Agenda) -> bool:
    test_path = _fixture_path()
    agenda_path = test_path / 'agenda.yaml'
    ta.store(agenda_path, agenda)
    with _quiet():
        evaluator = tapi.online(
            test_path,
            agenda_path,
            test_path / 'depend.yaml',
            test_path / 'tickle.cache'
        )
        if evaluator is None: return False
        runner = Thread(target = evaluator.start, daemon = True)
        runner.start()
        try:
            # Everything gets built
            if not _wait_for(lambda: all(
                path.is_file() for path in _outputs(test_path, agenda)
            )): return False

            # A task added to the agenda gets built, also when the agenda
            # is saved by renaming a new file over it
            proc = next(iter(agenda.procs.values()), None)
            if proc is None:
                agenda.procs['extra'] = [
                    sys.executable, str(SUCCESS_SCRIPT),
                    '-i', '$inputs', '-o', '$outputs'
                ]
                agenda.flows['extra'] = [['extra']]
            name = next(iter(agenda.procs))
            flow = next(
                flow for flow, stages in agenda.flows.items()
                if any(name in stage for stage in stages)
            )
            agenda.tasks.append(ta.Task(
                desc = 'Extra task', proc = name, flows = [flow],
                args = {'inputs': [], 'outputs': ['temp/extra.txt']},
                inputs = [], outputs = ['temp/extra.txt']
            ))
            temp_path = test_path / 'agenda.yaml.new'
            ta.store(temp_path, agenda)
            os.replace(temp_path, agenda_path)
            if not _wait_for(
                lambda: (test_path / 'temp' / 'extra.txt').is_file()
            ): return False
        finally:
            evaluator.stop()
            runner.join(timeout = 10)
    return not runner.is_alive()


@context(evaluated_agendas)
@prop('Clean removes every output and the cache', attempts=100)
def _agenda_clean(agenda: ta.Agenda) -> bool:
    test_path = _fixture_path()
    if not _offline(test_path, agenda): return False

    # A file tickle did not generate, in a folder it did
    stray_path = test_path / 'temp' / 'stray.txt'
    stray_path.parent.mkdir(parents = True, exist_ok = True)
    stray_path.write_text('keep')

    cache_path = test_path / 'tickle.cache'
    with _quiet():
        if not tapi.clean(test_path, cache_path): return False
    if cache_path.exists(): return False
    if not (test_path / 'agenda.yaml').is_file(): return False
    if stray_path.read_text() != 'keep': return False
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
    _agenda_store_load_inverse_text,
    _depend_store_load_inverse,
    _agenda_command_expansion,
    _agenda_evaluate_offline,
    _agenda_evaluate_offline_failure,
    _agenda_evaluate_offline_cycle,
    _agenda_evaluate_offline_incremental,
    _agenda_evaluate_offline_depend,
    _agenda_evaluate_offline_damaged_cache,
    _agenda_evaluate_offline_order,
    _agenda_evaluate_offline_reorder,
    _agenda_evaluate_offline_missing_output,
    _agenda_evaluate_offline_changed_task,
    _agenda_evaluate_offline_skip,
    _agenda_evaluate_offline_cyclic_depend,
    _agenda_evaluate_online,
    _agenda_clean,
)
