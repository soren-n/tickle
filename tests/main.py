# External module dependencies
from collections.abc import Iterator
from contextlib import contextmanager, redirect_stdout
from pathlib import Path
from threading import Lock, Thread
import io
import os
import pickle
import signal
import time
import logging
import random
import shutil
import subprocess
import sys

import yaml

from minigun import assume, conj, context, prop
from minigun import generate as g
from minigun.fixture import temporary_path

import tickle.dataspec as td
import tickle.agenda as ta
import tickle.api as tapi
import tickle.depend as tdep
import tickle.evaluate as tev
import tickle.graph as tgraph

# Internal module dependencies
from ._agenda import (
    FAILURE_SCRIPT,
    SUCCESS_SCRIPT,
    Word,
    agendas,
    command_templates,
    cyclic_agendas,
    cyclic_depends,
    respell,
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

# Worker counts from one, the most serial, to more than most agendas use
worker_counts = g.int_range(1, 8)

# Properties that start processes declare how many attempts they need; CI
# scales that down where starting a process is slow (Windows), and declared
# attempts are not limited by minigun's time budget.
ATTEMPT_SCALE = float(os.environ.get('TICKLE_TEST_ATTEMPT_SCALE', '1'))


def _attempts(count: int) -> int:
    return max(1, round(count * ATTEMPT_SCALE))

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
    agenda: ta.Agenda | None = None,
    workers: int | None = None
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
                test_path / 'tickle.cache',
                tapi.default_worker_count() if workers is None else workers
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


def _norm(path: str) -> str:
    """The canonical spelling of an agenda path; generated agendas write
    paths in several equivalent ways."""
    return os.path.normpath(path).replace(os.sep, '/')


def _dependents(agenda: ta.Agenda, task: ta.Task) -> set[str]:
    """The descriptions of the task and every task downstream of it."""
    producer = {
        _norm(output): other.desc
        for other in agenda.tasks
        for output in other.outputs
    }
    result = {task.desc}
    changed = True
    while changed:
        changed = False
        for other in agenda.tasks:
            if other.desc in result: continue
            if any(producer.get(_norm(input)) in result for input in other.inputs):
                result.add(other.desc)
                changed = True
    return result


def _outputs(test_path: Path, agenda: ta.Agenda) -> list[Path]:
    return [
        test_path / _norm(output)
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
        for output in map(_norm, task.outputs):
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
            file_to_task[_norm(output_file)] = task_index

    # Build task graph
    graph: dict[int, set[int]] = {}
    for task_index, task in enumerate(agenda.tasks):
        graph[task_index] = {
            file_to_task[_norm(input_file)]
            for input_file in task.inputs
            if _norm(input_file) in file_to_task
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
@prop('Offline evaluation succeeds and creates every output', attempts=_attempts(100))
def _agenda_evaluate_offline(agenda: ta.Agenda) -> bool:
    test_path = _fixture_path()
    if not _offline(test_path, agenda): return False
    return all(path.is_file() for path in _outputs(test_path, agenda))


@context(evaluated_agendas)
@prop('Offline evaluation reports a failing task', attempts=_attempts(100))
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
@prop('Offline evaluation without changes runs no tasks', attempts=_attempts(100))
def _agenda_evaluate_offline_incremental(agenda: ta.Agenda) -> bool:
    test_path = _fixture_path()
    if not _offline(test_path, agenda): return False
    success, performed = _run_offline(test_path)
    return success and len(performed) == 0


@context(evaluated_agendas)
@prop('Offline evaluation reruns a task when a depend file changes', attempts=_attempts(100))
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

    # Changing it must rerun exactly the tasks reading that input, and the
    # tasks after them
    header_path.write_text('after')
    readers = [
        other for other in agenda.tasks
        if _norm(task.inputs[0]) in map(_norm, other.inputs)
    ]
    expected = set().union(*(_dependents(agenda, other) for other in readers))
    success, performed = _run_offline(test_path)
    return success and set(performed) == expected


@context(evaluated_agendas)
@prop('Offline evaluation recovers from a damaged cache', attempts=_attempts(100))
def _agenda_evaluate_offline_damaged_cache(agenda: ta.Agenda) -> bool:
    test_path = _fixture_path()
    if not _offline(test_path, agenda): return False
    (test_path / 'tickle.cache').write_bytes(b'not a pickle')
    success, _ = _run_offline(test_path)
    return success and all(
        path.is_file() for path in _outputs(test_path, agenda)
    )


@context(evaluated_agendas, worker_counts)
@prop('Offline evaluation starts tasks after their dependencies and earlier stages', attempts=_attempts(100))
def _agenda_evaluate_offline_order(agenda: ta.Agenda, workers: int) -> bool:
    success, performed = _run_offline(_fixture_path(), agenda, workers)
    if not success or len(performed) != len(agenda.tasks): return False
    started = {desc: index for index, desc in enumerate(performed)}
    tasks = {task.desc: task for task in agenda.tasks}
    producer = {
        _norm(output): task.desc
        for task in agenda.tasks
        for output in task.outputs
    }

    # Dependencies start first
    for task in agenda.tasks:
        for input in map(_norm, task.inputs):
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
@prop('Offline evaluation after reordering the tasks runs no tasks', attempts=_attempts(100))
def _agenda_evaluate_offline_reorder(
    agenda: ta.Agenda,
    rng: random.Random
) -> bool:
    test_path = _fixture_path()
    if not _offline(test_path, agenda): return False
    rng.shuffle(agenda.tasks)
    success, performed = _run_offline(test_path, agenda)
    return success and len(performed) == 0


@context(evaluated_agendas, worker_counts)
@prop('Offline evaluation reruns a task with a missing output and the tasks after it', attempts=_attempts(100))
def _agenda_evaluate_offline_missing_output(
    agenda: ta.Agenda,
    workers: int,
    rng: random.Random
) -> bool:
    assume(len(agenda.tasks) != 0)
    test_path = _fixture_path()
    if not _run_offline(test_path, agenda, workers)[0]: return False
    task = rng.choice(agenda.tasks)
    (test_path / _norm(task.outputs[0])).unlink()
    success, performed = _run_offline(test_path, None, workers)
    return success and set(performed) == _dependents(agenda, task)


@context(evaluated_agendas, worker_counts)
@prop('Offline evaluation reruns changed tasks and the tasks after them', attempts=_attempts(100))
def _agenda_evaluate_offline_changed_task(
    agenda: ta.Agenda,
    workers: int,
    change_proc: bool,
    rng: random.Random
) -> bool:
    assume(len(agenda.tasks) != 0)
    test_path = _fixture_path()
    if not _run_offline(test_path, agenda, workers)[0]: return False

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
    success, performed = _run_offline(test_path, agenda, workers)
    return success and set(performed) == expected


@context(evaluated_agendas)
@prop('Offline evaluation skips a task with an impossible input and the tasks after it, and fails', attempts=_attempts(100))
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
    if success: return False
    if set(performed) != {t.desc for t in agenda.tasks} - skipped: return False
    return all(
        (test_path / _norm(output)).is_file() == (other.desc not in skipped)
        for other in agenda.tasks
        for output in other.outputs
    )


@context(evaluated_agendas, cyclic_depends())
@prop('Offline evaluation rejects a cyclic depend file', attempts=_attempts(50))
def _agenda_evaluate_offline_cyclic_depend(
    agenda: ta.Agenda,
    depend: tdep.Depend
) -> bool:
    test_path = _fixture_path()
    tdep.store(test_path / 'depend.yaml', depend)
    return not _offline(test_path, agenda)


def _add_trigger(agenda: ta.Agenda, task: ta.Task, trigger: str) -> None:
    """Give the task an extra source input that its script reads, so writing
    'fail' into it makes the task fail without changing its definition."""
    task.inputs.append(trigger)
    task.args['inputs'] = task.args['inputs'] + [trigger]


@context(evaluated_agendas, worker_counts)
@prop('Offline evaluation reruns a task that failed until it succeeds', attempts=_attempts(100))
def _agenda_evaluate_offline_rerun_failed(
    agenda: ta.Agenda,
    workers: int,
    rng: random.Random
) -> bool:
    assume(len(agenda.tasks) != 0)
    task = rng.choice(agenda.tasks)
    _add_trigger(agenda, task, 'src/trigger.txt')
    test_path = _fixture_path()
    trigger_path = test_path / 'src' / 'trigger.txt'
    trigger_path.parent.mkdir(parents = True, exist_ok = True)
    trigger_path.write_text('ok')
    if not _run_offline(test_path, agenda, workers)[0]: return False

    # A failing run, then another one with nothing changed: the failed task
    # must run again, and fail again, and no task after it may start
    after = _dependents(agenda, task) - {task.desc}
    trigger_path.write_text('fail')
    success, performed = _run_offline(test_path, None, workers)
    if success or task.desc not in performed or after & set(performed):
        return False
    success, performed = _run_offline(test_path, None, workers)
    if success or task.desc not in performed or after & set(performed):
        return False

    # Once it can succeed, it and every task after it run; the content
    # differs from the last successful run, which would need no rerun
    trigger_path.write_text('ok again')
    success, performed = _run_offline(test_path, None, workers)
    return success and _dependents(agenda, task) <= set(performed)


@context(g.int_range(1, 4), worker_counts)
@prop('Offline evaluation reruns a task whose input changed while it ran', attempts=_attempts(30))
def _agenda_evaluate_offline_edit_during_run(
    chain_length: int,
    workers: int
) -> bool:
    # A chain of tasks; the first reads src/live.txt and appends to it after
    # reading, as a user saving the file while the task runs would
    script = [sys.executable, str(SUCCESS_SCRIPT), '-i', '$inputs', '-o', '$outputs']
    agenda = ta.Agenda(
        procs = {'edit': script + ['--append', 'src/live.txt'], 'proc': script},
        flows = {'flow': [['edit', 'proc']]}
    )
    for index in range(chain_length):
        inputs = ['src/live.txt'] if index == 0 else ['temp/file_%d.txt' % (index - 1)]
        outputs = ['temp/file_%d.txt' % index]
        agenda.tasks.append(ta.Task(
            desc = 'Task %d' % index, proc = 'edit' if index == 0 else 'proc',
            flows = ['flow'], args = {'inputs': inputs, 'outputs': outputs},
            inputs = inputs, outputs = outputs
        ))
    test_path = _fixture_path()
    (test_path / 'src').mkdir()
    (test_path / 'src' / 'live.txt').write_text('live')
    if not _run_offline(test_path, agenda, workers)[0]: return False

    # The task read the file before the edit, so it is out of date, and its
    # output changes, so the whole chain runs again
    success, performed = _run_offline(test_path, None, workers)
    return success and set(performed) == {task.desc for task in agenda.tasks}


def _clean_build(test_path: Path, agenda: ta.Agenda, workers: int) -> Path | None:
    """Build the agenda from scratch next to the sources of test_path, and
    return where, or None if that build fails."""
    clean_path = _fixture_path() / 'clean'
    clean_path.mkdir()
    for name in ['src', 'hdr']:
        if (test_path / name).exists():
            shutil.copytree(test_path / name, clean_path / name)
    if (test_path / 'depend.yaml').exists():
        shutil.copy(test_path / 'depend.yaml', clean_path / 'depend.yaml')
    if not _run_offline(clean_path, agenda, workers)[0]: return None
    return clean_path


@context(evaluated_agendas, worker_counts, g.int_range(1, 6))
@prop('Offline evaluation after random changes and failures matches a clean build', attempts=_attempts(30))
def _agenda_evaluate_offline_matches_clean_build(
    agenda: ta.Agenda,
    workers: int,
    step_count: int,
    rng: random.Random
) -> bool:
    """The outputs of the test task are a digest of everything it read, so
    any missed rerun leaves an output different from a clean build's."""
    assume(len(agenda.tasks) != 0)
    test_path = _fixture_path()
    counter = iter(range(1000000))

    def _write(rel_path: str, text: str) -> None:
        path = test_path / rel_path
        path.parent.mkdir(parents = True, exist_ok = True)
        path.write_text(text)

    def _fresh() -> str:
        return 'content %d' % next(counter)

    # Sources for some tasks, a few headers, and a trigger that makes one
    # task fail without changing its definition
    sources: list[str] = []
    for index, task in enumerate(agenda.tasks):
        if not rng.choice([True, False]): continue
        source = 'src/source_%d.txt' % index
        task.inputs.append(source)
        task.args['inputs'] = task.args['inputs'] + [source]
        sources.append(source)
    headers = ['hdr/header_%d.txt' % index for index in range(3)]
    trigger_task = rng.choice(agenda.tasks)
    _add_trigger(agenda, trigger_task, 'src/trigger.txt')

    def _random_depend() -> tdep.Depend:
        # Inputs depend on headers, and headers on later headers, so the
        # depend file never has a cycle
        files = sources + [
            output for task in agenda.tasks for output in task.outputs
        ]
        result: tdep.Depend = {}
        for file in files:
            if rng.choice([True, False]): continue
            result[file] = rng.sample(headers, rng.randint(1, len(headers)))
        for index, header in enumerate(headers[:-1]):
            if rng.choice([True, False]): continue
            result[header] = [rng.choice(headers[index + 1:])]
        return result

    for file in sources + headers: _write(file, _fresh())
    _write('src/trigger.txt', 'ok')
    tdep.store(test_path / 'depend.yaml', _random_depend())
    if not _run_offline(test_path, agenda, workers)[0]: return False

    failing = False
    for _ in range(step_count):
        action = rng.choice([
            'source', 'header', 'depend', 'output', 'reorder',
            'args', 'proc', 'fail', 'unfail', 'move', 'respell', 'flows'
        ])
        if action == 'source' and len(sources) != 0:
            _write(rng.choice(sources), _fresh())
        elif action == 'header':
            _write(rng.choice(headers), _fresh())
        elif action == 'depend':
            tdep.store(test_path / 'depend.yaml', _random_depend())
        elif action == 'output':
            path = test_path / _norm(rng.choice(agenda.tasks).outputs[0])
            if path.exists(): path.unlink()
        elif action == 'reorder':
            rng.shuffle(agenda.tasks)
        elif action == 'args':
            rng.choice(agenda.tasks).args['unused'] = [_fresh()]
        elif action == 'proc':
            proc = rng.choice(list(agenda.procs))
            agenda.procs[proc] = agenda.procs[proc] + ['--salt', _fresh()]
        elif action == 'fail':
            _write('src/trigger.txt', 'fail')
            failing = True
        elif action == 'unfail':
            _write('src/trigger.txt', _fresh())
            failing = False
        elif action == 'respell':
            # Another spelling of the same files changes nothing
            task = rng.choice(agenda.tasks)
            task.inputs = [respell(path, rng.randrange(5)) for path in task.inputs]
            task.outputs = [respell(path, rng.randrange(5)) for path in task.outputs]
        elif action == 'flows':
            # Flows only order tasks; regrouping them changes no output
            if rng.choice([True, False]):
                renamed = {flow: flow + '_renamed' for flow in agenda.flows}
                agenda.flows = {renamed[f]: stages for f, stages in agenda.flows.items()}
                for task in agenda.tasks:
                    task.flows = [renamed[flow] for flow in task.flows]
            else:
                agenda.flows = {'single': [sorted({t.proc for t in agenda.tasks})]}
                for task in agenda.tasks: task.flows = ['single']
        elif action == 'move':
            moved_path = _fixture_path() / 'moved'
            shutil.copytree(test_path, moved_path)
            test_path = moved_path

        # While the trigger says fail, its task is out of date, so every run
        # must run it and fail; otherwise every run must succeed
        success, _ = _run_offline(test_path, agenda, workers)
        if success == failing: return False

    # Once everything can succeed: a run succeeds, the next has nothing to
    # do, and the outputs are those of a clean build
    _write('src/trigger.txt', _fresh())
    if not _run_offline(test_path, agenda, workers)[0]: return False
    success, performed = _run_offline(test_path, agenda, workers)
    if not success or len(performed) != 0: return False
    clean_path = _clean_build(test_path, agenda, workers)
    if clean_path is None: return False
    return all(
        (test_path / _norm(output)).read_text()
        == (clean_path / _norm(output)).read_text()
        for task in agenda.tasks
        for output in task.outputs
    )


@context(evaluated_agendas)
@prop('Offline evaluation after moving the project runs no tasks', attempts=_attempts(100))
def _agenda_evaluate_offline_relocate(agenda: ta.Agenda) -> bool:
    test_path = _fixture_path()
    if not _offline(test_path, agenda): return False
    moved_path = _fixture_path() / 'moved'
    shutil.copytree(test_path, moved_path)
    success, performed = _run_offline(moved_path)
    return success and len(performed) == 0


@context(g.int_range(1, 4))
@prop('Offline evaluation orders a task after the task generating a file its depend entries name', attempts=_attempts(50))
def _agenda_evaluate_offline_depend_order(consumer_count: int) -> bool:
    agenda = ta.Agenda()
    agenda.procs['generate'] = [
        sys.executable, '-c',
        'import pathlib, sys, time; time.sleep(0.3); '
        'path = pathlib.Path(sys.argv[1]); '
        'path.parent.mkdir(parents = True, exist_ok = True); '
        'path.write_text("generated")',
        '$output'
    ]
    agenda.procs['consume'] = [
        sys.executable, str(SUCCESS_SCRIPT),
        '-i', 'gen/header.txt', '$inputs', '-o', '$outputs'
    ]
    agenda.flows['flow'] = [['generate', 'consume']]
    agenda.tasks.append(ta.Task(
        desc = 'Task generate', proc = 'generate', flows = ['flow'],
        args = {'output': ['gen/header.txt']},
        inputs = [], outputs = ['gen/header.txt']
    ))
    test_path = _fixture_path()
    depend: tdep.Depend = {}
    for index in range(consumer_count):
        source = 'src/source_%d.txt' % index
        (test_path / 'src').mkdir(exist_ok = True)
        (test_path / source).write_text('source')
        depend[source] = ['gen/header.txt']
        agenda.tasks.append(ta.Task(
            desc = 'Task consume %d' % index, proc = 'consume', flows = ['flow'],
            args = {'inputs': [source], 'outputs': ['out/%d.txt' % index]},
            inputs = [source], outputs = ['out/%d.txt' % index]
        ))
    tdep.store(test_path / 'depend.yaml', depend)
    success, performed = _run_offline(test_path, agenda)
    return success and performed[:1] == ['Task generate']


def _program_order(agenda: ta.Agenda) -> dict[str, tuple[int, int, int]] | None:
    """Compile the agenda's task graph into a program without running it, and
    return for each task its (batch, sequence, position) in the program."""
    _, tasks = _task_graph(agenda)
    for task in tasks:
        task.set_valid(False)
        task.set_active(True)
    result: dict[str, tuple[int, int, int]] = {}
    for batch_index, batch in enumerate(tgraph.compile(tasks)):
        for sequence_index, sequence in enumerate(batch):
            for position, task in enumerate(sequence):
                desc = task.description
                if desc in result: return None
                result[desc] = (batch_index, sequence_index, position)
    return result


@context(any_agendas)
@prop('Compiled programs run every task once, after its dependencies and earlier stages', attempts=1000)
def _program_order_respected(agenda: ta.Agenda) -> bool:
    order = _program_order(agenda)
    if order is None or len(order) != len(agenda.tasks): return False

    def _before(first: str, second: str) -> bool:
        # A task finishes before another starts if its batch is earlier, or
        # if both run in order in the same sequence
        batch_a, sequence_a, position_a = order[first]
        batch_b, sequence_b, position_b = order[second]
        if batch_a != batch_b: return batch_a < batch_b
        return sequence_a == sequence_b and position_a < position_b

    producer = {
        _norm(output): task.desc
        for task in agenda.tasks
        for output in task.outputs
    }
    for task in agenda.tasks:
        for input in map(_norm, task.inputs):
            if input not in producer: continue
            if not _before(producer[input], task.desc): return False
    stage_of = {
        (flow, proc): index
        for flow, stages in agenda.flows.items()
        for index, stage in enumerate(stages)
        for proc in stage
    }
    for first in agenda.tasks:
        for second in agenda.tasks:
            for flow in set(first.flows) & set(second.flows):
                if stage_of[(flow, first.proc)] >= stage_of[(flow, second.proc)]:
                    continue
                if not _before(first.desc, second.desc): return False
    return True


@context(g.int_range(2, 4), g.int_range(1, 3))
@prop('A task error is reported without cancelling the next program', attempts=_attempts(10))
def _evaluator_error_keeps_next_program(
    failure_count: int,
    task_count: int
) -> bool:
    reported: list[str] = []
    performed: list[int] = []

    class _Evaluator(tev.Evaluator):
        def on_task_error(self, error: tev.TaskError) -> None:
            reported.append(error.description)

    def _fail(index: int) -> None:
        raise tev.TaskError('Failure %d' % index, 'failed')

    evaluator = _Evaluator(4)
    runner = Thread(target = evaluator.start, daemon = True)
    runner.start()
    try:
        failing = [
            tev.Task({'flow': 0}, (lambda index = index: _fail(index)))
            for index in range(failure_count)
        ]
        evaluator.pause()
        evaluator.reprogram(tgraph.compile(failing))
        evaluator.resume()
        if not _wait_for(lambda: len(reported) != 0, 5.0): return False

        # A new program right after the first failure is reported, as a file
        # save would cause; the other failures belong to the old program
        working = [
            tev.Task({'flow': 0}, (lambda index = index: performed.append(index)))
            for index in range(task_count)
        ]
        evaluator.pause()
        evaluator.reprogram(tgraph.compile(working))
        evaluator.resume()
        if not _wait_for(lambda: len(performed) == task_count, 5.0):
            return False
        return _wait_for(lambda: len(reported) == failure_count, 5.0)
    finally:
        evaluator.stop()
        runner.join(timeout = 10)


def _wait_for(condition, timeout: float = 20.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition(): return True
        time.sleep(0.05)
    return False


@context(evaluated_agendas, worker_counts)
@prop('Online evaluation creates every output, follows the agenda and stops', attempts=_attempts(20))
def _agenda_evaluate_online(agenda: ta.Agenda, workers: int) -> bool:
    test_path = _fixture_path()
    agenda_path = test_path / 'agenda.yaml'
    ta.store(agenda_path, agenda)
    with _quiet():
        evaluator = tapi.online(
            test_path,
            agenda_path,
            test_path / 'depend.yaml',
            test_path / 'tickle.cache',
            workers
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
@prop('Clean removes every output and the cache', attempts=_attempts(100))
def _agenda_clean(agenda: ta.Agenda) -> bool:
    test_path = _fixture_path()
    if not _offline(test_path, agenda): return False

    # A file tickle did not generate, in a folder it did
    stray_path = test_path / 'temp' / 'stray.txt'
    stray_path.parent.mkdir(parents = True, exist_ok = True)
    stray_path.write_text('keep')

    if not _clean(test_path): return False
    if (test_path / 'tickle.cache').exists(): return False
    if not (test_path / 'agenda.yaml').is_file(): return False
    if stray_path.read_text() != 'keep': return False
    return not any(path.exists() for path in _outputs(test_path, agenda))


def _clean(test_path: Path) -> bool:
    with _quiet():
        return tapi.clean(
            test_path,
            test_path / 'tickle.cache',
            test_path / 'agenda.yaml',
            test_path / 'depend.yaml'
        )


@context(evaluated_agendas)
@prop('Clean keeps sources, files changed since they were generated, and other projects', attempts=_attempts(100))
def _agenda_clean_safe(
    agenda: ta.Agenda,
    rng: random.Random
) -> bool:
    readers = {_norm(input) for task in agenda.tasks for input in task.inputs}
    leafs = [
        task for task in agenda.tasks
        if not any(_norm(output) in readers for output in task.outputs)
    ]
    assume(len(leafs) >= 2)
    test_path = _fixture_path()
    if not _offline(test_path, agenda): return False

    # One generated file becomes a hand-written source of a new task
    source_task, edited_task = rng.sample(leafs, 2)
    source = _norm(source_task.outputs[0])
    agenda.tasks.remove(source_task)
    agenda.tasks.append(ta.Task(
        desc = 'Copy', proc = edited_task.proc, flows = edited_task.flows,
        args = {'inputs': [source], 'outputs': ['temp/copy.txt']},
        inputs = [source], outputs = ['temp/copy.txt']
    ))
    (test_path / source).write_text('hand written')
    if not _offline(test_path, agenda): return False

    # Another generated file is edited after it was generated
    edited = test_path / _norm(edited_task.outputs[0])
    edited.write_text('hand edited, and longer than before')

    # A copy of the project is cleaned without touching the original
    copy_path = _fixture_path() / 'copy'
    shutil.copytree(test_path, copy_path)
    if not _clean(copy_path): return False
    if not all(path.exists() for path in _outputs(test_path, agenda)):
        return False

    if not _clean(test_path): return False
    if (test_path / source).read_text() != 'hand written': return False
    if edited.read_text() != 'hand edited, and longer than before': return False
    return not (test_path / 'temp' / 'copy.txt').exists()

###############################################################################
# Scheduling without processes
###############################################################################
def _task_graph(agenda: ta.Agenda) -> tuple[list[ta.CompiledTask], list[tev.Task]]:
    """The agenda's task graph, with work that does nothing."""
    compiled = ta.compile(Path('/target'), agenda)
    tasks = [
        tev.Task(task.flows, lambda: None, description = task.description)
        for task in compiled
    ]
    producer = {
        output: tasks[index]
        for index, task in enumerate(compiled)
        for output in task.outputs
    }
    for index, task in enumerate(compiled):
        for input in task.inputs:
            if input in producer: tasks[index].add_dependency(producer[input])
    return compiled, tasks


@context(any_agendas)
@prop('Compiled programs run every out-of-date task that can run, also those only skipped tasks need', attempts=1000)
def _program_runs_runnable_tasks(agenda: ta.Agenda, rng: random.Random) -> bool:
    # Online mode compiles the graph without the task that offline mode adds
    # to end the run, so nothing above a skipped task leads to its producers
    _, tasks = _task_graph(agenda)
    for task in tasks:
        task.set_valid(rng.choice([True, False]))
        task.set_active(rng.random() < 0.8)
    tgraph.propagate(tasks)
    expected = {
        id(task) for task in tasks
        if not task.get_valid() and task.get_active()
    }
    scheduled = [
        id(task)
        for batch in tgraph.compile(tasks)
        for sequence in batch
        for task in sequence
    ]
    return len(scheduled) == len(set(scheduled)) and set(scheduled) == expected


@context(any_agendas, g.int_range(0, 20))
@prop('Compiled programs keep every order with dependencies beyond the inputs', attempts=1000)
def _program_order_extra_deps(
    agenda: ta.Agenda,
    extra_count: int,
    rng: random.Random
) -> bool:
    # Depend entries add dependencies that inputs alone would not, also
    # between tasks whose stages in their other flows disagree with them
    compiled, tasks = _task_graph(agenda)
    assume(len(tasks) >= 2)
    for _ in range(extra_count):
        dst, src = rng.sample(tasks, 2)
        if dst in _upstream(src): continue
        try: dst.add_dependency(src)
        except RuntimeError: continue
    for task in tasks:
        task.set_valid(False)
        task.set_active(True)

    # Dependencies and stage order together may form a cycle, which no
    # program can follow; compile must then refuse rather than drop tasks
    order: dict[int, set[int]] = {index: set() for index in range(len(tasks))}
    for index, task in enumerate(tasks):
        order[index].update(tasks.index(dep) for dep in task.get_deps())
        for other, other_data in enumerate(compiled):
            for flow, stage in compiled[index].flows.items():
                if other_data.flows.get(flow, stage) < stage: order[index].add(other)
    cyclic = _has_cycle(order)
    try: program = tgraph.compile(tasks)
    except RuntimeError: return cyclic
    if cyclic: return False
    position: dict[int, tuple[int, int]] = {}
    for batch_index, batch in enumerate(program):
        for sequence in batch:
            for index, task in enumerate(sequence):
                if id(task) in position: return False
                position[id(task)] = (batch_index, index)
    if len(position) != len(tasks): return False
    for task in tasks:
        for dep in task.get_deps():
            if position[id(dep)] >= position[id(task)]: return False
    for first, first_data in zip(tasks, compiled, strict = True):
        for second, second_data in zip(tasks, compiled, strict = True):
            for flow in set(first_data.flows) & set(second_data.flows):
                if first_data.flows[flow] >= second_data.flows[flow]: continue
                if position[id(first)] >= position[id(second)]: return False
    return True


def _upstream(task: tev.Task) -> set[tev.Task]:
    """The task and every task it depends on, directly or not."""
    result = {task}
    worklist = [task]
    while len(worklist) != 0:
        for dep in worklist.pop().get_deps():
            if dep in result: continue
            result.add(dep)
            worklist.append(dep)
    return result


def _wide_graph(width: int, stages: int) -> list[tev.Task]:
    """Stages of independent tasks, each task reading one task of the stage
    before, as per-frame steps of a pipeline do."""
    tasks: list[tev.Task] = []
    previous: list[tev.Task] = []
    for stage in range(stages):
        current = [tev.Task({'flow': stage}, lambda: None) for _ in range(width)]
        for task, dep in zip(current, previous, strict = False):
            task.add_dependency(dep)
        tasks += current
        previous = current
    for task in tasks:
        task.set_valid(False)
        task.set_active(True)
    return tasks


def _compile_time(width: int, stages: int) -> tuple[float, bool]:
    """The best of three times to compile a wide graph with a task that
    depends on every other, as offline mode adds, and whether the program
    holds every task."""
    best = float('inf')
    complete = True
    for _ in range(3):
        tasks = _wide_graph(width, stages)
        hub = tev.Task({'': 0}, lambda: None, True)
        hub.add_dependencies(tasks)
        tasks.append(hub)
        start = time.monotonic()
        program = tgraph.compile(tasks)
        best = min(best, time.monotonic() - start)
        count = sum(len(sequence) for batch in program for sequence in batch)
        complete = complete and count == len(tasks)
    return best, complete


@context(g.int_range(8000, 9000), g.int_range(2, 2))
@prop('Compiling a wide program takes time linear in its size', attempts=3)
def _program_compile_wide(width: int, stages: int) -> bool:
    # Batches used to be joined by an edge from every task of one batch to
    # every task of the next, so two stages of 4000 tasks took minutes and
    # gigabytes to compile; and the cycle check was quadratic in the number
    # of dependencies of one task (a ratio of about 10 here, against 4).
    # Comparing four times the tasks keeps the bound independent of the
    # machine.
    small, small_complete = _compile_time(width, stages)
    large, large_complete = _compile_time(4 * width, stages)
    return small_complete and large_complete and large < 7 * max(small, 0.01)

@context(any_agendas)
@prop('Compiled programs put each task in the earliest batch its dependencies and stages allow', attempts=1000)
def _program_batches_earliest(agenda: ta.Agenda) -> bool:
    # A task waiting for a batch it does not need costs wall time: with the
    # batch barrier, it waits for the slowest task of every batch before it
    compiled, tasks = _task_graph(agenda)
    for task in tasks:
        task.set_valid(False)
        task.set_active(True)
    batch_of: dict[int, int] = {}
    for batch_index, batch in enumerate(tgraph.compile(tasks)):
        for sequence in batch:
            for task in sequence: batch_of[id(task)] = batch_index

    # A task's batch is one more than that of its latest dependency or of
    # the latest task of an earlier stage in a flow it shares
    before: dict[int, set[int]] = {}
    for index, task in enumerate(compiled):
        before[index] = {
            other for other, dep in enumerate(tasks)
            if dep in tasks[index].get_deps()
        }
        for other, other_task in enumerate(compiled):
            for flow, stage in task.flows.items():
                if other_task.flows.get(flow, stage) < stage:
                    before[index].add(other)
    expected: dict[int, int] = {}

    def _level(index: int) -> int:
        if index not in expected:
            expected[index] = max(
                [-1] + [_level(other) for other in before[index]]
            ) + 1
        return expected[index]

    return all(
        batch_of.get(id(task)) == _level(index)
        for index, task in enumerate(tasks)
    )


@context(g.int_range(1, 4), g.int_range(1, 6), worker_counts, g.int_range(1, 8))
@prop('The evaluator runs each new program once and nothing of the programs it replaced', attempts=_attempts(30))
def _evaluator_reprogram_stress(
    program_count: int,
    task_count: int,
    workers: int,
    error_rate: int,
    rng: random.Random
) -> bool:
    # A watcher thread reprograms while tasks run and fail, as file saves
    # do in online mode; every program but the last may fail
    events: list[tuple[str, int, int]] = []
    lock = Lock()
    reported: list[str] = []

    class _Evaluator(tev.Evaluator):
        def on_task_error(self, error: tev.TaskError) -> None:
            reported.append(error.description)

    def _work(program: int, index: int, fail: bool) -> None:
        with lock: events.append(('start', program, index))
        time.sleep(rng.random() * 0.005)
        if fail: raise tev.TaskError('Task %d.%d' % (program, index), 'failed')
        with lock: events.append(('done', program, index))

    def _program(program: int, last: bool) -> list[tev.Task]:
        tasks: list[tev.Task] = []
        for index in range(task_count):
            fail = not last and rng.randrange(error_rate) == 0
            task = tev.Task(
                {'flow': 0},
                (lambda p = program, i = index, f = fail: _work(p, i, f))
            )
            for dep in tasks:
                if rng.random() < 0.3: task.add_dependency(dep)
            task.set_valid(False)
            task.set_active(True)
            tasks.append(task)
        return tasks

    evaluator = _Evaluator(workers)
    runner = Thread(target = evaluator.start, daemon = True)
    runner.start()
    try:
        for program in range(program_count):
            last = program == program_count - 1
            tasks = _program(program, last)
            evaluator.pause()
            evaluator.reprogram(tgraph.compile(tasks))
            with lock: events.append(('installed', program, -1))
            evaluator.resume()
            time.sleep(rng.random() * 0.01)
        if not _wait_for(lambda: sum(
            1 for kind, program, _ in events
            if kind == 'done' and program == program_count - 1
        ) >= task_count, 10.0): return False
        time.sleep(0.05)
    finally:
        evaluator.stop()
        runner.join(timeout = 10)
    if runner.is_alive(): return False

    # Nothing of a replaced program starts after its replacement is in
    installed = -1
    final_starts: list[int] = []
    for kind, program, index in events:
        if kind == 'installed': installed = program
        elif kind == 'start':
            if program < installed: return False
            if program == program_count - 1: final_starts.append(index)
    return sorted(final_starts) == list(range(task_count))

###############################################################################
# Agenda validation, paths and identity
###############################################################################
@context(any_agendas, g.int_range(0, 4))
@prop('Agenda load reports where a value does not fit')
def _agenda_decode_error_location(
    agenda: ta.Agenda,
    kind: int,
    rng: random.Random
) -> bool:
    raw = td.encode(ta.Agenda, agenda)
    tasks = raw['tasks']
    if kind == 0:
        assume(len(tasks) != 0)
        index = rng.randrange(len(tasks))
        tasks[index]['desc'] = 7
        where = 'tasks[%d].desc' % index
    elif kind == 1:
        candidates = [i for i, task in enumerate(tasks) if len(task['inputs']) != 0]
        assume(len(candidates) != 0)
        index = rng.choice(candidates)
        item = rng.randrange(len(tasks[index]['inputs']))
        tasks[index]['inputs'][item] = 7
        where = 'tasks[%d].inputs[%d]' % (index, item)
    elif kind == 2:
        assume(len(tasks) != 0)
        index = rng.randrange(len(tasks))
        tasks[index]['outputs'] = 'not a list'
        where = 'tasks[%d].outputs' % index
    elif kind == 3:
        assume(len(tasks) != 0)
        index = rng.randrange(len(tasks))
        tasks[index]['args']['inputs'] = 7
        where = 'tasks[%d].args.inputs' % index
    else:
        assume(len(raw['procs']) != 0)
        name = rng.choice(sorted(raw['procs']))
        raw['procs'][name] = [7]
        where = 'procs.%s[0]' % name
    agenda_path = _fixture_path() / 'agenda.yaml'
    agenda_path.write_text(yaml.safe_dump(raw))
    try: ta.load(agenda_path)
    except RuntimeError as error:
        return ('\n%s: ' % where) in str(error)
    return False
@context(any_agendas)
@prop('Agenda compile rejects an empty flow name')
def _agenda_rejects_empty_flow(agenda: ta.Agenda) -> bool:
    assume(len(agenda.flows) != 0)
    name = next(iter(agenda.flows))
    agenda.flows[''] = agenda.flows.pop(name)
    for task in agenda.tasks:
        task.flows = ['' if flow == name else flow for flow in task.flows]
    try: ta.compile(Path('/target'), agenda)
    except RuntimeError: return True
    return False


@context(evaluated_agendas, worker_counts)
@prop('Offline evaluation treats spellings of one path as one file', attempts=_attempts(100))
def _agenda_evaluate_offline_spellings(
    agenda: ta.Agenda,
    workers: int,
    rng: random.Random
) -> bool:
    # Only the inputs and outputs fields are respelled; the args the script
    # reads stay as they are, so the commands do not change
    def _spelled() -> ta.Agenda:
        result = ta.Agenda(dict(agenda.procs), dict(agenda.flows), [])
        for task in agenda.tasks:
            result.tasks.append(ta.Task(
                desc = task.desc, proc = task.proc, flows = task.flows,
                args = task.args,
                inputs = [respell(path, rng.randrange(5)) for path in task.inputs],
                outputs = [respell(path, rng.randrange(5)) for path in task.outputs]
            ))
        return result

    test_path = _fixture_path()
    success, performed = _run_offline(test_path, _spelled(), workers)
    if not success or len(performed) != len(agenda.tasks): return False
    if not all(path.is_file() for path in _outputs(test_path, agenda)):
        return False
    success, performed = _run_offline(test_path, _spelled(), workers)
    return success and len(performed) == 0


@context(worker_counts)
@prop('Offline evaluation rejects two spellings of one output', attempts=_attempts(5))
def _agenda_evaluate_offline_output_spellings(workers: int) -> bool:
    script = [sys.executable, str(SUCCESS_SCRIPT), '-i', '$inputs', '-o', '$outputs']
    agenda = ta.Agenda(procs = {'proc': script}, flows = {'flow': [['proc']]})
    for index, output in enumerate(['temp/file.txt', 'temp/../temp/file.txt']):
        agenda.tasks.append(ta.Task(
            desc = 'Task %d' % index, proc = 'proc', flows = ['flow'],
            args = {'inputs': [], 'outputs': ['temp/file_%d.txt' % index]},
            inputs = [], outputs = [output]
        ))
    success, performed = _run_offline(_fixture_path(), agenda, workers)
    return not success and len(performed) == 0


@context(evaluated_agendas, worker_counts)
@prop('Offline evaluation after renaming or regrouping flows runs no tasks', attempts=_attempts(100))
def _agenda_evaluate_offline_flows_identity(
    agenda: ta.Agenda,
    workers: int,
    rng: random.Random
) -> bool:
    # Flows only order tasks; changing them changes no output
    test_path = _fixture_path()
    if not _run_offline(test_path, agenda, workers)[0]: return False
    if rng.choice([True, False]):
        renamed = {flow: 'renamed_%s' % flow for flow in agenda.flows}
        agenda.flows = {renamed[flow]: stages for flow, stages in agenda.flows.items()}
        for task in agenda.tasks:
            task.flows = [renamed[flow] for flow in task.flows]
    else:
        # Every proc in one stage of one flow: dependencies alone order tasks
        procs = sorted({task.proc for task in agenda.tasks})
        agenda.flows = {'single': [procs]} if len(procs) != 0 else {}
        for task in agenda.tasks: task.flows = ['single']
    success, performed = _run_offline(test_path, agenda, workers)
    return success and len(performed) == 0

@context(evaluated_agendas, worker_counts)
@prop('Offline evaluation orders and reruns a task after the task producing a file its depend entry names', attempts=_attempts(100))
def _agenda_evaluate_offline_depend_generated(
    agenda: ta.Agenda,
    workers: int,
    rng: random.Random
) -> bool:
    # Task B gets a source whose depend entry names task A's output. A must
    # not come after B, and B's stage must allow depending on A.
    stage_of = {
        (flow, proc): index
        for flow, stages in agenda.flows.items()
        for index, stage in enumerate(stages)
        for proc in stage
    }
    def _ordered(a: ta.Task, b: ta.Task) -> bool:
        # B after A must not close a cycle with the other dependencies and
        # the stage order, which tickle rejects
        index = {task.desc: i for i, task in enumerate(agenda.tasks)}
        producer = {
            _norm(output): index[task.desc]
            for task in agenda.tasks for output in task.outputs
        }
        order: dict[int, set[int]] = {}
        for i, task in enumerate(agenda.tasks):
            order[i] = {
                producer[_norm(input)] for input in task.inputs
                if _norm(input) in producer
            }
            for j, other in enumerate(agenda.tasks):
                for flow in set(task.flows) & set(other.flows):
                    if stage_of[(flow, other.proc)] < stage_of[(flow, task.proc)]:
                        order[i].add(j)
        order[index[b.desc]].add(index[a.desc])
        return not _has_cycle(order)

    pairs = [
        (a, b) for a in agenda.tasks for b in agenda.tasks
        if a is not b and a.desc not in _dependents(agenda, b) and _ordered(a, b)
    ]
    assume(len(pairs) != 0)
    task_a, task_b = rng.choice(pairs)
    source = 'src/source.txt'
    task_b.inputs.append(source)
    task_b.args['inputs'] = task_b.args['inputs'] + [source]
    test_path = _fixture_path()
    (test_path / 'src').mkdir()
    (test_path / source).write_text('source')
    tdep.store(test_path / 'depend.yaml', {source: [task_a.outputs[0]]})
    success, performed = _run_offline(test_path, agenda, workers)
    if not success: return False
    if performed.index(task_a.desc) > performed.index(task_b.desc): return False

    # A rerun of A reruns B, which reads A's output through the entry
    task_a.args['unused'] = ['changed']
    success, performed = _run_offline(test_path, agenda, workers)
    return success and task_b.desc in performed and task_a.desc in performed


@context(evaluated_agendas, worker_counts)
@prop('Offline evaluation reruns a task changed back to an earlier definition', attempts=_attempts(50))
def _agenda_evaluate_offline_changed_back(
    agenda: ta.Agenda,
    workers: int,
    rng: random.Random
) -> bool:
    # Its outputs were last written by the other definition
    assume(len(agenda.tasks) != 0)
    test_path = _fixture_path()
    if not _run_offline(test_path, agenda, workers)[0]: return False
    task = rng.choice(agenda.tasks)
    task.args['unused'] = ['changed']
    success, performed = _run_offline(test_path, agenda, workers)
    if not success or task.desc not in performed: return False
    del task.args['unused']
    success, performed = _run_offline(test_path, agenda, workers)
    return success and set(performed) == _dependents(agenda, task)


@context(worker_counts)
@prop('Online evaluation builds what a skipped task needs, and the task once it can run', attempts=_attempts(10))
def _agenda_evaluate_online_skipped(workers: int) -> bool:
    script = [sys.executable, str(SUCCESS_SCRIPT), '-i', '$inputs', '-o', '$outputs']
    agenda = ta.Agenda(procs = {'proc': script}, flows = {'flow': [['proc']]})
    for desc, inputs, outputs in [
        ('Task produce', ['src/produce.txt'], ['temp/produced.txt']),
        ('Task consume', ['temp/produced.txt', 'src/later.txt'], ['temp/consumed.txt'])
    ]:
        agenda.tasks.append(ta.Task(
            desc = desc, proc = 'proc', flows = ['flow'],
            args = {'inputs': inputs, 'outputs': outputs},
            inputs = inputs, outputs = outputs
        ))
    test_path = _fixture_path()
    (test_path / 'src').mkdir()
    (test_path / 'src' / 'produce.txt').write_text('produce')
    agenda_path = test_path / 'agenda.yaml'
    ta.store(agenda_path, agenda)
    with _quiet():
        evaluator = tapi.online(
            test_path, agenda_path,
            test_path / 'depend.yaml', test_path / 'tickle.cache', workers
        )
        if evaluator is None: return False
        runner = Thread(target = evaluator.start, daemon = True)
        runner.start()
        try:
            if not _wait_for(
                lambda: (test_path / 'temp' / 'produced.txt').is_file()
            ): return False
            (test_path / 'src' / 'later.txt').write_text('later')
            if not _wait_for(
                lambda: (test_path / 'temp' / 'consumed.txt').is_file()
            ): return False
        finally:
            evaluator.stop()
            runner.join(timeout = 10)
    return not runner.is_alive()


@context(evaluated_agendas)
@prop('Clean reads a cache from before version 2', attempts=_attempts(50))
def _agenda_clean_old_cache(agenda: ta.Agenda) -> bool:
    # Such a cache held absolute paths of generated files and folders, and
    # no file stats; clean removes them but still keeps sources and files
    # outside the project
    assume(len(agenda.tasks) != 0)
    agenda.tasks[0].inputs.append('src/source.txt')
    agenda.tasks[0].args['inputs'] = agenda.tasks[0].args['inputs'] + ['src/source.txt']
    test_path = _fixture_path()
    (test_path / 'src').mkdir()
    source = test_path / 'src' / 'source.txt'
    source.write_text('source')
    if not _offline(test_path, agenda): return False
    outside = _fixture_path() / 'outside.txt'
    outside.write_text('outside')
    outputs = _outputs(test_path, agenda)
    with (test_path / 'tickle.cache').open('wb') as file:
        pickle.dump({
            'files': {str(path) for path in outputs + [source, outside]},
            'folders': {str(test_path / 'temp')}
        }, file)
    if not _clean(test_path): return False
    return (
        not any(path.exists() for path in outputs)
        and not (test_path / 'temp').exists()
        and source.read_text() == 'source'
        and outside.read_text() == 'outside'
        and not (test_path / 'tickle.cache').exists()
    )


@context(g.int_range(1, 3), g.int_range(1, 3))
@prop('Interrupting offline mode exits with 130 and reruns exactly the unfinished tasks', attempts=_attempts(5))
def _cli_interrupt(fast_count: int, slow_count: int) -> bool:
    # Ctrl-C in a terminal signals the whole process group, tasks included
    if os.name == 'nt': return True
    script = [sys.executable, str(SUCCESS_SCRIPT), '-i', '$inputs', '-o', '$outputs']
    slow = [
        sys.executable, '-c',
        'import pathlib, sys, time; '
        'pathlib.Path(sys.argv[1]).write_text("started"); '
        'time.sleep(60 if pathlib.Path("src/slow").exists() else 0); '
        'pathlib.Path(sys.argv[2]).write_text("done")',
        '$marker', '$output'
    ]
    agenda = ta.Agenda(
        procs = {'fast': script, 'slow': slow},
        flows = {'flow': [['fast'], ['slow']]}
    )
    for index in range(fast_count):
        output = 'temp/fast_%d.txt' % index
        agenda.tasks.append(ta.Task(
            desc = 'Task fast %d' % index, proc = 'fast', flows = ['flow'],
            args = {'inputs': [], 'outputs': [output]},
            inputs = [], outputs = [output]
        ))
    for index in range(slow_count):
        output = 'temp/slow_%d.txt' % index
        agenda.tasks.append(ta.Task(
            desc = 'Task slow %d' % index, proc = 'slow', flows = ['flow'],
            args = {'marker': ['temp/started_%d.txt' % index], 'output': [output]},
            inputs = [], outputs = [output]
        ))
    test_path = _fixture_path()
    (test_path / 'src').mkdir()
    (test_path / 'src' / 'slow').write_text('')
    ta.store(test_path / 'agenda.yaml', agenda)
    process = subprocess.Popen(
        [sys.executable, '-c', 'from tickle.cli import main; main()',
         '-w', str(slow_count), 'offline'],
        cwd = test_path, start_new_session = True,
        stdout = subprocess.DEVNULL, stderr = subprocess.DEVNULL
    )
    try:
        if not _wait_for(lambda: all(
            (test_path / 'temp' / ('started_%d.txt' % index)).exists()
            for index in range(slow_count)
        )): return False
        os.killpg(process.pid, signal.SIGINT)
        if process.wait(timeout = 20) != 130: return False
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()
    (test_path / 'src' / 'slow').unlink()
    success, performed = _run_offline(test_path)
    return success and set(performed) == {
        'Task slow %d' % index for index in range(slow_count)
    }

###############################################################################
# Processes and reporting
###############################################################################
@context(worker_counts)
@prop('Offline evaluation gives tasks no stdin to wait on', attempts=_attempts(5))
def _agenda_evaluate_offline_no_stdin(workers: int) -> bool:
    agenda = ta.Agenda(
        procs = {'read': [
            sys.executable, '-c',
            'import pathlib, sys; sys.stdin.read(); '
            'pathlib.Path(sys.argv[1]).write_text("read")',
            '$output'
        ]},
        flows = {'flow': [['read']]},
        tasks = [ta.Task(
            desc = 'Task read', proc = 'read', flows = ['flow'],
            args = {'output': ['out.txt']}, inputs = [], outputs = ['out.txt']
        )]
    )
    test_path = _fixture_path()

    # Give tickle a stdin that never ends, as a terminal or an open pipe is
    results: list[bool] = []
    read_fd, write_fd = os.pipe()
    saved_fd = os.dup(0)
    os.dup2(read_fd, 0)
    try:
        runner = Thread(
            target = lambda: results.append(_run_offline(test_path, agenda, workers)[0]),
            daemon = True
        )
        runner.start()
        runner.join(20)
        finished = not runner.is_alive()
    finally:
        os.dup2(saved_fd, 0)
        os.close(saved_fd)
        os.close(read_fd)
        os.close(write_fd)
    runner.join(20)
    return finished and results == [True]


class _Records(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


@context(g.int_range(0, 1))
@prop('Tickle logs to its own logger and leaves the root logger alone', attempts=_attempts(5))
def _logging_own_logger(missing: int) -> bool:
    # An API user's script configures logging after calling tickle; a call
    # through the root logger would have configured it already
    test_path = _fixture_path()
    if not missing:
        ta.store(test_path / 'agenda.yaml', ta.Agenda(tasks = [ta.Task(
            desc = 'Task without outputs', proc = 'proc', flows = [],
            args = {}, inputs = [], outputs = []
        )], procs = {'proc': ['true']}))
    root = logging.getLogger()
    handlers = list(root.handlers)
    records = _Records()
    root.addHandler(records)
    try:
        with redirect_stdout(io.StringIO()):
            tapi.offline(
                test_path,
                test_path / 'agenda.yaml',
                test_path / 'depend.yaml',
                test_path / 'tickle.cache'
            )
    finally:
        root.removeHandler(records)
    if root.handlers != handlers: return False
    return len(records.records) != 0 and all(
        record.name == 'tickle' for record in records.records
    )


@context(g.int_range(0, 3), g.one_of(['offline', 'clean']))
@prop('The CLI reports bad cache and log paths without a traceback', attempts=_attempts(8))
def _cli_bad_cache_path(kind: int, mode: str) -> bool:
    test_path = _fixture_path()
    ta.store(test_path / 'agenda.yaml', ta.Agenda())
    (test_path / 'folder').mkdir()
    option, value = [
        ('-c', str(Path('/', 'elsewhere', 'tickle.cache'))),
        ('-c', 'missing/tickle.cache'),
        ('-c', 'folder'),
        ('-l', 'folder')
    ][kind]
    result = subprocess.run(
        [sys.executable, '-c', 'from tickle.cli import main; main()',
         option, value, mode],
        cwd = test_path, capture_output = True, text = True
    )
    return result.returncode != 0 and 'Traceback' not in result.stderr

###############################################################################
# Stopping, failures and paths
###############################################################################
@context(g.int_range(1, 3), g.int_range(2, 4))
@prop('Stopping the evaluator starts no more tasks of the running batch', attempts=_attempts(10))
def _evaluator_stop_starts_nothing(workers: int, factor: int) -> bool:
    started: list[int] = []
    lock = Lock()

    def _work(index: int) -> None:
        with lock: started.append(index)
        time.sleep(0.3)

    evaluator = tev.Evaluator(workers)
    tasks = [
        tev.Task({'flow': 0}, (lambda index = index: _work(index)))
        for index in range(workers * factor)
    ]
    for task in tasks:
        task.set_valid(False)
        task.set_active(True)
    evaluator.pause()
    evaluator.reprogram(tgraph.compile(tasks))
    evaluator.resume()
    runner = Thread(target = evaluator.start, daemon = True)
    runner.start()
    try:
        if not _wait_for(lambda: len(started) != 0, 5.0): return False
        time.sleep(0.05)
        evaluator.stop()
        runner.join(timeout = 10)
    finally:
        evaluator.stop()
    return not runner.is_alive() and len(started) <= workers


@prop('Stopping the evaluator before it starts ends it at once', attempts=_attempts(5))
def _evaluator_stop_before_start() -> bool:
    evaluator = tev.Evaluator(2)
    evaluator.stop()
    runner = Thread(target = evaluator.start, daemon = True)
    runner.start()
    runner.join(timeout = 5)
    return not runner.is_alive()


@context(g.int_range(1, 4))
@prop('A failing task is reported while the rest of its batch still runs', attempts=_attempts(10))
def _evaluator_reports_failure_at_once(slow_count: int) -> bool:
    finished: list[int] = []
    reported_while: list[int] = []

    class _Evaluator(tev.Evaluator):
        def on_task_failed(self, error: tev.TaskError) -> None:
            reported_while.append(len(finished))

        def on_task_error(self, error: tev.TaskError) -> None:
            pass

    def _slow(index: int) -> None:
        time.sleep(0.5)
        finished.append(index)

    def _fail() -> None:
        raise tev.TaskError('Failure', 'failed')

    tasks = [
        tev.Task({'flow': 0}, (lambda index = index: _slow(index)))
        for index in range(slow_count)
    ] + [tev.Task({'flow': 0}, _fail)]
    for task in tasks:
        task.set_valid(False)
        task.set_active(True)
    evaluator = _Evaluator(slow_count + 1)
    evaluator.pause()
    evaluator.reprogram(tgraph.compile(tasks))
    evaluator.resume()
    runner = Thread(target = evaluator.start, daemon = True)
    runner.start()
    try:
        if not _wait_for(lambda: len(finished) == slow_count, 5.0): return False
    finally:
        evaluator.stop()
        runner.join(timeout = 10)
    return reported_while == [0]


@context(any_agendas, g.int_range(0, 20))
@prop('The stage check rejects a cycle with the stage order whatever is up to date', attempts=1000)
def _graph_check_stages(
    agenda: ta.Agenda,
    extra_count: int,
    rng: random.Random
) -> bool:
    compiled, tasks = _task_graph(agenda)
    assume(len(tasks) >= 2)
    for _ in range(extra_count):
        dst, src = rng.sample(tasks, 2)
        if dst in _upstream(src): continue
        try: dst.add_dependency(src)
        except RuntimeError: continue
    for task in tasks:
        task.set_valid(rng.choice([True, False]))
        task.set_active(rng.random() < 0.8)
    order: dict[int, set[int]] = {index: set() for index in range(len(tasks))}
    for index, task in enumerate(tasks):
        order[index].update(tasks.index(dep) for dep in task.get_deps())
        for other, other_data in enumerate(compiled):
            for flow, stage in compiled[index].flows.items():
                if other_data.flows.get(flow, stage) < stage: order[index].add(other)
    try: tgraph.check_stages(tasks)
    except RuntimeError: return _has_cycle(order)
    return not _has_cycle(order)


@context(worker_counts)
@prop('Offline evaluation rejects a cycle with the stage order also when every task is up to date', attempts=_attempts(5))
def _agenda_evaluate_offline_stage_cycle_cached(workers: int) -> bool:
    # A reads D's output and C reads B's; with A before B in one flow and C
    # before D in another, no order satisfies both
    script = [sys.executable, str(SUCCESS_SCRIPT), '-i', '$inputs', '-o', '$outputs']
    agenda = ta.Agenda(
        procs = {name: script for name in ['pa', 'pb', 'pc', 'pd']},
        flows = {'f1': [['pa', 'pb']], 'f2': [['pc', 'pd']]}
    )
    for desc, proc, flow, inputs, output in [
        ('Task A', 'pa', 'f1', ['temp/d.txt'], 'temp/a.txt'),
        ('Task B', 'pb', 'f1', [], 'temp/b.txt'),
        ('Task C', 'pc', 'f2', ['temp/b.txt'], 'temp/c.txt'),
        ('Task D', 'pd', 'f2', [], 'temp/d.txt')
    ]:
        agenda.tasks.append(ta.Task(
            desc = desc, proc = proc, flows = [flow],
            args = {'inputs': inputs, 'outputs': [output]},
            inputs = inputs, outputs = [output]
        ))
    test_path = _fixture_path()
    if not _run_offline(test_path, agenda, workers)[0]: return False

    # Flows do not change what a task is, so nothing is out of date
    agenda.flows = {'f1': [['pa'], ['pb']], 'f2': [['pc'], ['pd']]}
    success, performed = _run_offline(test_path, agenda, workers)
    return not success and len(performed) == 0


@context(evaluated_agendas, worker_counts)
@prop('Offline evaluation after writing paths absolute runs no tasks', attempts=_attempts(30))
def _agenda_evaluate_offline_absolute_paths(agenda: ta.Agenda, workers: int) -> bool:
    test_path = _fixture_path()
    if not _run_offline(test_path, agenda, workers)[0]: return False
    for task in agenda.tasks:
        task.inputs = [str(test_path / _norm(path)) for path in task.inputs]
        task.outputs = [str(test_path / _norm(path)) for path in task.outputs]
    success, performed = _run_offline(test_path, agenda, workers)
    return success and len(performed) == 0


@context(g.int_range(0, 12))
@prop('A succeeding task prints only the end of its stderr and logs all of it', attempts=_attempts(5))
def _agenda_evaluate_offline_stderr_tail(line_count: int) -> bool:
    agenda = ta.Agenda(
        procs = {'noisy': [
            sys.executable, '-c',
            'import pathlib, sys\n'
            'for i in range(int(sys.argv[2])): print("line %d" % i, file = sys.stderr)\n'
            'pathlib.Path(sys.argv[1]).write_text("done")',
            '$output', '$count'
        ]},
        flows = {'flow': [['noisy']]},
        tasks = [ta.Task(
            desc = 'Task noisy', proc = 'noisy', flows = ['flow'],
            args = {'output': ['out.txt'], 'count': [str(line_count)]},
            inputs = [], outputs = ['out.txt']
        )]
    )
    test_path = _fixture_path()
    ta.store(test_path / 'agenda.yaml', agenda)
    records = _Records()
    logger = logging.getLogger('tickle')
    logger.addHandler(records)
    output = io.StringIO()
    try:
        with redirect_stdout(output):
            success = tapi.offline(
                test_path, test_path / 'agenda.yaml',
                test_path / 'depend.yaml', test_path / 'tickle.cache', 1
            )
    finally:
        logger.removeHandler(records)
    printed = [
        line for line in output.getvalue().splitlines()
        if line.startswith('line ')
    ]
    logged = '\n'.join(record.getMessage() for record in records.records)
    expected = ['line %d' % i for i in range(line_count)]
    return (
        success
        and printed == expected[-5:]
        and all(line in logged for line in expected)
    )

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
    _agenda_evaluate_offline_rerun_failed,
    _agenda_evaluate_offline_edit_during_run,
    _agenda_evaluate_offline_matches_clean_build,
    _agenda_evaluate_offline_relocate,
    _agenda_evaluate_offline_depend_order,
    _program_order_respected,
    _evaluator_error_keeps_next_program,
    _agenda_evaluate_online,
    _agenda_clean,
    _agenda_clean_safe,
    _program_runs_runnable_tasks,
    _program_compile_wide,
    _agenda_rejects_empty_flow,
    _agenda_evaluate_offline_spellings,
    _agenda_evaluate_offline_output_spellings,
    _agenda_evaluate_offline_flows_identity,
    _agenda_evaluate_offline_no_stdin,
    _logging_own_logger,
    _cli_bad_cache_path,
    _program_batches_earliest,
    _program_order_extra_deps,
    _evaluator_reprogram_stress,
    _agenda_decode_error_location,
    _agenda_evaluate_offline_depend_generated,
    _agenda_evaluate_offline_changed_back,
    _agenda_evaluate_online_skipped,
    _agenda_clean_old_cache,
    _cli_interrupt,
    _evaluator_stop_starts_nothing,
    _evaluator_stop_before_start,
    _evaluator_reports_failure_at_once,
    _graph_check_stages,
    _agenda_evaluate_offline_stage_cycle_cached,
    _agenda_evaluate_offline_absolute_paths,
    _agenda_evaluate_offline_stderr_tail,
)
