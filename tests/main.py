# External module dependencies
from collections.abc import Iterator
from contextlib import contextmanager, redirect_stdout
import importlib.util
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
import tickle.process as tproc
import tickle.schedule as tsched

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

# Most properties that evaluate agendas run the success script in this
# process instead of starting one per task, which is much faster; setting
# TICKLE_TEST_PROCESSES=1 makes them all start real processes, as the
# nightly CI run does.
PROCESSES = os.environ.get('TICKLE_TEST_PROCESSES', '0') == '1'

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


def _load_success():
    spec = importlib.util.spec_from_file_location('_success', SUCCESS_SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

_success = _load_success()
_RealProcess = tproc.Process


class _InProcess:
    """Stands in for tickle.process.Process: runs the success script in
    this process, and any other command as a real process."""

    def __init__(self, command: list[str], cwd: Path, log_path: Path) -> None:
        self._real: tproc.Process | None = None
        if command[:2] != [sys.executable, str(SUCCESS_SCRIPT)]:
            self._real = _RealProcess(command, cwd, log_path)
            return
        start = time.monotonic()
        with log_path.open('w') as log_file:
            try: self._code = _success.run(command[2:], cwd, log_file)
            except BaseException as error:
                log_file.write('%s: %s\n' % (type(error).__name__, error))
                self._code = 2
        self._duration = time.monotonic() - start

    def wait(self, timeout: float = 0.0) -> tproc.Outcome:
        if self._real is not None: return self._real.wait(timeout)
        return tproc.Outcome(self._code, self._duration)

    def stop(self) -> None:
        if self._real is not None: self._real.stop()


@contextmanager
def _executor(processes: bool) -> Iterator[None]:
    """Run the success script in-process inside the block, unless real
    processes are asked for."""
    if processes or PROCESSES:
        yield
        return
    saved = tapi.Process
    tapi.Process = _InProcess  # type: ignore
    try: yield
    finally: tapi.Process = saved


def _offline_result(
    test_path: Path,
    agenda: ta.Agenda | None = None,
    workers: int | None = None,
    processes: bool = False,
    **options
) -> tuple[tapi.Result, list[str], str]:
    """Run offline mode, storing the agenda first if one is given; returns
    the result, the descriptions of the tasks it ran in the order they
    started, and what it printed."""
    agenda_path = test_path / 'agenda.yaml'
    if agenda is not None: ta.store(agenda_path, agenda)
    output = io.StringIO()
    logging.disable(logging.CRITICAL)
    try:
        with redirect_stdout(output), _executor(processes):
            result = tapi.offline(
                test_path,
                agenda_path,
                test_path / 'depend.yaml',
                test_path / 'tickle.cache',
                tapi.default_worker_count() if workers is None else workers,
                **options
            )
    finally:
        logging.disable(logging.NOTSET)
    performed = [
        line.removeprefix('[tickle] ')
        for line in output.getvalue().splitlines()
        if line.startswith('[tickle] Task ')
    ]
    return result, performed, output.getvalue()


def _run_offline(
    test_path: Path,
    agenda: ta.Agenda | None = None,
    workers: int | None = None,
    processes: bool = False,
    **options
) -> tuple[bool, list[str]]:
    """Run offline mode; returns whether it succeeded and the tasks it
    ran, in the order they started."""
    result, performed, _ = _offline_result(
        test_path, agenda, workers, processes, **options
    )
    return bool(result), performed


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
    # Real processes, as everything after the first run is in-process
    test_path = _fixture_path()
    if not _run_offline(test_path, agenda, processes = True)[0]: return False
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
@prop('Offline evaluation reruns a task with a missing output, and not the tasks after it', attempts=_attempts(100))
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
    # It writes what it wrote before, so the tasks after it are cut off
    return success and performed == [task.desc]


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
        expected = set().union(*(_dependents(agenda, other) for other in changed))
    else:
        # An argument the command does not use: the task reruns, writes
        # what it wrote before, and the tasks after it are cut off
        task.args['unused'] = ['changed']
        expected = {task.desc}
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
        tev.Task(task.flows, lambda run: None, description = task.description)
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
        current = [tev.Task({'flow': stage}, lambda run: None) for _ in range(width)]
        for task, dep in zip(current, previous, strict = False):
            task.add_dependency(dep)
        tasks += current
        previous = current
    for task in tasks:
        task.set_valid(False)
        task.set_active(True)
    return tasks








def _random_graph(
    agenda: ta.Agenda,
    extra_count: int,
    rng: random.Random
) -> tuple[list[ta.CompiledTask], list[tev.Task]]:
    """The agenda's task graph with some extra dependencies, as depend
    entries add, also between tasks whose stages in other flows disagree."""
    compiled, tasks = _task_graph(agenda)
    if len(tasks) < 2: return compiled, tasks
    for _ in range(extra_count):
        dst, src = rng.sample(tasks, 2)
        if dst in _upstream(src): continue
        try: dst.add_dependency(src)
        except RuntimeError: continue
    return compiled, tasks


def _stage_order(
    compiled: list[ta.CompiledTask],
    tasks: list[tev.Task]
) -> dict[int, set[int]]:
    """For each task, the tasks that must finish before it starts: its
    dependencies and the tasks of earlier stages in the flows it shares."""
    order: dict[int, set[int]] = {index: set() for index in range(len(tasks))}
    position = {task: index for index, task in enumerate(tasks)}
    for index, task in enumerate(tasks):
        order[index].update(position[dep] for dep in task.get_deps())
        for other, other_data in enumerate(compiled):
            for flow, stage in compiled[index].flows.items():
                if other_data.flows.get(flow, stage) < stage: order[index].add(other)
    return order


def _simulate(
    schedule: tsched.Schedule,
    rng: random.Random,
    fail: set[tev.Task] | None = None,
    fail_times: dict[tev.Task, int] | None = None
) -> list[tuple[str, tev.Task]]:
    """Drive a schedule as workers would, finishing running tasks in a
    random order; fail holds tasks that always fail, and fail_times how
    often other tasks fail before they succeed. Returns the start and
    finish events, and checks the worker and pool limits all along."""
    events: list[tuple[str, tev.Task]] = []
    running: list[tev.Task] = []
    failures: dict[tev.Task, int] = {}
    while True:
        while True:
            task = schedule.next()
            if task is None: break
            running.append(task)
            events.append(('start', task))
        if len(running) > schedule._worker_count:
            raise AssertionError('more tasks running than workers')
        for pool, depth in schedule._pools.items():
            if sum(1 for task in running if task.get_pool() == pool) > depth:
                raise AssertionError('more tasks of pool %s than it allows' % pool)
        if len(running) == 0: break
        task = running.pop(rng.randrange(len(running)))
        failed = task in (fail or set())
        if not failed and fail_times is not None:
            failures[task] = failures.get(task, 0) + 1
            failed = failures[task] <= fail_times.get(task, 0)
        schedule.finish(task, not failed)
        events.append(('fail' if failed else 'done', task))
    return events


@context(any_agendas, g.int_range(0, 20), g.int_range(1, 4))
@prop('Schedules start every task to run once, after its dependencies and earlier stages', attempts=1000)
def _schedule_order(
    agenda: ta.Agenda,
    extra_count: int,
    workers: int,
    rng: random.Random
) -> bool:
    compiled, tasks = _random_graph(agenda, extra_count, rng)
    for task in tasks:
        task.set_valid(rng.choice([True, False]))
        task.set_active(rng.random() < 0.8)
    tgraph.propagate(tasks)
    to_run = [task for task in tasks if not task.get_valid() and task.get_active()]
    position = {task: index for index, task in enumerate(tasks)}

    # Dependencies and stage order together may form a cycle, which no
    # schedule can follow; it must then refuse rather than drop tasks
    order = _stage_order(compiled, tasks)
    members = {position[task] for task in to_run}
    cyclic = _has_cycle({
        index: order[index] & members for index in members
    })
    try: schedule = tsched.Schedule(tasks, workers)
    except RuntimeError: return cyclic
    if cyclic: return False

    events = _simulate(schedule, rng)
    started = [task for kind, task in events if kind == 'start']
    if sorted(map(id, started)) != sorted(map(id, to_run)): return False
    finished: set[int] = set()
    for kind, task in events:
        if kind == 'start':
            index = position[task]
            if not (order[index] & members) <= finished: return False
        else:
            finished.add(position[task])
    return schedule.finished()


@context(any_agendas, g.int_range(1, 6), g.int_range(1, 3), g.int_range(1, 3))
@prop('Schedules keep the worker count and the depth of every pool', attempts=500)
def _schedule_pools(
    agenda: ta.Agenda,
    workers: int,
    pool_count: int,
    depth: int,
    rng: random.Random
) -> bool:
    pools = {'pool_%d' % index: rng.randint(1, depth) for index in range(pool_count)}
    compiled = ta.compile(Path('/target'), agenda)
    tasks = [
        tev.Task(
            task.flows, lambda run: None, description = task.description,
            pool = rng.choice([''] + sorted(pools))
        )
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
    for task in tasks:
        task.set_valid(False)
        task.set_active(True)
    schedule = tsched.Schedule(tasks, workers, pools)
    try: events = _simulate(schedule, rng)
    except AssertionError: return False
    return (
        len([kind for kind, _ in events if kind == 'start']) == len(tasks)
        and schedule.finished()
    )


@context(any_agendas, g.int_range(1, 4))
@prop('With keep going, a failure blocks exactly the tasks that read from it', attempts=500)
def _schedule_keep_going(
    agenda: ta.Agenda,
    workers: int,
    rng: random.Random
) -> bool:
    compiled, tasks = _task_graph(agenda)
    assume(len(tasks) != 0)
    for task in tasks:
        task.set_valid(False)
        task.set_active(True)
    failing = set(rng.sample(tasks, rng.randint(1, min(3, len(tasks)))))
    schedule = tsched.Schedule(tasks, workers, keep_going = True)
    events = _simulate(schedule, rng, fail = failing)

    # Downstream of a failure through dependencies, not through stages; a
    # failing task downstream of another never runs, so it is blocked too
    blocked: set[tev.Task] = set()
    worklist = list(failing)
    while len(worklist) != 0:
        for ref in worklist.pop().get_refs():
            if ref in blocked: continue
            blocked.add(ref)
            worklist.append(ref)
    started = {task for kind, task in events if kind == 'start'}
    for task in tasks:
        state = schedule.state[task]
        if task in blocked:
            if task in started or state != tsched.BLOCKED: return False
        elif task in failing:
            if state != tsched.FAILED: return False
        elif state != tsched.DONE:
            return False
    return schedule.finished()


@context(any_agendas, g.int_range(1, 4))
@prop('Without keep going, nothing starts after a failure', attempts=500)
def _schedule_stop_on_failure(
    agenda: ta.Agenda,
    workers: int,
    rng: random.Random
) -> bool:
    _, tasks = _task_graph(agenda)
    assume(len(tasks) != 0)
    for task in tasks:
        task.set_valid(False)
        task.set_active(True)
    failing = rng.choice(tasks)
    schedule = tsched.Schedule(tasks, workers)
    events = _simulate(schedule, rng, fail = {failing})
    kinds = [kind for kind, _ in events]
    if 'fail' not in kinds: return False
    first_failure = kinds.index('fail')
    return 'start' not in kinds[first_failure:] and schedule.finished()


@context(g.int_range(0, 3), g.int_range(0, 4), g.int_range(1, 3))
@prop('Schedules run a failing task again as often as its retries allow', attempts=200)
def _schedule_retries(
    retries: int,
    failures: int,
    workers: int,
    rng: random.Random
) -> bool:
    task = tev.Task({}, lambda run: None, description = 'Task', retries = retries)
    after = tev.Task({}, lambda run: None, description = 'After')
    after.add_dependency(task)
    for item in [task, after]:
        item.set_valid(False)
        item.set_active(True)
    schedule = tsched.Schedule([task, after], workers)
    events = _simulate(schedule, rng, fail_times = {task: failures})
    starts = [item for kind, item in events if kind == 'start' and item is task]
    if failures <= retries:
        return (
            len(starts) == failures + 1
            and schedule.state[task] == tsched.DONE
            and schedule.state[after] == tsched.DONE
        )
    return (
        len(starts) == retries + 1
        and schedule.state[task] == tsched.FAILED
        and after not in {item for kind, item in events if kind == 'start'}
    )


@context(g.int_range(2, 6), g.int_range(1, 5))
@prop('One worker starts the ready task with the longest chain of durations first', attempts=500)
def _schedule_priority(
    chain_count: int,
    max_length: int,
    rng: random.Random
) -> bool:
    # Independent chains of tasks with random durations; with one worker
    # the first task to start heads the chain with the most work left
    tasks: list[tev.Task] = []
    totals: dict[tev.Task, float] = {}
    for chain in range(chain_count):
        previous: tev.Task | None = None
        chain_tasks: list[tev.Task] = []
        for index in range(rng.randint(1, max_length)):
            task = tev.Task(
                {}, lambda run: None,
                description = 'Task %d.%d' % (chain, index),
                weight = rng.choice([0.5, 1.0, 2.0, 7.0])
            )
            if previous is not None: task.add_dependency(previous)
            previous = task
            chain_tasks.append(task)
        total = sum(task.get_weight() for task in chain_tasks)
        totals[chain_tasks[0]] = total
        tasks += chain_tasks
    for task in tasks:
        task.set_valid(False)
        task.set_active(True)
    first = tsched.Schedule(tasks, 1).next()
    return first is not None and totals.get(first) == max(totals.values())


def _scale_time(width: int) -> tuple[float, bool]:
    """The best of three times to propagate and schedule a wide graph, and
    whether the schedule holds every task."""
    best = float('inf')
    complete = True
    for _ in range(3):
        tasks = _wide_graph(width, 2)
        start = time.monotonic()
        tgraph.propagate(tasks)
        schedule = tsched.Schedule(tasks, 8)
        best = min(best, time.monotonic() - start)
        complete = complete and len(schedule.tasks) == len(tasks)
    return best, complete


@context(g.int_range(8000, 9000))
@prop('Scheduling a wide graph takes time linear in its size', attempts=3)
def _schedule_scale(width: int) -> bool:
    # Batches used to be joined by an edge from every task of one batch to
    # every task of the next, so two stages of 4000 tasks took minutes and
    # gigabytes; and the cycle check was quadratic in the dependencies of
    # one task. Comparing four times the tasks keeps the bound independent
    # of the machine.
    small, small_complete = _scale_time(width)
    large, large_complete = _scale_time(4 * width)
    return small_complete and large_complete and large < 7 * max(small, 0.01)


###############################################################################
# The evaluator, with work that starts no processes
###############################################################################
def _install(evaluator: tev.Evaluator, tasks: list[tev.Task], keep=None) -> None:
    for task in tasks:
        task.set_valid(False)
        task.set_active(True)
    evaluator.install(evaluator.make_schedule(tasks), keep)


def _sleep(run: tev.Run, seconds: float) -> None:
    """Sleep as a task would run, ending early when the run is cancelled."""
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline and not run.cancelled():
        time.sleep(0.01)


@contextmanager
def _running(evaluator: tev.Evaluator) -> Iterator[Thread]:
    runner = Thread(target = evaluator.start, daemon = True)
    runner.start()
    try: yield runner
    finally:
        evaluator.stop(cancel = True)
        runner.join(timeout = 10)


@context(g.int_range(1, 3))
@prop('A task starts as soon as what it waits for has finished', attempts=_attempts(10))
def _evaluator_starts_when_ready(chain_length: int) -> bool:
    # A slow task next to a chain of fast ones: the chain must not wait
    # for the slow task, as it did with batches
    done: list[str] = []
    slow = tev.Task({}, lambda run: (_sleep(run, 1.0), done.append('slow')), description = 'slow')
    chain: list[tev.Task] = []
    for index in range(chain_length):
        task = tev.Task(
            {}, (lambda run, index = index: done.append('chain %d' % index)),
            description = 'chain %d' % index
        )
        if len(chain) != 0: task.add_dependency(chain[-1])
        chain.append(task)
    evaluator = tev.Evaluator(2)
    _install(evaluator, [slow] + chain)
    with _running(evaluator) as runner:
        if not _wait_for(lambda: len(done) == chain_length + 1, 5.0): return False
    return not runner.is_alive() and done[-1] == 'slow'


@context(g.int_range(1, 3), g.int_range(2, 4))
@prop('Stopping the evaluator starts no more tasks', attempts=_attempts(10))
def _evaluator_stop_starts_nothing(workers: int, factor: int) -> bool:
    started: list[int] = []
    lock = Lock()

    def _work(run: tev.Run, index: int) -> None:
        with lock: started.append(index)
        _sleep(run, 0.3)

    evaluator = tev.Evaluator(workers)
    _install(evaluator, [
        tev.Task({'flow': 0}, (lambda run, index = index: _work(run, index)))
        for index in range(workers * factor)
    ])
    runner = Thread(target = evaluator.start, daemon = True)
    runner.start()
    if not _wait_for(lambda: len(started) != 0, 5.0): return False
    time.sleep(0.05)
    evaluator.stop()
    runner.join(timeout = 10)
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
@prop('A failing task is reported while the tasks next to it still run', attempts=_attempts(10))
def _evaluator_reports_failure_at_once(slow_count: int) -> bool:
    finished: list[int] = []
    reported_while: list[int] = []

    class _Evaluator(tev.Evaluator):
        def on_task_failed(self, error: tev.TaskError, retrying: bool) -> None:
            reported_while.append(len(finished))

    def _fail(run: tev.Run) -> None:
        raise tev.TaskError('Failure', 'failed')

    evaluator = _Evaluator(slow_count + 1, keep_going = True)
    _install(evaluator, [
        tev.Task({}, (lambda run, index = index: (_sleep(run, 0.5), finished.append(index))))
        for index in range(slow_count)
    ] + [tev.Task({}, _fail)])
    with _running(evaluator):
        if not _wait_for(lambda: len(finished) == slow_count, 5.0): return False
    return reported_while == [0]


@context(g.int_range(1, 4), g.int_range(1, 6), worker_counts, g.int_range(1, 8))
@prop('The evaluator runs each new schedule once and starts nothing of the schedules it replaced', attempts=_attempts(30))
def _evaluator_reschedule_stress(
    program_count: int,
    task_count: int,
    workers: int,
    error_rate: int,
    seed: int
) -> bool:
    # A watcher thread installs schedules while tasks run and fail, as file
    # saves do in online mode; every schedule but the last may fail. The
    # tasks have no keys, so a new schedule cancels the running ones.
    rng = random.Random(seed)
    plan = [
        [
            (program != program_count - 1 and rng.randrange(error_rate) == 0,
             rng.random() * 0.005, [i for i in range(index) if rng.random() < 0.3])
            for index in range(task_count)
        ]
        for program in range(program_count)
    ]
    pauses = [rng.random() * 0.01 for _ in range(program_count)]
    events: list[tuple[str, int, int]] = []
    lock = Lock()

    def _work(run: tev.Run, program: int, index: int) -> None:
        fail, delay, _ = plan[program][index]
        with lock: events.append(('start', program, index))
        _sleep(run, delay)
        if fail: raise tev.TaskError('Task %d.%d' % (program, index), 'failed')
        with lock: events.append(('done', program, index))

    evaluator = tev.Evaluator(workers, keep_going = True)
    with _running(evaluator) as runner:
        for program in range(program_count):
            tasks: list[tev.Task] = []
            for index in range(task_count):
                task = tev.Task({'flow': 0}, (
                    lambda run, p = program, i = index: _work(run, p, i)
                ))
                for dep in plan[program][index][2]: task.add_dependency(tasks[dep])
                tasks.append(task)
            with lock:
                _install(evaluator, tasks)
                events.append(('installed', program, -1))
            time.sleep(pauses[program])
        if not _wait_for(lambda: sum(
            1 for kind, program, _ in events
            if kind == 'done' and program == program_count - 1
        ) >= task_count, 10.0): return False
        time.sleep(0.05)
    if runner.is_alive(): return False

    installed = -1
    final_starts: list[int] = []
    for kind, program, index in events:
        if kind == 'installed': installed = program
        elif kind == 'start':
            if program < installed: return False
            if program == program_count - 1: final_starts.append(index)
    return sorted(final_starts) == list(range(task_count))


@context(g.int_range(1, 3))
@prop('A running task that a new schedule would start carries on in it, and one it would not is cancelled', attempts=_attempts(10))
def _evaluator_adopts_running(dependent_count: int) -> bool:
    starts: list[str] = []
    cancelled: list[str] = []
    lock = Lock()

    def _work(run: tev.Run, name: str, seconds: float) -> None:
        with lock: starts.append(name)
        run.on_cancel(lambda: cancelled.append(name))
        _sleep(run, seconds)

    def _graph() -> tuple[tev.Task, tev.Task, list[tev.Task]]:
        kept = tev.Task({}, lambda run: _work(run, 'kept', 0.6), key = 'kept')
        dropped = tev.Task({}, lambda run: _work(run, 'dropped', 5.0), key = 'dropped')
        after = [
            tev.Task({}, (lambda run, index = index: _work(run, 'after %d' % index, 0.0)))
            for index in range(dependent_count)
        ]
        for task in after: task.add_dependency(kept)
        return kept, dropped, after

    evaluator = tev.Evaluator(4)
    kept, dropped, after = _graph()
    _install(evaluator, [kept, dropped] + after)
    with _running(evaluator):
        if not _wait_for(lambda: {'kept', 'dropped'} <= set(starts), 5.0): return False

        # The new schedule has the kept task, and the dropped one only up to
        # date; keep accepts the kept task's run
        kept, dropped, after = _graph()
        dropped.set_valid(True)
        tasks = [kept] + after
        for task in tasks:
            task.set_valid(False)
            task.set_active(True)
        dropped.set_active(True)
        evaluator.install(
            evaluator.make_schedule([kept, dropped] + after),
            lambda run, task: task.key == 'kept'
        )
        if not _wait_for(lambda: len(starts) == 2 + dependent_count, 5.0): return False
        time.sleep(0.1)
    return (
        starts.count('kept') == 1
        and cancelled == ['dropped']
        and starts.count('dropped') == 1
    )


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
    _add_trigger(agenda, task_a, 'src/a_trigger.txt')
    source = 'src/source.txt'
    task_b.inputs.append(source)
    task_b.args['inputs'] = task_b.args['inputs'] + [source]
    test_path = _fixture_path()
    (test_path / 'src').mkdir()
    (test_path / source).write_text('source')
    (test_path / 'src' / 'a_trigger.txt').write_text('first')
    tdep.store(test_path / 'depend.yaml', {source: [task_a.outputs[0]]})
    success, performed = _run_offline(test_path, agenda, workers)
    if not success: return False
    if performed.index(task_a.desc) > performed.index(task_b.desc): return False

    # A new output of A reruns B, which reads it through the entry
    (test_path / 'src' / 'a_trigger.txt').write_text('second')
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
    return success and performed == [task.desc]


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
# Stage checks and paths
###############################################################################

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



###############################################################################
# Pipeline features
###############################################################################
def _script_task(desc: str, code: str, args: dict[str, list[str]], outputs: list[str], **fields) -> ta.Task:
    """A task running a short Python program with its args as words."""
    return ta.Task(
        desc = desc, proc = desc, flows = [], args = args,
        inputs = fields.pop('inputs', []), outputs = outputs, **fields
    )


def _script_agenda(tasks: list[tuple[ta.Task, str, list[str]]], **fields) -> ta.Agenda:
    agenda = ta.Agenda(**fields)
    for task, code, params in tasks:
        agenda.procs[task.proc] = [sys.executable, '-c', code] + ['$' + p for p in params]
        agenda.tasks.append(task)
    return agenda


@context(evaluated_agendas, worker_counts)
@prop('With keep going, offline mode runs everything but what reads from a failed task', attempts=_attempts(100))
def _agenda_evaluate_offline_keep_going(
    agenda: ta.Agenda,
    workers: int,
    rng: random.Random
) -> bool:
    assume(len(agenda.tasks) != 0)
    task = rng.choice(agenda.tasks)
    _add_trigger(agenda, task, 'src/trigger.txt')
    test_path = _fixture_path()
    (test_path / 'src').mkdir()
    (test_path / 'src' / 'trigger.txt').write_text('fail')
    result, performed, _ = _offline_result(test_path, agenda, workers, keep_going = True)
    downstream = _dependents(agenda, task) - {task.desc}
    everyone = {other.desc for other in agenda.tasks}
    return (
        not result
        and result.failed == [task.desc]
        and set(result.blocked) == downstream
        and set(performed) == everyone - downstream
        and set(result.succeeded) == everyone - downstream - {task.desc}
    )


@context(g.int_range(2, 5), g.int_range(1, 2))
@prop('A pool runs no more of its tasks at once than its depth', attempts=_attempts(5))
def _agenda_evaluate_offline_pools(task_count: int, depth: int) -> bool:
    code = (
        'import sys, time, pathlib\n'
        'start = time.time(); time.sleep(0.2)\n'
        'pathlib.Path(sys.argv[1]).write_text("%r %r" % (start, time.time()))'
    )
    tasks = []
    for index in range(task_count):
        task = _script_task(
            'Task %d' % index, code, {'output': ['out/%d.txt' % index]},
            ['out/%d.txt' % index], pool = 'render'
        )
        task.proc = 'proc'
        tasks.append(task)
    agenda = ta.Agenda(
        procs = {'proc': [sys.executable, '-c', code, '$output']},
        tasks = tasks, pools = {'render': depth}
    )
    test_path = _fixture_path()
    (test_path / 'out').mkdir()
    if not _run_offline(test_path, agenda, 8, processes = True)[0]: return False
    spans = sorted(
        tuple(map(float, (test_path / 'out' / ('%d.txt' % index)).read_text().split()))
        for index in range(task_count)
    )
    for start, _ in spans:
        overlapping = sum(1 for other_start, other_end in spans if other_start <= start < other_end)
        if overlapping > depth: return False
    return True


def _alive(pid: int) -> bool:
    if os.name == 'nt':
        result = subprocess.run(
            ['tasklist', '/FI', 'PID eq %d' % pid], capture_output = True, text = True
        )
        return str(pid) in result.stdout
    try: os.kill(pid, 0)
    except ProcessLookupError: return False
    except PermissionError: return True
    return True


# Starts a child process that sleeps, writes the child's pid, then sleeps
_SLEEPER = (
    'import subprocess, sys, time, pathlib\n'
    'child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])\n'
    'pathlib.Path(sys.argv[1]).write_text(str(child.pid))\n'
    'time.sleep(60)'
)


@prop('A task that runs past its timeout fails, and its processes are stopped', attempts=_attempts(3))
def _agenda_evaluate_offline_timeout() -> bool:
    task = _script_task('Task slow', _SLEEPER, {'pid': ['pid.txt'], 'output': ['out.txt']}, ['out.txt'], timeout = 1)
    agenda = _script_agenda([(task, _SLEEPER, ['pid', 'output'])])
    test_path = _fixture_path()
    start = time.monotonic()
    result, _, printed = _offline_result(test_path, agenda, 1, processes = True)
    if result or result.failed != ['Task slow']: return False
    if 'timed out' not in printed or time.monotonic() - start > 20: return False
    pid = int((test_path / 'pid.txt').read_text())
    return _wait_for(lambda: not _alive(pid), 15.0)


@context(g.int_range(0, 3), g.int_range(0, 2))
@prop('A failing task runs again as often as its retries allow', attempts=_attempts(5))
def _agenda_evaluate_offline_retries(failures: int, retries: int) -> bool:
    code = (
        'import sys, pathlib\n'
        'count = pathlib.Path(sys.argv[1])\n'
        'n = int(count.read_text()) if count.exists() else 0\n'
        'count.write_text(str(n + 1))\n'
        'if n < int(sys.argv[3]): sys.exit("failure %d" % n)\n'
        'pathlib.Path(sys.argv[2]).write_text("done")'
    )
    task = _script_task(
        'Task flaky', code,
        {'count': ['count.txt'], 'output': ['out.txt'], 'failures': [str(failures)]},
        ['out.txt'], retries = retries
    )
    agenda = _script_agenda([(task, code, ['count', 'output', 'failures'])])
    test_path = _fixture_path()
    success, _ = _run_offline(test_path, agenda, 1, processes = True)
    runs = int((test_path / 'count.txt').read_text())
    if failures <= retries: return success and runs == failures + 1
    return not success and runs == retries + 1


@context(g.one_of(['SIGINT', 'SIGTERM']))
@prop('Interrupting offline mode stops running tasks and their processes', attempts=_attempts(4))
def _cli_interrupt_kills(signal_name: str) -> bool:
    if os.name == 'nt': return True
    task = _script_task('Task slow', _SLEEPER, {'pid': ['pid.txt'], 'output': ['out.txt']}, ['out.txt'])
    agenda = _script_agenda([(task, _SLEEPER, ['pid', 'output'])])
    test_path = _fixture_path()
    ta.store(test_path / 'agenda.yaml', agenda)
    process = subprocess.Popen(
        [sys.executable, '-c', 'from tickle.cli import main; main()', 'offline'],
        cwd = test_path, start_new_session = True,
        stdout = subprocess.DEVNULL, stderr = subprocess.DEVNULL
    )
    try:
        if not _wait_for(lambda: (test_path / 'pid.txt').exists()): return False
        time.sleep(0.2)
        os.kill(process.pid, getattr(signal, signal_name))
        code = process.wait(timeout = 20)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()
    pid = int((test_path / 'pid.txt').read_text())
    expected = 130 if signal_name == 'SIGINT' else 143
    return code == expected and _wait_for(lambda: not _alive(pid), 15.0)


@prop('A second run in a project waits for the first to end', attempts=_attempts(3))
def _agenda_evaluate_offline_lock() -> bool:
    code = 'import sys, time, pathlib; pathlib.Path(sys.argv[1]).write_text("x"); time.sleep(1.5); pathlib.Path(sys.argv[2]).write_text("x")'
    task = _script_task('Task slow', code, {'marker': ['started.txt'], 'output': ['out.txt']}, ['out.txt'])
    agenda = _script_agenda([(task, code, ['marker', 'output'])])
    test_path = _fixture_path()
    ta.store(test_path / 'agenda.yaml', agenda)
    first: list[bool] = []
    runner = Thread(target = lambda: first.append(_run_offline(test_path, processes = True)[0]), daemon = True)
    runner.start()
    try:
        if not _wait_for(lambda: (test_path / 'started.txt').exists()): return False
        second, _, printed = _offline_result(test_path)
        cleaned = _clean(test_path)
    finally:
        runner.join(timeout = 20)
    if second or 'Another tickle run' not in (second.error or ''): return False
    if cleaned: return False
    return first == [True] and bool(_offline_result(test_path)[0])


@context(evaluated_agendas, worker_counts)
@prop('Offline evaluation of targets runs only them and what they need', attempts=_attempts(50))
def _agenda_evaluate_offline_targets(
    agenda: ta.Agenda,
    workers: int,
    by_output: bool,
    rng: random.Random
) -> bool:
    assume(len(agenda.tasks) != 0)
    targets = rng.sample(agenda.tasks, rng.randint(1, min(3, len(agenda.tasks))))
    names = [
        _norm(task.outputs[0]) if by_output else task.desc
        for task in targets
    ]
    producer = {_norm(output): task for task in agenda.tasks for output in task.outputs}
    needed: set[str] = set()
    worklist = list(targets)
    while len(worklist) != 0:
        task = worklist.pop()
        if task.desc in needed: continue
        needed.add(task.desc)
        worklist.extend(
            producer[_norm(input)] for input in task.inputs
            if _norm(input) in producer
        )
    test_path = _fixture_path()
    success, performed = _run_offline(test_path, agenda, workers, targets = names)
    if not success or set(performed) != needed: return False
    success, performed = _run_offline(test_path, agenda, workers)
    return success and set(performed) == {task.desc for task in agenda.tasks} - needed


@context(evaluated_agendas, worker_counts)
@prop('A dry run plans what a run then does, and does nothing', attempts=_attempts(50))
def _agenda_evaluate_offline_dry_run(
    agenda: ta.Agenda,
    workers: int,
    rng: random.Random
) -> bool:
    test_path = _fixture_path()
    if not _run_offline(test_path, agenda, workers)[0]: return False
    if len(agenda.tasks) != 0:
        (test_path / _norm(rng.choice(agenda.tasks).outputs[0])).unlink()
    cache_before = (test_path / 'tickle.cache').read_bytes()
    result, performed, _ = _offline_result(test_path, agenda, workers, dry_run = True)
    if not result or len(performed) != 0: return False
    if (test_path / 'tickle.cache').read_bytes() != cache_before: return False
    success, performed = _run_offline(test_path, agenda, workers)
    # Tasks after a deleted output are planned, and then cut off when it
    # comes out as before
    return success and set(performed) <= set(result.planned) and (
        len(agenda.tasks) == 0 or len(performed) >= 1
    )


@context(evaluated_agendas, worker_counts)
@prop('A task that runs always runs on every run, and changes nothing else', attempts=_attempts(30))
def _agenda_evaluate_offline_always(agenda: ta.Agenda, workers: int) -> bool:
    script = [sys.executable, str(SUCCESS_SCRIPT), '-i', '$inputs', '-o', '$outputs']
    agenda.procs['always'] = script
    agenda.tasks.append(ta.Task(
        desc = 'Task always', proc = 'always', args = {'inputs': [], 'outputs': []},
        always = True
    ))
    test_path = _fixture_path()
    success, performed = _run_offline(test_path, agenda, workers)
    if not success or set(performed) != {task.desc for task in agenda.tasks}: return False
    success, performed = _run_offline(test_path, agenda, workers)
    return success and performed == ['Task always']


def _to_version_2(cache_path: Path) -> None:
    """Rewrite a cache as tickle 0.5.0 wrote it: task records of only their
    input hashes, and no stats or logs."""
    with cache_path.open('rb') as file: data = pickle.load(file)
    data = {
        'version': 2,
        'tasks': {key: record['inputs'] for key, record in data['tasks'].items()},
        'files': data['files'],
        'folders': data['folders']
    }
    with cache_path.open('wb') as file: pickle.dump(data, file)


@context(evaluated_agendas, worker_counts)
@prop('A cache written by tickle 0.5.0 is migrated without running any task', attempts=_attempts(50))
def _agenda_evaluate_offline_migrate(agenda: ta.Agenda, workers: int) -> bool:
    test_path = _fixture_path()
    if not _run_offline(test_path, agenda, workers)[0]: return False
    _to_version_2(test_path / 'tickle.cache')
    success, performed = _run_offline(test_path, agenda, workers)
    if not success or len(performed) != 0: return False
    with (test_path / 'tickle.cache').open('rb') as file:
        return pickle.load(file)['version'] == tapi._CACHE_VERSION


@context(evaluated_agendas)
@prop('A cache written by a newer tickle is refused and left alone', attempts=_attempts(20))
def _agenda_evaluate_offline_newer_cache(agenda: ta.Agenda) -> bool:
    test_path = _fixture_path()
    if not _run_offline(test_path, agenda)[0]: return False
    cache_path = test_path / 'tickle.cache'
    with cache_path.open('rb') as file: data = pickle.load(file)
    data['version'] = tapi._CACHE_VERSION + 1
    with cache_path.open('wb') as file: pickle.dump(data, file)
    before = cache_path.read_bytes()
    result, performed, _ = _offline_result(test_path, agenda)
    if result or 'newer version' not in (result.error or '') or len(performed) != 0:
        return False
    return cache_path.read_bytes() == before and not _clean(test_path)


@context(evaluated_agendas, worker_counts)
@prop('A run with nothing to do hashes no file whose size and time are unchanged', attempts=_attempts(30))
def _agenda_evaluate_offline_no_rehash(agenda: ta.Agenda, workers: int) -> bool:
    for index, task in enumerate(agenda.tasks):
        _add_trigger(agenda, task, 'src/source_%d.txt' % index)
    test_path = _fixture_path()
    (test_path / 'src').mkdir()
    past = time.time() - 60
    for index in range(len(agenda.tasks)):
        path = test_path / 'src' / ('source_%d.txt' % index)
        path.write_text('source %d' % index)
        os.utime(path, (past, past))
    if not _run_offline(test_path, agenda, workers)[0]: return False

    # Outputs written in this run are too new to be trusted; age them too
    for path in _outputs(test_path, agenda): os.utime(path, (past, past))
    if not _run_offline(test_path, agenda, workers)[0]: return False
    hashed: list[Path] = []
    original = tapi._md5

    def _counting(path: Path) -> str | None:
        # The agenda's files, not the depend file tickle watches
        if path.name != 'depend.yaml': hashed.append(path)
        return original(path)

    tapi._md5 = _counting  # type: ignore
    try: success, performed = _run_offline(test_path, agenda, workers)
    finally: tapi._md5 = original  # type: ignore
    return success and len(performed) == 0 and len(hashed) == 0


@context(evaluated_agendas, worker_counts)
@prop('The API takes an agenda object, and its result lists what happened', attempts=_attempts(30))
def _api_agenda_object(agenda: ta.Agenda, workers: int) -> bool:
    test_path = _fixture_path()
    output = io.StringIO()
    logging.disable(logging.CRITICAL)
    try:
        with redirect_stdout(output), _executor(False):
            result = tapi.offline(test_path, agenda, worker_count = workers)
            again = tapi.offline(test_path, agenda, worker_count = workers)
    finally:
        logging.disable(logging.NOTSET)
    descs = sorted(task.desc for task in agenda.tasks)
    return (
        bool(result) and sorted(result.succeeded) == descs
        and bool(again) and again.succeeded == []
        and (test_path / 'tickle.cache').exists()
        and not (test_path / 'agenda.yaml').exists()
    )


@prop('A failing task reports the end of its output and keeps all of it in its log', attempts=_attempts(5))
def _agenda_evaluate_offline_task_log() -> bool:
    code = 'import sys\nfor i in range(50): print("line %d" % i)\nsys.exit(3)'
    task = _script_task('Task noisy', code, {'output': ['out.txt']}, ['out.txt'])
    agenda = _script_agenda([(task, code, ['output'])])
    test_path = _fixture_path()
    result, _, printed = _offline_result(test_path, agenda, 1, processes = True)
    logs = list((test_path / 'tickle-logs').glob('*.log'))
    if result or len(logs) != 1: return False
    logged = logs[0].read_text().splitlines()
    return (
        logged == ['line %d' % i for i in range(50)]
        and 'exited with code 3' in printed
        and 'line 49' in printed and 'line 10\n' not in printed
        and str(logs[0].relative_to(test_path)) in printed
    )


@context(worker_counts)
@prop('Online mode keeps a running task through unrelated changes and restarts it when its input changes', attempts=_attempts(5))
def _agenda_evaluate_online_adopts(workers: int) -> bool:
    code = (
        'import sys, time, pathlib\n'
        'starts = pathlib.Path(sys.argv[2])\n'
        'starts.write_text(starts.read_text() + "x" if starts.exists() else "x")\n'
        'time.sleep(1.5)\n'
        'pathlib.Path(sys.argv[3]).write_text(pathlib.Path(sys.argv[1]).read_text())'
    )
    slow = _script_task(
        'Task slow', code,
        {'input': ['src/slow.txt'], 'starts': ['starts.txt'], 'output': ['out/slow.txt']},
        ['out/slow.txt'], inputs = ['src/slow.txt']
    )
    agenda = _script_agenda([(slow, code, ['input', 'starts', 'output'])])
    agenda.procs['copy'] = [sys.executable, str(SUCCESS_SCRIPT), '-i', '$inputs', '-o', '$outputs']
    agenda.tasks.append(ta.Task(
        desc = 'Task other', proc = 'copy',
        args = {'inputs': ['src/other.txt'], 'outputs': ['out/other.txt']},
        inputs = ['src/other.txt'], outputs = ['out/other.txt']
    ))
    test_path = _fixture_path()
    (test_path / 'src').mkdir()
    (test_path / 'out').mkdir()
    (test_path / 'src' / 'slow.txt').write_text('first')
    (test_path / 'src' / 'other.txt').write_text('other')
    agenda_path = test_path / 'agenda.yaml'
    ta.store(agenda_path, agenda)
    starts = test_path / 'starts.txt'
    with _quiet():
        evaluator = tapi.online(test_path, agenda_path, worker_count = workers)
        if evaluator is None: return False
        runner = Thread(target = evaluator.start, daemon = True)
        runner.start()
        try:
            # An unrelated change while the slow task runs
            if not _wait_for(lambda: starts.exists()): return False
            (test_path / 'src' / 'other.txt').write_text('changed')
            if not _wait_for(lambda: (test_path / 'out' / 'slow.txt').exists()): return False
            if starts.read_text() != 'x': return False

            # A change to its own input while it runs restarts it
            (test_path / 'out' / 'slow.txt').unlink()
            (test_path / 'src' / 'slow.txt').write_text('second')
            if not _wait_for(lambda: starts.read_text() == 'xx'): return False
            time.sleep(0.3)
            (test_path / 'src' / 'slow.txt').write_text('third')
            if not _wait_for(lambda: (test_path / 'out' / 'slow.txt').exists()
                and (test_path / 'out' / 'slow.txt').read_text() == 'third'):
                return False
            ok = starts.read_text() == 'xxx'
        finally:
            evaluator.stop()
            runner.join(timeout = 10)
    return ok and not runner.is_alive()


@context(g.one_of(['delete', 'replace']))
@prop('The watcher follows a file whose folder is deleted and created again, or replaced', attempts=_attempts(5))
def _watcher_reanchors(how: str) -> bool:
    import tickle.watch as twatch
    test_path = _fixture_path()
    folder = test_path / 'src'
    folder.mkdir()
    (folder / 'file.txt').write_text('first')
    events: list[str] = []
    watcher = twatch.FileWatcher()
    watcher.subscribe(folder / 'file.txt', lambda event: events.append(event.value))
    watcher.start()
    try:
        time.sleep(0.3)
        if how == 'delete':
            shutil.rmtree(folder)
            time.sleep(0.3)
            folder.mkdir()
        else:
            other = test_path / 'other'
            other.mkdir()
            (other / 'unrelated.txt').write_text('x')
            shutil.rmtree(folder)
            os.rename(other, folder)
        time.sleep(0.5)
        events.clear()
        (folder / 'file.txt').write_text('second')
        return _wait_for(lambda: len(events) != 0, 5.0)
    finally:
        watcher.stop()


###############################################################################
# Folders, early cutoff, depfiles and explanations
###############################################################################
# Writes one file per frame into a folder: argv is the folder, the frame
# count, and a word mixed into every frame
_FRAMES = (
    'import sys, pathlib\n'
    'folder = pathlib.Path(sys.argv[1]); folder.mkdir(parents = True, exist_ok = True)\n'
    'for i in range(int(sys.argv[2])): (folder / ("%04d.txt" % i)).write_text("%s %d" % (sys.argv[3], i))'
)
# Concatenates the files of a folder, in name order, into a file
_JOIN = (
    'import sys, pathlib\n'
    'folder = pathlib.Path(sys.argv[1])\n'
    'parts = [p.read_text() for p in sorted(folder.rglob("*")) if p.is_file()]\n'
    'pathlib.Path(sys.argv[2]).write_text("|".join(parts))'
)


def _frames_agenda(count: int, word: str, source: bool) -> ta.Agenda:
    """Frames rendered into a folder and joined into one file; or with a
    source folder, joined from that folder."""
    agenda = ta.Agenda(procs = {
        'frames': [sys.executable, '-c', _FRAMES, '$folder', '$count', '$word'],
        'join': [sys.executable, '-c', _JOIN, '$folder', '$output']
    })
    folder = 'src/frames/' if source else 'out/frames/'
    if not source:
        agenda.tasks.append(ta.Task(
            desc = 'Task frames', proc = 'frames',
            args = {'folder': ['out/frames'], 'count': [str(count)], 'word': [word]},
            outputs = [folder]
        ))
    agenda.tasks.append(ta.Task(
        desc = 'Task join', proc = 'join',
        args = {'folder': [folder.rstrip('/')], 'output': ['out/joined.txt']},
        inputs = [folder], outputs = ['out/joined.txt']
    ))
    return agenda


@context(g.int_range(1, 4), worker_counts)
@prop('A folder written by one task is read by another, and a changed file in it reruns the reader', attempts=_attempts(5))
def _agenda_evaluate_offline_folder_output(count: int, workers: int) -> bool:
    test_path = _fixture_path()
    agenda = _frames_agenda(count, 'first', False)
    success, performed = _run_offline(test_path, agenda, workers)
    if not success or performed != ['Task frames', 'Task join']: return False
    joined = test_path / 'out' / 'joined.txt'
    if joined.read_text() != '|'.join('first %d' % i for i in range(count)): return False
    success, performed = _run_offline(test_path, agenda, workers)
    if not success or performed != []: return False

    # Fewer frames: the frames the last run wrote, and that nobody changed,
    # are removed before the task runs again, so none is left stale
    agenda = _frames_agenda(count - 1 if count > 1 else 1, 'second', False)
    success, performed = _run_offline(test_path, agenda, workers)
    frames = sorted(p.name for p in (test_path / 'out' / 'frames').iterdir())
    expected = max(count - 1, 1)
    if not success or performed != ['Task frames', 'Task join']: return False
    if frames != ['%04d.txt' % i for i in range(expected)]: return False
    if not _clean(test_path): return False
    return not (test_path / 'out').exists()


@context(g.int_range(1, 4), worker_counts, g.one_of(['edit', 'add', 'remove']))
@prop('A task reading a source folder reruns when a file in it changes, appears or goes', attempts=_attempts(10))
def _agenda_evaluate_offline_folder_input(count: int, workers: int, change: str) -> bool:
    test_path = _fixture_path()
    folder = test_path / 'src' / 'frames'
    folder.mkdir(parents = True)
    for i in range(count): (folder / ('%04d.txt' % i)).write_text('frame %d' % i)
    agenda = _frames_agenda(count, '', True)
    if not _run_offline(test_path, agenda, workers)[0]: return False
    success, performed = _run_offline(test_path, agenda, workers)
    if not success or performed != []: return False
    if change == 'edit': (folder / '0000.txt').write_text('edited')
    elif change == 'add': (folder / 'sub').mkdir(); (folder / 'sub' / 'extra.txt').write_text('extra')
    else: (folder / '0000.txt').unlink()
    success, performed = _run_offline(test_path, agenda, workers)
    return success and performed == ['Task join'] and (folder / '..').exists()


@context(evaluated_agendas, worker_counts)
@prop('A task that reruns and writes what it wrote before does not rerun the tasks after it', attempts=_attempts(50))
def _agenda_evaluate_offline_cutoff(
    agenda: ta.Agenda,
    workers: int,
    rng: random.Random
) -> bool:
    assume(len(agenda.tasks) != 0)
    test_path = _fixture_path()
    if not _run_offline(test_path, agenda, workers)[0]: return False
    task = rng.choice(agenda.tasks)
    if rng.choice([True, False]):
        # An argument the command does not use: the task is new to tickle
        task.args['unused'] = ['changed']
    else:
        (test_path / _norm(task.outputs[0])).unlink()
    result, performed, _ = _offline_result(test_path, agenda, workers)
    return bool(result) and performed == [task.desc]


@context(g.lists(g.lists(g.strings())))
@prop('Depfiles in make syntax give back the prerequisites they name')
def _depfile_parse(rules: list[list[str]]) -> bool:
    import tickle.depfile as tdepfile
    # Names without line breaks, tabs or backslashes, escaped as compilers
    # do: a backslash before a space, $$ for $, and lines continued
    def _clean_name(name: str) -> str:
        return ''.join(c for c in name if c.isprintable() and c not in '\\\t#:') or 'x'
    expected: list[str] = []
    text = ''
    for index, rule in enumerate(rules):
        names = [_clean_name(name) for name in rule]
        escaped = [name.replace('$', '$$').replace(' ', '\\ ') for name in names]
        text += 'target_%d.o: %s\n' % (index, ' \\\n  '.join(escaped))
        for name in names:
            if name not in expected: expected.append(name)
    return tdepfile.parse(text) == expected


@context(worker_counts)
@prop('Online mode reruns a task reading a folder when a file appears in it, also in a new subfolder', attempts=_attempts(5))
def _agenda_evaluate_online_folder(workers: int) -> bool:
    test_path = _fixture_path()
    folder = test_path / 'src' / 'frames'
    folder.mkdir(parents = True)
    (folder / '0000.txt').write_text('frame 0')
    agenda_path = test_path / 'agenda.yaml'
    ta.store(agenda_path, _frames_agenda(1, '', True))
    joined = test_path / 'out' / 'joined.txt'
    with _quiet():
        evaluator = tapi.online(test_path, agenda_path, worker_count = workers)
        if evaluator is None: return False
        runner = Thread(target = evaluator.start, daemon = True)
        runner.start()
        try:
            if not _wait_for(lambda: joined.exists() and joined.read_text() == 'frame 0'): return False
            (folder / 'later').mkdir()
            (folder / 'later' / '0001.txt').write_text('frame 1')
            ok = _wait_for(lambda: joined.read_text() == 'frame 0|frame 1')
        finally:
            evaluator.stop()
            runner.join(timeout = 10)
    return ok and not runner.is_alive()


_DEPFILE_COMPILER = (
    'import sys, pathlib\n'
    'source, output, depfile = sys.argv[1:4]\n'
    'headers = [line.split()[1] for line in pathlib.Path(source).read_text().splitlines() if line.startswith("include ")]\n'
    'text = pathlib.Path(source).read_text() + "".join(pathlib.Path(h).read_text() for h in headers)\n'
    'pathlib.Path(output).write_text(text)\n'
    'pathlib.Path(depfile).write_text("%s: %s %s\\n" % (output, source, " ".join(h.replace(" ", "\\\\ ") for h in headers)))'
)


@context(worker_counts)
@prop('A task with a depfile reruns when a file its depfile names changes, and only then', attempts=_attempts(5))
def _agenda_evaluate_offline_depfile(workers: int) -> bool:
    test_path = _fixture_path()
    (test_path / 'src').mkdir()
    (test_path / 'src' / 'a header.h').write_text('header one\n')
    (test_path / 'src' / 'other.h').write_text('other\n')
    (test_path / 'src' / 'main.c').write_text('main\ninclude src/other.h\n')
    agenda = ta.Agenda(procs = {
        'compile': [sys.executable, '-c', _DEPFILE_COMPILER, '$source', '$output', '$depfile']
    }, tasks = [ta.Task(
        desc = 'Task compile', proc = 'compile',
        args = {'source': ['src/main.c'], 'output': ['out/main.o'], 'depfile': ['out/main.d']},
        inputs = ['src/main.c'], outputs = ['out/main.o'], depfile = 'out/main.d'
    )])
    (test_path / 'out').mkdir()
    if not _run_offline(test_path, agenda, workers, processes = True)[0]: return False
    success, performed = _run_offline(test_path, agenda, workers, processes = True)
    if not success or performed != []: return False

    # A header the depfile does not name changes nothing; one it names
    # reruns the task
    (test_path / 'src' / 'a header.h').write_text('header two\n')
    success, performed = _run_offline(test_path, agenda, workers, processes = True)
    if not success or performed != []: return False
    (test_path / 'src' / 'other.h').write_text('other changed\n')
    success, performed = _run_offline(test_path, agenda, workers, processes = True)
    if not success or performed != ['Task compile']: return False
    if (test_path / 'out' / 'main.o').read_text() != 'main\ninclude src/other.h\nother changed\n':
        return False

    # Once the source stops including it, the header no longer counts
    (test_path / 'src' / 'main.c').write_text('main\n')
    if not _run_offline(test_path, agenda, workers, processes = True)[0]: return False
    (test_path / 'src' / 'other.h').write_text('other changed again\n')
    success, performed = _run_offline(test_path, agenda, workers, processes = True)
    return success and performed == []


@context(evaluated_agendas, worker_counts)
@prop('A dry run says why each task would run', attempts=_attempts(30))
def _agenda_evaluate_offline_explain(
    agenda: ta.Agenda,
    workers: int,
    rng: random.Random
) -> bool:
    assume(len(agenda.tasks) != 0)
    test_path = _fixture_path()
    result, _, printed = _offline_result(test_path, agenda, workers, dry_run = True)
    if any(result.reasons.get(task.desc) != 'it is new or changed' for task in agenda.tasks):
        return False
    if not _run_offline(test_path, agenda, workers)[0]: return False
    task = rng.choice(agenda.tasks)
    output = _norm(task.outputs[0])
    (test_path / output).unlink()
    result, _, printed = _offline_result(test_path, agenda, workers, dry_run = True)
    if result.reasons.get(task.desc) != 'its output %s is missing' % output: return False
    others = _dependents(agenda, task) - {task.desc}
    return (
        set(result.planned) == others | {task.desc}
        and all(result.reasons[desc].startswith('it reads from') for desc in others)
        and ('Would run "%s": its output %s is missing' % (task.desc, output)) in printed
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
    _agenda_evaluate_online,
    _agenda_clean,
    _agenda_clean_safe,
    _agenda_rejects_empty_flow,
    _agenda_evaluate_offline_spellings,
    _agenda_evaluate_offline_output_spellings,
    _agenda_evaluate_offline_flows_identity,
    _agenda_evaluate_offline_no_stdin,
    _logging_own_logger,
    _cli_bad_cache_path,
    _agenda_decode_error_location,
    _agenda_evaluate_offline_depend_generated,
    _agenda_evaluate_offline_changed_back,
    _agenda_evaluate_online_skipped,
    _agenda_clean_old_cache,
    _cli_interrupt,
    _graph_check_stages,
    _agenda_evaluate_offline_stage_cycle_cached,
    _agenda_evaluate_offline_absolute_paths,
    _schedule_order,
    _schedule_pools,
    _schedule_keep_going,
    _schedule_stop_on_failure,
    _schedule_retries,
    _schedule_priority,
    _schedule_scale,
    _evaluator_starts_when_ready,
    _evaluator_stop_starts_nothing,
    _evaluator_stop_before_start,
    _evaluator_reports_failure_at_once,
    _evaluator_reschedule_stress,
    _evaluator_adopts_running,
    _agenda_evaluate_offline_keep_going,
    _agenda_evaluate_offline_pools,
    _agenda_evaluate_offline_timeout,
    _agenda_evaluate_offline_retries,
    _cli_interrupt_kills,
    _agenda_evaluate_offline_lock,
    _agenda_evaluate_offline_targets,
    _agenda_evaluate_offline_dry_run,
    _agenda_evaluate_offline_always,
    _agenda_evaluate_offline_migrate,
    _agenda_evaluate_offline_newer_cache,
    _agenda_evaluate_offline_no_rehash,
    _api_agenda_object,
    _agenda_evaluate_offline_task_log,
    _agenda_evaluate_online_adopts,
    _watcher_reanchors,
    _agenda_evaluate_offline_folder_output,
    _agenda_evaluate_offline_folder_input,
    _agenda_evaluate_online_folder,
    _agenda_evaluate_offline_cutoff,
    _agenda_evaluate_offline_depfile,
    _depfile_parse,
    _agenda_evaluate_offline_explain,
)
