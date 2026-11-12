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
import shutil
import sys

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

    # Changing it must rerun exactly the tasks reading that input, and the
    # tasks after them
    header_path.write_text('after')
    readers = [other for other in agenda.tasks if task.inputs[0] in other.inputs]
    expected = set().union(*(_dependents(agenda, other) for other in readers))
    success, performed = _run_offline(test_path)
    return success and set(performed) == expected


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


@context(evaluated_agendas, worker_counts)
@prop('Offline evaluation starts tasks after their dependencies and earlier stages', attempts=100)
def _agenda_evaluate_offline_order(agenda: ta.Agenda, workers: int) -> bool:
    success, performed = _run_offline(_fixture_path(), agenda, workers)
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


@context(evaluated_agendas, worker_counts)
@prop('Offline evaluation reruns a task with a missing output and the tasks after it', attempts=100)
def _agenda_evaluate_offline_missing_output(
    agenda: ta.Agenda,
    workers: int,
    rng: random.Random
) -> bool:
    assume(len(agenda.tasks) != 0)
    test_path = _fixture_path()
    if not _run_offline(test_path, agenda, workers)[0]: return False
    task = rng.choice(agenda.tasks)
    (test_path / task.outputs[0]).unlink()
    success, performed = _run_offline(test_path, None, workers)
    return success and set(performed) == _dependents(agenda, task)


@context(evaluated_agendas, worker_counts)
@prop('Offline evaluation reruns changed tasks and the tasks after them', attempts=100)
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
@prop('Offline evaluation skips a task with an impossible input and the tasks after it, and fails', attempts=100)
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


def _add_trigger(agenda: ta.Agenda, task: ta.Task, trigger: str) -> None:
    """Give the task an extra source input that its script reads, so writing
    'fail' into it makes the task fail without changing its definition."""
    task.inputs.append(trigger)
    task.args['inputs'] = task.args['inputs'] + [trigger]


@context(evaluated_agendas, worker_counts)
@prop('Offline evaluation reruns a task that failed until it succeeds', attempts=100)
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
@prop('Offline evaluation reruns a task whose input changed while it ran', attempts=30)
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
@prop('Offline evaluation after random changes and failures matches a clean build', attempts=30)
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
            'args', 'proc', 'fail', 'unfail', 'move'
        ])
        if action == 'source' and len(sources) != 0:
            _write(rng.choice(sources), _fresh())
        elif action == 'header':
            _write(rng.choice(headers), _fresh())
        elif action == 'depend':
            tdep.store(test_path / 'depend.yaml', _random_depend())
        elif action == 'output':
            path = test_path / rng.choice(agenda.tasks).outputs[0]
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
        (test_path / output).read_text() == (clean_path / output).read_text()
        for task in agenda.tasks
        for output in task.outputs
    )


@context(evaluated_agendas)
@prop('Offline evaluation after moving the project runs no tasks', attempts=100)
def _agenda_evaluate_offline_relocate(agenda: ta.Agenda) -> bool:
    test_path = _fixture_path()
    if not _offline(test_path, agenda): return False
    moved_path = _fixture_path() / 'moved'
    shutil.copytree(test_path, moved_path)
    success, performed = _run_offline(moved_path)
    return success and len(performed) == 0


@context(g.int_range(1, 4))
@prop('Offline evaluation orders a task after the task generating a file its depend entries name', attempts=50)
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
    compiled = ta.compile(Path('/target'), agenda)
    tasks = [tev.Task(task.flows, lambda: None) for task in compiled]
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
    descs = {id(task): compiled[index].description for index, task in enumerate(tasks)}
    result: dict[str, tuple[int, int, int]] = {}
    for batch_index, batch in enumerate(tgraph.compile(tasks)):
        for sequence_index, sequence in enumerate(batch):
            for position, task in enumerate(sequence):
                desc = descs[id(task)]
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
        output: task.desc
        for task in agenda.tasks
        for output in task.outputs
    }
    for task in agenda.tasks:
        for input in task.inputs:
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
@prop('A task error is reported without cancelling the next program', attempts=10)
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
@prop('Clean keeps sources, files changed since they were generated, and other projects', attempts=100)
def _agenda_clean_safe(
    agenda: ta.Agenda,
    rng: random.Random
) -> bool:
    readers = {input for task in agenda.tasks for input in task.inputs}
    leafs = [
        task for task in agenda.tasks
        if not any(output in readers for output in task.outputs)
    ]
    assume(len(leafs) >= 2)
    test_path = _fixture_path()
    if not _offline(test_path, agenda): return False

    # One generated file becomes a hand-written source of a new task
    source_task, edited_task = rng.sample(leafs, 2)
    source = source_task.outputs[0]
    agenda.tasks.remove(source_task)
    agenda.tasks.append(ta.Task(
        desc = 'Copy', proc = edited_task.proc, flows = edited_task.flows,
        args = {'inputs': [source], 'outputs': ['temp/copy.txt']},
        inputs = [source], outputs = ['temp/copy.txt']
    ))
    (test_path / source).write_text('hand written')
    if not _offline(test_path, agenda): return False

    # Another generated file is edited after it was generated
    edited = test_path / edited_task.outputs[0]
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
)
