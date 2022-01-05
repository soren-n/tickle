# External module dependencies
from dataclasses import dataclass, field
from typing import Callable
from pathlib import Path
import yaml

# Internal module dependencies
from . import dataspec

###############################################################################
# Datatypes
###############################################################################
@dataclass
class Task:
    desc: str
    proc: str
    args: dict[str, list[str]]
    inputs: list[str]
    outputs: list[str]

@dataclass
class Agenda:
    procs: dict[str, list[str]] = field(default_factory = dict)
    stages: list[list[str]] = field(default_factory = list)
    tasks: list[Task] = field(default_factory = list)

@dataclass
class CompiledTask:
    stage: int
    description: str
    command: list[str]
    inputs: set[Path]
    outputs: set[Path]

CompiledAgenda = list[CompiledTask]

###############################################################################
# Functions
###############################################################################
def load(agenda_path):
    def _compile_proc(template):
        def _compile(template):
            params = []
            parts = []
            for part in template:
                if len(part) == 0: continue
                if part.startswith('$'):
                    params.append(part[1:])
                    parts.append('%s')
                else:
                    parts.append(part)
            return params, ' '.join(parts)

        params, command = _compile(template)

        def _apply(**args):
            for param in params:
                if param in args: continue
                raise ArgumentError('Missing argument %s' % param)
            return list(filter(
                lambda part: part != '',
                (command % tuple(
                    ' '.join(args[param])
                    for param in params
                )).split(' ')
            ))

        return _apply

    def _parse(agenda_data):

        # Compile procs
        procs = {
            name : _compile_proc(proc)
            for name, proc in agenda_data.procs.items()
        }

        # Stage to proc mapping
        proc_stage = dict()
        for index, stage in enumerate(agenda_data.stages):
            for proc in stage:
                if proc not in procs:
                    raise RuntimeError('Undefined proc \"%s\" in stage %d' % (
                        proc, index + 1
                    ))
                if proc in proc_stage:
                    raise RuntimeError('Proc \"%s\" reserved for stage %d' % (
                        proc, proc_stage[proc] + 1
                    ))
                proc_stage[proc] = index

        # Parse tasks
        agenda = list()
        for task in agenda_data.tasks:
            if task.proc not in procs:
                raise RuntimeError('Undefined proc \"%s\" for task \"%s\"' % (
                    task.proc, task.desc
                ))
            agenda.append(CompiledTask(
                stage = proc_stage[task.proc],
                description = task.desc,
                command = procs[task.proc](**task.args),
                inputs = { Path(input) for input in set(task.inputs) },
                outputs = { Path(output) for output in set(task.outputs) }
            ))

        # Done
        return agenda

    with agenda_path.open('r') as agenda_file:
        raw_data = yaml.safe_load(agenda_file)
        return _parse(dataspec.decode(Agenda, raw_data))

def store(agenda_path, agenda_data):
    assert isinstance(agenda_data, Agenda)
    with agenda_path.open('w+') as agenda_file:
        raw_data = dataspec.encode(Agenda, agenda_data)
        yaml.dump(raw_data, agenda_file)
