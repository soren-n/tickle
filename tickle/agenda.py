# External module dependencies
from dataclasses import dataclass
from typing import Callable
from pathlib import Path
import yaml

# Internal module dependencies
from . import dataspec

###############################################################################
# Types
###############################################################################
@dataclass
class _Task:
    desc: str
    proc: str
    args: dict[str, list[str]]
    inputs: list[str]
    outputs: list[str]

@dataclass
class _Agenda:
    procs: dict[str, list[str]]
    tasks: list[_Task]

@dataclass
class Task:
    description: str
    command: list[str]
    inputs: set[Path]
    outputs: set[Path]

Agenda = list[Task]

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

        # Parse tasks
        cwd = Path.cwd()
        agenda = [
            Task(
                description = task.desc,
                command = procs[task.proc](**task.args),
                inputs = { Path(cwd, input) for input in set(task.inputs) },
                outputs = { Path(cwd, output) for output in set(task.outputs) }
            )
            for task in agenda_data.tasks
        ]

        # Done
        return agenda

    with agenda_path.open('r') as agenda_file:
        raw_data = yaml.safe_load(agenda_file)
        return _parse(dataspec.parse(_Agenda, raw_data))
