# External module dependencies
from dataclasses import dataclass, field
from typing import Callable
from pathlib import Path
import hashlib
import json
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
    flows: list[str]
    args: dict[str, list[str]]
    inputs: list[str]
    outputs: list[str]

@dataclass
class Agenda:
    procs: dict[str, list[str]] = field(default_factory = dict)
    flows: dict[str, list[list[str]]] = field(default_factory = dict)
    tasks: list[Task] = field(default_factory = list)

@dataclass
class CompiledTask:
    hash: int
    description: str
    flows: dict[str, int]
    command: list[str]
    inputs: set[Path]
    outputs: set[Path]

CompiledAgenda = list[CompiledTask]

###############################################################################
# Functions
###############################################################################
def load(agenda_path):
    assert isinstance(agenda_path, Path)
    with agenda_path.open('r') as agenda_file:
        raw_data = yaml.safe_load(agenda_file)
        return dataspec.decode(Agenda, raw_data)

def store(agenda_path, agenda_data):
    assert isinstance(agenda_data, Agenda)
    with agenda_path.open('w+') as agenda_file:
        raw_data = dataspec.encode(Agenda, agenda_data)
        yaml.dump(
            raw_data,
            agenda_file,
            width = 80,
            indent = 2,
            default_flow_style = False
        )

def compile(target_dir, agenda_data):
    assert isinstance(target_dir, Path)
    assert isinstance(agenda_data, Agenda)

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

        def _join(inputs):
            def _wrap(input):
                if ' ' not in input: return input
                return '\"%s\"' % input

            return ' '.join([ _wrap(input) for input in inputs ])

        def _split(input):
            result = []
            subresult = ''
            worklist = input.split(' ')
            while len(worklist) != 0:
                part = worklist.pop(0)
                if len(subresult) != 0:
                    subresult = '%s %s' % (subresult, part)
                    if part.endswith('\"'):
                        result.append(subresult[1:-1])
                        subresult = ''
                    continue
                if part.startswith('\"'):
                    subresult += part
                    continue
                result.append(part)
            return result

        def _apply(**args):
            for param in params:
                if param in args: continue
                raise ArgumentError('Missing argument %s' % param)
            return list(filter(
                lambda part: part != '',
                _split(command % tuple(
                    _join(args[param])
                    for param in params
                ))
            ))

        return _apply

    def _task_hash(task_data):
        data = json.dumps({
            'proc': task_data.proc,
            'args': task_data.args,
            'flows': list(sorted(task_data.flows)),
            'inputs': list(sorted(task_data.inputs)),
            'outputs': list(sorted(task_data.outputs))
        }, sort_keys = True)
        return hashlib.md5(data.encode('utf-8')).hexdigest()

    # Compile procs
    procs = {
        name : _compile_proc(proc)
        for name, proc in agenda_data.procs.items()
    }

    # Flow and stage to proc mapping
    flow_proc_stage = dict()
    for flow in agenda_data.flows:
        if flow not in flow_proc_stage: flow_proc_stage[flow] = dict()
        for index, stage in enumerate(agenda_data.flows[flow]):
            for proc in stage:
                if proc not in procs:
                    raise RuntimeError(
                        'Undefined proc \"%s\" in stage %d of flow %s' % (
                            proc, index + 1, flow
                        )
                    )
                if proc in flow_proc_stage[flow]:
                    raise RuntimeError(
                        'Proc \"%s\" reserved for stage %d of flow %s' % (
                            proc, proc_stage[proc] + 1, flow
                        )
                    )
                flow_proc_stage[flow][proc] = index

    # Parse tasks
    agenda = list()
    for task_data in agenda_data.tasks:
        if task_data.proc not in procs:
            raise RuntimeError('Undefined proc \"%s\" for task \"%s\"' % (
                task_data.proc, task_data.desc
            ))
        for flow in task_data.flows:
            if flow not in flow_proc_stage:
                raise RuntimeError('Undefined flow \"%s\" for task \"\"' % (
                    flow, task_data.desc
                ))
        agenda.append(CompiledTask(
            hash = _task_hash(task_data),
            description = task_data.desc,
            flows = {
                flow : flow_proc_stage[flow][task_data.proc]
                for flow in task_data.flows
            },
            command = procs[task_data.proc](**task_data.args),
            inputs = {
                Path(target_dir, input)
                for input in set(task_data.inputs)
            },
            outputs = {
                Path(target_dir, output)
                for output in set(task_data.outputs)
            }
        ))

    # Done
    return agenda
