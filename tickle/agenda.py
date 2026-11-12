# External module dependencies
from dataclasses import dataclass, field
from typing import Callable, List, Dict, Set
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
    flows: List[str]
    args: Dict[str, List[str]]
    inputs: List[str]
    outputs: List[str]

@dataclass
class Agenda:
    procs: Dict[str, List[str]] = field(default_factory = dict)
    flows: Dict[str, List[List[str]]] = field(default_factory = dict)
    tasks: List[Task] = field(default_factory = list)

@dataclass
class CompiledTask:
    hash: str
    description: str
    flows: Dict[str, int]
    command: List[str]
    inputs: Set[Path]
    outputs: Set[Path]

CompiledAgenda = List[CompiledTask]

###############################################################################
# Functions
###############################################################################
def load(agenda_path : Path) -> Agenda:
    assert isinstance(agenda_path, Path)
    with agenda_path.open('r') as agenda_file:
        try:
            raw_data = yaml.safe_load(agenda_file)
            return dataspec.decode(Agenda, raw_data)
        except (yaml.YAMLError, TypeError) as error:
            raise RuntimeError(
                'Invalid agenda file %s:\n%s' % (agenda_path, error)
            ) from error

def store(agenda_path : Path, agenda_data : Agenda):
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

def compile(target_dir : Path, agenda_data : Agenda) -> CompiledAgenda:
    def _compile_proc(template : List[str]) -> Callable[..., List[str]]:
        # Build the argument vector word by word: a literal word stays one
        # word, and a $parameter word becomes one word per argument value.
        # Joining into a string and splitting it again would break literal
        # words with spaces, values with quotes, and words with % in them.
        params = [ part[1:] for part in template if part.startswith('$') ]

        def _apply(**args : List[str]) -> List[str]:
            for param in params:
                if param in args: continue
                raise KeyError(param)
            result : List[str] = []
            for part in template:
                if part.startswith('$'): result.extend(args[part[1:]])
                else: result.append(part)
            return result

        return _apply

    def _task_hash(task_data : Task, command : List[str]) -> str:
        # The command is hashed rather than the proc's name, so editing a
        # proc's definition changes the tasks that use it.
        data = json.dumps({
            'command': command,
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
    flow_proc_stage : Dict[str, Dict[str, int]] = {}
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
                            proc, flow_proc_stage[flow][proc] + 1, flow
                        )
                    )
                flow_proc_stage[flow][proc] = index

    def _command(task_data : Task) -> List[str]:
        try: return procs[task_data.proc](**task_data.args)
        except KeyError as error:
            raise RuntimeError(
                'Missing argument \"%s\" of proc \"%s\" for task \"%s\"' % (
                    error.args[0], task_data.proc, task_data.desc
                )
            ) from error

    # Parse tasks
    agenda : CompiledAgenda = []
    for task_data in agenda_data.tasks:
        if task_data.proc not in procs:
            raise RuntimeError('Undefined proc \"%s\" for task \"%s\"' % (
                task_data.proc, task_data.desc
            ))
        for flow in task_data.flows:
            if flow not in flow_proc_stage:
                raise RuntimeError('Undefined flow \"%s\" for task \"%s\"' % (
                    flow, task_data.desc
                ))
        command = _command(task_data)
        agenda.append(CompiledTask(
            hash = _task_hash(task_data, command),
            description = task_data.desc,
            flows = {
                flow : flow_proc_stage[flow][task_data.proc]
                for flow in task_data.flows
            },
            command = command,
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
