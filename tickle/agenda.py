# External module dependencies
from dataclasses import dataclass, field
from collections.abc import Callable
from pathlib import Path
import hashlib
import json
import yaml
import os

# Internal module dependencies
from . import dataspec

###############################################################################
# Datatypes
###############################################################################
# Fields added after 0.5.0 are left out of a stored agenda while they hold
# their default, so agendas that do not use them look as they did
_NEW = { 'omit_default': True }

@dataclass
class Task:
    desc: str
    proc: str
    flows: list[str] = field(default_factory = list)
    args: dict[str, list[str]] = field(default_factory = dict)
    inputs: list[str] = field(default_factory = list)
    outputs: list[str] = field(default_factory = list)
    # Name of a pool in Agenda.pools limiting how many of its tasks run at
    # once; '' for none
    pool: str = field(default = '', metadata = _NEW)
    # Seconds the task may run before it is stopped and fails; 0 for no limit
    timeout: float = field(default = 0.0, metadata = _NEW)
    # How many times a failing task is run again before it counts as failed
    retries: int = field(default = 0, metadata = _NEW)
    # Run on every run; only for tasks without outputs
    always: bool = field(default = False, metadata = _NEW)

@dataclass
class Agenda:
    procs: dict[str, list[str]] = field(default_factory = dict)
    flows: dict[str, list[list[str]]] = field(default_factory = dict)
    tasks: list[Task] = field(default_factory = list)
    # Pool name to how many of its tasks may run at once
    pools: dict[str, int] = field(default_factory = dict, metadata = _NEW)

@dataclass
class CompiledTask:
    hash: str
    description: str
    flows: dict[str, int]
    command: list[str]
    inputs: set[Path]
    outputs: set[Path]
    pool: str = ''
    timeout: float = 0.0
    retries: int = 0
    always: bool = False

CompiledAgenda = list[CompiledTask]

###############################################################################
# Functions
###############################################################################
def resolve_path(base_dir : Path, rel_path : str) -> Path:
    """An agenda or depend path made absolute against base_dir, without
    redundant separators or . and .. parts, so every way of writing a path
    names the same file."""
    return Path(os.path.normpath(os.path.join(base_dir, rel_path)))

# The C parser is much faster on large agendas, when PyYAML was built with it
YamlLoader = getattr(yaml, 'CSafeLoader', yaml.SafeLoader)

def load(agenda_path : Path) -> Agenda:
    assert isinstance(agenda_path, Path)
    try:
        with agenda_path.open('r', encoding = 'utf-8') as agenda_file:
            raw_data = yaml.load(agenda_file, Loader = YamlLoader)
        return dataspec.decode(Agenda, raw_data)
    except (OSError, UnicodeDecodeError) as error:
        raise RuntimeError(
            'Could not read agenda file %s: %s' % (agenda_path, error)
        ) from error
    except (yaml.YAMLError, TypeError) as error:
        raise RuntimeError(
            'Invalid agenda file %s:\n%s' % (agenda_path, error)
        ) from error

def store(agenda_path : Path, agenda_data : Agenda):
    assert isinstance(agenda_data, Agenda)
    with agenda_path.open('w+', encoding = 'utf-8') as agenda_file:
        raw_data = dataspec.encode(Agenda, agenda_data)
        yaml.dump(
            raw_data,
            agenda_file,
            width = 80,
            indent = 2,
            default_flow_style = False
        )

def compile(target_dir : Path, agenda_data : Agenda) -> CompiledAgenda:
    def _compile_proc(template : list[str]) -> Callable[..., list[str]]:
        # Build the argument vector word by word: a literal word stays one
        # word, and a $parameter word becomes one word per argument value.
        # Joining into a string and splitting it again would break literal
        # words with spaces, values with quotes, and words with % in them.
        params = [ part[1:] for part in template if part.startswith('$') ]

        def _apply(**args : list[str]) -> list[str]:
            for param in params:
                if param in args: continue
                raise KeyError(param)
            result : list[str] = []
            for part in template:
                if part.startswith('$'): result.extend(args[part[1:]])
                else: result.append(part)
            return result

        return _apply

    def _hash_path(path : str) -> str:
        # Relative to the agenda's folder, with / separators, so a path
        # written absolute or relative, and on any system, hashes the same
        absolute = resolve_path(target_dir, path)
        try: return Path(os.path.relpath(absolute, target_dir)).as_posix()
        except ValueError: return absolute.as_posix() # another drive

    def _task_hash(task_data : Task, command : list[str]) -> str:
        # The command is hashed rather than the proc's name, so editing a
        # proc's definition changes the tasks that use it. Flows only order
        # tasks and change no output, so they are left out, and paths are
        # normalised, so writing one differently changes nothing either.
        data = json.dumps({
            'command': command,
            'args': task_data.args,
            'inputs': sorted({ _hash_path(path) for path in task_data.inputs }),
            'outputs': sorted({ _hash_path(path) for path in task_data.outputs })
        }, sort_keys = True)
        return hashlib.md5(data.encode('utf-8')).hexdigest()

    # Compile procs
    procs = {
        name : _compile_proc(proc)
        for name, proc in agenda_data.procs.items()
    }

    # Flow and stage to proc mapping
    flow_proc_stage : dict[str, dict[str, int]] = {}
    for flow in agenda_data.flows:
        if flow == '':
            raise RuntimeError('A flow must have a name; found a flow named ""')
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

    def _command(task_data : Task) -> list[str]:
        try: return procs[task_data.proc](**task_data.args)
        except KeyError as error:
            raise RuntimeError(
                'Missing argument \"%s\" of proc \"%s\" for task \"%s\"' % (
                    error.args[0], task_data.proc, task_data.desc
                )
            ) from error

    # Pools
    for pool, depth in agenda_data.pools.items():
        if pool == '':
            raise RuntimeError('A pool must have a name; found a pool named ""')
        if depth < 1:
            raise RuntimeError(
                'Pool \"%s\" must allow at least one task, not %d' % (pool, depth)
            )

    # Parse tasks
    agenda : CompiledAgenda = []
    for task_data in agenda_data.tasks:
        if task_data.pool != '' and task_data.pool not in agenda_data.pools:
            raise RuntimeError('Undefined pool \"%s\" for task \"%s\"' % (
                task_data.pool, task_data.desc
            ))
        if task_data.timeout < 0:
            raise RuntimeError('Negative timeout for task \"%s\"' % task_data.desc)
        if task_data.retries < 0:
            raise RuntimeError('Negative retries for task \"%s\"' % task_data.desc)
        if task_data.always and len(task_data.outputs) != 0:
            raise RuntimeError(
                'Task \"%s\" runs always, so it can not have outputs' % (
                    task_data.desc
                )
            )
        if task_data.proc not in procs:
            raise RuntimeError('Undefined proc \"%s\" for task \"%s\"' % (
                task_data.proc, task_data.desc
            ))
        for flow in task_data.flows:
            if flow not in flow_proc_stage:
                raise RuntimeError('Undefined flow \"%s\" for task \"%s\"' % (
                    flow, task_data.desc
                ))
            if task_data.proc not in flow_proc_stage[flow]:
                raise RuntimeError(
                    'Proc \"%s\" of task \"%s\" is in no stage of flow %s' % (
                        task_data.proc, task_data.desc, flow
                    )
                )
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
                resolve_path(target_dir, input)
                for input in set(task_data.inputs)
            },
            outputs = {
                resolve_path(target_dir, output)
                for output in set(task_data.outputs)
            },
            pool = task_data.pool,
            timeout = float(task_data.timeout),
            retries = task_data.retries,
            always = task_data.always
        ))

    # Done
    return agenda
