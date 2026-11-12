# External module dependencies
from typing import Dict, List, Set
from pathlib import Path
import yaml

# Internal module dependencies
from .agenda import YamlLoader
from . import dataspec

###############################################################################
# Datatypes
###############################################################################
Depend = Dict[str, List[str]]
CompiledDepend = Dict[Path, Set[Path]]

###############################################################################
# Functions
###############################################################################
def empty() -> Depend:
    return {}

def load(depend_path : Path) -> Depend:
    try:
        with depend_path.open('r', encoding = 'utf-8') as depend_file:
            raw_data = yaml.load(depend_file, Loader = YamlLoader)
        if raw_data is None: return {}
        return dataspec.decode(Depend, raw_data)
    except (OSError, UnicodeDecodeError) as error:
        raise RuntimeError(
            'Could not read depend file %s: %s' % (depend_path, error)
        ) from error
    except (yaml.YAMLError, TypeError) as error:
        raise RuntimeError(
            'Invalid depend file %s:\n%s' % (depend_path, error)
        ) from error

def store(depend_path : Path, depend_data : Depend):
    with depend_path.open('w+', encoding = 'utf-8') as depend_file:
        raw_data = dataspec.encode(Depend, depend_data)
        yaml.dump(raw_data, depend_file)

def compile(target_dir : Path, depend_data : Depend) -> CompiledDepend:
    return {
        Path(target_dir, src_path) : {
            Path(target_dir, dst_path)
            for dst_path in set(dst_paths)
        }
        for src_path, dst_paths in depend_data.items()
    }