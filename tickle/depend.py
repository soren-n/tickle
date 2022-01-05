# External module dependencies
from pathlib import Path
import yaml

# Internal module dependencies
from . import dataspec

###############################################################################
# Datatypes
###############################################################################
Depend = dict[str, list[str]]
CompiledDepend = dict[Path, set[Path]]

###############################################################################
# Functions
###############################################################################
def load(depend_path):
    cwd = Path.cwd()
    with depend_path.open('r') as depend_file:
        raw_data = yaml.safe_load(depend_file)
        if raw_data is None: raw_data = {}
        depend_data = dataspec.decode(Depend, raw_data)
        return {
            Path(cwd, src_path) : {
                Path(cwd, dst_path)
                for dst_path in set(dst_paths)
            }
            for src_path, dst_paths in depend_data.items()
        }

def store(depend_path, depend_data):
    assert isinstance(depend_data, depend)
    with depend_path.open('w+') as depend_file:
        raw_data = dataspec.encode(Depend, depend_data)
        yaml.dump(raw_data, depend_file)
