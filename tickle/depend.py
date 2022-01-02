# External module dependencies
from pathlib import Path
import yaml

# Internal module dependencies
from . import dataspec

###############################################################################
# Types
###############################################################################
_Depend = dict[str, list[str]]
Depend = dict[str, set[Path]]

###############################################################################
# Functions
###############################################################################
def load(depend_path):
    cwd = Path.cwd()
    with depend_path.open('r') as depend_file:
        raw_data = yaml.safe_load(depend_file)
        depend_data = dataspec.parse(_Depend, raw_data)
        return {
            Path(cwd, src_path) : {
                Path(cwd, dst_path)
                for dst_path in set(dst_paths)
            }
            for src_path, dst_paths in depend_data.items()
        }
