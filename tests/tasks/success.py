# External module dependencies
from pathlib import Path
import argparse
import hashlib
import json
import sys
import os

###############################################################################
# Functions
###############################################################################
def _implicits(cwd : Path, inputs : list) -> list:
    """The files the inputs depend on through depend.yaml in the working
    directory, the way a compiler reads the headers a source includes."""
    depend_path = cwd / 'depend.yaml'
    if not depend_path.exists(): return []
    import yaml
    depend = yaml.safe_load(depend_path.read_text()) or {}
    depend = {
        os.path.normpath(src): [os.path.normpath(dst) for dst in dsts]
        for src, dsts in depend.items()
    }
    result : list = []
    worklist = [os.path.normpath(input) for input in inputs]
    while len(worklist) != 0:
        file = worklist.pop(0)
        for dep in depend.get(file, []):
            if dep in result: continue
            result.append(dep)
            worklist.append(dep)
    return sorted(result)

def _read(path : Path) -> str:
    try: return path.read_text()
    except FileNotFoundError: return '<missing>'

def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog = 'success',
        description = 'Checks inputs files exists, and creates output files.',
        exit_on_error = False
    )
    parser.add_argument('-i', '--inputs', type = str, nargs = '*', dest = 'inputs', default = [])
    parser.add_argument('-o', '--outputs', type = str, nargs = '*', dest = 'outputs', default = [])
    # Ignored words that change the command and the outputs
    parser.add_argument('--salt', type = str, nargs = '*', dest = 'salt', default = [])
    # Files to append to after reading the inputs
    parser.add_argument('--append', type = str, nargs = '*', dest = 'append', default = [])
    return parser

def run(argv : list, cwd : Path, log) -> int:
    """Run the script with the arguments argv in the folder cwd, writing
    messages to log; returns the exit code. The tests call this in-process
    as well, from several threads at once, so it never changes the working
    directory."""
    args = _parser().parse_args(argv)

    # Check inputs
    for input in args.inputs:
        input_path = cwd / input
        if not input_path.exists():
            log.write('No such file or directory: %s\n' % input)
            return 1
        if not input_path.is_file():
            log.write('Input path is not a file: %s\n' % input)
            return 1
        # Lets a property make a task fail without changing its definition
        if input_path.read_text() == 'fail':
            log.write('Input asked to fail: %s\n' % input)
            return 1

    # The outputs are a digest of everything the task read, so a stale
    # output differs from the one a clean build writes
    implicits = _implicits(cwd, args.inputs)
    content = hashlib.md5(json.dumps({
        'argv': argv,
        'inputs': [[input, _read(cwd / input)] for input in args.inputs],
        'implicits': [[file, _read(cwd / file)] for file in implicits]
    }, sort_keys = True).encode('utf-8')).hexdigest()

    # Edit a file after reading the inputs, like a user saving a file while
    # the task runs
    for path in args.append:
        with open(cwd / path, 'a') as file: file.write('+')

    # Create outputs
    for output in args.outputs:
        output_path = cwd / output
        output_path.parent.mkdir(parents = True, exist_ok = True)
        output_path.write_text(content)

    # Done
    return 0

###############################################################################
# Main entry
###############################################################################
if __name__ == '__main__':
    sys.exit(run(sys.argv[1:], Path.cwd(), sys.stderr))
