# External module dependencies
from pathlib import Path
import argparse
import hashlib
import logging
import json
import sys
import os

###############################################################################
# Functions
###############################################################################
def _implicits(inputs : list) -> list:
    """The files the inputs depend on through depend.yaml in the working
    directory, the way a compiler reads the headers a source includes."""
    depend_path = Path('depend.yaml')
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

def _read(path : str) -> str:
    try: return Path(path).read_text()
    except FileNotFoundError: return '<missing>'

###############################################################################
# Main entry
###############################################################################
def main(args : argparse.Namespace) -> bool:

    # Check inputs
    for input in args.inputs:
        input_path = Path(input)
        if not input_path.exists():
            logging.critical('Not such file or directory:\n%s' % input)
            return False
        if not input_path.is_file():
            logging.critical('Input path is not a file:\n%s' % input)
            return False
        # Lets a property make a task fail without changing its definition
        if input_path.read_text() == 'fail':
            logging.critical('Input asked to fail:\n%s' % input)
            return False

    # The outputs are a digest of everything the task read, so a stale
    # output differs from the one a clean build writes
    implicits = _implicits(args.inputs)
    content = hashlib.md5(json.dumps({
        'argv': sys.argv[1:],
        'inputs': [[input, _read(input)] for input in args.inputs],
        'implicits': [[file, _read(file)] for file in implicits]
    }, sort_keys = True).encode('utf-8')).hexdigest()

    # Edit a file after reading the inputs, like a user saving a file while
    # the task runs
    for path in args.append:
        with open(path, 'a') as file: file.write('+')

    # Create outputs
    for output in args.outputs:
        output_path = Path(output)
        parent_path = output_path.parent
        if not parent_path.exists(): os.makedirs(parent_path, exist_ok = True)
        with output_path.open('w+') as output_file: output_file.write(content)

    # Done
    return True

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog = 'success',
        description = 'Checks inputs files exists, and creates output files.',
        formatter_class = argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        '-i', '--inputs',
        type = str,
        nargs = '*',
        dest = 'inputs',
        default = [],
        help = 'The input file paths that will be check for existance.'
    )
    parser.add_argument(
        '-o', '--outputs',
        type = str,
        nargs = '*',
        dest = 'outputs',
        default = [],
        help = 'The output file paths that will be created.'
    )
    parser.add_argument(
        '--salt',
        type = str,
        nargs = '*',
        dest = 'salt',
        default = [],
        help = 'Ignored words that change the command and the outputs.'
    )
    parser.add_argument(
        '--append',
        type = str,
        nargs = '*',
        dest = 'append',
        default = [],
        help = 'Files to append to after reading the inputs.'
    )
    success = main(parser.parse_args())
    sys.exit(0 if success else -1)
