# External module dependencies
from pathlib import Path
import argparse
import logging
import sys
import os

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

    # Create outputs
    for output in args.outputs:
        output_path = Path(output)
        parent_path = output_path.parent
        if not parent_path.exists(): os.makedirs(parent_path)
        with output_path.open('w+') as output_file: output_file.write('')

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
    success = main(parser.parse_args())
    sys.exit(0 if success else -1)