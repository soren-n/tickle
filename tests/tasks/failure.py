# External module dependencies
import argparse
import sys

###############################################################################
# Main entry
###############################################################################
def main(args : argparse.Namespace) -> bool:
    return False

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog = 'failure',
        description = 'Fail always.',
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