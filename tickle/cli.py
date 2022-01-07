# External module dependencies
from pathlib import Path

# Internal module dependencies
from . import api

###############################################################################
# Main entry
###############################################################################
def main():
    import argparse
    import sys

    def _app(args):
        cwd = Path.cwd()

        # Check if in version mode
        if args.mode == 'version':
            from . import __version__
            print(__version__)
            return True

        # Run specified mode
        if args.mode == 'offline':
            return api.offline(
                cwd,
                Path(cwd, args.agenda),
                Path(cwd, args.depend),
                Path(cwd, args.cache),
                Path(cwd, args.log),
                args.workers,
                args.debug
            )
        if args.mode == 'online':
            return api.online(
                cwd,
                Path(cwd, args.agenda),
                Path(cwd, args.depend),
                Path(cwd, args.cache),
                Path(cwd, args.log),
                args.workers,
                args.debug
            )
        if args.mode == 'clean':
            return api.clean(
                cwd,
                Path(cwd, args.cache),
                Path(cwd, args.log),
                args.debug
            )

    parser = argparse.ArgumentParser(
        prog = 'tickle',
        description = 'Task graph scheduling with asynchronous evaluation.',
        formatter_class = argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        'mode', type = str,
        choices = ['offline', 'online', 'clean', 'version'],
        help = 'Offline mode for an inattentive evaluation mode where file modifications are ignored once tasks have been scheduled. Online mode for an attentive evaluation mode where file creations or modifications trigger a rescheduling of the task graph. Clean mode will delete all files and folders generated during offline or online evaluation. Version mode will print the tool version.'
    )
    parser.add_argument(
        '--debug',
        dest = 'debug',
        action = 'store_true',
        help = 'Sets debug logging level for tool messages'
    )
    parser.add_argument(
        '-w', '--workers',
        type = int,
        dest = 'workers',
        default = api.default_worker_count(),
        help = 'The number of concurrent workers; defaults to the number of logical cores minus one for the main thread'
    )
    parser.add_argument(
        '-a', '--agenda',
        type = str,
        dest = 'agenda',
        default = api.default_agenda_path(),
        help = 'Agenda YAML file location; contains the procedure and task definitions, file path must be relative to current working directory'
    )
    parser.add_argument(
        '-d', '--depend',
        type = str,
        dest = 'depend',
        default = api.default_depend_path(),
        help = 'Depend YAML file location; contains a map of dynamic task dependencies, this file is optional, file path must be relative to current working directory'
    )
    parser.add_argument(
        '-c', '--cache',
        type = str,
        dest = 'cache',
        default = api.default_cache_path(),
        help = 'Binary cache file location; contains inter-run persistent data, file path must be relative to current working directory'
    )
    parser.add_argument(
        '-l', '--log',
        type = str,
        dest = 'log',
        default = api.default_log_path(),
        help = 'Log file location; contains runtime messages, file path must be relative to current working directory'
    )
    success = _app(parser.parse_args())
    if not success: parser.print_help()
    sys.exit(0 if success else -1)
