# External module dependencies
from typing import Any
from pathlib import Path
import logging
import signal

# Internal module dependencies
from . import api
from . import log

###############################################################################
# Defaults
###############################################################################
def default_log_path(dir_path : Path = Path('./')) -> Path:
    return Path(dir_path, 'tickle.log')

###############################################################################
# Main entry
###############################################################################
def main():
    import argparse
    import sys

    def _app(args : argparse.Namespace) -> int:
        cwd = Path.cwd()

        # Check if in version mode
        if args.mode == 'version':
            from . import __version__
            print(__version__)
            return 0

        # The files tickle reads and writes must lie in the current
        # directory, and the ones it writes need a folder to go in
        for name, value in [
            ('agenda', args.agenda), ('depend', args.depend),
            ('cache', args.cache), ('log', args.log)
        ]:
            file_path = Path(cwd, value)
            if not file_path.resolve().is_relative_to(cwd.resolve()):
                parser.error('the %s file %s is not inside the current '
                    'directory %s' % (name, value, cwd))
            if name in ['cache', 'log'] and not file_path.parent.is_dir():
                parser.error('the folder of the %s file %s does not exist' % (
                    name, value
                ))

        # Handle logging
        logging.basicConfig(
            filename = Path(cwd, args.log),
            encoding = 'utf-8',
            level = 'DEBUG' if args.debug else 'INFO',
            format = '%(asctime)s | %(levelname)s | %(message)s'
        )

        # Run specified mode
        if args.mode == 'offline':
            try:
                success = api.offline(
                    cwd,
                    Path(cwd, args.agenda),
                    Path(cwd, args.depend),
                    Path(cwd, args.cache),
                    args.workers
                )
            except KeyboardInterrupt:
                # Tasks that did not finish are not recorded, so the next
                # run performs them again
                log.info('Interrupted; unfinished tasks run again next time')
                return 130
            return 0 if success else 1
        if args.mode == 'online':

            # Make evaluator
            evaluator = api.online(
                cwd,
                Path(cwd, args.agenda),
                Path(cwd, args.depend),
                Path(cwd, args.cache),
                args.workers
            )
            if evaluator is None: return 1

            # Setup user termination
            def _terminate(*args : Any):
                log.info('Ctrl-C registered; terminating ...')
                evaluator.stop()

            signal.signal(signal.SIGINT, _terminate)
            signal.signal(signal.SIGTERM, _terminate)

            # Defer to evaluator
            evaluator.start()

            # Done
            return 0

        if args.mode == 'clean':
            success = api.clean(
                cwd,
                Path(cwd, args.cache),
                Path(cwd, args.agenda),
                Path(cwd, args.depend)
            )
            return 0 if success else 1

        raise RuntimeError('Unknown mode \"%s\"' % args.mode)

    parser = argparse.ArgumentParser(
        prog = 'tickle',
        description = 'Task graph scheduling with asynchronous evaluation.',
        formatter_class = argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        'mode', type = str,
        choices = ['offline', 'online', 'clean', 'version'],
        help = 'Offline mode for an inattentive evaluation mode where file modifications are ignored once tasks have been scheduled. Online mode for an attentive evaluation mode where file creations or modifications trigger a rescheduling of the task graph. Clean mode will delete the files and folders generated during offline or online evaluation, keeping files that are now sources or that changed since they were generated. Version mode will print the tool version.'
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
        default = default_log_path(),
        help = 'Log file location; contains runtime messages, file path must be relative to current working directory'
    )
    sys.exit(_app(parser.parse_args()))
