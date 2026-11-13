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

class _Terminated(KeyboardInterrupt):
    """Raised in the main thread on SIGTERM, which then ends tickle the way
    Ctrl-C does: running tasks are stopped and the cache is written."""
    pass

def _raise_terminated(*args : Any):
    raise _Terminated()

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
            ('cache', args.cache), ('log', args.log),
            ('log folder', args.log_dir)
        ]:
            file_path = Path(cwd, value)
            if not file_path.resolve().is_relative_to(cwd.resolve()):
                parser.error('the %s file %s is not inside the current '
                    'directory %s' % (name, value, cwd))
            if name in ['cache', 'log'] and not file_path.parent.is_dir():
                parser.error('the folder of the %s file %s does not exist' % (
                    name, value
                ))
            if name in ['cache', 'log'] and file_path.is_dir():
                parser.error('the %s file %s is a folder' % (name, value))
            if name == 'log folder' and file_path.exists() and not file_path.is_dir():
                parser.error('the log folder %s is a file' % value)
        targets = args.targets if len(args.targets) != 0 else None
        if args.mode in ['clean', 'version'] and targets is not None:
            parser.error('%s mode takes no targets' % args.mode)

        # Handle logging
        logging.basicConfig(
            filename = Path(cwd, args.log),
            encoding = 'utf-8',
            level = 'DEBUG' if args.debug else 'INFO',
            format = '%(asctime)s | %(levelname)s | %(message)s'
        )

        # Run specified mode
        if args.mode == 'offline':
            signal.signal(signal.SIGTERM, _raise_terminated)
            try:
                result = api.offline(
                    cwd,
                    Path(cwd, args.agenda),
                    Path(cwd, args.depend),
                    Path(cwd, args.cache),
                    args.workers,
                    keep_going = args.keep_going,
                    targets = targets,
                    dry_run = args.dry_run,
                    log_dir = Path(cwd, args.log_dir)
                )
            except KeyboardInterrupt as interrupt:
                # Running tasks were stopped; tasks that did not finish are
                # not recorded, so the next run performs them again
                log.info('Interrupted; unfinished tasks run again next time')
                return 143 if isinstance(interrupt, _Terminated) else 130
            return 0 if result else 1
        if args.mode == 'online':

            # Make evaluator
            evaluator = api.online(
                cwd,
                Path(cwd, args.agenda),
                Path(cwd, args.depend),
                Path(cwd, args.cache),
                args.workers,
                keep_going = not args.stop_on_failure,
                targets = targets,
                log_dir = Path(cwd, args.log_dir)
            )
            if evaluator is None: return 1

            # Setup user termination; running tasks are stopped
            def _terminate(*args : Any):
                log.info('Stopping ...')
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
        'targets', nargs = '*', metavar = 'TARGET',
        help = 'Run only what these need: task descriptions or output files; all tasks if none are given'
    )
    parser.add_argument(
        '-k', '--keep-going',
        dest = 'keep_going',
        action = 'store_true',
        help = 'Offline mode: after a task fails, keep running the tasks that do not read from it'
    )
    parser.add_argument(
        '--stop-on-failure',
        dest = 'stop_on_failure',
        action = 'store_true',
        help = 'Online mode: after a task fails, start nothing more until the next change'
    )
    parser.add_argument(
        '-n', '--dry-run',
        dest = 'dry_run',
        action = 'store_true',
        help = 'Offline mode: print the tasks that would run, without running them'
    )
    parser.add_argument(
        '--log-dir',
        type = str,
        dest = 'log_dir',
        default = api.default_log_dir(),
        help = 'Folder for the output of each task, one log file per task; must be inside the current working directory'
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
        help = 'Agenda YAML file location; contains the procedure and task definitions, must be inside the current working directory'
    )
    parser.add_argument(
        '-d', '--depend',
        type = str,
        dest = 'depend',
        default = api.default_depend_path(),
        help = 'Depend YAML file location; contains a map of dynamic task dependencies, this file is optional, must be inside the current working directory'
    )
    parser.add_argument(
        '-c', '--cache',
        type = str,
        dest = 'cache',
        default = api.default_cache_path(),
        help = 'Binary cache file location; contains inter-run persistent data, must be inside the current working directory'
    )
    parser.add_argument(
        '-l', '--log',
        type = str,
        dest = 'log',
        default = default_log_path(),
        help = 'Log file location; contains runtime messages, must be inside the current working directory'
    )
    sys.exit(_app(parser.parse_args()))
