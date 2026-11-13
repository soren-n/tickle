import logging

# Tickle logs to its own logger rather than the root logger: calling the
# root logger's functions configures logging for the whole process when it
# has no handlers yet, which would override the host script's own setup.
# The null handler keeps Python from printing records to stderr when nobody
# configured logging; progress is printed to stdout below anyway.
_logger = logging.getLogger('tickle')
_logger.addHandler(logging.NullHandler())

# Flushed, so progress shows up as it happens when stdout is a pipe or file
def _print(msg : str):
    print('[tickle] %s' % msg, flush = True)

def info(msg : str):
    _logger.info(msg)
    _print(msg)

def warning(msg : str, detail : str = '', tail : int = 0):
    """Log msg with the whole of detail, and print msg with only the last
    tail lines of detail, or all of it when tail is 0."""
    detail = detail.rstrip()
    if len(detail) == 0:
        _logger.warning(msg)
        _print('Warning: %s' % msg)
        return
    _logger.warning('%s:\n%s' % (msg, detail))
    lines = detail.splitlines()
    if tail == 0 or len(lines) <= tail:
        _print('Warning: %s:\n%s' % (msg, detail))
        return
    _print('Warning: %s; last %d of %d lines (all in the log):\n%s' % (
        msg, tail, len(lines), '\n'.join(lines[-tail:])
    ))

def error(msg : str):
    _logger.error(msg)
    _print('Error: %s' % msg)

def critical(msg : str):
    _logger.critical(msg)
    _print('Critical: %s' % msg)

def debug(msg : str):
    _logger.debug(msg)
