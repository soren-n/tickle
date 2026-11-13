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

def warning(msg : str):
    _logger.warning(msg)
    _print('Warning: %s' % msg)

def error(msg : str):
    _logger.error(msg)
    _print('Error: %s' % msg)

def critical(msg : str):
    _logger.critical(msg)
    _print('Critical: %s' % msg)

def debug(msg : str):
    _logger.debug(msg)
