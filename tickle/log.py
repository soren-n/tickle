import logging

def info(msg : str):
    logging.info(msg)
    print('[tickle] %s' % msg)

def error(msg : str):
    logging.error(msg)
    print('[tickle] Error: %s' % msg)

def critical(msg : str):
    logging.critical(msg)
    print('[tickle] Critical: %s' % msg)

def debug(msg : str):
    logging.debug(msg)
