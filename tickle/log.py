import logging

def info(msg):
    logging.info(msg)
    print('[tickle] %s' % msg)

def error(msg):
    logging.error(msg)
    print('[tickle] Error: %s' % msg)

def critical(msg):
    logging.critical(msg)
    print('[tickle] Critical: %s' % msg)
