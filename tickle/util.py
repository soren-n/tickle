# External module dependencies
from threading import Lock, Condition

###############################################################################
# Classes
###############################################################################
class SingleWriteMultipleReadLock:
    def __init__(self):
        self._access = Lock()
        self._writer = Condition()
        self._reader_count = 0
        self._waiting = False

    def acquire_write(self):
        self._access.acquire()
        while self._reader_count != 0:
            self._waiting = True
            self._writer.wait()
        self._waiting = False

    def release_write(self):
        self._access.release()

    def acquire_read(self):
        self._access.acquire()
        self._access.release()
        self._reader_count += 1

    def release_read(self):
        self._reader_count -= 1
        if self._waiting:
            self._writer.notify_all()
