# External module dependencies
from threading import Condition

###############################################################################
# Classes
###############################################################################
class SingleWriteMultipleReadLock:
    """Many readers or one writer; a waiting writer blocks new readers."""
    def __init__(self):
        self._condition = Condition()
        self._reader_count = 0
        self._writer = False

    def acquire_write(self):
        with self._condition:
            while self._writer:
                self._condition.wait()
            self._writer = True
            while self._reader_count != 0:
                self._condition.wait()

    def release_write(self):
        with self._condition:
            self._writer = False
            self._condition.notify_all()

    def acquire_read(self):
        with self._condition:
            while self._writer:
                self._condition.wait()
            self._reader_count += 1

    def release_read(self):
        with self._condition:
            self._reader_count -= 1
            if self._reader_count == 0:
                self._condition.notify_all()
