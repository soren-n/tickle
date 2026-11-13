# External module dependencies
from pathlib import Path
import datetime
import socket
import os

if os.name == 'nt':
    import msvcrt
else:
    import fcntl

###############################################################################
# Project lock
###############################################################################
class LockError(RuntimeError):
    pass

class ProjectLock:
    """Keeps two tickle runs out of one project. The operating system holds
    the lock on an open file and releases it when the process ends, however
    it ends, so a crash leaves no stale lock; the file only says who holds
    it, for the message the other run prints. Byte 0 is the one locked, and
    the description follows it, so it stays readable on Windows, where locks
    keep other processes from reading the locked bytes."""

    def __init__(self, lock_path : Path):
        self._path = lock_path
        self._file = None

    def acquire(self, mode : str):
        lock_file = self._path.open('a+b')
        try:
            if os.name == 'nt':
                lock_file.seek(0)
                msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as error:
            holder = self._holder(lock_file)
            lock_file.close()
            raise LockError(
                'Another tickle run is using this project%s; wait for it to '
                'end' % (' (%s)' % holder if holder else '')
            ) from error
        self._file = lock_file
        lock_file.seek(0)
        lock_file.truncate()
        lock_file.write(('\n%s mode, process %d on %s, started %s\n' % (
            mode, os.getpid(), socket.gethostname(),
            datetime.datetime.now().isoformat(timespec = 'seconds')
        )).encode('utf-8'))
        lock_file.flush()

    def _holder(self, lock_file) -> str:
        try:
            lock_file.seek(1)
            return lock_file.read().decode('utf-8', errors = 'replace').strip()
        except OSError:
            return ''

    def release(self, remove : bool = False):
        if self._file is None: return
        lock_file = self._file
        self._file = None
        try:
            if os.name == 'nt':
                lock_file.seek(0)
                msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        except OSError:
            pass
        lock_file.close()
        if remove:
            try: self._path.unlink()
            except OSError: pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.release()
