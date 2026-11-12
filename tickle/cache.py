# External module dependencies
from typing import Any, Dict
from threading import RLock
from pathlib import Path
import pickle
import time
import os

# Internal module dependencies
from . import log

###############################################################################
# Classes
###############################################################################
# How often changes are written out at most while tasks finish; a write that
# is lost only makes the tasks it records run again.
_WRITE_INTERVAL = 0.5

class Cache:
    def __init__(self, cache_path : Path):
        self._path : Path = cache_path
        # Workers update the cache concurrently; hold the lock while
        # changing its contents and flushing, so a flush never pickles data
        # another thread is changing.
        self.lock = RLock()
        self._dirty : bool = False
        self._written : float = 0.0
        self._data : Dict[str, Any] = (
            self._load() if cache_path.exists() else {}
        )

    def _load(self) -> Dict[str, Any]:
        try:
            with self._path.open('rb') as file:
                data = pickle.load(file)
            if not isinstance(data, dict):
                raise TypeError('expected a dict, got %s' % type(data).__name__)
            return data
        except Exception as error:
            # The cache only saves work; a damaged one means running again.
            log.error('Ignoring unreadable cache %s: %s' % (self._path, error))
            return {}

    def _store(self):
        # Write to a temporary file and rename it over the cache, so an
        # interrupted write never leaves a truncated cache behind.
        temp_path = self._path.with_name(self._path.name + '.tmp')
        with temp_path.open('wb') as file:
            pickle.dump(self._data, file)
        os.replace(temp_path, self._path)
        self._dirty = False
        self._written = time.monotonic()

    def __setitem__(self, key : str, value : Any):
        with self.lock: self._data[key] = value

    def __getitem__(self, key : str) -> Any:
        return self._data[key]

    def __contains__(self, key : str) -> bool:
        return key in self._data

    def __iter__(self):
        return iter(self._data)

    def get(self, key : str, default : Any = None) -> Any:
        return self._data.get(key, default)

    def clear(self):
        with self.lock: self._data = {}

    def changed(self):
        """Record that the contents changed; they are written out now if the
        last write was long enough ago, and otherwise by a later call to
        changed() or flush()."""
        with self.lock:
            self._dirty = True
            if time.monotonic() - self._written < _WRITE_INTERVAL: return
            self._store()

    def flush(self):
        """Write out any changes not yet written."""
        with self.lock:
            if not self._dirty: return
            self._store()

    def store(self):
        """Write out the contents now."""
        with self.lock: self._store()
