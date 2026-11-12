# External module dependencies
from typing import Any, Dict
from threading import RLock
from pathlib import Path
import pickle
import os

# Internal module dependencies
from . import log

###############################################################################
# Classes
###############################################################################
class Cache:
    def __init__(self, cache_path : Path):
        self._path : Path = cache_path
        # Workers update the cache concurrently; hold the lock while
        # changing its contents and flushing, so a flush never pickles data
        # another thread is changing.
        self.lock = RLock()
        self._data : Dict[str, Any] = (
            self._load() if cache_path.exists() else {}
        )

    def _load(self) -> Dict[str, Any]:
        try:
            with self._path.open('rb') as file:
                return pickle.load(file)
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

    def __setitem__(self, key : str, value : Any):
        with self.lock: self._data[key] = value

    def __getitem__(self, key : str) -> Any:
        return self._data[key]

    def __contains__(self, key : str) -> bool:
        return key in self._data

    def __iter__(self):
        return iter(self._data)

    def flush(self):
        with self.lock: self._store()
