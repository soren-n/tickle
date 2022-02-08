# External module dependencies
from typing import Any, Dict
from pathlib import Path
import pickle

###############################################################################
# Classes
###############################################################################
class Cache:
    def __init__(self, cache_path : Path):
        self._path : Path = cache_path
        self._data : Dict[str, Any] = (
            self._load() if cache_path.exists() else {}
        )

    def _load(self) -> Dict[str, Any]:
        with self._path.open('rb') as file:
            return pickle.load(file)

    def _store(self):
        with self._path.open('wb+') as file:
            pickle.dump(self._data, file)

    def __setitem__(self, key : str, value : Any):
        self._data[key] = value

    def __getitem__(self, key : str) -> Any:
        return self._data[key]

    def __iter__(self):
        return iter(self._data)

    def flush(self): self._store()
