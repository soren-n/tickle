# External module dependencies
import pickle

###############################################################################
# Classes
###############################################################################
class Cache:
    def __init__(self, cache_path):
        self._path = cache_path
        self._data = self._load() if cache_path.exists() else {}

    def _load(self):
        with self._path.open('rb') as file:
            return pickle.load(file)

    def _store(self):
        with self._path.open('wb+') as file:
            pickle.dump(self._data, file)

    def __setitem__(self, key, value):
        self._data[key] = value

    def __getitem__(self, key):
        return self._data[key]

    def __iter__(self):
        return iter(self._data)

    def flush(self): self._store()
