# External module dependencies
from typing import Callable, List, Optional, Tuple, Dict
from threading import RLock
from pathlib import Path
from enum import Enum

# Watchdog module dependencies
from watchdog.observers.api import ObservedWatch
from watchdog.observers import Observer
from watchdog.events import FileSystemEvent, FileSystemEventHandler

# Internal module dependencies
from . import log

###############################################################################
# Enums
###############################################################################
class Event(Enum):
    Created = 'Created'
    Modified = 'Modified'
    Deleted = 'Deleted'
    Moved = 'Moved'

EventHandler = Callable[[Event], None]

###############################################################################
# Functions
###############################################################################
def _nearest_existing(dir_path : Path) -> Path:
    while not dir_path.is_dir() and dir_path.parent != dir_path:
        dir_path = dir_path.parent
    return dir_path

###############################################################################
# Classes
###############################################################################
class FileWatcher(FileSystemEventHandler):
    """Calls a callback when a subscribed file changes. Watches each file's
    folder; while that folder does not exist, it watches the nearest folder
    above it that does, and moves down as the missing folders appear."""

    def __init__(self):
        super().__init__()
        self._lock = RLock()
        self._observer = Observer()
        self._callbacks : Dict[Path, EventHandler] = {}
        self._watched : Dict[Path, Path] = {}
        self._watch_objs : Dict[Path, ObservedWatch] = {}
        self._watch_count : Dict[Path, int] = {}

    def _watch(self, dir_path : Path):
        if dir_path in self._watch_count:
            self._watch_count[dir_path] += 1
            return
        self._watch_objs[dir_path] = self._observer.schedule(self, str(dir_path))
        self._watch_count[dir_path] = 1

    def _unwatch(self, dir_path : Path):
        self._watch_count[dir_path] -= 1
        if self._watch_count[dir_path] != 0: return
        self._observer.unschedule(self._watch_objs[dir_path])
        del self._watch_objs[dir_path]
        del self._watch_count[dir_path]

    def subscribe(self, file_path : Path, callback : EventHandler):
        _file_path = file_path.resolve()
        with self._lock:
            if _file_path in self._callbacks: return
            self._callbacks[_file_path] = callback
            dir_path = _nearest_existing(_file_path.parent)
            self._watched[_file_path] = dir_path
            self._watch(dir_path)

    def unsubscribe(self, file_path : Path):
        _file_path = file_path.resolve()
        with self._lock:
            if _file_path not in self._callbacks: return
            del self._callbacks[_file_path]
            self._unwatch(self._watched.pop(_file_path))

    def _descend(self) -> List[Tuple[EventHandler, Event]]:
        # Move the watches of files whose folders were missing down to the
        # nearest folder that exists now. A file may have been created
        # before its folder was watched, so report the files that exist.
        result : List[Tuple[EventHandler, Event]] = []
        with self._lock:
            for file_path, dir_path in list(self._watched.items()):
                if dir_path == file_path.parent: continue
                nearest = _nearest_existing(file_path.parent)
                if nearest == dir_path: continue
                self._watch(nearest)
                self._unwatch(dir_path)
                self._watched[file_path] = nearest
                if file_path.exists():
                    result.append((self._callbacks[file_path], Event.Created))
        return result

    def start(self):
        self._observer.start()

    def stop(self):
        self._observer.stop()
        self._observer.join()

    def _callback(self, path : Optional[str]) -> Optional[EventHandler]:
        if not path: return None
        with self._lock: return self._callbacks.get(Path(path))

    def on_any_event(self, event : FileSystemEvent):
        kind = {
            'created': Event.Created,
            'modified': Event.Modified,
            'deleted': Event.Deleted,
            'moved': Event.Moved
        }.get(event.event_type)
        if kind is None: return
        calls : List[Tuple[EventHandler, Event]] = []
        if event.is_directory:
            calls = self._descend()
        else:
            # A move reports the old path as the source and the new one as
            # the destination; saving by renaming a new file over the
            # watched one shows up only as such a move on some platforms.
            for path in (event.src_path, getattr(event, 'dest_path', '')):
                callback = self._callback(str(path) if path else None)
                if callback is None: continue
                log.debug('FileWatcher: %s %s' % (kind.value, path))
                calls.append((callback, kind))
        for callback, callback_kind in calls:
            # A failing callback must not stop the observer thread, which
            # would end all watching
            try: callback(callback_kind)
            except Exception as error:
                log.error('Failed handling a change to a watched file: '
                    '%s: %s' % (type(error).__name__, error))
