# External module dependencies
from collections.abc import Callable
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
    above it that does, and moves down as the missing folders appear, and
    up again when they go away. A watched folder that is replaced, deleted
    and created again or renamed over, is watched again, since the old
    watch may follow the folder that went away."""

    def __init__(self):
        super().__init__()
        self._lock = RLock()
        self._observer = Observer()
        self._callbacks : dict[Path, EventHandler] = {}
        self._watched : dict[Path, Path] = {}
        self._watch_objs : dict[Path, ObservedWatch] = {}
        self._watch_count : dict[Path, int] = {}

    def _watch(self, dir_path : Path):
        if dir_path in self._watch_count:
            self._watch_count[dir_path] += 1
            return
        self._watch_objs[dir_path] = self._observer.schedule(self, str(dir_path))
        self._watch_count[dir_path] = 1

    def _unwatch(self, dir_path : Path):
        self._watch_count[dir_path] -= 1
        if self._watch_count[dir_path] != 0: return
        # The watch of a folder that was deleted may be gone already
        try: self._observer.unschedule(self._watch_objs[dir_path])
        except Exception: pass
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

    def _refresh(self, dir_path : Path):
        # Watch a folder again, in case its watch followed a folder that
        # was deleted or renamed away
        watch = self._watch_objs.get(dir_path)
        if watch is None: return
        try: self._observer.unschedule(watch)
        except Exception: pass
        try: self._watch_objs[dir_path] = self._observer.schedule(self, str(dir_path))
        except Exception as error:
            log.debug('FileWatcher: could not watch %s again: %s' % (dir_path, error))

    def _reanchor(self, paths : list[Path]) -> list[tuple[EventHandler, Event]]:
        # Move each file's watch to the nearest folder that exists now, down
        # as folders appear and up as they go away, and watch again a folder
        # that a directory event touched. A file may have changed while it
        # was not watched, so report the files that exist; their callbacks
        # judge by content.
        result : list[tuple[EventHandler, Event]] = []
        with self._lock:
            refreshed : set[Path] = set()
            for file_path, dir_path in list(self._watched.items()):
                nearest = _nearest_existing(file_path.parent)
                touched = any(
                    path == dir_path or path in dir_path.parents
                    for path in paths
                )
                if nearest == dir_path and not touched: continue
                if nearest != dir_path:
                    self._watch(nearest)
                    self._unwatch(dir_path)
                    self._watched[file_path] = nearest
                elif nearest not in refreshed:
                    refreshed.add(nearest)
                    self._refresh(nearest)
                if file_path.exists():
                    result.append((self._callbacks[file_path], Event.Created))
        return result

    def start(self):
        self._observer.start()

    def stop(self):
        self._observer.stop()
        self._observer.join()

    def _callback(self, path : str | None) -> EventHandler | None:
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
        calls : list[tuple[EventHandler, Event]] = []
        if event.is_directory:
            calls = self._reanchor([
                Path(str(path))
                for path in (event.src_path, getattr(event, 'dest_path', ''))
                if path
            ])
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
