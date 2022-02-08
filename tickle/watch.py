# External module dependencies
from typing import Callable, Union, Dict
from pathlib import Path
from enum import Enum

# Watchdog module dependencies
from watchdog.observers.api import ObservedWatch
from watchdog.observers import Observer
from watchdog.events import (
    FileSystemEventHandler,
    FileMovedEvent,
    DirMovedEvent,
    FileModifiedEvent,
    DirModifiedEvent,
    FileCreatedEvent,
    DirCreatedEvent,
    FileDeletedEvent,
    DirDeletedEvent
)
MovedEvent = Union[FileMovedEvent, DirMovedEvent]
ModifiedEvent = Union[FileModifiedEvent, DirModifiedEvent]
CreatedEvent = Union[FileCreatedEvent, DirCreatedEvent]
DeletedEvent = Union[FileDeletedEvent, DirDeletedEvent]

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
# Classes
###############################################################################
class FileWatcher(FileSystemEventHandler):
    def __init__(self):
        super().__init__()
        self._observer : Observer = Observer()
        self._callbacks : Dict[Path, EventHandler] = {}
        self._watch_objs : Dict[Path, ObservedWatch] = {}
        self._watch_count : Dict[Path, int] = {}

    def subscribe(self, file_path : Path, callback : EventHandler):
        _file_path = file_path.resolve()
        if _file_path in self._callbacks: return
        self._callbacks[_file_path] = callback
        file_dir = _file_path.parent
        if file_dir in self._watch_count:
            self._watch_count[file_dir] += 1
            return
        watch = self._observer.schedule(self, file_dir)
        self._watch_objs[file_dir] = watch
        self._watch_count[file_dir] = 1

    def unsubscribe(self, file_path : Path):
        _file_path = file_path.resolve()
        if _file_path not in self._callbacks: return
        del self._callbacks[_file_path]
        file_dir = _file_path.parent
        self._watch_count[file_dir] -= 1
        if self._watch_count[file_dir] == 0:
            watch = self._watch_objs[file_dir]
            self._observer.unschedule(watch)
            del self._watch_objs[file_dir]
            del self._watch_count[file_dir]

    def start(self):
        self._observer.start()

    def stop(self):
        self._observer.stop()
        self._observer.join()

    def on_created(self, event : CreatedEvent):
        src_path = Path(event.src_path)
        if src_path.is_dir(): return
        if src_path not in self._callbacks: return
        log.debug('FileWatcher.on_created(): %s' % src_path)
        self._callbacks[src_path](Event.Created)

    def on_modified(self, event : ModifiedEvent):
        src_path = Path(event.src_path)
        if src_path.is_dir(): return
        if src_path not in self._callbacks: return
        log.debug('FileWatcher.on_modified(): %s' % src_path)
        self._callbacks[src_path](Event.Modified)

    def on_moved(self, event : MovedEvent):
        src_path = Path(event.src_path)
        if src_path.is_dir(): return
        if src_path not in self._callbacks: return
        log.debug('FileWatcher.on_moved(): %s' % src_path)
        self._callbacks[src_path](Event.Moved)

    def on_deleted(self, event : DeletedEvent):
        src_path = Path(event.src_path)
        if src_path.is_dir(): return
        if src_path not in self._callbacks: return
        log.debug('FileWatcher.on_deleted(): %s' % src_path)
        self._callbacks[src_path](Event.Deleted)
