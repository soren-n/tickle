# External module dependencies
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
from pathlib import Path
from enum import Enum
import logging

###############################################################################
# Enums
###############################################################################
class Event(Enum):
    Created = 'Created'
    Modified = 'Modified'
    Deleted = 'Deleted'
    Moved = 'Moved'

###############################################################################
# Classes
###############################################################################
class FileWatcher(FileSystemEventHandler):
    def __init__(self):
        super().__init__()
        self._observer = Observer()
        self._callbacks = {}
        self._watch_objs = {}
        self._watch_count = {}

    def subscribe(self, file_path, callback):
        if file_path in self._callbacks: return
        self._callbacks[file_path] = callback
        file_dir = file_path.parent
        if file_dir in self._watch_count:
            self._watch_count[file_dir] += 1
            return
        watch = self._observer.schedule(self, file_dir)
        self._watch_objs[file_dir] = watch
        self._watch_count[file_dir] = 1

    def unsubscribe(self, file_path):
        if file_path not in self._callbacks: return
        del self._callbacks[file_path]
        file_dir = file_path.parent
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

    def on_created(self, event):
        src_path = Path(event.src_path)
        if src_path.is_dir(): return
        if src_path not in self._callbacks: return
        logging.debug('FileWatcher.on_created(): %s' % src_path)
        self._callbacks[src_path](Event.Created)

    def on_modified(self, event):
        src_path = Path(event.src_path)
        if src_path.is_dir(): return
        if src_path not in self._callbacks: return
        logging.debug('FileWatcher.on_modified(): %s' % src_path)
        self._callbacks[src_path](Event.Modified)

    def on_moved(self, event):
        src_path = Path(event.src_path)
        if src_path.is_dir(): return
        if src_path not in self._callbacks: return
        logging.debug('FileWatcher.on_moved(): %s' % src_path)
        self._callbacks[src_path](Event.Moved)

    def on_deleted(self, event):
        src_path = Path(event.src_path)
        if src_path.is_dir(): return
        if src_path not in self._callbacks: return
        logging.debug('FileWatcher.on_deleted(): %s' % src_path)
        self._callbacks[src_path](Event.Deleted)
