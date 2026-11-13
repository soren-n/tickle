# External module dependencies
from collections.abc import Callable
from threading import RLock
from pathlib import Path
from enum import Enum

# Watchdog module dependencies
from watchdog.observers.api import BaseObserver, ObservedWatch
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
# A watch is of a folder, alone or with everything below it
Watch = tuple[Path, bool]

class _Subscription:
    __slots__ = ('callback', 'tree', 'watch')

    def __init__(self, callback : EventHandler, tree : bool, watch : Watch):
        self.callback = callback
        self.tree = tree
        self.watch = watch

class FileWatcher(FileSystemEventHandler):
    """Calls a callback when a subscribed file changes, or for a subscribed
    tree, when anything in a folder changes, at any depth. Watches each
    file's folder, or a tree's folder with everything below it; while that
    folder does not exist, it watches the nearest folder above it that
    does, and moves down as the missing folders appear, and up again when
    they go away. A watched folder that is replaced, deleted and created
    again or renamed over, is watched again, since the old watch may follow
    the folder that went away.

    The observer exists only from start() to stop(): on Linux an observer
    takes an inotify instance and a thread as soon as it is made, and
    gives them back only when it is started and stopped, so a watcher that
    is never started must not make one."""

    def __init__(self):
        super().__init__()
        self._lock = RLock()
        self._observer : BaseObserver | None = None
        self._subscriptions : dict[Path, _Subscription] = {}
        self._watch_objs : dict[Watch, ObservedWatch] = {}
        self._watch_count : dict[Watch, int] = {}

    def _watch(self, watch : Watch):
        if watch in self._watch_count:
            self._watch_count[watch] += 1
            return
        self._watch_count[watch] = 1
        if self._observer is not None: self._schedule(watch)

    def _schedule(self, watch : Watch):
        assert self._observer is not None
        dir_path, recursive = watch
        self._watch_objs[watch] = self._observer.schedule(
            self, str(dir_path), recursive = recursive
        )

    def _unschedule(self, watch : Watch):
        # The watch of a folder that was deleted may be gone already
        old = self._watch_objs.pop(watch, None)
        if old is None or self._observer is None: return
        try: self._observer.unschedule(old)
        except Exception: pass

    def _unwatch(self, watch : Watch):
        self._watch_count[watch] -= 1
        if self._watch_count[watch] != 0: return
        self._unschedule(watch)
        del self._watch_count[watch]

    def _anchor(self, path : Path, tree : bool) -> Watch:
        # A file is watched through its folder; a tree through its own
        # folder, recursively, or while that is missing, the nearest above
        if tree:
            nearest = _nearest_existing(path)
            return nearest, True
        return _nearest_existing(path.parent), False

    def subscribe(self, path : Path, callback : EventHandler, tree : bool = False):
        _path = path.resolve()
        with self._lock:
            if _path in self._subscriptions: return
            watch = self._anchor(_path, tree)
            self._subscriptions[_path] = _Subscription(callback, tree, watch)
            self._watch(watch)

    def unsubscribe(self, path : Path):
        _path = path.resolve()
        with self._lock:
            subscription = self._subscriptions.pop(_path, None)
            if subscription is None: return
            self._unwatch(subscription.watch)

    def _refresh(self, watch : Watch):
        # Watch a folder again, in case its watch followed a folder that
        # was deleted or renamed away
        if self._observer is None or watch not in self._watch_count: return
        self._unschedule(watch)
        try: self._schedule(watch)
        except Exception as error:
            log.debug('FileWatcher: could not watch %s again: %s' % (watch[0], error))

    def _reanchor(self, paths : list[Path]) -> list[tuple[EventHandler, Event]]:
        # Move each watch to the nearest folder that exists now, down as
        # folders appear and up as they go away, and watch again a folder
        # that a directory event touched. Something may have changed while
        # it was not watched, so report what exists; callbacks judge by
        # content.
        result : list[tuple[EventHandler, Event]] = []
        with self._lock:
            refreshed : set[Watch] = set()
            for path, subscription in list(self._subscriptions.items()):
                watch = self._anchor(path, subscription.tree)
                dir_path = subscription.watch[0]
                touched = any(
                    event_path == dir_path or event_path in dir_path.parents
                    for event_path in paths
                )
                if watch == subscription.watch and not touched: continue
                if watch != subscription.watch:
                    self._watch(watch)
                    self._unwatch(subscription.watch)
                    subscription.watch = watch
                elif watch not in refreshed:
                    refreshed.add(watch)
                    self._refresh(watch)
                if path.exists():
                    result.append((subscription.callback, Event.Created))
        return result

    def start(self):
        with self._lock:
            assert self._observer is None
            self._observer = Observer()
            for watch in self._watch_count: self._schedule(watch)
            self._observer.start()

    def stop(self):
        with self._lock:
            observer = self._observer
            self._observer = None
            self._watch_objs.clear()
        if observer is None: return
        observer.stop()
        observer.join()

    def _callbacks_for(self, path : Path) -> list[EventHandler]:
        # The file's own subscription, and those of trees holding it
        with self._lock:
            result : list[EventHandler] = []
            subscription = self._subscriptions.get(path)
            if subscription is not None: result.append(subscription.callback)
            for parent in path.parents:
                subscription = self._subscriptions.get(parent)
                if subscription is not None and subscription.tree:
                    result.append(subscription.callback)
            return result

    def on_any_event(self, event : FileSystemEvent):
        kind = {
            'created': Event.Created,
            'modified': Event.Modified,
            'deleted': Event.Deleted,
            'moved': Event.Moved
        }.get(event.event_type)
        if kind is None: return
        calls : list[tuple[EventHandler, Event]] = []
        paths = [
            Path(str(path))
            for path in (event.src_path, getattr(event, 'dest_path', ''))
            if path
        ]
        if event.is_directory:
            calls = self._reanchor(paths)
        # A move reports the old path as the source and the new one as the
        # destination; saving by renaming a new file over the watched one
        # shows up only as such a move on some platforms. A tree hears of
        # everything below it, folders included.
        for path in paths:
            for callback in self._callbacks_for(path):
                log.debug('FileWatcher: %s %s' % (kind.value, path))
                calls.append((callback, kind))
        seen : list[EventHandler] = []
        for callback, callback_kind in calls:
            if any(callback is other for other in seen): continue
            seen.append(callback)
            # A failing callback must not stop the observer thread, which
            # would end all watching
            try: callback(callback_kind)
            except Exception as error:
                log.error('Failed handling a change to a watched file: '
                    '%s: %s' % (type(error).__name__, error))
