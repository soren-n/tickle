# External module dependencies
from threading import Timer
from pathlib import Path
import subprocess
import signal
import time
import os

###############################################################################
# Processes
###############################################################################
# How long a stopped task gets to end on its own before it is killed
KILL_GRACE = 5.0

class Outcome:
    """How a command ended: its exit code, or that it timed out or was
    stopped, and how long it ran."""
    def __init__(self,
        returncode : int | None,
        duration : float,
        timed_out : bool = False,
        stopped : bool = False
        ):
        self.returncode = returncode
        self.duration = duration
        self.timed_out = timed_out
        self.stopped = stopped

class Process:
    """A command running in a process group of its own, with its output
    going to a log file. A task's tool may start processes of its own, so
    stopping it signals the whole group: first asking it to end, and after
    a grace period killing what is left."""

    def __init__(self, command : list[str], cwd : Path, log_path : Path):
        self._stopped = False
        self._timer : Timer | None = None
        with log_path.open('wb') as log_file:
            # Tasks get no stdin: one waiting for input would hang the run,
            # and tasks running at the same time would compete for the
            # terminal. A group of its own also keeps Ctrl-C in the terminal
            # from reaching the task directly; tickle stops it instead.
            if os.name == 'nt':
                self._process = subprocess.Popen(
                    command, cwd = cwd,
                    stdin = subprocess.DEVNULL,
                    stdout = log_file, stderr = subprocess.STDOUT,
                    creationflags = getattr(subprocess, 'CREATE_NEW_PROCESS_GROUP', 0)
                )
            else:
                self._process = subprocess.Popen(
                    command, cwd = cwd,
                    stdin = subprocess.DEVNULL,
                    stdout = log_file, stderr = subprocess.STDOUT,
                    start_new_session = True
                )
        self._start = time.monotonic()

    @property
    def pid(self) -> int:
        return self._process.pid

    def wait(self, timeout : float = 0.0) -> Outcome:
        """Wait for the command to end, stopping it after timeout seconds
        if timeout is not 0."""
        timed_out = False
        try: self._process.wait(timeout = timeout if timeout > 0 else None)
        except subprocess.TimeoutExpired:
            timed_out = True
            self.stop()
            self._process.wait()
        if self._timer is not None: self._timer.cancel()
        return Outcome(
            self._process.returncode,
            time.monotonic() - self._start,
            timed_out = timed_out,
            stopped = self._stopped and not timed_out
        )

    def stop(self):
        """Ask the process group to end, and kill it after a grace period;
        returns at once."""
        if self._stopped or self._process.poll() is not None:
            self._stopped = True
            return
        self._stopped = True
        self._signal(kill = False)
        self._timer = Timer(KILL_GRACE, lambda: self._signal(kill = True))
        self._timer.daemon = True
        self._timer.start()

    def _signal(self, kill : bool):
        if self._process.poll() is not None and not kill: return
        try:
            if os.name == 'nt':
                if kill:
                    subprocess.run(
                        ['taskkill', '/F', '/T', '/PID', str(self._process.pid)],
                        stdout = subprocess.DEVNULL, stderr = subprocess.DEVNULL
                    )
                else:
                    self._process.send_signal(signal.CTRL_BREAK_EVENT)
            else:
                os.killpg(self._process.pid, signal.SIGKILL if kill else signal.SIGTERM)
        except (OSError, ValueError):
            # The group is gone already
            pass

def tail(log_path : Path, count : int) -> tuple[list[str], int]:
    """The last count lines of a log file, and how many lines it has."""
    try:
        with log_path.open('rb') as log_file:
            text = log_file.read().decode('utf-8', errors = 'replace')
    except OSError:
        return [], 0
    lines = text.rstrip().splitlines()
    return lines[-count:], len(lines)
