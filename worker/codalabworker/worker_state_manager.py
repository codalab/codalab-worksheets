import threading
import os
import time
import traceback
import logging
import json

from formatting import size_str

logger = logging.getLogger(__name__)

class WorkerStateManager(object):
    """
    Manages the state of Runs on the worker
    DependencyManager should be instantiated before DockerImageManager, to
    ensure that the work directory already exists.
    """
    STATE_FILENAME = 'worker-state.json'

    def __init__(self, work_dir, shared_file_system=False):
        self._state_file = os.path.join(work_dir, self.STATE_FILENAME)
        self._lock = threading.Lock()

        # Dictionary from UUID to Run that keeps track of bundles currently
        # running. These runs are added to this dict inside _run, and removed
        # when the Run class calls finish_run.
        self._runs = {}
        self._runs_lock = threading.Lock()

        self.previous_runs = {}
        if not os.path.exists(self._state_file):
            if not os.path.exists(work_dir):
                os.makedirs(work_dir, 0770)
            self.save_state()
        self.load_state()

    def _get_run(self, uuid):
        with self._runs_lock:
            return self._runs.get(uuid)

    def has_runs(self):
        with self._lock:
            return True if self._runs else False

    def finish_run(self, uuid):
        with self._runs_lock:
            del self._runs[uuid]

    def add_run(self, uuid, run):
        with self._runs_lock:
            self._runs[uuid] = run

    def resume_previous_runs(self, run_deserializer):
        with self._runs_lock:
            for uuid, run_info in self.previous_runs.items():
                run = run_deserializer(run_info)
                run.resume()
                self._runs[uuid] = run
        self.previous_runs = {}

    def load_state(self):
        with self._lock:
            with open(self._state_file, 'r') as f:
                state = json.load(f)
                for uuid, run_info in state['runs'].items():
                    self.previous_runs[uuid] = run_info

    def save_state(self):
        # In case we're initializing the state for the first time
        state = {
            'runs': {}
        }

        with self._lock:
            for uuid, run in self._runs.items():
                state['runs'][uuid] = run.serialize()

            with open(self._state_file, 'w') as f:
                json.dump(state, f)


