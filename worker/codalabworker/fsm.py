from collections import namedtuple
import threading
import os
import tempfile
import shutil

import pyjson
from synchronized import synchronized

class BaseStateCommitter(object):

    def load(self):
        """ Load and return the state """
        raise NotImplementedError

    def commit(self, state):
        """ Commit the state """
        raise NotImplementedError

class JsonStateCommitter(BaseStateCommitter):
    def __init__(self, json_path, schema=None):
        self._state_file = json_path

    def load(self, default={}):
        if not os.path.exists(self._state_file):
            return default
        with open(self._state_file) as json_data:
            return pyjson.load(json_data)

    def commit(self, state):
        """ Write out the state in JSON format to a temporary file and rename it into place """

        dirname, basename = os.path.split(self._state_file)
        with tempfile.NamedTemporaryFile('w', prefix=basename, dir=dirname, delete=False) as f:
            pyjson.dump(state, f)
            f.flush()
            shutil.copy2(f.name, self._state_file)

class DependencyStatus(object):
    # reset: -> STARTING
    # transition: decide directories (if not fixed) -> DOWNLOADING
    STARTING = 'STARTING'

    # reset: -> STARTING
    # transition: if not in _downloading, create and start _downloading thread.
    # thread recreates directories and does download
    # If not done -> DOWNLOADING else -> READY
    DOWNLOADING = 'DOWNLOADING'

    # reset: -> READY
    # transition: -> READY
    READY = 'READY'

    # reset: -> FAILED
    # transition: -> FAILED
    FAILED = 'FAILED'

class BaseDependencyManager(object):

    def run(self):
        """ Start execution of this dependency manager. Returns immediately """
        raise NotImplementedError

    def has(self, dependency):
        """ Return whether or not the manager has the dependency downloaded and ready """
        raise NotImplementedError

    def get(self, dependency):
        """
        Download the dependency if not self.has(dependency) in a non-blocking manner
        """
        raise NotImplementedError

    def list_all(self):
        """ Return a list all dependencies that are ready """
        raise NotImplementedError

    def stop(self):
        """ Stop execution of this running dependency manager. Blocks until stopped """
        raise NotImplementedError

class BaseStateHandler(object):
    def reset(self, state):
        raise NotImplementedError
        return new_state

    def transition(self, state):
        raise NotImplementedError
        return new_state

class BaseStateManager(object):

    def __init__(self):
        self._handlers = {}

    def register_handler(self, handler):
        self._handlers[handler.status_name] = handler
