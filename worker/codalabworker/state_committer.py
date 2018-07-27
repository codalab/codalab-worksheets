import tempfile
import os
import shutil
import pyjson


class BaseStateCommitter(object):
    def load(self):
        """ Load and return the state """
        raise NotImplementedError

    def commit(self, state):
        """ Commit the state """
        raise NotImplementedError


class JsonStateCommitter(BaseStateCommitter):
    def __init__(self, json_path):
        self._state_file = json_path

    def load(self, default=None):
        if not os.path.exists(self._state_file):
            return dict() if default is None else default
        with open(self._state_file) as json_data:
            return pyjson.load(json_data)

    def commit(self, state):
        """ Write out the state in JSON format to a temporary file and rename it into place """

        with tempfile.NamedTemporaryFile() as f:
            f.write(pyjson.dumps(state))
            f.flush()
            shutil.copyfile(f.name, self._state_file)
