import tempfile
import shutil
from . import pyjson


class BaseStateCommitter(object):
    def load(self):
        """ Load and return the state """
        raise NotImplementedError

    def commit(self, state):
        """ Commit the state """
        raise NotImplementedError


class JsonStateCommitter(BaseStateCommitter):
    def __init__(self, json_path, pre_commit_transform=None, post_load_transform=None):
        self._state_file = json_path
        self._pre_commit_transform = pre_commit_transform
        self._post_load_transform = post_load_transform

    def load(self, default=None):
        try:
            with open(self._state_file) as json_data:
                data = pyjson.load(json_data)
                if self._post_load_transform:
                    data = self._post_load_transform(data)
                return data
        except (ValueError, EnvironmentError):
            return dict() if default is None else default

    def commit(self, state):
        """ Write out the state in JSON format to a temporary file and rename it into place """
        if self._pre_commit_transform:
            state = self._pre_commit_transform(state)
        with tempfile.NamedTemporaryFile() as f:
            f.write(pyjson.dumps(state).encode())
            f.flush()
            shutil.copyfile(f.name, self._state_file)
