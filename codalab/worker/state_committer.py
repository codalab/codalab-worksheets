import logging
import os
import tempfile
import shutil

from . import pyjson


logger = logging.getLogger(__name__)


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

    @property
    def path(self):
        return self._state_file

    @property
    def state_file_exists(self) -> bool:
        return os.path.isfile(self._state_file)

    def load(self, default=None):
        """
        Loads and reads from state file. If an error occurs, `default` will be returned, if it exists.
        """
        try:
            with open(self._state_file) as json_data:
                return pyjson.load(json_data)
        except (ValueError, EnvironmentError) as e:
            if default is not None:
                logger.warning(
                    f"Failed to load state from {self.path} due to {e}. Returning default: {default}.",
                    exc_info=True,
                )
                return default
            logger.error(f"Failed to load state from {self.path}: {e}", exc_info=True)
            raise e

    def commit(self, state):
        """ Write out the state in JSON format to a temporary file and rename it into place """
        with tempfile.NamedTemporaryFile(delete=False) as f:
            try:
                f.write(pyjson.dumps(state).encode())
                f.flush()
                shutil.copyfile(f.name, self._state_file)
            finally:
                try:
                    os.unlink(f.name)
                except FileNotFoundError:
                    logger.error(
                        "Problem occurred in deleting temp file {} via os.unlink".format(f.name)
                    )
