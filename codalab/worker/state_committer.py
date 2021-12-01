import logging

import os
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
        self.temp_file = None
        self._state_file = json_path

    def load(self, default=None):
        try:
            with open(self._state_file) as json_data:
                return pyjson.load(json_data)
        except (ValueError, EnvironmentError) as e:
            if default is not None:
                logger.warning(f"Failed to load state due to {e}. Returning default: {default}.")
                return default
            logger.error(f"Failed to load state: {e}", exc_info=True)
            raise e

    def commit(self, state):
        """
        Write out the state in JSON format to the state file with an immediate flush and fsync
        to deal with file buffering.
        """
        try:
            with open(self._state_file, 'w+') as f:
                f.write(pyjson.dumps(state))
                f.flush()
                os.fsync(f)
        except Exception as e:
            logger.error(f"Failed to commit state: {e}", exc_info=True)
            raise e
