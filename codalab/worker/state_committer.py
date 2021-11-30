import logging

# import os
# import tempfile
# import shutil
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
            if default:
                return default
            else:
                logger.error(f"Failed to commit state: {e}", exc_info=True)
                raise e
        except Exception as e:
            logger.error(f"Failed to commit state: {e}", exc_info=True)
            raise e

    def commit(self, state):
        """ Write out the state in JSON format to a temporary file and rename it into place """
        # TODO: temporary files still randomly disappear. Why can't we just write directly to the JSON file? -Tony
        #       See the following PR for more info: https://github.com/codalab/codalab-worksheets/pull/2765.
        # with tempfile.NamedTemporaryFile(delete=False) as f:
        #     try:
        #         self.temp_file = f.name
        #         f.write(pyjson.dumps(state).encode())
        #         f.flush()
        #         shutil.copyfile(self.temp_file, self._state_file)
        #     finally:
        #         try:
        #             os.unlink(self.temp_file)
        #         except FileNotFoundError:
        #             logger.error(
        #                 "Problem occurred in deleting temp file {} via os.unlink".format(
        #                     self.temp_file
        #                 )
        #             )
        try:
            with open(self._state_file, 'w+') as f:
                f.write(pyjson.dumps(state))
        except Exception as e:
            logger.error(f"Failed to commit state: {e}", exc_info=True)
            raise e
