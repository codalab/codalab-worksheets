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
        # TODO: Do we want to add a schema to this?
        self._state_file = json_path

    def load(self, default=None):
        if not os.path.exists(self._state_file):
            return dict() if default is None else default
        with open(self._state_file) as json_data:
            return pyjson.load(json_data)

    def commit(self, state):
        """ Write out the state in JSON format to a temporary file and rename it into place """

        dirname, basename = os.path.split(self._state_file)
        tempname = self._state_file + '.new'
        with open(tempname, 'w') as f:
            f.write(pyjson.dumps(state))
            f.flush()
            shutil.move(f.name, self._state_file)


class DependencyStage(object):
    '''
    Defines the finite set of possible stages and transition functions
    Note that it is important that each state be able to be re-executed
    without unintended adverse effects (which happens upon manager resume)
    '''

    # if not in _downloading, create and start _downloading thread.
    # (thread recreates directories and downloads)
    # if thread still alive -> DOWNLOADING else -> READY
    DOWNLOADING = 'DOWNLOADING'

    # -> READY
    READY = 'READY'

    # -> FAILED
    FAILED = 'FAILED'


class StateTransitioner(object):
    def __init__(self):
        self._transition_functions = {}  # stage_name -> transition_function

    def transition(self, state):
        """ Return the updated state """
        return self._transition_functions[state.stage](state)

    def add_transition(self, stage_name, transition_function):
        if stage_name not in self._transition_functions:
            self._transition_functions[stage_name] = transition_function
        else:
            raise Exception('Stage name already exists!')


class BaseDependencyManager(object):

    def run(self):
        """ Start execution of this dependency manager. Returns immediately """
        raise NotImplementedError

    def has(self, dependency):
        """ Return whether or not the corresponding DependencyState exists in the manager """
        raise NotImplementedError

    def get(self, dependency):
        """
        Start downloading the corresponding dependency if not already in progress.
        Return the corresponding DependencyState.
        """
        raise NotImplementedError

    def list_all(self):
        """ Return a list all dependencies that are ready """
        raise NotImplementedError

    def stop(self):
        """ Stop execution of this running dependency manager. Blocks until stopped """
        raise NotImplementedError
