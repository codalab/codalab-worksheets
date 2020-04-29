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
        self._terminal_states = []  # stage_name

    def add_terminal(self, stage_name):
        if stage_name not in self._transition_functions and stage_name not in self._terminal_states:
            self._terminal_states.append(stage_name)
        else:
            raise Exception('Stage name already exists!')

    def transition(self, state):
        """ Return the updated state """
        if state.stage in self._terminal_states:
            return state
        return self._transition_functions[state.stage](state)

    def add_transition(self, stage_name, transition_function):
        if stage_name not in self._transition_functions and stage_name not in self._terminal_states:
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

    def get(self, uuid, dependency):
        """
        Start downloading the corresponding dependency if not already in progress.
        Register that the given uuid is a dependent of this dependency.
        Return the corresponding DependencyState.
        """
        raise NotImplementedError

    def release(self, uuid, digest):
        """
        Register that the run with uuid is no longer dependent on this dependency
        If no more runs are dependent on this dependency image, kill it
        """
        raise NotImplementedError

    def list_all(self):
        """ Return a list all dependencies that are ready """
        raise NotImplementedError

    def stop(self):
        """ Stop execution of this running dependency manager. Blocks until stopped """
        raise NotImplementedError
