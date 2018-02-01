import threading
import time


class State(object):
    """
    The base class for a state in a finite state machine (FSM).
    """

    # The state to return from update when the FSM is complete
    DONE = None

    def update(self):
        """
        The update method which is called as part of the primary run loop of a FSM.
        :return: a tuple of a new state for the FSM and a list of output messages
        """
        return self

    @property
    def update_period(self):
        """
        How frequently `update` should be called while in this state
        :return: the period in seconds
        """
        return 0.1


class FiniteStateMachine(object):
    """
    A FSM which will run until it reaches the State.DONE state
    """
    def __init__(self, initial_state, done_check=lambda s: s == State.DONE):
        self._state = initial_state
        self._should_run = True
        self._done_check = done_check

    def run(self):
        """
        Run this finite state machine until it reaches the State.DONE state
        """
        while self._should_run and not self._done_check(self._state):
            state = self._state.update()
            self._state = state
            if state:
                time.sleep(float(self._state.update_period))

    def stop(self):
        """
        Make this FSM stop gracefully.
        """
        self._should_run = False


# TODO Make an implementation which uses a single thread to manage many FSMs
class ThreadedFiniteStateMachine(FiniteStateMachine):
    """
    A FSM which will run on a separate thread.
    """
    def __init__(self, initial_state, daemonic=False):
        super(ThreadedFiniteStateMachine, self).__init__(initial_state)
        # The thread for running this FSM
        self._thread = threading.Thread(target=FiniteStateMachine.run, args=[self])
        self._thread.setDaemon(daemonic)

    def start(self):
        """
        Start the thread which runs the FSM asynchronously
        """
        return self._thread.start()

    @property
    def is_alive(self):
        return self._thread.isAlive()
