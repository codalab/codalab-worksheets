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


# TODO Make an implementation which uses a single thread to manage many FSMs
class ThreadedFiniteStateMachine(object):
    """
    A FSM which will run on a separate thread.
    """
    def __init__(self, initial_state):
        self._state = initial_state
        # The thread for running this FSM
        self._thread = threading.Thread(target=ThreadedFiniteStateMachine._run, args=[self])
        self._should_run = True

    def _loop(self):
        state = self._state.update()
        self._state = state

    def _run(self):
        while self._should_run and self._state is not None:
            self._loop()
            if self._state:
                time.sleep(self._state.update_period)

    def start(self):
        """
        Start the thread which runs the FSM
        :return:
        """
        return self._thread.start()

    def stop(self):
        """
        Indicate to the thread which is running the FSM that it should stop
        :return:
        """
        self._should_run = False
