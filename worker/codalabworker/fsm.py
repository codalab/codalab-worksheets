import Queue
import threading
import time


class State(object):
    """
    The base class for a state in a finite state machine (FSM).
    """
    def update(self, events):
        """
        The update method which is called as part of the primary run loop of a FSM.
        :param events: list of input events, possibly empty
        :return: a tuple of a new state for the FSM and a list of output messages
        """
        return self, []


class MessageQueue(object):
    """
    A simple wrapper around queue for passing messages back and forth between threads
    """
    def __init__(self):
        self._queue = Queue.Queue()

    def put(self, message):
        """
        Puts a new message on the queue
        """
        self._queue.put_nowait(message)

    def put_all(self, messages):
        for message in messages:
            self.put(message)

    def get(self):
        """
        Gets a single message from the queue, removing that message from the queue
        :return: A message or None if there were no messages
        """
        try:
            item = self._queue.get_nowait()
            self._queue.task_done()
            return item
        except Queue.Empty:
            pass
        return None

    def get_all(self):
        """
        Gets all messages from the queue, removing all messages form the queue
        :return: A list of all messages on the queue
        """
        messages = []
        try:
            while True:
                message = self._queue.get_nowait()
                self._queue.task_done()
                messages.append(message)
        except Queue.Empty:
            pass
        return messages


class ThreadedFiniteStateMachine(object):
    """
    A FSM which will run on a separate thread until
    """
    def __init__(self, initial_state, sleep_time=0.1):
        self._state = initial_state
        self._sleep_time = sleep_time
        # TODO Remove these as they are unneeded
        self._input = MessageQueue()
        self._output = MessageQueue()
        # The thread for running this FSM
        self._thread = threading.Thread(target=ThreadedFiniteStateMachine._run, args=[self])
        self._should_run = True

    def _loop(self):
        input_events = self._input.get_all()
        state, output_events = self._state.update(input_events)
        self._output.put_all(output_events)
        self._state = state

    def _run(self):
        while self._should_run and self._state is not None:
            self._loop()
            time.sleep(self._sleep_time)

    def start(self):
        return self._thread.start()

    def stop(self):
        self._should_run = False
