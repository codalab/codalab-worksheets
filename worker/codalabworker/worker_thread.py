import copy
import threading


class WorkerThread(object):
    """
    Keeps track of a thread and related information fields
    """

    def __init__(self, thread, fields=None):
        """
        Creates a new ThreadDict.
        :param thread: Thread object
        :param fields: A Dict from strings(field names) to values
        """
        if fields and 'thread' in fields:
            raise KeyError('Field name \'thread\' reserved for threads')
        self.thread = thread
        self.fields = fields

    def __getitem__(self, key):
        """
        Getitem acts on the fields dict, treating the 'thread' key as a special key
        """
        if key == 'thread':
            return self.thread
        else:
            return self.fields[key]

    def __setitem__(self, key, val):
        """
        Setitem acts on the fields dict, treating the 'thread' key as a special key
        """
        if key == 'thread':
            self.thread = val
        else:
            self.fields[key] = val

    def is_alive(self):
        """
        is_alive acts directly on the thread object
        """
        return self.thread.is_alive()

    def join(self):
        """
        join acts directly on the thread object
        """
        return self.thread.join()

    def start(self):
        """
        start acts directly on the thread object
        """
        return self.thread.start()


class ThreadDict(dict):
    """
    Class for worker components to keep track of various threads that may succeed or fail
    and may have other fields to keep track of

    Can be used like a regular dict with:
        dict[key]['thread'] referring to the thread and
        dict[key][<field_name>] referring to any of the fields
    """

    def __init__(self, fields=None, lock=False):
        """
        Creates a new ThreadDict.
        :param fields: A Dict from strings(field names) to initial values
        :param lock: A bool that if true adds an RLock field for each entry
        """
        dict.__init__(self)
        self._initial_fields = fields
        self._lock = lock

    def add_if_new(self, key, thread):
        """
        Add the given thread with the given key to the dict and start the thread
            IF the key is not already in the dict
        :param key: key to refer to this thread
        :param thread: thread to be added to the dict and to be started
        """
        if key not in self:
            self.add_thread(key, thread)

    def add_thread(self, key, thread):
        """
        Add the given thread with the given key to the dict and start the thread
        :param key: key to refer to this thread
        :param thread: thread to be added to the dict and to be started
        """
        new_fields = copy.deepcopy(self._initial_fields)
        if self._lock:
            new_fields['lock'] = threading.RLock()
        new_thread = WorkerThread(thread=thread, fields=new_fields)
        self[key] = new_thread
        new_thread.start()

    def remove(self, key):
        """
        Joins and removes the thread with key from the dict if it exists
        """
        if key in self:
            self[key].join()
            del self[key]

    def stop(self):
        """
        Joins and removes all the threads in the dict
        """
        for key in self.keys():
            self[key].join()
