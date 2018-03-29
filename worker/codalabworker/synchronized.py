"""
Java-like synchronization primitives in Python

source: http://theorangeduck.com/page/synchronized-python
http://blog.dscpl.com.au/2014/01/the-synchronized-decorator-as-context.html
"""

import thread
import threading
import types

class function_wrapper(object_proxy):
    def __init__(self, wrapped):
       super(function_wrapper, self).__init__(wrapped)
    def __get__(self, instance, owner):
        wrapped = self.wrapped.__get__( instance, owner)
        return bound_function_wrapper(wrapped)
    def __call__(self, *args, **kwargs):
        return self.wrapped(*args, **kwargs)

def synchronized(wrapped):
    def _synchronized_lock(owner):
        lock = vars(owner).get('_synchronized_lock', None)
        if lock is None:
            meta_lock = vars(synchronized).setdefault(
                    '_synchronized_meta_lock', threading.Lock())
            with meta_lock:
                lock = vars(owner).get('_synchronized_lock', None)
                if lock is None:
                    lock = threading.RLock()
                    setattr(owner, '_synchronized_lock', lock)
        return lock

    def _synchronized_wrapper(wrapped, instance, args, kwargs):
        with _synchronized_lock(instance or wrapped):
            return wrapped(*args, **kwargs)

    class _synchronized_function_wrapper(function_wrapper):
        def __enter__(self):
            self._lock = _synchronized_lock(self.wrapped)
            self._lock.acquire()
            return self._lock

        def __exit__(self, *args):
            self._lock.release()

    return _synchronized_function_wrapper(wrapped, _synchronized_wrapper)

'''
def synchronized_with_attr(lock_name):

    def decorator(method):

        def synced_method(self, *args, **kws):
            lock = getattr(self, lock_name)
            with lock:
                return method(self, *args, **kws)

        return synced_method

    return decorator


def syncronized_with(lock):

    def synchronized_obj(obj):

        if type(obj) is types.FunctionType:

            obj.__lock__ = lock

            def func(*args, **kws):
                with lock:
                    obj(*args, **kws)
            return func

        elif type(obj) is types.ClassType:

            orig_init = obj.__init__
            def __init__(self, *args, **kws):
                self.__lock__ = lock
                orig_init(self, *args, **kws)
            obj.__init__ = __init__

            for key in obj.__dict__:
                val = obj.__dict__[key]
                if type(val) is types.FunctionType:
                    decorator = syncronized_with(lock)
                    obj.__dict__[key] = decorator(val)

            return obj

    return synchronized_obj


def synchronized(item):

    if type(item) is types.StringType:
        decorator = synchronized_with_attr(item)
        return decorator(item)

    if type(item) is thread.LockType:
        decorator = syncronized_with(item)
        return decorator(item)

    else:
        new_lock = threading.Lock()
        decorator = syncronized_with(new_lock)
        return decorator(item)
'''
