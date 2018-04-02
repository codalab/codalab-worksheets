"""
Java-like synchronization primitives in Python

http://blog.dscpl.com.au/2014/01/the-synchronized-decorator-as-context.html
"""

import thread
import threading
import types
import functools

class object_proxy(object):
    def __init__(self, wrapped):
        self.wrapped = wrapped
        try:
            self.__name__ = wrapped.__name__
        except AttributeError:
            pass

    @property
    def __class__(self):
        return self.wrapped.__class__

    def __getattr__(self, name):
        return getattr(self.wrapped, name)

class function_wrapper(object_proxy):
    def __init__(self, wrapped, wrapper):
        super(function_wrapper, self).__init__(wrapped)
        self.wrapper = wrapper
        if isinstance(wrapped, classmethod):
            self.binding = 'classmethod'
        elif isinstance(wrapped, staticmethod):
            self.binding = 'staticmethod'
        else:
            self.binding = 'function'

    def __get__(self, instance, owner):
        wrapped = self.wrapped.__get__(instance, owner)
        return bound_function_wrapper(wrapped, instance, self.wrapper,
                self.binding, self)

    def __call__(self, *args, **kwargs):
        return self.wrapper(self.wrapped, None, args, kwargs)

class bound_function_wrapper(object_proxy):
    def __init__(self, wrapped, instance, wrapper, binding, parent):
        super(bound_function_wrapper, self).__init__(wrapped)
        self.instance = instance
        self.wrapper = wrapper
        self.binding = binding
        self.parent = parent

    def __call__(self, *args, **kwargs):
        if self.binding == 'function':
            if self.instance is None:
                instance, args = args[0], args[1:]
                wrapped = functools.partial(self.wrapped, instance)
                return self.wrapper(wrapped, instance, args, kwargs)
            else:
                return self.wrapper(self.wrapped, self.instance, args, kwargs)
        else:
            instance = getattr(self.wrapped, '__self__', None)
            return self.wrapper(self.wrapped, instance, args, kwargs)

    def __get__(self, instance, owner):
        if self.instance is None and self.binding == 'function':
            descriptor = self.parent.wrapped.__get__(instance, owner)
            return bound_function_wrapper(descriptor, instance, self.wrapper,
                    self.binding, self.parent)
        return self

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

