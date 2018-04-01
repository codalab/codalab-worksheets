"""
Java-like synchronization primitives in Python

source: http://theorangeduck.com/page/synchronized-python
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

'''
def decorator(wrapper):
    def _wrapper(wrapped, instance, args, kwargs):
        def _execute(wrapped):
            if instance is None:
                return function_wrapper(wrapped, wrapper)
            elif inspect.is
'''

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
