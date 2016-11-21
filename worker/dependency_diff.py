"""
Shared utility functions to hash, diff, and patch dependency lists.

Dependency lists are assumed to be lists of 2-tuples.
These implementations currently focus on readability rather than performance,
so there is a lot of room for optimization.

Future changes to the hashing scheme need not be backwards-compatible, since
it is very unlikely to get a hash collision between the new and old schemes,
forcing the dependencies to the fully resynced.
"""
import hashlib


def hash_dependencies(dependencies):
    dependencies.sort()
    compact = ':'.join([d[0] + '/' + d[1] for d in dependencies])
    h = hashlib.md5()
    h.update(compact)
    return h.hexdigest()


def diff_dependencies(old, new):
    old = set(old)
    new = set(new)
    return {
        '+': list(new - old),
        '-': list(old - new),
    }


def patch_dependencies(dependencies, patch):
    to_remove = set(patch['-'])
    result = [d for d in dependencies if d not in to_remove]
    result.extend(patch['+'])
    return result
