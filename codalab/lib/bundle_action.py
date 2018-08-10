from codalab.common import PreconditionViolation


class BundleAction(object):
    """
    A bundle action is something that a client sends to a bundle, which gets
    directed to the worker running the bundle.
    A bundle action is serialized as a string, which consists of a sequence of arguments.
    """
    KILL = 'kill'
    WRITE = 'write'

    SEPARATOR = '\t'

    @staticmethod
    def as_string(action):
        if action['type'] == BundleAction.KILL:
            return BundleAction.KILL
        elif action['type'] == BundleAction.WRITE:
            # Note: assume subpath must not have the separator in it.
            return BundleAction.SEPARATOR.join([BundleAction.WRITE, action['subpath'], action['string']])
        else:
            raise PreconditionViolation("Unsupported bundle action %r" % action['type'])
