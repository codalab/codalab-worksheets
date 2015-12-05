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
    def kill():
        return BundleAction.KILL

    @staticmethod
    def write(subpath, string):
        # Note: assume subpath must not have the separator in it.
        return BundleAction.SEPARATOR.join([BundleAction.WRITE, subpath, string])
