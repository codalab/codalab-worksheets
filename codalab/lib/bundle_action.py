class BundleAction(object):
    KILL = 'kill'
    WRITE = 'write'

    """
    A bundle action is something that a client sends to a bundle, which gets
    directed to the worker running the bundle.
    A bundle action is serialized as a string, which consists of a sequence of arguments.
    """

    @staticmethod
    def kill():
        return BundleAction.KILL

    @staticmethod
    def write(subpath, string):
        return ' '.join([BundleAction.WRITE, subpath, string])
