"""
docker_util.py

Utility class wrapping the docker executable
"""
import re
from subprocess import Popen, PIPE


class Docker(object):
    """
    Wrapper class. Various class methods defined in here serve as wrappers for functionality that calls out to
    the `docker` binary installed on the host.
    """

    """
    Compiled regular expression to parse the stdout of `docker search` calls.

    This captures the beginning of the line up until the first whitespace character, i.e.
    the first column of the output, corresponding to the image tag.
    """
    DOCKER_SEARCH_TAG_REGEX = re.compile(r'^(?P<tag>\S+)\s+')

    @classmethod
    def search(cls, keyword, failure_cb=None):
        """
        Performs `docker search <keyword>`. Returns tuple of image tags matching the search keyword.
        Also accepts an optional `failure_cb`, which is a function that is called with the return code of the
        process and the process' stderr.
        """
        docker = Popen(['/usr/bin/env', 'docker', 'search', keyword], stdout=PIPE, stderr=PIPE)
        if docker.wait() != 0 and failure_cb is not None:
            failure_cb(docker.returncode, docker.stderr.read())
        else:
            return (cls.DOCKER_SEARCH_TAG_REGEX.match(line).group('tag') for line in docker.stdout.readlines()[1:])

