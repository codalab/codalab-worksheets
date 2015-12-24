"""
docker_util.py

Utility class wrapping the docker executable
"""
from argcomplete import warn
import docker
from docker.utils import kwargs_from_env
from requests.exceptions import RequestException


class Docker(object):
    DOCKER_CLIENT = docker.Client(**kwargs_from_env(assert_hostname=False))

    """
    Wrapper class. Various class methods defined in here serve as wrappers for functionality that calls out to
    the `docker` binary installed on the host.
    """

    """
    Compiled regular expression to parse the stdout of `docker search` calls.

    This captures the beginning of the line up until the first whitespace character, i.e.
    the first column of the output, corresponding to the image tag.
    """

    @classmethod
    def search(cls, keyword, failure_cb=None):
        """
        Performs `docker search <keyword>`. Returns tuple of image tags matching the search keyword.
        Also accepts an optional `failure_cb`, which is a function that is called with the exception
        that was thrown
        """
        try:
            return (image['name'] for image in cls.DOCKER_CLIENT.search(keyword))
        except RequestException as e:
            failure_cb(e)

