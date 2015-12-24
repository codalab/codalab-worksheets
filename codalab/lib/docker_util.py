"""
docker_util.py

Utility class wrapping the docker executable
"""
from argcomplete import warn
import docker
from docker.utils import kwargs_from_env
from requests.exceptions import RequestException


class Docker(object):
    """
    Wrapper class. Various class methods defined in here serve as wrappers for functionality that calls out to
    the `docker` binary installed on the host.
    """

    """
    Docker REST client, makes calls out to docker-py, which in turn dispatches to Docker's Remote REST API.
    assert_hostname is disabled to avoid SSL verification issues.
    """
    DOCKER_CLIENT = docker.Client(**kwargs_from_env(assert_hostname=False))

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

