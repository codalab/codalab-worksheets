from codalab.common import BundleRuntime
from .kubernetes_runtime import KubernetesRuntime
from ..docker_utils import DockerRuntime


class Runtime:
    """Base class for a runtime."""

    pass


def get_runtime(runtime_name: str):
    """Gets the appropriate runtime."""
    if runtime_name == BundleRuntime.KUBERNETES.value:
        return KubernetesRuntime
    else:
        return DockerRuntime
