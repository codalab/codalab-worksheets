from types import SimpleNamespace
from codalab.worker.fsm import DependencyStage


class NoOpImageManager:
    """A "no-op" ImageManager. Doesn't do any downloading of images.
    This is used by the Kubernetes runtime, because Kubernetes itself will take care of image downloading once
    a pod is launched later.
    """

    def start(self):
        pass

    def stop(self):
        pass

    def get(self, image_spec: str):
        return SimpleNamespace(stage=DependencyStage.READY, digest=image_spec)
