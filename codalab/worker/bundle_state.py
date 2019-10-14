class State(object):
    """
    An enumeration of states that a bundle can be in.
    """

    # Waiting for contents to be uploaded
    UPLOADING = 'uploading'
    # Just created
    CREATED = 'created'
    # All the dependencies are met
    STAGED = 'staged'
    # Creating a make bundle.
    MAKING = 'making'
    # Wait for the worker to start running the bundle.
    STARTING = 'starting'
    # Wait for worker to download dependencies and docker images
    PREPARING = 'preparing'
    # Actually running
    RUNNING = 'running'
    # Run finished and finalized server-side, tell worker to discard it
    FINALIZING = 'finalizing'
    # Done running and succeeded
    READY = 'ready'
    # Done running and failed
    FAILED = 'failed'
    # Killed by user
    KILLED = 'killed'
    # Assigned worker has gone offline
    WORKER_OFFLINE = 'worker_offline'

    OPTIONS = {CREATED, STAGED, MAKING, STARTING, RUNNING, READY, FAILED, PREPARING, FINALIZING}
    ACTIVE_STATES = {MAKING, STARTING, RUNNING, FINALIZING, PREPARING}
    FINAL_STATES = {READY, FAILED, KILLED}


class RunResources(object):
    """
    Defines all the resource fields the server propagates to the worker for its runs
    """

    def __init__(self, cpus, gpus, docker_image, time, memory, disk, network):
        self.cpus = cpus  # type: int
        self.gpus = gpus  # type: str
        self.docker_image = docker_image  # type: str
        self.time = time  # type: int
        self.memory = memory  # type: int
        self.disk = disk  # type: int
        self.network = network  # type: bool

        if ":" not in self.docker_image:
            self.docker_image += ":latest"

    def to_dict(self):
        return generic_to_dict(self)

    @classmethod
    def from_dict(cls, dct):
        return cls(
            cpus=int(dct["cpus"]),
            gpus=dct["gpus"],
            docker_image=dct["docker_image"],
            time=int(dct["time"]),
            memory=int(dct["memory"]),
            disk=int(dct["disk"]),
            network=bool(dct["network"]),
        )


def generic_to_dict(obj):
    dct = {}
    if isinstance(obj, dict):
        iter_dict = obj
    elif hasattr(obj, '__dict__'):
        iter_dict = obj.__dict__
    else:
        return obj
    for k, v in iter_dict.items():
        if isinstance(v, dict) or hasattr(v, '__dict__'):
            dct[k] = generic_to_dict(v)
        else:
            dct[k] = v
    return dct
