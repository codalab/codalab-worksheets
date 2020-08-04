from collections import namedtuple
from typing import Any, Dict, List, Optional


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


# Used to uniquely identify dependencies on a worker. We don't use child features here since
# multiple dependent child bundles can use the same parent dependency
DependencyKey = namedtuple('DependencyKey', 'parent_uuid parent_path')
# Location is an optional key that holds the actual path to the dependency bundle on shared bundle
# mount worker machines
Dependency = namedtuple(
    'Dependency', 'parent_name parent_path parent_uuid child_path child_uuid location'
)


class LinkFormat(object):
    """
    An enumeration of link formats that a bundle supports in the
    metadata.link_format field.
    """

    RAW = 'raw'
    ZIP = 'zip'

    OPTIONS = {RAW, ZIP}


class BundleInfo(object):
    """
    Defines the bundle info passed to the worker by the server.
    Refer to codalab/lib/bundle_util.py#bundle_to_bundle_info() for how the
    dict to construct this object is created on server side.
    """

    def __init__(
        self,
        uuid,  # type: str
        bundle_type,  # type: str
        owner_id,  # type: str
        command,  # type: str
        data_hash,  # type: str
        state,  # type: State
        is_anonymous,  # type: bool
        metadata,  # type: Dict[Any, Any]
        dependencies,  # type: List[Dict[str, str]]
        args,  # type: Any
        location=None,  # type: Optional[str]
    ):
        self.uuid = uuid
        self.bundle_type = bundle_type
        self.owner_id = owner_id
        self.command = command
        self.data_hash = data_hash
        self.state = state
        self.is_anonymous = is_anonymous
        self.metadata = metadata
        self.args = args
        self.dependencies = [
            Dependency(
                parent_name=dep["parent_name"],
                parent_path=dep["parent_path"],
                parent_uuid=dep["parent_uuid"],
                child_path=dep["child_path"],
                child_uuid=dep["child_uuid"],
                location=dep.get("location", None),
            )
            for dep in dependencies
        ]  # type: List[Dependency]

        self.location = location  # set if local filesystem

    @property
    def as_dict(self):
        dct = generic_to_dict(self)
        dct['dependencies'] = [generic_to_dict(v) for v in dct['dependencies']]
        return dct

    def __str__(self):
        return str(self.as_dict)

    @classmethod
    def from_dict(cls, dct):
        return cls(
            uuid=dct["uuid"],
            bundle_type=dct["bundle_type"],
            owner_id=dct["owner_id"],
            command=dct["command"],
            data_hash=dct["data_hash"],
            state=dct["state"],
            is_anonymous=dct["is_anonymous"],
            metadata=dct["metadata"],
            dependencies=dct["dependencies"],
            args=dct["args"],
            location=dct.get("location", None),
        )


class RunResources(object):
    """
    Defines all the resource fields the server propagates to the worker for its runs
    """

    def __init__(
        self,
        cpus,  # type: int
        gpus,  # type: int
        docker_image,  # type: str
        time,  # type: int
        memory,  # type: int
        disk,  # type: int
        network,  # type: bool
    ):
        self.cpus = cpus
        self.gpus = gpus
        self.docker_image = docker_image
        self.time = time
        self.memory = memory
        self.disk = disk
        self.network = network

    @property
    def as_dict(self):
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


class BundleCheckinState(object):
    """
    Defines all the fields the worker needs to check in with the server
    for a given bundle it's running.
    """

    def __init__(
        self,
        uuid,  # type: str
        run_status,  # type: str
        bundle_start_time,  # type: int
        container_time_total,  # type: int
        container_time_user,  # type: int
        container_time_system,  # type: int
        docker_image,  # type: str
        state,  # type: State
        remote,  # type: str
        exitcode,  # type: Optional[str]
        failure_message,  # type: Optional[str]
    ):
        self.uuid = uuid
        self.run_status = run_status
        self.bundle_start_time = bundle_start_time
        self.container_time_total = container_time_total
        self.container_time_user = container_time_user
        self.container_time_system = container_time_system
        self.docker_image = docker_image
        self.state = state
        self.remote = remote
        self.exitcode = exitcode
        self.failure_message = failure_message

    @classmethod
    def from_dict(cls, dct):
        return cls(
            uuid=dct['uuid'],
            run_status=dct['run_status'],
            bundle_start_time=dct['bundle_start_time'],
            container_time_total=dct['container_time_total'],
            container_time_user=dct['container_time_user'],
            container_time_system=dct['container_time_system'],
            docker_image=dct['docker_image'],
            state=dct['state'],
            remote=dct['remote'],
            exitcode=dct['exitcode'],
            failure_message=dct['failure_message'],
        )

    @property
    def as_dict(self):
        return generic_to_dict(self)


def generic_to_dict(obj):
    dct = {}
    if isinstance(obj, dict):
        iter_dict = obj
    elif hasattr(obj, '_asdict'):
        iter_dict = obj._asdict()
    elif hasattr(obj, '__dict__'):
        iter_dict = obj.__dict__
    elif hasattr(obj, 'as_dict'):
        iter_dict = obj.as_dict
    else:
        return obj
    for k, v in iter_dict.items():
        if isinstance(v, dict) or hasattr(v, '__dict__') or hasattr(v, '_asdict'):
            dct[k] = generic_to_dict(v)
        else:
            dct[k] = v
    return dct
