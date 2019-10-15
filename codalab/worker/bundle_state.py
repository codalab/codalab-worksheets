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


class DependencyKey(object):
    """
    Defines the uniquely identifying properties of a dependency that can be used as a key
    for caching dependencies
    """

    def __init__(
            self,
            parent_uuid,  # type: str
            parent_path,  # type: str
        ):
        self.parent_uuid = parent_uuid
        self.parent_path = parent_path

    def __eq__(self, other):
        return (
            self.parent_uuid == other.parent_uuid
            and self.parent_path == other.parent_path
        )

    def __hash__(self):
        return hash((self.parent_uuid, self.parent_path))

    def to_dict(self):
        return generic_to_dict(self)


class Dependency(object):
    """
    Defines a RunBundle dependency passed from server to worker.
    Refer to codalab/lib/bundle_util.py#bundle_to_bundle_info() for how the
    dict to construct this object is created on server side.
    """

    def __init__(
        self,
        parent_name,  # type: str
        parent_path,  # type: str
        parent_uuid,  # type: str child_path,  # type: str
        child_uuid,  # type: str
        child_path,  # type: str
        location=None,  # type: Optional[str]
    ):
        self.parent_name = parent_name
        self.parent_path = parent_path
        self.parent_uuid = parent_uuid
        self.child_path = child_path
        self.child_uuid = child_uuid
        self.location = location  # Set if local filesystem dependency

    def to_dict(self):
        return generic_to_dict(self)


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
        dependencies,  # type: List[str, Dict[str, str]]
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
        self.dependencies = {
            DependencyKey(dep["parent_uuid"], dep["parent_path"]): Dependency(
                parent_name=dep["parent_name"],
                parent_path=dep["parent_path"],
                parent_uuid=dep["parent_uuid"],
                child_path=dep["child_path"],
                child_uuid=dep["child_uuid"],
                location=dep.get("location", None),
            )
            for dep in dependencies
        }  # type: Dict[DependencyKey, Dependency]
        self.location = location  # set if local filesystem

    def to_dict(self):
        dct = generic_to_dict(self)
        dct['dependencies'] = [v for k,v  in dct['dependencies'].items()]
        return dct

    def __str__(self):
        return str(self.to_dict())

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
            network, # type: bool
        ):
        self.cpus = cpus
        self.gpus = gpus
        self.docker_image = docker_image
        self.time = time
        self.memory = memory
        self.disk = disk
        self.network = network

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


class WorkerRun(object):
    """
    Defines all the field the worker needs to check in with the server for its runs
    """

    def __init__(
            self,
            run_status,  # type: str
            bundle_start_time,  # type: int
            container_start_time,  # type: int
            container_time_total,  # type: int
            container_time_user,  # type: int
            container_time_system,  # type: int
            docker_image,  # type: str
            info,  # type: Dict[str, Any]
            state,  # type: State
            remote,  # type: str
    ):
        self.run_status = run_status
        self.bundle_start_time = bundle_start_time
        self.container_start_time = container_start_time
        self.container_time_total = container_time_total
        self.container_time_user = container_time_user
        self.container_time_system = container_time_system
        self.docker_image = docker_image
        self.info = info
        self.state = state
        self.remote = remote

    @classmethod
    def from_dict(cls, dct):
        return cls(
            run_status=dct['run_status'],
            bundle_start_time=dct['bundle_start_time'],
            container_start_time=dct['container_start_time'],
            container_time_total=dct['container_time_total'],
            container_time_user=dct['container_time_user'],
            container_time_system=dct['container_time_system'],
            docker_image=dct['docker_image'],
            info=dct['info'],
            state=dct['state'],
            remote=dct['remote'],
        )

    def to_dict(self):
        return generic_to_dict(self)

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
