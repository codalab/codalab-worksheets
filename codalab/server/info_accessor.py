from collections import defaultdict
from codalab.lib import bundle_util, formatting, path_util
from codalab.worker.bundle_state import RunResources
import copy


class BundleResourcesInfoAccessor(object):
    """
    Intermediate object to cache bundle resources from the model
    """

    def __init__(
        self,
        user_info_accessor,
        bundle_model,
        max_request_disk,
        max_request_time,
        default_gpu_image,
        default_cpu_image,
    ):
        self.user_info_accessor = user_info_accessor
        self.bundle_model = bundle_model
        self.bundle_resources = {}
        self.max_request_disk = max_request_disk
        self.max_request_time = max_request_time
        self.default_cpu_image = default_cpu_image
        self.default_gpu_image = default_gpu_image

    def __contains__(self, uuid):
        return uuid in self.bundle_resources

    def __getitem__(self, uuid):
        if uuid not in self.bundle_resources:
            bundle = self.bundle_model.get_bundle(uuid)
            self.bundle_resources[uuid] = self._compute_bundle_resources(
                bundle, self.user_info_accessor
            )
        return self.bundle_resources[uuid]

    def _compute_bundle_resources(self, bundle, user_info_accessor):
        return RunResources(
            cpus=self._compute_request_cpus(bundle),
            gpus=self._compute_request_gpus(bundle),
            docker_image=self._get_docker_image(bundle),
            time=self._compute_request_time(bundle),
            memory=self._compute_request_memory(bundle),
            disk=self._compute_request_disk(bundle),
            network=bundle.metadata.request_network,
        )

    @staticmethod
    def _compute_request_cpus(bundle):
        """
        Compute the CPU limit used for scheduling the run.
        The default of 1 is for backwards compatibilty for
        runs from before when we added client-side defaults
        """
        if not bundle.metadata.request_cpus:
            return 1
        return bundle.metadata.request_cpus

    @staticmethod
    def _compute_request_gpus(bundle):
        """
        Compute the GPU limit used for scheduling the run.
        The default of 0 is for backwards compatibilty for
        runs from before when we added client-side defaults
        """
        if bundle.metadata.request_gpus is None:
            return 0
        return bundle.metadata.request_gpus

    @staticmethod
    def _compute_request_memory(bundle):
        """
        Compute the memory limit used for scheduling the run.
        The default of 2g is for backwards compatibilty for
        runs from before when we added client-side defaults
        """
        if not bundle.metadata.request_memory:
            return formatting.parse_size('2g')
        return formatting.parse_size(bundle.metadata.request_memory)

    def _compute_request_disk(self, bundle):
        """
        Compute the disk limit used for scheduling the run.
        The default is min(disk quota the user has left, global max)
        """
        if not bundle.metadata.request_disk:
            return min(
                self.user_info_accessor.get_disk_quota_left(bundle.owner_id) - 1,
                self.max_request_disk,
            )
        return formatting.parse_size(bundle.metadata.request_disk)

    def _compute_request_time(self, bundle):
        """
        Compute the time limit used for scheduling the run.
        The default is min(time quota the user has left, global max)
        """
        if not bundle.metadata.request_time:
            return min(
                self.user_info_accessor.get_time_quota_left(bundle.owner_id) - 1,
                self.max_request_time,
            )
        return formatting.parse_duration(bundle.metadata.request_time)

    def _get_docker_image(self, bundle):
        """
        Set docker image to be the default if not specified
        Unlike other metadata fields this can actually be None
        from client
        Also add the `latest` tag if no tag is specified to be
        consistent with Docker's own behavior.
        """
        if not bundle.metadata.request_docker_image:
            if bundle.metadata.request_gpus:
                docker_image = self.default_gpu_image
            else:
                docker_image = self.default_cpu_image
        else:
            docker_image = bundle.metadata.request_docker_image
        if ':' not in docker_image:
            docker_image += ':latest'
        return docker_image


class UserInfoAccessor(object):
    """
    Intermediate object to cache user info from the model and compute derived values.
    """

    def __init__(self, bundle_model):
        self.bundle_model = bundle_model
        self.user_infos = {}
        self.active_run_counts = {}

    def __contains__(self, user_id):
        return user_id in self.user_infos

    def __getitem__(self, user_id):
        return self.get_user_info(user_id)

    def get_user_info(self, user_id):
        if user_id not in self.user_infos:
            self.user_infos[user_id] = self.bundle_model.get_user_info(user_id)
        return self.user_infos[user_id]

    def get_parallel_run_quota_left(self, user_id):
        user_info = self.get_user_info(user_id)
        if user_id not in self.active_run_counts:
            self.active_run_counts[user_id] = len(self.bundle_model.get_user_active_runs(user_id))
        return user_info['parallel_run_quota'] - self.active_run_counts[user_id]

    def get_disk_quota_left(self, user_id):
        user_info = self.get_user_info(user_id)
        return user_info['disk_quota'] - user_info['disk_used']

    def get_time_quota_left(self, user_id):
        user_info = self.get_user_info(user_id)
        return user_info['time_quota'] - user_info['time_used']


class WorkerInfoAccessor(object):
    """
    Helps with accessing the list of workers returned by the worker model.
    """

    def __init__(self, workers):
        self._workers = workers
        self._uuid_to_worker = {}
        self._user_id_to_workers = defaultdict(list)
        for worker in self._workers:
            for uuid in worker['run_uuids']:
                self._uuid_to_worker[uuid] = worker
            self._user_id_to_workers[worker['user_id']].append(worker)

    def workers(self):
        return list(self._workers)

    def worker_with_id(self, user_id, worker_id):
        for worker in self._workers:
            if worker['user_id'] == user_id and worker['worker_id'] == worker_id:
                return worker
        return None

    def user_owned_workers(self, user_id):
        # A deep copy is necessary here due to the following facts:
        # 1. assignment statements in Python do not copy objects, meaning it generates a shallow copy
        # 2. deep copy is only necessary for compound objects which contains other objects, like lists or class instances
        # 3. a deep copy will guarantee that one can change one copy without changing the other
        return list(copy.deepcopy(worker) for worker in self._user_id_to_workers[user_id])

    def remove(self, worker):
        self._workers.remove(worker)
        for uuid in worker['run_uuids']:
            del self._uuid_to_worker[uuid]
        self._user_id_to_workers[worker['user_id']].remove(worker)

    def is_running(self, uuid):
        return uuid in self._uuid_to_worker

    def set_starting(self, uuid, worker):
        worker['run_uuids'].append(uuid)
        self._uuid_to_worker[uuid] = worker

    def restage(self, uuid):
        if uuid in self._uuid_to_worker:
            worker = self._uuid_to_worker[uuid]
            worker['run_uuids'].remove(uuid)
            del self._uuid_to_worker[uuid]
