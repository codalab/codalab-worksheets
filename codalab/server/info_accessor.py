from collections import defaultdict
import copy


class UserInfoAccessor(object):
    """
    Intermediate object to cache user info from the model and compute derived values.
    """

    def __init__(self, bundle_model):
        self.bundle_model = bundle_model
        self.user_infos = {}
        self.active_run_counts = {}

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
