from collections import defaultdict

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
        return list(self._user_id_to_workers[user_id])

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
