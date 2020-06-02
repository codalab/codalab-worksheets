from collections import defaultdict
import datetime


class WorkerInfoAccessor(object):
    """
    Helps with accessing the list of workers returned by the worker model.
    """

    def __init__(self, model):
        self._model = model
        self._last_fetch = None
        self._fetch_workers()

    def _fetch_workers(self):
        self._workers = {worker['worker_id']: worker for worker in self._model.get_workers()}
        self._last_fetch = datetime.datetime.utcnow()
        self._uuid_to_worker = {}
        self._user_id_to_workers = defaultdict(list)
        for worker in self._workers.values():
            for uuid in worker['run_uuids']:
                self._uuid_to_worker[uuid] = worker
            self._user_id_to_workers[worker['user_id']].append(worker)
            # 'gpus' field contains the number of free GPUs that comes with each worker. Adding an additional
            # 'has_gpus' flag here to indicate if the current worker has GPUs or not.
            worker['has_gpus'] = True if worker['gpus'] > 0 else False

    def workers(self):
        return list(self._workers.values())

    def user_owned_workers(self, user_id):
        return list(worker for worker in self._user_id_to_workers[user_id])

    def remove(self, worker_id):
        worker = self._workers[worker_id]
        for uuid in worker['run_uuids']:
            del self._uuid_to_worker[uuid]
        self._user_id_to_workers[worker['user_id']].remove(worker)
        del self._workers[worker_id]

    def is_running(self, uuid):
        return uuid in self._uuid_to_worker

    def set_starting(self, uuid, worker_id):
        worker = self._workers[worker_id]
        worker['run_uuids'].append(uuid)
        self._uuid_to_worker[uuid] = worker

    def restage(self, uuid):
        if uuid in self._uuid_to_worker:
            worker = self._uuid_to_worker[uuid]
            worker['run_uuids'].remove(uuid)
            del self._uuid_to_worker[uuid]
