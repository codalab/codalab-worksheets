from collections import defaultdict

import datetime
from typing import Callable, Union


def refresh_cache(
    f: Union[
        Callable[['WorkerInfoAccessor'], list],
        Callable[['WorkerInfoAccessor', str], Union[list, None, bool]],
        Callable[['WorkerInfoAccessor', str, str], None],
    ]
):
    def wrapper(*args, **kwargs):
        self = args[0]
        if datetime.datetime.utcnow() - self._last_fetch >= datetime.timedelta(
            seconds=self._timeout_seconds
        ):
            self._fetch_workers()
        return f(*args, **kwargs)

    return wrapper


class WorkerInfoAccessor(object):
    """
    Helps with accessing the list of workers returned by the worker model.
    """

    def __init__(self, model, worker_model, timeout_seconds):
        self._model = model
        self._worker_model = worker_model
        self._timeout_seconds = timeout_seconds
        self._last_fetch = None
        self._fetch_workers()

    def _fetch_workers(self):
        self._workers = {worker['worker_id']: worker for worker in self._worker_model.get_workers()}
        self._last_fetch = datetime.datetime.utcnow()
        self._uuid_to_worker = {}
        self._user_id_to_workers = defaultdict(list)

        for worker in self._workers.values():
            for uuid in worker['run_uuids']:
                self._uuid_to_worker[uuid] = worker

            owner_id = worker['user_id']
            self._user_id_to_workers[owner_id].append(worker)

            # Add the worker to all the users of the worker's group except the owner
            memberships = self._model.batch_get_user_in_group(group_uuid=worker['group_uuid'])
            for m in memberships:
                if m['user_id'] != owner_id:
                    self._user_id_to_workers[m['user_id']].append(worker)

            # 'gpus' field contains the number of free GPUs that comes with each worker. Adding an additional
            # 'has_gpus' flag here to indicate if the current worker has GPUs or not.
            worker['has_gpus'] = True if worker['gpus'] > 0 else False

    @refresh_cache
    def workers(self):
        return list(self._workers.values())

    @refresh_cache
    def get_user_workers(self, user_id):
        """
        Gets all the workers that the user owns or has permissions for
        :param user_id: ID of the user
        :return: List of workers
        """
        return list(worker for worker in self._user_id_to_workers[user_id])

    @refresh_cache
    def remove(self, worker_id):
        worker = self._workers[worker_id]
        for uuid in worker['run_uuids']:
            del self._uuid_to_worker[uuid]
        self._user_id_to_workers[worker['user_id']].remove(worker)
        del self._workers[worker_id]

    @refresh_cache
    def is_running(self, uuid):
        return uuid in self._uuid_to_worker

    @refresh_cache
    def set_starting(self, uuid, worker_id):
        worker = self._workers[worker_id]
        worker['run_uuids'].append(uuid)
        self._uuid_to_worker[uuid] = worker

    @refresh_cache
    def restage(self, uuid):
        if uuid in self._uuid_to_worker:
            worker = self._uuid_to_worker[uuid]
            worker['run_uuids'].remove(uuid)
            del self._uuid_to_worker[uuid]
