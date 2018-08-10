import logging

from codalab.worker.bundle_manager import BundleManager
from codalab.worker.worker_info_accessor import WorkerInfoAccessor
from codalabworker.bundle_state import State


logger = logging.getLogger(__name__)


class DefaultBundleManager(BundleManager):
    def _schedule_run_bundles(self):
        """
        This method implements a state machine. The states are:

        STAGED, no worker_run DB entry:
            Ready to send run message to available worker.
        STARTING, has worker_run DB entry:
            Run message sent, waiting for the run to start.
        RUNNING, has worker_run DB entry:
            Worker reported that the run has started.
        READY / FAILED, no worker_run DB entry:
            Finished.
        """
        workers = WorkerInfoAccessor(self._worker_model.get_workers())

        # Handle some exceptional cases.
        self._cleanup_dead_workers(workers)
        self._restage_stuck_starting_bundles(workers)
        self._bring_offline_stuck_running_bundles(workers)
        self._fail_on_too_many_resources(workers)
        self._acknowledge_recently_finished_bundles(workers)

        # Schedule, preferring user-owned workers.
        self._schedule_run_bundles_on_workers(workers, user_owned=True)
        self._schedule_run_bundles_on_workers(workers, user_owned=False)

    def _check_resource_failure(self, value, user_fail_string=None, global_fail_string=None, user_max=None, global_max=None):
        if value:
            if user_max and value > user_max:
                return user_fail_string % (value, user_max)
            elif global_max and value > global_max:
                return global_fail_string % (value, global_max)
        return None

    def _fail_on_too_many_resources(self, workers):
        """
        Fails bundles that request more resources than available on any worker.
        """
        for bundle in self._model.batch_get_bundles(state=State.STAGED, bundle_type='run'):
            workers_list = (workers.user_owned_workers(bundle.owner_id) +
                            workers.user_owned_workers(self._model.root_user_id))

            if len(workers_list) == 0:
                return

            failures = []

            failures.append(self._check_resource_failure(
                            self._compute_request_cpus(bundle),
                            global_fail_string='No workers available with %d CPUs, max available: %d',
                            global_max=max(map(lambda worker: worker['cpus'], workers_list))))

            failures.append(self._check_resource_failure(
                            self._compute_request_gpus(bundle),
                            global_fail_string='No workers available with %d GPUs, max available: %d',
                            global_max=max(map(lambda worker: worker['gpus'], workers_list))))

            failures.append(self._check_resource_failure(
                            self._compute_request_disk(bundle),
                            user_fail_string='Requested more disk (%s) than user disk quota left (%s)',
                            user_max=self._model.get_user_disk_quota_left(bundle.owner_id),
                            global_fail_string='Maximum job disk size (%s) exceeded (%s)',
                            global_max=self._max_request_disk))

            failures.append(self._check_resource_failure(
                            self._compute_request_time(bundle),
                            user_fail_string='Requested more time (%s) than user time quota left (%s)',
                            user_max=self._model.get_user_time_quota_left(bundle.owner_id),
                            global_fail_string='Maximum job time (%s) exceeded (%s)',
                            global_max=self._max_request_time))

            failures.append(self._check_resource_failure(
                            self._compute_request_memory(bundle),
                            global_fail_string='Maximum memory limit (%s) exceeded (%s)',
                            global_max=self._max_request_memory))

            failures = [f for f in failures if f is not None]

            if len(failures) > 0:
                failure_message = '. '.join(failures)
                logger.info('Failing %s: %s', bundle.uuid, failure_message)

                self._model.update_bundle(
                    bundle, {'state': State.FAILED,
                             'metadata': {'failure_message': failure_message}})
