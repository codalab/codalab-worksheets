import logging

from codalab.common import State
from codalab.worker.bundle_manager import BundleManager
from codalab.worker.worker_info_accessor import WorkerInfoAccessor


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
        self._fail_stuck_running_bundles(workers)
        self._fail_on_too_many_resources(workers)

        # Schedule, preferring user-owned workers.
        self._schedule_run_bundles_on_workers(workers, user_owned=True)
        self._schedule_run_bundles_on_workers(workers, user_owned=False)

    def _fail_on_too_many_resources(self, workers):
        """
        Fails bundles that request more resources than available on any worker.
        """
        for bundle in self._model.batch_get_bundles(state=State.STAGED, bundle_type='run'):
            workers_list = (workers.user_owned_workers(bundle.owner_id) +
                            workers.user_owned_workers(self._model.root_user_id))

            failure_message = None
            
            request_cpus = self._compute_request_cpus(bundle)
            if request_cpus:
                max_cpus = max(map(lambda worker: worker['cpus'], workers_list))
                if request_cpus > max_cpus:
                    failure_message = 'No workers with enough CPUs'

            request_memory = self._compute_request_memory(bundle)
            if request_memory:
                max_memory = max(map(lambda worker: worker['memory_bytes'], workers_list))
                if request_memory > max_memory:
                    failure_message = 'No workers with enough memory'

            if failure_message is not None:
                logger.info('Failing %s: %s', bundle.uuid, failure_message)
                self._model.update_bundle(
                    bundle, {'state': State.FAILED,
                             'metadata': {'failure_message': failure_message}})
