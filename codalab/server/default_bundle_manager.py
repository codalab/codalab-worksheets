import logging

from codalab.server.bundle_manager import BundleManager
from codalab.server.worker_info_accessor import WorkerInfoAccessor
from codalab.worker.bundle_state import State
from codalab.lib.formatting import size_str, duration_str


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
        self._fail_on_too_many_resources()
        self._acknowledge_recently_finished_bundles(workers)

        # Schedule, preferring user-owned workers.
        self._schedule_run_bundles_on_workers(workers, user_owned=True)
        self._schedule_run_bundles_on_workers(workers, user_owned=False)

    def _check_resource_failure(
        self,
        value,
        user_fail_string=None,
        global_fail_string=None,
        user_max=None,
        global_max=None,
        pretty_print=lambda x: str(x),
    ):
        """
        Returns a failure message in case a certain resource limit is not respected.
        If value > user_max, user_fail_string is formatted with value and user_max in that order
        If value > global_max, global_fail_strintg is formatted with value and global_max in that order
        Pretty print is applied to both the value and max values before they're passed on to the functions
        The strings should expect string inputs for formatting and pretty_print should convert values to strings
        """
        if value:
            if user_max and value > user_max:
                return user_fail_string % (pretty_print(value), pretty_print(user_max))
            elif global_max and value > global_max:
                return global_fail_string % (pretty_print(value), pretty_print(global_max))
        return None

    def _fail_on_too_many_resources(self):
        """
        Fails bundles that request more resources than available for the given user.
        Note: allow more resources than available on any worker because new
        workers might get spun up in response to the presence of this run.
        """
        for bundle in self._model.batch_get_bundles(state=State.STAGED, bundle_type='run'):
            failures = []

            failures.append(
                self._check_resource_failure(
                    self._compute_request_disk(bundle),
                    user_fail_string='Requested more disk (%s) than user disk quota left (%s)',
                    user_max=self._model.get_user_disk_quota_left(bundle.owner_id),
                    global_fail_string='Maximum job disk size (%s) exceeded (%s)',
                    global_max=self._max_request_disk,
                    pretty_print=size_str,
                )
            )

            failures.append(
                self._check_resource_failure(
                    self._compute_request_time(bundle),
                    user_fail_string='Requested more time (%s) than user time quota left (%s)',
                    user_max=self._model.get_user_time_quota_left(bundle.owner_id),
                    global_fail_string='Maximum job time (%s) exceeded (%s)',
                    global_max=self._max_request_time,
                    pretty_print=duration_str,
                )
            )

            failures.append(
                self._check_resource_failure(
                    self._compute_request_memory(bundle),
                    global_fail_string='Requested more memory (%s) than maximum limit (%s)',
                    global_max=self._max_request_memory,
                    pretty_print=size_str,
                )
            )

            failures = [f for f in failures if f is not None]

            if len(failures) > 0:
                failure_message = '. '.join(failures)
                logger.info('Failing %s: %s', bundle.uuid, failure_message)

                self._model.update_bundle(
                    bundle,
                    {'state': State.FAILED, 'metadata': {'failure_message': failure_message}},
                )
