import logging
import os
import re
import subprocess
import sys
import threading
import time
import traceback

from codalab.common import State
from codalab.lib import path_util
from codalab.worker.bundle_manager import BundleManager
from codalab.worker.worker_info_accessor import WorkerInfoAccessor
from codalabworker.file_util import remove_path


logger = logging.getLogger(__name__)


class TorqueBundleManager(BundleManager):
    def __init__(self, codalab_manager, torque_config):
        assert(codalab_manager.worker_model().shared_file_system)
        self._torque_ssh_host = torque_config['ssh_host']
        self._torque_bundle_service_url = torque_config['bundle_service_url']
        self._torque_password_file = torque_config['password_file']
        self._torque_log_dir = torque_config['log_dir']
        self._torque_min_seconds_between_qsub = torque_config.get('min_seconds_between_qsub', 0)
        path_util.make_directory(self._torque_log_dir)
        if 'worker_code_dir' in torque_config:
            self._torque_worker_code_dir = torque_config['worker_code_dir']
        else:
            codalab_cli = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            self._torque_worker_code_dir = os.path.join(codalab_cli, 'worker')
        self._last_delete_attempt = {}
        self._last_qsub_time = 0

    def run(self, sleep_time):
        # Start separate thread for creating Torque jobs (see method
        # documentation for more explanation).
        threading.Thread(
            target=TorqueBundleManager._listen_for_staged_bundles, args=[self, sleep_time]
        ).start()
        # Start main work loop
        super(TorqueBundleManager, self).run(sleep_time)

    def _schedule_run_bundles(self):
        """
        This method implements a state machine. The states are:

        STAGED, no job_handle, no worker, no worker_run DB entry:
            Ready to run on a CodaLab-owned Torque worker or on a user-owned
            worker.

        For user-owned workers, the states are the same as used in the
        DefaultBundleManager:

        STARTING, no job_handle, has worker_run DB entry:
            Run message sent, waiting for the run to start.
        RUNNING, no job_handle, has worker_run DB entry:
            Worker reported that the run has started.
        READY / FAILED, no job_handle, no worker_run DB entry:
            Finished.

        For CodaLab-owned Torque workers:

        WAITING_FOR_WORKER_STARTUP, has job_handle, worker starting, no worker_run DB entry:
            Waiting for the Torque worker to start before sending a run message.
        STARTING, has job_handle, worker running, has worker_run DB entry:
            Run message sent, waiting for the run to start. 
        RUNNING, has job_handle, worker running, has worker_run DB entry:
            Worker reported that the run has started.
        READY / FAILED, has job_handle, worker running, no worker_run DB entry:
            Will send shutdown message to worker.
        READY / FAILED, has job_handle, no worker, no worker_run DB entry:
            Finished.
        """
        workers = WorkerInfoAccessor(self._worker_model.get_workers())

        # Handle some exceptional cases.
        self._cleanup_dead_workers(workers,
                                   lambda worker: self._clear_torque_logs(worker['worker_id']))
        self._restage_stuck_starting_bundles(workers)
        self._fail_stuck_running_bundles(workers)
        self._fail_on_bad_torque_start()

        # Prefer user-owned workers.
        self._schedule_run_bundles_on_workers(workers, user_owned=True)

        # Run the normal Torque workflow for the rest.
        # self._start_torque_workers()  # done in a separate thread!
        self._start_bundles(workers)
        self._delete_finished_torque_workers(workers)

    def _fail_on_bad_torque_start(self):
        """
        Fail the bundle and clean-up the Torque worker if the Torque worker
        failed to start. This would happen if:
            1) The Torque worker outputs some errors.
            2) If Torque fails to schedule the worker at all, for example, when
               the user has requested too many resources. 
        """
        for bundle in self._model.batch_get_bundles(state=State.WAITING_FOR_WORKER_STARTUP, bundle_type='run'):
            failure_message = self._read_torque_error_log(bundle.metadata.job_handle)
            if failure_message is None and time.time() - bundle.metadata.last_updated > 20 * 60:
                failure_message = 'Worker failed to start. You may have requested too many resources.'
            if failure_message is not None:
                logger.info('Failing %s: %s', bundle.uuid, failure_message)
                self._model.update_bundle(
                    bundle, {'state': State.FAILED,
                             'metadata': {'failure_message': failure_message}})

    def _read_torque_error_log(self, job_handle):
        error_log_path = os.path.join(self._torque_log_dir, 'stderr.' + job_handle)
        if os.path.exists(error_log_path):
            with open(error_log_path) as f:
                lines = [line for line in f.readlines() if not line.startswith('PBS')]
                if lines:
                    return ''.join(lines)

        return None

    def _listen_for_staged_bundles(self, sleep_time):
        """
        Separate run loop dedicated to waiting for staged bundles and firing off
        the requests to Torque.

        We do this in a separate thread because while we want to throttle
        requests to Torque and send them one-by-one, we also don't want to lose
        responsiveness in the other operations (e.g. staging bundles).
        """
        while not self._is_exiting():
            try:
                self._start_torque_workers()
                time.sleep(sleep_time)
            except Exception:
                traceback.print_exc()

    def _start_torque_workers(self):
        """
        Starts Torque workers for bundles that are ready to run (i.e. STAGED).
        """
        for bundle in self._model.batch_get_bundles(state=State.STAGED, bundle_type='run'):
            resource_args = []
            
            request_cpus = self._compute_request_cpus(bundle)
            if request_cpus:
                resource_args.extend(['-l', 'nodes=1:ppn=%d' % request_cpus])
            
            request_memory = self._compute_request_memory(bundle)
            if request_memory:
                resource_args.extend(['-l', 'mem=%d' % request_memory])
            
            request_queue = bundle.metadata.request_queue or self._default_request_queue
            if request_queue:
                # Either host=<host-name> or <queue-name>, but not tag=<tag>
                m = re.match('host=(.+)', request_queue)
                tagm = re.match('tag=.+', request_queue)
                if m:
                    resource_args.extend(['-l', 'host=' + m.group(1)])
                elif not tagm:
                    resource_args.extend(['-q', request_queue])
            
            request_priority = bundle.metadata.request_priority or self._default_request_priority
            if request_priority:
                resource_args.extend(['-p', str(request_priority)])
            
            script_args = [
                '--server',  self._torque_bundle_service_url,
                '--password-file', self._torque_password_file,
                '--shared-file-system',
            ]
            
            script_env = {
                'LOG_DIR': self._torque_log_dir,
                'WORKER_CODE_DIR': self._torque_worker_code_dir,
                # -v doesn't work with spaces, so we have to hack it.
                'WORKER_ARGS': '|'.join(script_args),
            }
            
            command = self._torque_ssh_command(
                ['qsub',
                 '-k', 'n',  # do not keep stdout/stderr streams (we redirect them manually to the configured log_dir)
                 '-d', '/tmp',  # avoid chdir permission problems, worker won't do anything in working directory anyway
                 '-v', ','.join([k + '=' + v for k, v in script_env.iteritems()])] +
                resource_args +
                ['-S', '/bin/bash', os.path.join(self._torque_worker_code_dir, 'worker.sh')])

            # Throttle Torque commands, sometimes scheduler has trouble keeping up
            elapsed = time.time() - self._last_qsub_time
            if elapsed < self._torque_min_seconds_between_qsub:
                time.sleep(self._torque_min_seconds_between_qsub - elapsed)

            try:
                job_handle = subprocess.check_output(command, stderr=subprocess.STDOUT).strip()
            except subprocess.CalledProcessError as e:
                failure_message = 'Failed to launch Torque job: ' + e.output
                logger.info('Failing %s: %s', bundle.uuid, failure_message)
                self._model.update_bundle(
                    bundle, {'state': State.FAILED,
                             'metadata': {'failure_message': failure_message}})
                continue
            finally:
                self._last_qsub_time = time.time()

            logger.info('Started Torque worker for bundle %s, job handle %s', bundle.uuid, job_handle)
            self._model.set_waiting_for_worker_startup_bundle(bundle, job_handle)

    def _start_bundles(self, workers):
        """
        Send run messages once the Torque worker starts and checks in.
        """
        for bundle in self._model.batch_get_bundles(state=State.WAITING_FOR_WORKER_STARTUP, bundle_type='run'):
            worker = workers.worker_with_id(self._model.root_user_id, bundle.metadata.job_handle)
            if worker is not None:
                if not self._try_start_bundle(workers, worker, bundle):
                    failure_message = 'Unable to communicate to Torque worker.'
                    logger.info('Failing %s: %s', bundle.uuid, failure_message)
                    self._model.update_bundle(
                        bundle, {'state': State.FAILED,
                                 'metadata': {'failure_message': failure_message}})

    def _delete_finished_torque_workers(self, workers):
        """
        Shut down Torque workers once the runs have finished. We use this
        mechanism, instead of shutting workers down automatically after they
        finish a single run, since this mechanism also handles exceptional cases
        such as the run message going missing.
        """
        running_job_handles = set()
        running_states = [State.WAITING_FOR_WORKER_STARTUP, State.STARTING, State.RUNNING]
        for bundle in self._model.batch_get_bundles(state=running_states, bundle_type='run'):
            if hasattr(bundle.metadata, 'job_handle'):
                running_job_handles.add(bundle.metadata.job_handle)

        for worker in workers.user_owned_workers(self._model.root_user_id):
            job_handle = worker['worker_id']
            if job_handle not in running_job_handles:
                if (job_handle in self._last_delete_attempt and
                    self._last_delete_attempt[job_handle] - time.time() < 60):
                    # Throttle the deletes in case there is a Torque problem.
                    continue
                self._last_delete_attempt[job_handle] = time.time()

                logger.info('Delete Torque worker with handle %s', job_handle)
                # Delete the worker job.
                command = self._torque_ssh_command(['qdel', job_handle])
                try:
                    subprocess.check_output(command, stderr=subprocess.STDOUT).strip()
                except subprocess.CalledProcessError as e:
                    print >> sys.stderr, 'Failure deleting Torque worker:', e.output
                    traceback.print_exc()
                    continue

                # Clear the logs.
                self._clear_torque_logs(job_handle)

    def _clear_torque_logs(self, job_handle):
        remove_path(os.path.join(self._torque_log_dir, 'stdout.' + job_handle))
        remove_path(os.path.join(self._torque_log_dir, 'stderr.' + job_handle))

    def _torque_ssh_command(self, args):
        args = ['"' + arg.replace('"', '\\"') + '"' for arg in args]  # Quote arguments
        return ['ssh', '-oBatchMode=yes', '-x', self._torque_ssh_host] + args
