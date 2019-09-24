import logging
import time
import traceback
import socket
import http.client
import sys

from .bundle_service_client import BundleServiceException
from .download_util import BUNDLE_NO_LONGER_RUNNING_MESSAGE
from .state_committer import JsonStateCommitter

VERSION = 20

COMMAND_RETRY_SECONDS = 60 * 12

logger = logging.getLogger(__name__)
"""
Codalab Worker
Workers handle communications with the Codalab server. Their main role in Codalab execution
is syncing the job states with the server and passing on job-related commands from the server
to architecture-specific RunManagers that run the jobs. Workers are execution platform antagonistic
but they expect the platform specific RunManagers they use to implement a common interface
"""


class Worker(object):
    def __init__(
        self,
        create_run_manager,  # type: Callable[[Worker], BaseRunManager]
        commit_file,  # type: str
        worker_id,  # type: str
        tag,  # type: str
        work_dir,  # type: str
        exit_when_idle,  # type: str
        bundle_service,  # type: BundleServiceClient
    ):
        self.id = worker_id
        self._state_committer = JsonStateCommitter(commit_file)
        self._tag = tag
        self._work_dir = work_dir
        self._bundle_service = bundle_service
        self._exit_when_idle = exit_when_idle
        self._stop = False
        self._last_checkin_successful = False
        self._run_manager = create_run_manager(self)

    def start(self):
        self._run_manager.start()
        while not self._stop:
            try:
                self._run_manager.process_runs()
                self._run_manager.save_state()
                self._checkin()
                self._run_manager.save_state()

                if not self._last_checkin_successful:
                    logger.info('Connected! Successful check in!')
                self._last_checkin_successful = True
                if (
                    self._exit_when_idle
                    and len(self._run_manager.all_runs) == 0
                    and self._last_checkin_successful
                ):
                    self._stop = True
                    break

            except Exception:
                self._last_checkin_successful = False
                traceback.print_exc()
                # Sleep for a long time so we don't keep on failing.
                logger.error('Sleeping for 1 hour due to exception...please help me!')
                time.sleep(1 * 60 * 60)
        self._run_manager.stop()

    def signal(self):
        self._stop = True

    def _checkin(self):
        """
        Checkin with the server and get a response. React to this response.
        This function must return fast to keep checkins frequent. Time consuming
        processes must be handled asynchronously.
        """
        request = {
            'version': VERSION,
            'tag': self._tag,
            'cpus': self._run_manager.cpus,
            'gpus': self._run_manager.gpus,
            'memory_bytes': self._run_manager.memory_bytes,
            'free_disk_bytes': self._run_manager.free_disk_bytes,
            'dependencies': self._run_manager.all_dependencies,
            'hostname': socket.gethostname(),
            'runs': self._run_manager.all_runs,
        }
        response = self._bundle_service.checkin(self.id, request)
        if response:
            action_type = response['type']
            logger.debug('Received %s message: %s', action_type, response)
            if action_type in ['read', 'netcat']:
                run_state = self._run_manager.get_run(response['uuid'])
                socket_id = response['socket_id']
                if run_state is None:
                    self.read_run_missing(socket_id)
                    return
            if action_type == 'run':
                self._run(response['bundle'], response['resources'])
            elif action_type == 'read':
                self._read(socket_id, response['uuid'], response['path'], response['read_args'])
            elif action_type == 'netcat':
                self._netcat(socket_id, response['uuid'], response['port'], response['message'])
            elif action_type == 'write':
                self._write(response['uuid'], response['subpath'], response['string'])
            elif action_type == 'kill':
                self._kill(response['uuid'])
            elif action_type == 'mark_finalized':
                self._mark_finalized(response['uuid'])

    def _run(self, bundle, resources):
        """
        First, checks in with the bundle service and sees if the bundle
        is still assigned to this worker. If not, returns immediately.
        Otherwise, tell RunManager to create the run.
        """
        now = time.time()
        start_message = {'hostname': socket.gethostname(), 'start_time': int(now)}

        if self._bundle_service.start_bundle(self.id, bundle['uuid'], start_message):
            self._run_manager.create_run(bundle, resources)
        else:
            print(
                'Bundle {} no longer assigned to this worker'.format(bundle['uuid']),
                file=sys.stdout,
            )

    def _read(self, socket_id, uuid, path, read_args):
        def reply(err, message={}, data=None):
            self._bundle_service_reply(socket_id, err, message, data)

        try:
            run_state = self._run_manager.get_run(uuid)
            dep_paths = set([dep['child_path'] for dep in run_state.bundle['dependencies']])
            self._run_manager.read(run_state, path, dep_paths, read_args, reply)
        except BundleServiceException:
            traceback.print_exc()
        except Exception as e:
            traceback.print_exc()
            err = (http.client.INTERNAL_SERVER_ERROR, str(e))
            reply(err)

    def _netcat(self, socket_id, uuid, port, message):
        def reply(err, message={}, data=None):
            self._bundle_service_reply(socket_id, err, message, data)

        try:
            run_state = self._run_manager.get_run(uuid)
            self._run_manager.netcat(run_state, port, message, reply)
        except BundleServiceException:
            traceback.print_exc()
        except Exception as e:
            traceback.print_exc()
            err = (http.client.INTERNAL_SERVER_ERROR, str(e))
            reply(err)

    def _write(self, uuid, subpath, string):
        run_state = self._run_manager.get_run(uuid)
        dep_paths = set([dep['child_path'] for dep in run_state.bundle['dependencies']])
        self._run_manager.write(run_state, subpath, dep_paths, string)

    def _kill(self, uuid):
        run_state = self._run_manager.get_run(uuid)
        self._run_manager.kill(run_state)

    def _mark_finalized(self, uuid):
        self._run_manager.mark_finalized(uuid)

    def upload_bundle_contents(self, bundle_uuid, bundle_path, update_status):
        self._execute_bundle_service_command_with_retry(
            lambda: self._bundle_service.update_bundle_contents(
                self.id, bundle_uuid, bundle_path, update_status
            )
        )

    def read_run_missing(self, socket_id):
        message = {
            'error_code': http.client.INTERNAL_SERVER_ERROR,
            'error_message': BUNDLE_NO_LONGER_RUNNING_MESSAGE,
        }
        self._bundle_service.reply(self.id, socket_id, message)

    def _bundle_service_reply(self, socket_id, err, message, data):
        if err:
            err = {'error_code': err[0], 'error_message': err[1]}
            self._bundle_service.reply(self.id, socket_id, err)
        elif data:
            self._bundle_service.reply_data(self.id, socket_id, message, data)
        else:
            self._bundle_service.reply(self.id, socket_id, message)

    def _execute_bundle_service_command_with_retry(self, f):
        retries_left = COMMAND_RETRY_SECONDS
        while True:
            try:
                retries_left -= 1
                f()
                return
            except BundleServiceException as e:
                if not e.client_error and retries_left > 0:
                    traceback.print_exc()
                    time.sleep(30)
                    continue
                raise
