import logging
import os
import time
import traceback
import socket
import httplib
import sys

from bundle_service_client import BundleServiceException
from download_util import BUNDLE_NO_LONGER_RUNNING_MESSAGE

VERSION = 18

COMMAND_RETRY_SECONDS = 2 * 60 * 6

logger = logging.getLogger(__name__)

"""
Resumable Workers

    If the worker process of a worker machine terminates and restarts while a
    bundle is running, the worker process is able to keep track of the running
    bundle once again, as long as the state is intact and the bundle container
    is still running or has finished running.
"""

class Worker(object):
    def __init__(self, run_manager, state_committer, worker_id, tag, work_dir, bundle_service):
        self.id = worker_id
        self._state_committer = state_committer
        self._tag = tag
        self._work_dir = work_dir
        self._bundle_service = bundle_service
        self._stop = False
        self._last_checkin_successful = False
        self._run_manager = run_manager

    def start(self):
        self._run_manager.start()
        while not self._stop:
            try:
                self._run_manager.process_runs()
                self._run_manager.save_state()
                self._checkin()
                self._run_manager.save_state()

                if not self._last_checkin_successful:
                    logger.info('Connected! Successful check in.')
                self._last_checkin_successful = True

            except Exception:
                self._last_checkin_successful = False
                traceback.print_exc()
                time.sleep(1)

        self._run_manager.stop()

    def _checkin(self):
        """
        Checkin with the server and get a response. React to this response.
        This function must return fast to keep checkins frequent. Time consuming
        processes must be handled asyncronously.
        """
        request = {
            'version': VERSION,
            'tag': self._tag,
            'cpus': len(self._run_manager.cpus),
            'gpus': len(self._run_manager.gpus),
            'memory_bytes': self._run_manager.memory_bytes,
            'dependencies': self._run_manager.all_dependencies,
            'hostname': socket.gethostname(),
            'runs': self._run_manager.all_runs
        }
        response = self._bundle_service.checkin(self.id, request)
        if response:
            action_type = response['type']
            logger.debug('Received %s message: %s', action_type, response)
            if action_type in ['read', 'write', 'netcat', 'kill']:
                run_state = self._run_manager.get_run(response['uuid'])
                socket_id = response['socket_id']
                if run_state is None:
                    self.read_run_missing(socket_id)
                    return
            if action_type == 'run':
                self._run(response['bundle'], response['resources'])
            elif action_type == 'read':
                self._read(socket_id, run_state, response['path'],
                           response['read_args'])
            elif action_type == 'netcat':
                self._netcat(socket_id, run_state, response['port'],
                           response['message'])
            elif action_type == 'write':
                self._write(run_state, response['subpath'],
                            response['string'])
            elif action_type == 'kill':
                self._kill(run_state)

    def _run(self, bundle, resources):
        """
        First, checks in with the bundle service and sees if the bundle
        is still assigned to this worker. If not, returns immediately.
        Otherwise, tell RunManager to create the run.
        """
        now = time.time()
        start_message = {
            'hostname': socket.gethostname(),
            'start_time': int(now),
        }

        if self._bundle_service.start_bundle(self.id, bundle['uuid'], start_message):
            self._run_manager.create_run(bundle, resources)
        else:
            print >>sys.stdout, 'Bundle {} no longer assigned to this worker'.format(bundle['uuid'])

    def _read(self, socket_id, run_state, path, read_args):
        def reply(err, message={}, data=None):
            if err:
                err = {
                    'error_code': err[0],
                    'error_message': err[1],
                }
                self._bundle_service.reply(err)
            elif data:
                self._bundle_service.reply_data(self.id, socket_id, message, data)
            else:
                self._bundle_service.reply(self.id, socket_id, message)

        try:
            dep_paths = set([dep['child_path'] for dep in run_state.bundle['dependencies']])
            self._run_manager.read(run_state, path, dep_paths, read_args, reply)
        except BundleServiceException:
            traceback.print_exc()
        except Exception as e:
            traceback.print_exc()
            err = (httplib.INTERNAL_SERVER_ERROR, e.message)
            reply(err)

    def _netcat(self, socket_id, run_state, port, message):
        pass

    def _write(self, run_state, subpath, string):
        pass

    def _kill(self, run_state):
        self._run_manager.kill(run_state)

    def finalize_bundle(self, bundle_uuid, finalize_message):
        # id, bundle_uuid, finalize_message
        self._execute_bundle_service_command_with_retry(
            lambda: self._bundle_service.finalize_bundle(
                self.id, bundle_uuid, finalize_message))

    def upload_bundle_contents(self, bundle_uuid, bundle_path, update_status):
        self._execute_bundle_service_command_with_retry(
            lambda: self._bundle_service.update_bundle_contents(
                self.id, bundle_uuid, bundle_path, update_status))

    def _execute_bundle_service_command_with_retry(self, f):
        # Retry for 6 hours before giving up.
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

    def read_run_missing(self, socket_id):
        message = {
            'error_code': httplib.INTERNAL_SERVER_ERROR,
            'error_message': BUNDLE_NO_LONGER_RUNNING_MESSAGE,
        }
        self._bundle_service.reply(self.id, socket_id, message)

