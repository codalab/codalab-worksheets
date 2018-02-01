import httplib
from contextlib import closing
from subprocess import check_output
import logging
import multiprocessing
import os
import shutil
import threading
import time
import traceback
import re
import json

from bundle_service_client import BundleServiceException
from download_util import BUNDLE_NO_LONGER_RUNNING_MESSAGE
from dependency_manager import DependencyManager
from worker_state_manager import WorkerStateManager
from file_util import remove_path, un_tar_directory

VERSION = 15

logger = logging.getLogger(__name__)

"""
Resumable Workers

    If the worker process of a worker machine terminates and restarts while a
    bundle is running, the worker process is able to keep track of the running
    bundle once again, as long as the state is intact and the bundle container
    is still running or has finished running.
"""


class Worker(object):
    """
    This class is responsible for:

        1) Registering with the bundle service and receiving all messages
           sent to the worker.
        2) Managing all the runs currently executing on the worker and
           forwarding messages associated with those runs to the appropriate
           instance of the Run class.
        3) Spawning classes and threads that manage other worker resources,
           specifically the storage of bundles (both running bundles as well as
           their dependencies) and the cache of Docker images.
        4) Upgrading the worker.
    """

    def __init__(self, worker_id, tag, work_dir, max_work_dir_size_bytes, shared_file_system, slots, bundle_service,
                 create_run_manager):
        self.id = worker_id
        self._tag = tag
        self.shared_file_system = shared_file_system
        self._bundle_service = bundle_service
        self._slots = slots
        self._run_manager = create_run_manager(self)

        self._worker_state_manager = WorkerStateManager(
            work_dir=work_dir,
            run_manager=self._run_manager,
            shared_file_system=self.shared_file_system
        )

        if not self.shared_file_system:
            # Manages which dependencies are available.
            self._dependency_manager = DependencyManager(
                    work_dir, max_work_dir_size_bytes, self._worker_state_manager.previous_runs.keys())

        self._exiting_lock = threading.Lock()
        self._exiting = False
        self._should_upgrade = False
        self._last_checkin_successful = False


    def run(self):
        if not self.shared_file_system:
            self._dependency_manager.start_cleanup_thread()

        self._run_manager.worker_did_start()

        while self._should_run():
            try:
                self._checkin()
                self._worker_state_manager.resume_previous_runs()
                self._worker_state_manager.save_state()
                if not self._last_checkin_successful:
                    logger.info('Connected! Successful check in.')
                self._last_checkin_successful = True

            except Exception:
                self._last_checkin_successful = False
                traceback.print_exc()
                time.sleep(1)

        self._checkout()
        self._worker_state_manager.save_state()

        if not self.shared_file_system:
            self._dependency_manager.stop_cleanup_thread()

        self._run_manager.worker_will_stop()

        if self._should_upgrade:
            self._upgrade()

    def signal(self):
        logger.info('Exiting: Will wait for exiting jobs to finish, but will not start any new jobs.')
        with self._exiting_lock:
            self._exiting = True

    def _is_exiting(self):
        with self._exiting_lock:
            return self._exiting

    def _should_run(self):
        if not self._is_exiting():
            return True
        return self._worker_state_manager.has_runs()

    def _get_allocated_memory_bytes(self):
        return sum(self._worker_state_manager.map_runs(lambda run: run.requested_memory_bytes))

    def _get_memory_bytes(self):
        return max(0, self._run_manager.memory_bytes - self._get_allocated_memory_bytes())

    def _checkin(self):
        request = {
            'version': VERSION,
            'will_upgrade': self._should_upgrade,
            'tag': self._tag,
            'slots': self._slots if not self._is_exiting() else 0,
            'cpus': self._run_manager.cpus,
            'gpus': self._run_manager.gpus,
            'memory_bytes': self._get_memory_bytes(),
            'dependencies': [] if self.shared_file_system else self._dependency_manager.dependencies()
        }
        response = self._bundle_service.checkin(self.id, request)
        if response:
            type = response['type']
            logger.debug('Received %s message: %s', type, response)
            if type == 'run':
                self._run(response['bundle'], response['resources'])
            elif type == 'read':
                self._read(response['socket_id'], response['uuid'], response['path'],
                           response['read_args'])
            elif type == 'write':
                self._write(response['uuid'], response['subpath'],
                            response['string'])
            elif type == 'kill':
                self._kill(response['uuid'])
            elif type == 'upgrade':
                with self._exiting_lock:
                    self._exiting = True
                self._should_upgrade = True

    def _run(self, bundle, resources):
        if self.shared_file_system:
            bundle_path = bundle['location']
        else:
            bundle_path = self._dependency_manager.get_run_path(bundle['uuid'])

        run = self._run_manager.create_run(
            bundle=bundle,
            bundle_path=bundle_path,
            resources=resources
        )
        try:
            run.pre_start()
            if run.start():
                self._worker_state_manager.add_run(bundle['uuid'], run)
        except Exception as e:
            run.kill('Problem starting run.')
            run.post_stop()
            raise e

    def add_dependency(self, parent_uuid, parent_path, uuid, loop_callback):
        """
        Registers that the run with UUID uuid depends on path parent_path in
        bundle with UUID parent_uuid. Downloads the dependency if necessary, and
        returns the path to the dependency. Note, remove_dependency should be
        called for every dependency added.

        loop_callback is a method that is called repeatedly while downloading
        the dependency. If that method throws an exception, the download gets
        interrupted and add_dependency fails with that same exception.
        """
        assert(not self.shared_file_system)
        dependency_path, should_download = (
            self._dependency_manager.add_dependency(parent_uuid, parent_path, uuid))
        if should_download:
            logger.debug('Downloading dependency %s/%s', parent_uuid, parent_path)
            try:
                download_success = False
                fileobj, target_type = (
                    self._bundle_service.get_bundle_contents(parent_uuid, parent_path))
                with closing(fileobj):
                    # "Bug" the fileobj's read function so that we can keep
                    # track of the number of bytes downloaded so far.
                    old_read_method = fileobj.read
                    bytes_downloaded = [0]
                    def interruptable_read(*args, **kwargs):
                        data = old_read_method(*args, **kwargs)
                        bytes_downloaded[0] += len(data)
                        loop_callback(bytes_downloaded[0])
                        return data
                    fileobj.read = interruptable_read

                    self._store_dependency(dependency_path, fileobj, target_type)
                    download_success = True
            finally:
                logger.debug('Finished downloading dependency %s/%s', parent_uuid, parent_path)
                self._dependency_manager.finish_download(
                    parent_uuid, parent_path, download_success)

        return dependency_path

    def _store_dependency(self, dependency_path, fileobj, target_type):
        try:
            if target_type == 'directory':
                un_tar_directory(fileobj, dependency_path, 'gz')
            else:
                with open(dependency_path, 'wb') as f:
                    shutil.copyfileobj(fileobj, f)
        except:
            remove_path(dependency_path)
            raise

    def remove_dependency(self, parent_uuid, parent_path, uuid):
        """
        Unregisters that the run with UUID uuid depends on path parent_path in
        bundle with UUID parent_uuid. This method is safe to call on
        dependencies that were never added with add_dependency.
        """
        assert(not self.shared_file_system)
        self._dependency_manager.remove_dependency(parent_uuid, parent_path, uuid)

    def _read(self, socket_id, uuid, path, read_args):
        socket = self._bundle_service.socket(worker_id=self.id, socket_id=socket_id)
        run = self._worker_state_manager._get_run(uuid)
        if run is None:
            message = {
                'error_code': httplib.INTERNAL_SERVER_ERROR,
                'error_message': BUNDLE_NO_LONGER_RUNNING_MESSAGE,
            }
            socket.reply(message)
        else:
            run.read(path=path, read_args=read_args, socket=socket)

    def _write(self, uuid, subpath, string):
        run = self._worker_state_manager._get_run(uuid)
        if run is not None:
            run.write(subpath, string)

    def _kill(self, uuid):
        run = self._worker_state_manager._get_run(uuid)
        if run is not None:
            run.kill('Kill requested.')

    def finish_run(self, uuid):
        """
        Registers that the run with the given UUID has finished.
        """
        run = self._worker_state_manager._get_run(uuid)
        if run:
            run.post_stop()
            self._worker_state_manager.finish_run(uuid)
            if not self.shared_file_system:
                self._dependency_manager.finish_run(uuid)

    def _checkout(self):
        try:
            self._bundle_service.checkout(self.id)
        except BundleServiceException:
            traceback.print_exc()

    def _upgrade(self):
        logger.debug('Upgrading')
        worker_dir = os.path.dirname(os.path.realpath(__file__))

        while True:
            try:
                with closing(self._bundle_service.get_code()) as code:
                    remove_path(worker_dir)
                    un_tar_directory(code, worker_dir, 'gz')
                    break
            except Exception:
                traceback.print_exc()
                time.sleep(1)

        exit(123)
