from contextlib import closing
from subprocess import check_output
import hashlib
import logging
import multiprocessing
import os
import shutil
import threading
import time
import traceback

from bundle_service_client import BundleServiceException
from dependency_manager import DependencyManager
from file_util import remove_path, un_gzip_stream, un_tar_directory
from dependency_diff import hash_dependencies, diff_dependencies
from run import Run

VERSION = 6

logger = logging.getLogger(__name__)

class Worker(object):
    """
    This class is responsible for:

        1) Registering with the bundle service and receiving all messages
           sent to the worker.
        2) Managing all the runs currently executing on the worker and
           forwarding messages associated with those runs to the appropriate
           instance of the Run class.
        3) Managing the storage of bundles, both running bundles as well as
           their dependencies.
        4) Upgrading the worker.
    """
    def __init__(self, id, tag, work_dir, max_work_dir_size_bytes,
                 shared_file_system, slots,
                 bundle_service, docker):
        self.id = id
        self._tag = tag
        self.shared_file_system = shared_file_system
        self._bundle_service = bundle_service
        self._docker = docker
        self._slots = slots

        if not self.shared_file_system:
            # Manages which dependencies are available.
            self._dependency_manager = DependencyManager(work_dir, max_work_dir_size_bytes)
            self._reset_last_dependencies()

        # Dictionary from UUID to Run that keeps track of bundles currently
        # running. These runs are added to this dict inside _run, and removed
        # when the Run class calls finish_run.
        self._runs_lock = threading.Lock()
        self._runs = {}
 
        self._exiting_lock = threading.Lock()
        self._exiting = False
        self._should_upgrade = False

    def run(self):
        if not self.shared_file_system:
            self._dependency_manager.start_cleanup_thread()

        while self._should_run():
            try:
                self._checkin()
            except Exception:
                traceback.print_exc()
                time.sleep(1)

        self._checkout()

        if not self.shared_file_system:
            self._dependency_manager.stop_cleanup_thread()

        if self._should_upgrade:
            self._upgrade()

    def signal(self):
        print('Exiting: Will wait for exiting jobs to finish, but will not '
              'start any new jobs.')
        with self._exiting_lock:
            self._exiting = True

    def _is_exiting(self):
        with self._exiting_lock:
            return self._exiting

    def _should_run(self):
        if not self._is_exiting():
            return True
        with self._runs_lock:
            if self._runs:
                return True
        return False

    def _get_memory_bytes(self):
        try:
            return os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')
        except ValueError:
            # Fallback to sysctl when os.sysconf('SC_PHYS_PAGES') fails on OS X
            return int(check_output(['sysctl', '-n', 'hw.memsize']).strip())

    def _get_dependencies(self):
        """
        Get dependencies to send to server on checkin.

        :return: either the full list of dependencies or a patch of the form:
          {
            "base_hash": "<hash of the previous set of dependencies>",
            "patch": { <patch based on the previous set of dependencies> }
          }
        """
        if self.shared_file_system:
            return []

        dependencies = self._dependency_manager.dependencies()
        new_hash = hash_dependencies(dependencies)

        # Send the full list of dependencies if there is no record of
        # successfully reported dependencies, otherwise send a patch
        if self._last_reported_dependencies is None or \
           self._last_reported_dependencies_hash is None:
            result = dependencies
        else:
            result = {
                'base_hash': self._last_reported_dependencies_hash,
                'patch': diff_dependencies(self._last_reported_dependencies, dependencies)
            }

        # Assume these dependencies will be successfully reported
        # If there is a failure, these variables should be set to None again
        self._last_reported_dependencies = dependencies
        self._last_reported_dependencies_hash = new_hash

        return result

    def _reset_last_dependencies(self):
        """
        Reset the variables for the last reported dependencies.
        This will make the worker send the full list of dependencies to the
        server on its next checkin, instead of a patch.
        """
        self._last_reported_dependencies = None
        self._last_reported_dependencies_hash = None

    def _checkin(self):
        request = {
            'version': VERSION,
            'will_upgrade': self._should_upgrade,
            'tag': self._tag,
            'slots': self._slots if not self._is_exiting() else 0,
            'cpus': multiprocessing.cpu_count(),
            'memory_bytes': self._get_memory_bytes(),
            'dependencies': self._get_dependencies(),
        }

        try:
            response = self._bundle_service.checkin(self.id, request)
        except BundleServiceException:
            # Make sure that full dependencies list is sent on next checkin
            self._reset_last_dependencies()
            raise

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
        run = Run(self._bundle_service, self._docker, self,
                  bundle, bundle_path, resources)
        if run.run():
            with self._runs_lock:
                self._runs[bundle['uuid']] = run

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
                fileobj, filename = (
                    self._bundle_service.get_bundle_contents(parent_uuid, parent_path))
                with closing(fileobj):
                    old_read_method = fileobj.read
                    bytes_downloaded = [0]
                    def interruptable_read(*args, **kwargs):
                        data = old_read_method(*args, **kwargs)
                        bytes_downloaded[0] += len(data)
                        loop_callback(bytes_downloaded[0])
                        return data
                    fileobj.read = interruptable_read
  
                    self._store_dependency(dependency_path, fileobj, filename)
                    download_success = True
            finally:
                logger.debug('Finished downloading dependency %s/%s', parent_uuid, parent_path)
                self._dependency_manager.finish_download(
                    parent_uuid, parent_path, download_success)
        
        return dependency_path

    def _store_dependency(self, dependency_path, fileobj, filename):
        try:
            if filename.endswith('.tar.gz'):
                un_tar_directory(fileobj, dependency_path, 'gz')
            else:
                with open(dependency_path, 'wb') as f:
                    shutil.copyfileobj(un_gzip_stream(fileobj), f)
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
        run = self._get_run(uuid)
        if run is None:
            Run.read_run_missing(self._bundle_service, self, socket_id)
        else:
            # Reads may take a long time, so do the read in a separate thread.
            threading.Thread(target=Run.read,
                             args=(run, socket_id, path, read_args)).start()

    def _write(self, uuid, subpath, string):
        run = self._get_run(uuid)
        if run is not None:
            run.write(subpath, string)

    def _kill(self, uuid):
        run = self._get_run(uuid)
        if run is not None:
            run.kill('Kill requested')

    def _get_run(self, uuid):
        with self._runs_lock:
            return self._runs.get(uuid)

    def finish_run(self, uuid):
        """
        Registers that the run with the given UUID has finished.
        """
        with self._runs_lock:
            del self._runs[uuid]
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
