import datetime
import os
import re
import sys
import threading
import time
import traceback

from codalab.common import State
from codalab.lib import bundle_util, formatting
from worker.file_util import remove_path


class BundleManager(object):
    """
    Assigns run bundles to workers and makes make bundles.
    """

    @staticmethod
    def create(codalab_manager):
        config = codalab_manager.config.get('workers')
        if not config:
            print >> sys.stderr, 'Config is missing a workers section'
            exit(1)

        if 'torque' in config:
            from codalab.worker.torque_bundle_manager import TorqueBundleManager
            self = TorqueBundleManager(codalab_manager, config['torque'])
        else:
            from codalab.worker.default_bundle_manager import DefaultBundleManager
            self = DefaultBundleManager()

        self._model = codalab_manager.model()
        self._worker_model = codalab_manager.worker_model()
        self._bundle_store = codalab_manager.bundle_store()
        self._upload_manager = codalab_manager.upload_manager()
        
        self._exiting_lock = threading.Lock()
        self._exiting = False

        self._make_uuids_lock = threading.Lock()
        self._make_uuids = set()

        if 'default_docker_image' not in config:
            print >> sys.stderr, 'workers config missing default_docker_image'
            exit(1)
        self._default_docker_image = config['default_docker_image']

        def parse(to_value, field):
            return to_value(config[field]) if field in config else None
        self._default_request_time = parse(formatting.parse_duration, 'default_request_time')
        self._default_request_memory = parse(formatting.parse_size, 'default_request_memory')
        self._default_request_disk = parse(formatting.parse_size, 'default_request_disk')
        self._max_request_time = parse(formatting.parse_duration, 'max_request_time')
        self._max_request_memory = parse(formatting.parse_size, 'max_request_memory')
        self._max_request_disk = parse(formatting.parse_size, 'max_request_disk')

        self._default_request_cpus = config.get('default_request_cpus')
        self._default_request_network = config.get('default_request_network')
        self._default_request_queue = config.get('default_request_queue')
        self._default_request_priority = config.get('default_request_priority')

        return self

    def run(self, sleep_time):
        while not self._is_exiting():
            try:
                self._run_iteration()
            except Exception:
                traceback.print_exc()

            time.sleep(sleep_time)

        while self._is_making_bundles():
            time.sleep(sleep_time)

    def signal(self):
        with self._exiting_lock:
            self._exiting = True

    def _is_exiting(self):
        with self._exiting_lock:
            return self._exiting

    def _run_iteration(self):
        self._stage_bundles()
        self._make_bundles()
        self._schedule_run_bundles()

    def _stage_bundles(self):
        """
        Stages bundles by:
            1) Failing any bundles that have any missing or failed dependencies.
            2) Staging any bundles that have all ready dependencies.
        """
        bundles = self._model.batch_get_bundles(state=State.CREATED)
        parent_uuids = set(
            dep.parent_uuid for bundle in bundles for dep in bundle.dependencies)
        parents = self._model.batch_get_bundles(uuid=parent_uuids)
        all_parent_states = {parent.uuid: parent.state for parent in parents}
        all_parent_uuids = set(all_parent_states)

        bundles_to_fail = []
        bundles_to_stage = []
        for bundle in bundles:
            parent_uuids = set(dep.parent_uuid for dep in bundle.dependencies)

            missing_uuids = parent_uuids - all_parent_uuids
            if missing_uuids:
                bundles_to_fail.append(
                    (bundle,
                     'Missing parent bundles: %s' % ', '.join(missing_uuids)))
                continue

            parent_states = {uuid: all_parent_states[uuid]
                             for uuid in parent_uuids}

            acceptable_states = [State.READY]
            if bundle.metadata.allow_failed_dependencies:
                acceptable_states.append(State.FAILED)
            else:
                failed_uuids = [
                    uuid for uuid, state in parent_states.iteritems()
                    if state == State.FAILED]
                if failed_uuids:
                    bundles_to_fail.append(
                        (bundle,
                         'Parent bundles failed: %s' % ', '.join(failed_uuids)))
                    continue

            if all(state in acceptable_states for state in parent_states.itervalues()):
                bundles_to_stage.append(bundle)

        for bundle, failure_message in bundles_to_fail:
            self._model.update_bundle(
                bundle, {'state': State.FAILED,
                         'metadata': {'failure_message': failure_message}})
        for bundle in bundles_to_stage:
            self._model.update_bundle(
                bundle, {'state': State.STAGED})

    def _make_bundles(self):
        # Re-stage any stuck bundles. This would happen if the bundle manager
        # died.
        for bundle in self._model.batch_get_bundles(state=State.MAKING, bundle_type='make'):
            if not self._is_making_bundle(bundle.uuid):
                self._model.update_bundle(bundle, {'state': State.STAGED})

        for bundle in self._model.batch_get_bundles(state=State.STAGED, bundle_type='make'):
            self._model.update_bundle(bundle, {'state': State.MAKING})
            with self._make_uuids_lock:
                self._make_uuids.add(bundle.uuid)
            # Making a bundle could take time, so do the work in a separate
            # thread to ensure quick scheduling.
            threading.Thread(
                target=BundleManager._make_bundle, args=[self, bundle]
            ).start()

    def _is_making_bundles(self):
        with self._make_uuids_lock:
            return bool(self._make_uuids)

    def _is_making_bundle(self, uuid):
        with self._make_uuids_lock:
            return uuid in self._make_uuids

    def _make_bundle(self, bundle):
        try:
            path = os.path.abspath(self._bundle_store.get_bundle_location(bundle.uuid))
            remove_path(path)
            os.mkdir(path)

            # Compute a dict mapping parent_uuid -> parent for each dep of this bundle.
            parent_uuids = set(dep.parent_uuid for dep in bundle.dependencies)
            parents = self._model.batch_get_bundles(uuid=parent_uuids)
            parent_dict = {parent.uuid: parent for parent in parents}

            bundle.install_dependencies(self._bundle_store, parent_dict, path, copy=True)

            self._upload_manager.update_metadata_and_save(bundle, new_bundle=False)
            self._model.update_bundle(bundle, {'state': State.READY})
        except Exception as e:
            self._model.update_bundle(
                bundle, {'state': State.FAILED,
                         'metadata': {'failure_message': str(e)}})
        finally:
            with self._make_uuids_lock:
                self._make_uuids.remove(bundle.uuid)

    def _cleanup_dead_workers(self, workers, callback=None):
        """
        Clean-up workers that we haven't heard from for more than 5 minutes.
        Such workers probably died without checking out properly.
        """
        for worker in workers.workers():
            if datetime.datetime.now() - worker['checkin_time'] > datetime.timedelta(minutes=5):
                self._worker_model.worker_cleanup(worker['user_id'], worker['worker_id'])
                workers.remove(worker)
                if callback is not None:
                    callback(worker)

    def _restage_stuck_starting_bundles(self, workers):
        """
        Moves bundles that got stuck in the STARTING state back to the STAGED
        state so that they can be scheduled to run again.
        """
        for bundle in self._model.batch_get_bundles(state=State.STARTING, bundle_type='run'):
            if (not workers.is_running(bundle.uuid) or  # Dead worker.
                time.time() - bundle.metadata.last_updated > 5 * 60):  # Run message went missing.
                if self._model.restage_bundle(bundle):
                    workers.restage(bundle.uuid)

    def _fail_stuck_running_bundles(self, workers):
        """
        Fails bundles that got stuck in the RUNNING state.
        """
        for bundle in self._model.batch_get_bundles(state=State.RUNNING, bundle_type='run'):
            if (not workers.is_running(bundle.uuid) or  # Dead worker.
                time.time() - bundle.metadata.last_updated > 60 * 60):  # Shouldn't really happen, but let's be safe.
                self._model.finalize_bundle(bundle, -1, exitcode=None, failure_message='Worker died')
                workers.unqueue(bundle.uuid)

    def _schedule_run_bundles_on_workers(self, workers, user_owned):
        """
        Schedules STAGED bundles to run on the given workers. If user_owned is
        True, then schedules on workers run by the owner of each bundle.
        Otherwise, uses CodaLab-owned workers, which have user ID root_user_id.
        """
        for bundle in self._model.batch_get_bundles(state=State.STAGED, bundle_type='run'):
            if user_owned:
                workers_list = workers.user_owned_workers(bundle.owner_id)
            else:
                workers_list = workers.user_owned_workers(self._model.root_user_id)

            workers_list = self._filter_and_sort_workers(workers_list, bundle)

            for worker in workers_list:
                if self._try_start_bundle(workers, worker, bundle):
                    break
                else:
                    continue  # Try the next worker.

    def _filter_and_sort_workers(self, workers_list, bundle):
        """
        Filters the workers to those than can run the given bundle and returns
        the list sorted in order of preference for running the bundle.
        """
        # Filter by slots.
        workers_list = filter(lambda worker: worker['slots'] - len(worker['run_uuids']) > 0,
                              workers_list)

        # Filter by CPUs.
        request_cpus = self._compute_request_cpus(bundle)
        if request_cpus:
            workers_list = filter(lambda worker: worker['cpus'] >= request_cpus,
                                  workers_list)

        # Filter by memory.
        request_memory = self._compute_request_memory(bundle)
        if request_memory:
            workers_list = filter(lambda worker: worker['memory_bytes'] >= request_memory,
                                  workers_list)

        # Filter by tag.
        request_queue = bundle.metadata.request_queue or self._default_request_queue
        if request_queue:
            tagm = re.match('tag=(.+)', request_queue)
            if tagm:
                workers_list = filter(lambda worker: worker['tag'] == tagm.group(1),
                                      workers_list)
            else:
                # We don't know how to handle this type of request queue
                # argument.
                return []

        # Sort according to the number of dependencies available, breaking
        # ties by the number of free slots.
        needed_deps = set(map(lambda dep: (dep.parent_uuid, dep.parent_path),
                              bundle.dependencies))
        def get_sort_key(worker):
            deps = set(worker['dependencies'])
            return (len(needed_deps & deps), worker['slots'] - len(worker['run_uuids']))
        workers_list.sort(key=get_sort_key, reverse=True)

        return workers_list

    def _try_start_bundle(self, workers, worker, bundle):
        """
        Tries to start running the bundle on the given worker, returning False
        if that failed.
        """
        if self._model.set_starting_bundle(bundle, worker['user_id'], worker['worker_id']):
            workers.set_starting(bundle.uuid, worker)
            if self._worker_model.send_json_message(
                worker['socket_id'], self._construct_run_message(worker, bundle), 0.2):
                return True
            else:
                self._model.restage_bundle(bundle)
                workers.restage(bundle.uuid)
                return False
        else:
            return False

    def _compute_request_cpus(self, bundle):
        """
        Compute the CPU limit used for scheduling the run.
        """
        return bundle.metadata.request_cpus or self._default_request_cpus

    def _compute_request_memory(self, bundle):
        """
        Compute the memory limit used for scheduling the run.
        """
        if bundle.metadata.request_memory:
            return formatting.parse_size(bundle.metadata.request_memory)
        return self._default_request_memory

    def _construct_run_message(self, worker, bundle):
        """
        Constructs the run message that is sent to the given worker to tell it
        to run the given bundle.
        """
        message = {}
        message['type'] = 'run'
        message['bundle'] = bundle_util.bundle_to_bundle_info(self._model, bundle)
        if self._worker_model.shared_file_system and worker['user_id'] == self._model.root_user_id:
            message['bundle']['location'] = self._bundle_store.get_bundle_location(bundle.uuid)
            for dependency in message['bundle']['dependencies']:
                dependency['location'] = self._bundle_store.get_bundle_location(dependency['parent_uuid'])

        # Figure out the resource requirements.
        resources = message['resources'] = {}

        resources['docker_image'] = (bundle.metadata.request_docker_image or
                                     self._default_docker_image)

        # Parse |request_string| using |to_value|, but don't exceed |max_value|.
        def parse_and_min(to_value, request_string, default_value, max_value):
            # Use default if request value doesn't exist
            if request_string:
                request_value = to_value(request_string)
            else:
                request_value = default_value
            if request_value and max_value:
                return int(min(request_value, max_value))
            elif request_value:
                return int(request_value)
            elif max_value:
                return int(max_value)
            else:
                return None

        # These limits are used for killing runs that use too many resources.
        resources['request_time'] = parse_and_min(formatting.parse_duration,
                                                  bundle.metadata.request_time,
                                                  self._default_request_time,
                                                  self._max_request_time)
        resources['request_memory'] = parse_and_min(formatting.parse_size,
                                                    bundle.metadata.request_memory,
                                                    self._default_request_memory,
                                                    self._max_request_memory)
        resources['request_disk'] = parse_and_min(formatting.parse_size,
                                                  bundle.metadata.request_disk,
                                                  self._default_request_disk,
                                                  self._max_request_disk)

        resources['request_network'] = (bundle.metadata.request_network or
                                        self._default_request_network)

        return message
