import datetime
import logging
import os
import random
import re
import sys
import threading
import time
import traceback

from codalab.objects.permission import check_bundles_have_read_permission
from codalab.common import PermissionError, NotFoundError
from codalab.lib import bundle_util, formatting, path_util
from codalab.server.worker_info_accessor import WorkerInfoAccessor
from codalab.worker.file_util import remove_path
from codalab.worker.bundle_state import State, RunResources


logger = logging.getLogger(__name__)

WORKER_TIMEOUT_SECONDS = 60
SECONDS_PER_DAY = 60 * 60 * 24
# Fail unresponsive bundles in uploading, staged and running state after this many days.
BUNDLE_TIMEOUT_DAYS = 60


class BundleManager(object):
    """
    Assigns run bundles to workers and makes make bundles.
    """

    def __init__(self, codalab_manager):
        config = codalab_manager.config.get('workers')
        if not config:
            print('config.json file missing a workers section.', file=sys.stderr)
            sys.exit(1)

        self._model = codalab_manager.model()
        self._worker_model = codalab_manager.worker_model()
        self._bundle_store = codalab_manager.bundle_store()
        self._upload_manager = codalab_manager.upload_manager()

        self._exiting_lock = threading.Lock()
        self._exiting = False

        self._make_uuids_lock = threading.Lock()
        self._make_uuids = set()

        def parse(to_value, field):
            return to_value(config[field]) if field in config else None

        self._max_request_time = parse(formatting.parse_duration, 'max_request_time') or 0
        self._max_request_memory = parse(formatting.parse_size, 'max_request_memory') or 0
        self._max_request_disk = parse(formatting.parse_size, 'max_request_disk') or 0

        self._default_cpu_image = config.get('default_cpu_image')
        self._default_gpu_image = config.get('default_gpu_image')

        logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

    def run(self, sleep_time):
        logger.info('Bundle manager running!')
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
        self._fail_unresponsive_bundles()

    def _stage_bundles(self):
        """
        Stages bundles by:
            1) Failing any bundles that have any missing or failed dependencies.
            2) Staging any bundles that have all ready dependencies.
        """
        bundles = self._model.batch_get_bundles(state=State.CREATED)
        parent_uuids = set(dep.parent_uuid for bundle in bundles for dep in bundle.dependencies)
        parents = self._model.batch_get_bundles(uuid=parent_uuids)

        all_parent_states = {parent.uuid: parent.state for parent in parents}
        all_parent_uuids = set(all_parent_states)

        bundles_to_fail = []
        bundles_to_stage = []
        for bundle in bundles:
            parent_uuids = set(dep.parent_uuid for dep in bundle.dependencies)

            try:
                check_bundles_have_read_permission(
                    self._model, self._model.get_user(bundle.owner_id), parent_uuids
                )
            except PermissionError as e:
                bundles_to_fail.append((bundle, str(e)))
                continue

            missing_uuids = parent_uuids - all_parent_uuids
            if missing_uuids:
                bundles_to_fail.append(
                    (bundle, 'Missing parent bundles: %s' % ', '.join(missing_uuids))
                )
                continue

            parent_states = {uuid: all_parent_states[uuid] for uuid in parent_uuids}

            acceptable_states = [State.READY]
            if bundle.metadata.allow_failed_dependencies:
                acceptable_states.append(State.FAILED)
                acceptable_states.append(State.KILLED)
            else:
                failed_uuids = [
                    uuid for uuid, state in parent_states.items() if state == State.FAILED
                ]
                killed_uuids = [
                    uuid for uuid, state in parent_states.items() if state == State.KILLED
                ]
                failure_message = ''
                if failed_uuids:
                    failure_message += ' Parent bundles failed: %s' % ', '.join(failed_uuids)
                if killed_uuids:
                    failure_message += ' Parent bundles were killed: %s' % ', '.join(killed_uuids)
                if failure_message:
                    failure_message += ' (Please use the --allow-failed-dependencies flag to depend on results fo failed or killed bundles)'
                    bundles_to_fail.append((bundle, failure_message))
                    continue

            if all(state in acceptable_states for state in parent_states.values()):
                bundles_to_stage.append(bundle)

        for bundle, failure_message in bundles_to_fail:
            logger.info('Failing bundle %s: %s', bundle.uuid, failure_message)
            self._model.update_bundle(
                bundle, {'state': State.FAILED, 'metadata': {'failure_message': failure_message}}
            )
        for bundle in bundles_to_stage:
            logger.info('Staging %s', bundle.uuid)
            self._model.update_bundle(bundle, {'state': State.STAGED})

    def _make_bundles(self):
        # Re-stage any stuck bundles. This would happen if the bundle manager
        # died.
        for bundle in self._model.batch_get_bundles(state=State.MAKING, bundle_type='make'):
            if not self._is_making_bundle(bundle.uuid):
                logger.info('Re-staging make bundle %s', bundle.uuid)
                self._model.update_bundle(bundle, {'state': State.STAGED})

        for bundle in self._model.batch_get_bundles(state=State.STAGED, bundle_type='make'):
            logger.info('Making bundle %s', bundle.uuid)
            self._model.update_bundle(bundle, {'state': State.MAKING})
            with self._make_uuids_lock:
                self._make_uuids.add(bundle.uuid)
            # Making a bundle could take time, so do the work in a separate
            # thread to ensure quick scheduling.
            threading.Thread(target=BundleManager._make_bundle, args=[self, bundle]).start()

    def _is_making_bundles(self):
        with self._make_uuids_lock:
            return bool(self._make_uuids)

    def _is_making_bundle(self, uuid):
        with self._make_uuids_lock:
            return uuid in self._make_uuids

    def _make_bundle(self, bundle):
        try:
            bundle_location = self._bundle_store.get_bundle_location(bundle.uuid)
            path = os.path.normpath(bundle_location)

            deps = []
            for dep in bundle.dependencies:
                parent_bundle_path = os.path.normpath(
                    self._bundle_store.get_bundle_location(dep.parent_uuid)
                )
                dependency_path = os.path.normpath(
                    os.path.join(parent_bundle_path, dep.parent_path)
                )
                if not dependency_path.startswith(parent_bundle_path) or (
                    not os.path.islink(dependency_path) and not os.path.exists(dependency_path)
                ):
                    raise Exception(
                        'Invalid dependency %s'
                        % (path_util.safe_join(dep.parent_uuid, dep.parent_path))
                    )

                child_path = os.path.normpath(os.path.join(path, dep.child_path))
                if not child_path.startswith(path):
                    raise Exception('Invalid key for dependency: %s' % (dep.child_path))

                deps.append((dependency_path, child_path))

            remove_path(path)

            if len(deps) == 1 and deps[0][1] == path:
                path_util.copy(deps[0][0], path, follow_symlinks=False)
            else:
                os.mkdir(path)
                for dependency_path, child_path in deps:
                    path_util.copy(dependency_path, child_path, follow_symlinks=False)

            self._model.update_disk_metadata(bundle, bundle_location, enforce_disk_quota=True)
            logger.info('Finished making bundle %s', bundle.uuid)
            self._model.update_bundle(bundle, {'state': State.READY})
        except Exception as e:
            logger.info('Failing bundle %s: %s', bundle.uuid, str(e))
            self._model.update_bundle(
                bundle, {'state': State.FAILED, 'metadata': {'failure_message': str(e)}}
            )
        finally:
            with self._make_uuids_lock:
                self._make_uuids.remove(bundle.uuid)

    def _cleanup_dead_workers(self, workers, callback=None):
        """
        Clean-up workers that we haven't heard from for more than WORKER_TIMEOUT_SECONDS seconds.
        Such workers probably died without checking out properly.
        """
        for worker in workers.workers():
            if datetime.datetime.now() - worker['checkin_time'] > datetime.timedelta(
                seconds=WORKER_TIMEOUT_SECONDS
            ):
                logger.info(
                    'Cleaning up dead worker (%s, %s)', worker['user_id'], worker['worker_id']
                )
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
            if (
                not workers.is_running(bundle.uuid)
                or time.time() - bundle.metadata.last_updated > 5 * 60
            ):  # Run message went missing.
                logger.info('Re-staging run bundle %s', bundle.uuid)
                if self._model.transition_bundle_staged(bundle):
                    workers.restage(bundle.uuid)

    def _acknowledge_recently_finished_bundles(self, workers):
        """
        Acknowledge recently finished bundles to workers so they can discard run information.
        """
        for bundle in self._model.batch_get_bundles(state=State.FINALIZING, bundle_type='run'):
            worker = self._model.get_bundle_worker(bundle.uuid)
            if worker is None:
                logger.info(
                    'Bringing bundle offline %s: %s', bundle.uuid, 'No worker claims bundle'
                )
                self._model.transition_bundle_worker_offline(bundle)
            elif self._worker_model.send_json_message(
                worker['socket_id'], {'type': 'mark_finalized', 'uuid': bundle.uuid}, 0.2
            ):
                logger.info('Acknowledged finalization of run bundle %s', bundle.uuid)
                bundle_location = self._bundle_store.get_bundle_location(bundle.uuid)
                self._model.transition_bundle_finished(bundle, bundle_location)

    def _bring_offline_stuck_running_bundles(self, workers):
        """
        Make bundles that got stuck in the RUNNING or PREPARING state into WORKER_OFFLINE state.
        Bundles in WORKER_OFFLINE state can be moved back to the RUNNING or PREPARING state if a
        worker resumes the bundle indicating that it's still in one of those states.
        """
        active_bundles = self._model.batch_get_bundles(
            state=State.RUNNING, bundle_type='run'
        ) + self._model.batch_get_bundles(state=State.PREPARING, bundle_type='run')
        now = time.time()
        for bundle in active_bundles:
            failure_message = None
            if not workers.is_running(bundle.uuid):
                failure_message = 'No worker claims bundle'
            if now - bundle.metadata.last_updated > WORKER_TIMEOUT_SECONDS:
                failure_message = 'Worker offline'
            if failure_message is not None:
                logger.info('Bringing bundle offline %s: %s', bundle.uuid, failure_message)
                self._model.transition_bundle_worker_offline(bundle)

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
                if not self._model.get_user_parallel_run_quota_left(bundle.owner_id):
                    logger.info(
                        "User %s has no parallel run quota left, skipping job for now",
                        bundle.owner_id,
                    )
                    continue  # Don't start this bundle yet
                workers_list = workers.user_owned_workers(self._model.root_user_id)

            workers_list = self._deduct_worker_resources(workers_list)
            bundle_resources = self._compute_bundle_resources(bundle)
            workers_list = self._filter_and_sort_workers(workers_list, bundle, bundle_resources)

            for worker in workers_list:
                if self._try_start_bundle(workers, worker, bundle):
                    break

    def _deduct_worker_resources(self, workers_list):
        """
        From each worker, subtract resources used by running bundles. Modifies the list.
        """
        for worker in workers_list:
            for uuid in worker['run_uuids']:
                try:
                    bundle = self._model.get_bundle(uuid)
                except NotFoundError:
                    logger.info(
                        'Bundle %s in WorkerInfoAccessor but no longer found. Skipping for resource deduction.',
                        uuid,
                    )
                    continue
                bundle_resources = self._compute_bundle_resources(bundle)
                worker['cpus'] -= bundle_resources.cpus
                worker['gpus'] -= bundle_resources.gpus
                worker['memory_bytes'] -= bundle_resources.memory
        return workers_list

    @staticmethod
    def _filter_and_sort_workers(workers_list, bundle, bundle_resources):
        """
        Filters the workers to those that can run the given bundle and returns
        the list sorted in order of preference for running the bundle.
        """

        # keep track of which workers have GPUs
        has_gpu = {}
        for worker in workers_list:
            worker_id = worker['worker_id']
            has_gpu[worker_id] = worker['gpus'] > 0

        # Filter by CPUs.
        workers_list = [
            worker for worker in workers_list if worker['cpus'] >= bundle_resources.cpus
        ]

        # Filter by GPUs.
        if bundle_resources.gpus:
            workers_list = [
                worker for worker in workers_list if worker['gpus'] >= bundle_resources.gpus
            ]

        # Filter by memory.
        workers_list = [
            worker for worker in workers_list if worker['memory_bytes'] >= bundle_resources.memory
        ]

        # Filter by tag.
        request_queue = bundle.metadata.request_queue
        if request_queue:
            tagm = re.match('tag=(.+)', request_queue)
            if tagm:
                workers_list = [worker for worker in workers_list if worker['tag'] == tagm.group(1)]
            else:
                # We don't know how to handle this type of request queue
                # argument.
                return []

        # Sort workers list according to these keys in the following succession:
        #  - whether the worker is a CPU-only worker, if the bundle doesn't request GPUs
        #  - number of dependencies available, descending
        #  - number of free cpus, descending
        #  - random key
        #
        # Breaking ties randomly is important, since multiple workers frequently
        # have the same number of dependencies and free CPUs for a given bundle
        # (in particular, bundles with no dependencies) and we may end up
        # selecting the same worker over and over again for new jobs. While this
        # is not a problem for the performance of the jobs themselves, this can
        # cause one worker to collect a disproportionate number of dependencies
        # in its cache.
        needed_deps = set([(dep.parent_uuid, dep.parent_path) for dep in bundle.dependencies])

        def get_sort_key(worker):
            if worker['shared_file_system']:
                num_available_deps = len(needed_deps)
            else:
                deps = set(worker['dependencies'])
                num_available_deps = len(needed_deps & deps)
            worker_id = worker['worker_id']

            # if the bundle doesn't request GPUs (only request CPUs), prioritize workers that don't have GPUs
            gpu_priority = bundle_resources.gpus or not has_gpu[worker_id]
            return (gpu_priority, num_available_deps, worker['cpus'], random.random())

        workers_list.sort(key=get_sort_key, reverse=True)

        return workers_list

    def _try_start_bundle(self, workers, worker, bundle):
        """
        Tries to start running the bundle on the given worker, returning False
        if that failed.
        """
        if self._model.transition_bundle_starting(bundle, worker['user_id'], worker['worker_id']):
            workers.set_starting(bundle.uuid, worker)
            if worker['shared_file_system']:
                # On a shared file system we create the path here to avoid NFS
                # directory cache issues.
                path = self._bundle_store.get_bundle_location(bundle.uuid)
                remove_path(path)
                os.mkdir(path)
            if self._worker_model.send_json_message(
                worker['socket_id'], self._construct_run_message(worker, bundle), 0.2
            ):
                logger.info('Starting run bundle %s', bundle.uuid)
                return True
            else:
                self._model.transition_bundle_staged(bundle)
                workers.restage(bundle.uuid)
                return False
        else:
            return False

    @staticmethod
    def _compute_request_cpus(bundle):
        """
        Compute the CPU limit used for scheduling the run.
        The default of 1 is for backwards compatibilty for
        runs from before when we added client-side defaults
        """
        if not bundle.metadata.request_cpus:
            return 1
        return bundle.metadata.request_cpus

    @staticmethod
    def _compute_request_gpus(bundle):
        """
        Compute the GPU limit used for scheduling the run.
        The default of 0 is for backwards compatibilty for
        runs from before when we added client-side defaults
        """
        if bundle.metadata.request_gpus is None:
            return 0
        return bundle.metadata.request_gpus

    @staticmethod
    def _compute_request_memory(bundle):
        """
        Compute the memory limit used for scheduling the run.
        The default of 2g is for backwards compatibilty for
        runs from before when we added client-side defaults
        """
        if not bundle.metadata.request_memory:
            return formatting.parse_size('2g')
        return formatting.parse_size(bundle.metadata.request_memory)

    def _compute_request_disk(self, bundle):
        """
        Compute the disk limit used for scheduling the run.
        The default is min(disk quota the user has left, global max)
        """
        if not bundle.metadata.request_disk:
            return min(
                self._model.get_user_disk_quota_left(bundle.owner_id) - 1, self._max_request_disk
            )
        return formatting.parse_size(bundle.metadata.request_disk)

    def _compute_request_time(self, bundle):
        """
        Compute the time limit used for scheduling the run.
        The default is min(time quota the user has left, global max)
        """
        if not bundle.metadata.request_time:
            return min(
                self._model.get_user_time_quota_left(bundle.owner_id) - 1, self._max_request_time
            )
        return formatting.parse_duration(bundle.metadata.request_time)

    def _get_docker_image(self, bundle):
        """
        Set docker image to be the default if not specified
        Unlike other metadata fields this can actually be None
        from client
        Also add the `latest` tag if no tag is specified to be
        consistent with Docker's own behavior.
        """
        if not bundle.metadata.request_docker_image:
            if bundle.metadata.request_gpus:
                docker_image = self._default_gpu_image
            else:
                docker_image = self._default_cpu_image
        else:
            docker_image = bundle.metadata.request_docker_image
        if ':' not in docker_image:
            docker_image += ':latest'
        return docker_image

    def _construct_run_message(self, worker, bundle):
        """
        Constructs the run message that is sent to the given worker to tell it
        to run the given bundle.
        """
        message = {}
        message['type'] = 'run'
        message['bundle'] = bundle_util.bundle_to_bundle_info(self._model, bundle)
        if worker['shared_file_system']:
            message['bundle']['location'] = self._bundle_store.get_bundle_location(bundle.uuid)
            for dependency in message['bundle']['dependencies']:
                dependency['location'] = self._bundle_store.get_bundle_location(
                    dependency['parent_uuid']
                )

        # Figure out the resource requirements.
        bundle_resources = self._compute_bundle_resources(bundle)
        message['resources'] = bundle_resources.to_dict()
        return message

    def _compute_bundle_resources(self, bundle):
        return RunResources(
            cpus=self._compute_request_cpus(bundle),
            gpus=self._compute_request_gpus(bundle),
            docker_image=self._get_docker_image(bundle),
            time=self._compute_request_time(bundle),
            memory=self._compute_request_memory(bundle),
            disk=self._compute_request_disk(bundle),
            network=bundle.metadata.request_network,
        )

    def _fail_unresponsive_bundles(self):
        """
        Fail bundles in uploading, staged and running state if we haven't heard from them for more than
        BUNDLE_TIMEOUT_DAYS days.
        """
        bundles_to_fail = (
            self._model.batch_get_bundles(state=State.UPLOADING)
            + self._model.batch_get_bundles(state=State.STAGED)
            + self._model.batch_get_bundles(state=State.RUNNING)
        )
        now = time.time()

        for bundle in bundles_to_fail:
            # For simplicity, we use field metadata.created to calculate timeout for now.
            # Ideally, we should use field metadata.last_updated.
            if now - bundle.metadata.created > BUNDLE_TIMEOUT_DAYS * SECONDS_PER_DAY:
                failure_message = 'Bundle has been stuck in {} state for more than {} days.'.format(
                    bundle.state, BUNDLE_TIMEOUT_DAYS
                )
                logger.info('Failing bundle %s: %s', bundle.uuid, failure_message)
                self._model.update_bundle(
                    bundle,
                    {'state': State.FAILED, 'metadata': {'failure_message': failure_message}},
                )

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

    @staticmethod
    def _check_resource_failure(
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
            bundle_resources = self._compute_bundle_resources(bundle)
            failures = []

            failures.append(
                self._check_resource_failure(
                    bundle_resources.disk,
                    user_fail_string='Requested more disk (%s) than user disk quota left (%s)',
                    user_max=self._model.get_user_disk_quota_left(bundle.owner_id),
                    global_fail_string='Maximum job disk size (%s) exceeded (%s)',
                    global_max=self._max_request_disk,
                    pretty_print=formatting.size_str,
                )
            )

            failures.append(
                self._check_resource_failure(
                    bundle_resources.time,
                    user_fail_string='Requested more time (%s) than user time quota left (%s)',
                    user_max=self._model.get_user_time_quota_left(bundle.owner_id),
                    global_fail_string='Maximum job time (%s) exceeded (%s)',
                    global_max=self._max_request_time,
                    pretty_print=formatting.duration_str,
                )
            )

            failures.append(
                self._check_resource_failure(
                    bundle_resources.memory,
                    global_fail_string='Requested more memory (%s) than maximum limit (%s)',
                    global_max=self._max_request_memory,
                    pretty_print=formatting.size_str,
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
