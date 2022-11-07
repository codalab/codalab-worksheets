import copy
import datetime
import logging
import os
import random
import shutil
import sys
import tempfile
import threading
import time
import traceback

from apache_beam.io.filesystems import FileSystems
from collections import defaultdict
from typing import List

from codalab.objects.permission import (
    check_bundles_have_read_permission,
    check_bundle_have_run_permission,
)
from codalab.common import NotFoundError, PermissionError, parse_linked_bundle_url
from codalab.lib import bundle_util, formatting, path_util
from codalab.server.worker_info_accessor import WorkerInfoAccessor
from codalab.worker.file_util import remove_path
from codalab.worker.un_tar_directory import un_tar_directory
from codalab.worker.bundle_state import State, RunResources
from codalab.worker.download_util import BundleTarget

logger = logging.getLogger(__name__)

SECONDS_PER_DAY = 60 * 60 * 24
# Fail unresponsive bundles in uploading, staged and running state after this many days.
BUNDLE_TIMEOUT_DAYS = 60
# Impose a minimum container request memory 4mb (4 * 1024 * 1024 bytes), same as docker's minimum allowed value
# https://docs.docker.com/config/containers/resource_constraints/#limit-a-containers-access-to-memory
# When using the REST api, it is allowed to set Memory to 0 but that means the container has unbounded
# access to the host machine's memory, which we have decided to not allow
MINIMUM_REQUEST_MEMORY_BYTES = 4 * 1024 * 1024
# Deduct DISK_QUOTA_SLACK_BYTES from the max user disk quota bytes when computing the default amount of disk space to
# request. Then the default max disk quota that can be requested becomes disk quota left - DISK_QUOTA_SLACK_BYTES.
DISK_QUOTA_SLACK_BYTES = 0.5 * 1024 * 1024 * 1024


def normpath(path):
    """Performs os.path.normpath on a path if it is on the filesystem, but if it is on Beam,
    doesn't do anything to the path.
    """
    if parse_linked_bundle_url(path).uses_beam:
        return path
    return os.path.normpath(path)


class BundleManager(object):
    """
    Assigns run bundles to workers and makes make bundles.
    """

    def __init__(self, codalab_manager, worker_timeout_seconds=60):
        config = codalab_manager.config.get('workers')
        if not config:
            print('config.json file missing a workers section.', file=sys.stderr)
            sys.exit(1)

        self._model = codalab_manager.model()
        self._worker_model = codalab_manager.worker_model()
        self._bundle_store = codalab_manager.bundle_store()
        self._upload_manager = codalab_manager.upload_manager()
        self._download_manager = codalab_manager.download_manager()

        self._exiting_lock = threading.Lock()
        self._exiting = False

        self._make_uuids_lock = threading.Lock()
        self._make_uuids = set()

        def parse(to_value, field):
            return to_value(config[field]) if field in config else None

        self._worker_timeout_seconds = worker_timeout_seconds
        self._max_request_time = parse(formatting.parse_duration, 'max_request_time') or 0
        self._max_request_memory = parse(formatting.parse_size, 'max_request_memory') or 0
        self._min_request_memory = (
            parse(formatting.parse_size, 'min_request_memory') or MINIMUM_REQUEST_MEMORY_BYTES
        )
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

    def _set_staged_status(self, bundle, staged_status):
        self._model.update_bundle(bundle, {'metadata': {'staged_status': staged_status}})

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

            missing_uuids = parent_uuids - all_parent_uuids
            if missing_uuids:
                bundles_to_fail.append(
                    (bundle, 'Missing parent bundles: %s' % ', '.join(missing_uuids))
                )
                continue

            try:
                check_bundles_have_read_permission(
                    self._model, self._model.get_user(bundle.owner_id), parent_uuids
                )
            except PermissionError as e:
                bundles_to_fail.append((bundle, str(e)))
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
                    failure_message += 'Parent bundles failed: %s ' % ', '.join(failed_uuids)
                if killed_uuids:
                    failure_message += 'Parent bundles were killed: %s ' % ', '.join(killed_uuids)
                if failure_message:
                    failure_message += '(Please use the --allow-failed-dependencies flag to depend on results of failed or killed bundles) '
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
            self._model.update_bundle(
                bundle,
                {
                    'state': State.STAGED,
                    'metadata': {
                        'staged_status': "Bundle's dependencies are all ready. Waiting for the bundle to be assigned to a worker to be run."
                    },
                },
            )

    def _make_bundles(self) -> List[threading.Thread]:
        # Re-stage any stuck bundles. This would happen if the bundle manager
        # died.
        for bundle in self._model.batch_get_bundles(state=State.MAKING, bundle_type='make'):
            if not self._is_making_bundle(bundle.uuid):
                logger.info('Re-staging make bundle %s', bundle.uuid)
                self._model.update_bundle(bundle, {'state': State.STAGED})

        threads = []
        for bundle in self._model.batch_get_bundles(state=State.STAGED, bundle_type='make'):
            logger.info('Making bundle %s', bundle.uuid)
            self._model.update_bundle(bundle, {'state': State.MAKING})
            with self._make_uuids_lock:
                self._make_uuids.add(bundle.uuid)
            # Making a bundle could take time, so do the work in a separate
            # thread to ensure quick scheduling.
            t = threading.Thread(target=self._make_bundle, args=[bundle])
            threads.append(t)
            t.start()
        return threads

    def _is_making_bundles(self):
        with self._make_uuids_lock:
            return bool(self._make_uuids)

    def _is_making_bundle(self, uuid):
        with self._make_uuids_lock:
            return uuid in self._make_uuids

    def _make_bundle(self, bundle):
        try:
            bundle_link_url = getattr(bundle.metadata, "link_url", None)
            bundle_location = bundle_link_url or self._bundle_store.get_bundle_location(bundle.uuid)

            path = normpath(bundle_location)

            deps = []
            parent_bundle_link_urls = self._model.get_bundle_metadata(
                [dep.parent_uuid for dep in bundle.dependencies], "link_url"
            )
            with tempfile.TemporaryDirectory() as tempdir:
                for dep in bundle.dependencies:
                    parent_bundle_link_url = parent_bundle_link_urls.get(dep.parent_uuid)
                    try:
                        parent_bundle_path = parent_bundle_link_url or normpath(
                            self._bundle_store.get_bundle_location(dep.parent_uuid)
                        )
                    except NotFoundError:
                        raise Exception(
                            'Invalid dependency %s'
                            % (path_util.safe_join(dep.parent_uuid, dep.parent_path))
                        )
                    dependency_path = normpath(os.path.join(parent_bundle_path, dep.parent_path))
                    if not dependency_path.startswith(parent_bundle_path) or (
                        not os.path.islink(dependency_path)
                        and not FileSystems.exists(dependency_path.rstrip("/"))
                    ):
                        raise Exception(
                            'Invalid dependency %s'
                            % (path_util.safe_join(dep.parent_uuid, dep.parent_path))
                        )

                    child_path = normpath(os.path.join(path, dep.child_path))
                    if not child_path.startswith(path):
                        raise Exception('Invalid key for dependency: %s' % (dep.child_path))

                    # If source path is on Azure Blob Storage, we should download it to a temporary local directory first.
                    if parse_linked_bundle_url(dependency_path).uses_beam:
                        dependency_path = os.path.join(tempdir, dep.parent_uuid)

                        target_info = self._download_manager.get_target_info(
                            BundleTarget(dep.parent_uuid, dep.parent_path), 0
                        )
                        target = target_info['resolved_target']

                        # Download the dependency to dependency_path (which is now in the temporary directory).
                        # TODO (Ashwin): Unify some of the logic here with the code in DependencyManager._store_dependency()
                        # into common utility functions.
                        if target_info['type'] == 'directory':
                            fileobj = self._download_manager.stream_tarred_gzipped_directory(target)
                            un_tar_directory(fileobj, dependency_path, 'gz')
                        else:
                            fileobj = self._download_manager.stream_file(target, gzipped=False)
                            with open(dependency_path, 'wb') as f:
                                shutil.copyfileobj(fileobj, f)

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
                bundle,
                {
                    'state': State.FAILED,
                    'metadata': {
                        'failure_message': str(e),
                        'error_traceback': traceback.format_exc(),
                    },
                },
            )
        finally:
            with self._make_uuids_lock:
                self._make_uuids.remove(bundle.uuid)

    def _cleanup_dead_workers(self, workers):
        """
        Clean-up workers that we haven't heard from for more than WORKER_TIMEOUT_SECONDS seconds.
        Such workers probably died without checking out properly.
        """
        for worker in workers.workers():
            if datetime.datetime.utcnow() - worker['checkin_time'] > datetime.timedelta(
                seconds=self._worker_timeout_seconds
            ):
                logger.info(
                    'Cleaning up dead worker (%s, %s)', worker['user_id'], worker['worker_id']
                )
                self._worker_model.worker_cleanup(worker['user_id'], worker['worker_id'])
                workers.remove(worker['worker_id'])

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
                logger.info(
                    'Acknowledged finalization of run bundle {} on worker {}'.format(
                        bundle.uuid, worker['worker_id']
                    )
                )
                bundle_location = self._bundle_store.get_bundle_location(bundle.uuid)
                # TODO(Ashwin): fix this -- bundle location could be linked.
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
                failure_message = 'No worker claims bundle.'
            if now - bundle.metadata.last_updated > self._worker_timeout_seconds:
                failure_message = 'Worker offline.'
            if failure_message is not None:
                logger.info('Bringing bundle offline %s: %s', bundle.uuid, failure_message)
                self._model.transition_bundle_worker_offline(bundle)

    def _schedule_run_bundles_on_workers(self, workers, staged_bundles_to_run, user_info_cache):
        """
        Schedule STAGED bundles to run on available workers based on the following logic:
        1. For a given user, schedule the highest-priority bundles first, followed by bundles
           that request to run on a specific worker.
        2. If the bundle requests to run on a specific worker, schedule the bundle
           to run on a worker that has a tag that exactly matches the bundle's request_queue.
        3. If the bundle doesn't request to run on a specific worker,
          (1) try to schedule the bundle to run on a worker that belongs to the bundle's owner
          (2) if there is no such qualified private worker, uses CodaLab-owned workers, which have user ID root_user_id.
        :param workers: a WorkerInfoAccessor object containing worker related information e.g. running uuid.
        :param staged_bundles_to_run: a list of tuples each contains a valid bundle and its bundle resources.
        :param user_info_cache: a dictionary mapping user id to user information.
        """
        # Build a dictionary which maps from user id to positions in the queue of the
        # user's staged bundles. We use this to sort bundles within each user. For example,
        # Suppose we have 4 staged bundles with the following attributes from 2 users:
        # Users: [A, B, A, B, A]
        # Bundle Priorities: [1, 2, 3, 1, 1]
        # Bundle specified request_queue: [False, False, False, False, True]
        # Original Bundle Order: [B1, B2, B3, B4, B5]
        # Sorted bundle order: [B3, B2, B5, B4, B1]
        user_queue_positions = defaultdict(list)
        for queue_position, staged_bundle in enumerate(staged_bundles_to_run):
            user_queue_positions[staged_bundle[0].owner_id].append(queue_position)

        for user, queue_positions in user_queue_positions.items():
            assert queue_positions == sorted(queue_positions)
            # Get this user's staged bundles
            user_staged_bundles = [
                staged_bundles_to_run[queue_position] for queue_position in queue_positions
            ]
            # Sort the staged bundles for this user, according to (1) their
            # priority. Larger values indicate higher priority (i.e., at the
            # start of the sorted list). Negative priority bundles should be
            # queued behind bundles with no specified priority (None priority)
            # and (2) whether it requested to run on a specific worker (bundles
            # with a specified worker have higher priority).
            sorted_user_staged_bundles = sorted(
                user_staged_bundles,
                key=lambda b: (
                    b[0].metadata.request_priority is not None
                    and b[0].metadata.request_priority >= 0,
                    b[0].metadata.request_priority is None,
                    b[0].metadata.request_priority,
                    b[0].metadata.request_queue is not None,
                ),
                reverse=True,
            )
            for queue_position, bundle in zip(queue_positions, sorted_user_staged_bundles):
                staged_bundles_to_run[queue_position] = bundle

        # Build a dictionary which maps from uuid to running bundle and bundle_resources
        running_bundles_info = self._get_running_bundles_info(workers, staged_bundles_to_run)

        # We pre-compute the workers available to each user (and the codalab-owned workers),
        # such that workers that come online or regain the necessary resources while we
        # are attempting to run each staged bundle will respect the ordering of
        # staged_bundles_to_run (i.e., they won't be used immediately, and will be instead
        # assigned bundles on the next run of _run_iteration).
        resource_deducted_user_workers = defaultdict(list)
        user_parallel_run_quota_left = {}
        for user in user_queue_positions.keys():
            # Skip for the root user as the user-owned workers will be the public CodaLab workers,
            # which are accounted for after this loop.
            if user != self._model.root_user_id:
                resource_deducted_user_workers[user] = self._deduct_worker_resources(
                    workers.get_user_workers(user), running_bundles_info
                )
            user_parallel_run_quota_left[user] = self._model.get_user_parallel_run_quota_left(
                user, user_info_cache[user]
            )
        resource_deducted_codalab_owned_workers = self._deduct_worker_resources(
            workers.get_user_workers(self._model.root_user_id), running_bundles_info
        )

        workers_list = []
        # We store a running record of the workers that go offline while we're dispatching
        # bundles, so if they come back online, we continue to ignore them in order in order to
        # respect bundle prioritization. Such workers will be assigned bundles in the BundleManager's
        # next iteration.
        offline_workers = set()
        # Dispatch bundles
        for bundle, bundle_resources in staged_bundles_to_run:
            if user_parallel_run_quota_left[bundle.owner_id] > 0:
                workers_list = (
                    resource_deducted_user_workers[bundle.owner_id]
                    + resource_deducted_codalab_owned_workers
                )
            else:
                workers_list = resource_deducted_user_workers[bundle.owner_id]
            # Although we pre-compute the available workers, workers might go offline.
            # As a result, we refresh the currently-online workers (by cleaning up the
            # dead workers), and filter out the precomputed workers that are no longer online.
            # If we don't do this, the workers might appear otherwise-eligible for runs, and we'll
            # attempt to start every bundle on every such worker. This can take a long time (if there
            # are many staged bundles, over an hour), and new bundles cannot be assigned to workers
            # in the meantime.
            self._cleanup_dead_workers(workers)
            online_worker_ids = set(
                worker["worker_id"]
                for worker in (
                    workers.get_user_workers(bundle.owner_id)
                    + workers.get_user_workers(self._model.root_user_id)
                )
            )
            # Store the worker IDs for workers that have gone offline.
            offline_workers.update(
                [
                    worker["worker_id"]
                    for worker in workers_list
                    if worker["worker_id"] not in online_worker_ids
                ]
            )
            # Filter worker that have gone offline. Note that we can't just use
            # online_worker_ids here, since we want to also exclude workers that go
            # offline, and then later come back online while we're still dispatching bundles.
            workers_list = [
                worker for worker in workers_list if worker["worker_id"] not in offline_workers
            ]

            workers_list = self._filter_and_sort_workers(workers_list, bundle, bundle_resources)
            # Try starting bundles on the workers that have enough computing resources
            for worker in workers_list:
                if self._try_start_bundle(workers, worker, bundle, bundle_resources):
                    # If we successfully started a bundle on a codalab-owned worker,
                    # decrement the parallel run quota left.
                    if worker["user_id"] == self._model.root_user_id:
                        user_parallel_run_quota_left[bundle.owner_id] -= 1
                    # Update available worker resources. This is a lower-bound,
                    # since resources released by jobs that finish are not used until
                    # the next call to _schedule_run_bundles_on_workers.
                    worker['cpus'] -= bundle_resources.cpus
                    worker['gpus'] -= bundle_resources.gpus
                    worker['memory_bytes'] -= bundle_resources.memory
                    worker['exit_after_num_runs'] -= 1
                    break

        # To avoid the potential race condition between bundle manager's dispatch frequency and
        # worker's checkin frequency, update the column "exit_after_num_runs" in worker table
        # before bundle manager's next scheduling loop
        for worker in workers_list:
            # Update workers that have "exit_after_num_runs" manually set from CLI.
            if (
                worker['exit_after_num_runs']
                < workers._workers[worker['worker_id']]['exit_after_num_runs']
            ):
                self._worker_model.update_workers(
                    worker["user_id"],
                    worker['worker_id'],
                    {'exit_after_num_runs': worker['exit_after_num_runs']},
                )

    def _deduct_worker_resources(self, workers_list, running_bundles_info):
        """
        From each worker, subtract resources used by running bundles.
        """
        workers_list = copy.deepcopy(workers_list)
        for worker in workers_list:
            for uuid in worker['run_uuids']:
                # Verify if the current bundle exists in both the worker table and the bundle table
                if uuid in running_bundles_info:
                    bundle_resources = running_bundles_info[uuid]["bundle_resources"]
                else:
                    try:
                        bundle = self._model.get_bundle(uuid)
                        bundle_resources = self._compute_bundle_resources(bundle)
                    except NotFoundError:
                        logger.info(
                            'Bundle {} exists on worker {} but no longer found in the bundle table. '
                            'Skipping for resource deduction.'.format(uuid, worker['worker_id'])
                        )
                        continue
                worker['cpus'] -= bundle_resources.cpus
                worker['gpus'] -= bundle_resources.gpus
                worker['memory_bytes'] -= bundle_resources.memory
        return workers_list

    @staticmethod
    def _worker_to_run_resources(worker):
        """
        :param worker: dict

        Converts a worker dict into a RunResources instance.
        """
        return RunResources(
            tag=worker['tag'],
            tag_exclusive=worker['tag_exclusive'],
            cpus=worker['cpus'],
            gpus=worker['gpus'],
            memory=worker['memory_bytes'],
            disk=worker['free_disk_bytes'],
            runs_left=worker['exit_after_num_runs'],
            docker_image=None,
            time=None,
            network=None,
        )

    def _get_dominating_workers(self, run_resources, workers_list, strict=False):
        """
        :param self: BundleManager
        :param run_resources: RunResources
        :param workers_list: list of worker dicts
        :param strict: bool that determines if domination should be strict

        Returns a list of worker dicts comprised of workers that can meet the
        resource requirements specified in run_resources.
        """
        dominating_workers = []
        for worker in workers_list:
            worker_resources = self._worker_to_run_resources(worker)
            if worker_resources.dominates(run_resources, strict):
                dominating_workers.append(worker)
        return dominating_workers

    def _get_resource_recommendations(self, run_resources, workers_list):
        """
        :param self: BundleManager
        :param run_resources: RunResources
        :param workers_list: list of worker dicts

        Returns a string containing bundle resource recommendations based on
        the workers in workers_list.
        """
        recommendations = []
        for worker in workers_list:
            worker_resources = self._worker_to_run_resources(worker)
            dominating_workers = self._get_dominating_workers(worker_resources, workers_list, True)

            # Only recommend this worker if no other worker strictly dominates it.
            if not dominating_workers:
                comparison = worker_resources.get_comparison(run_resources)
                recommendations.append(comparison)

        if len(recommendations) != 0:
            return f"Available resources: {', '.join(recommendations)}"
        return ''

    def _filter_and_sort_workers(self, workers_list, bundle, bundle_resources):
        """
        :param self: BundleManager
        :param workers_list: list of worker dicts
        :param bundle: dict
        :param bundle_resources: RunResources

        Filters the workers to those that can run the given bundle and returns
        the list sorted in order of preference for running the bundle.
        """
        # Get a list of workers that can meet the bundle's resource requirements.
        dominating_workers = self._get_dominating_workers(bundle_resources, workers_list)

        # If no workers can meet the bundle's resource reqs, add resource recommendations to staged_status.
        if not dominating_workers:
            recommendations = self._get_resource_recommendations(bundle_resources, workers_list)
            staged_status = (
                f"No worker can meet your bundle's resource requirements. {recommendations}"
            )
            self._set_staged_status(bundle, staged_status)
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

            # Subject to the worker meeting the resource requirements of the bundle, we also want to:
            # 1. prioritize workers that are tag-exclusive.
            # 2. prioritize workers with fewer GPUs (including zero).
            # 3. prioritize workers that have more bundle dependencies.
            # 4. prioritize workers with fewer CPUs.
            # 5. prioritize workers with fewer running jobs.
            # 6. break ties randomly by a random seed.
            return (
                not worker['tag_exclusive'],
                worker['gpus'] or worker['has_gpus'],
                -num_available_deps,
                worker['cpus'],
                len(worker['run_uuids']),
                random.random(),
            )

        dominating_workers.sort(key=get_sort_key)

        return dominating_workers

    def _try_start_bundle(self, workers, worker, bundle, bundle_resources):
        """
        Tries to start running the bundle on the given worker, returning False
        if that failed.
        """
        if not check_bundle_have_run_permission(
            self._model, self._model.get_user(worker['user_id']), bundle
        ) or not self._model.transition_bundle_starting(
            bundle, worker['user_id'], worker['worker_id']
        ):
            return False

        workers.set_starting(bundle.uuid, worker['worker_id'])
        if worker['shared_file_system']:
            # On a shared file system we create the path here to avoid NFS
            # directory cache issues.
            # TODO(Ashwin): fix for --link
            path = self._bundle_store.get_bundle_location(bundle.uuid)
            remove_path(path)
            os.mkdir(path)
        if self._worker_model.send_json_message(
            worker['socket_id'],
            self._construct_run_message(worker['shared_file_system'], bundle, bundle_resources),
            0.2,
        ):
            logger.info(
                'Starting run bundle {} on worker {}'.format(bundle.uuid, worker['worker_id'])
            )
            return True
        else:
            self._model.transition_bundle_staged(bundle)
            workers.restage(bundle.uuid)
            return False

    @staticmethod
    def _compute_request_cpus(bundle):
        """
        Compute the CPU limit used for scheduling the run.
        The default of 1 (if no GPUs specified)
        is for backwards compatibility for
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
        The default of 2g is for backwards compatibility for
        runs from before when we added client-side defaults
        """
        if not bundle.metadata.request_memory:
            return formatting.parse_size('2g')
        return formatting.parse_size(bundle.metadata.request_memory)

    def _compute_request_disk(self, bundle, user_info=None):
        """
        Compute the disk limit used for scheduling the run.
        The default is min(disk quota the user has left, global max)
        """
        if not bundle.metadata.request_disk:
            return min(
                self._model.get_user_disk_quota_left(bundle.owner_id, user_info) - 1,
                self._max_request_disk,
            )
        return formatting.parse_size(bundle.metadata.request_disk)

    def _compute_request_time(self, bundle, user_info=None):
        """
        Compute the time limit used for scheduling the run.
        The default is min(time quota the user has left, global max)
        """
        if not bundle.metadata.request_time:
            return min(
                self._model.get_user_time_quota_left(bundle.owner_id, user_info) - 1,
                self._max_request_time,
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

    def _construct_run_message(self, shared_file_system, bundle, bundle_resources):
        """
        Constructs the run message that is sent to the given worker to tell it
        to run the given bundle.
        """
        message = {}
        message['type'] = 'run'
        message['bundle'] = bundle_util.bundle_to_bundle_info(self._model, bundle)
        if shared_file_system:
            bundle_link_url = getattr(bundle.metadata, "link_url", None)
            message['bundle'][
                'location'
            ] = bundle_link_url or self._bundle_store.get_bundle_location(bundle.uuid)
            parent_bundle_link_urls = self._model.get_bundle_metadata(
                [dep['parent_uuid'] for dep in message['bundle']['dependencies']], "link_url"
            )
            for dependency in message['bundle']['dependencies']:
                parent_bundle_link_url = parent_bundle_link_urls.get(dependency['parent_uuid'])
                dependency['location'] = (
                    parent_bundle_link_url
                    or self._bundle_store.get_bundle_location(dependency['parent_uuid'])
                )

        # Figure out the resource requirements.
        message['resources'] = bundle_resources.as_dict
        return message

    def _compute_bundle_resources(self, bundle, user_info=None):
        return RunResources(
            cpus=self._compute_request_cpus(bundle),
            gpus=self._compute_request_gpus(bundle),
            docker_image=self._get_docker_image(bundle),
            # _compute_request_time contains database queries that may reduce efficiency
            time=self._compute_request_time(bundle, user_info),
            memory=self._compute_request_memory(bundle),
            # _compute_request_disk contains database queries that may reduce efficiency
            disk=self._compute_request_disk(bundle, user_info),
            network=bundle.metadata.request_network,
            tag=bundle.metadata.request_queue,
            tag_exclusive=False,
            runs_left=None,
        )

    def _fail_unresponsive_bundles(self):
        """
        Fail bundles in uploading, staged and running state if we haven't heard from them for more than
        BUNDLE_TIMEOUT_DAYS days.
        """
        bundles_to_fail = self._model.batch_get_bundles(
            state=[State.UPLOADING, State.STAGED, State.RUNNING]
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
        workers = WorkerInfoAccessor(
            self._model, self._worker_model, self._worker_timeout_seconds - 5
        )

        # Handle some exceptional cases.
        self._cleanup_dead_workers(workers)
        self._restage_stuck_starting_bundles(workers)
        self._bring_offline_stuck_running_bundles(workers)
        self._acknowledge_recently_finished_bundles(workers)
        # A dictionary structured as {user id : user information} to track those visited user information
        user_info_cache = {}
        staged_bundles_to_run = self._get_staged_bundles_to_run(workers, user_info_cache)

        # Schedule, preferring user-owned workers.
        self._schedule_run_bundles_on_workers(workers, staged_bundles_to_run, user_info_cache)

    @staticmethod
    def _check_resource_failure(
        value,
        user_fail_string=None,
        global_fail_string=None,
        user_max=None,
        global_max=None,
        global_min=None,
        pretty_print=lambda x: str(x),
    ):
        """
        Returns a failure message in case a certain resource limit is not respected.
        If value > user_max, user_fail_string is formatted with value and user_max in that order
        If value > global_max, global_fail_string is formatted with value and global_max in that order
        If value < global_min, global_fail_string is formatted with value and global_min in that order
        Pretty print is applied to both the value and max values before they're passed on to the functions
        The strings should expect string inputs for formatting and pretty_print should convert values to strings
        """
        if value:
            if user_max and value > user_max:
                return user_fail_string % (
                    pretty_print(value),
                    pretty_print(user_max),
                    pretty_print(value - user_max),
                )
            elif global_max and value > global_max:
                return global_fail_string % (
                    pretty_print(value),
                    pretty_print(global_max),
                    pretty_print(value - global_max),
                )
            elif global_min and value < global_min:
                return global_fail_string % (
                    pretty_print(value),
                    pretty_print(global_min),
                    pretty_print(global_min - value),
                )
        return None

    def _get_staged_bundles_to_run(self, workers, user_info_cache):
        """
        Fails bundles that request more resources than available for the given user.
        Note: allow more resources than available on any worker because new
        workers might get spun up in response to the presence of this run.
        :param workers: a WorkerInfoAccessor object containing worker related information e.g. running uuid.
        :param user_info_cache: a dictionary mapping user id to user information.
        :return: a list of tuple which contains valid staged bundles and their bundle_resources.
        """
        # Keep track of staged bundles that have valid resources requested
        staged_bundles_to_run = []

        for bundle in self._model.batch_get_bundles(state=State.STAGED, bundle_type='run'):
            # Cache those visited user information
            if bundle.owner_id in user_info_cache:
                user_info = user_info_cache[bundle.owner_id]
            else:
                user_info = self._model.get_user_info(bundle.owner_id)
                user_info_cache[bundle.owner_id] = user_info

            bundle_resources = self._compute_bundle_resources(bundle, user_info)

            failures = []
            failures.append(
                self._check_resource_failure(
                    bundle_resources.disk,
                    user_fail_string='Requested more disk (%s) than user disk quota left (%s) by %s',
                    # The default max disk quota that can be requested is disk quota left - DISK_QUOTA_SLACK_BYTES.
                    user_max=self._model.get_user_disk_quota_left(bundle.owner_id, user_info)
                    - DISK_QUOTA_SLACK_BYTES,
                    global_fail_string='Maximum job disk size (%s) exceeded (%s)',
                    global_max=self._max_request_disk,
                    pretty_print=formatting.size_str,
                )
            )

            failures.append(
                self._check_resource_failure(
                    bundle_resources.time,
                    user_fail_string='Requested more time (%s) than user time quota left (%s) by %s',
                    user_max=self._model.get_user_time_quota_left(bundle.owner_id, user_info),
                    global_fail_string='Maximum job time (%s) exceeded (%s)',
                    global_max=self._max_request_time,
                    pretty_print=formatting.duration_str,
                )
            )

            failures.append(
                self._check_resource_failure(
                    bundle_resources.memory,
                    global_fail_string='Requested more memory (%s) than maximum limit (%s) by %s',
                    global_max=self._max_request_memory,
                    pretty_print=formatting.size_str,
                )
            )
            # Filter out all the bundles that have requested memory less than 4m which is the
            # minimum amount of memory to start a Docker container
            failures.append(
                self._check_resource_failure(
                    bundle_resources.memory,
                    global_fail_string='Requested less memory (%s) than minimum limit (%s) by %s',
                    global_min=self._min_request_memory,
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
            else:
                staged_bundles_to_run.append((bundle, bundle_resources))

        return staged_bundles_to_run

    def _get_running_bundles_info(self, workers, staged_bundles_to_run):
        """
        Build a nested dictionary to store information (bundle and bundle_resources) including
        the current running bundles and staged bundles.
        Note that the primary usage of this function is to improve efficiency when calling
        self._compute_bundle_resources(), e.g. reusing constants (gpus, cpus, memory) from
        the returning values of self._compute_bundle_resources() as they don't change over time.
        However, be careful when using this function to improve efficiency for returning values
        like disk and time from self._compute_bundle_resources() as they do depend on the number
        of jobs that are running during the time of computation. Accuracy might be affected
        without considering this factor.
        :param workers: a WorkerInfoAccessor object containing worker related information e.g. running uuid.
        :return: a nested dictionary structured as follows:
                {
                    uuid: {
                        "bundle": bundle,
                        "bundle_resources": bundle_resources
                    }
                }
        """
        # Get uuid of all the running bundles from workers (a WorkerInfoAccessor object)
        run_uuids = list(workers._uuid_to_worker.keys())
        staged_bundles_to_run_dict = {
            bundle.uuid: bundle_resources for (bundle, bundle_resources) in staged_bundles_to_run
        }
        # Get uuid of all the staged bundles that will be dispatched on workers
        staged_uuids = list(staged_bundles_to_run_dict.keys())

        # Get the running bundles (including the potential running bundles: staged bundles) that exist
        # in the bundle table as well. Including staged bundles here is to avoid overestimating worker resources in
        # the function self._deduct_worker_resources(). We could potentially be conservative on dispatching jobs to
        # workers, but this is still better than over assigning jobs.
        running_bundles = self._model.batch_get_bundles(uuid=run_uuids + staged_uuids)
        # Build a dictionary which maps from uuid to running bundle and bundle_resources
        running_bundles_info = {
            bundle.uuid: {
                "bundle": bundle,
                "bundle_resources": self._compute_bundle_resources(bundle)
                if bundle.uuid in run_uuids
                else staged_bundles_to_run_dict[bundle.uuid],
            }
            for bundle in running_bundles
        }

        return running_bundles_info
