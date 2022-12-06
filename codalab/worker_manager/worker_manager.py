import http
import logging
import os
import psutil
import socket
import sys
import time
import traceback
import urllib
from argparse import ArgumentParser
from collections import namedtuple
from typing import Dict, List, Union

from codalab.common import NotFoundError, LoginPermissionError
from codalab.client.json_api_client import JsonApiException
from codalab.lib.codalab_manager import CodaLabManager
from codalab.lib.formatting import parse_size
from codalab.worker.bundle_state import State

logger = logging.getLogger(__name__)

# Type aliases
BundlesPayload = List[Dict[str, Dict[str, Union[int, str]]]]

# Represents a AWS/Azure job that runs a single cl-worker.
# `active` is a Boolean field that's set to true if the worker is
# actively running at the moment. (As opposed to being staged, queued, preparing etc)
WorkerJob = namedtuple('WorkerJob', ['active'])


def restart():
    """
    Restarts the current program, cleaning up file objects and descriptors
    """
    try:
        p = psutil.Process(os.getpid())
        for handler in p.open_files() + p.connections():
            os.close(handler.fd)
    except Exception as e:
        logger.error(e)
    python = sys.executable
    os.execl(python, python, *sys.argv)


class WorkerManager(object):
    """
    The abstract class for a worker manager.  Different backends like AWS,
    Azure, Slurm should override `get_worker_jobs` and `start_worker_job`.

    The basic architecture of the WorkerManager is extremely simple: to a
    first-order approximation, it simply launches `cl-worker`s as AWS/Azure
    batch jobs as long as there are staged bundles.

    The simplicity means that we don't need to manage state about how workers
    and bundles are related - that logic is complex and is done by the usual
    worker system, and we don't want to duplicate that for every possible
    worker backend.

    More specifically, a worker manager will monitor a job queue to see how
    many worker jobs are running, and try to keep that between `min_workers`
    and `max_workers`.  It will also monitor the staged bundles that satisfy a
    certain `search` criterion.  If there are staged bundles then it will issue
    a `start_worker_job()` call, provided some other conditions are met (e.g.,
    don't start workers too fast).

    The WorkerManager is all client-side code, so it can be customized as one
    sees fit.

    Notes:
    - The worker manager is not visible via CodaLab (i.e., CodaLab has no
      notion of a worker manager or what it's trying to do - all it sees is
      bundles and workers).  One needs to monitor the AWS/Azure Batch system
      separately.
    - Resource handling is not currently supported.  Generally, the safe thing
      is to create a separate queue for different resource needs and put the
      burden of deciding on the user.
    """

    # Subcommand name to use for this worker manager type
    NAME: str = 'worker-manager'
    DESCRIPTION: str = 'Base class for Worker Managers, please implement for your deployment'

    @staticmethod
    def add_arguments_to_subparser(subparser: ArgumentParser) -> None:
        """
        Add any arguments specific to this worker manager to the given subparser
        """
        raise NotImplementedError

    def __init__(self, args):
        self.args = args
        self.codalab_manager = CodaLabManager(temporary=args.temp_session)
        self.codalab_client = self.codalab_manager.client(args.server)
        self.staged_uuids = []
        self.worker_manager_start_time = time.time()
        self.last_worker_start_time = 0
        logger.info('Started worker manager.')

    def get_worker_jobs(self):
        """Return a list of `WorkerJob`s."""
        raise NotImplementedError

    def start_worker_job(self):
        """Start a new `WorkerJob`."""
        raise NotImplementedError

    def build_command(self, worker_id: str, work_dir: str) -> List[str]:
        command: List[str] = [
            self.args.worker_executable,
            '--server',
            self.args.server,
            '--verbose',
            '--exit-when-idle',
            '--idle-seconds',
            str(self.args.worker_idle_seconds),
            '--work-dir',
            work_dir,
            '--id',
            f'$(hostname -s)-{worker_id}',
            '--network-prefix',
            'cl_worker_{}_network'.format(worker_id),
        ]

        # Additional optional arguments
        if self.args.worker_tag:
            command.extend(['--tag', self.args.worker_tag])
        if self.args.worker_group:
            command.extend(['--group', self.args.worker_group])
        if self.args.worker_exit_after_num_runs and self.args.worker_exit_after_num_runs > 0:
            command.extend(['--exit-after-num-runs', str(self.args.worker_exit_after_num_runs)])
        if self.args.worker_download_dependencies_max_retries:
            command.extend(
                [
                    '--download-dependencies-max-retries',
                    str(self.args.worker_download_dependencies_max_retries),
                ]
            )
        if self.args.worker_max_work_dir_size:
            command.extend(['--max-work-dir-size', self.args.worker_max_work_dir_size])
        if self.args.worker_delete_work_dir_on_exit:
            command.extend(['--delete-work-dir-on-exit'])
        if self.args.worker_exit_on_exception:
            command.extend(['--exit-on-exception'])
        if self.args.worker_tag_exclusive:
            command.extend(['--tag-exclusive'])
        if self.args.worker_pass_down_termination:
            command.extend(['--pass-down-termination'])
        if self.args.worker_checkin_frequency_seconds:
            command.extend(
                ['--checkin-frequency-seconds', str(self.args.worker_checkin_frequency_seconds)]
            )
        if self.args.worker_shared_memory_size_gb:
            command.extend(['--shared-memory-size-gb', str(self.args.worker_shared_memory_size_gb)])
        if self.args.worker_preemptible:
            command.extend(['--preemptible'])

        return command

    def run_loop(self):
        while True:
            try:
                self.run_one_iteration()
            except (
                urllib.error.URLError,
                http.client.HTTPException,
                socket.error,
                JsonApiException,
                NotFoundError,
            ):
                # Sometimes, network errors occur when running the WorkerManager . These are often
                # transient exceptions, and retrying the command would lead to success---as a result,
                # we ignore these network-based exceptions (rather than fatally exiting from the
                # WorkerManager )
                traceback.print_exc()
            except LoginPermissionError:
                print("Invalid username or password. Please try again:")
                break
            logger.debug('Sleeping {} seconds'.format(self.args.sleep_time))
            time.sleep(self.args.sleep_time)

    def run_one_iteration(self):
        if self.args.restart_after_seconds:
            seconds_since_start = int(time.time() - self.worker_manager_start_time)
            if seconds_since_start > self.args.restart_after_seconds:
                logger.info(
                    f"{seconds_since_start} seconds have elapsed since this WorkerManager "
                    f"was started, which is greater than {self.args.restart_after_seconds}"
                )
                logger.info("Restarting...")
                restart()
        # Get staged bundles for the current user. The principle here is that we want to get all of
        # the staged bundles can be run by this user.
        keywords = ['state=' + State.STAGED] + self.args.search
        # If the current user is "codalab", don't filter by .mine because the workers owned
        # by "codalab" can be shared by all users. But, for all other users, we only
        # want to see their staged bundles.
        if os.environ.get('CODALAB_USERNAME') != "codalab":
            keywords += [".mine"]
        # The keywords below search for `request_queue=<worker tag>` OR `request_queue=tag=<worker tag>`
        # If support for this is removed so that 'request_queue' is always set to be '<worker tag>'
        # (and not tag=<worker tag>) this search query can again be simplified.
        # NOTE: server/bundle_manager.py has the server-side matching logic that should be synced
        # with this search request.
        if self.args.worker_tag_exclusive and self.args.worker_tag:
            keywords += ["request_queue=%s,tag=%s" % (self.args.worker_tag, self.args.worker_tag)]

        bundles: BundlesPayload = self.codalab_client.fetch(
            'bundles', params={'worksheet': None, 'keywords': keywords, 'include': ['owner']}
        )
        # Unless no_prefilter is set, filter out otherwise-eligible run bundles that request more
        # resources than this WorkerManager's workers have.
        if not self.args.no_prefilter:
            bundles = self.filter_bundles(bundles)

        new_staged_uuids = [bundle['uuid'] for bundle in bundles]
        old_staged_uuids = self.staged_uuids
        # Bundles that were staged but now aren't
        removed_uuids = [uuid for uuid in old_staged_uuids if uuid not in new_staged_uuids]
        self.staged_uuids = new_staged_uuids
        logger.info(
            'Staged bundles [{}]: {}'.format(
                ' '.join(keywords), ' '.join(self.staged_uuids) or '(none)'
            )
        )

        # Get worker jobs
        worker_jobs = self.get_worker_jobs()
        pending_worker_jobs, active_worker_jobs = [], []

        for job in worker_jobs:
            (active_worker_jobs if job.active else pending_worker_jobs).append(job)

        # Print status
        logger.info(
            '{} staged bundles ({} removed since last time), {} worker jobs (min={}, max={}) ({} active, {} pending)'.format(
                len(self.staged_uuids),
                len(removed_uuids),
                len(worker_jobs),
                self.args.min_workers,
                self.args.max_workers,
                len(active_worker_jobs),
                len(pending_worker_jobs),
            )
        )

        want_workers = False

        # There is a staged bundle AND there aren't any workers that are still booting up/starting
        if len(self.staged_uuids) > 0:
            logger.info(
                'Want to launch a worker because we have {} > 0 staged bundles'.format(
                    len(self.staged_uuids)
                )
            )
            want_workers = True

        if want_workers:
            # Make sure we don't launch workers too quickly.
            seconds_since_last_worker = int(time.time() - self.last_worker_start_time)
            if seconds_since_last_worker < self.args.min_seconds_between_workers:
                logger.info(
                    'Don\'t launch because waited {} < {} seconds since last worker'.format(
                        seconds_since_last_worker, self.args.min_seconds_between_workers
                    )
                )
                want_workers = False

            # Make sure we don't queue up more workers than staged UUIDs if there are
            # more workers still booting up than staged bundles
            if len(pending_worker_jobs) >= len(self.staged_uuids):
                logger.info(
                    'Don\'t launch because still more pending workers than staged bundles ({} >= {})'.format(
                        len(pending_worker_jobs), len(self.staged_uuids)
                    )
                )
                want_workers = False

            # Don't launch more than `max_workers`.
            # For now, only the number of workers is used to determine what workers
            # we launch.
            if len(worker_jobs) >= self.args.max_workers:
                logger.info(
                    'Don\'t launch because too many workers already ({} >= {})'.format(
                        len(worker_jobs), self.args.max_workers
                    )
                )
                want_workers = False

        # We have fewer than min_workers, so launch one regardless of other constraints
        if len(worker_jobs) < self.args.min_workers:
            logger.info(
                'Launch a worker because we are under the minimum ({} < {})'.format(
                    len(worker_jobs), self.args.min_workers
                )
            )
            want_workers = True

        if want_workers:
            logger.info('Starting a worker!')
            self.start_worker_job()
            self.last_worker_start_time = time.time()

    def filter_bundles(self, bundles: BundlesPayload) -> BundlesPayload:
        filtered_bundles: BundlesPayload = []
        worker_memory_bytes: int = parse_size('{}m'.format(self.args.memory_mb))
        logger.info(
            f"Current worker manager allocates {self.args.cpus} CPUs, {self.args.gpus} GPUs, "
            f"and {worker_memory_bytes} bytes of RAM"
        )
        for bundle in bundles:
            # Filter bundles based on the resources specified when creating the worker manager
            if bundle['metadata']['request_cpus'] > self.args.cpus:
                logger.info(
                    'Filtered out bundle {} based on unfulfillable resource requested: request_cpus={}'.format(
                        bundle['uuid'], bundle['metadata']['request_cpus'],
                    )
                )
            elif bundle['metadata']['request_gpus'] > self.args.gpus:
                logger.info(
                    'Filtered out bundle {} based on unfulfillable resource requested: request_gpus={}'.format(
                        bundle['uuid'], bundle['metadata']['request_gpus'],
                    )
                )
            elif parse_size(bundle['metadata']['request_memory']) > worker_memory_bytes:
                logger.info(
                    'Filtered out bundle {} based on unfulfillable resource requested: request_memory={}'.format(
                        bundle['uuid'], bundle['metadata']['request_memory'],
                    )
                )
            else:
                filtered_bundles.append(bundle)

        return filtered_bundles
