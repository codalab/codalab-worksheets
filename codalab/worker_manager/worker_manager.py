from collections import namedtuple
import http
import logging
import os
import socket
import time
import traceback
import urllib

from codalab.common import NotFoundError
from codalab.client.json_api_client import JsonApiException
from codalab.lib.codalab_manager import CodaLabManager
from codalab.worker.bundle_state import State


logger = logging.getLogger(__name__)

# Represents a AWS/Azure job that runs a single cl-worker.
# `active` is a Boolean field that's set to true if the worker is
# actively running at the moment. (As opposed to being staged, queued, preparing etc)
WorkerJob = namedtuple('WorkerJob', ['active'])


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
    NAME = 'worker-manager'
    DESCRIPTION = 'Base class for Worker Managers, please implement for your deployment'

    @staticmethod
    def add_arguments_to_subparser(subparser):
        """
        Add any arguments specific to this worker manager to the given subparser
        """
        raise NotImplementedError

    def __init__(self, args):
        self.args = args
        self.codalab_manager = CodaLabManager()
        self.codalab_client = self.codalab_manager.client(args.server)
        self.staged_uuids = []
        self.last_worker_start_time = 0
        logger.info('Started worker manager.')

    def get_worker_jobs(self):
        """Return a list of `WorkerJob`s."""
        raise NotImplementedError

    def start_worker_job(self):
        """Start a new `WorkerJob`."""
        raise NotImplementedError

    def build_command(self, worker_id, work_dir):
        command = [
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

        return command

    def run_loop(self):
        while True:
            try:
                self.run_one_iteration()
            except (
                urllib.error.URLError,
                http.client.HTTPException,
                socket.error,
                NotFoundError,
                JsonApiException,
            ):
                # Sometimes, network errors occur when running the WorkerManager . These are often
                # transient exceptions, and retrying the command would lead to success---as a result,
                # we ignore these network-based exceptions (rather than fatally exiting from the
                # WorkerManager )
                traceback.print_exc()
            if self.args.once:
                break
            logger.debug('Sleeping {} seconds'.format(self.args.sleep_time))
            time.sleep(self.args.sleep_time)

    def run_one_iteration(self):
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

        bundles = self.codalab_client.fetch(
            'bundles', params={'worksheet': None, 'keywords': keywords, 'include': ['owner']}
        )
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
