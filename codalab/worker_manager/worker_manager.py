import logging
import time
from codalab.lib.codalab_manager import CodaLabManager
from codalab.worker.bundle_state import State
from collections import namedtuple

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

    def run_loop(self):
        while True:
            self.run_one_iteration()
            if self.args.once:
                break
            logger.debug('Sleeping {} seconds'.format(self.args.sleep_time))
            time.sleep(self.args.sleep_time)

    def run_one_iteration(self):
        # Get staged bundles
        keywords = ['state=' + State.STAGED] + self.args.search
        if self.args.worker_tag:
            keywords.append('request_queue=tag=' + self.args.worker_tag)
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
                    'Don\'t launch becaused waited {} < {} seconds since last worker'.format(
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
