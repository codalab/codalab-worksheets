import logging
import time
from codalab.lib.codalab_manager import CodaLabManager
from codalab.worker.bundle_state import State
from collections import namedtuple

logger = logging.getLogger(__name__)

# Represents a AWS/Azure job that runs a single cl-worker.
# For now, we don't need to keep any information.
WorkerJob = namedtuple('WorkerJob', [])


class WorkerManager(object):
    """
    The abstract class for a worker manager.  Different backends like AWS,
    Azure, Slurm should override `get_worker_jobs` and `start_worker_job`.

    The basic architecture of the WorkerManager is extremely simple: to a
    first-order approximation, it simply launches `cl-worker`s as AWS/Azure
    batch jobs as long as there staged bundles.

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
    - The worker manager is not visible via CodaLab.  One needs to monitor the
      AWS/Azure Batch system separately.
    - Resource handling is not currently supported.  Generally, the safe thing
      is to create a separate queue for different resource needs and put the
      burden of deciding on the user.
    """

    def __init__(self, args):
        self.args = args
        self.codalab_manager = CodaLabManager()
        self.codalab_client = self.codalab_manager.client(args.server)
        self.staged_uuids = []
        self.wait_for_progress = False  # Started a worker, waiting for something to happen
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
        if len(removed_uuids) > 0:
            self.wait_for_progress = False
        self.staged_uuids = new_staged_uuids
        logger.info(
            'Staged bundles [{}]: {}'.format(
                ' '.join(keywords), ' '.join(new_staged_uuids) or '(none)'
            )
        )

        # Get worker jobs
        worker_jobs = self.get_worker_jobs()

        # Print status
        logger.info(
            '{} staged bundles ({} removed since last time{}), {} worker jobs (min={}, max={})'.format(
                len(new_staged_uuids),
                len(removed_uuids),
                ', waiting for >0 before launching worker' if self.wait_for_progress else '',
                len(worker_jobs),
                self.args.min_workers,
                self.args.max_workers,
            )
        )

        # There are two conditions under which we want more workers.
        want_more_workers = False

        # 1) We have fewer than min_workers.
        if len(worker_jobs) < self.args.min_workers:
            logger.info(
                'Want to launch a worker because we are under the minimum ({} < {})'.format(
                    len(worker_jobs), self.args.min_workers
                )
            )
            want_more_workers = True

        # 2) There is a staged bundle AND we have made progress running the
        # previously staged bundles.
        # To expand on this, if we just started a worker and we haven't made any
        # progress towards removing bundles from staged, don't launch another
        # one yet.  This is so that if someone starts a run that requires
        # massive resources that we can't handle, we don't keep on trying to
        # launch workers to no avail.  Of course, there are many reasons that a
        # bundle might be removed from staged (user might have deleted the
        # bundle).  Also, the bundle that was removed might not be the one that
        # we were originally intending to run.
        if len(new_staged_uuids) > 0:
            logger.info(
                'Want to launch a worker because we have {} > 0 staged bundles'.format(
                    len(new_staged_uuids)
                )
            )
            if self.wait_for_progress and len(removed_uuids) == 0:
                logger.info(
                    'But we already launched a worker and waiting for some progress first (previous bundles to be removed from staged)'
                )
                return
            want_more_workers = True

        if want_more_workers:
            # Make sure we don't launch workers too quickly.
            seconds_since_last_worker = int(time.time() - self.last_worker_start_time)
            if seconds_since_last_worker < self.args.min_seconds_between_workers:
                logger.info(
                    'Don\'t launch becaused waited {} < {} seconds since last worker'.format(
                        seconds_since_last_worker, self.args.min_seconds_between_workers
                    )
                )
                return

            # Don't launch more than `max_workers`.
            # For now, only the number of workers is used to determine what workers
            # we launch.
            if len(worker_jobs) >= self.args.max_workers:
                logger.info(
                    'Don\'t launch because too many workers already ({} >= {})'.format(
                        len(worker_jobs), self.args.max_workers
                    )
                )
                return

            logger.info('Starting a worker!')
            self.start_worker_job()
            self.wait_for_progress = True  # Need to wait for progress to happen
            self.last_worker_start_time = time.time()
