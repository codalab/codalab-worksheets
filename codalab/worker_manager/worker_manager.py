import argparse
import logging
import time
import getpass
from codalab.lib.codalab_manager import CodaLabManager
from codalabworker.bundle_state import State

logger = logging.getLogger(__name__)


class WorkerManager(object):
    """
    The abstract class for a worker manager.  Different backends like AWS,
    Azure, Slurm should override `get_workers` and `start_worker`.
    """

    def __init__(self, args):
        self.args = args
        self.codalab_manager = CodaLabManager()
        self.codalab_client = self.codalab_manager.client(args.server)
        self.staged_uuids = []
        self.pending_worker = False  # Started a worker, waiting for something to happen
        logger.info('Started worker manager.')

    def get_workers(self):
        """Return a list of workers."""
        raise NotImplementedError

    def start_worker(self):
        """Start a new worker."""
        raise NotImplementedError

    def run_loop(self):
        while True:
            self.run_one_iteration()
            break  # TMP
            time.sleep(self.args.sleep_time)

    def run_one_iteration(self):
        # Get staged bundles
        keywords = ['state=' + State.STAGED] + self.args.search
        # if self.args.worker_tag:
        # keywords.append('request_queue=tag=' + self.args.worker_tag)
        bundles = self.codalab_client.fetch(
            'bundles', params={'worksheet': None, 'keywords': keywords, 'include': ['owner']}
        )
        new_staged_uuids = [bundle['uuid'] for bundle in bundles]
        old_staged_uuids = self.staged_uuids
        # Bundles that were staged but now aren't
        removed_uuids = [uuid for uuid in old_staged_uuids if uuid not in new_staged_uuids]
        self.staged_uuids = new_staged_uuids
        logger.debug('Bundles: {}'.format(' '.join(new_staged_uuids)))

        # Get workers
        workers = self.get_workers()
        logger.debug(
            '{} staged bundles ({} removed from last time), {} workers'.format(
                len(new_staged_uuids), len(removed_uuids), len(workers)
            )
        )

        # If we just started a worker and we haven't made any progress towards
        # removing bundles from staged, don't launch another one.  This is so
        # that if someone starts a run that requires massive resources that we
        # can't handle, we don't keep on trying to launch workers to no avail.
        # Of course, there are many reasons that a bundle might be removed from
        # staged (user might have deleted).  And the bundle that was removed
        # might not be the one that we were originally intending to run.
        if self.pending_worker and len(removed_uuids) == 0:
            return
        self.pending_worker = False

        # Don't launch more than `max_workers`.
        if len(workers) >= self.args.max_workers:
            return

        # Are there staged bundles to run?
        if len(new_staged_uuids) == 0:
            return

        # Start a worker.
        logger.debug(
            'Not enough workers ({} staged), starting a worker'.format(len(new_staged_uuids))
        )
        self.start_worker()
        self.pending_worker = True
