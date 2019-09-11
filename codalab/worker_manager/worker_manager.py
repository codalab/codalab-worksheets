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
        #if self.args.worker_tag:
            #keywords.append('request_queue=tag=' + self.args.worker_tag)
        bundles = self.codalab_client.fetch(
            'bundles', params={'worksheet': None, 'keywords': keywords, 'include': ['owner']}
        )
        logger.debug('Bundles: {}'.format(' '.join(bundle['uuid'] for bundle in bundles)))

        # Get workers
        workers = self.get_workers()

        logger.debug('{} staged bundles, {} workers'.format(len(bundles), len(workers)))

        # Don't launch more than `max_workers`.
        can_launch_workers = len(workers) < self.args.max_workers

        # Logic: keep 1 more than the number of staged runs.
        extra_workers = 1
        need_more_workers = len(workers) < len(bundles) + extra_workers

        if can_launch_workers and need_more_workers:
            logger.debug('Not enough workers ({} < {} + {}), starting a worker'.format(len(workers), len(bundles), extra_workers))
            self.start_worker()
