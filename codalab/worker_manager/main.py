import argparse
import logging
import time
import getpass
from codalab.lib.codalab_manager import CodaLabManager
from codalabworker.bundle_state import State
from .aws_worker_manager import AWSWorkerManager


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--server', help='CodaLab instance to connect to', default='https://worksheets.codalab.org'
    )
    parser.add_argument('-t', '--worker-manager-type', help='Type of worker manager', required=True)
    parser.add_argument('--max-workers', help='Maximum number of workers', type=int, default=10)
    # The worker manager has only crude knowledge about what the queues are
    # doing (CPU versus GPU).  For finer-grained control, the user should
    # request specific queues with --request-queue=...
    parser.add_argument('--queue', help='Monitor and submit to this queue')
    parser.add_argument(
        '--search', nargs='*', help='Monitor only runs that satisfy these criteria', default=[]
    )
    parser.add_argument('--worker-tag', help='Tag to look for and put on workers')
    parser.add_argument(
        '--verbose', action='store_true', help='Whether to print out extra information'
    )
    parser.add_argument('--sleep-time', help='Number of seconds to wait between checks', default=10)
    parser.add_argument('--once', help='Just run once and exit', action='store_true')
    parser.add_argument(
        '--worker-idle-seconds',
        help='Wait this long for extra runs before quitting ',
        default=60 * 10,
    )
    args = parser.parse_args()

    # Set up logging.
    if args.verbose:
        logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)
    else:
        logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

    # Choose the worker manager type.
    if args.worker_manager_type == 'aws':
        manager = AWSWorkerManager(args)
    else:
        raise Exception('Invalid worker manager type: {}'.format(args.worker_manager_type))

    manager.run_loop()


if __name__ == '__main__':
    main()
