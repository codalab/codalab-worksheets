"""
Main entry point for the worker managers.
"""

import argparse
import logging
from .aws_batch_worker_manager import AWSBatchWorkerManager


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--server', help='CodaLab instance to connect to', default='https://worksheets.codalab.org'
    )
    parser.add_argument(
        '-t',
        '--worker-manager-type',
        help='Type of worker manager',
        choices=['aws-batch'],
        required=True,
    )
    parser.add_argument('--min-workers', help='Minimum number of workers', type=int, default=1)
    parser.add_argument('--max-workers', help='Maximum number of workers', type=int, default=10)
    parser.add_argument('--queue', help='Monitor and run workers on this queue (e.g., AWS Batch)')
    parser.add_argument(
        '--search', nargs='*', help='Monitor only runs that satisfy these criteria', default=[]
    )
    parser.add_argument('--worker-tag', help='Tag to look for and put on workers')
    parser.add_argument(
        '--verbose', action='store_true', help='Whether to print out extra information'
    )
    parser.add_argument('--sleep-time', help='Number of seconds to wait between checks', default=5)
    parser.add_argument(
        '--once',
        help='Just run once and exit instead of looping (for debugging)',
        action='store_true',
    )
    parser.add_argument(
        '--worker-idle-seconds',
        help='Workers wait this long for extra runs before quitting',
        default=10 * 60,
    )
    parser.add_argument(
        '--min-seconds-between-workers',
        help='Minimum time to wait between launching workers',
        default=1 * 60,
    )
    args = parser.parse_args()

    # Set up logging.
    if args.verbose:
        logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)
    else:
        logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)
    if args.max_workers < args.min_workers:
        raise ValueError(
            'Minimum # of workers (%d) greater than maximum # of workers (%d)'
            % (args.min_workers, args.max_workers)
        )

    # Choose the worker manager type.
    if args.worker_manager_type == 'aws-batch':
        manager = AWSBatchWorkerManager(args)
    else:
        raise ValueError('Invalid worker manager type: {}'.format(args.worker_manager_type))

    # Go!
    manager.run_loop()


if __name__ == '__main__':
    main()
