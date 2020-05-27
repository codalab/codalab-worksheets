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
    parser.add_argument('--min-workers', help='Minimum number of workers', type=int, default=1)
    parser.add_argument('--max-workers', help='Maximum number of workers', type=int, default=10)
    parser.add_argument(
        '--search', nargs='*', help='Monitor only runs that satisfy these criteria', default=[]
    )
    parser.add_argument('--worker-tag', help='Tag to look for and put on workers')
    parser.add_argument(
        '--worker-max-work-dir-size', help='Maximum size of the temporary bundle data'
    )
    parser.add_argument(
        '--verbose', action='store_true', help='Whether to print out extra information'
    )
    parser.add_argument(
        '--sleep-time', help='Number of seconds to wait between checks', default=5, type=int
    )
    parser.add_argument(
        '--once',
        help='Just run once and exit instead of looping (for debugging)',
        action='store_true',
    )
    parser.add_argument(
        '--worker-idle-seconds',
        help='Workers wait this long for extra runs before quitting',
        default=10 * 60,
        type=int,
    )
    parser.add_argument(
        '--min-seconds-between-workers',
        help='Minimum time to wait between launching workers',
        default=1 * 60,
        type=int,
    )
    subparsers = parser.add_subparsers(
        title='Worker Manager to run',
        description='Which worker manager to run (AWS Batch etc.)',
        dest='worker_manager_name',
    )

    # Each worker manager class defines its NAME, which is the subcommand the users use
    # to invoke that type of Worker Manager. We map those to their respective classes
    # so we can automatically initialize the correct worker manager class from the argument
    worker_manager_types = {AWSBatchWorkerManager.NAME: AWSBatchWorkerManager}
    for worker_manager_name, worker_manager_type in worker_manager_types.items():
        # This lets each worker manager class to define its own arguments
        worker_manager_subparser = subparsers.add_parser(
            worker_manager_name, description=worker_manager_type.DESCRIPTION
        )
        worker_manager_type.add_arguments_to_subparser(worker_manager_subparser)
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

    manager = worker_manager_types[args.worker_manager_name](args)
    manager.run_loop()


if __name__ == '__main__':
    main()
