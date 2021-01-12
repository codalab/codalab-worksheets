"""
Main entry point for the worker managers.
"""

import argparse
import logging
from .aws_batch_worker_manager import AWSBatchWorkerManager
from .azure_batch_worker_manager import AzureBatchWorkerManager
from .slurm_batch_worker_manager import SlurmBatchWorkerManager


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--server', help='CodaLab instance to connect to', default='https://worksheets.codalab.org'
    )
    parser.add_argument(
        '--temp-session',
        action='store_false',
        help='Whether to use a temporary session (defaults to true).',
    )
    parser.add_argument('--min-workers', help='Minimum number of workers', type=int, default=1)
    parser.add_argument('--max-workers', help='Maximum number of workers', type=int, default=10)
    parser.add_argument(
        '--search', nargs='*', help='Monitor only runs that satisfy these criteria', default=[]
    )
    parser.add_argument(
        '--verbose', action='store_true', help='Whether to print out extra information'
    )
    parser.add_argument(
        '--sleep-time', help='Number of seconds to wait between checks', default=5, type=int
    )
    parser.add_argument(
        '--restart-after-seconds',
        type=int,
        help='Restart the worker manager after this many seconds have passed since launch',
    )
    parser.add_argument(
        '--no-prefilter',
        action='store_true',
        help='If set, do not filter run bundles by whether the created workers satisfy their requested resources.',
    )
    parser.add_argument(
        '--once',
        help='Just run once and exit instead of looping (for debugging)',
        action='store_true',
    )
    parser.add_argument(
        '--min-seconds-between-workers',
        help='Minimum time to wait between launching workers',
        default=1 * 60,
        type=int,
    )
    parser.add_argument('--worker-tag', help='Tag to look for and put on workers')
    parser.add_argument(
        '--worker-work-dir-prefix', help="Prefix to use for each worker's working directory."
    )
    parser.add_argument(
        '--worker-max-work-dir-size', help='Maximum size of the temporary bundle data'
    )
    parser.add_argument(
        '--worker-delete-work-dir-on-exit',
        action='store_true',
        help="Delete a worker's working directory when the worker process exits.",
    )
    parser.add_argument(
        '--worker-idle-seconds',
        help='Workers wait this long for extra runs before quitting',
        default=10 * 60,
        type=int,
    )
    parser.add_argument(
        '--worker-checkin-frequency-seconds',
        type=int,
        help='If specified, the CodaLab worker will wait this many seconds between check-ins',
    )
    parser.add_argument(
        '--worker-exit-after-num-runs',
        type=int,
        help='If specified, the CodaLab worker will exit after finishing this many of runs',
    )
    parser.add_argument(
        '--worker-pass-down-termination',
        action='store_true',
        help="If set, the CodaLab worker will kill and restage all existing running bundles when terminated.",
    )
    parser.add_argument(
        '--worker-exit-on-exception',
        action='store_true',
        help="If set, the CodaLab worker will exit if it encounters an exception (rather than sleeping).",
    )
    parser.add_argument(
        '--worker-tag-exclusive',
        action='store_true',
        help="If set, the CodaLab worker will only run bundles that match the worker\'s tag.",
    )
    parser.add_argument(
        '--worker-group',
        type=str,
        help="If set, the CodaLab worker will only run bundles for that group.",
    )
    parser.add_argument(
        '--worker-executable', default="cl-worker", help="The CodaLab worker executable to run."
    )
    subparsers = parser.add_subparsers(
        title='Worker Manager to run',
        description='Which worker manager to run (AWS Batch etc.)',
        dest='worker_manager_name',
    )
    # This is a workaround for setting a subparser as required for older
    # versions of python (< 3.7) , necessary due to a bug in Python 3.x .
    # https://bugs.python.org/issue9253#msg186387
    subparsers.required = True

    # Each worker manager class defines its NAME, which is the subcommand the users use
    # to invoke that type of Worker Manager. We map those to their respective classes
    # so we can automatically initialize the correct worker manager class from the argument
    worker_manager_types = {
        AWSBatchWorkerManager.NAME: AWSBatchWorkerManager,
        AzureBatchWorkerManager.NAME: AzureBatchWorkerManager,
        SlurmBatchWorkerManager.NAME: SlurmBatchWorkerManager,
    }
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
