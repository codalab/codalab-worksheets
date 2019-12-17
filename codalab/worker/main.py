#!/usr/bin/env python3
# For information about the design of the worker, see design.pdf in the same
# directory as this file. For information about running a worker, see the
# tutorial on the CodaLab documentation.

import argparse
import getpass
import os
import logging
import signal
import socket
import stat
import sys


from codalab.lib.formatting import parse_size
from .bundle_service_client import BundleServiceClient, BundleAuthException
from . import docker_utils
from .worker import Worker

from .local_run.local_dependency_manager import LocalFileSystemDependencyManager
from .local_run.docker_image_manager import DockerImageManager
from .local_run.local_run_manager import LocalRunManager

from .aws_batch.aws_batch_run_manager import AWSBatchRunManager

logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description='CodaLab worker.')
    parser.add_argument('--tag', help='Tag that allows for scheduling runs on specific workers.')
    parser.add_argument(
        '--server',
        default='https://worksheets.codalab.org',
        help='URL of the CodaLab server, in the format '
        '<http|https>://<hostname>[:<port>] (e.g., https://worksheets.codalab.org)',
    )
    parser.add_argument(
        '--work-dir',
        default='codalab-worker-scratch',
        help='Directory where to store temporary bundle data, '
        'including dependencies and the data from run '
        'bundles.',
    )
    parser.add_argument(
        '--password-file',
        help='Path to the file containing the username and '
        'password for logging into the bundle service, '
        'each on a separate line. If not specified, the '
        'password is read from standard input.',
    )
    parser.add_argument(
        '--verbose', action='store_true', help='Whether to output verbose log messages.'
    )
    parser.add_argument(
        '--exit-when-idle',
        action='store_true',
        help='If specified the worker quits if it finds itself with no jobs after a checkin',
    )
    parser.add_argument(
        '--idle-seconds',
        help='Not running anything for this many seconds constitutes idle',
        type=int,
        default=0,
    )
    parser.add_argument(
        '--id',
        default='%s(%d)' % (socket.gethostname(), os.getpid()),
        help='Internal use: ID to use for the worker.',
    )
    parser.add_argument(
        '--shared-file-system',
        action='store_true',
        help='To be used when the server and the worker share the bundle store on their filesystems.',
    )
    subparsers = parser.add_subparsers(
        title='Run Manager to run',
        description='Which run manager to run (Local, AWS Batch etc.)',
        dest='run_manager_name',
    )
    # Each run manager class defines its NAME, which is the subcommand the users use
    # to invoke that type of Run Manager. We map those to their respective classes
    # so we can automatically initialize the correct run manager class from the argument
    run_manager_types = {
        AWSBatchRunManager.NAME: AWSBatchRunManager,
        LocalRunManager.NAME: LocalRunManager,
    }
    for run_manager_name, run_manager_type in run_manager_types.items():
        # This lets each run manager class to define its own arguments
        run_manager_subparser = subparsers.add_parser(
            run_manager_name, description=run_manager_type.DESCRIPTION
        )
        run_manager_type.add_arguments_to_subparser(run_manager_subparser)
    args = parser.parse_args()

    # Get the username and password.
    logger.info('Connecting to %s' % args.server)
    if args.password_file:
        if os.stat(args.password_file).st_mode & (stat.S_IRWXG | stat.S_IRWXO):
            print(
                """
Permissions on password file are too lax.
Only the user should be allowed to access the file.
On Linux, run:
chmod 600 %s"""
                % args.password_file,
                file=sys.stderr,
            )
            exit(1)
        with open(args.password_file) as f:
            username = f.readline().strip()
            password = f.readline().strip()
    else:
        username = os.environ.get('CODALAB_USERNAME')
        if username is None:
            username = input('Username: ')
        password = os.environ.get('CODALAB_PASSWORD')
        if password is None:
            password = getpass.getpass()

    # Set up logging.
    if args.verbose:
        logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)
    else:
        logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

    try:
        bundle_service = BundleServiceClient(args.server, username, password)
    except BundleAuthException as ex:
        logger.error('Cannot log into the bundle service. Please check your worker credentials.\n')
        logger.debug('Auth error: {}'.format(ex))
        return

    if not os.path.exists(args.work_dir):
        logging.debug('Work dir %s doesn\'t exist, creating.', args.work_dir)
        os.makedirs(args.work_dir, 0o770)

    run_manager_factory = lambda worker: run_manager_types[
        args.run_manager_name
    ].create_run_manager(args, worker)

    worker = Worker(
        run_manager_factory,
        os.path.join(args.work_dir, 'worker-state.json'),
        args.id,
        args.tag,
        args.work_dir,
        args.exit_when_idle,
        args.idle_seconds,
        bundle_service,
        args.shared_file_system,
    )

    # Register a signal handler to ensure safe shutdown.
    for sig in [signal.SIGTERM, signal.SIGINT, signal.SIGHUP]:
        signal.signal(sig, lambda signup, frame: worker.signal())

    # BEGIN: DO NOT CHANGE THIS LINE UNLESS YOU KNOW WHAT YOU ARE DOING
    # THIS IS HERE TO KEEP TEST-CLI FROM HANGING
    logger.info('Worker started!')
    # END

    worker.start()


if __name__ == '__main__':
    main()
