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
import multiprocessing


from codalab.lib.formatting import parse_size
from .bundle_service_client import BundleServiceClient, BundleAuthException
from . import docker_utils
from .worker import Worker
from .local_run.local_dependency_manager import LocalFileSystemDependencyManager
from .local_run.docker_image_manager import DockerImageManager
from .local_run.local_run_manager import LocalRunManager

logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description='CodaLab worker.')
    parser.add_argument('--tag', help='Tag that allows for scheduling runs on specific ' 'workers.')
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
        '--network-prefix', default='codalab_worker_network', help='Docker network name prefix'
    )
    parser.add_argument(
        '--cpuset',
        type=str,
        metavar='CPUSET_STR',
        default='ALL',
        help='Comma-separated list of CPUs in which to allow bundle execution, '
        '(e.g., \"0,2,3\", \"1\").',
    )
    parser.add_argument(
        '--gpuset',
        type=str,
        metavar='GPUSET_STR',
        default='ALL',
        help='Comma-separated list of GPUs in which to allow bundle execution. '
        'Each GPU can be specified by its index or UUID'
        '(e.g., \"0,1\", \"1\", \"GPU-62casdfasd-asfas...\"',
    )
    parser.add_argument(
        '--max-work-dir-size',
        type=str,
        metavar='SIZE',
        default='10g',
        help='Maximum size of the temporary bundle data ' '(e.g., 3, 3k, 3m, 3g, 3t).',
    )
    parser.add_argument(
        '--max-image-cache-size',
        type=str,
        metavar='SIZE',
        help='Limit the disk space used to cache Docker images '
        'for worker jobs to the specified amount (e.g. '
        '3, 3k, 3m, 3g, 3t). If the limit is exceeded, '
        'the least recently used images are removed first. '
        'Worker will not remove any images if this option '
        'is not specified.',
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
        '--id',
        default='%s(%d)' % (socket.gethostname(), os.getpid()),
        help='Internal use: ID to use for the worker.',
    )
    parser.add_argument(
        '--shared-file-system',
        action='store_true',
        help='Internal use: Whether the file system containing '
        'bundle data is shared between the bundle service '
        'and the worker.',
    )
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

    max_work_dir_size_bytes = parse_size(args.max_work_dir_size)
    if args.max_image_cache_size is None:
        max_images_bytes = None
    else:
        max_images_bytes = parse_size(args.max_image_cache_size)

    if not os.path.exists(args.work_dir):
        logging.debug('Work dir %s doesn\'t exist, creating.', args.work_dir)
        os.makedirs(args.work_dir, 0o770)

    def create_local_run_manager(worker):
        """
        To avoid circular dependencies the Worker initializes takes a RunManager factory
        to initilize its run manager. This method creates a LocalFilesystem-Docker RunManager
        which is the default execution architecture Codalab uses
        """
        docker_runtime = docker_utils.get_available_runtime()
        cpuset = parse_cpuset_args(args.cpuset)
        gpuset = parse_gpuset_args(args.gpuset)

        dependency_manager = LocalFileSystemDependencyManager(
            os.path.join(args.work_dir, 'dependencies-state.json'),
            bundle_service,
            args.work_dir,
            max_work_dir_size_bytes,
        )

        image_manager = DockerImageManager(
            os.path.join(args.work_dir, 'images-state.json'), max_images_bytes
        )

        return LocalRunManager(
            worker,
            image_manager,
            dependency_manager,
            os.path.join(args.work_dir, 'run-state.json'),
            cpuset,
            gpuset,
            args.work_dir,
            docker_runtime=docker_runtime,
            docker_network_prefix=args.network_prefix,
        )

    worker = Worker(
        create_local_run_manager,
        os.path.join(args.work_dir, 'worker-state.json'),
        args.id,
        args.tag,
        args.work_dir,
        args.exit_when_idle,
        bundle_service,
    )

    # Register a signal handler to ensure safe shutdown.
    for sig in [signal.SIGTERM, signal.SIGINT, signal.SIGHUP]:
        signal.signal(sig, lambda signup, frame: worker.signal())

    # BEGIN: DO NOT CHANGE THIS LINE UNLESS YOU KNOW WHAT YOU ARE DOING
    # THIS IS HERE TO KEEP TEST-CLI FROM HANGING
    logger.info('Worker started!')
    # END

    worker.start()


def parse_cpuset_args(arg):
    """
    Parse given arg into a set of integers representing cpus

    Arguments:
        arg: comma seperated string of ints, or "ALL" representing all available cpus
    """
    cpu_count = multiprocessing.cpu_count()
    if arg == 'ALL':
        cpuset = list(range(cpu_count))
    else:
        try:
            cpuset = [int(s) for s in arg.split(',')]
        except ValueError:
            raise ValueError(
                "CPUSET_STR invalid format: must be a string of comma-separated integers"
            )

        if not len(cpuset) == len(set(cpuset)):
            raise ValueError("CPUSET_STR invalid: CPUs not distinct values")
        if not all(cpu in range(cpu_count) for cpu in cpuset):
            raise ValueError("CPUSET_STR invalid: CPUs out of range")
    return set(cpuset)


def parse_gpuset_args(arg):
    """
    Parse given arg into a set of strings representing gpu UUIDs

    Arguments:
        arg: comma seperated string of ints, or "ALL" representing all gpus
    """
    if arg == '':
        return set()

    try:
        all_gpus = docker_utils.get_nvidia_devices()  # Dict[GPU index: GPU UUID]
    except docker_utils.DockerException:
        all_gpus = {}

    if arg == 'ALL':
        return set(all_gpus.values())
    else:
        gpuset = arg.split(',')
        if not all(gpu in all_gpus or gpu in all_gpus.values() for gpu in gpuset):
            raise ValueError("GPUSET_STR invalid: GPUs out of range")
        return set(all_gpus.get(gpu, gpu) for gpu in gpuset)


if __name__ == '__main__':
    main()
