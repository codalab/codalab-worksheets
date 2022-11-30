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
import psutil
import requests

from codalab.common import SingularityError
from codalab.common import BundleRuntime
from codalab.lib.formatting import parse_size
from codalab.lib.telemetry_util import initialize_sentry, load_sentry_data, using_sentry
from .bundle_service_client import BundleServiceClient, BundleAuthException
from .worker import Worker
from codalab.worker.docker_utils import DockerRuntime, DockerException
from codalab.worker.dependency_manager import DependencyManager
from codalab.worker.docker_image_manager import DockerImageManager
from codalab.worker.singularity_image_manager import SingularityImageManager

logger = logging.getLogger(__name__)


DEFAULT_EXIT_AFTER_NUM_RUNS = 999999999


def parse_args():
    parser = argparse.ArgumentParser(description='CodaLab worker.')
    parser.add_argument(
        '--tag',
        help='Tag (can only contain letters, numbers or hyphens) that allows for scheduling runs on specific workers.',
    )
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
        type=parse_cpuset_args,
        metavar='CPUSET_STR',
        default='ALL',
        help='Comma-separated list of CPUs in which to allow bundle execution, '
        '(e.g., \"0,2,3\", \"1\").',
    )
    parser.add_argument(
        '--gpuset',
        type=parse_gpuset_args,
        metavar='GPUSET_STR',
        default='ALL',
        help='Comma-separated list of GPUs in which to allow bundle execution. '
        'Each GPU can be specified by its index or UUID'
        '(e.g., \"0,1\", \"1\", \"GPU-62casdfasd-asfas...\"',
    )
    parser.add_argument(
        '--max-work-dir-size',
        type=parse_size,
        metavar='SIZE',
        default='10g',
        help='Maximum size of the temporary bundle data ' '(e.g., 3, 3k, 3m, 3g, 3t).',
    )
    parser.add_argument(
        '--max-image-cache-size',
        type=parse_size,
        metavar='SIZE',
        default=None,
        help='Limit the disk space used to cache Docker images '
        'for worker jobs to the specified amount (e.g. '
        '3, 3k, 3m, 3g, 3t). If the limit is exceeded, '
        'the least recently used images are removed first. '
        'Worker will not remove any images if this option '
        'is not specified.',
    )
    parser.add_argument(
        '--max-image-size',
        type=parse_size,
        metavar='SIZE',
        default=None,
        help='Limit the size of Docker images to download from the Docker Hub'
        '(e.g. 3, 3k, 3m, 3g, 3t). If the limit is exceeded, '
        'the requested image will not be downloaded. '
        'The bundle depends on this image will fail accordingly. '
        'If running an image on the singularity runtime, there is no size '
        'check because singularity hub does not support the querying of image size',
    )
    parser.add_argument(
        '--max-memory',
        type=parse_size,
        metavar='SIZE',
        default=None,
        help='Limit the amount of memory to a worker in bytes' '(e.g. 3, 3k, 3m, 3g, 3t).',
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
        '--bundle-runtime',
        choices=[BundleRuntime.DOCKER.value, BundleRuntime.SINGULARITY.value,],
        default=BundleRuntime.DOCKER.value,
        help='The runtime through which the worker will run bundles. The options are docker (default) or singularity',
    )
    parser.add_argument(
        '--idle-seconds',
        help='Not running anything for this many seconds constitutes idle',
        type=int,
        default=0,
    )
    parser.add_argument(
        '--checkin-frequency-seconds',
        help='Number of seconds to wait between worker check-ins',
        type=int,
        default=5,
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
    parser.add_argument(
        '--group', default=None, help='Name of the group that can run jobs on this worker'
    )
    parser.add_argument(
        '--tag-exclusive',
        action='store_true',
        help='To be used when the worker should only run bundles that match the worker\'s tag.',
    )
    parser.add_argument(
        '--pass-down-termination',
        action='store_true',
        help='Terminate the worker and kill all the existing running bundles.',
    )
    parser.add_argument(
        '--delete-work-dir-on-exit',
        action='store_true',
        help="Delete the worker's working directory when the worker process exits.",
    )
    parser.add_argument(
        '--exit-after-num-runs',
        type=int,
        default=DEFAULT_EXIT_AFTER_NUM_RUNS,
        help='The worker quits after this many jobs assigned to this worker',
    )
    parser.add_argument(
        '--exit-on-exception',
        action='store_true',
        help="Exit the worker if it encounters an exception (rather than sleeping).",
    )
    parser.add_argument(
        '--download-dependencies-max-retries',
        type=int,
        default=3,
        help='The number of times to retry downloading dependencies after a failure (defaults to 3).',
    )
    parser.add_argument(
        '--shared-memory-size-gb',
        type=int,
        default=1,
        help='The shared memory size of the run container in GB (defaults to 1).',
    )
    parser.add_argument(
        '--preemptible', action='store_true', help='Whether the worker is preemptible.',
    )
    return parser.parse_args()


def connect_to_codalab_server(server, password_file):
    # Get the username and password.
    logger.info('Connecting to %s' % server)
    if password_file:
        if os.stat(password_file).st_mode & (stat.S_IRWXG | stat.S_IRWXO):
            print(
                "Permissions on password file are too lax.\n\
                Only the user should be allowed to access the file.\n\
                On Linux, run:\n\
                chmod 600 %s"
                % password_file,
                file=sys.stderr,
            )
            sys.exit(1)
        with open(password_file) as f:
            username = f.readline().strip()
            password = f.readline().strip()
    else:
        username = os.environ.get('CODALAB_USERNAME')
        if username is None:
            username = input('Username: ')
        password = os.environ.get('CODALAB_PASSWORD')
        if password is None:
            password = getpass.getpass()
    try:
        bundle_service = BundleServiceClient(server, username, password)
        return bundle_service
    except BundleAuthException as ex:
        logger.error(
            'Cannot log into the bundle service. Please check your worker credentials.\n'
            f'Username: "{username}" , server "{server}"\n'
        )
        logger.debug('Auth error: {}'.format(ex))
        sys.exit(1)


def main():
    args = parse_args()

    if args.tag and not args.tag.replace("-", "").isalnum():
        raise argparse.ArgumentTypeError(
            "Worker tag must only contain letters, numbers or hyphens."
        )

    # Configure logging
    log_format: str = '%(asctime)s %(message)s'
    if args.verbose:
        log_format += ' %(pathname)s %(lineno)d'
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO
    logging.basicConfig(format=log_format, level=log_level)

    logging.getLogger('urllib3').setLevel(logging.INFO)
    # Initialize sentry logging
    if using_sentry():
        initialize_sentry()

    # This quits if connection unsuccessful
    bundle_service = connect_to_codalab_server(args.server, args.password_file)

    # Load some data into sentry
    if using_sentry():
        load_sentry_data(username=bundle_service._username, **vars(args))

    if args.shared_file_system:
        # No need to store bundles locally if filesystems are shared
        local_bundles_dir = None
        # Also no need to download dependencies if they're on the filesystem already
        dependency_manager = None
    else:
        local_bundles_dir = os.path.join(args.work_dir, 'runs')
        dependency_manager = DependencyManager(
            os.path.join(args.work_dir, 'dependencies-state.json'),
            bundle_service,
            args.work_dir,
            args.max_work_dir_size,
            args.download_dependencies_max_retries,
        )

    if args.bundle_runtime == BundleRuntime.SINGULARITY.value:
        singularity_folder = os.path.join(args.work_dir, 'codalab_singularity_images')
        if not os.path.exists(singularity_folder):
            logger.info(
                'Local singularity image location %s doesn\'t exist, creating.', singularity_folder,
            )
            os.makedirs(singularity_folder, 0o770)
        image_manager = SingularityImageManager(
            args.max_image_size, args.max_image_cache_size, singularity_folder,
        )
        # todo workers with singularity don't work because this is set to none -- handle this
        bundle_runtime_class = None
        docker_runtime = None
    else:
        image_manager = DockerImageManager(
            os.path.join(args.work_dir, 'images-state.json'),
            args.max_image_cache_size,
            args.max_image_size,
        )
        bundle_runtime_class = DockerRuntime()
        docker_runtime = bundle_runtime_class.get_available_runtime()
    # Set up local directories
    if not os.path.exists(args.work_dir):
        logging.debug('Work dir %s doesn\'t exist, creating.', args.work_dir)
        os.makedirs(args.work_dir, 0o770)
    if local_bundles_dir and not os.path.exists(local_bundles_dir):
        logger.info('%s doesn\'t exist, creating.', local_bundles_dir)
        os.makedirs(local_bundles_dir, 0o770)

    worker = Worker(
        image_manager,
        dependency_manager,
        # Include the worker ID in the worker state JSON path, so multiple workers
        # sharing the same work directory maintain their own state.
        os.path.join(args.work_dir, f'worker-state-{args.id}.json'),
        args.cpuset,
        args.gpuset,
        args.max_memory,
        args.id,
        args.tag,
        args.work_dir,
        local_bundles_dir,
        args.exit_when_idle,
        args.exit_after_num_runs,
        args.idle_seconds,
        args.checkin_frequency_seconds,
        bundle_service,
        args.shared_file_system,
        args.tag_exclusive,
        args.group,
        docker_runtime=docker_runtime,
        docker_network_prefix=args.network_prefix,
        pass_down_termination=args.pass_down_termination,
        delete_work_dir_on_exit=args.delete_work_dir_on_exit,
        exit_on_exception=args.exit_on_exception,
        shared_memory_size_gb=args.shared_memory_size_gb,
        preemptible=args.preemptible,
        bundle_runtime=DockerRuntime(),
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
        arg: comma separated string of ints, or "ALL" representing all available cpus
    """
    try:
        # Get the set of cores that the process can actually use.
        # For instance, on Slurm, the returning value may contain only 4 cores: {2,3,20,21}.
        return os.sched_getaffinity(0)
    except AttributeError:
        # os.sched_getaffinity() isn't available on all platforms,
        # so fallback to using the number of physical cores.
        cpu_count = psutil.cpu_count(logical=False)

    if arg == 'ALL':
        cpuset = list(range(cpu_count))
    else:
        try:
            cpuset = [int(s) for s in arg.split(',')]
        except ValueError:
            raise argparse.ArgumentTypeError(
                "CPUSET_STR invalid format: must be a string of comma-separated integers"
            )

        if not len(cpuset) == len(set(cpuset)):
            raise argparse.ArgumentTypeError("CPUSET_STR invalid: CPUs not distinct values")
        if not all(cpu in range(cpu_count) for cpu in cpuset):
            raise argparse.ArgumentTypeError("CPUSET_STR invalid: CPUs out of range")
    return set(cpuset)


def parse_gpuset_args(arg):
    """
    Parse given arg into a set of strings representing gpu UUIDs
    By default, we will try to start a Docker container with nvidia-smi to get the GPUs.
    If we get an exception that the Docker socket does not exist, which will be the case
    on Singularity workers, because they do not have root access, and therefore, access to
    the Docker socket, we should try to get the GPUs with Singularity.

    Arguments:
        arg: comma separated string of ints, or "ALL" representing all gpus
    """
    logger.info(f"GPUSET arg: {arg}")
    if arg == '' or arg == 'NONE':
        return set()

    try:
        all_gpus = DockerRuntime().get_nvidia_devices()  # Dict[GPU index: GPU UUID]
    except DockerException:
        all_gpus = {}
    # Docker socket can't be used
    except requests.exceptions.ConnectionError:
        try:
            all_gpus = DockerRuntime().get_nvidia_devices(use_docker=False)
        except SingularityError:
            all_gpus = {}

    if arg == 'ALL':
        return set(all_gpus.values())
    else:
        gpuset = arg.split(',')
        if not all(gpu in all_gpus or gpu in all_gpus.values() for gpu in gpuset):
            raise argparse.ArgumentTypeError("GPUSET_STR invalid: GPUs out of range")
        return set(all_gpus.get(gpu, gpu) for gpu in gpuset)


if __name__ == '__main__':
    main()
