#! /usr/bin/env python3

"""
The main entry point for bringing up CodaLab services.  This is used for both
local development and actual deployment.

A full deployment is governed by a set of *arguments*, which can either be
specified via (later overriding the former):
- (i) defaults defined in this file,
- (ii) environment variables (e.g., CODALAB_HOME),
- (iii) command-line arguments

We then launch a set of services (e.g., `rest-server`), where for each one:
- We create an environment from a subset of the above arguments.
- We call `docker-compose` or `docker run`, which might read these environment
  variables and pass them through to the actual service (see Docker files for
  exact logic).
"""
from __future__ import print_function

import argparse
import errno
import os
import socket
import subprocess
import yaml

DEFAULT_SERVICES = [
    'mysql',
    'nginx',
    'frontend',
    'rest-server',
    'bundle-manager',
    'worker',
    'init',
]

ALL_SERVICES = DEFAULT_SERVICES + ['azurite', 'monitor', 'worker-manager-cpu', 'worker-manager-gpu']

ALL_NO_SERVICES = [
    'no-' + service for service in ALL_SERVICES
]  # Identifiers that stand for exclusion of certain services

BASE_DIR = os.path.dirname(os.path.realpath(__file__))

# Which docker image is used to run each service?
SERVICE_TO_IMAGE = {
    'frontend': 'frontend',
    'rest-server': 'server',
    'bundle-manager': 'server',
    'worker-manager-cpu': 'server',
    'worker-manager-gpu': 'server',
    'monitor': 'server',
    'worker': 'worker',
}

# Max timeout in seconds to wait for request to a service to get through
SERVICE_REQUEST_TIMEOUT_SECONDS = 600


def ensure_directory_exists(path):
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise


def print_header(description):
    print('[CodaLab] {}'.format(description))


def should_run_service(args, service):
    # `default` is generally used to bring up everything for local dev or quick testing.
    # `default no-worker` is generally used for real deployment since we don't want a worker running on the same machine.
    services = [] if args.services is None else args.services
    if 'default' in args.services:
        services.extend(DEFAULT_SERVICES)

    # 'worker-shared-file-system` is just `worker` but with a different argument, so they're equivalent for us
    if service == 'worker-shared-file-system':
        service = 'worker'
    elif 'worker-manager-cpu' in service:
        service = 'worker-manager-cpu'
    elif 'worker-manager-gpu' in service:
        service = 'worker-manager-gpu'

    return (service in services) and ('no-' + service not in services)


def need_image_for_service(args, image):
    """Does `image` support a service we want to run?"""
    for service, service_image in SERVICE_TO_IMAGE.items():
        if should_run_service(args, service) and image == service_image:
            return True
    return False


def should_build_image(args, image):
    if image in args.images:
        return True
    if 'all' in args.images:
        return True
    if 'services' in args.images:
        return need_image_for_service(args, image)
    return False


def main():
    args = CodalabArgs.get_args()
    service_manager = CodalabServiceManager(args)
    service_manager.execute()


def clean_version(version):
    """Clean version name (usually a branch name) so it can be used as a
    tag name for a Docker image."""
    return version.replace("/", "_").replace("-", "_")


def get_default_version():
    """Get the current git branch."""
    return clean_version(
        subprocess.check_output(
            ['git', 'rev-parse', '--abbrev-ref', 'HEAD'], encoding='utf-8'
        ).strip()
    )


def var_path(name):
    return lambda args: os.path.join(BASE_DIR, 'var', args.instance_name, name)


# An configuration argument.
class CodalabArg(object):
    def __init__(self, name, help, type=str, env_var=None, flag=None, default=None):
        self.name = name
        self.help = help
        self.type = type
        self.env_var = env_var or 'CODALAB_' + name.upper()
        self.flag = flag  # Command-line argument
        self.default = default

    def has_constant_default(self):
        return self.default is not None and not callable(self.default)

    def has_callable_default(self):
        return self.default is not None and callable(self.default)


CODALAB_ARGUMENTS = [
    # Basic settings
    CodalabArg(name='version', help='Version of CodaLab (usually the branch name)', flag='-v',),
    CodalabArg(
        name='instance_name',
        help='Instance name (prefixed to Docker containers)',
        default='codalab',
    ),
    CodalabArg(
        name='protected_mode',
        env_var='CODALAB_PROTECTED_MODE',
        help='Whether to run the instance in protected mode',
        type=bool,
        default=False,
        flag='-p',
    ),
    CodalabArg(
        name='worker_network_prefix',
        help='Network name for the worker',
        default=lambda args: args.instance_name + '-worker-network',
    ),
    # Docker
    CodalabArg(name='docker_username', help='Docker Hub username to push built images'),
    CodalabArg(name='docker_password', help='Docker Hub password to push built images'),
    # CodaLab
    CodalabArg(
        name='codalab_username',
        env_var='CODALAB_USERNAME',
        help='CodaLab (root) username',
        default='codalab',
    ),
    CodalabArg(
        name='codalab_password',
        env_var='CODALAB_PASSWORD',
        help='CodaLab (root) password',
        default='codalab',
    ),
    # MySQL
    CodalabArg(name='mysql_host', help='MySQL hostname', default='mysql'),  # Inside Docker
    CodalabArg(name='mysql_port', help='MySQL port', default=3306, type=int),
    CodalabArg(name='mysql_database', help='MySQL database name', default='codalab_bundles'),
    CodalabArg(name='mysql_username', help='MySQL username', default='codalab'),
    CodalabArg(name='mysql_password', help='MySQL password', default='codalab'),
    CodalabArg(name='mysql_root_password', help='MySQL root password', default='codalab'),
    CodalabArg(
        name='uid',
        help='UID:GID to run everything inside Docker and owns created files',
        default='%s:%s' % (os.getuid(), os.getgid()),
    ),
    CodalabArg(
        name='codalab_home',
        env_var='CODALAB_HOME',
        help='Path to store things like config.json for the REST server',
        default=var_path('home'),
    ),
    CodalabArg(
        name='bundle_mount',
        help='Path to bundle data (just for mounting into Docker)',
        default=var_path(''),  # Put any non-empty path here
    ),
    CodalabArg(name='mysql_mount', help='Path to store MySQL data', default=var_path('mysql')),
    CodalabArg(
        name='monitor_dir',
        help='Path to store monitor logs and DB backups',
        default=var_path('monitor'),
    ),
    CodalabArg(
        name='worker_dir',
        help='Path to store worker state / cached dependencies',
        default=var_path('worker'),
    ),
    CodalabArg(name='http_port', help='Port for nginx', type=int, default=80),
    CodalabArg(name='https_port', help='Port for nginx (when using SSL)', type=int, default=443),
    CodalabArg(name='frontend_port', help='Port for frontend', type=int, default=2700),
    CodalabArg(name='rest_port', help='Port for REST server', type=int, default=2900),
    CodalabArg(name='rest_num_processes', help='Number of processes', type=int, default=1),
    CodalabArg(name='server', help='URL to server (used by external worker to connect to)'),
    CodalabArg(
        name='shared_file_system', help='Whether worker has access to the bundle mount', type=bool
    ),
    # User
    CodalabArg(name='user_disk_quota', help='How much space a user can use', default='100g'),
    CodalabArg(name='user_time_quota', help='How much total time a user can use', default='100y'),
    CodalabArg(
        name='user_parallel_run_quota',
        help='How many simultaneous runs a user can have',
        type=int,
        default=100,
    ),
    # Email
    CodalabArg(name='admin_email', help='Email to send admin notifications to (e.g., monitoring)'),
    CodalabArg(name='support_email', help='Help email to send user questions to'),
    CodalabArg(name='email_host', help='Send email by logging into this SMTP server'),
    CodalabArg(name='email_username', help='Username of email account for sending email'),
    CodalabArg(name='email_password', help='Password of email account for sending email'),
    # SSL
    CodalabArg(name='use_ssl', help='Use HTTPS instead of HTTP', type=bool, default=False),
    CodalabArg(name='ssl_cert_file', help='Path to the cert file for SSL'),
    CodalabArg(name='ssl_key_file', help='Path to the key file for SSL'),
    # Sentry
    CodalabArg(
        name='sentry_ingest_url',
        help=(
            'Ingest URL for logging exceptions with Sentry. If not provided, Sentry is not used.'
        ),
    ),
    # Bundle Manager
    CodalabArg(
        name='bundle_manager_worker_timeout_seconds',
        help='Number of seconds to wait after a worker check-in before determining a worker is offline',
        type=int,
        default=60,
    ),
    # Worker manager
    CodalabArg(
        name='worker_manager_type',
        help='Type of worker manager (azure-batch or aws-batch)',
        default='azure-batch',
    ),
    CodalabArg(
        name='worker_manager_worker_work_dir_prefix',
        help='Prefix to use for each worker\'s working directory of the worker manager',
        default='/tmp',
    ),
    CodalabArg(
        name='worker_manager_worker_max_work_dir_size',
        help='Maximum size of the temporary bundle data for a worker of the worker manager',
        default='10g',
    ),
    CodalabArg(
        name='worker_manager_worker_checkin_frequency_seconds',
        help='Number of seconds to wait between check-ins for a worker of the worker manager',
        type=int,
        default=5,
    ),
    CodalabArg(
        name='worker_manager_idle_seconds',
        help='Number of seconds workers wait idle before exiting',
        type=int,
        default=10 * 60,
    ),
    CodalabArg(
        name='worker_manager_seconds_between_workers',
        help='Number of seconds to wait between launching two workers',
        type=int,
        default=60,
    ),
    CodalabArg(
        name='worker_manager_sleep_time_seconds',
        help='Number of seconds to wait between checks',
        type=int,
        default=5,
    ),
    CodalabArg(
        name='worker_manager_default_gpus',
        type=int,
        default=0,
        help='Default number of GPUs for each worker',
    ),
    CodalabArg(
        name='worker_manager_azure_batch_account_name',
        type=str,
        help='Azure Batch account name for the Azure Batch worker manager',
    ),
    CodalabArg(
        name='worker_manager_azure_batch_account_key',
        type=str,
        help='Azure Batch account key for the Azure Batch worker manager',
    ),
    CodalabArg(
        name='worker_manager_azure_batch_service_url',
        type=str,
        help='Azure Batch service url for the Azure Batch worker manager',
    ),
    CodalabArg(
        name='worker_manager_aws_region',
        type=str,
        default='us-east-1',
        help='AWS region to run jobs in',
    ),
    CodalabArg(
        name='worker_manager_aws_batch_job_definition_name',
        type=str,
        default='codalab-worker',
        help='Name for the job definitions that will be generated by this worker manager',
    ),
    CodalabArg(
        name='compose_http_timeout',
        env_var='COMPOSE_HTTP_TIMEOUT',
        type=int,
        default=SERVICE_REQUEST_TIMEOUT_SECONDS,
        help='Docker Compose HTTP timeout (in seconds)',
    ),
    CodalabArg(
        name='docker_client_timeout',
        env_var='DOCKER_CLIENT_TIMEOUT',
        type=int,
        default=SERVICE_REQUEST_TIMEOUT_SECONDS,
        help='Docker client timeout (in seconds)',
    ),
    CodalabArg(
        name='link_mounts',
        help='Comma-separated list of directories that are mounted on the REST server, allowing their contents to be used in the --link argument.',
        default='/tmp/codalab/link-mounts',
    ),
    # Public workers
    CodalabArg(name='public_workers', help='Comma-separated list of worker ids to monitor'),
]

for worker_manager_type in ['cpu', 'gpu']:
    CODALAB_ARGUMENTS += [
        CodalabArg(
            name='worker_manager_{}_default_cpus'.format(worker_manager_type),
            type=int,
            default=1,
            help='Default number of CPUs for each worker started by the {} worker manager'.format(
                worker_manager_type
            ),
        ),
        CodalabArg(
            name='worker_manager_{}_default_memory_mb'.format(worker_manager_type),
            type=int,
            default=2048,
            help='Default memory (in MB) for each worker started by the {} worker manager'.format(
                worker_manager_type
            ),
        ),
        CodalabArg(
            name='worker_manager_{}_tag'.format(worker_manager_type),
            help='Tag of worker for {} jobs'.format(worker_manager_type),
            default='codalab-{}'.format(worker_manager_type),
        ),
        CodalabArg(
            name='worker_manager_max_{}_workers'.format(worker_manager_type),
            help='Maximum number of {} workers per worker manager'.format(worker_manager_type),
            type=int,
            default=10,
        ),
        CodalabArg(
            name='worker_manager_min_{}_workers'.format(worker_manager_type),
            help='Minimum number of {} workers per worker manager'.format(worker_manager_type),
            type=int,
            default=1,
        ),
        CodalabArg(
            name='worker_manager_{}_azure_batch_job_id'.format(worker_manager_type),
            type=str,
            default='codalab-{}'.format(worker_manager_type),
            help='ID of the Azure Batch job to add tasks to for the {} worker manager'.format(
                worker_manager_type
            ),
        ),
        CodalabArg(
            name='worker_manager_{}_azure_log_container_url'.format(worker_manager_type),
            type=str,
            help='URL of the Azure Storage container to store the worker logs for the {} Azure Batch worker manager'.format(
                worker_manager_type
            ),
        ),
        CodalabArg(
            name='worker_manager_{}_aws_batch_queue'.format(worker_manager_type),
            help='Name of queue to submit {} jobs'.format(worker_manager_type),
            default='codalab-{}'.format(worker_manager_type),
        ),
    ]


class CodalabArgs(object):
    @staticmethod
    def _get_parser():
        parser = argparse.ArgumentParser(
            description='Manages your local CodaLab Worksheets service deployment'
        )
        subparsers = parser.add_subparsers(dest='command', description='Command to run')

        # Subcommands
        start_cmd = subparsers.add_parser('start', help='Start a CodaLab service instance')
        logs_cmd = subparsers.add_parser('logs', help='View logs for existing CodaLab instance')
        pull_cmd = subparsers.add_parser('pull', help='Pull images from Docker Hub')
        build_cmd = subparsers.add_parser(
            'build', help='Build CodaLab docker images using the local codebase'
        )
        run_cmd = subparsers.add_parser('run', help='Run a command inside a service container')
        stop_cmd = subparsers.add_parser('stop', help='Stop any existing CodaLab service instances')
        delete_cmd = subparsers.add_parser(
            'delete',
            help='Bring down any existing CodaLab service instances (and delete all non-external data!)',
        )

        # Arguments for every subcommand
        for cmd in [start_cmd, logs_cmd, pull_cmd, build_cmd, run_cmd, stop_cmd, delete_cmd]:
            cmd.add_argument(
                '--dry-run',
                action='store_true',
                help='Just print out the commands that will be run and not execute anything',
            )

            # Populate the command-line parser with CODALAB_ARGUMENTS.
            for arg in CODALAB_ARGUMENTS:
                # Unnamed parameters to add_argument
                unnamed = ['--' + arg.name.replace('_', '-')]
                if arg.flag:
                    unnamed.append(arg.flag)
                # Named parameters to add_argument
                named = {'help': arg.help}
                # Don't set defaults here or else we won't know downstream
                # whether a value was a default or passed in on the
                # command-line.
                if arg.type == bool:
                    named['action'] = 'store_true'
                else:
                    named['type'] = arg.type
                # Add it!
                cmd.add_argument(*unnamed, **named)

            cmd.add_argument(
                '--build-images',
                '-b',
                action='store_true',
                help='Build Docker images using local code',
            )
            cmd.add_argument(
                '--pull',
                action='store_true',
                help='Pull images from Docker Hub (for caching) before building',
                default=False,
            )
            cmd.add_argument(
                '--push',
                action='store_true',
                help='Push the images to Docker Hub after building',
                default=False,
            )
            cmd.add_argument(
                (
                    'images' if cmd == build_cmd else '--images'
                ),  # For the explicit build command make this argument positional
                default='services',
                help='Images to build. \'services\' for server-side images (frontend, server, worker) \
                        \'all\' to include default execution images',
                choices=CodalabServiceManager.ALL_IMAGES + ['all', 'services'],
                nargs='*',
            )
            cmd.add_argument(
                '--dev',
                '-d',
                action='store_true',
                help='Mount local code for frontend so that changes are reflected right away',
            )
            cmd.add_argument(
                '--services',
                '-s',
                nargs='*',
                help='List of services to run',
                choices=ALL_SERVICES + ALL_NO_SERVICES + ['default'],
                default=['default'],
            )

        # Arguments for `logs`
        logs_cmd.add_argument(
            'services',
            nargs='*',
            default='default',
            help='Services to print logs for',
            choices=ALL_SERVICES + ['default'],
        )
        logs_cmd.add_argument(
            '--follow',
            '-f',
            action='store_true',
            help='If specified, follow the logs',
            default=True,
        )
        logs_cmd.add_argument(
            '--tail',
            '-t',
            type=int,
            help='If specified, tail TAIL lines from the ends of each log',
            default=100,
        )

        # Arguments for `run`
        run_cmd.add_argument(
            'service',
            metavar='SERVICE',
            choices=ALL_SERVICES,
            help='Service container to run command on',
        )
        run_cmd.add_argument('service_command', metavar='CMD', type=str, help='Command to run')

        return parser

    @classmethod
    def get_args(cls):
        parser = cls._get_parser()
        args = argparse.Namespace()

        # Set from command-line arguments
        parser.parse_args(namespace=args)

        # Set from environment variables
        for arg in CODALAB_ARGUMENTS:
            if getattr(args, arg.name, None):  # Skip if set
                continue
            if arg.env_var in os.environ:
                # All environment variables are string-valued.  Need to convert
                # into the appropriate types.
                value = os.environ[arg.env_var]
                if arg.type == 'bool':
                    value = value == 'true'
                elif arg.type == 'int':
                    value = int(value)
                elif arg.type == 'float':
                    value = float(value)
                setattr(args, arg.name, value)

        # Set constant default values
        for arg in CODALAB_ARGUMENTS:
            if getattr(args, arg.name, None):  # Skip if set
                continue
            if arg.has_constant_default():
                setattr(args, arg.name, arg.default)

        # Set functional default values (needs to be after everything)
        for arg in CODALAB_ARGUMENTS:
            if getattr(args, arg.name, None):  # Skip if set
                continue
            if arg.has_callable_default():
                setattr(args, arg.name, arg.default(args))

        return args


class CodalabServiceManager(object):
    SERVICE_IMAGES = ['server', 'frontend', 'worker']
    ALL_IMAGES = SERVICE_IMAGES + ['default-cpu', 'default-gpu']

    @staticmethod
    def resolve_env_vars(args):
        """Return environment with all the arguments (which might have originally come from the environment too)."""
        environment = {}
        for arg in CODALAB_ARGUMENTS:
            value = getattr(args, arg.name, None)
            if value:
                environment[arg.env_var] = str(value)
        # Additional environment variables to pass through
        for env_var in ['PATH', 'DOCKER_HOST']:
            if env_var in os.environ:
                environment[env_var] = os.environ[env_var]
        environment[
            'HOSTNAME'
        ] = socket.gethostname()  # Set HOSTNAME since it's sometimes not available
        return environment

    def __init__(self, args):
        self.args = args

        if self.args.version:
            self.args.version = clean_version(self.args.version)
        else:
            self.args.version = get_default_version()
        self.compose_cwd = os.path.join(BASE_DIR, 'docker_config', 'compose_files')

        self.compose_files = []
        self.compose_tempfile_name = ""
        if self.args.link_mounts:
            # We want to be able to mount a variable number of folders to the Docker container,
            # so we can't just use regular interpolation with environment variables. Instead,
            # we create a temporary file with the modified docker-compose.yml and use that file instead.
            with open(os.path.join(self.compose_cwd, 'docker-compose.yml')) as f:
                compose_options = yaml.safe_load(f)
            for mount_path in self.args.link_mounts.split(","):
                mount_path = os.path.abspath(mount_path)
                compose_options["x-codalab-server"]["volumes"].append(
                    f"{mount_path}:/opt/codalab-worksheets-link-mounts{mount_path}"
                )
            docker_compose_custom_path = os.path.join(
                self.args.codalab_home, 'docker-compose-custom.yml'
            )
            os.makedirs(os.path.dirname(docker_compose_custom_path), exist_ok=True)
            with open(docker_compose_custom_path, 'w+') as f:
                yaml.dump(compose_options, f)
                self.compose_tempfile_name = f.name
            self.compose_files.append(self.compose_tempfile_name)
        else:
            self.compose_files.append('docker-compose.yml')

        if self.args.dev:
            self.compose_files.append('docker-compose.dev.yml')
        if self.args.use_ssl:
            self.compose_files.append('docker-compose.ssl.yml')

        self.compose_env = self.resolve_env_vars(args)
        ensure_directory_exists(self.args.codalab_home)
        ensure_directory_exists(self.args.monitor_dir)
        ensure_directory_exists(self.args.worker_dir)
        ensure_directory_exists(self.args.mysql_mount)

    def execute(self):
        command = self.args.command
        if command == 'build':
            self.build_images()
        elif command == 'pull':
            self.pull_images()
        elif command == 'start':
            if self.args.build_images:
                self.build_images()
            self.start_services()
        elif command == 'run':
            self.run_service_cmd(self.args.service_command, service=self.args.service)
        elif command == 'logs':
            cmd_str = 'logs'
            if self.args.tail is not None:
                cmd_str += ' --tail %s' % self.args.tail
            if self.args.follow:
                cmd_str += ' -f'
            self._run_compose_cmd('logs')
        elif command == 'stop':
            self._run_compose_cmd('stop')
        elif command == 'delete':
            self._run_compose_cmd('down --remove-orphans -v')
        else:
            raise Exception('Bad command: ' + command)

    def build_image(self, image):
        print_header('Building {} image'.format(image))
        master_docker_image = 'codalab/{}:{}'.format(image, 'master')
        docker_image = 'codalab/{}:{}'.format(image, self.args.version)

        # Pull the previous image on this version (branch) if we have it.  Otherwise, use master.
        if self.args.pull:
            if self._run_docker_cmd('pull {}'.format(docker_image), allow_fail=True):
                cache_image = docker_image
            else:
                self._run_docker_cmd('pull {}'.format(master_docker_image))
                cache_image = master_docker_image
            cache_args = ' --cache-from {}'.format(cache_image)
        else:
            cache_args = ''

        if self.args.dev:
            build_args = ' --build-arg dev=true'
        else:
            build_args = ''

        # Build the image using the cache
        self._run_docker_cmd(
            'build%s %s -t %s -f docker_config/dockerfiles/Dockerfile.%s .'
            % (cache_args, build_args, docker_image, image)
        )

    def push_image(self, image):
        self._run_docker_cmd('push codalab/%s:%s' % (image, self.args.version))

    def pull_image(self, image):
        self._run_docker_cmd('pull codalab/%s:%s' % (image, self.args.version))

    def _run_docker_cmd(self, cmd, allow_fail=False):
        """Return whether the command succeeded."""
        command_string = 'docker ' + cmd
        print(command_string)
        if self.args.dry_run:
            success = True
        else:
            try:
                subprocess.check_call(command_string, shell=True, cwd=BASE_DIR)
                success = True
            except subprocess.CalledProcessError as e:
                success = False
                if not allow_fail:
                    raise e
        print('')
        return success

    def _run_compose_cmd(self, cmd):
        compose_files_str = ' '.join('-f ' + f for f in self.compose_files)
        command_string = 'docker-compose -p %s --project-directory %s %s %s' % (
            self.args.instance_name,
            self.compose_cwd,
            compose_files_str,
            cmd,
        )

        compose_env_string = ' '.join('{}={}'.format(k, v) for k, v in self.compose_env.items())
        print('(cd {}; {} {})'.format(self.compose_cwd, compose_env_string, command_string))
        if self.args.dry_run:
            success = True
        else:
            try:
                popen = subprocess.Popen(
                    command_string,
                    cwd=self.compose_cwd,
                    env=self.compose_env,
                    shell=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                )
                # Note: don't do `for stdout_line in popen.stdout` because that buffers.
                while True:
                    stdout_line = popen.stdout.readline()
                    if not stdout_line:
                        break
                    print("process: " + stdout_line.decode('utf-8'), end="")
                popen.wait()
                success = popen.returncode == 0
                if not success:
                    raise Exception('Command exited with code {}'.format(popen.returncode))
            except subprocess.CalledProcessError as e:
                print("CalledProcessError: {}, {}".format(str(e), e.output.decode('utf-8')))
                raise e
        print('')
        return success

    def bring_up_service(self, service):
        if should_run_service(self.args, service):
            print_header('Bringing up {}'.format(service))
            self._run_compose_cmd('up -d --no-deps %s' % service)

    def run_service_cmd(self, cmd, root=False, service='rest-server'):
        if root:
            uid = '0:0'
        else:
            uid = self.args.uid
        self._run_compose_cmd(
            ('run --no-deps --rm --user=%s ' % uid)
            + service
            + (
                ' bash -c \'%s\'' % cmd
            )  # TODO: replace with shlex.quote(cmd) once we're on Python 3
        )

    @staticmethod
    def wait(host, port, cmd):
        return '/opt/wait-for-it.sh --timeout={} {}:{} -- {}'.format(
            SERVICE_REQUEST_TIMEOUT_SECONDS, host, port, cmd
        )

    def wait_mysql(self, cmd):
        return self.wait(self.args.mysql_host, self.args.mysql_port, cmd)

    def wait_rest_server(self, cmd):
        return self.wait('rest-server', self.args.rest_port, cmd)

    def start_services(self):
        if self.args.protected_mode:
            print_header('Starting CodaLab services in protected mode...')

        mysql_url = 'mysql://{}:{}@{}:{}/{}'.format(
            self.args.mysql_username,
            self.args.mysql_password,
            self.args.mysql_host,
            self.args.mysql_port,
            self.args.mysql_database,
        )
        rest_url = 'http://rest-server:{}'.format(self.args.rest_port)

        if self.args.mysql_host == 'mysql':
            self.bring_up_service('mysql')

        self.bring_up_service('azurite')

        if should_run_service(self.args, 'init'):
            print_header('Populating config.json')
            commands = [
                'cl config {} {}'.format(config_prop, value)
                for config_prop, value in [
                    ('cli/default_address', rest_url),
                    ('server/engine_url', mysql_url),
                    ('server/rest_host', '0.0.0.0'),
                    ('server/rest_port', self.args.rest_port),
                    ('server/admin_email', self.args.admin_email),
                    ('server/support_email', self.args.support_email),  # Use support_email
                    ('server/default_user_info/disk_quota', self.args.user_disk_quota),
                    ('server/default_user_info/time_quota', self.args.user_time_quota),
                    (
                        'server/default_user_info/parallel_run_quota',
                        self.args.user_parallel_run_quota,
                    ),
                    ('email/host', self.args.email_host),
                    ('email/username', self.args.email_username),
                    ('email/password', self.args.email_password),
                ]
                if value
            ]
            self.run_service_cmd(' && '.join(commands))

            print_header('Initializing the database with alembic')
            # We need to upgrade the current database revision to the most recent revision before any other database operations.
            self.run_service_cmd(
                'if [ $(alembic current | wc -l) -gt 0 ]; then echo upgrade; alembic upgrade head; fi'
            )

            print_header('Creating root user')
            self.run_service_cmd(
                self.wait_mysql(
                    'python3 scripts/create-root-user.py {}'.format(self.args.codalab_password)
                )
            )

            print_header('Stamping the database with alembic')
            # We stamp the revision table with the given revision.
            self.run_service_cmd(
                'if [ $(alembic current | wc -l) -eq 0 ]; then echo stamp; alembic stamp head; fi'
            )

        if should_run_service(self.args, 'azurite'):
            # Run for local development with Azurite only
            print_header('Setting up Azurite')
            self.run_service_cmd('python3 scripts/initialize-azurite.py')

        self.bring_up_service('rest-server')

        if should_run_service(self.args, 'init'):
            print_header('Creating home and dashboard worksheets')
            self.run_service_cmd(
                self.wait_rest_server(
                    'cl logout && cl status && ((cl new home && cl new dashboard) || exit 0)'
                )
            )

        self.bring_up_service('bundle-manager')
        self.bring_up_service('frontend')
        self.bring_up_service('nginx')
        if self.args.shared_file_system:
            self.bring_up_service('worker-shared-file-system')
        else:
            self.bring_up_service('worker')

        self.bring_up_service('monitor')

        # Bring up the worker managers
        if self.args.worker_manager_type == 'azure-batch':
            self.bring_up_service('azure-batch-worker-manager-cpu')
            self.bring_up_service('azure-batch-worker-manager-gpu')
        elif self.args.worker_manager_type == 'aws-batch':
            self.bring_up_service('aws-batch-worker-manager-cpu')
            self.bring_up_service('aws-batch-worker-manager-gpu')
        else:
            print(
                'Worker manager type: {} is not supported. Skipping bringing up worker managers.'.format(
                    self.args.worker_manager_type
                )
            )

    def pull_images(self):
        for image in self.SERVICE_IMAGES:
            self.pull_image(image)

    def build_images(self):
        images_to_build = [
            image for image in self.ALL_IMAGES if should_build_image(self.args, image)
        ]

        for image in images_to_build:
            self.build_image(image)

        if self.args.push:
            self._run_docker_cmd(
                'login --username {} --password {}'.format(
                    self.args.docker_username, self.args.docker_password
                )
            )
            for image in images_to_build:
                self.push_image(image)


if __name__ == '__main__':
    main()
