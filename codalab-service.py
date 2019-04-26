import argparse
import os
import subprocess
from test_cli import TestModule


def define_commands():
    parser = argparse.ArgumentParser(description="Manages your local CodaLab worksheets back-end deployment using docker-compose")
    subparsers = parser.add_subparsers(dest='command', description='Command to run', required=True)

    # SUBCOMMANDS

    start_cmd = subparsers.add_parser('start', aliases=['s'], description='Start a CodaLab backend instance')
    logs_cmd = subparsers.add_parser('logs', aliases=['l'], description='View logs for existing CodaLab instance')
    tests_cmd = subparsers.add_parser('test', description='Run tests against an existing CodaLab instance')
    build_cmd = subparsers.add_parser('build', aliases=['b'], description='Build CodaLab docker images using the local codebase')

    subparsers.add_parser('stop', description='Stop any existing CodaLab backend instance')
    subparsers.add_parser('down', description='Bring down any existing CodaLab backend instance')
    subparsers.add_parser('restart', description='Restart any existing CodaLab backend instance')

    #  BUILD SETTINGS

    for cmd in [build_cmd, start_cmd]:
        cmd.add_argument('--version', type='str', description='CodaLab version to use for building and deployment', default='latest')
        cmd.add_argument('--dev', action='store_true', description='If specified use dev versions of images',)
        cmd.add_argument('--push', action='store_true', description='If specified push the images to Dockerhub',)
        cmd.add_argument('--docker-user', type='str', description='DockerHub username to push images from', default=None)
        cmd.add_argument('--docker-pwd', type='str', description='DockerHub password to push images from', default=None)

    #  DEPLOYMENT SETTINGS

    start_cmd.add_argument('--build-locally', '-b', action='store_true', description='If specified build VERSION using local code.')
    start_cmd.add_argument('--user-compose-file', type='str', description='If specified path to a user-defined Docker compose file that overwrites the defaults', default=None)
    start_cmd.add_argument('--start-worker', '-w', action='store_true', description='If specified start a CodaLab worker on this machine.')
    start_cmd.add_argument('--initial-config', '-i', action='store_true', description='If specified, save the initial configuration of the instance (defaults to true if the service home or the database mounts are ephemeral)')

    #  USER CREDENTIALS

    start_cmd.add_argument('--mysql-root-pwd', type='str', description='Root password for the database', default='mysql_root_pwd')
    start_cmd.add_argument('--mysql-user', type='str', description='MYSQL username for the Codalab MYSQL client', default='codalab')
    start_cmd.add_argument('--mysql-pwd', type='str', description='MYSQL password for the Codalab MYSQL client', default='mysql_pwd')
    start_cmd.add_argument('--root-user', type='str', description='Codalab username for the Codalab admin user', default='codalab')
    start_cmd.add_argument('--root-pwd', type='str', description='Codalab password for the Codalab admin user', default='testpassword')

    #  HOST FILESYSTEM MOUNTS

    start_cmd.add_argument('--uid', type='str', description='Linux UID that owns the files created by Codalab. default=(ID of the user running this script)', default=None)
    start_cmd.add_argument('--service-home', type='str', description='Path on the host machine to store home directory of the Codalab server (by default nothing is stored', default=None)
    start_cmd.add_argument('--mysql-mount', type='str', description='Path on the host machine to store mysql data files, by default the database is ephemeral', default=None)
    start_cmd.add_argument('--worker-dir', type='str', description='Path on the host machine to store worker data files, by default these are ephemeral', default=None)
    start_cmd.add_argument('--bundle-store', type='str', description='Path on the host machine to store bundle data files, by default these are ephemeral', default=[], dest='bundle_stores', action='append')

    start_cmd.add_argument('--worker-docker-network-name', type='str', description='Name of the docker network that includes the worker and runs', default='codalab-worker-network')

    #  HOST PORT MOUNTS

    start_cmd.add_argument('--http-port', type='str', description='HTTP port for the server to listen on', default='80')
    start_cmd.add_argument('--rest-port', type='str', description='Port for the REST server to listen on (by default it is not exposed to the host machine)', default=None)
    start_cmd.add_argument('--frontend-port', type='str', description='Port for the React server to listen on (by default it is not exposed to the host machine)', default=None)
    start_cmd.add_argument('--mysql-port', type='str', description='Port for the MYSQL database to listen on (by default it is not exposed to the host machine)', default=None)

    #  SSL CONFIGURATION

    start_cmd.add_argument('--use-ssl', action='store_true', description='If specified set the server up with SSL')
    start_cmd.add_argument('--ssl-cert-file', type='str', description='Path to the cert file for SSL')
    start_cmd.add_argument('--ssl-key-file', type='str', description='Path to the key file for SSL')

    #  LOGS SETTINGS

    logs_cmd.add_argument('services', nargs='*', default='all', help='Services to print logs for', choices=['mysql', 'rest-server', 'bundle-manager', 'frontend', 'nginx', 'worker', 'all'])
    logs_cmd.add_argument('--follow', '-f', action='store_true', help='If specified follow the logs')
    logs_cmd.add_argument('--tail', '-t', type=int, default=None, help='If specified tail TAIL lines from the ends of each log')

    #  TESTS SETTINGS

    tests_cmd.add_argument('tests', metavar='TEST', nargs='+', type=str, choices=TestModule.modules.keys() + ['all', 'default'], help='Tests to run')
    return parser


def parse_args():
    parser = define_commands()
    args = parser.parse_args()
    args.root_dir = os.path.dirname(os.path.realpath(__file__))
    # TODO: Reconcile this with environment variables
    return args


def main():
    args = parse_args()
    service_manager = CodalabServiceManager(args)
    service_manager.execute()

class CodalabServiceManager(object):

    @staticmethod
    def resolve_env_vars(args):
        environment = {
            'CODALAB_UID': args.codalab_uid,
            'CODALAB_MYSQL_ROOT_PWD': args.mysql_root_pwd,
            'CODALAB_MYSQL_USER': args.mysql_user,
            'CODALAB_MYSQL_PWD': args.mysql_pwd,
            'CODALAB_ROOT_USER': args.root_user,
            'CODALAB_ROOT_PWD': args.root_pwd,
            'CODALAB_HTTP_PORT': args.http_port,
            'CODALAB_VERSION': args.codalab_version,
        }
        if args.mount_home:
            environment['CODALAB_SERVICE_HOME'] = args.service_home
        if args.mysql_mount:
            environment['CODALAB_MYSQL_MOUNT'] = args.mysql_mount
        if args.start_worker:
            environment['CODALAB_WORKER_NETWORK_NAME'] = args.worker_docker_network_name
            if args.worker_dir:
                environment['CODALAB_WORKER_DIR'] = args.worker_dir
        if args.rest_port:
            environment['CODALAB_REST_PORT'] = args.rest_port
        if args.frontend_port:
            environment['CODALAB_FRONTEND_PORT'] = args.frontend_port
        if args.mysql_port:
            environment['CODALAB_MYSQL_PORT'] = args.mysql_port
        if args.use_ssl:
            environment['CODALAB_SSL_CERT_FILE'] = args.ssl_cert_file
            environment['CODALAB_SSL_KEY_FILE'] = args.ssl_key_file
        return environment

    @staticmethod
    def resolve_compose_files(args):
        compose_files = ['docker_compose.yml']
        if args.dev:
            compose_files.append('docker_compose.dev.yml')
        if args.mount_home:
            compose_files.append('docker_compose.home_mount.yml')
        if args.mysql_mount:
            compose_files.append('docker_compose.mysql_mount.yml')
        if args.bundle_stores:
            compose_files.append('docker_compose.bundle_mounts.yml')
        if args.start_worker and args.worker_dir:
            compose_files.append('docker_compose.worker_mount.yml')
        if args.rest_port:
            compose_files.append('docker_compose.rest_port.yml')
        if args.frontend_port:
            compose_files.append('docker_compose.frontend_port.yml')
        if args.mysql_port:
            compose_files.append('docker_compose.mysql_port.yml')
        if args.use_ssl:
            compose_files.append('docker_compose.ssl.yml')
        if args.user_compose_file:
            compose_files.append(args.user_compose_file)
        return compose_files

    def __init__(self, args):
        self.args = args
        self.root_dir = args.root_dir
        self.command = args.command
        self.compose_cwd = os.path.join(self.root_dir, 'docker', 'compose_files')
        self.compose_files = self.resolve_compose_files(args)
        self.compose_env = self.resolve_env_vars(args)

    def execute(self):
        if self.command == 'build' or (self.command == 'start' and self.args.build_locally):
            self.build()
        if self.command == 'start':
            self.start_service()
        elif self.command == 'restart':
            pass
        elif self.command == 'logs':
            pass
        elif self.command == 'test':
            pass
        elif self.command == 'stop':
            pass
        elif self.command == 'down':
            pass

    def _run_docker_cmd(self, cmd):
        subprocess.check_call(['docker'] + cmd, shell=True, cwd=self.root_dir)

    def build_image(self, image, dockerfile):
        print("[CODALAB] ==> Building %s image " % image)
        self._run_docker_cmd(['build', '--cache-from', 'codalab/%s' % image, '-t', 'codalab/%s:%s' % (image, self.args.version), '-f', 'docker/dockerfiles/Dockerfile.%s' % dockerfile, '.'])

    def push_image(self, image):
        self._run_docker_cmd(['push', 'codalab/%s:%s' % (image, self.args.version)])

    def _run_compose_cmd(self, cmd):
        subprocess.check_call(['docker-compose'] + ' -f '.join(self.compose_files) + cmd, cwd=self.compose_cwd, env=self.compose_env, shell=True)

    def bring_up_service(self, service):
        self._run_compose_cmd(['up', '-d', '--no-deps', '--no-recreate', service])

    def run_service_cmd(self, cmd, root=False, service='rest-server'):
        if root:
            uid = '0:0'
        else:
            uid = self.compose_env['CODALAB_UID']
        self._run_compose_cmd(['run', '--no-deps', '--rm', '--entrypoint=""', '--user=%s' % uid, service, 'bash', '-c', '"%s"' % cmd])

    def start_service(self):
        print("[CODALAB] ==> Starting MySQL")
        self.bring_up_service('mysql')

        print("[CODALAB] ==> Configuring the service")
        self.run_service_cmd("data/bin/wait-for-it.sh mysql:3306 -- /opt/codalab-worksheets/codalab/bin/cl config server/engine_url mysql://$CODALAB_MYSQL_USER:$CODALAB_MYSQL_PWD@mysql:3306/codalab_bundles && /opt/codalab-worksheets/codalab/bin/cl config cli/default_address http://rest-server:$CODALAB_REST_PORT && /opt/codalab-worksheets/codalab/bin/cl config server/rest_host 0.0.0.0")

        if self.args.initial_config:
            print("[CODALAB] ==> Creating root user")
            self.run_service_cmd("/opt/codalab-worksheets/venv/bin/pip install /opt/codalab-worksheets && data/bin/wait-for-it.sh mysql:3306 -- opt/codalab-worksheets/venv/bin/python /opt/codalab-worksheets/scripts/create-root-user.py $CODALAB_ROOT_PWD", root=True)

        print("[CODALAB] ==> Starting rest server")
        self.bring_up_service('rest-server')

        if args.initial_config:
            print("[CODALAB] ==> Creating initial worksheets")
            self.run_service_cmd("data/bin/wait-for-it.sh rest-server:$CODALAB_REST_PORT -- opt/codalab-worksheets/codalab/bin/cl logout && /opt/codalab-worksheets/codalab/bin/cl new home && /opt/codalab-worksheets/codalab/bin/cl new dashboard")

        print("[CODALAB] ==> Starting bundle manager")
        self.bring_up_service('bundle-manager')
        print("[CODALAB] ==> Starting frontend")
        self.bring_up_service('frontend')
        print("[CODALAB] ==> Starting nginx")
        self.bring_up_service('nginx')
        if args.start_worker:
            print("[CODALAB] ==> Starting worker")
            self.bring_up_service('worker')

    def build(self):
        print("[CODALAB] => Building Docker images")
        self.build_image('bundleserver', 'server')
        self.build_image('frontend', 'frontend')
        self.build_image('worker', 'worker')
        self.build_image('default-cpu', 'cpu')
        self.build_image('default-gpu', 'gpu')
        if self.args.push:
            self._run_docker_cmd(['login', '-u', self.args.docker_user, '-p', self.args.docker_pwd])
            self.push_image('bundleserver')
            self.push_image('frontend')
            self.push_image('worker')
            self.push_image('default-cpu')
            self.push_image('default-gpu')
