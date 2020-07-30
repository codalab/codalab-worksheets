from codalab.lib.spec_util import generate_uuid
from codalab.lib.codalab_manager import CodaLabManager

import argparse
import docker
import os
import shutil
import sys


class InteractiveSession:
    """
    Creates an interactive session by launching a Docker container with the dependencies mounted. Users can run
    commands and interact with the environment. When the user exits the interactive session, an editor pops up
    with the list of all the commands that were ran during the interactive session. The user can delete any
    extraneous commands and do any editing (e.g., remove commands that causes errors or add additional commands).
    When the editing is done, the official command for the bundle gets constructed.
    """

    _BASH_HISTORY_CONTAINER_PATH = '/root/.bash_history'
    _CHOOSE_COMMAND_INSTRUCTIONS = (
        "\n\n@@\n"
        "@@ Choose the commands to use for cl run:\n"
        "@@\n"
        "@@ 1. Delete the lines of any unwanted commands.\n"
        "@@ 2. Add any additional commands on a new line above these instructions.\n"
        "@@\n"
    )
    _INSTRUCTIONS_DELIMITER = '@@'
    _MAX_SESSION_TIMEOUT = 8 * 60 * 60  # 8 hours in seconds
    _NULL_BYTE = '\x00'

    @staticmethod
    def _validate_bundle_locations(bundle_locations, dependencies):
        for _, bundle_target in dependencies:
            if bundle_target.bundle_uuid not in bundle_locations:
                raise RuntimeError(
                    'Missing bundle location for bundle uuid: %s' % bundle_target.bundle_uuid
                )

    def __init__(
        self,
        docker_image,
        manager=None,
        dependencies=[],
        bundle_locations={},
        stdout=sys.stdout,
        stderr=sys.stderr,
    ):
        # Instantiate a CodaLabManager if one is not passed in
        self._manager = manager if manager else CodaLabManager()
        self._docker_image = docker_image

        InteractiveSession._validate_bundle_locations(bundle_locations, dependencies)
        self._dependencies = dependencies
        self._bundle_locations = bundle_locations

        self._docker_client = docker.from_env(timeout=InteractiveSession._MAX_SESSION_TIMEOUT)
        self._session_uuid = generate_uuid()
        self._stdout = stdout
        self._stderr = stderr

    def start(self):
        self._bundle_path = os.path.join(
            self._manager.codalab_home, 'local_bundles', self._session_uuid
        )
        os.makedirs(self._bundle_path)

        run_command = self.get_docker_run_command()
        print('\nStarting an interactive session...', file=self._stdout)
        print('%s\n' % run_command, file=self._stdout)
        print('=' * 150, file=self._stdout)
        print('Session UUID: ', self._session_uuid, file=self._stdout)
        print('CodaLab instance: ', self._manager.current_client().address, file=self._stdout)
        print('Container name: ', self._get_container_name(), file=self._stdout)
        print('Container Docker image: ', self._docker_image, file=self._stdout)
        print('You can find local bundle contents at: ', self._bundle_path, file=self._stdout)
        print('=' * 150 + '\n', file=self._stdout)

        self._container = self._start_session(run_command)
        return self._construct_final_command()

    def get_docker_run_command(self):
        def get_docker_path(sub_path):
            return os.path.sep + os.path.join(self._session_uuid, sub_path)

        # Use a dict to keep track of volumes to mount. The key is the path on Docker and the value is the local path.
        volumes = {}
        for key, bundle_target in self._dependencies:
            dependency_local_path = os.path.realpath(
                os.path.join(
                    self._bundle_locations[bundle_target.bundle_uuid], bundle_target.subpath
                )
            )
            if key == '.':
                if not os.path.isdir(dependency_local_path):
                    raise RuntimeError(
                        'Key value . is not compatible with non-directories: %s'
                        % dependency_local_path
                    )

                for child in os.listdir(dependency_local_path):
                    volumes[get_docker_path(child)] = os.path.join(dependency_local_path, child)
            else:
                volumes[get_docker_path(key)] = dependency_local_path

        name = self._get_container_name()
        command = ['docker run', '-it', f'--name {name}', f'-w {os.path.sep}{self._session_uuid}']
        command.extend(
            [
                # Example: -v local_path/some_folder:/0x707e903500e54bcf9b072ac7e3f5ed36_dependencies/foo:ro
                '-v {}:{}:ro'.format(local_path, docker_path)
                for docker_path, local_path in volumes.items()
            ]
        )
        command.append(self._docker_image)
        command.append('bash')
        return ' '.join(command)

    def cleanup(self):
        print('\nCleaning up the session...', file=self._stdout)
        self._container.stop()
        self._container.remove()
        shutil.rmtree(self._bundle_path, ignore_errors=True)
        print('Done.\n', file=self._stdout)

    def _start_session(self, docker_command):
        # Create a Docker container for this interactive session
        os.system(docker_command)

        # Find the newly created Docker container by name and return it
        name = self._get_container_name()
        containers = self._docker_client.containers.list(all=True, filters={'name': name})
        if len(containers) == 0:
            raise RuntimeError('Could not find interactive session container with name: %s' % name)
        return containers[0]

    def _get_container_name(self):
        return 'interactive-session-%s' % self._session_uuid

    def _construct_final_command(self):
        try:
            candidate_commands = self._get_bash_history()
        except:
            print(
                'The history of bash commands could not be retrieved at path: %s'
                % InteractiveSession._BASH_HISTORY_CONTAINER_PATH,
                file=self._stderr,
            )
            return ''

        # Write out the commands to choose from and the instructions out to a file
        path = os.path.join(self._bundle_path, 'edit_commands.txt')
        with open(path, 'w') as f:
            for command in candidate_commands:
                f.write(command)
            f.write(InteractiveSession._CHOOSE_COMMAND_INSTRUCTIONS)

        # Use vi to allow users to choose commands
        os.system('vi %s' % path)

        # Extract out the final commands minus the instructions
        commands = []
        for line in open(path).read().splitlines():
            command = line.lstrip().rstrip()
            if command and not command.startswith(InteractiveSession._INSTRUCTIONS_DELIMITER):
                commands.append(command)

        final_command = '&&'.join(commands)
        print('\nFinal constructed command:\n%s' % final_command, file=self._stdout)
        return final_command

    def _get_bash_history(self):
        # Copies out .bash_history from the container to bundle_path
        path = os.path.join(self._bundle_path, '.bash_history')
        f = open(path, 'wb')
        stream, _ = self._container.get_archive(InteractiveSession._BASH_HISTORY_CONTAINER_PATH)
        for chunk in stream:
            f.write(chunk)
        f.close()

        # Extract out a list of commands from the .bash_history file
        commands = []
        with open(path) as f:
            for i, line in enumerate(f):
                command = (
                    line.rstrip(InteractiveSession._NULL_BYTE)
                    if i > 0
                    else line.split(InteractiveSession._NULL_BYTE)[-1]
                )
                if command:
                    commands.append(command)
        return commands


def main():
    # Example usage of InteractiveSession
    session = InteractiveSession(
        args.docker_image, manager=CodaLabManager(), dependencies=[], bundle_locations={}
    )
    session.start()
    session.cleanup()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Runs the specified CodaLab stress tests against the specified CodaLab instance (defaults to localhost).'
    )
    parser.add_argument(
        '--docker-image',
        type=str,
        help='Docker image used to create a container for the interactive session.',
        default='codalab/default-cpu:latest',
    )

    # Parse args and run this script
    args = parser.parse_args()
    main()
