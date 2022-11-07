from codalab.lib.editor_util import find_default_editor
from codalab.lib.spec_util import generate_uuid
from codalab.lib.codalab_manager import CodaLabManager

import docker
import os
import shutil
import sys
from typing import Dict, List


class InteractiveSession:
    """
    Creates an interactive session by launching a Docker container with the dependencies mounted. Users can run
    commands and interact with the environment. When the user exits the interactive session, an editor pops up
    with the list of all the commands that were ran during the interactive session. The user can delete any
    extraneous commands and do any editing (e.g., remove commands that causes errors or add additional commands).
    When the editing is done, the official command for the bundle gets constructed.

    Workflow:

        1. The user starts an interactive session with the desired Docker image and dependencies.
        2. Create a Docker container with the specified image on the client machine for the interactive session.
        3. Mount the dependencies as read-only.
        4. Set working directory to some arbitrary path: `/<session uuid>`
        5. The user will interact with this container and try out different commands. Once satisfied the user will
           exit the bash session. All the commands the user tried out will be stored at path `/usr/sbin/.bash_history`
           in the container.
        6. Copy `.bash_history` out to the host machine.
        7. Open an editor and allow the user to select and edit commands for the official run.
        8. Return the final command.
        9. Stop and remove the interactive session container.
    """

    _BASH_HISTORY_CONTAINER_PATH = "/usr/sbin/.bash_history"
    _CHOOSE_COMMAND_INSTRUCTIONS = (
        "\n\n#\n"
        "# Choose the commands to use for cl run:\n"
        "#\n"
        "# 1. Delete the lines of any unwanted commands. If you don't want to create the bundle, just remove all commands.\n"
        "# 2. Add any additional commands on a new line above these instructions.\n"
        "#\n"
    )
    _INSTRUCTIONS_DELIMITER = '#'
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
        initial_command="",
        manager=None,
        dependencies=[],
        bundle_locations={},
        verbose=False,
        stdout=sys.stdout,
        stderr=sys.stderr,
    ):
        # Instantiate a CodaLabManager if one is not passed in
        self._manager = manager if manager else CodaLabManager()
        self._docker_image = docker_image
        self._initial_command = initial_command

        InteractiveSession._validate_bundle_locations(bundle_locations, dependencies)
        self._dependencies = dependencies
        self._bundle_locations = bundle_locations

        self._docker_client = docker.from_env(timeout=InteractiveSession._MAX_SESSION_TIMEOUT)
        self._session_uuid = generate_uuid()
        self._host_bundle_path: str = os.path.join(
            self._manager.codalab_home, 'local_bundles', self._session_uuid
        )

        self._verbose = verbose
        self._stdout = stdout
        self._stderr = stderr

    def start(self):
        os.makedirs(self._host_bundle_path)

        # Create a blank file which will be used as the bash history file that will later be
        # mounted and populated during the interactive session.
        self._host_bash_history_path = os.path.join(self._host_bundle_path, ".bash_history")
        open(self._host_bash_history_path, 'w').close()

        run_command = self.get_docker_run_command()

        if self._verbose:
            print('\nStarting an interactive session...', file=self._stdout)
            print('%s\n' % run_command, file=self._stdout)
            print('=' * 150, file=self._stdout)
            print('Session UUID: ', self._session_uuid, file=self._stdout)
            print('CodaLab instance: ', self._manager.current_client().address, file=self._stdout)
            print('Container name: ', self._get_container_name(), file=self._stdout)
            print('Container Docker image: ', self._docker_image, file=self._stdout)
            print(
                'You can find local bundle contents at: ', self._host_bundle_path, file=self._stdout
            )
            print('=' * 150 + '\n', file=self._stdout)

        self._container = self._start_session(run_command)
        return self._construct_final_command()

    def get_docker_run_command(self):
        """
        Constructs the Docker run command used to start the interactive session.

        TODO: The logic in this method is similar to the dependencies mounting logic in worker_run_state.py.
              This code needs to be updated if there are any changes to the mounting logic in the worker code.
              Also, we should refactor in order to reduce the redundancy with the worker code.

        :return: The command as a string
        """

        def get_docker_path(sub_path):
            return os.path.sep + os.path.join(self._session_uuid, sub_path)

        # Use a dict to keep track of volumes to mount. The key is the path on Docker and the value is the local path.
        volumes: Dict[str, str] = {}
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

        name: str = self._get_container_name()
        container_working_directory: str = f'{os.path.sep}{self._session_uuid}'

        # Start a container as a non-root user
        command: List[str] = [
            'docker run',
            '-it',
            f'--name {name}',
            f'-w {container_working_directory}',
            f'-e HOME={container_working_directory}',
            f'-e HISTFILE={InteractiveSession._BASH_HISTORY_CONTAINER_PATH}',
            '-e PROMPT_COMMAND="history -a"',
            '-u $(id -u):$(id -g)',
        ]
        command.extend(
            [
                # Example: -v local_path/some_folder:/0x707e903500e54bcf9b072ac7e3f5ed36_dependencies/foo:ro
                f'-v {local_path}:{docker_path}:ro'
                for docker_path, local_path in volumes.items()
            ]
        )
        command.extend(
            [
                f'-v {self._host_bash_history_path}:{InteractiveSession._BASH_HISTORY_CONTAINER_PATH}:rw',
                f'-v {self._host_bundle_path}:{container_working_directory}:rw',
            ]
        )
        command.append(self._docker_image)
        return ' '.join(command)

    def cleanup(self):
        if self._verbose:
            print('\nCleaning up the session...', file=self._stdout)

        self._container.stop()
        self._container.remove()
        shutil.rmtree(self._host_bundle_path, ignore_errors=True)

        if self._verbose:
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
        except Exception as e:
            print(
                'The history of bash commands could not be retrieved at path {}: {}'.format(
                    InteractiveSession._BASH_HISTORY_CONTAINER_PATH, e
                ),
                file=self._stderr,
            )
            return ''

        # If a user passed in an initial command, prepend it to list of possible commands to choose from
        if self._initial_command:
            candidate_commands.insert(0, self._initial_command + '\n')

        # Write out the commands to choose from and the instructions out to a file
        path = os.path.join(self._host_bundle_path, 'edit_commands.txt')
        with open(path, 'w') as f:
            for command in candidate_commands:
                f.write(command)
            f.write(InteractiveSession._CHOOSE_COMMAND_INSTRUCTIONS)

        # Open an editor to allow users to choose commands
        os.system('{} {}'.format(find_default_editor(), path))

        # Extract out the final commands minus the instructions
        commands = []
        for line in open(path).read().splitlines():
            command = line.lstrip().rstrip()
            if command and not command.startswith(InteractiveSession._INSTRUCTIONS_DELIMITER):
                commands.append(command)

        final_command = '\n'.join(commands)
        if final_command:
            print('\nFinal constructed command:\n{}\n'.format(final_command), file=self._stdout)
        return final_command

    def _get_bash_history(self):
        # Extract out a list of commands from .bash_history
        commands = []
        with open(self._host_bash_history_path) as f:
            for i, line in enumerate(f):
                command = (
                    line.rstrip(InteractiveSession._NULL_BYTE)
                    if i > 0
                    else line.split(InteractiveSession._NULL_BYTE)[-1]
                )
                if command:
                    commands.append(command)
        return commands
