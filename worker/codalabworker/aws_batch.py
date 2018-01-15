import fsm
import time
import os


from contextlib import closing
import httplib
import logging
import os
import socket
import threading
import time
import traceback

from bundle_service_client import BundleServiceException
from docker_client import DockerException
from download_util import BUNDLE_NO_LONGER_RUNNING_MESSAGE, get_target_info, get_target_path, PathException
from file_util import get_path_size, gzip_file, gzip_string, read_file_section, summarize_file, tar_gzip_directory, remove_path
from formatting import duration_str, size_str

try:
    import boto3
except ImportError:
    print("Missing dependencies, please install boto3 to enable AWS support.")
    import sys

    sys.exit(1)


def parse_int(to_parse, default_value):
    try:
        return int(to_parse)
    except (ValueError, TypeError):
        return default_value


class AwsBatchRun(object):
    """
    This class manages a single run on AWS Batch.
    This is similar to the standard Run, but all execution happens in a Batch job rather than locally on the worker.

    It also has the added constraint for the shared file system option, that the file system must be shared properly
    on the compute cluster which is serving the Batch queue.
    For an article on how to achieve this, see:

    """
    def __init__(self, bundle_service, batch_client, worker, bundle, bundle_path, resources, state=None):
        batch_client = batch_client if batch_client is not None else boto3.client('batch')
        self._bundle_service = bundle_service
        self._batch_client = batch_client
        self._worker = worker
        self._bundle = bundle
        self._uuid = bundle['uuid']
        self._bundle_path = bundle_path
        self._resources = resources
        # TODO Add a cleanup state which is used when exceptions are thrown from anything
        state = state if state is not None else \
            Initial(bundle=bundle, batch_client=batch_client, worker=worker, bundle_service=bundle_service,
                    bundle_path=bundle_path)
        self._fsm = fsm.ThreadedFiniteStateMachine(state, sleep_time=5.0)
        self._dep_paths = []  # TODO Do this for deps somehow

    def run(self):
        self._fsm.thread.start()
        return True

    def resume(self):
        # TODO Do we need to do anything special here? We already deserialized to the correct state presumably
        self._fsm.thread.start()
        return True

    def read(self, socket_id, path, read_args):
        def reply_error(code, message):
            message = {
                'error_code': code,
                'error_message': message,
            }
            self._bundle_service.reply(self._worker.id, socket_id, message)

        try:
            read_type = read_args['type']
            if read_type == 'get_target_info':
                # At the top-level directory, we should ignore dependencies.
                if path and os.path.normpath(path) in self._dep_paths:
                    target_info = None
                else:
                    try:
                        target_info = get_target_info(
                            self._bundle_path, self._uuid, path, read_args['depth'])
                    except PathException as e:
                        reply_error(httplib.BAD_REQUEST, e.message)
                        return

                    if not path and read_args['depth'] > 0:
                        target_info['contents'] = [
                            child for child in target_info['contents']
                            if child['name'] not in self._dep_paths]

                self._bundle_service.reply(self._worker.id, socket_id,
                                           {'target_info': target_info})
            else:
                try:
                    final_path = get_target_path(self._bundle_path, self._uuid, path)
                except PathException as e:
                    reply_error(httplib.BAD_REQUEST, e.message)
                    return

                if read_type == 'stream_directory':
                    if path:
                        exclude_names = []
                    else:
                        exclude_names = self._dep_paths
                    with closing(tar_gzip_directory(final_path, exclude_names=exclude_names)) as fileobj:
                        self._bundle_service.reply_data(self._worker.id, socket_id, {}, fileobj)
                elif read_type == 'stream_file':
                    with closing(gzip_file(final_path)) as fileobj:
                        self._bundle_service.reply_data(self._worker.id, socket_id, {}, fileobj)
                elif read_type == 'read_file_section':
                    string = gzip_string(read_file_section(
                        final_path, read_args['offset'], read_args['length']))
                    self._bundle_service.reply_data(self._worker.id, socket_id, {}, string)
                elif read_type == 'summarize_file':
                    string = gzip_string(summarize_file(
                        final_path, read_args['num_head_lines'],
                        read_args['num_tail_lines'], read_args['max_line_length'],
                        read_args['truncation_text']))
                    self._bundle_service.reply_data(self._worker.id, socket_id, {}, string)
        except BundleServiceException:
            traceback.print_exc()
        except Exception as e:
            traceback.print_exc()
            reply_error(httplib.INTERNAL_SERVER_ERROR, e.message)

    def write(self, subpath, string):
        # Make sure you're not trying to write over a dependency.
        if os.path.normpath(subpath) in self._dep_paths:
            return

        # Do the write.
        with open(os.path.join(self._bundle_path, subpath), 'w') as f:
            f.write(string)

    def kill(self, message):
        self._fsm.send_input(event(Event.Kill))

    def serialize(self):
        """ Output a dictionary able to be serialized into json """
        run_info = {
            'bundle': self._bundle,
            'bundle_path': self._bundle_path,
            'resources': self._resources,
            'state': self._fsm._state.__class__.__name__  # TODO Make states serializable
        }
        return run_info

    @staticmethod
    def deserialize(bundle_service, docker, image_manager, worker, run_info):
        """ Create a new Run object and populate it based on given run_info dictionary """

        state_class = globals().get(run_info.get('state', Initial.__class__.__name__))
        state = state_class(bundle=run_info['bundle'],
                            batch_client=boto3.client('batch'),
                            worker=worker,
                            bundle_service=bundle_service,
                            bundle_path=run_info['bundle_path'])

        run = AwsBatchRun(bundle_service=bundle_service,
                          batch_client=None,
                          worker=worker,
                          bundle=run_info['bundle'],
                          bundle_path=run_info['bundle_path'],
                          resources=run_info['resources'],
                          state=state)
        return run

    @property
    def requested_memory_bytes(self):
        """
        If request_memory is defined, then return that.
        Otherwise, this run's memory usage does not get checked, so return inf.
        """
        return self._resources.get('request_memory') or float('inf')


def event(name, **payload):
    return {
        'name': name,
        'payload': payload
    }


class Event(object):
    UPDATE_METADATA = 'update_metadata'
    FAILED = 'failed'
    SUCCESS = 'success'
    Kill = 'kill'


class AwsBatchRunState(fsm.State):
    def __init__(self, bundle, batch_client, worker, bundle_service, bundle_path):
        self._bundle = bundle
        self._batch_client = batch_client if batch_client else boto3.client('batch')
        self._worker = worker
        self._bundle_service = bundle_service
        self._bundle_path = bundle_path
        self._logger = logging.getLogger(self.uuid)

    @property
    def logger(self):
        return self._logger

    @property
    def uuid(self):
        return self._bundle['uuid']

    @property
    def batch_queue(self):
        # TODO Get this from somewhere meaningful, probably the worker
        return 'scarecrow-training'

    @property
    def is_shared_file_system(self):
        return self._worker.shared_file_system

    @property
    def metadata(self):
        return self._bundle['metadata']

    @property
    def name(self):
        return self.__class__.__name__

    def transition(self, NewState, outputs=None):
        status_event = event(Event.UPDATE_METADATA, run_status=self.name, last_updated=int(time.time()))
        outputs = outputs + [status_event] if outputs is not None else [status_event]
        new_state = NewState(bundle=self._bundle, batch_client=self._batch_client, worker=self._worker,
                             bundle_service=self._bundle_service, bundle_path=self._bundle_path)
        self.logger.info("[%] Transitioning %s -> %s", self.uuid, self.name, new_state.name)
        return new_state, outputs

    def noop(self):
        return self, []

    def update_metadata(self, **kwargs):
        self.logger.debug("Updating metadata: %s", kwargs)
        # Update the bundle locally
        self._bundle['metadata'].update(kwargs)
        # Update the bundle on the bundle service
        self._bundle_service.update_bundle_metadata(self._worker.id, self._bundle['uuid'], kwargs)

    # TODO Support this
    def should_kill(self, events):
        return any([e['name'] == Event.Kill for e in events])


# TODO If there is really nothing to do here, then just go straight to setup
class Initial(AwsBatchRunState):
    def update(self, events):
        transition = None
        # If this job has previously made some progress, then pickup where we left off
        if self.metadata.get('batch_job_id'):
            transition = self.transition(Running)
        elif self.metadata.get('batch_job_definition'):
            transition = self.transition(Submit)
        else:
            transition = self.transition(Setup)

        return transition

class Setup(AwsBatchRunState):
    def update(self, events):
        # TODO Create the <bundle_uuid>_dependencies folder with either downloaded or symlinked deps
        dependencies = self.setup_dependencies()

        # Create job definition
        job_definition = self.create_job_definition(dependencies)

        self.update_metadata(batch_job_definition=job_definition)
        return self.transition(Submit)

    @property
    def docker_image(self):
        return self._bundle['metadata']['docker_image']

    @property
    def docker_working_directory(self):
        return '/' + self.uuid

    @property
    def docker_dependencies_directory(self):
        return '/' + self.uuid + '_dependencies'

    @property
    def shared_batch_efs_directory(self):
        # TODO This is the filesystem shared between the worker and the compute cluster. For now hard-code
        return '/data/codalab-home/bundles'

    def setup_dependencies(self):
        # TODO This need careful testing/review
        dependencies = []
        for dep in self._bundle['dependencies']:
            child_path = os.path.normpath(os.path.join(self._bundle_path, dep['child_path']))
            if not child_path.startswith(self._bundle_path):
                raise Exception('Invalid key for dependency: %s' % (
                    dep['child_path']))

            if self.is_shared_file_system:
                parent_bundle_path = dep['location']

                # Check that the dependency is valid (i.e. points inside the
                # bundle and isn't a broken symlink).
                parent_bundle_path = os.path.realpath(parent_bundle_path)
                dependency_path = os.path.realpath(os.path.join(parent_bundle_path, dep['parent_path']))
                if not (dependency_path.startswith(parent_bundle_path) and os.path.exists(dependency_path)):
                    raise Exception('Invalid dependency %s/%s' % (dep['parent_uuid'], dep['parent_path']))
            else:
                raise Exception('Only shared file system is supported for now')

            docker_dependency_path = os.path.join(self.docker_dependencies_directory, dep['child_path'])
            os.symlink(docker_dependency_path, child_path)
            dependencies.append((dependency_path, docker_dependency_path, dep['child_uuid']))

        return dependencies

    @property
    def docker_command(self):
        bash_commands = [
            'cd %s' % self.docker_working_directory,
            '(%s) >stdout 2>stderr' % self._bundle['command'],
            ]

        return ['bash', '-c', '; '.join(bash_commands)]

    def create_job_definition(self, dependencies):
        """
        Create the Batch job definition.
        Each run has its own job definition which it cleans up when the run is complete.
        """
        bundle = self._bundle
        # TODO Get defaults from config
        image = self.metadata.get('docker_image', 'bash')
        # TODO Need a better way to do this, maybe a bootstrap script or something?
        memory = parse_int(self.metadata.get('request_memory'), 1024)
        cpus = parse_int(self.metadata.get('cpus'), 1)

        # TODO All of this mounting only works on shared file systems.
        #      Figure out a strategy for when this isn't the case (e.g. s3, transfer beforehand, etc)

        volumes_and_mounts = [self.volume_and_mount(
            host_path=os.path.join(self.shared_batch_efs_directory, self.uuid),
            container_path=self.docker_working_directory,
            name=self.uuid,
            read_only=False
        )]
        for host_path, docker_path, uuid in dependencies:
            volumes_and_mounts.append(self.volume_and_mount(
                host_path=host_path,
                container_path=docker_path,
                name='dependency_'+uuid,
                read_only=True
            ))

        job_definition = {
            'jobDefinitionName': bundle['uuid'],
            'type': 'container',
            'parameters': {},
            'containerProperties': {
                'image': image,
                'vcpus': cpus,
                'memory': memory,
                'command': self.docker_command,
                'volumes': map(lambda pair: pair[0], volumes_and_mounts),
                'environment': [
                    {
                        'name': 'HOME',
                        'value': self.docker_working_directory
                    }
                ],  # TODO Should the env be set?
                'mountPoints': map(lambda pair: pair[1], volumes_and_mounts),
                'readonlyRootFilesystem': False,
            },
            'retryStrategy': {
                'attempts': 1
            }
        }

        self.logger.debug(job_definition)

        response = self._batch_client.register_job_definition(**job_definition)
        arn = response['jobDefinitionArn']

        self.logger.debug("Job %s registered job definition arn %s", self.uuid, arn)
        return arn

    def volume_and_mount(self, host_path, container_path, name, read_only):
        volume_definition = {
            'host': {
                # TODO Figure out how to break assumption about shared file system
                'sourcePath': host_path
            },
            'name': name
        }

        mount_point = {
            'containerPath': container_path,
            'readOnly': read_only,
            'sourceVolume': name
        }

        return volume_definition, mount_point


class Submit(AwsBatchRunState):
    def update(self, events):
        job_definition = self._bundle['metadata']['batch_job_definition']

        # Submit job to AWS Batch
        response = self._batch_client.submit_job(
            jobName=self.uuid,
            jobQueue=self.batch_queue,
            jobDefinition=job_definition
        )

        job_id = response['jobId']

        self.update_metadata(batch_job_id=job_id, submit_time=int(time.time()))

        return self.transition(Running)


class BatchStatus(object):
    Submitted = 'SUBMITTED'
    Pending = 'PENDING'
    Runnable = 'RUNNABLE'
    Starting = 'STARTING'
    Running = 'RUNNING'
    Succeeded = 'SUCCEEDED'
    Failed = 'FAILED'


class Running(AwsBatchRunState):
    def __init__(self, *args, **kwargs):
        super(Running, self).__init__(*args, **kwargs)
        self._last_check_time = 0
        self._check_frequency = 30  # seconds
        self._job = None

    def should_refresh(self):
        now = int(time.time())
        if now - self._last_check_time > self._check_frequency:
            self._last_check_time = now
            return True
        return False

    def get_job(self):
        if self.should_refresh() or self._job is None:
            response = self._batch_client.describe_jobs(jobs=[self.metadata['batch_job_id']])
            self._job = response['jobs'][0]
        return self._job

    """
    Waiting for Batch to schedule the job
    """
    def update(self, events):
        job = self.get_job()
        status = job['status']

        started = job.get('startedAt')
        stopped = job.get('stoppedAt')
        runtime = stopped - started if stopped and started else 0
        now = int(time.time())

        if status in [BatchStatus.Failed, BatchStatus.Succeeded]:
            finalize_message = {
                'exitcode': job.get('exitCode'),
                'failure_message': job.get('reason')
            }
            run_status = 'Succeeded' if status == BatchStatus.Succeeded else 'Failed'

            self.update_metadata(run_status=run_status, last_updated=now, time=runtime)
            self._bundle_service.finalize_bundle(self._worker.id, self.uuid, finalize_message)

            return self.transition(Cleanup)

        updates = {
            'run_status': 'Batch Status: %s' % status,
            'last_updated': now
        }

        if started:
            updates['time'] = now - started

        self.update_metadata(**updates)

        return self.noop()


class Cleanup(AwsBatchRunState):
    def update(self, events):
        self._batch_client.deregister_job_definition(
            jobDefinition=self.metadata['batch_job_definition']
        )

        # TODO Cleanup <bundle_uuid>_dependencies folder
        return self.transition(Complete)


class Complete(AwsBatchRunState):
    def update(self, events):
        self._worker.finish_run(self.uuid)
        # TODO Maybe we should set the final success/failed status here instead of above
        return None, []

