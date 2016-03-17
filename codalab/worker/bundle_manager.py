import datetime
import os
import re
import subprocess
import sys
import time
import traceback

from codalab.common import State
from codalab.lib import bundle_util, formatting, path_util
from codalab.objects.permission import check_bundle_have_run_permission


class BundleManager(object):
    def __init__(self, codalab_manager):
        self._model = codalab_manager.model()
        self._worker_model = codalab_manager.worker_model()
        self._bundle_store = codalab_manager.bundle_store()

        # Parse the config.
        config = codalab_manager.config.get('workers')
        if not config:
            print >> sys.stderr, 'Config is missing a workers section'
            exit(1)

        if 'default_docker_image' not in config:
            print >> sys.stderr, 'workers config missing default_docker_image'
            exit(1)
        self._default_docker_image = config['default_docker_image']

        if 'torque' in config:
            assert(self._worker_model.shared_file_system)
            self._use_torque = True
            self._torque_host = config['torque']['host']
            self._torque_bundle_service_url = config['torque']['bundle_service_url']
            self._torque_password_file = config['torque']['password_file']
            self._torque_log_dir = config['torque']['log_dir']
            path_util.make_directory(self._torque_log_dir)
            if 'worker_code_dir' in config['torque']:
                self._torque_worker_code_dir = config['torque']['worker_code_dir']
            else:
                codalab_cli = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
                self._torque_worker_code_dir = os.path.join(codalab_cli, 'worker')
        else:
            self._use_torque = False

        def parse(to_value, field):
            return to_value(config[field]) if field in config else None
        self._default_request_time = parse(formatting.parse_duration, 'default_request_time')
        self._default_request_memory = parse(formatting.parse_size, 'default_request_memory')
        self._default_request_disk = parse(formatting.parse_size, 'default_request_disk')
        self._max_request_time = parse(formatting.parse_duration, 'max_request_time')
        self._max_request_memory = parse(formatting.parse_size, 'max_request_memory')
        self._max_request_disk = parse(formatting.parse_size, 'max_request_disk')

        self._default_request_cpus = config.get('default_request_cpus')
        self._default_request_network = config.get('default_request_network')
        self._default_request_queue = config.get('default_request_queue')
        self._default_request_priority = config.get('default_request_priority')

    def run(self, num_iterations, sleep_time):
        iteration = 1
        while True:
            try:
                self._run_iteration()
            except Exception:
                traceback.print_exc()
            iteration += 1
            if num_iterations and iteration > num_iterations:
                return
            time.sleep(sleep_time)

    def _run_iteration(self):
        self._stage_bundles()
        # TODO: Handle make bundles.
        self._schedule()

    def _stage_bundles(self):
        """
        Stages bundles by:
            1) Failing any bundles that have any missing or failed dependencies.
            2) Staging any bundles that have all ready dependencies.
        """
        bundles = self._model.batch_get_bundles(state=State.CREATED)
        parent_uuids = set(
          dep.parent_uuid for bundle in bundles for dep in bundle.dependencies)
        parents = self._model.batch_get_bundles(uuid=parent_uuids)
        all_parent_states = {parent.uuid: parent.state for parent in parents}
        all_parent_uuids = set(all_parent_states)

        bundles_to_fail = []
        bundles_to_stage = []
        for bundle in bundles:
            parent_uuids = set(dep.parent_uuid for dep in bundle.dependencies)

            missing_uuids = parent_uuids - all_parent_uuids
            if missing_uuids:
                bundles_to_fail.append(
                    (bundle,
                     'Missing parent bundles: %s' % ', '.join(missing_uuids)))
                continue

            parent_states = {uuid: all_parent_states[uuid]
                             for uuid in parent_uuids}

            # TODO: Allow failed dependencies.
            # TODO: Don't depend on bundles that you can't read.
            failed_uuids = [
              uuid for uuid, state in parent_states.iteritems()
              if state == State.FAILED]
            if failed_uuids:
                bundles_to_fail.append(
                  (bundle,
                   'Parent bundles failed: %s' % ', '.join(failed_uuids)))
                continue

            if all(state == State.READY for state in parent_states.itervalues()):
                bundles_to_stage.append(bundle)

        for bundle, failure_message in bundles_to_fail:
            self._model.update_bundle(
                bundle, {'state': State.FAILED,
                         'metadata': {'failure_message': failure_message}})
        for bundle in bundles_to_stage:
            self._model.update_bundle(
                bundle, {'state': State.STAGED})

    def _schedule(self):
        """
        This method implements a state machine. It is different between whether
        we are using Torque or not.

        With Torque, the states are:

        Staged, no job_handle, no worker, no worker_run DB entry:
            Needs a Torque worker to be started.
        Staged, has job_handle, worker starting, no worker_run DB entry:
            Waiting for the Torque worker to start before sending a run message.
        Queued, has job_handle, worker running, has worker_run DB entry:
            Run message sent, waiting for the run to start. 
        Running, has job_handle, worker running, has worker_run DB entry:
            Worker reported that the run has started.
        Ready / Failed, has job_handle, worker running, no worker_run DB entry:
            Will send shutdown message to worker.
        Ready / Failed, has job_handle, no worker, no worker_run DB entry:
            Finished.

        Without Torque, the states are:

        Staged, no worker_run DB entry:
            Ready to schedule send run message to available worker.
        Queued, has worker_run DB entry:
            Run message sent, waiting for the run to start.
        Running, has worker_run DB entry:
            Worker reported that the run has started.
        Ready / Failed, no worker_run DB entry:
            Finished.
        """
        # TODO: Can't we just add a new state for Torque.
        workers = WorkerInfoAccessor(self._worker_model.get_workers())

        # Clean-up dead workers. If we haven't heard from a worker for 5
        # minutes, it's considered dead and should be cleaned up.
        for worker in workers.workers():
            if datetime.datetime.now() - worker['checkin_time'] > datetime.timedelta(minutes=5):
                self._worker_model.worker_cleanup(worker['user_id'], worker['worker_id'])
                workers.remove(worker)
                if self._use_torque:
                    self._clear_torque_logs(worker['worker_id'])

        # Re-stage bundles that got stuck in QUEUED state.
        for bundle in self._model.batch_get_bundles(state=State.QUEUED, bundle_type='run'):
            if (not workers.is_running(bundle.uuid) or  # Dead worker.
                time.time() - bundle.metadata.last_updated > 5 * 60):  # Run message went missing.
                if self._model.unqueue_bundle(bundle):
                    workers.unqueue(bundle.uuid)

        # Fail bundles that got stuck in RUNNING state.
        for bundle in self._model.batch_get_bundles(state=State.RUNNING, bundle_type='run'):
            if (not workers.is_running(bundle.uuid) or  # Dead worker.
                time.time() - bundle.metadata.last_updated > 60 * 60):  # Shouldn't really happen, but let's be safe.
                self._model.finalize_bundle(bundle, exitcode=None, failure_message='Worker died')
                workers.unqueue(bundle.uuid)
                
        if self._use_torque:
            for bundle in self._model.batch_get_bundles(state=State.STAGED, bundle_type='run'):
                if hasattr(bundle.metadata, 'job_handle'):
                    # Fail bundles where the worker didn't start.
                    failure_message = self._read_torque_error_log(bundle.metadata.job_handle)
                    if failure_message is None and time.time() - bundle.metadata.last_updated > 20 * 60:
                        failure_message = 'Worker disappeared'
                    if failure_message is not None:
                        self._model.finalize_bundle(bundle, exitcode=None, failure_message=failure_message)
                        self._clear_torque_logs(bundle.metadata.job_handle)
                else:
                    # Start Torque workers.
                    try:
                        job_handle = self._start_torque_worker(bundle)
                    except TorqueException as e:
                        self._model.finalize_bundle(bundle, exitcode=None, failure_message=e.message)
                        continue
                    self._model.torque_stage_bundle(bundle, job_handle)
                 
        # Run bundles.
        for bundle in self._model.batch_get_bundles(state=State.STAGED, bundle_type='run'):
            if self._use_torque and not hasattr(bundle.metadata, 'job_handle'):
                continue
            for worker in self._schedule_bundle(workers, bundle):
                if self._model.queue_bundle(bundle, worker['user_id'], worker['worker_id']):
                    workers.queue(bundle.uuid, worker)
                    if self._worker_model.send_json_message(
                        worker['socket_id'], self._construct_run_message(bundle), 0.2):
                        break
                    else:
                        self._model.unqueue_bundle(bundle)
                        workers.unqueue(bundle.uuid)
                        # Try the next worker.

        if self._use_torque:
            # Shutdown finished Torque workers.
            running_job_handles = set()
            for bundle in self._model.batch_get_bundles(state=[State.STAGED, State.QUEUED, State.RUNNING], bundle_type='run'):
                if hasattr(bundle.metadata, 'job_handle'):
                    running_job_handles.add(bundle.metadata.job_handle)
            for worker in workers.workers():
                if worker['worker_id'] not in running_job_handles:
                    shutdown_message = {
                        'type': 'shutdown',
                    }
                    self._worker_model.send_json_message(
                        worker['socket_id'], shutdown_message, 0.2)
                    self._clear_torque_logs(worker['worker_id'])

    def _start_torque_worker(self, bundle):
        resource_args = []

        request_cpus = bundle.metadata.request_cpus or self._default_request_cpus
        if request_cpus:
            resource_args.extend(['-l', 'nodes=1:ppn=%d' % request_cpus])

        request_memory = self._construct_run_message(bundle)['resources']['request_memory']
        if request_memory:
            resource_args.extend(['-l', 'mem=%d' % request_memory])

        request_queue = bundle.metadata.request_queue or self._default_request_queue
        if request_queue:
            # Either host=<host-name> or <queue-name>
            m = re.match('^host=(.+)$', request_queue)
            if m:
                resource_args.extend(['-l', 'host=' + m.group(1)])
            else:
                resource_args.extend(['-q', request_queue])

        request_priority = bundle.metadata.request_priority or self._default_request_priority
        if request_priority:
            resource_args.extend(['-p', str(request_priority)])

        script_args = [
            '--bundle-service-url',  self._torque_bundle_service_url,
            '--password-file', self._torque_password_file,
            '--shared-file-system',
        ]

        script_env = {
            'LOG_DIR': self._torque_log_dir,
            'WORKER_CODE_DIR': self._torque_worker_code_dir,
              # -v doesn't work with spaces, so we have to hack it.
            'WORKER_ARGS': '|'.join(script_args),
        }

        command = self._torque_ssh_command(
            ['qsub', '-o', '/dev/null', '-e', '/dev/null',
             '-v', ','.join([k + '=' + v for k, v in script_env.iteritems()])] +
            resource_args +
            [ '-S', '/bin/bash', os.path.join(self._torque_worker_code_dir, 'worker.sh')])

        try:
            return subprocess.check_output(command, stderr=subprocess.STDOUT).strip()
        except subprocess.CalledProcessError as e:
            raise TorqueException('Failed to launch Torque job: ' + e.output)

    def _torque_ssh_command(self, args):
        args = ['"' + arg.replace('"', '\\"') + '"' for arg in args]  # Quote arguments
        return ['ssh', '-oBatchMode=yes', '-x', self._torque_host] + args

    def _read_torque_error_log(self, job_handle):
        error_log_path = os.path.join(self._torque_log_dir, 'stderr.' + job_handle)
        if os.path.exists(error_log_path):
            with open(error_log_path) as f:
                lines = [line for line in f.readlines() if not line.startswith('PBS')]
                if lines:
                    return ''.join(lines)
                    
        return None

    def _clear_torque_logs(self, job_handle):
        try:
            os.remove(os.path.join(self._torque_log_dir, 'stdout.' + job_handle))
        except OSError:
            pass
        try:
            os.remove(os.path.join(self._torque_log_dir, 'stderr.' + job_handle))
        except OSError:
            pass

    def _schedule_bundle(self, workers, bundle):
        # TODO: Prefer user worker!!!!
        if self._use_torque:
            for worker in workers.workers():
                if worker['worker_id'] == bundle.metadata.job_handle:
                    yield worker
        else:
            # TODO: Make this more intelligent.
            # TODO: Handle case of shared_file_system.
            for worker in workers.workers():
                if (check_bundle_have_run_permission(self._model, worker['user_id'], bundle) and
                    worker['slots'] - len(worker['run_uuids']) > 0):
                    yield worker

    def _construct_run_message(self, bundle):
        message = {}
        message['type'] = 'run'
        message['bundle'] = bundle_util.bundle_to_bundle_info(self._model, bundle)
        if self._worker_model.shared_file_system:
            message['bundle']['location'] = self._bundle_store.get_bundle_location(bundle.uuid)
            for dependency in message['bundle']['dependencies']:
                dependency['location'] = self._bundle_store.get_bundle_location(dependency['parent_uuid'])


        # Figure out the resource requirements.
        resources = message['resources'] = {}

        resources['docker_image'] = (bundle.metadata.request_docker_image or
                                     self._default_docker_image)

        # Parse |request_string| using |to_value|, but don't exceed |max_value|.
        def parse_and_min(to_value, request_string, default_value, max_value):
            # Use default if request value doesn't exist
            if request_string:
                request_value = to_value(request_string)
            else:
                request_value = default_value
            if request_value and max_value:
                return int(min(request_value, max_value))
            elif request_value:
                return int(request_value)
            elif max_value:
                return int(max_value)
            else:
                return None
        resources['request_time'] = parse_and_min(formatting.parse_duration,
                                                  bundle.metadata.request_time,
                                                  self._default_request_time,
                                                  self._max_request_time)
        resources['request_memory'] = parse_and_min(formatting.parse_size,
                                                    bundle.metadata.request_memory,
                                                    self._default_request_memory,
                                                    self._max_request_memory)
        resources['request_disk'] = parse_and_min(formatting.parse_size,
                                                  bundle.metadata.request_disk,
                                                  self._default_request_disk,
                                                  self._max_request_disk)
        resources['request_network'] = (bundle.metadata.request_network or
                                        self._default_request_network)

        return message


class WorkerInfoAccessor(object):
    def __init__(self, workers):
        self._workers = workers
        self._uuid_to_worker = {}
        for worker in self._workers:
            for uuid in worker['run_uuids']:
                self._uuid_to_worker[uuid] = worker

    def workers(self):
        return list(self._workers)

    def remove(self, worker):
        self._workers.remove(worker)
        for uuid in worker['run_uuids']:
            del self._uuid_to_worker[uuid]

    def is_running(self, uuid):
        return uuid in self._uuid_to_worker

    def queue(self, uuid, worker):
        worker['run_uuids'].append(uuid)
        self._uuid_to_worker[uuid] = worker

    def unqueue(self, uuid):
        if uuid in self._uuid_to_worker:
            worker = self._uuid_to_worker[uuid]
            worker['run_uuids'].remove(uuid)
            del self._uuid_to_worker[uuid]


class TorqueException(Exception):
    def __init__(self, message):
        super(TorqueException, self).__init__(message)
