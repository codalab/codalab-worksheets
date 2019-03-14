from codalabworker.bundle_state_objects import BundleInfo, RunResources, WorkerRun
from codalabworker.run_manager import BaseRunManager, Reader
from codalabworker.download_util import get_target_path, PathException
import codalabworker.download_util as download_util
import errno
import fcntl
import json
import os
import time
import shutil
import subprocess


KILLED_STRING = '<KILLED>'


class SlurmRunManager(BaseRunManager):
    STATE_FILE_NAME = "state.json"
    KILL_COMMAND_FILE_NAME = "killed.txt"
    BUNDLE_FILE_NAME = "bundle_info.json"
    RESOURCES_FILE_NAME = "resources.json"
    SLURM_OUTPUT_FILE_NAME = "slurm_log.txt"
    MAX_CORES_ALLOWED = 1
    MAX_GPUS_ALLOWED = 3

    def __init__(self, worker_dir, sbatch_binary='sbatch', slurm_run_binary='/u/nlp/bin/cl-slurm-job', slurm_host=None, docker_network_internal_name='codalab-docker-network-int', docker_network_external_name='codalab-docker-network-ext'):
        self.runs = set()  # List[str] UUIDs of runs
        self.run_states = {}  # uuid -> WorkerRun
        self.bundle_infos = {}  # uuid -> BundleInfo
        self.run_paths = {}  # uuid -> str
        self.run_jobs = {}  # uuid -> str (Slurm JOBIDs)
        self.kill_queue = []  # List[uuid]

        self.docker_network_internal_name = docker_network_internal_name
        self.docker_network_external_name = docker_network_external_name

        self.slurm_host = slurm_host
        self.sbatch_binary = sbatch_binary
        self.slurm_run_binary = slurm_run_binary
        self.work_dir = os.path.join(worker_dir, "run_manager")

        try:
            os.mkdir(self.work_dir)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise

    def start(self):
        """
        Nothing needs to be started at this time
        """
        pass

    def stop(self):
        for uuid in self.runs:
            if uuid in self.run_jobs:
                jobname = self.run_jobs[uuid]
                squeue_command = 'squeue -o \%A -h -n {}'.format(jobname)
                try:
                    jobid = subprocess.check_output(squeue_command)
                except subprocess.CalledProcessError as ex:
                    print(ex)
                else:
                    subprocess.check_call('scancel {}'.format(jobid))

    def save_state(self):
        """
        Saving state to disk not supported yet
        """
        pass

    def process_runs(self):
        for uuid in self.runs:
            try:
                self.run_states[uuid] = self._read_run_state(self.run_paths[uuid])
            except (IOError, ValueError):
                pass
            if uuid in self.kill_queue:
                try:
                    self._write_kill_command(uuid)
                except (IOError, ValueError):
                    pass


    def create_run(self, bundle, resources):
        """
        1. Add run to your local state (path of bundle, resources, state, lock files)
        2. Write bundle, resources and lock files
        3. Submit slurm job
        """
        bundle = BundleInfo.from_dict(bundle)
        resources = RunResources.from_dict(resources)
        self.runs.add(bundle.uuid)
        self.bundle_infos[bundle.uuid] = bundle
        self.run_paths[bundle.uuid] = os.path.abspath(os.path.join(self.work_dir, bundle.uuid))

        os.mkdir(self.run_paths[bundle.uuid])
        state_path = os.path.abspath(os.path.join(self.run_paths[bundle.uuid], self.STATE_FILE_NAME))
        kill_command_path = os.path.abspath(os.path.join(
            self.run_paths[bundle.uuid], self.KILL_COMMAND_FILE_NAME
        ))
        bundle_info_path = os.path.abspath(os.path.join(
            self.run_paths[bundle.uuid], self.BUNDLE_FILE_NAME
        ))
        resources_path = os.path.abspath(os.path.join(
            self.run_paths[bundle.uuid], self.RESOURCES_FILE_NAME
        ))
        slurm_output_path = os.path.abspath(os.path.join(
            self.run_paths[bundle.uuid], self.SLURM_OUTPUT_FILE_NAME
        ))

        with open(bundle_info_path, "w") as outfile:
            json.dump(bundle.to_dict(), outfile)

        with open(resources_path, "w") as outfile:
            json.dump(resources.to_dict(), outfile)
        open(state_path, 'a').close()
        open(kill_command_path, 'a').close()
        # TODO: request_queue =
        # TODO: host, gpu_type, parition = parse_tags()
        job_name = "codalab-run-{}".format(bundle.uuid)
        gpu_type = None
        partition = "jag-lo" if resources.gpus else "john"
        sbatch_flags = [
            self.sbatch_binary,
            "--mem={}K".format(resources.memory//1024),
            "--chdir={}".format(self.run_paths[bundle.uuid]),
            "--job-name={}".format(job_name),
            "--output={}".format(slurm_output_path),
            "--partition={}".format(partition),
            "--export==ANACONDA_ENV=py-3.6.8,ALL",
        ]
        if resources.cpus > 1:
            sbatch_flags.append("--cpus-per-task={}".format(resources.cpus))
        if resources.gpus:
            gpu_type = "{}:".format(gpu_type) if gpu_type else ""
            sbatch_flags.append("--gres=gpu:{}{}".format(gpu_type, resources.gpus))
        sbatch_command = " ".join(sbatch_flags)
        slurm_run_command = "{} \
                --bundle-file {} \
                --resources-file {} \
                --state-file {} \
                --kill-command-file {} \
                --docker-network-internal-name {} \
                --docker-network-external-name {}".format(
            self.slurm_run_binary,
            bundle_info_path,
            resources_path,
            state_path,
            kill_command_path,
            self.docker_network_internal_name,
            self.docker_network_external_name,
        )
        final_command = '{} {}'.format(sbatch_command, slurm_run_command)
        if self.slurm_host is not None:
            final_command = 'ssh {} {}'.format(self.slurm_host, final_command)
        try:
            subprocess.check_call(final_command, shell=True)
        except Exception as e:
            # TODO: something went wrong
            print(e)
        else:
            self.run_jobs[bundle.uuid] = job_name

    def has_run(self, uuid):
        return uuid in self.runs

    def mark_finalized(self, uuid):
        self.runs.remove(uuid)
        shutil.rmtree(self.run_paths[uuid])
        del self.run_states[uuid]
        del self.run_paths[uuid]
        if uuid in self.kill_queue:
            self.kill_queue.remove(uuid)

    def read(self, uuid, path, args, reply_fn):
        """
        TODO: Test
        """
        run_state = self.run_states[uuid]
        bundle_info = self.bundle_infos[uuid]
        read_type = args['type']
        if read_type != 'get_target_info':
            err = (httplib.BAD_REQUEST, "Unsupported read_type for slurm worker read: %s" % read_type)
            reply_fn(err)
            return

        target_info = None
        dependency_paths = set([dep.child_path for dep in bundle_info.dependencies])

        # if path is a dependency raise an error
        if path and os.path.normpath(path) in dependency_paths:
            err = (httplib.NOT_FOUND, '{} not found in bundle {}'.format(path, bundle_info.uuid))
            reply_fn(err, None, None)
            return
        else:
            try:
                target_info = download_util.get_target_info(
                    bundle_info.location, bundle_info.uuid, path, args['depth']
                )
            except PathException as e:
                err = (httplib.NOT_FOUND, e.message)
                reply_fn(err, None, None)
                return

        if not path and args['depth'] > 0:
            target_info['contents'] = [
                child for child in target_info['contents'] if child['name'] not in dependency_paths
            ]

        reply_fn(None, {'target_info': target_info}, None)

    def write(self, uuid, path, string):
        """
        Write string to path in bundle with uuid
        TODO: Test
        """
        bundle_info = self.bundle_infos[uuid]
        dependency_paths = set([dep.child_path for dep in bundle_info.dependencies])
        if os.path.normpath(path) in dependency_paths:
            return
        with open(os.path.join(bundle_info.location, path), 'w') as f:
            f.write(string)

    def netcat(self, uuid, port, message, reply):
        """
        Write message to port of bundle with uuid and read the response.
        Returns a stream with the response contents
        TODO: Test this
        """
        run_state = self.run_states[uuid]
        bundle_info = self.bundle_infos[uuid]
        if bundle_info.uuid not in self.runs or 'container_id' not in run_state.info:
            return
        container_ip = docker_utils.get_container_ip(
            self.docker_network_external_name, run_state.info['container_id']
        )
        if not container_ip:
            container_ip = docker_utils.get_container_ip(
                self.docker_network_internal_name, run_state.info['container_id']
            )
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((container_ip, port))
        s.sendall(message)

        total_data = []
        while True:
            data = s.recv(LocalRunManager.NETCAT_BUFFER_SIZE)
            if not data:
                break
            total_data.append(data)
        s.close()
        reply(None, {}, ''.join(total_data))

    def kill(self, uuid):
        """
        Kill bundle with uuid
        TODO: Test
        Test
        """
        bundle_info = self.bundle_infos[uuid]
        if bundle_info.uuid not in self.runs or bundle_info.uuid in self.kill_queue:
            return
        self.kill_queue.append(bundle_info.uuid)

    @property
    def all_runs(self):
        """
        Returns a list of all the runs managed by this RunManager
        """
        runs = {k: v.to_dict() for k, v in self.run_states.items()}
        if len(runs):
            print(runs)
        return runs

    @property
    def all_dependencies(self):
        """
        Returns a list of all dependencies available in this RunManager
        This returns empty since SlurmRunManager only supports shared filesystems
        """
        return []

    @property
    def cpus(self):
        """
        SlurmWorker doesn't really have a number of CPUs, but we don't want infinitely
        many runs to be submitted and sent to Slurm through CodaLab, so we set an artificial
        limit to how many CPU cores we say we have.
        """
        return self.MAX_CORES_ALLOWED

    @property
    def gpus(self):
        """
        SlurmWorker doesn't really have a number of CPUs, but we don't want infinitely
        many runs to be submitted and sent to Slurm through CodaLab, so we set an artificial
        limit to how many GPUs we say we have.
        """
        return self.MAX_GPUS_ALLOWED

    @property
    def memory_bytes(self):
        """
        This worker doesn't really have a memory limit so we just send a really large number
        this should be around 930 GBs
        """
        return 1e12

    def _read_run_state(self, path):
        """
        Reads a WorkerRun run state dict from the state file in the given dir
        """
        state_path = os.path.join(path, self.STATE_FILE_NAME)
        num_retries = 10
        while num_retries:
            try:
                with open(state_path, "r") as f:
                    run_state = WorkerRun.from_dict(json.load(f))
                return run_state
            except Exception as ex:
                num_retries -= 1
                if num_retries == 0:
                    print("Can't read state file: {}".format(ex))
                    raise

    def _write_kill_command(self, uuid):
        """
        Writes a kill command to the kill command path of the given bundle
        """
        command_path = os.path.join(self.run_paths[uuid], self.KILL_COMMAND_FILE_NAME)
        with open(command_path, "w") as f:
            f.write(KILLED_STRING)
