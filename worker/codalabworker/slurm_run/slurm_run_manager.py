from codalabworker.bundle_state_objects import BundleInfo, RunResources, WorkerRun
from codalabworker.run_manager import BaseRunManager
import errno
import fcntl
import json
import os
import time
import shutil
import subprocess


class SlurmRunManager(BaseRunManager):
    STATE_FILE_NAME = "state.json"
    COMMANDS_LOCK_FILE_NAME = "commands.lock"
    COMMANDS_FILE_NAME = "commands.json"
    BUNDLE_FILE_NAME = "bundle_info.json"
    RESOURCES_FILE_NAME = "resources.json"
    SLURM_OUTPUT_FILE_NAME = "slurm_log.txt"
    MAX_CORES_ALLOWED = 1
    MAX_GPUS_ALLOWED = 0

    def __init__(self, worker_dir, sbatch_binary='sbatch', slurm_run_binary='/u/nlp/bin/cl-slurm-job', slurm_host=None):
        self.runs = set()  # List[str] UUIDs of runs
        self.run_states = {}  # uuid -> Dict (of WorkerRun)
        self.run_commands = {}  # uuid -> List[Command]
        self.run_paths = {}  # uuid -> str
        self.run_jobs = {}  # uuid -> str (Slurm JOBIDs)

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
                self._write_commands(self.run_commands[uuid], self.run_paths[uuid])
            except IOError:
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
        self.run_paths[bundle.uuid] = os.path.abspath(os.path.join(self.work_dir, bundle.uuid))
        self.run_commands[bundle.uuid] = []

        os.mkdir(self.run_paths[bundle.uuid])
        state_path = os.path.abspath(os.path.join(self.run_paths[bundle.uuid], self.STATE_FILE_NAME))
        commands_lock_path = os.path.abspath(os.path.join(
            self.run_paths[bundle.uuid], self.COMMANDS_LOCK_FILE_NAME
        ))
        commands_path = os.path.abspath(os.path.join(
            self.run_paths[bundle.uuid], self.COMMANDS_FILE_NAME
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
            json.dump(bundle.__dict__, outfile)
        with open(resources_path, "w") as outfile:
            json.dump(resources.__dict__, outfile)
        open(state_path, 'a').close()
        open(commands_path, 'a').close()
        self._write_commands(
            self.run_commands[bundle.uuid], self.run_paths[bundle.uuid]
        )
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
                --commands-file {} \
                --commands-lock-file {}".format(
            self.slurm_run_binary,
            bundle_info_path,
            resources_path,
            state_path,
            commands_path,
            commands_lock_path,
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

    def get_run(self, uuid):
        if uuid in self.run_paths:
            try:
                return self._read_run_state(self.run_paths[uuid])
            except Exception:
                return self.run_states.get(uuid, None)
        return None

    def mark_finalized(self, uuid):
        self.runs.remove(uuid)
        shutil.rmtree(self.run_paths[uuid])
        del self.run_commands[uuid]
        del self.run_states[uuid]
        del self.run_paths[uuid]

    def write(self, bundle_uuid, path, string):
        """
        Write string to path in bundle with uuid
        """
        if bundle_uuid not in self.runs:
            return
        self.run_commands[bundle_uuid].append(
            {"command": "write", "args": {"path": path, "string": string}}
        )

    def netcat(self, bundle_uuid, port, message):
        """
        Write message to port of bundle with uuid and read the response.
        Returns a stream with the response contents
        """
        if bundle_uuid not in self.runs:
            return
        self.run_commands[bundle_uuid].append(
            {"command": "kill", "args": {"port": port, "message": message}}
        )

    def kill(self, bundle_uuid):
        """
        Kill bundle with uuid
        """
        if bundle_uuid not in self.runs:
            return
        self.run_commands[bundle_uuid].append({"command": "kill"})

    @property
    def all_runs(self):
        """
        Returns a list of all the runs managed by this RunManager
        """
        return self.run_states

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
                    run_state = json.load(f)
                return run_state
            except Exception as ex:
                num_retries -= 1
                if num_retries == 0:
                    print("Can't read state file: {}".format(ex))
                    raise

    def _write_commands(self, commands, path):
        """
        Writes the given commands to the commands file in the given dir
        waiting for the lock at the lock file in the given dir
        """
        lock_path = os.path.join(path, self.COMMANDS_LOCK_FILE_NAME)
        commands_path = os.path.join(path, self.COMMANDS_FILE_NAME)
        while True:
            try:
                lock_file = open(lock_path, "w+")
                fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except IOError as ex:
                if ex.errno != errno.EAGAIN:
                    raise
                else:
                    time.sleep(0.1)
        with open(commands_path, "w") as f:
            json.dump(commands, f)
        fcntl.flock(lock_file, fcntl.LOCK_UN)
        lock_file.close()
