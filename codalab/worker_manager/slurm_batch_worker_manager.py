import logging
import uuid
import subprocess
import getpass
import re
import textwrap
from pathlib import Path

from .worker_manager import WorkerManager, WorkerJob

logger = logging.getLogger(__name__)


class SlurmBatchWorkerManager(WorkerManager):
    NAME = 'slurm-batch'
    DESCRIPTION = 'Worker manager for submitting jobs using Slurm Batch'

    """
    An enumeration of Slurm commands.
    """
    SRUN = 'srun'
    SBATCH = 'sbatch'
    SQUEUE = 'squeue'
    SCONTROL = 'scontrol'

    """
    sbatch configuration in bash script
    """
    SBATCH_PREFIX = '#SBATCH'
    SBATCH_COMMAND_RETURN_REGEX = re.compile(r'^Submitted batch job (\d+)$')

    @staticmethod
    def add_arguments_to_subparser(subparser):
        subparser.add_argument(
            '--job-name',
            type=str,
            default='codalab-slurm-worker',
            help='Name for the job that will be generated by this worker manager',
        )
        subparser.add_argument(
            '--nodelist', type=str, default='', help='The worker node to run jobs in'
        )
        subparser.add_argument(
            '--partition', type=str, required=True, help='Name of batch job queue to use'
        )
        subparser.add_argument(
            '--cpus', type=int, default=1, help='Default number of CPUs for each worker'
        )
        subparser.add_argument(
            '--gpus', type=int, default=1, help='Default number of GPUs for each worker'
        )
        subparser.add_argument('--gpu-type', type=str, help='GPU type to request from Slurm')
        subparser.add_argument(
            '--memory-mb', type=int, default=2048, help='Default memory (in MB) for each worker'
        )
        subparser.add_argument(
            '--time',
            type=str,
            default='10-0',
            help='Set a limit on the total run time in minutes or days-hours '
            'of the job allocation. Default to 10-0 (10 days, 0 hours)',
        )
        subparser.add_argument(
            '--dry-run',
            action='store_true',
            help='Print out Slurm batch job definition without submitting to Slurm',
        )
        subparser.add_argument(
            '--user', type=str, default=getpass.getuser(), help='User to run the Batch jobs as'
        )
        subparser.add_argument(
            '--password-file',
            type=str,
            help='Path to the file containing the username and password '
            'for logging into the CodaLab worker each on a separate '
            'line. If not specified, the worker will read from '
            'environment variable CODALAB_USERNAME and CODALAB_PASSWORD',
        )
        subparser.add_argument(
            '--slurm-work-dir',
            default='slurm-worker-scratch',
            help='Directory where to store Slurm batch scripts, logs, etc',
        )

    def __init__(self, args):
        super().__init__(args)
        self.username = self.args.user
        # A set of newly submitted job id to keep tracking worker status, as worker might not be created right away.
        self.submitted_jobs = self.load_worker_jobs()

    def load_worker_jobs(self):
        """
        Load worker jobs that are created using SlurmWorkerManager and
        owned by the current user from the Slurm scheduling queue.
        :return: a set of job id
        """
        # Get all the Slurm workers that are submitted by Slurm Batch Worker Manager and owned by the current user.
        # Returning result will be in the following format:
        # JOBID:NAME (header won't be included with "--noheader" option)
        # 1487896,john-job-3157358
        # 1478830,john-job-5234492
        submitted_jobs = set()
        jobs = self.run_command(
            [
                self.SQUEUE,
                '-u',
                self.username,
                '-p',
                self.args.partition,
                '--format',
                '%A,%j',
                '--noheader',
            ]
        )
        for job in jobs.strip().split():
            job_id, job_name = job.split(',')
            if job_name.startswith(self.username) and self.args.job_name in job_name:
                submitted_jobs.add(job_id)
        return submitted_jobs

    def get_worker_jobs(self):
        """
        Return a list of workers in RUNNING and PENDING state.
        The current Slurm Batch Worker Manager is developed and tested with Slurm version 17.11.13-2.
        TODO: use the default rest api when Slurm upgraded to version >= 20.02
        """
        # Documentation can be found at https://slurm.schedmd.com
        # Since allocating resource for a worker may take a while, we periodically check
        # for worker status and remove those workers that failed at starting phase.
        jobs_to_remove = set()
        for job_id in self.submitted_jobs:
            job_acct = self.run_command(
                [self.SCONTROL, 'show', 'jobid', '-d', job_id, '--oneliner'], verbose=False
            )
            # Extract out the JobState from the full scontrol output.
            job_state = re.search(r'JobState=(.*)\sReason', job_acct).group(1)
            logger.info("Job ID {} has state {}".format(job_id, job_state))
            if 'FAILED' in job_state:
                jobs_to_remove.add(job_id)
                logger.error("Failed to start job {}".format(job_id))
            elif 'COMPLETED' in job_state or 'CANCELLED' in job_state or "TIMEOUT" in job_state:
                jobs_to_remove.add(job_id)
                logger.info("Removing job ID {}".format(job_id))
        self.submitted_jobs = self.submitted_jobs - jobs_to_remove
        logger.info("Submitted jobs: {}".format(self.submitted_jobs))

        # Get all the Slurm workers that are submitted by SlurmWorkerManager and owned by the current user.
        # Returning result will be in the following format:
        # JOBID (header won't be included with "--noheader" option)
        # 1478828
        # 1478830
        jobs = self.run_command([self.SQUEUE, '-u', self.username, '--format', '%A', '--noheader'])
        jobs = jobs.strip().split()
        logger.info(
            'Workers: {}'.format(
                ' '.join(job for job in jobs if job in self.submitted_jobs) or '(none)'
            )
        )

        # Get all the RUNNING jobs that are owned by the current user.
        # Returning result will be in the following format:
        # JOBID (header won't be included with "--noheader" option)
        # 1478828
        # 1478830
        running_jobs = self.run_command(
            [self.SQUEUE, '-u', self.username, '-t', 'RUNNING', '--format', '%A', '--noheader']
        )
        running_jobs = running_jobs.strip().split()

        return [
            WorkerJob(active=True) if job in running_jobs else WorkerJob(active=False)
            for job in self.submitted_jobs
        ]

    def start_worker_job(self):
        """
        Start a CodaLab Slurm worker by submitting a batch job to Slurm
        """
        worker_id = self.username + "-" + self.args.job_name + '-' + uuid.uuid4().hex[:8]

        # Set up the Slurm worker directory
        slurm_work_dir = self.setup_slurm_work_directory(worker_id)
        if slurm_work_dir is None:
            return

        # Map command line arguments to Slurm arguments
        slurm_args = self.create_slurm_args(worker_id, slurm_work_dir)
        command = self.setup_codalab_worker(worker_id)
        job_definition = self.create_job_definition(slurm_args=slurm_args, command=command)

        # Do not submit job to Slurm if dry run is specified
        if self.args.dry_run:
            return

        batch_script = str(slurm_work_dir.joinpath(slurm_args['job-name'] + '.slurm'))
        self.save_job_definition(batch_script, job_definition)
        job_id_str = self.run_command([self.SBATCH, batch_script])

        match = re.match(self.SBATCH_COMMAND_RETURN_REGEX, job_id_str)
        if match is not None:
            job_id = match.group(1)
        else:
            logger.error("Cannot find job_id in {}.".format(job_id_str))
            return

        # Add the newly submitted job to submitted_jobs for tracking purpose
        self.submitted_jobs.add(job_id)

    def setup_slurm_work_directory(self, worker_id):
        """
        Set up the work directory for Slurm Batch Worker Manager
        :param worker_id: a string representing the worker id
        :return: slurm work directory
        """
        # Set up the Slurm worker directory
        slurm_work_dir = Path(self.args.slurm_work_dir, worker_id)
        try:
            slurm_work_dir.mkdir(parents=True, exist_ok=True)
            return slurm_work_dir
        except PermissionError as e:
            logger.error(
                "Failed to create the Slurm work directory: {}. "
                "Stop creating new workers.".format(e)
            )
            return None

    def setup_codalab_worker(self, worker_id):
        """
        Set up the configuration for the codalab worker that will run on the Slurm worker
        :param worker_id: a string representing the worker id
        :return: the command to run on
        """
        # This needs to be a unique directory since Batch jobs may share a host
        worker_network_prefix = 'cl_worker_{}_network'.format(worker_id)
        # Codalab worker's work directory
        if self.args.worker_work_dir_prefix:
            work_dir_prefix = Path(self.args.worker_work_dir_prefix)
        else:
            work_dir_prefix = Path()

        worker_work_dir = work_dir_prefix.joinpath(Path('slurm-codalab-worker-scratch', worker_id))

        command = [
            self.args.worker_executable,
            '--server',
            self.args.server,
            '--verbose',
            '--exit-when-idle',
            '--idle-seconds',
            str(self.args.worker_idle_seconds),
            '--work-dir',
            str(worker_work_dir),
            '--id',
            worker_id,
            '--network-prefix',
            worker_network_prefix,
            # always set in the Slurm worker manager to ensure safe shutdown
            '--pass-down-termination',
        ]
        if self.args.worker_tag:
            command.extend(['--tag', self.args.worker_tag])
        if self.args.password_file:
            command.extend(['--password-file', self.args.password_file])
        if self.args.worker_exit_after_num_runs and self.args.worker_exit_after_num_runs > 0:
            command.extend(['--exit-after-num-runs', str(self.args.worker_exit_after_num_runs)])
        if self.args.worker_max_work_dir_size:
            command.extend(['--max-work-dir-size', self.args.worker_max_work_dir_size])
        if self.args.worker_delete_work_dir_on_exit:
            command.extend(['--delete-work-dir-on-exit'])
        if self.args.worker_exit_on_exception:
            command.extend(['--exit-on-exception'])
        if self.args.worker_tag_exclusive:
            command.extend(['--tag-exclusive'])

        return command

    def run_command(self, command, verbose=True):
        """
        Run a given shell command and return the result
        :param command: the input command as list
        :return: an empty string if an error is caught. Otherwise, return the actual result
        """
        try:
            proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            output, errors = proc.communicate()
            if output:
                logger.info("Executed command: {}".format(' '.join(command)))
                result = output.decode()
                if verbose:
                    logger.info(result)
                return result
            if errors:
                logger.error(
                    "Failed to execute {}: {}, {}".format(
                        ' '.join(command), errors, proc.returncode
                    )
                )
        except Exception as e:
            logger.error(
                "Caught an exception when executing command {}: {}".format(' '.join(command), e)
            )
        return ""

    def save_job_definition(self, job_file, job_definition):
        """
        Save the batch job definition to file.
        :param job_file: a file storing the Slurm batch job configuration
        :param job_definition: the job definition of a Slurm batch job
        """
        with open(job_file, 'w') as f:
            f.write(job_definition)
        logger.info("Saved the Slurm Batch Job Definition to {}".format(job_file))

    def create_job_definition(self, slurm_args, command):
        """
        Create a Slurm batch job definition structured as a list of Slurm batch arguments and a srun command
        :param slurm_args: arguments for launching a Slurm batch job
        :param command: arguments for starting a CodaLab worker
        :return: a string containing the Slurm batch job definition
        """
        sbatch_args = [
            '{} --{}={}'.format(self.SBATCH_PREFIX, key, slurm_args[key])
            for key in sorted(slurm_args.keys())
        ]

        # Check the existence of environment variables CODALAB_USERNAME and
        # CODALAB_PASSWORD when password_file is not given.
        worker_authentication = textwrap.dedent(
            '''
            if [ ! -z "${CODALAB_USERNAME}" ] && [ ! -z "${CODALAB_PASSWORD}" ]
            then
                  echo "Found environment variables CODALAB_USERNAME and CODALAB_PASSWORD."
            else
                  echo "Environment variable CODALAB_USERNAME or CODALAB_PASSWORD is not set properly."
                  echo "Stop creating new workers."
                  exit 1
            fi
            '''
            + '\n\n'
            if not self.args.password_file
            else ''
        )

        # Using the --unbuffered option with srun command will allow output
        # appear in the output file as soon as it is produced.
        srun_args = [self.SRUN, '--unbuffered'] + command

        # Job definition contains two sections: sbatch arguments and srun command
        job_definition = (
            '#!/usr/bin/env bash\n\n'
            + '\n'.join(sbatch_args)
            + '\n\n'
            + worker_authentication
            + ' '.join(srun_args)
        )
        logger.info("Slurm Batch Job Definition")
        logger.info(job_definition)
        return job_definition

    def create_slurm_args(self, worker_id, slurm_worker_dir):
        """
        Convert command line arguments to Slurm arguments
        :return: a dictionary of Slurm arguments
        """
        slurm_args = {}
        slurm_args['nodelist'] = self.args.nodelist
        slurm_args['mem'] = self.args.memory_mb
        slurm_args['partition'] = self.args.partition
        gpu_gres_value = "gpu"
        if self.args.gpu_type:
            gpu_gres_value += ":" + self.args.gpu_type
        gpu_gres_value += ":" + str(self.args.gpus)
        slurm_args['gres'] = gpu_gres_value
        # job-name is unique
        slurm_args['job-name'] = worker_id
        slurm_args['cpus-per-task'] = str(self.args.cpus)
        slurm_args['ntasks-per-node'] = 1
        slurm_args['time'] = self.args.time
        slurm_args['open-mode'] = 'append'
        slurm_args['output'] = str(Path(slurm_worker_dir, slurm_args['job-name'] + '.out'))
        return slurm_args
