#!/usr/bin/env python
"""
Script that's designed for individual users of a Slurm managed cluster to run on a machine
with srun access. Once the user starts the script and logs in, the script fires a daemon that
busy loops, querying the given CodaLab instance for staged bundles belonging to the user.
Every time there's a staged bundle, its CodaLab resource requests (gpu, cpu, memory etc)
are converted to srun options and a new worker with that many resources is fired up on slurm.
These workers die when they're idle.

Some values are Stanford NLP cluster specific (CodaLab instance used, worker binary locations),
but can be configured for other Slurm clusters. The script just requires codalab-worker to be installed
on all slurm machines on a consitent location.
"""

import argparse
import atexit
import errno
import getpass
import math
import os
import subprocess
import stat
import sys
import threading
import time
from signal import SIGTERM


FIELDS = ['uuid', 'request_cpus', 'request_gpus', 'request_memory', 'request_time', 'tags']


class Daemon:
    """
    A generic daemon class.
    Usage: subclass the Daemon class and override the run() and cleanup() methods
    Source: http://www.jejik.com/articles/2007/02/a_simple_unix_linux_daemon_in_python/
    """

    def __init__(
        self, pidfile, chdir='/', stdin='/dev/null', stdout='/dev/null', stderr='/dev/null'
    ):
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.pidfile = pidfile
        self.chdir = chdir
        self.last_args = []
        self.last_kwargs = {}

    def daemonize(self):
        """
        Do the UNIX double-fork magic, see Stevens' "Advanced
        Programming in the UNIX Environment" for details (ISBN 0201563177)
        http://www.erlenstar.demon.co.uk/unix/faq_2.html#SEC16
        """
        try:
            pid = os.fork()
            if pid > 0:
                # exit first parent
                sys.exit(0)
        except OSError as e:
            sys.stderr.write("fork #1 failed: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1)

        # decouple from parent environment
        os.chdir(self.chdir)
        os.setsid()
        os.umask(0)

        # do second fork
        try:
            pid = os.fork()
            if pid > 0:
                # exit from second parent
                sys.exit(0)
        except OSError as e:
            sys.stderr.write("fork #2 failed: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1)

        # redirect standard file descriptors
        sys.stdout.flush()
        sys.stderr.flush()
        si = file(self.stdin, 'r')
        so = file(self.stdout, 'a+')
        se = file(self.stderr, 'a+', 0)
        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())

        # write pidfile
        atexit.register(self.exit)
        pid = str(os.getpid())
        with open(self.pidfile, 'w+') as pidfile:
            pidfile.write("%s\n" % pid)

    def exit(self):
        self.cleanup()
        os.remove(self.pidfile)

    def start(self, *args, **kwargs):
        """
        Start the daemon
        """
        # Check for a pidfile to see if the daemon already runs
        try:
            pf = file(self.pidfile, 'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None

        if pid:
            message = "pidfile %s already exist. Daemon already running?\n"
            sys.stderr.write(message % self.pidfile)
            sys.exit(1)

        # Start the daemon
        self.daemonize()
        self.last_args = args
        self.last_kwargs = kwargs
        self.run(*args, **kwargs)

    def stop(self):
        """
        Stop the daemon
        """
        # Get the pid from the pidfile
        try:
            pf = file(self.pidfile, 'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None

        if not pid:
            message = "pidfile %s does not exist. Daemon not running?\n"
            sys.stderr.write(message % self.pidfile)
            return  # not an error in a restart

        # Try killing the daemon process
        try:
            while 1:
                os.kill(pid, SIGTERM)
                time.sleep(0.1)
        except OSError as err:
            err = str(err)
            if err.find("No such process") > 0:
                if os.path.exists(self.pidfile):
                    os.remove(self.pidfile)
            else:
                print(str(err))
                sys.exit(1)

    def restart(self):
        """
        Restart the daemon
        """
        self.stop()
        self.start(*self.last_args, **self.last_kwargs)

    def run(self, *args, **kwargs):
        """
        You should override this method when you subclass Daemon. It will be called after the process has been
        daemonized by start() or restart().
        """
        raise NotImplementedError

    def cleanup(self):
        """
        Do the necessary cleanup before exiting
        """
        raise NotImplementedError


class SlurmWorkerDaemon(Daemon):
    def __init__(self, daemon_dir):
        pidfile = os.path.join(daemon_dir, 'worker.pid')
        stdout = os.path.join(daemon_dir, 'worker.stdout.log')
        stderr = os.path.join(daemon_dir, 'worker.stderr.log')
        self.daemon_dir = daemon_dir
        self.password_file = os.path.join(daemon_dir, 'worker.password')
        self.worker_parent_dir = os.path.join(daemon_dir, 'workers')
        self.worker_threads = []  # type: List[threading.Thread]
        Daemon.__init__(self, pidfile, chdir=daemon_dir, stdout=stdout, stderr=stderr)

    def login(self, args):
        """
        Log in to the CLI
        Also ensure the password_file in args exists with the correct permissions so workers may be easily
        created in the future
        """

        # Prepare the log files
        def make_parent_dir(filepath):
            """
            Make all the directories in filepath until the leaf is reached
            Raises an error if a non-directory file exists by the same name
            as one of the directories in the filesystem (or if the directories
            cannot be created for some other reason). Quietly exits if the
            directories already exist.
            """
            dirpath = os.path.dirname(filepath)
            try:
                os.makedirs(dirpath)
            except OSError as exc:
                if not exc.errno == errno.EEXIST:
                    raise exc
                elif not os.path.isdir(dirpath):
                    raise IOError(
                        'Directory in a given path exists but is not a directory: (%s in %s)'
                        % (dirpath, filepath)
                    )

        def reset_file(filepath):
            """
            If the given filepath exists, appends '.old' to its filename
            """
            if os.path.isfile(filepath):
                os.rename(filepath, '{}.old'.format(filepath))

        for path in (
            self.stdin,
            self.stdout,
            self.stderr,
            self.pidfile,
            self.password_file,
            self.worker_parent_dir,
        ):
            make_parent_dir(path)

        for path in (self.stdout, self.stderr):
            reset_file(path)

        logged_in = False
        while not logged_in:
            cli_login_output = subprocess.check_output(
                '{} work {}::'.format(args.cl_binary, args.server_instance), shell=True
            )
            logged_in = 'Invalid' not in cli_login_output

        if os.path.isfile(self.password_file):
            if os.stat(self.password_file).st_mode & (stat.S_IRWXG | stat.S_IRWXO):
                os.chmod(self.password_file, 0o600)
        else:
            print("No password file for workers, getting from user")
            username = os.environ.get('CODALAB_USERNAME')
            if username is None:
                username = raw_input('Username: ')
            password = os.environ.get('CODALAB_PASSWORD')
            if password is None:
                password = getpass.getpass()
            with open(self.password_file, 'w+') as password_file:
                password_file.write('{0}\n{1}'.format(username, password))
            os.chmod(self.password_file, 0o600)

        print('Logged in to {}'.format(args.server_instance))

    def print_logs(self, tail=None):
        """
        Print the logs of this daemon.
        :param tail: If specified print last N lines from both STDERR and STDOUT
        """
        with open(self.stdout, 'r') as stdout, open(self.stderr, 'r') as stderr:
            stdout_lines = stdout.readlines()
            stderr_lines = stderr.readlines()
            if tail:
                stdout_lines = stdout_lines[-tail:]
                stderr_lines = stderr_lines[-tail:]
            stdout = os.linesep.join(stdout_lines)
            stderr = os.linesep.join(stderr_lines)
            print(">>>>>>STDOUT")
            print(stdout)
            print(">>>>>>STDERR")
            print(stderr)

    def run(self, args):
        """
        Run the daemon, expect the CLI to be logged in to the given server instance already
        """
        self.cl_binary = args.cl_binary
        self.cl_worker_binary = args.cl_worker_binary
        self.server_instance = args.server_instance
        self.srun_binary = args.srun_binary
        self.sleep_interval = args.sleep_interval
        self.max_concurrent_workers = args.max_concurrent_workers
        self.worker_threads = []  # type: List[threading.Thread]

        # Cache runs that we started workers for for one extra iteration in case they're still staged
        # during the next iteration as worker booting might take some time. Un-cache them after one
        # iteration to start booting new workers for them
        cooldown_runs = set()
        subprocess.check_call(
            '{} work {}::'.format(self.cl_binary, self.server_instance), shell=True
        )
        status = subprocess.check_output('{} status'.format(self.cl_binary), shell=True)
        print("Starting daemon, status: {}".format(status))
        while True:
            # Prune finished threads
            self.worker_threads = [thread for thread in self.worker_threads if thread.is_alive()]
            run_lines = subprocess.check_output(
                '{} search .mine state=staged -u'.format(self.cl_binary), shell=True
            )
            try:
                # Make python3 compatible by decoding if we can
                run_lines = run_lines.decode('utf-8')
            except Exception:
                pass
            uuids = run_lines.splitlines()
            for uuid in uuids:
                if len(self.worker_threads) > self.max_concurrent_workers:
                    print(
                        "Maximum number of concurrent workers reached, waiting for some to finish"
                    )
                    break
                if uuid not in cooldown_runs:
                    info_cmd = '{} info {} -f {}'.format(self.cl_binary, uuid, ','.join(FIELDS))
                    field_values = subprocess.check_output(info_cmd, shell=True)
                    try:
                        # Make python3 compatible by decoding if we can
                        field_values = field_values.decode('utf-8')
                    except Exception:
                        pass
                    run = {
                        field: SlurmWorkerDaemon.parse_field(field, val)
                        for field, val in zip(FIELDS, field_values.split())
                    }

                    def worker():
                        self.start_worker_for(run)

                    worker_thread = threading.Thread(target=worker)
                    self.worker_threads.append(worker_thread)
                    worker_thread.start()
                    cooldown_runs.add(uuid)
                else:
                    print(
                        "Previous worker for run {} hasn't been successful, uncaching it".format(
                            uuid
                        )
                    )
                    cooldown_runs.remove(uuid)

            time.sleep(self.sleep_interval)

    def cleanup(self):
        """
        Do the necessary cleanup before exiting
        """
        print("Waiting for all the current workers to finish")
        for thread in self.worker_threads:
            thread.join()

    def start_worker_for(self, run_fields):
        """
        Start a worker suitable to run the given run with run_fields
        with the given run_number for the worker directory.
        This function makes the actual command call to start the job on Slurm.
        """
        current_directory = os.getcwd()
        worker_name = 'worker-{}'.format(run_fields['uuid'])
        tag = None
        if run_fields['request_gpus']:
            if 'jag_hi' in run_fields['tags']:
                partition = 'jag-hi'
                tag = 'jag-hi'
            else:
                partition = 'jag-lo'
        else:
            partition = 'john'

        srun_flags = [
            self.srun_binary,
            '--partition={}'.format(partition),
            '--mem={}'.format(run_fields['request_memory']),
            '--gres=gpu:{}'.format(run_fields['request_gpus']),
            '--chdir={}'.format(current_directory),
            '--nodes 1',
        ]

        if run_fields['request_cpus'] > 1:
            srun_flags.append('--cpus-per-task={}'.format(run_fields['request_cpus']))

        srun_command = ' '.join(srun_flags)
        worker_command_script = self.write_worker_invocation(
            worker_name=worker_name,
            tag=tag,
            num_cpus=run_fields['request_cpus'],
            num_gpus=run_fields['request_gpus'],
            verbose=True,
        )
        final_command = '{} bash {}'.format(srun_command, worker_command_script)
        print('Starting worker for run {}'.format(run_fields['uuid']))
        try:
            subprocess.check_call(final_command, shell=True)
        except Exception as e:
            print('Anomaly in worker run: {}'.format(e))
        finally:
            try:
                os.remove(worker_command_script)
            except Exception as ex:
                print('Anomaly when trying to remove old worker script: {}'.format(ex))

    def write_worker_invocation(
        self, worker_name='worker', tag=None, num_cpus=1, num_gpus=0, verbose=False
    ):
        """
        Return the name to a local file that has the commands that prepare the worker work dir,
        run the worker and clean up the work dir upon exit.
        """
        work_dir = os.path.join(self.worker_parent_dir, worker_name)
        prepare_command = 'mkdir -vp {0} && mkdir -vp {0}/runs;'.format(work_dir)
        cleanup_command = 'rm -rf {};'.format(work_dir)
        flags = [
            '--exit-when-idle',
            '--server {}'.format(self.server_instance),
            '--password {}'.format(self.password_file),
            '--cpuset {}'.format(','.join(str(idx) for idx in range(num_cpus))),
            '--work-dir {}'.format(work_dir),
        ]

        if tag is not None:
            flags.append('--tag {}'.format(tag))
        if verbose:
            flags.append('--verbose')
        worker_command = '{} {};'.format(self.cl_worker_binary, ' '.join(flags))
        with open('start-{}.sh'.format(worker_name), 'w') as script_file:
            script_file.write(prepare_command)
            script_file.write(worker_command)
            script_file.write(cleanup_command)
        return script_file.name

    @staticmethod
    def parse_duration(dur):
        """
        s: <number>[<s|m|h|d|y>]
        Returns the number of minutes
        """
        try:
            if dur[-1].isdigit():
                return math.ceil(float(dur) / 60.0)
            n, unit = float(dur[0:-1]), dur[-1].lower()
            if unit == 's':
                return math.ceil(n / 60.0)
            if unit == 'm':
                return n
            if unit == 'h':
                return n * 60
            if unit == 'd':
                return n * 60 * 24
            if unit == 'y':
                return n * 60 * 24 * 365
        except (IndexError, ValueError):
            pass  # continue to next line and throw error
        raise ValueError('Invalid duration: %s, expected <number>[<s|m|h|d|y>]' % dur)

    @staticmethod
    def parse_field(field, val):
        """
        Parses cl's printed bundle info fields into typed constructs
        Currently converts request_time to minutes as that's what slurm
        takes in
        """
        if field == 'request_time':
            return SlurmWorkerDaemon.parse_duration(val)
        elif field in ['request_cpus', 'request_gpus']:
            return int(val)
        else:
            return val

    @staticmethod
    def list_instances(script_dir):
        print('Active slurm worker daemons:')
        print('{:^10} {:^10} {:^10}'.format('Name', 'Run Status', 'Pid'))
        print('-' * 32)
        for name, running, pid in SlurmWorkerDaemon.get_instances(script_dir):
            status = 'Running' if running else 'Stopped'
            print('{:<10} {:<10} {:<10}'.format(name, status, pid))

    @staticmethod
    def get_instances(script_dir):
        """
        List all current known instances of Slurm worker.
        This works off the assumption that all instances are subdirectories of the daemon
        directory, and subsequently cannot catch instances that are started with a different
        script_dir argument.
        Returns a list of tuples where the first element is the name, the second run status and the third the pid
        """
        instances = []
        for instance_dir in os.listdir(script_dir):
            try:
                with open(os.path.join(script_dir, instance_dir, 'worker.pid'), 'r') as pidfile:
                    pid = pidfile.read().strip()
                    try:
                        # send SIG 0 to check if PID exists
                        os.kill(pid, 0)
                        is_running = True
                    except OSError as err:
                        if err.errno == errno.ESRCH:
                            # ESRCH == No such process
                            is_running = False
                        elif err.errno == errno.EPERM:
                            # EPERM clearly means there's a process to deny access to
                            is_running = True
                        else:
                            # According to "man 2 kill" possible error values are
                            # (EINVAL, EPERM, ESRCH)
                            raise
                instances.append((instance_dir, is_running, pid))
            except (IOError, OSError):
                continue
        return instances


def parse_args():
    home = os.environ.get('HOME')
    parser = argparse.ArgumentParser(
        description='Controller for the CodaLab Slurm Worker daemon. The daemon logs in to the specified CodaLab instance with your credentials '
        'and continuously polls for runs. Every time it encounters a stage run, it launches a Slurm job that contains a single-use CodaLab '
        'worker that has access to the resources requested in the bundle. The daemon needs to be started only once, and it keeps starting '
        'new workers as needed until it is stopped with the stop command.'
    )
    parser.add_argument(
        'action',
        type=str,
        choices={'start', 'stop', 'restart', 'logs', 'list', 'status'},
        help='start, stop or restart the daemon or use logs to print the logs. Use list or status to list all known daemons for this user',
    )
    parser.add_argument(
        '--name',
        type=str,
        help='Name of the daemon instance to be worked on. If you want to run multiple instances of Slurm Worker (e.g. one for the internal CodaLab instance and one for the public CodaLab instance), name the latter ones differently using this argument. You need to use the same name when using any of the daemon commands for a given instance (start, stop, logs, restart)',
        default='default',
    )
    parser.add_argument(
        '--server-instance',
        type=str,
        help='Codalab server to run the workers for',
        default='https://worksheets.codalab.org',
    )
    parser.add_argument(
        '--max-concurrent-workers',
        type=int,
        help='Maximum number of concurrent workers this script will launch (default 10)',
        default=10,
    )
    parser.add_argument(
        '--sleep-interval',
        type=int,
        help='Number of seconds to wait between each time the server is polled for new runs (default 30)',
        default=30,
    )
    parser.add_argument(
        '--srun-binary',
        type=str,
        help='Where the binary for the srun command lives (default is the location on the Stanford NLP cluster)',
        default='srun',
    )
    parser.add_argument(
        '--cl-worker-binary',
        type=str,
        help='Where the cl-worker binary lives (default is the location on the Stanford NLP cluster',
        default='/u/nlp/bin/cl-worker',
    )
    parser.add_argument(
        '--cl-binary',
        type=str,
        help='Where the cl (Codalab CLI) binary lives (default is the location on the Stanford NLP cluster',
        default='/u/nlp/bin/cl',
    )
    parser.add_argument(
        '--script-dir',
        type=str,
        help='ONLY FOR ADVANCED USE: Base directory to store daemon files, if changed for one invocation needs to be changed for any future invocation. Do not change if you do not know what you are doing.',
        default=os.path.join(home, '.cl_slurm_worker'),
    )
    parser.add_argument(
        '--tail', type=int, help='If specified only print this many lines from logs', default=None
    )
    return parser.parse_args()


def main():
    args = parse_args()
    daemon_dir = os.path.join(args.script_dir, args.name)
    daemon = SlurmWorkerDaemon(daemon_dir)
    if args.action == 'start':
        # Login to the server given in the args before we daemonize
        daemon.login(args)
        daemon.start(args)
    elif args.action == 'stop':
        daemon.stop()
    elif args.action == 'restart':
        # if daemon wasn't started previously, fail
        if not daemon.last_args:
            print(
                "Trying to restart a non-started daemon, please use start with the rest of your args"
            )
            sys.exit(2)
        # Login to the server we last logged in to before we daemonize
        daemon.login(*daemon.last_args)
        daemon.restart()
    elif args.action == 'logs':
        daemon.print_logs(args.tail)
    elif args.action == 'list' or args.action == 'status':
        daemon.list_instances(args.script_dir)
    else:
        print("Unknown command %s" % args.action)
        sys.exit(2)
    sys.exit(0)


if __name__ == '__main__':
    main()
