"""
Executable python file that runs a Codalab RunBundle meant to be submitted to a job management environment
like Slurm
"""

from argparse import ArgumentParser
from codalabworker.bundle_state import State
from codalabworker.bundle_state_objects import BundleInfo, RunResources, WorkerRun
from codalabworker.file_util import get_path_size, remove_path
from codalabworker.formatting import duration_str, size_str
from codalabworker.slurm_run.slurm_run_manager import KILLED_STRING
from codalabworker import docker_utils
import docker
import errno
import fcntl
import json
import os
import shutil
import signal
import subprocess
import threading
import time
import traceback


def parse_args():
    parser = ArgumentParser()
    parser.add_argument("--bundle-file", type=str)
    parser.add_argument("--resources-file", type=str)
    parser.add_argument("--state-file", type=str)
    parser.add_argument("--kill-command-file", type=str)
    parser.add_argument("--docker-network-internal-name", type=str)
    parser.add_argument("--docker-network-external-name", type=str)
    args = parser.parse_args()
    return args


def main():
    args = parse_args()
    run = SlurmRun(args)
    # Slurm sends a SIGTERM when jobs are preempted
    signal.signal(signal.SIGTERM, lambda signum, frame: run.handle_term())
    run.start()


class KilledException(Exception):
    pass


class SlurmRun(object):
    # Default cache size is 30GB
    CACHE_SIZE = 1024 * 1024 * 1024 * 30
    def __init__(self, args):
        with open(args.bundle_file, "r") as infile:
            bundle_dict = json.load(infile)
            self.bundle = BundleInfo.from_dict(bundle_dict)
            print("====BUNDLE=====")
            print(self.bundle)
        with open(args.resources_file, "r") as infile:
            resources_dict = json.load(infile)
            self.resources = RunResources.from_dict(resources_dict)

        self.state_file = args.state_file
        self.kill_command_file = args.kill_command_file

        self.run_state = WorkerRun(
            uuid=self.bundle.uuid,
            run_status="Starting run",
            start_time=time.time(),
            docker_image=self.resources.docker_image,
            info={},  # should have 'exitcode' and 'failure_message' fields if run fails
            state=State.PREPARING,
        )
        self.docker_client = docker.from_env()
        self.docker_runtime = "nvidia" if self.resources.gpus else "runc"
        self.docker_network = self.docker_client.networks.create(
            "%s-net" % self.bundle.uuid, internal=self.resources.network
        )
        self.UPDATE_INTERVAL = 5
        self.finished = False
        self.killed = False
        self.kill_message = None
        self.has_contents = False
        self.resource_use = {"disk": 0, "time": 0, "memory": 0}

    def get_gpus(self):
        if self.resources.gpus > 0:
            self.gpus = subprocess.check_output('nvidia-smi --query-gpu=uuid --format=csv,noheader', shell=True).split('\n')[:-1]
        else:
            self.gpus = []

    def handle_term(self):
        self.kill_message = "Job preempted by Slurm"
        self.killed = True

    def start(self):
        try:
            print("Run started")
            self.run_state.run_status = "Propagating resource requests"
            print(self.run_state.run_status)
            self.write_state()
            self.get_gpus()
            self.run_state.run_status = "Pulling docker image {}".format(
                self.resources.docker_image
            )
            print(self.run_state.run_status)
            self.write_state()
            self.run_state.docker_image = self.pull_docker_image()
            self.run_state.run_status = "Preparing the filesystem"
            print(self.run_state.run_status)
            self.write_state()
            self.wait_for_bundle_folder()
            self.run_state.run_status = "Starting container"
            self.container = self.start_container()
            self.run_state.info['container_id'] = self.container.id
            self.run_state.info['container_ip'] = docker_utils.get_container_ip(
                self.docker_network.name, self.container
            )
            self.run_state.run_status = "Running job in Docker"
            print(self.run_state.run_status)
            self.run_state.state = State.RUNNING
            self.write_state()
            self.monitor_container()
        except Exception as ex:
            self.container = None
            if 'container_ip' in self.run_state.info:
                del self.run_state.info['container_ip']
            if 'container_id' in self.run_state.info:
                del self.run_state.info['container_id']
            if 'exitcode' not in self.run_state.info:
                self.run_state.info['exitcode'] = '1'
            if not self.kill_message:
                self.kill_message = str(ex)
            self.run_state.info['failure_message'] = "Error while %s: %s" % (self.run_state.run_status, self.kill_message)
            self.run_state.state = State.FINALIZING
            self.finished = True
            self.write_state(no_except=True)
            print("Run failed: {}".format(ex))
        self.run_state.run_status = "Execution finished. Cleaning up."
        print(self.run_state.run_status)
        self.write_state(no_except=True)
        self.cleanup()
        self.run_state.run_status = "Run finished, finalizing"
        print(self.run_state.run_status)
        self.run_state.state = State.FINALIZING
        self.write_state(no_except=True)

    def write_state(self, no_except=False):
        with open(self.state_file, "wb") as f:
            json.dump(self.run_state.__dict__, f)
        if not self.killed:
            with open(self.kill_command_file, 'r') as f:
                if f.read() == KILLED_STRING:
                    print("Kill message received")
                    self.killed = True
                    self.kill_message = 'Kill requested by server'
        if self.killed and not no_except:
            self.run_state.info['exitcode'] = 1
            raise KilledException()

    def pull_docker_image(self):
        """
        Pulls the image specified in image_spec
        Tags the image with:
        - codalab-image-cache/dependents:<uuid>
        - codalab-image-cache/last-used:<timestamp-of-now>
        Removes previous timestamp tag if there is one
        Return the sha256 digest of the image
        """
        try:
            image = self.docker_client.images.get(self.resources.docker_image)
        except docker.errors.ImageNotFound:
            self.docker_client.images.pull(self.resources.docker_image)
            image = self.docker_client.images.get(self.resources.docker_image)
        # Tag the image to save that this bundle is dependent on this image
        image.tag("codalab-image-cache/dependents", tag=self.bundle.uuid)
        # Timestamp the image using a tag, removing the old timestamp if there is one
        timestamp = str(time.time())
        for tag in image.tags:
            if tag.split(":")[0] == "codalab-image-cache/last-used":
                self.docker_client.images.remove(tag)
        image.tag("codalab-image-cache/last-used", tag=timestamp)
        return image.attrs.get("RepoDigests", [self.resources.docker_image])[0]

    def wait_for_bundle_folder(self):
        """
        To avoid NFS directory caching issues, the bundle manager creates
        the bundle folder on shared filesystems. Here we wait for the cache
        to be renewed on the worker machine and for the folder to show up
        """
        retries_left = 120
        while not os.path.exists(self.bundle.location) and retries_left > 0:
            retries_left -= 1
            time.sleep(0.5)
        if not retries_left:
            self.kill_message = "Bundle location {} not found".format(self.bundle.location)
            raise Exception()

    def start_container(self):
        """
        Symlinks to dependencies on the filesystem, sets docker runtime and network and starts the container
        """
        # Symlink dependencies
        docker_dependencies = []
        docker_dep_prefix = '/' + self.bundle.uuid + '/'
        for dep_key, dep in self.bundle.dependencies.items():
            full_child_path = os.path.normpath(
                os.path.join(self.bundle.location, dep.child_path)
            )
            if not full_child_path.startswith(self.bundle.location):
                self.kill_message = "Invalid key for dependency: %s" % (dep.full_child_path)
                raise Exception()
            child_path = os.path.join(docker_dep_prefix, dep.child_path)

            dependency_path = os.path.realpath(
                os.path.join(os.path.realpath(dep.location), dep.parent_path)
            )
            # These are turned into docker volume bindings like:
            #   dependency_path:child_path:ro
            docker_dependencies.append((dependency_path, child_path))
        return docker_utils.start_bundle_container(
            self.bundle.location,
            self.bundle.uuid,
            docker_dependencies,
            self.bundle.command,
            self.resources.docker_image,
            gpuset=self.gpus,
            network=self.docker_network.name,
            memory_bytes=self.resources.memory,
            runtime=self.docker_runtime,
        )

    def monitor_container(self):
        """
        Monitors the Docker container for the run, checking resource usage and killing it
        if it uses too many resources
        Returns when the docker container is finished
        """

        def check_disk_utilization():
            while not self.finished and not self.killed:
                start_time = time.time()
                try:
                    self.resource_use["disk"] = get_path_size(self.bundle.location)
                except Exception:
                    traceback.print_exc()
                end_time = time.time()

                # To ensure that we don't hammer the disk for this computation when
                # there are lots of files, we run it at most 10% of the time.
                time.sleep(max((end_time - start_time) * 10, 1.0))

        disk_utilization_thread = threading.Thread(
            target=check_disk_utilization, args=[]
        )
        disk_utilization_thread.start()

        def check_resource_utilization():
            """
            Checks the time, memory and disk use of the container, setting it up to be killed
            if it is going over its allocated resources
            :returns: List[str]: Kill messages with reasons to kill if there are reasons, otherwise empty
            """
            kill_messages = []

            run_stats = docker_utils.get_container_stats(self.container)
            self.resource_use["time"] = time.time() - self.run_state.start_time
            self.resource_use["memory"] = max(
                self.resource_use["memory"], run_stats.get("memory", 0)
            )

            if (
                self.resources.time
                and self.resource_use["time"] > self.resources.time
            ):
                kill_messages.append(
                    "Time limit %s exceeded."
                    % duration_str(self.resources.time)
                )

            if self.resource_use["memory"] > self.resources.memory:
                kill_messages.append(
                    "Memory limit %s exceeded."
                    % size_str(self.resources.memory)
                )

            if (
                self.resources.disk
                and self.resource_use["disk"]
                > self.resources.disk
            ):
                kill_messages.append(
                    "Disk limit %sb exceeded." % size_str(self.resources.disk)
                )

            return kill_messages

        while not self.killed and not self.finished:
            time.sleep(self.UPDATE_INTERVAL)
            self.finished, exitcode, failure_message = docker_utils.check_finished(self.container)
            kill_messages = check_resource_utilization()
            try:
                self.write_state()
            except KilledException as ex:
                kill_messages.append(self.kill_message)
            if kill_messages:
                self.killed = True
                self.kill_message = ', '.join(kill_messages)
                self.run_state.info['exitcode'] = 1
                self.run_state.info['failure_message'] = self.kill_message

            if self.killed:
                try:
                    self.container.kill()
                except docker.errors.APIError:
                    self.finished, _, _ = docker_utils.check_finished(self.container)
                    if not self.finished:
                        # If we can't kill a Running container, something is wrong
                        # Otherwise all well
                        traceback.print_exc()
        disk_utilization_thread.join()

    def clean_docker_cache(self):
        """
        1. Untag your own image with your dependency
        2. Get all images with tag "codalab-image-cache"
        3. While their sum > 30G:
            4. Delete oldest image whose tags don't include dependents
        """
        # 1
        self.docker_client.images.remove("codalab-image-cache/dependents:{}".format(self.bundle.uuid))
        # 2
        def last_used(image):
            for tag in image.tags:
                if tag.split(":")[0] == "codalab-image-cache/last-used":
                    return float(tag.split(":")[1])
        all_images = self.docker_client.images.list("codalab-image-cache/last-used")
        for image in sorted(all_images, key=last_used):
            cache_use = sum(float(image.attrs['VirtualSize']) for image in all_images)
            if cache_use > self.CACHE_SIZE:
                for tag in image.tags:
                    if tag.split(":")[0] == "codalab-image-cache/dependents":
                        # This image still has dependents, don't attempt to delete
                        continue
                    for tag in image.tags:
                        self.docker_client.images.remove(tag)



    def cleanup(self):
        # Make sure the container is removed
        if self.container:
            try:
                self.container.remove()
            except Exception:
                traceback.print_exc()
        # Remove the dependency symlinks from the bundle folder
        for dep_key, dep in self.bundle.dependencies.items():
            child_path = os.path.normpath(
                os.path.join(self.bundle.location, dep.child_path)
            )
            try:
                remove_path(child_path)
            except Exception:
                traceback.print_exc()
        self.docker_network.remove()
        self.clean_docker_cache()


if __name__ == "__main__":
    main()
