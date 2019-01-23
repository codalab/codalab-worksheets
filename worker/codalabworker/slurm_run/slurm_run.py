"""
Executable python file that runs a Codalab RunBundle meant to be submitted to a job management environment
like Slurm
"""

from argparse import ArgumentParser
from codalabworker.bundle_state import State
from codalabworker.bundle_state_objects import BundleInfo, RunResources, WorkerRun
from codalabworker.file_util import get_path_size
from codalabworker.formatting import duration_str, size_str
from codalabworker import docker_utils
import docker
import errno
import fcntl
import json
import os
import threading
import time
import traceback


def parse_args():
    parser = ArgumentParser()
    parser.add_argument("--bundle-file", type=str)
    parser.add_argument("--resources-file", type=str)
    parser.add_argument("--state-file", type=str)
    parser.add_argument("--lock-file", type=str)
    args = parser.parse_args()
    return args


def main():
    args = parse_args()
    run = SlurmRun(args)
    run.start()


class SlurmRun(object):

    def __init__(self, args):
        with open(args.bundle_file, "r") as infile:
            bundle_dict = json.load(infile)
            self.bundle = BundleInfo.from_dict(bundle_dict)
        with open(args.resources_file, "r") as infile:
            resources_dict = json.load(infile)
            self.resources = RunResources.from_dict(resources_dict)
        self.run_state = WorkerRun(
            uuid=self.bundle.uuid,
            run_status="Starting run",
            start_time=time.time(),
            docker_image=self.resources.docker_image,
            info={},
            state=State.PREPARING,
        )
        self.docker_client = docker.from_env()
        self.docker_runtime = "nvidia" if self.resources.gpus else "runc"
        if self.resources.network:
            self.docker_network_name = "codalab-external"
            internal = False
        else:
            self.docker_network_name = "codalab-internal"
            internal = True
        try:
            self.docker_client.networks.create(
                self.docker_network_name, internal=internal, check_duplicate=True
            )
        except docker.errors.APIError:
            # Network already exists, go on
            pass
        self.UPDATE_INTERVAL = 5

    def start(self):
        try:
            self.run_state.run_status = "Pulling docker image {}".format(self.resources.docker_image)
            self.write_state()
            try:
                self.run_state.docker_image = self.pull_docker_image()
            except Exception as ex:
                # TODO Set failure messages ie to be docker
                print("Docker image download failed: {}".format(ex))
                raise
            self.run_state.run_status = "Preparing the filesystem"
            self.write_state()
            self.wait_for_bundle_folder()
            self.container = self.start_container()
            self.run_state.run_status = "Running job in Docker"
            self.run_state.state = State.RUNNING
            self.write_state()
            self.monitor_container()
            self.run_state.run_status = "Execution finished. Uploading results"
            self.write_state()
        except Exception as ex:
            # TODO fail the image with ex
            print("Run failed: {}".format(ex))

    def write_state(self):
        while True:
            try:
                lock_file = open(self.lock_file, "w+")
                fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except IOError as ex:
                if ex.errno != errno.EAGAIN:
                    raise
                else:
                    time.sleep(0.1)
        with open(self.state_file, "wb") as f:
            json.dump(self.run_state.__dict__, f)
        fcntl.flock(lock_file, fcntl.LOCK_UN)
        lock_file.close()

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
            raise Exception("Bundle location {} not found".format(self.bundle.location))

    def start_container(self):
        """
        Symlinks to dependencies on the filesystem, sets docker runtime and network and starts the container
        """
        # Symlink dependencies
        docker_dependencies = []
        docker_dependencies_path = "/" + self.bundle.uuid + "_dependencies"
        for dep_key, dep in self.bundle.dependencies.items():
            child_path = os.path.normpath(os.path.join(self.bundle.location, dep.child_path))
            if not child_path.startswith(self.bundle.location):
                raise Exception("Invalid key for dependency: %s" % (dep.child_path))

            docker_dependency_path = os.path.join(docker_dependencies_path, dep.child_path)
            os.symlink(docker_dependency_path, child_path)
            dependency_path = os.path.realpath(
                os.path.join(os.path.realpath(dep.location), dep.parent_path)
            )
            # These are turned into docker volume bindings like:
            #   dependency_path:docker_dependency_path:ro
            docker_dependencies.append((dependency_path, docker_dependency_path))

        return docker_utils.start_bundle_container(
            self.bundle.location,
            self.bundle.uuid,
            docker_dependencies,
            self.bundle.command,
            self.resources.docker_image,
            network=self.docker_network_name,
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
            while not self.run_state.info['finished']:
                start_time = time.time()
                try:
                    self.run_state.info['disk_utilization'] = get_path_size(self.bundle.location)
                except Exception:
                    traceback.print_exc()
                end_time = time.time()

                # To ensure that we don't hammer the disk for this computation when
                # there are lots of files, we run it at most 10% of the time.
                time.sleep(max((end_time - start_time) * 10, 1.0))
        self.disk_utilization_thread = threading.Thread(target=check_disk_utilization, args=[])
        self.disk_utilization_thread.start()

        def check_resource_utilization():
            """.
            Checks the time, memory and disk use of the container, setting it up to be killed
            if it is going over its allocated resources
            :returns: List[str]: Kill messages with reasons to kill if there are reasons, otherwise empty
            """
            kill_messages = []

            run_stats = docker_utils.get_container_stats(self.container)
            time_used = time.time() - self.run_state.start_time

            self.run_state.info.update(dict(
                time_used=time_used,
                max_memory=max(self.run_state.info['max_memory'], run_stats.get('memory', 0))
            ))

            if (
                self.resources.request_time
                and self.run_state.info['time_used'] > self.resources.request_time
            ):
                kill_messages.append(
                    'Time limit %s exceeded.' % duration_str(self.resources.request_time)
                )

            if self.run_state.info['max_memory'] > self.resources.request_memory:
                kill_messages.append(
                    'Memory limit %s exceeded.'
                    % size_str(self.resources.request_memory)
                )

            if (
                self.resources.request_disk
                and self.run_state.info['disk_utilization'] > self.resources.request_disk
            ):
                kill_messages.append(
                    'Disk limit %sb exceeded.' % size_str(self.resources.request_disk)
                )

            return kill_messages

        finished = False
        while not self.killed and not finished:
            time.sleep(self.UPDATE_INTERVAL)
            finished, exitcode, failure_message = self.check_and_report_finished()
            kill_messages = self.check_resource_utilization()
            if kill_messages:
                self.killed = True

            if self.killed:
                try:
                    self.container.kill()
                except docker.errors.APIError:
                    finished, _, _ = docker_utils.check_finished(self.container)
                    if not finished:
                        # If we can't kill a Running container, something is wrong
                        # Otherwise all well
                        traceback.print_exc()
        self.disk_utilization_thread.join()


if __name__ == "__main__":
    main()
