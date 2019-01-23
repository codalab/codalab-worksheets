"""
Executable python file that runs a Codalab RunBundle meant to be submitted to a job management environment
like Slurm
"""

from argparse import ArgumentParser
from codalabworker.bundle_state import State
from codalabworker.bundle_state_objects import BundleInfo, RunResources, WorkerRun
from codalabworker import docker_utils
import docker
import errno
import fcntl
import json
import os
import time


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
    run_bundle(args)


def write_state(state, state_file, lock_file):
    while True:
        try:
            lock_file = open(lock_file, "w+")
            fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
            break
        except IOError as ex:
            if ex.errno != errno.EAGAIN:
                raise
            else:
                time.sleep(0.1)
    with open(state_file, "wb") as f:
        json.dump(state.__dict__, f)
    fcntl.flock(lock_file, fcntl.LOCK_UN)
    lock_file.close()


def run_bundle(args):
    with open(args.bundle_file, "r") as infile:
        bundle_dict = json.load(infile)
        bundle = BundleInfo.from_dict(bundle_dict)
    with open(args.resources_file, "r") as infile:
        resources_dict = json.load(infile)
        resources = RunResources.from_dict(resources_dict)
    run_state = WorkerRun(
        uuid=bundle.uuid,
        run_status="Starting run",
        start_time=time.time(),
        docker_image=resources.docker_image,
        info={},
        state=State.PREPARING,
    )
    try:
        run_state.run_status = "Pulling docker image {}".format(resources.docker_image)
        write_state(run_state, args.state_file, args.lock_file)
        try:
            run_state.docker_image = pull_docker_image(
                bundle.uuid, resources.docker_image
            )
        except Exception as ex:
            # TODO Set failure messages ie to be docker
            print("Docker image download failed: {}".format(ex))
            raise
        run_state.run_status = "Preparing the filesystem"
        write_state(run_state, args.state_file, args.lock_file)
        wait_for_bundle_folder(bundle)
        container = start_container(bundle, resources)
        run_state.run_status = "Running job in Docker"
        run_state.state = State.RUNNING
        write_state(run_state, args.state_file, args.lock_file)
    except Exception as ex:
        # TODO fail the image with ex
        print("Run failed: {}".format(ex))


def pull_docker_image(uuid, image_spec):
    """
    Pulls the image specified in image_spec
    Tags the image with:
    - codalab-image-cache/dependents:<uuid>
    - codalab-image-cache/last-used:<timestamp-of-now>
    Removes previous timestamp tag if there is one
    Return the sha256 digest of the image
    """
    docker_client = docker.from_env()
    try:
        image = docker_client.images.get(image_spec)
    except docker.errors.ImageNotFound:
        docker_client.images.pull(image_spec)
        image = docker_client.images.get(image_spec)
    # Tag the image to save that this bundle is dependent on this image
    image.tag("codalab-image-cache/dependents", tag=uuid)
    # Timestamp the image using a tag, removing the old timestamp if there is one
    timestamp = str(time.time())
    for tag in image.tags:
        if tag.split(":")[0] == "codalab-image-cache/last-used":
            docker_client.images.remove(tag)
    image.tag("codalab-image-cache/last-used", tag=timestamp)
    return image.attrs.get("RepoDigests", [image_spec])[0]


def wait_for_bundle_folder(bundle):
    """
    To avoid NFS directory caching issues, the bundle manager creates
    the bundle folder on shared filesystems. Here we wait for the cache
    to be renewed on the worker machine and for the folder to show up
    """
    retries_left = 120
    while not os.path.exists(bundle.location) and retries_left > 0:
        retries_left -= 1
        time.sleep(0.5)
    if not retries_left:
        raise Exception("Bundle location {} not found".format(bundle.location))


def start_container(bundle, resources):
    """
    Symlinks to dependencies on the filesystem, sets docker runtime and network, starts and returns the container object
    """
    docker_client = docker.from_env()
    # Symlink dependencies
    docker_dependencies = []
    docker_dependencies_path = "/" + bundle.uuid + "_dependencies"
    for dep_key, dep in bundle.dependencies.items():
        child_path = os.path.normpath(os.path.join(bundle.location, dep.child_path))
        if not child_path.startswith(bundle.location):
            raise Exception("Invalid key for dependency: %s" % (dep.child_path))

        docker_dependency_path = os.path.join(docker_dependencies_path, dep.child_path)
        os.symlink(docker_dependency_path, child_path)
        dependency_path = os.path.realpath(
            os.path.join(os.path.realpath(dep.location), dep.parent_path)
        )
        # These are turned into docker volume bindings like:
        #   dependency_path:docker_dependency_path:ro
        docker_dependencies.append((dependency_path, docker_dependency_path))

    # Figure out docker details
    docker_runtime = "nvidia" if resources.gpus else "runc"
    if resources.network:
        docker_network_name = "codalab-external"
        internal = False
    else:
        docker_network_name = "codalab-internal"
        internal = True
    try:
        docker_client.networks.create(
            docker_network_name, internal=internal, check_duplicate=True
        )
    except docker.errors.APIError:
        # Network already exists, go on
        pass
    return docker_utils.start_bundle_container(
        bundle.location,
        bundle.uuid,
        docker_dependencies,
        bundle.command,
        resources.docker_image,
        network=docker_network_name,
        memory_bytes=resources.memory,
        runtime=docker_runtime,
    )


if __name__ == "__main__":
    main()
