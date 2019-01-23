"""
Executable python file that runs a Codalab RunBundle meant to be submitted to a job management environment
like Slurm
"""

from argparse import ArgumentParser
import docker
import errno
import fcntl
import json
import time


def parse_args():
    parser = ArgumentParser()
    parser.add_argument("--bundle-file", type=str)
    parser.add_argument("--resources-file", type=str)
    parser.add_argument("--lock-file", type=str)
    args = parser.parse_args()
    return args


def main():
    args = parse_args()
    run_bundle(args)


def write_state(state, state_file, lock_file):
    while True:
        try:
            lock_file = open(lock_file, 'w+')
            fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
            break
        except IOError as ex:
            if ex.errno != errno.EAGAIN:
                raise
            else:
                time.sleep(0.1)
    with open(state_file, 'wb') as f:
        json.dump(state, f)
    fcntl.flock(lock_file, fcntl.LOCK_UN)
    lock_file.close()


def run_bundle(args):
    # TODO: Make a dict of everything that needs to be reported to the worker
    with open(args.bundle_file, "r") as infile:
        bundle = json.load(infile)
    with open(args.resources_file, "r") as infile:
        resources = json.load(infile)
    run_state = {"docker_image": resources["request_docker_image"]}
    try:
        run_state["docker_image"] = pull_docker_image(
            bundle["uuid"], resources["request_docker_image"]
        )
    except Exception as ex:
        # TODO fail the image with ex
        print("Docker image download failed: {}".format(ex))


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


if __name__ == "__main__":
    main()
