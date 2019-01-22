"""
Executable python file that runs a Codalab RunBundle meant to be submitted to a job management environment
like Slurm
"""

from argparse import ArgumentParser
import docker
import json
import time


def parse_args():
    parser = ArgumentParser()
    parser.add_argument("--bundle-file", type=str)
    parser.add_argument("--resources-file", type=str)
    args = parser.parse_args()
    return args


def main():
    args = parse_args()
    with open(args.bundle_file, "r") as infile:
        bundle_dict = json.load(infile)
    with open(args.resources_file, "r") as infile:
        resources_dict = json.load(infile)
    run_bundle(bundle_dict, resources_dict)


def run_bundle(bundle, resources):
    # TODO: Make a dict of everything that needs to be reported to the worker
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
