import argparse
import collections
import docker
import errno
import fcntl
import pickle
import threading
import time


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--bundle-file')
    parser.add_argument('--resources-file')
    parser.add_argument('--state-file-path')
    return parser.parse_args()


def main():
    args = parse_args()
    with open(args.bundle_file, 'r') as infile:
        bundle = pickle.load(infile)
    with open(args.resources_file, 'r') as infile:
        resources = pickle.load(infile)
    slurm_run = SlurmRun(bundle, resources, args.state_file_path)
    slurm_run.start()


SlurmRunState = collections.namedtuple('SlurmRunState', ['uuid', 'status', 'error_mesage'])


class AvailabilityState(object):
    NOT_READY = 'NOT_READY'
    READY = 'READY'
    NOT_AVAILABLE = 'NOT_AVAILABLE'

    def __init__(self, initial_status=NOT_READY, initial_info=''):
        self.status = initial_status
        self.info = initial_info


class SlurmRun(object):

    def __init__(self, bundle, resources, state_file_path):
        self.bundle = bundle
        self.resource = resources
        self.docker = docker.from_env()
        self.state_file_path = state_file_path
        self.state_file_lock_path = state_file_path + '.lock'
        self.image_state = AvailabilityState(initial_info='Fetching docker image')
        self.dependencies_state = AvailabilityState(initial_info='Fetching dependencies')
        self.run_state = SlurmRunState(uuid=bundle['uuid'], status='PREPARING', error_message=None)

    def write_state(self):
        while True:
            try:
                lock_file = open(self.state_file_lock_path, 'w+')
                fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except IOError as ex:
                if ex.errno != errno.EAGAIN:
                    raise
                else:
                    time.sleep(0.1)
        with open(self.state_file_path, 'wb') as f:
            pickle.dump(self.run_state, f)
        fcntl.flock(lock_file, fcntl.LOCK_UN)
        lock_file.close()

    def start(self):
        try:
            self.prepare_dependencies()
            self.run_container()
        except Exception as ex:
            # TODO: failure case
            pass
        self.finalize()

    def pull_or_get_image(self):
        # TODO: Return AvailabilityState (try to get, if not do the threaddict trick)
        pass

    def download_or_get_dependencies(self):
        # TODO: Return AvailabilityState for downloads of all dependencies. Use threaddict to download them
        pass

    @property
    def preparing_dependencies(self):
        return (self.image_state.status != AvailabilityState.READY or self.dependencies_state.status != AvailabilityState.READY or (self.image_state.status != AvailabilityState.NOT_AVAILABLE and self.dependencies_state.status != AvailabilityState.NOT_AVAILABLE))

    def prepare_dependencies(self, bundle, resources):
        while self.preparing_dependencies:
            self.pull_or_get_image()
            self.download_or_get_dependency()
        if (self.image_state.status == AvailabilityState.NOT_AVAILABLE or self.dependencies_state.status == AvailabilityState.NOT_AVAILABLE):
            # TODO: Raise properly
            raise Exception

    def run_container(self):
        # TODO: Create and start docker container
        # TODO: Loop, checking container info from docker API, reading run state from shared file to check for killed status, writing updates to the shared file
        pass

    def finalize(self):
        # TODO: remove docker artifacts, remove downloaded dependencies, write to shared file that run is finished
        pass
