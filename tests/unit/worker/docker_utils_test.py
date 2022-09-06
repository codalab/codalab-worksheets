from docker.errors import APIError
import unittest

from codalab.worker.docker_utils import (
    DockerUserErrorException,
    DockerException,
    wrap_exception,
    parse_image_progress,
)


class ParseImageProgressTest(unittest.TestCase):
    def test_parse_image_progress_expected(self):
        image_info = {
            'progressDetail': {'current': 20320000, 'total': 28540000},
            'progress': '[===============>     ]  20.32MB/28.54MB',
        }
        progress = parse_image_progress(image_info)
        self.assertEqual(progress, '20.32MB/28.54MB (71% done)')

    def test_parse_image_progress_missing_detail(self):
        progress = parse_image_progress({})
        self.assertEqual(progress, '')

    def test_parse_image_progress_missing_progress(self):
        image_info = {
            'progressDetail': {'current': 20320000, 'total': 28540000},
        }
        progress = parse_image_progress(image_info)
        self.assertEqual(progress, '(71% done)')

    def test_parse_image_progress_partial_progress(self):
        image_info = {
            'progressDetail': {'current': 20320000, 'total': 28540000},
            'progress': '  20.32MB/28.54MB',
        }
        progress = parse_image_progress(image_info)
        self.assertEqual(progress, '20.32MB/28.54MB (71% done)')


class WrapExceptionTest(unittest.TestCase):
    def test_wrap_exception(self):
        error = (
            'Cannot start Docker container: Unable to start Docker container: 500 Server '
            'Error: Internal Server Error "OCI runtime create failed: some other error"'
        )

        @wrap_exception('Should throw DockerException')
        def throw_error():
            raise APIError(error)

        try:
            throw_error()
        except Exception as e:
            self.assertEqual(str(e), 'Should throw DockerException: ' + error)
            self.assertIsInstance(e, DockerException)

    def test_wrap_exception_with_cuda_error(self):
        error = (
            'Cannot start Docker container: Unable to start Docker container: 500 Server '
            'Error: Internal Server Error ("OCI runtime create failed: container_linux.go:'
            '345: starting container process caused "process_linux.go:430: container init '
            'caused "process_linux.go:413: running prestart hook 1 caused "error '
            'running hook: exit status 1, stdout: ,  stderr: nvidia-container-cli: mount '
            'error: file creation failed: /mnt/scratch/docker/overlay2/678d6b'
            '19396c4ccd341786b21393f3f/merged/usr/bin/nvidia-smi'
        )

        @wrap_exception('Should throw DockerUserErrorException')
        def throw_cuda_error():
            raise APIError(error)

        try:
            throw_cuda_error()
        except Exception as e:
            self.assertEqual(str(e), 'Should throw DockerUserErrorException: ' + error)
            self.assertIsInstance(e, DockerUserErrorException)

    def test_wrap_exception_with_memory_limit_error(self):
        error = (
            'Unable to start Docker container: 500 Server Error: Internal Server Error '
            '("OCI runtime create failed: container_linux.go:349: starting container process '
            'caused "process_linux.go:449: container init caused \"process_linux.go:415: '
            'setting cgroup config for procHooks process caused \\\"failed to write\\\\\\\"8388608'
            '\\\\\\\" to \\\\\\\"/sys/fs/cgroup/memory/docker/a5475e95e98bbb534870dfdf290e91251f54'
            'e5c13be07a7b6819619a2dba48ef/memory.limit_in_bytes\\\\\\\":write /sys/fs/cgroup/memory'
            '/docker/a5475e95e98bbb534870dfdf290e91251f54e5c13be07a7b6819619a2dba48ef/'
            'memory.limit_in_bytes: device or resource busy\\\"\"": unknown'
        )

        @wrap_exception('Should throw DockerUserErrorException')
        def throw_memory_error():
            raise APIError(error)

        try:
            throw_memory_error()
        except Exception as e:
            self.assertEqual(str(e), 'Should throw DockerUserErrorException: ' + error)
            self.assertIsInstance(e, DockerUserErrorException)
