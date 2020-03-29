import docker
import unittest

from codalab.worker.docker_utils import DockerUserErrorException, DockerException, wrap_exception


class DockerUtilsTest(unittest.TestCase):
    def test_wrap_exception(self):
        error = (
            'Cannot start Docker container: Unable to start Docker container: 500 Server '
            'Error: Internal Server Error "OCI runtime create failed: some other error"'
        )

        @wrap_exception('Should throw DockerException')
        def throw_error():
            raise docker.errors.ApiError(error)

        try:
            throw_error()
        except Exception as e:
            print(str(e))
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
            raise docker.errors.ApiError(error)

        try:
            throw_cuda_error()
        except Exception as e:
            print(str(e))
            self.assertEqual(str(e), 'Should throw DockerUserErrorException: ' + error)
            self.assertIsInstance(e, DockerUserErrorException)
