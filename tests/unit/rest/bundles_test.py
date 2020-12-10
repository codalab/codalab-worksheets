from .base import BaseTestCase
from freezegun import freeze_time
from io import BytesIO
import tarfile


@freeze_time("2012-01-14")
class BundlesTest(BaseTestCase):
    def test_create(self):
        worksheet_id = self.create_worksheet()
        body = {
            'data': [
                {
                    'type': 'bundles',
                    'attributes': {
                        'bundle_type': 'run',
                        'command': 'echo TEST',
                        'metadata': {
                            'name': 'run-echo',
                            'description': '',
                            'tags': [''],
                            'allow_failed_dependencies': False,
                            'request_docker_image': 'codalab/default-cpu:latest',
                            'request_time': '',
                            'request_memory': '4g',
                            'request_disk': '',
                            'request_cpus': 1,
                            'request_gpus': 0,
                            'request_queue': '',
                            'request_priority': 0,
                            'request_network': False,
                            'exclude_patterns': [],
                        },
                        'dependencies': [],
                    },
                }
            ]
        }
        response = self.app.post_json(f'/rest/bundles?worksheet={worksheet_id}', body)
        self.assertEqual(response.status_int, 200)
        data = response.json["data"]
        bundle_id = data[0]["id"]
        data[0]["attributes"].pop("state")
        data[0]["attributes"]["metadata"].pop("failure_message", None)
        self.assertEqual(
            data,
            [
                {
                    "type": "bundles",
                    "attributes": {
                        "permission": 2,
                        "data_hash": None,
                        "uuid": bundle_id,
                        "args": "run \"echo TEST\" --request-cpus 1 --request-docker-image codalab/default-cpu:latest --request-memory 4g",
                        "metadata": {
                            "request_docker_image": "codalab/default-cpu:latest",
                            "allow_failed_dependencies": False,
                            "request_disk": "",
                            "actions": [],
                            "created": 1326499200,
                            "name": "run-echo",
                            "request_time": "",
                            "request_cpus": 1,
                            "request_gpus": 0,
                            "request_network": False,
                            "exclude_patterns": [],
                            "description": "",
                            "request_queue": "",
                            "tags": [""],
                            "request_memory": "4g",
                            "request_priority": 0,
                        },
                        "permission_spec": "all",
                        "is_anonymous": False,
                        "bundle_type": "run",
                        "command": "echo TEST",
                        "dependencies": [],
                    },
                    "relationships": {"owner": {"data": {"type": "users", "id": "0"}}},
                    "id": bundle_id,
                }
            ],
        )

    def test_upload_file(self):
        """Upload a single file as a bundle, then retrieve its
        contents through the REST API.
        """
        worksheet_id = self.create_worksheet()
        body = {
            "data": [
                {
                    "attributes": {
                        "bundle_type": "dataset",
                        "metadata": {
                            "description": "",
                            "license": "",
                            "name": "File.txt",
                            "source_url": "",
                            "tags": [],
                        },
                    },
                    "type": "bundles",
                }
            ]
        }
        response = self.app.post_json(f'/rest/bundles?worksheet={worksheet_id}', body)
        self.assertEqual(response.status_int, 200)
        data = response.json["data"]
        bundle_id = data[0]["id"]

        response = self.app.request(
            f'/rest/bundles/{bundle_id}/contents/blob/?finalize=1&filename=File.txt&unpack=0',
            method='PUT',
            content_type='application/octet-stream',
            body='hello world'.encode(),
        )
        self.assertEqual(response.status_int, 200)

        response = self.app.head(f'/rest/bundles/{bundle_id}/contents/blob/')
        self.assertEqual(response.status_int, 200)
        self.assertEqual(response.headers['Content-Type'], 'text/plain')
        self.assertEqual(response.headers['Content-Encoding'], 'identity')
        self.assertEqual(response.headers['Content-Disposition'], 'inline; filename="File.txt"')
        self.assertEqual(response.headers['Target-Type'], 'file')
        self.assertEqual(response.headers['X-Codalab-Target-Size'], '11')

    def test_upload_zip_file(self):
        """Upload a zip file as a bundle, then retrieve the contents of the file,
        as well as individual files within the bundle, using the REST API.
        """
        worksheet_id = self.create_worksheet()
        body = {
            "data": [
                {
                    "attributes": {
                        "bundle_type": "dataset",
                        "metadata": {
                            "description": "",
                            "license": "",
                            "name": "File",
                            "source_url": "",
                            "tags": [],
                        },
                    },
                    "type": "bundles",
                }
            ]
        }
        response = self.app.post_json(f'/rest/bundles?worksheet={worksheet_id}', body)
        self.assertEqual(response.status_int, 200)
        data = response.json["data"]
        bundle_id = data[0]["id"]

        f = BytesIO()
        with tarfile.open(fileobj=f, mode='w:gz') as tf:
            tinfo = tarfile.TarInfo("file1.txt")
            tinfo.size = 6
            tf.addfile(tinfo, BytesIO("file 1".encode()))

            tinfo = tarfile.TarInfo("file-two.txt")
            tinfo.size = 8
            tf.addfile(tinfo, BytesIO("file two".encode()))
        f.seek(0)
        response = self.app.request(
            f'/rest/bundles/{bundle_id}/contents/blob/?finalize=1&filename=File.tar.gz&unpack=1',
            method='PUT',
            content_type='application/octet-stream',
            body=f.read(),
        )
        self.assertEqual(response.status_int, 200)

        response = self.app.head(f'/rest/bundles/{bundle_id}/contents/blob/')
        self.assertEqual(response.status_int, 200)
        self.assertEqual(response.headers['Content-Type'], 'application/gzip')
        self.assertEqual(response.headers['Content-Encoding'], 'identity')
        self.assertEqual(
            response.headers['Content-Disposition'], 'attachment; filename="File.tar.gz"'
        )
        self.assertEqual(response.headers['Target-Type'], 'directory')
        self.assertEqual(response.headers['X-Codalab-Target-Size'], '128')

        response = self.app.head(f'/rest/bundles/{bundle_id}/contents/blob/file-two.txt')
        self.assertEqual(response.status_int, 200)
        self.assertEqual(response.headers['Content-Type'], 'text/plain')
        self.assertEqual(response.headers['Content-Encoding'], 'identity')
        self.assertEqual(response.headers['Content-Disposition'], 'inline; filename="file-two.txt"')
        self.assertEqual(response.headers['Target-Type'], 'file')
        self.assertEqual(response.headers['X-Codalab-Target-Size'], '8')

        self.app.head(f'/rest/bundles/{bundle_id}/contents/blob/invalid.txt', status=404)
