from codalab.server.rest_server import create_rest_app
from webtest import TestApp
import unittest
import uuid
import os


class BaseTestCase(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        self.app = TestApp(create_rest_app())
        os.environ["CODALAB_TEST_USER"] = "codalab"

    def tearDown(self):
        os.environ["CODALAB_TEST_USER"] = ""

    def create_worksheet(self, worksheet_name=None):
        worksheet_name = worksheet_name or f"codalab-{uuid.uuid4()}"
        response = self.app.post_json(
            '/rest/worksheets',
            {'data': [{'type': 'worksheets', 'attributes': {'name': worksheet_name}}]},
        )
        worksheet_id = response.json["data"][0]["id"]
        return worksheet_id
