import unittest
from mock import Mock
from bottle import local, request
from codalab.server.rest_server import request, create_rest_app
from webtest import TestApp
import os
import json
import uuid
from codalab.objects.worksheet import Worksheet
from .base import BaseTestCase


class WorksheetsTest(BaseTestCase):
    def test_create(self):
        worksheet_name = f"codalab-{uuid.uuid4()}"
        response = self.app.post_json(
            '/rest/worksheets',
            {'data': [{'type': 'worksheets', 'attributes': {'name': worksheet_name}}]},
        )
        worksheet_id = response.json["data"][0]["id"]
        self.assertEqual(
            response.json["data"],
            [
                {
                    "type": "worksheets",
                    "id": worksheet_id,
                    "attributes": {"name": worksheet_name, "uuid": worksheet_id},
                    "relationships": {
                        "items": {
                            "data": [
                                {"type": "worksheet-items", "id": f"('name', '{worksheet_name}')"},
                                {"type": "worksheet-items", "id": f"('uuid', '{worksheet_id}')"},
                            ]
                        }
                    },
                }
            ],
        )
