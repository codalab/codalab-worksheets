"""
Unit tests for the static methods of the JsonApiClient
"""
import unittest

from codalab.client.json_api_client import (
    EmptyJsonApiRelationship,
    JsonApiClient,
    JsonApiRelationship,
)
from codalab.common import PreconditionViolation


class JsonApiClientTest(unittest.TestCase):
    def setUp(self):
        self.client = JsonApiClient('', lambda: None)

    def test_pack_params(self):
        self.assertItemsEqual(self.client._pack_params({
            'int': 2,
            'float': 2.2,
            'str': 'stringy',
            'true': True,
            'false': False,
            'list': [1, '2', 3.3, True],
            'comma': 'I,have,commas',
            'commalist': ['I,also', 'have,commas'],
        }), [
            ('int', 2),
            ('float', 2.2),
            ('str', 'stringy'),
            ('true', 1),
            ('false', 0),
            ('list', '1'),
            ('list', '2'),
            ('list', '3.3'),
            ('list', 'True'),
            ('comma', 'I,have,commas'),
            ('commalist', 'I,also'),
            ('commalist', 'have,commas'),
        ])

    def test_resource_path(self):
        self.assertEqual(self.client._get_resource_path('bundles'),
                         '/bundles')
        self.assertEqual(self.client._get_resource_path('bundles', 'abc'),
                         '/bundles/abc')

    def test_pack_document(self):
        doc = self.client._pack_document({
            'owner': JsonApiRelationship('users', '345'),
            'friend': EmptyJsonApiRelationship(),
            'id': '123',
            'name': 'hello'
        }, 'bundles')

        self.assertDictEqual(doc, {
            'data': {
                'id': '123',
                'type': 'bundles',
                'attributes': {
                    'name': 'hello'
                },
                'relationships': {
                    'owner': {
                        'data': {
                            'id': '345',
                            'type': 'users'
                        }
                    },
                    'friend': {
                        'data': None
                    }
                },
            }
        })

    def test_unpack_document(self):
        obj = self.client._unpack_document({
            'data': {
                'id': '123',
                'type': 'bundles',
                'attributes': {
                    'name': 'hello'
                },
                'relationships': {
                    'owner': {
                        'data': {
                            'id': '345',
                            'type': 'users'
                        }
                    },
                    'parent': {
                        'data': {
                            'id': '567',
                            'type': 'bundles'
                        }
                    }
                },
            },
            'included': [
                {
                    'type': 'users',
                    'id': '345',
                    'attributes': {
                        'name': 'percy',
                        'affiliation': 'stanford'
                    }
                }
            ]
        })

        self.assertDictEqual(obj, {
            'owner': JsonApiRelationship('users', '345', {
                'name': 'percy',
                'affiliation': 'stanford'
            }),
            'parent': JsonApiRelationship('bundles', '567'),
            'id': '123',
            'name': 'hello'
        })

    def test_fetch_one(self):
        class MockJsonApiClient(JsonApiClient):
            def __init__(self):
                pass

            def fetch(self, count, **kwargs):
                """
                Returns list with as many dicts as specified by |count|,
                or return a dict directly if |count| is None.
                """
                if count is None:
                    return {}
                else:
                    return [{}] * count

        client = MockJsonApiClient()
        self.assertEqual(client.fetch_one(None), {}, "fetch_one doesn't return dict directly")
        self.assertEqual(client.fetch_one(1), {}, "fetch_one doesn't extract single dict from list")
        with self.assertRaises(PreconditionViolation):
            client.fetch_one(2)
        with self.assertRaises(PreconditionViolation):
            client.fetch_one(10)
