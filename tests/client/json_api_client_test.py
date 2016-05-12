"""
Unit tests for the static methods of the JsonApiClient
"""
import unittest

from codalab.client.json_api_client import (
    JsonApiClient,
    JsonApiException,
    JsonApiRelationship
)


class JsonApiClientTest(unittest.TestCase):
    def test_pack_params(self):
        self.assertDictEqual(JsonApiClient._pack_params({
            'int': 2,
            'float': 2.2,
            'str': 'stringy',
            'true': True,
            'false': False,
            'list': [1, '2', 3.3, True]
        }), {
            'int': 2,
            'float': 2.2,
            'str': 'stringy',
            'true': 1,
            'false': 0,
            'list': '1,2,3.3,True'
        })

    def test_resource_path(self):
        self.assertEqual(JsonApiClient._get_resource_path('bundles'),
                         '/bundles')
        self.assertEqual(JsonApiClient._get_resource_path('bundles', 'abc'),
                         '/bundles/abc')

    def test_pack_document(self):
        doc = JsonApiClient._pack_document({
            'owner': JsonApiRelationship('users', '345'),
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
                    }
                },
            }
        })

    def test_unpack_document(self):
        obj = JsonApiClient._unpack_document({
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
            'owner': {
                'id': '345',
                'name': 'percy',
                'affiliation': 'stanford'
            },
            'parent': {
                'id': '567',
                'type': 'bundles'
            },
            'id': '123',
            'name': 'hello'
        })


