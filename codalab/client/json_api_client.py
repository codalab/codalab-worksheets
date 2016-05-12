from collections import defaultdict
from contextlib import closing
import json
import urllib
import urllib2

from codalab.client.bundle_client import BundleClient
from codalab.client.rest_client import RestClient, RestClientException
from codalab.common import http_error_to_exception
from worker.file_util import un_gzip_stream


# TODO(sckoo): deal with more error cases
def wrap_exception(f):
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except urllib2.HTTPError as err:
            raise http_error_to_exception(err.code, err.read())
    return wrapper


def unpack_document(document):
    """
    Unpack a JSON API document into a plain dict, with the relationship keys
    wired up to the 'included' resources. Supports multiple levels of
    relationships. Relationships that do not have a corresponding object in the
    'included' objects will be filled in with a dict that only has the 'id'
    key. For example:

     >> unpack_document({
            'data': {
                'id': '123',
                'type: 'bundles',
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
                    'type': 'users':
                    'id': '345',
                    'attributes': {
                        'name': 'percy',
                        'affiliation': 'stanford'
                    }
                }
            ]
        })

     => {
            'owner': {
                'id': '345',
                'name': 'percy',
                'affiliation: 'stanford'
            },
            'parent': {
                'id': '567'
            },
            'id': '123',
            'name': 'hello'
        }

    :param document: the JSON-API payload as a dict
    :return:
    """
    def unpack_object(obj_data):
        obj = obj_data['attributes']
        obj['id'] = obj_data['id']

        if 'meta' in obj_data:
            obj['meta'] = obj_data['meta']

        for key, relationship in obj_data.get('relationships', {}).iteritems():
            if isinstance(relationship['data'], list):
                obj[key] = []
                for linkage in relationship['data']:
                    if linkage['id'] in included[linkage['type']].keys():
                        obj[key].append(unpack_object(included[linkage['type']][linkage['id']]))
                    else:
                        obj[key].append({'id': linkage['id']})
            else:
                linkage = relationship['data']
                if linkage is None:
                    obj[key] = None
                elif linkage['id'] in included[linkage['type']]:
                    obj[key] = unpack_object(included[linkage['type']][linkage['id']])
                else:
                    obj[key] = {'id': linkage['id']}

        return obj

    included = defaultdict(dict)
    for resource in document.get('included', []):
        included[resource['type']][resource['id']] = resource

    data = document.get('data', None)
    if isinstance(data, list):
        result = [unpack_object(data) for data in document['data']]
    elif isinstance(data, dict):
        result = unpack_object(document['data'])
    else:
        result = {}

    # Include meta
    # (Warning: this may overwrite meta present at the resource object level.)
    if 'meta' in document:
        result['meta'] = document['meta']

    return result


def pack_document(objects, type_):
    """
    Pack resource object(s) into a JSON API request document.
    References to relationships should be indicated by a placeholder
    JsonApiRelationship object. For example:

     >> pack_document({
            'owner': JsonApiRelationship('users', '345'),
            'id': '123',
            'name': 'hello'
        }, 'bundles')

     => {
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
        }

    :param objects: a dict or list of dicts representing resources
    :param type_: resource type
    :return:
    """
    def pack_object(obj):
        packed_obj = {'type': type_}
        attributes = {}
        relationships = {}
        for key, value in obj.iteritems():
            if isinstance(value, JsonApiRelationship):
                relationships[key] = {
                    'data': {
                        'id': value.id_,
                        'type': value.type_,
                    }
                }
            elif key == 'id':
                packed_obj['id'] = value
            else:
                attributes[key] = value

        if attributes:
            packed_obj['attributes'] = attributes

        if relationships:
            packed_obj['relationships'] = relationships

        return packed_obj

    if isinstance(objects, list):
        packed_objects = [pack_object(obj) for obj in objects]
    else:
        packed_objects = pack_object(objects)

    return {
        'data': packed_objects
    }


class JsonApiRelationship(object):
    """
    Placeholder for a relationship to another resource.
    Used to build requests to create or update a resource.

    For example, to update the owner of a bundle:
        client.update('bundles', uuid, data={
            'owner': JsonApiRelationship('users', owner_id)
        })
    """
    def __init__(self, type_, id_):
        self.type_ = type_
        self.id_ = id_


class JsonApiClient(RestClient, BundleClient):
    """
    Simple JSON API client.
    """
    def __init__(self, address, auth_handler, authenticate):
        self.address = address
        self.auth_handler = auth_handler
        self._authenticate = authenticate

        base_url = address + '/rest'
        super(JsonApiClient, self).__init__(base_url)

    def _get_access_token(self):
        return self._authenticate(self)

    @staticmethod
    def _get_resource_path(resource_type, resource_id=None):
        if resource_id is None:
            return '/' + resource_type
        else:
            return '/' + resource_type + '/' + str(resource_id)

    @staticmethod
    def _pack_params(params):
        """
        Process lists into comma-separated strings, and booleans into 1/0.
        """
        if params is None:
            return None

        result = {}
        for k, v in (params.iteritems() if isinstance(params, dict) else params):
            if isinstance(v, list):
                result[k] = ','.join(map(unicode, v))
            elif isinstance(v, bool):
                result[k] = int(v)
            else:
                result[k] = v
        return result

    @wrap_exception
    def fetch(self, resource_type, resource_id=None, params=None):
        url = self._get_resource_path(resource_type, resource_id)
        params = self._pack_params(params)
        return unpack_document(
            self._make_request('GET', url, query_params=params))

    @wrap_exception
    def create(self, resource_type, data, params=None):
        url = self._get_resource_path(resource_type)
        params = self._pack_params(params)
        data = pack_document(data, resource_type)
        return unpack_document(
            self._make_request('POST', url, query_params=params, data=data))

    @wrap_exception
    def update(self, resource_type, data, params=None):
        """
        Request to update a resource or resources.

        :param resource_type: resource type as string
        :param data: update dict or list of update dicts, update dicts
        must contain 'id' fields specifying the resources to update
        :param params: dict of query parameters
        :return:
        """
        if isinstance(data, list):
            path = self._get_resource_path(resource_type)
        else:
            path = self._get_resource_path(resource_type, data['id'])
        params = self._pack_params(params)
        data = pack_document(data, resource_type)
        return unpack_document(
            self._make_request('PATCH', path, query_params=params, data=data))

    @wrap_exception
    def delete(self, resource_type, resource_id, params=None):
        if isinstance(resource_id, list):
            url = self._get_resource_path(resource_type)
            data = {
                'data': [{
                    'id': id_,
                    'type': resource_type,
                } for id_ in resource_id],
            }
        else:
            url = self._get_resource_path(resource_type, resource_id)
            data = None
        params = self._pack_params(params)

        return unpack_document(
            self._make_request('DELETE', url, query_params=params, data=data))

    @wrap_exception
    def fetch_contents_info(self, bundle_id, target_path='', depth=0):
        request_path = '/bundles/%s/contents/info/%s' % (bundle_id, target_path)
        response = self._make_request('GET', request_path,
                                      query_params={'depth': depth})
        return response['data']

    @wrap_exception
    def fetch_contents_blob(self, bundle_id, target_path='', range_=None,
                            head=None, tail=None):
        """
        Returns a file-like object for the target on the given bundle.

        :param bundle_id: id of target bundle
        :param target_path: path to target in bundle
        :param range_: range of bytes to fetch
        :param head: number of lines to summarize from beginning of file
        :param tail: number of lines to summarize from end of file
        :return: file-like object containing requested data blob
        """
        request_path = '/bundles/%s/contents/blob/%s' % (bundle_id, target_path)
        headers = {'Accept-Encoding': 'gzip'}
        if range_ is not None:
            headers['Range'] = 'bytes=%d-%d' % range_
        params = {}
        if head is not None:
            params['head'] = head
        if tail is not None:
            params['tail'] = tail
        response = self._make_request('GET', request_path, headers=headers,
                                      query_params=params, return_response=True)

        if response.headers.get('Content-Encoding') == 'gzip':
            return un_gzip_stream(response)
        return response
