import urllib2

from codalab.client.rest_client import RestClient, RestClientException
from codalab.common import (
    http_error_to_exception,
    precondition,
    PreconditionViolation,
    UsageError,
)


# TODO(sckoo): deal with more error cases
def wrap_exception(f):
    def wrapper(*args, **kwargs):
        # Translate errors to the standard CodaLab errors
        try:
            return f(*args, **kwargs)
        except urllib2.HTTPError as e:
            raise http_error_to_exception(e.code, e.read())
        except RestClientException as e:
            if e.client_error:
                raise UsageError(e.message)
            else:
                raise PreconditionViolation()

    return wrapper


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


class JsonApiClient(RestClient):
    """
    Simple JSON API client.
    """
    def __init__(self, address, authenticate):
        self._authenticate = authenticate

        base_url = address + '/rest'
        super(JsonApiClient, self).__init__(base_url)

    def _get_access_token(self):
        return self._authenticate(self)

    @staticmethod
    def _get_resource_path(resource_type, resource_id=None):
        """
        Build and return API path.
        """
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

    @staticmethod
    def _unpack_document(document):
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

        If the document does not follow the expected JSON API format, this
        function throws a ValueError.
        If relationships do not contain resource linkages (the 'data' item),
        this function also throws a ValueError.

        :param document: the JSON-API payload as a dict
        :return: unpacked resource info as a dict
        """
        def unpack_linkage(linkage):
            # Return recursively unpacked object if the data was included in the
            # document, otherwise just return the linkage object
            if linkage is None or (linkage['id'], linkage['type']) not in included:
                return linkage
            else:
                return unpack_object(included[linkage['id'], linkage['type']])

        def unpack_object(obj_data):
            # Merge attributes, id, meta, and relationships into a single dict
            obj = obj_data['attributes'].copy()
            obj['id'] = obj_data['id']
            if 'meta' in obj_data:
                obj['meta'] = obj_data['meta']
            for key, relationship in obj_data.get('relationships', {}).iteritems():
                linkage = relationship['data']
                if isinstance(linkage, list):
                    obj[key] = [unpack_linkage(l) for l in linkage]
                else:
                    obj[key] = unpack_linkage(linkage)
            return obj

        # Load included resources into dict indexed by (type, id)
        included = {
            (resource['type'], resource['id']): resource
            for resource in document.get('included', [])
        }

        # Build result info dict
        try:
            data = document.get('data', None)
            if isinstance(data, list):
                result = [unpack_object(d) for d in document['data']]
            elif isinstance(data, dict):
                result = unpack_object(document['data'])
            else:
                result = {}
        except KeyError:
            raise ValueError('Invalid or unsupported JSON API document format')

        # Include meta
        # (Warning: this may overwrite meta present at the resource object level.)
        if 'meta' in document:
            result['meta'] = document['meta']

        return result

    @staticmethod
    def _pack_document(objects, type_):
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
        :param type_: resource type as string
        :return: packed JSON API document
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

        try:
            if isinstance(objects, list):
                packed_objects = [pack_object(obj) for obj in objects]
            else:
                packed_objects = pack_object(objects)
        except KeyError:
            raise ValueError('Invalid resource info format')

        return {
            'data': packed_objects
        }

    @wrap_exception
    def fetch(self, resource_type, resource_id=None, params=None):
        """
        Request to fetch a resource or resources.

        :param resource_type: resource type as string
        :param resource_id: id of resource to fetch, or None if bulk fetch
        :param params: dict of query parameters
        :return: the fetched objects
        """
        url = self._get_resource_path(resource_type, resource_id)
        params = self._pack_params(params)
        return self._unpack_document(
            self._make_request('GET', url, query_params=params))

    @wrap_exception
    def create(self, resource_type, data, params=None):
        """
        Request to create a resource or resources.

        :param resource_type: resource type as string
        :param data: info dict or list of info dicts
        :param params: dict of query parameters
        :return: the created object(s)
        """
        url = self._get_resource_path(resource_type)
        params = self._pack_params(params)
        data = self._pack_document(data, resource_type)
        return self._unpack_document(
            self._make_request('POST', url, query_params=params, data=data))

    @wrap_exception
    def update(self, resource_type, data, params=None):
        """
        Request to update a resource or resources.

        :param resource_type: resource type as string
        :param data: update dict or list of update dicts, update dicts
        must contain 'id' fields specifying the resources to update
        :param params: dict of query parameters
        :return: the updated object(s)
        """
        path = self._get_resource_path(resource_type)
        params = self._pack_params(params)
        data = self._pack_document(data, resource_type)
        return self._unpack_document(
            self._make_request('PATCH', path, query_params=params, data=data))

    @wrap_exception
    def delete(self, resource_type, resource_id, params=None):
        """
        Request to delete a resource or resources.

        :param resource_type: resource type as string
        :param resource_id: id or list of ids of resources to delete
        :param params: dict of query parameters
        :return: response data as dict, but otherwise undefined
        """
        url = self._get_resource_path(resource_type)
        if not isinstance(resource_id, list):
            resource_id = [resource_id]
        data = {
            'data': [{
                'id': id_,
                'type': resource_type,
            } for id_ in resource_id],
        }
        params = self._pack_params(params)
        return self._unpack_document(
            self._make_request('DELETE', url, query_params=params, data=data))

