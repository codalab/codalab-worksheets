import httplib
import socket
import sys
import urllib
import urllib2

from codalab.common import (
    http_error_to_exception,
    UsageError,
)
from worker.rest_client import RestClient, RestClientException
from worker.file_util import un_gzip_stream


def wrap_exception(message):
    def decorator(f):
        def wrapper(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            except urllib2.HTTPError as e:
                # Translate known errors to the standard CodaLab errors
                exc = http_error_to_exception(e.code, e.read())
                # All standard CodaLab errors are subclasses of UsageError
                if isinstance(exc, UsageError):
                    raise exc.__class__, exc, sys.exc_info()[2]
                else:
                    # Shunt other exceptions into one class
                    raise JsonApiException, \
                        JsonApiException(message.format(*args, **kwargs) +
                                         ': ' + httplib.responses[e.code] +
                                         ' - ' + e.read(),
                                         400 <= e.code < 500), \
                        sys.exc_info()[2]
            except RestClientException as e:
                raise JsonApiException, \
                    JsonApiException(message.format(*args, **kwargs) +
                                     ': ' + e.message, e.client_error), \
                    sys.exc_info()[2]
            except (urllib2.URLError, httplib.HTTPException, socket.error) as e:
                raise JsonApiException, \
                    JsonApiException(message.format(*args, **kwargs) +
                                     ': ' + str(e), False), \
                    sys.exc_info()[2]
        return wrapper
    return decorator


class JsonApiException(RestClientException):
    """
    Exception raised by the JsonApiClient methods on error. If
    client_error is False, the failure is caused by a server-side error and
    can be retried.
    """


class JsonApiRelationship(object):
    """
    Placeholder for a relationship to another resource.
    Used to build requests to create or update a resource.

    For example, to update the owner of a bundle:
        client.update('bundles', {
            'id': '0x7d67f3e0fda249e5b0531670f473c04f',
            'owner': JsonApiRelationship('users', owner_id)
        })
    """
    def __init__(self, type_, id_):
        self.type_ = type_
        self.id_ = id_

    def as_dict(self):
        return {
            'data': {
                'type': self.type_,
                'id': self.id_,
            }
        }


class JsonApiClient(RestClient):
    """
    Simple JSON API client.
    """
    def __init__(self, address, get_access_token):
        self._get_access_token = get_access_token
        self.address = address  # Used as key in client and token caches
        base_url = address + '/rest'
        super(JsonApiClient, self).__init__(base_url)

    @staticmethod
    def _get_resource_path(resource_type, resource_id=None, relationship=None):
        """
        Build and return API path.
        """
        path = '/' + resource_type
        if resource_id is not None:
            path += '/' + str(resource_id)
        if relationship is not None:
            path += '/relationships/' + relationship
        return path

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

         => {
                'owner': {
                    'id': '345',
                    'name': 'percy',
                    'affiliation': 'stanford'
                },
                'parent': {
                    'id': '567'
                },
                'id': '123',
                'name': 'hello'
            }

        If the document does not follow the expected JSON API format, this
        method throws a JsonApiException.
        If relationships do not contain resource linkages (the 'data' item),
        this method also throws a JsonApiException.

        :param document: the JSON-API payload as a dict
        :return: unpacked resource info as a dict
        """
        def unpack_linkage(linkage):
            # Return recursively unpacked object if the data was included in the
            # document, otherwise just return the linkage object
            if linkage is None or (linkage['type'], linkage['id']) not in included:
                return linkage
            else:
                return unpack_object(included[linkage['type'], linkage['id']])

        def unpack_object(obj_data):
            # Merge attributes, id, meta, and relationships into a single dict
            obj = {}
            obj['id'] = obj_data['id']
            if 'attributes' in obj_data:
                obj.update(obj_data['attributes'])
            if 'meta' in obj_data:
                obj['meta'] = obj_data['meta']
            for key, relationship in obj_data.get('relationships', {}).iteritems():
                linkage = relationship['data']
                if isinstance(linkage, list):
                    obj[key] = [unpack_linkage(l) for l in linkage]
                else:
                    obj[key] = unpack_linkage(linkage)
            return obj

        # No response data
        if document is None:
            return None

        # Load included resources into dict indexed by (type, id)
        included = {
            (resource['type'], resource['id']): resource
            for resource in document.get('included', [])
        }

        # Build result info dict
        try:
            data = document.get('data', None)
            if isinstance(data, list):
                result = [unpack_object(d) for d in data]
            elif isinstance(data, dict):
                result = unpack_object(data)
            else:
                result = {}
        except KeyError:
            raise JsonApiException('Invalid or unsupported JSON API '
                                   'document format', True)

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

        If the dict does not follow the expected format, this method throws a
        JsonApiException.

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
                    relationships[key] = value.as_dict()
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
            raise JsonApiException('Invalid resource info format', True)

        return {
            'data': packed_objects
        }

    @wrap_exception('Unable to fetch {1}')
    def fetch(self, resource_type, resource_id=None, params=None):
        """
        Request to fetch a resource or resources.

        :param resource_type: resource type as string
        :param resource_id: id of resource to fetch, or None if bulk fetch
        :param params: dict of query parameters
        :return: the fetched objects
        """
        return self._unpack_document(
            self._make_request(
                method='GET',
                path=self._get_resource_path(resource_type, resource_id),
                query_params=self._pack_params(params)))

    @wrap_exception('Unable to create {1}')
    def create(self, resource_type, data, params=None):
        """
        Request to create a resource or resources.
        Always uses bulk update.

        :param resource_type: resource type as string
        :param data: info dict or list of info dicts
        :param params: dict of query parameters
        :return: the created object(s)
        """
        result = self._unpack_document(
            self._make_request(
                method='POST',
                path=self._get_resource_path(resource_type),
                query_params=self._pack_params(params),
                data=self._pack_document(
                    data if isinstance(data, list) else [data], resource_type)))
        # Return list iff original data was list
        return result if isinstance(data, list) else result[0]

    @wrap_exception('Unable to update {1}')
    def update(self, resource_type, data, params=None):
        """
        Request to update a resource or resources.
        Always uses bulk update.

        :param resource_type: resource type as string
        :param data: update dict or list of update dicts, update dicts
        must contain 'id' fields specifying the resources to update
        :param params: dict of query parameters
        :return: the updated object(s)
        """
        result = self._unpack_document(
            self._make_request(
                method='PATCH',
                path=self._get_resource_path(resource_type),
                query_params=self._pack_params(params),
                data=self._pack_document(
                    data if isinstance(data, list) else [data], resource_type)))
        # Return list iff original data was list
        return result if isinstance(data, list) else result[0]

    @wrap_exception('Unable to delete {1}')
    def delete(self, resource_type, resource_ids, params=None):
        """
        Request to delete a resource or resources.

        :param resource_type: resource type as string
        :param resource_ids: id or list of ids of resources to delete
        :param params: dict of query parameters
        :return: response data as dict, but otherwise undefined
        """
        if not isinstance(resource_ids, list):
            resource_ids = [resource_ids]
        data = {
            'data': [{
                'id': id_,
                'type': resource_type,
            } for id_ in resource_ids],
        }
        return self._unpack_document(
            self._make_request(
                method='DELETE',
                path=self._get_resource_path(resource_type),
                query_params=self._pack_params(params),
                data=data))

    @wrap_exception('Unable to create {1}/{2}/relationships/{3}')
    def create_relationship(self, resource_type, resource_id, relationship_key,
                            relationship, params=None):
        """
        Request to add to a to-many relationship.

        :param resource_type: resource type as string
        :param resource_id: id of resource to update
        :param relationship_key: name of the relationship to add to
        :param relationship: JsonApiRelationship defining link to add
        :param params: dict of query parameters
        :return: response data as dict, but otherwise undefined
        """
        return self._unpack_document(
            self._make_request(
                method='POST',
                path=self._get_resource_path(
                    resource_type, resource_id, relationship_key),
                query_params=self._pack_params(params),
                data=(relationship and relationship.as_dict())))

    @wrap_exception('Unable to delete {1}/{2}/relationships/{3}')
    def delete_relationship(self, resource_type, resource_id, relationship_key,
                            relationship, params=None):
        """
        Request to delete from a to-many relationship.

        :param resource_type: resource type as string
        :param resource_id: id of resource to update
        :param relationship_key: name of the relationship to delete from
        :param relationship: JsonApiRelationship defining link to delete
        :param params: dict of query parameters
        :return: response data as dict, but otherwise undefined
        """
        return self._unpack_document(
            self._make_request(
                method='DELETE',
                path=self._get_resource_path(
                    resource_type, resource_id, relationship_key),
                query_params=self._pack_params(params),
                data=(relationship and relationship.as_dict())))

    @wrap_exception('Unable to update authenticated user')
    def update_authenticated_user(self, data, params=None):
        """
        Request to update the authenticated user.
        Uses special /user endpoint, but keeps the 'users' resource type.

        :param data: dict containing user field updates
        :param params: dict of query parameters
        :return: updated user dict
        """
        return self._unpack_document(
            self._make_request(
                method='PATCH',
                path=self._get_resource_path('user'),
                query_params=self._pack_params(params),
                data=self._pack_document(data, 'users')))

    @wrap_exception('Unable to fetch contents info of bundle {1}')
    def fetch_contents_info(self, bundle_id, target_path='', depth=0):
        request_path = '/bundles/%s/contents/info/%s' % \
                       (bundle_id, urllib.quote(target_path))
        response = self._make_request('GET', request_path,
                                      query_params={'depth': depth})
        return response['data']

    @wrap_exception('Unable to fetch contents blob of bundle {1}')
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
        request_path = '/bundles/%s/contents/blob/%s' % \
                       (bundle_id, urllib.quote(target_path))
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
