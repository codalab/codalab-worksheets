import http.client
import socket
import sys
import six
import urllib.request
import urllib.parse
import urllib.error

from codalab.common import http_error_to_exception, precondition, ensure_str, UsageError
from codalab.worker.rest_client import RestClient, RestClientException
from codalab.worker.download_util import BundleTarget


def wrap_exception(message):
    def decorator(f):
        def wrapper(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            except urllib.error.HTTPError as e:
                # Translate known errors to the standard CodaLab errors
                error_body = ensure_str(e.read())
                exc = http_error_to_exception(e.code, error_body)
                # All standard CodaLab errors are subclasses of UsageError
                if isinstance(exc, UsageError):
                    six.reraise(exc.__class__, exc, sys.exc_info()[2])
                else:
                    # Shunt other exceptions into one class
                    six.reraise(
                        JsonApiException,
                        JsonApiException(
                            message.format(*args, **kwargs)
                            + ': '
                            + http.client.responses[e.code]
                            + ' - '
                            + error_body,
                            400 <= e.code < 500,
                        ),
                        sys.exc_info()[2],
                    )
            except RestClientException as e:
                six.reraise(
                    JsonApiException,
                    JsonApiException(
                        message.format(*args, **kwargs) + ': ' + str(e), e.client_error
                    ),
                    sys.exc_info()[2],
                )
            except (urllib.error.URLError, http.client.HTTPException, socket.error) as e:
                six.reraise(
                    JsonApiException,
                    JsonApiException(message.format(*args, **kwargs) + ': ' + str(e), False),
                    sys.exc_info()[2],
                )

        return wrapper

    return decorator


class JsonApiException(RestClientException):
    """
    Exception raised by the JsonApiClient methods on error. If
    client_error is False, the failure is caused by a server-side error and
    can be retried.
    """


class JsonApiRelationship(dict):
    """
    Placeholder for a relationship to another resource.
    Used to build requests to create or update a resource.

    For example, to update the owner of a bundle:
        client.update('bundles', {
            'id': '0x7d67f3e0fda249e5b0531670f473c04f',
            'owner': JsonApiRelationship('users', owner_id)
        })

    JsonApiRelationship is also a subclass of dict, to store and provide access
    to the attributes of the referred object.
    """

    def __init__(self, type_, id_, *args):
        self.type_ = type_
        self.id_ = id_
        dict.__init__(self, *args)
        # Allow the JSON API resource object "attributes" [not Python instance
        # attributes] to override the type and id keys in the dict.
        # The actual type and id will still be accessible as instance attrs.
        self.setdefault('type', type_)
        self.setdefault('id', id_)

    def as_linkage(self):
        """Serialize into relationship linkage dict for JSON API requests."""
        return {'data': {'type': self.type_, 'id': self.id_}}

    def __eq__(self, other):
        return self.type_ == other.type_ and self.id_ == other.id_ and dict.__eq__(self, other)

    def __neq__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return 'JsonApiRelationship(type_=%s, id_=%s, data=%s)' % (
            self.type_,
            self.id_,
            dict.__repr__(self),
        )

    def __bool__(self):
        """
        Implements value of bool(relationship).
        Should be true for non-empty relationships.
        """
        return True


class EmptyJsonApiRelationship(JsonApiRelationship):
    """
    Represents an empty to-one relationship.

    Note that we have:

        assert bool(EmptyJsonApiRelationship()) == False

    Using an EmptyJsonApiRelationship is required when the client needs
    to set a to-one relationship to null, otherwise _pack_document will be
    unable to figure out whether the user is attempting to set an attribute
    or a relationship to null.
    """

    def __init__(self):
        JsonApiRelationship.__init__(self, None, None)

    def as_linkage(self):
        """Empty relationships should be serialized as a null linkage."""
        return {'data': None}

    def __bool__(self):
        """Empty relationship should be falsey."""
        return False

    def __repr__(self):
        return 'EmptyJsonApiRelationship()'


class JsonApiClient(RestClient):
    """
    Simple JSON API client.
    """

    def __init__(self, address, get_access_token, extra_headers={}, check_version=lambda _: None):
        self._get_access_token = get_access_token
        self._extra_headers = extra_headers
        self._check_version = check_version
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
        Convert query parameters into the canonical format expected by the server.

        :param dict[str, *] params: map from query parameter key to value
        :rtype: list[(str, str)]
        """
        if params is None:
            return None

        result = []

        # `include` query parameter is a comma-separated list, as defined by:
        # http://jsonapi.org/format/#fetching-includes
        include = params.pop('include', None)
        if include is not None:
            result.append(('include', ','.join(include)))

        for k, v in params.items() if isinstance(params, dict) else params:
            if isinstance(v, list):
                for item in map(str, v):
                    result.append((k, item))
            elif isinstance(v, bool):
                result.append((k, int(v)))
            else:
                result.append((k, v))

        return result

    def _unpack_document(self, document):
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
            if linkage is None:
                return EmptyJsonApiRelationship()
            elif (linkage['type'], linkage['id']) in included:
                # Wrap in a JsonApiRelationship proxy
                # This allows you to send an unpacked object back up through
                # create or update requests.
                return JsonApiRelationship(
                    linkage['type'],
                    linkage['id'],
                    unpack_object(included[linkage['type'], linkage['id']]),
                )
            else:
                return JsonApiRelationship(linkage['type'], linkage['id'])

        def unpack_object(obj_data):
            # Merge attributes, id, meta, and relationships into a single dict
            obj = {}
            obj['id'] = obj_data['id']
            if 'attributes' in obj_data:
                obj.update(obj_data['attributes'])
            if 'meta' in obj_data:
                obj['meta'] = obj_data['meta']
            for key, relationship in obj_data.get('relationships', {}).items():
                linkage = relationship['data']
                if isinstance(linkage, list):
                    obj[key] = [unpack_linkage(v) for v in linkage]
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
            raise JsonApiException('Invalid or unsupported JSON API ' 'document format', True)

        if 'meta' in document:
            meta = document['meta']
            # Check version
            if 'version' in meta:
                self._check_version(meta['version'])

            # Include document meta if there is only a single object
            # (Warning: this will overwrite any meta present at the resource object level.)
            if isinstance(result, dict):
                result['meta'] = meta

        return result

    def _pack_document(self, objects, type_):
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
            for key, value in obj.items():
                if isinstance(value, JsonApiRelationship):
                    relationships[key] = value.as_linkage()
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

        return {'data': packed_objects}

    @wrap_exception('Unable to fetch {1}')
    def fetch(self, resource_type, resource_id=None, params=None, include=None):
        """
        Request to fetch a resource or resources.

        :param resource_type: resource type as string
        :param resource_id: id of resource to fetch, or None if bulk fetch
        :param params: dict of query parameters
        :param include: iterable of related resources to include
        :return: the fetched objects
        """
        return self._unpack_document(
            self._make_request(
                method='GET',
                path=self._get_resource_path(resource_type, resource_id),
                query_params=self._pack_params(params),
            )
        )

    def fetch_one(self, resource_type, resource_id=None, params=None):
        """
        Same as JsonApiClient.fetch, but always returns exactly one resource
        dictionary, or throws a NotFoundError if the results contain any more
        or less than exactly one.
        """
        results = self.fetch(resource_type, resource_id=resource_id, params=params)
        precondition(
            not isinstance(results, list) or len(results) == 1,
            "Got %d %s when expecting exactly 1." % (len(results), resource_type),
        )
        if not isinstance(results, list):
            return results
        else:
            return results[0]

    @wrap_exception('Unable to netcat {1}')
    def netcat(self, bundle_id, port, data):
        """
        Request to send data to a running bundle

        :param bundle_id: running bundle uuid
        :param data: bytestring
        :param port: service port running on bundle
        :return: the response
        """
        request_path = '/bundles/%s/netcat/%s/' % (bundle_id, port)
        return self._make_request('PUT', request_path, data=data, return_response=True)

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
                data=self._pack_document(data if isinstance(data, list) else [data], resource_type),
            )
        )
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
                data=self._pack_document(data if isinstance(data, list) else [data], resource_type),
            )
        )
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
        data = {'data': [{'id': id_, 'type': resource_type} for id_ in resource_ids]}
        return self._unpack_document(
            self._make_request(
                method='DELETE',
                path=self._get_resource_path(resource_type),
                query_params=self._pack_params(params),
                data=data,
            )
        )

    @wrap_exception('Unable to create {1}/{2}/relationships/{3}')
    def create_relationship(
        self, resource_type, resource_id, relationship_key, relationship, params=None
    ):
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
                path=self._get_resource_path(resource_type, resource_id, relationship_key),
                query_params=self._pack_params(params),
                data=(relationship and relationship.as_linkage()),
            )
        )

    @wrap_exception('Unable to delete {1}/{2}/relationships/{3}')
    def delete_relationship(
        self, resource_type, resource_id, relationship_key, relationship, params=None
    ):
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
                path=self._get_resource_path(resource_type, resource_id, relationship_key),
                query_params=self._pack_params(params),
                data=(relationship and relationship.as_linkage()),
            )
        )

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
                data=self._pack_document(data, 'users'),
            )
        )

    @wrap_exception('Unable to fetch contents info of bundle {1}')
    def fetch_interpreted_worksheet(self, worksheet_uuid):
        request_path = '/interpret/worksheet/%s' % worksheet_uuid
        response = self._make_request('GET', request_path)
        return response

    @wrap_exception('Unable to fetch contents info of bundle {1}')
    def fetch_contents_info(self, target, depth=0):
        """
        Calls download_manager.get_target_info server-side and returns the target_info.
        For details on return value look at worker.download_util.get_target_info
        :param target: a worker.download_util.BundleTarget
        """
        request_path = '/bundles/%s/contents/info/%s' % (
            target.bundle_uuid,
            urllib.parse.quote(target.subpath),
        )
        response = self._make_request('GET', request_path, query_params={'depth': depth})
        # Deserialize the target. See /rest/bundles/_fetch_contents_info for serialization side
        response['data']['resolved_target'] = BundleTarget.from_dict(
            response['data']['resolved_target']
        )
        return response['data']

    @wrap_exception('Unable to fetch contents blob of bundle {1}')
    def fetch_contents_blob(self, target, range_=None, head=None, tail=None, truncation_text=None):
        """
        Returns a file-like object for the target on the given bundle.

        :param target: A worker.download_util.BundleTarget
        :param range_: range of bytes to fetch
        :param head: number of lines to summarize from beginning of file
        :param tail: number of lines to summarize from end of file
        :return: file-like object containing requested data blob
        """
        request_path = '/bundles/%s/contents/blob/%s' % (
            target.bundle_uuid,
            urllib.parse.quote(target.subpath),
        )
        headers = {'Accept-Encoding': 'gzip'}
        if range_ is not None:
            headers['Range'] = 'bytes=%d-%d' % range_
        params = {'support_redirect': 1}
        if head is not None:
            params['head'] = head
        if tail is not None:
            params['tail'] = tail
        if truncation_text is not None:
            params['truncation_text'] = truncation_text
        return self._make_request(
            'GET', request_path, headers=headers, query_params=params, return_response=True
        )

    @wrap_exception('Unable to upload contents of bundle {1}')
    def upload_contents_blob(self, bundle_id, fileobj=None, params=None, progress_callback=None):
        """
        Uploads the contents of the given fileobj as the contents of specified
        bundle.

        :param bundle_id: the id of the target bundle
        :param fileobj: file-like object containing the data to upload
        :param params: dict of query parameters
        :param progress_callback: function that will be called periodically
                                  with the number of bytes uploaded so far
        :return: None
        """
        request_path = '/bundles/%s/contents/blob/' % bundle_id
        params = params or {}
        params['finalize_on_failure'] = True  # no retry mechanism implemented yet
        params = self._pack_params(params)
        if fileobj is None:
            self._make_request(method='PUT', path=request_path, query_params=params)
        else:
            self._upload_with_chunked_encoding(
                method='PUT',
                url=request_path,
                query_params=params,
                fileobj=fileobj,
                progress_callback=progress_callback,
            )

    @wrap_exception('Unable to get the locations of bundles')
    def get_bundles_locations(self, bundle_uuids):
        response = self._make_request(
            method='GET',
            path='/bundles/locations',
            query_params=self._pack_params({'uuids': bundle_uuids}),
        )
        return response['data']

    @wrap_exception('Unable to interpret file genpaths')
    def interpret_file_genpaths(self, queries):
        """
        :param queries: list of (bundle_uuid, genpath, post) tuples
        :return: list of strings
        """
        return self._make_request(
            method='POST',
            path='/interpret/file-genpaths',
            data={
                'queries': [
                    {'bundle_uuid': bundle_uuid, 'genpath': genpath, 'post': post}
                    for bundle_uuid, genpath, post in queries
                ]
            },
        )['data']

    @wrap_exception('Unable to interpret genpath table contents')
    def interpret_genpath_table_contents(self, contents):
        return self._make_request(
            method='POST', path='/interpret/genpath-table-contents', data={'contents': contents}
        )['contents']

    @wrap_exception('Unable to update worksheet')
    def update_worksheet_raw(self, worksheet_id, lines):
        self._make_request(
            method='POST',
            path='/worksheets/%s/raw' % worksheet_id,
            headers={'Content-Type': 'text/plain'},
            data='\n'.join(lines),
        )

    @wrap_exception('Unable to fetch worker information')
    def get_workers_info(self):
        request_path = '/workers/info'
        response = self._make_request('GET', request_path)
        return response['data']

    @wrap_exception('Unable to get the locations of bundles')
    def get_bundle_locations(self, bundle_uuid):
        response = self._make_request(
            method='GET', path='/bundles/{}/locations/'.format(bundle_uuid),
        )
        return response['data']

    @wrap_exception('Unable to create the location of bundles')
    def add_bundle_location(self, bundle_uuid, bundle_store_uuid, params):
        response = self._make_request(
            method='POST',
            path='/bundles/{}/locations/'.format(bundle_uuid),
            data=self._pack_document(
                [{'bundle_uuid': bundle_uuid, 'bundle_store_uuid': bundle_store_uuid}],
                'bundle_locations',
            ),
            query_params=self._pack_params(params),
        )
        return response['data']

    @wrap_exception("Unable to finalize the state of blob storage bundles")
    def update_bundle_state(self, bundle_uuid, params):
        response = self._make_request(
            method='POST',
            path='/bundles/{}/state'.format(bundle_uuid),
            query_params=self._pack_params(params),
        )
        return response['data']
