'''
RemoteBundleClient is a BundleClient implementation that shells out to a
BundleRPCServer for each command. Filesystem operations are implemented using
the FileServer operations exposed by the RPC server.
'''
import os
import contextlib
import sys
import urllib
import tempfile
import xmlrpclib
import socket
xmlrpclib.Marshaller.dispatch[int] = lambda _, v, w : w("<value><i8>%d</i8></value>" % v)  # Hack to allow 64-bit integers

from codalab.client import get_address_host
from codalab.client.bundle_client import BundleClient
from codalab.common import (
    PermissionError,
    UsageError
)
from codalab.lib import (
  file_util,
  path_util,
  zip_util,
)
from codalab.server.rpc_file_handle import RPCFileHandle

class AuthenticatedTransport(xmlrpclib.SafeTransport):
    '''
    Provides an implementation of xmlrpclib.Transport which injects an
    Authorization header into HTTP requests to the remove server.
    '''
    def __init__(self, address, get_auth_token):
        '''
        address: the address of the remote server
        get_auth_token: a function which yields the access token for
          the Bearer authentication scheme.
        '''
        xmlrpclib.SafeTransport.__init__(self, use_datetime=0)
        url_type, _ = urllib.splittype(address)
        if url_type not in ("http", "https"):
            raise UsageError("Unsupported protocol: expected http://... or https://... but got %s" % address)
        self._url_type = url_type
        self._bearer_token = get_auth_token

    def send_content(self, connection, request_body):
        '''
        Overrides Transport.send_content in order to inject Authorization header.
        '''
        _, command = xmlrpclib.loads(request_body)
        token = self._bearer_token(command)
        if token is not None and len(token) > 0:
            connection.putheader("Authorization", "Bearer: {0}".format(token))
        xmlrpclib.SafeTransport.send_content(self, connection, request_body)

    def make_connection(self, host):
        '''
        Create a connection based on the communication scheme, http vs https.
        '''
        if self._url_type == "https":
            return xmlrpclib.SafeTransport.make_connection(self, host)
        else:
            return xmlrpclib.Transport.make_connection(self, host)

############################################################

class RemoteBundleClient(BundleClient):
    # Implemented by a nested copy of a LocalBundleClient.
    CLIENT_COMMANDS = (
      'upload_bundle_url',
      'derive_bundle',
      'update_bundle_metadata',
      'delete_bundles',
      'kill_bundles',
      'chown_bundles',
      'get_bundle_uuid',
      'search_bundle_uuids',
      'get_bundle_info',
      'get_bundle_infos',
      'get_target_info',
      'head_target',
      'mimic',
      # Worksheet-related commands all have JSON-able inputs and outputs.
      'new_worksheet',
      'list_worksheets',
      'search_worksheets',
      'get_worksheet_uuid',
      'get_worksheet_info',
      'add_worksheet_item',
      'update_worksheet',
      'rename_worksheet',
      'chown_worksheet',
      'delete_worksheet',
      'interpret_file_genpaths',
      'resolve_interpreted_items',
      # Commands related to authentication (in BundleClient).
      'login',
      # Commands related to groups and permissions.
      'list_groups',
      'new_group',
      'rm_group',
      'group_info',
      'user_info',
      'add_user',
      'rm_user',
      'set_bundles_perm',
      'set_worksheet_perm',
    )
    # Implemented by the BundleRPCServer.
    SERVER_COMMANDS = (
      'upload_bundle_zip',
      'open_target',  # Limited access to files (read)
      'open_target_zip',  # Limited access to files (read)
    )
    # Implemented by the FileServer (superclass of BundleRPCServer).
    FILE_COMMANDS = (
      'open_temp_file',  # Limited access to files (write)
      'read_file',
      'readline_file',
      'tell_file',
      'seek_file',
      'write_file',
      'close_file',
      'finalize_file',
    )
    COMMANDS = CLIENT_COMMANDS + SERVER_COMMANDS + FILE_COMMANDS

    def __init__(self, address, get_auth_token, verbose):
        self.address = address
        self.verbose = verbose
        host = get_address_host(address)
        transport = AuthenticatedTransport(host, lambda cmd: None if cmd == 'login' else get_auth_token(self))
        self.proxy = xmlrpclib.ServerProxy(host, transport=transport, allow_none=True)
        def do_command(command):
            def inner(*args, **kwargs):
                try:
                    if self.verbose >= 2:
                        print 'remote_bundle_client: %s %s %s' % (command, args, kwargs)
                    return getattr(self.proxy, command)(*args, **kwargs)
                except xmlrpclib.ProtocolError, e:
                    raise UsageError("Could not authenticate on %s: %s" % (host, e))
                except xmlrpclib.Fault, e:
                    # Transform server-side UsageErrors into client-side UsageErrors.
                    if 'codalab.common.UsageError' in e.faultString:
                        index = e.faultString.find(':')
                        raise UsageError(e.faultString[index + 1:])
                    elif 'codalab.common.PermissionError' in e.faultString:
                        index = e.faultString.find(':')
                        raise PermissionError(e.faultString[index + 1:])
                    else:
                        raise
                except socket.error, e:
                    raise UsageError('Failed to connect to %s: %s' % (host, e))
            return inner
        for command in self.COMMANDS:
            setattr(self, command, do_command(command))

    def upload_bundle(self, path, info, worksheet_uuid, follow_symlinks, exclude_patterns, add_to_worksheet):
        # URLs can be directly passed to the local client.
        if path and not isinstance(path, list) and path_util.path_is_url(path):
            return self.upload_bundle_url(path, info, worksheet_uuid, follow_symlinks, exclude_patterns)

        # First, zip path up (temporary local zip file).
        if path:
            name = info['metadata']['name']
            zip_path = zip_util.zip(path, follow_symlinks=follow_symlinks, exclude_patterns=exclude_patterns, file_name=name)
            # Copy it up to the server (temporary remote zip file)
            with open(zip_path, 'rb') as source:
                remote_file_uuid = self.open_temp_file()
                dest = RPCFileHandle(remote_file_uuid, self.proxy)
                # FileServer does not expose an API for forcibly flushing writes, so
                # we rely on closing the file to flush it.
                file_util.copy(source, dest, autoflush=False, print_status='Uploading %s%s to %s' % (zip_path, ' ('+info['uuid']+')' if 'uuid' in info else '', self.address))
                dest.close()
        else:
            remote_file_uuid = None
            zip_path = None

        # Finally, install the zip file (this will be in charge of deleting that zip file).
        result = self.upload_bundle_zip(remote_file_uuid, info, worksheet_uuid, follow_symlinks, add_to_worksheet)

        if zip_path:
            path_util.remove(zip_path)  # Remove local zip
        return result

    def open_target_handle(self, target):
        remote_file_uuid = self.open_target(target)
        if remote_file_uuid:
            return RPCFileHandle(remote_file_uuid, self.proxy)
        return None
    def close_target_handle(self, handle):
        handle.close()
        self.finalize_file(handle.file_uuid, False)

    def cat_target(self, target, out):
        source = self.open_target_handle(target)
        if not source: return
        file_util.copy(source, out)
        self.close_target_handle(source)

    def download_target(self, target, follow_symlinks, return_zip=False):
        # Create remote zip file, download to local zip file
        (fd, zip_path) = tempfile.mkstemp(dir=tempfile.gettempdir())
        os.close(fd)
        source_uuid, name = self.open_target_zip(target, follow_symlinks)
        source = RPCFileHandle(source_uuid, self.proxy)
        with open(zip_path, 'wb') as dest:
            with contextlib.closing(source):
                file_util.copy(source, dest, autoflush=False, print_status='Downloading %s on %s to %s' % ('/'.join(target), self.address, zip_path))

        self.finalize_file(source_uuid, True)  # Delete remote zip file
        # Unpack the local zip file
        container_path = tempfile.mkdtemp()
        if return_zip:
            return zip_path, container_path

        result_path = zip_util.unzip(zip_path, container_path, name)
        path_util.remove(zip_path)  # Delete local zip file

        return (result_path, container_path)

    def copy_bundle(self, source_bundle_uuid, info, dest_client, dest_worksheet_uuid, add_to_worksheet):
        '''
        A streamlined combination of download_target and upload_bundle.
        Copy from self to dest_client.
        '''
        # Open source
        source_file_uuid, name = self.open_target_zip((source_bundle_uuid, ''), False)
        source = RPCFileHandle(source_file_uuid, self.proxy)
        # Open target
        dest_file_uuid = dest_client.open_temp_file()
        dest = RPCFileHandle(dest_file_uuid, dest_client.proxy)

        # Copy contents over
        file_util.copy(source, dest, autoflush=False, print_status='Copying %s from %s to %s' % (source_bundle_uuid, self.address, dest_client.address))
        dest.close()
        # Finally, install the zip file (this will be in charge of deleting that zip file).
        result = dest_client.upload_bundle_zip(dest_file_uuid, info, dest_worksheet_uuid, False)

        self.finalize_file(source_file_uuid, True)  # Delete remote zip file
        return result
