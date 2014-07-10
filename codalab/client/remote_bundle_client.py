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

from codalab.client import get_address_host
from codalab.client.bundle_client import BundleClient
from codalab.common import (
    PermissionError,
    UsageError
)
from codalab.lib import (
  file_util,
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
            raise IOError("unsupported XML-RPC protocol")
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
      'make_bundle',
      'run_bundle',
      'update_bundle_metadata',
      'delete_bundle',
      'get_bundle_uuid',
      'get_bundle_info',
      'get_target_info',
      'head_target',
      # Worksheet-related commands all have JSON-able inputs and outputs.
      'new_worksheet',
      'list_worksheets',
      'get_worksheet_info',
      'add_worksheet_item',
      'update_worksheet',
      'rename_worksheet',
      'delete_worksheet',
      # Commands related to authentication.
      'login',
      # Commands related to groups and permissions.
      'list_groups',
      'new_group',
      'rm_group',
      'group_info',
      'add_user',
      'rm_user',
      'set_worksheet_perm',
    )
    # Implemented by the BundleRPCServer.
    SERVER_COMMANDS = (
      'upload_zip',
      'download_target_zip',
      'open_target_uuid',
    )
    # Implemented by the FileServer (superclass of BundleRPCServer).
    FILE_COMMANDS = (
      'open_temp_file',
      'read_file',
      'readline_file',
      'tell_file',
      'seek_file',
      'write_file',
      'close_file',
    )
    COMMANDS = CLIENT_COMMANDS + SERVER_COMMANDS + FILE_COMMANDS

    def __init__(self, address, get_auth_token):
        self.address = address
        host = get_address_host(address)
        transport = AuthenticatedTransport(host, lambda cmd: None if cmd == 'login' else get_auth_token(self))
        self.proxy = xmlrpclib.ServerProxy(host, transport=transport, allow_none=True)
        def do_command(command):
            def inner(*args, **kwargs):
                try:
                    print 'remote_bundle_client: %s %s %s' % (command, args, kwargs)
                    return getattr(self.proxy, command)(*args, **kwargs)
                except xmlrpclib.ProtocolError, e:
                    if e.errcode == 401:
                        raise UsageError("Could not authenticate request.")
                    else:
                        raise
                except xmlrpclib.Fault, e:
                    # Transform server-side UsageErrors into client-side UsageErrors.
                    if 'codalab.common.UsageError' in e.faultString:
                        index = e.faultString.find(':')
                        raise UsageError(e.faultString[index + 1:])
                    elif 'codalab.common.PermissionError' in e.faultString:
                        raise PermissionError()
                    else:
                        raise
            return inner
        for command in self.COMMANDS:
            setattr(self, command, do_command(command))

    def upload_bundle(self, bundle_type, path, metadata, worksheet_uuid=None, check_validity=True):
        zip_path = zip_util.zip(path)
        with open(zip_path, 'rb') as source:
            remote_file_uuid = self.open_temp_file()
            dest = RPCFileHandle(remote_file_uuid, self.proxy)
            with contextlib.closing(dest):
                # FileServer does not expose an API for forcibly flushing writes, so
                # we rely on closing the file to flush it.
                file_util.copy(source, dest, autoflush=False)
        return self.upload_zip(bundle_type, remote_file_uuid, metadata,
                worksheet_uuid, check_validity)

    def cat_target(self, target, out):
        source = self.open_target(target)
        with contextlib.closing(source):
            file_util.copy(source, out)

    def open_target(self, target):
        remote_file_uuid = self.open_target_uuid(target)
        if remote_file_uuid:
            return RPCFileHandle(remote_file_uuid, self.proxy)
        return None

    def download_target(self, target):
        (fd, dest_path) = tempfile.mkstemp(dir=tempfile.gettempdir())
        os.close(fd)
        source_uuid = self.download_target_zip(target)
        source = RPCFileHandle(source_uuid, self.proxy)
        with open(dest_path, 'wb') as dest:
            with contextlib.closing(source):
                file_util.copy(source, dest, autoflush=False)
        path = zip_util.unzip(dest_path)
        return path
