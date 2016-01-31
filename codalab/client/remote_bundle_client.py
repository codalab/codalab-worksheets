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

from codalab.client import get_address_host
from codalab.client.bundle_client import BundleClient
from codalab.common import (
    PermissionError,
    UsageError,
    AuthorizationError,
)
from codalab.lib import (
  file_util,
  path_util,
  zip_util,
)
from codalab.server.rpc_file_handle import RPCFileHandle

# Hack to allow 64-bit integers
xmlrpclib.Marshaller.dispatch[int] = lambda _, v, w : w("<value><i8>%d</i8></value>" % v)


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
      'write_targets',
      'chown_bundles',
      'get_bundle_uuids',
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
      'update_worksheet_items',
      'update_worksheet_metadata',
      'delete_worksheet',
      'interpret_file_genpaths',
      'resolve_interpreted_items',
      # Commands related to authentication (in BundleClient).
      'login',
      'get_user_info',
      'update_user_info',
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
      # Commands related to ChatBox.
      'add_chat_log_info',
      'get_chat_log_info',
      'update_chat_log_info',
    )
    # Implemented by the BundleRPCServer.
    SERVER_COMMANDS = (
      'finish_upload_bundle',
      'open_target',  # Limited access to files (read)
      'open_target_archive',  # Limited access to files (read)
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
                import time
                time_delay = 1
                if self.verbose >= 2:
                    print 'remote_bundle_client: %s %s %s' % (command, args, kwargs)
                while True:
                    try:
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
                        elif 'codalab.common.AuthorizationError' in e.faultString:
                            index = e.faultString.find(':')
                            raise AuthorizationError(e.faultString[index + 1:])
                        else:
                            raise
                    except socket.error, e:
                        print >>sys.stderr, "Failed to connect to %s: %s. Trying to reconnect in %s seconds..." % (host, e, time_delay)
                        time.sleep(time_delay)
                        time_delay *= 2
                        if time_delay > 512:
                            raise UsageError('Failed to connect to %s: %s' % (host, e))
            return inner
        for command in self.COMMANDS:
            setattr(self, command, do_command(command))

    def upload_bundle(self, sources, follow_symlinks, exclude_patterns, git, unpack, remove_sources, info, worksheet_uuid, add_to_worksheet):
        """
        See local_bundle_client.py for documentation on the usage.
        Strategy:
        1) We copy the |sources| to a temporary directory on the server
          (streaming either a tar or tar.gz depending on whether compression is
          needed).
        2) We politely ask the server to finish_upload_bundle (performs a
          LocalBundleClient.upload_bundle from the temporary directory).
        """
        # URLs can be directly passed to the local client.
        if all(path_util.path_is_url(source) for source in sources):
            return self.upload_bundle_url(sources, follow_symlinks, exclude_patterns, git, unpack, remove_sources, info, worksheet_uuid, add_to_worksheet)

        # 1) Copy sources up to the server (temporary remote zip file)
        remote_file_uuids = []
        for source in sources:
            remote_file_uuid = self.open_temp_file(zip_util.add_packed_suffix(os.path.basename(source)))
            remote_file_uuids.append(remote_file_uuid)
            dest_handle = RPCFileHandle(remote_file_uuid, self.proxy)
            if zip_util.path_is_archive(source):
                source_handle = open(source)
            else:
                source_handle = zip_util.open_packed_path(source, follow_symlinks, exclude_patterns)
                unpack = True  # We packed it, so we have to unpack it
            status = 'Uploading %s%s to %s' % (source, ' ('+info['uuid']+')' if 'uuid' in info else '', self.address)
            # FileServer does not expose an API for forcibly flushing writes, so
            # we rely on closing the file to flush it.
            file_util.copy(source_handle, dest_handle, autoflush=False, print_status=status)
            dest_handle.close()

        # 2) Install upload (this call will be in charge of deleting the temporary file).
        result = self.finish_upload_bundle(remote_file_uuids, unpack, info, worksheet_uuid, add_to_worksheet)

        return result

    def open_target_handle(self, target):
        remote_file_uuid = self.open_target(target)
        if remote_file_uuid is not None:
            return RPCFileHandle(remote_file_uuid, self.proxy)
        return None

    def close_target_handle(self, handle):
        handle.close()
        self.finalize_file(handle.file_uuid)

    def cat_target(self, target, out):
        source = self.open_target_handle(target)
        if not source: return
        file_util.copy(source, out)
        self.close_target_handle(source)

    def download_target(self, target, final_path):
        source_uuid = self.open_target_archive(target)
        source = RPCFileHandle(source_uuid, self.proxy)
        zip_util.unpack(source, final_path)
        self.finalize_file(source_uuid)
