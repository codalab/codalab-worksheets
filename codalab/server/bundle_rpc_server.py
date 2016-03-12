'''
BundleRPCServer is an XML RPC server with RPC methods supporting a
RemoteBundleClient.

Methods that take JSON-able input and return JSON-able output (that is, methods
in RemoteBundleClient.CLIENT_COMMANDS) are simply passed to the internal
LocalBundleClient client.

Other methods, like upload_bundle and cat_target, are more complicated because
they perform filesystem operations. BundleRPCServer supports variants of these
methods:

  finish_upload_bundle: used to implement RemoteBundleClient.upload_bundle
  open_target: used to implement RemoteBundleClient.cat_target

These methods are supported by the FileServer.

Important: each call to open_temp_file, open_target, open_target_archive should
have a matching call to finalize_file.
'''
from SimpleXMLRPCServer import  SimpleXMLRPCServer, SimpleXMLRPCRequestHandler
import SocketServer
import time
import traceback
import xmlrpclib

from codalab.common import (
    precondition,
    UsageError,
    PermissionError,
)
from codalab.client.remote_bundle_client import RemoteBundleClient
from codalab.server.file_server import FileServer


# Hack to allow 64-bit integers
xmlrpclib.Marshaller.dispatch[int] = lambda _, v, w : w("<value><i8>%d</i8></value>" % v)


class AuthenticatedXMLRPCRequestHandler(SimpleXMLRPCRequestHandler):
    """
    Simple XML-RPC request handler class which also reads authentication
    information included in HTTP headers.
    """

    def decode_request_content(self, data):
        '''
        Overrides in order to capture Authorization header.
        '''
        token = None
        if 'Authorization' in self.headers:
            value = self.headers.get("Authorization", "")
            token = value[8:] if value.startswith("Bearer: ") else ""

        if self.server.auth_handler.validate_token(token):
            return SimpleXMLRPCRequestHandler.decode_request_content(self, data)
        else:
            self.send_response(401, "Could not authenticate with OAuth")
            self.send_header("WWW-Authenticate", "realm=\"https://www.codalab.org\"")
            self.send_header("Content-length", "0")
            self.end_headers()

    def send_response(self, code, message=None):
        '''
        Overrides to capture end of request.
        '''
        # Clear current user
        self.server.auth_handler.validate_token(None)
        SimpleXMLRPCRequestHandler.send_response(self, code, message)


class BundleRPCServer(SocketServer.ThreadingMixIn, SimpleXMLRPCServer):
    def __init__(self, manager):
        self.host = manager.config['server']['host']
        self.port = manager.config['server']['port']
        self.verbose = manager.config['server']['verbose']
        self.auth_handler = manager.auth_handler()

        # This server is backed by a LocalBundleClient that processes client commands
        self.client = manager.client('local', is_cli=False)

        # This server is backed by a file server that processes file commands.
        self.file_server = FileServer()

        # args might be a large object; summarize it (e.g., take prefixes of lists)
        def compress_args(args):
            if isinstance(args, basestring):
                if len(args) > 100:
                    args = args[:100] + '...'
            if isinstance(args, list):
                if len(args) > 4:
                    args = args[:4] + ['...']
            if isinstance(args, tuple):
                return tuple(map(compress_args, args))
            elif isinstance(args, list):
                return map(compress_args, args)
            elif isinstance(args, dict):
                return dict((compress_args(k), compress_args(v)) for k, v in args.items())
            return args

        SimpleXMLRPCServer.__init__(self, (self.host, self.port), allow_none=True,
                                    requestHandler=AuthenticatedXMLRPCRequestHandler,
                                    logRequests=(self.verbose >= 1))

        def wrap(target, command):
            def function_to_register(*args, **kwargs):
                # Process args for logging
                if command == 'login':
                    log_args = args[:2]  # Don't log password
                else:
                    log_args = compress_args(args)

                if self.verbose >= 1:
                    print "bundle_rpc_server: %s %s" % (command, log_args)

                try:
                    start_time = time.time()

                    # Dynamically bind method and call it
                    result = getattr(target, command)(*args, **kwargs)

                    # Log this activity.
                    if not isinstance(target, FileServer):
                        self.client.model.update_events_log(
                            start_time=start_time,
                            user_id=self.client._current_user_id(),
                            user_name=self.client._current_user_name(),
                            command=command,
                            args=log_args)

                    return result
                except Exception, e:
                    if not (isinstance(e, UsageError) or isinstance(e, PermissionError)):
                        # This is really bad and shouldn't happen.
                        # If it does, someone should get paged.
                        print '=== INTERNAL ERROR:', e
                        traceback.print_exc()
                    raise e

            return function_to_register

        for command in RemoteBundleClient.CLIENT_COMMANDS:
            self.register_function(wrap(self.client, command), command)

        for command in RemoteBundleClient.SERVER_COMMANDS:
            self.register_function(wrap(self, command), command)

        for command in RemoteBundleClient.FILE_COMMANDS:
            print(command)
            self.register_function(wrap(self.file_server, command), command)

    def finish_upload_bundle(self, file_uuids, unpack, info, worksheet_uuid, add_to_worksheet):
        '''
        |file_uuids| specifies a pointer to temporary files.
        Upload these and return the new bundle's uuid.
        Note: delete the file_uuids as these are temporary files.
        '''
        if file_uuids is not None:
            paths = []
            for file_uuid in file_uuids:
                # Note: cheat and look at file_server's data to get paths
                precondition(file_uuid in self.file_server.file_paths, 'Invalid file_uuid: %s' % file_uuid)
                paths.append(self.file_server.file_paths[file_uuid])
        else:
            paths = None

        # Upload the paths
        result = self.client.upload_bundle(
            paths,
            follow_symlinks=False,
            exclude_patterns=None,
            git=False,
            unpack=unpack,
            remove_sources=True,
            info=info,
            worksheet_uuid=worksheet_uuid,
            add_to_worksheet=add_to_worksheet)

        # Remove temporary file
        if file_uuids is not None:
            for file_uuid in file_uuids:
                self.finalize_file(file_uuid)
        return result

    def open_target(self, target):
        '''
        Open a read-only file handle to the given bundle target and return a file
        uuid identifying it.
        '''
        path = self.client.get_target_path(target)
        if path is None:
            return None
        return self.file_server.open_file(path)

    def open_target_archive(self, target):
        '''
        Return file uuid for the archive file.
        '''
        path = self.client.get_target_path(target)
        if path is None:
            return None
        return self.file_server.open_packed_path(path)

    def serve_forever(self):
        print 'BundleRPCServer serving to %s at port %s...' % ('ALL hosts' if self.host == '' else 'host ' + self.host, self.port)
        SimpleXMLRPCServer.serve_forever(self)
