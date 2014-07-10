'''
Abstract base class that describes the client interface for interacting with
the CodaLab bundle system.

There are three categories of BundleClient commands:
  - Commands that create and edit bundles: upload, make, run and update.
  - Commands for browsing bundles: info, ls, cat, search, and wait.
  - Various utility commands for pulling bundles back out of the system.

There are a couple of implementations of this class:
  - LocalBundleClient - interacts directly with a BundleStore and BundleModel.
  - RemoteBundleClient - shells out to a BundleRPCServer to implement its API.
'''
# TODO: We should probably implement grep at some point. grep will take a
# target (like the target passed to ls or cat) and a list of command-line args.
# The RemoteBundleClient implementation of grep will have to use the FileServer
# file-handle API to stream the results back.
import time
from sys import stdout

from codalab.common import State


class BundleClient(object):
    # Commands for creating/editing bundles: upload, make, run, edit, and delete.

    def upload_bundle(self, bundle_type, path, metadata, worksheet_uuid=None, check_validity=True):
        '''
        Create a new bundle with a copy of the directory at the given path in the
        local filesystem. Return its uuid. If the path leads to a file, the new
        bundle will only contain only that file.
        '''
        raise NotImplementedError

    def make_bundle(self, targets, metadata):
        '''
        Create a new bundle with dependencies on the given targets. Return its uuid.
        targets should be a dict mapping target keys to (bundle_spec, path) pairs.
        Each of the targets will by symlinked into the new bundle at its key.
        '''
        raise NotImplementedError

    def run_bundle(self, program_target, input_target, command, metadata):
        '''
        Run the given program bundle, create bundle of output, and return its uuid.
        The program and input targets are (bundle_spec, path) pairs that are
        symlinked in as dependencies during runtime.
        '''
        raise NotImplementedError

    def update_bundle_metadata(self, uuid, metadata):
        '''
        Update the bundle with the given uuid with the new metadata.
        '''
        raise NotImplementedError

    def delete_bundle(self, bundle_spec, force=False):
        '''
        bundle_spec should be either a bundle uuid, a unique prefix of a uuid, or
        a unique bundle name.

        Delete this bundle. If force is True, delete all its (direct and indirect)
        descendents too. If force is False, and if this bundle has any downstream
        dependencies, this method will raise a UsageError.
        '''
        raise NotImplementedError

    # Commands for browsing bundles: info, ls, cat, search, and wait.

    def get_bundle_info(self, bundle_uuid, parents=False, children=False):
        '''
        Return a dict containing detailed information about a given bundle:
          bundle_type: one of (program, dataset, macro, make, run)
          data_hash: hash of the bundle's data, if the bundle is READY
          metadata: its metadata dict
          state: its current state
          uuid: its uuid
          hard_dependencies: list of this bundle's realized dependencies

        If parents is True, this dict will also map 'parents' to a list of string
        representations of each bundle that this bundle depends on. If children is
        True, then it will map 'children' to bundles that depend on this bundle.

        Note that the list of parents and children include all logical dependencies,
        not just dependencies that are realized within the final bundle. For
        example, a run would depend on its program and input even though symlinks to
        those bundles are deleted before the program is uploaded.
        '''
        raise NotImplementedError

    def get_target_info(self, target, depth):
        '''
        Return information about the given target (bundle_uuid, subpath).
        Recurse up to the given depth.
        '''
        raise NotImplementedError

    def cat_target(self, target):
        '''
        Print the contents of the target file to stdout.
        '''
        raise NotImplementedError

    def head_target(self, target, num_lines):
        '''
        Return contents of target file as a list of lines.
        '''
        raise NotImplementedError

    def download_target(self, target):
        '''
        Download a target. Return the local path to where target has been
        downloaded.
        '''
        raise NotImplementedError

    #############################################################################
    # Worksheet-related client methods follow!
    #############################################################################

    def new_worksheet(self, name):
        '''
        Create a new worksheet with the given name and return its uuid.
        '''
        raise NotImplementedError

    def list_worksheets(self):
        '''
        Return a list of worksheet row dicts. Does NOT include worksheet items.
        '''
        raise NotImplementedError

    def get_worksheet_info(self, worksheet_spec):
        '''
        worksheet_spec should be either a worksheet uuid, a unique prefix of a uuid,
        or a unique worksheet name. Return an info dict for this worksheet.

        This dict will have the following keys:
          uuid: the worksheet uuid
          name: the worksheet name
          items: an list of (bundle_info, value) pairs, where bundle_info is either:
                  - a bundle info dict
                  - a dict mapping 'uuid' to a bundle_uuid, if the uuid is orphaned
                  - None (for non-bundle rows)
          last_item_id: the last database id of any item in the list
        '''
        raise NotImplementedError

    def add_worksheet_item(self, worksheet_spec, bundle_spec):
        '''
        Add the bundle specified by the bundle_spec to the worksheet specified by
        the worksheet_spec.
        '''
        raise NotImplementedError

    def update_worksheet(self, worksheet_info, new_items):
        '''
        Take a worksheet info dict and a list of new (bundle_spec, value) pairs and
        update the worksheet. Raise a UsageError if there was a concurrent update.
        '''
        raise NotImplementedError

    def rename_worksheet(self, worksheet_spec, name):
        '''
        Update the specified worksheet to have the new name.
        '''
        raise NotImplementedError

    def delete_worksheet(self, worksheet_spec):
        '''
        Delete the specified worksheet.
        '''
        raise NotImplementedError

    #############################################################################
    # Commands for authentication
    #############################################################################

    def login(self, grant_type, username, key):
        '''
        Generate OAuth access token from username/password or from a refresh token.

        grant_type: Type of grant requested: 'credentials' or 'refresh_token'.
        username: Name of user to authenticate.
        key: User's secret which is a password for the 'credentials' grant type
            or a refresh token for the 'refresh_token' grant type.

        If the grant succeeds, the method returns a dictionary of the form:
        { 'token_type': 'Bearer',
          'access_token': <token>,
          'expires_in': <span in seconds>,
          'refresh_token': <token> }
        If the grant fails because of invalid credentials, None is returned.
        '''
        if not hasattr(self, 'auth_handler'):
            raise NotImplementedError
        return self.auth_handler.generate_token(grant_type, username, key)

    #############################################################################
    # Commands for groups and permissions.
    #############################################################################

    def list_groups(self):
        '''
        Returns a list of group row dicts. These are the groups of the current user.
        '''
        raise NotImplementedError

    def new_group(self, name):
        '''
        Create a group.
        '''
        raise NotImplementedError

    def rm_group(self, group_spec):
        '''
        Delete a group.
        '''
        raise NotImplementedError

    def group_info(self, group_spec):
        '''
        Show details (including membership) about the specified group.
        '''
        raise NotImplementedError

    def add_user(self, username, group_spec, is_admin):
        '''
        Add a user to a group.
        '''
        raise NotImplementedError

    def rm_user(self, username, group_spec):
        '''
        Remove a user from a group.
        '''
        raise NotImplementedError

    def set_worksheet_perm(self, worksheet_spec, permission, group_spec):
        '''
        Set permission for a group on a worksheet.
        '''
        raise NotImplementedError
