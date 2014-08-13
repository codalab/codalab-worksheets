'''
LocalBundleClient is BundleClient implementation that interacts directly with a
BundleStore and a BundleModel. All filesystem operations are handled locally.
'''
from time import sleep
import contextlib
import os, sys

from codalab.bundles import (
    get_bundle_subclass,
    UPLOADED_TYPES,
)
from codalab.common import (
  precondition,
  State,
  UsageError,
  AuthorizationError,
  Command,
)
from codalab.client.bundle_client import BundleClient
from codalab.lib import (
    canonicalize,
    path_util,
    file_util,
    worksheet_util,
)
from codalab.objects.worksheet import Worksheet
from codalab.objects import permission
from codalab.objects.permission import (
    check_has_full_permission,
    check_has_read_permission,
    Group,
    parse_permission
)

def authentication_required(func):
    def decorate(self, *args, **kwargs):
        if self.auth_handler.current_user() is None:
            raise AuthorizationError("Not authenticated")
        return func(self, *args, **kwargs)
    return decorate

class LocalBundleClient(BundleClient):
    def __init__(self, address, bundle_store, model, auth_handler, verbose):
        self.address = address
        self.bundle_store = bundle_store
        self.model = model
        self.auth_handler = auth_handler
        self.verbose = verbose

    def _current_user_id(self):
        return self.auth_handler.current_user().unique_id

    def _bundle_to_bundle_info(self, bundle, children=None):
        '''
        Helper: Convert bundle to bundle_info.
        '''
        hard_dependencies = bundle.get_hard_dependencies()
        # See tables.py
        result = {
          'uuid': bundle.uuid,
          'bundle_type': bundle.bundle_type,
          'command': bundle.command,
          'data_hash': bundle.data_hash,
          'state': bundle.state,
          'metadata': bundle.metadata.to_dict(),
          'dependencies': [dep.to_dict() for dep in bundle.dependencies],
          'hard_dependencies': [dep.to_dict() for dep in hard_dependencies]
        }
        for dep in result['dependencies']: dep['parent_name'] = self.model.get_name(dep['parent_uuid'])
        for dep in result['hard_dependencies']: dep['parent_name'] = self.model.get_name(dep['parent_uuid'])
        if children is not None:
            result['children'] = [child.simple_str() for child in children]
        return result

    def get_bundle_uuid(self, worksheet_uuid, bundle_spec):
        return canonicalize.get_bundle_uuid(self.model, worksheet_uuid, bundle_spec)

    def search_bundle_uuids(self, worksheet_uuid, keywords, max_results, count):
        return self.model.get_bundle_uuids({
            '*': keywords,
            'worksheet_uuid': worksheet_uuid
        }, max_results=max_results, count=count)

    # Helper
    def get_target_path(self, target):
        return canonicalize.get_target_path(self.bundle_store, self.model, target)

    # Helper
    def get_bundle_target(self, target):
        (bundle_uuid, subpath) = target
        return (self.model.get_bundle(bundle_uuid), subpath)

    def get_worksheet_uuid(self, worksheet_spec):
        # Create default worksheet if necessary
        if worksheet_spec == Worksheet.DEFAULT_WORKSHEET_NAME:
            try:
                return canonicalize.get_worksheet_uuid(self.model, worksheet_spec)
            except UsageError:
                return self.new_worksheet(worksheet_spec)
        else:
            return canonicalize.get_worksheet_uuid(self.model, worksheet_spec)

    def validate_user_metadata(self, bundle_subclass, metadata):
        '''
        Check that the user did not supply values for any auto-generated metadata.
        Raise a UsageError with the offending keys if they are.
        '''
        #legal_keys = set(spec.key for spec in bundle_subclass.get_user_defined_metadata())
        # Allow generated keys as well 
        legal_keys = set(spec.key for spec in bundle_subclass.METADATA_SPECS)
        illegal_keys = [key for key in metadata if key not in legal_keys]
        if illegal_keys:
            raise UsageError('Illegal metadata keys: %s' % (', '.join(illegal_keys),))

    def bundle_info_to_construct_args(self, info):
        # Convert info (see bundle_model) to the actual information to construct
        # the bundle.  This is a bit ad-hoc.  Future: would be nice to have a more
        # uniform way of serializing bundle information.
        bundle_type = info['bundle_type']
        #print 'CONVERT', bundle_type, info
        if bundle_type == 'program' or bundle_type == 'dataset':
            construct_args = {'metadata': info['metadata'], 'uuid': info['uuid'],
                              'data_hash': info['data_hash']}
        elif bundle_type == 'make' or bundle_type == 'run':
            targets = { item['child_path'] : (item['parent_uuid'], item['parent_path'])
                        for item in info['dependencies'] }
            construct_args = {'targets': targets, 'command': info['command'],
                              'metadata': info['metadata'], 'uuid': info['uuid'],
                              'data_hash': info['data_hash'], 'state': info['state']}
        else:
            raise UsageError('Invalid bundle_type: %s' % bundle_type)
        return construct_args

    def upload_bundle(self, path, info, worksheet_uuid, follow_symlinks):
        bundle_type = info['bundle_type']
        if 'uuid' in info:
            existing = True
            construct_args = self.bundle_info_to_construct_args(info)
        else:
            existing = False
            construct_args = {'metadata': info['metadata']}
        metadata = construct_args['metadata']
        message = 'Invalid upload bundle_type: %s' % (bundle_type,)
        if not existing:
            precondition(bundle_type in UPLOADED_TYPES, message)
        bundle_subclass = get_bundle_subclass(bundle_type)
        if not existing:
            self.validate_user_metadata(bundle_subclass, metadata)

        # Upload the given path and record additional metadata from the upload.
        (data_hash, bundle_store_metadata) = self.bundle_store.upload(path, follow_symlinks=follow_symlinks)
        metadata.update(bundle_store_metadata)
        # TODO: check that if the data hash already exists, it's the same as before.
        construct_args['data_hash'] = data_hash

        bundle = bundle_subclass.construct(**construct_args)
        self.model.save_bundle(bundle)
        if worksheet_uuid:
            self.add_worksheet_item(worksheet_uuid, (bundle.uuid, None, worksheet_util.TYPE_BUNDLE))
        return bundle.uuid

    def derive_bundle(self, bundle_type, targets, command, metadata, worksheet_uuid):
        '''
        For both make and run bundles.
        Add the resulting bundle to the given worksheet_uuid (optional).
        '''
        bundle_subclass = get_bundle_subclass(bundle_type)
        self.validate_user_metadata(bundle_subclass, metadata)
        bundle = bundle_subclass.construct(targets=targets, command=command, metadata=metadata)
        self.model.save_bundle(bundle)
        if worksheet_uuid:
            self.add_worksheet_item(worksheet_uuid, (bundle.uuid, None, worksheet_util.TYPE_BUNDLE))
        return bundle.uuid

    def kill(self, bundle_spec):
        uuid = self.get_spec_uuid(bundle_spec)
        bundle = self.model.get_bundle(uuid)
        self.model.update_bundle(bundle, {'worker_command': Command.KILL});

    def open_target(self, target):
        (bundle_spec, subpath) = target
        path = self.get_target_path(target)
        path_util.check_isfile(path, 'open_target')
        return open(path)

    def update_bundle_metadata(self, uuid, metadata):
        bundle = self.model.get_bundle(uuid)
        self.validate_user_metadata(bundle, metadata)
        self.model.update_bundle(bundle, {'metadata': metadata})

    def delete_bundles(self, uuids, force, recursive):
        uuids = set(uuids)
        relevant_uuids = self.model.get_self_and_descendants(uuids, depth=sys.maxint)
        if not recursive:
            # If any descendants exist, then we only delete uuids if force = True.
            if (not force) and uuids != relevant_uuids:
                relevant = self.model.batch_get_bundles(uuid=(relevant_uuids - uuids))
                raise UsageError('Can\'t delete because the following bundles depend on %s:\n  %s' % (
                  uuids,
                  '\n  '.join(bundle.simple_str() for bundle in relevant),
                ))
            relevant_uuids = uuids
        self.model.delete_bundles(relevant_uuids)
        return list(relevant_uuids)

    def get_bundle_info(self, uuid, get_children=False):
        '''
        Return information about the bundle.
        get_children: whether we want to return information about the children too.
        '''
        bundle = self.model.get_bundle(uuid)
        if get_children:
            children_uuids = self.model.get_children_uuids(uuid)
            children = self.model.batch_get_bundles(uuid=children_uuids)
        else:
            children = None
        return self._bundle_to_bundle_info(bundle, children=children)

    def get_bundle_infos(self, uuids):
        # TODO: move get_children logic into this.
        bundles = self.model.batch_get_bundles(uuid=uuids)
        bundle_dict = {bundle.uuid: self._bundle_to_bundle_info(bundle) for bundle in bundles}
        return bundle_dict

    # Return information about an individual target inside the bundle.

    def get_target_info(self, target, depth):
        path = self.get_target_path(target)
        return path_util.get_info(path, depth)

    def cat_target(self, target, out):
        path = self.get_target_path(target)
        path_util.cat(path, out)

    def head_target(self, target, num_lines):
        path = self.get_target_path(target)
        return path_util.read_lines(path, num_lines)

    def open_target_handle(self, target):
        path = self.get_target_path(target)
        return open(path) if path and os.path.exists(path) else None
    def close_target_handle(self, handle):
        handle.close()

    def download_target(self, target, follow_symlinks):
        # Don't need to download anything because it's already local.
        # Note that we can't really enforce follow_symlinks, but this is okay,
        # because we will follow them when we copy it from the target path.
        return (self.get_target_path(target), None)

    def mimic(self, old_inputs, old_output, new_inputs, new_output_name, worksheet_uuid, depth, shadow):
        '''
        old_inputs: list of bundle uuids
        old_output: bundle uuid that we produced
        new_inputs: list of bundle uuids that are analogous to old_inputs
        new_output_name: name of the bundle to create to be analogous to old_output (possibly None)
        worksheet_uuid: add newly created bundles to this worksheet
        depth: how far to do a BFS up
        shadow: whether to add the new inputs right after all occurrences of the old inputs in worksheets.
        '''
        #print 'old_inputs: %s, new_inputs: %s, old_output: %s, new_output_name: %s' % (old_inputs, new_inputs, old_output, new_output_name)

        # Build the graph.
        # If old_output is given, look at ancestors of old_output until we
        # reached some depth.  If it's not given, we first get all the
        # descendants first, and then get their ancestors.
        infos = {}  # uuid -> bundle info
        if old_output:
            bundle_uuids = set([old_output])
        else:
            bundle_uuids = self.model.get_self_and_descendants(old_inputs, depth=depth)
        for _ in range(depth):
            new_bundle_uuids = set()
            for bundle_uuid in bundle_uuids:
                if bundle_uuid in infos: continue  # Already visited
                info = infos[bundle_uuid] = self.get_bundle_info(bundle_uuid)
                for dep in info['dependencies']:
                    new_bundle_uuids.add(dep['parent_uuid'])
            bundle_uuids = new_bundle_uuids

        # Now go recursively create the bundles.
        old_to_new = {}  # old_uuid -> new_uuid
        downstream = set()  # old_uuid -> whether we're downstream of an input (and actually needs to be mapped onto a new uuid)
        for old, new in zip(old_inputs, new_inputs):
            old_to_new[old] = new
            downstream.add(old)

        # Return corresponding new_bundle_uuid
        def recurse(old_bundle_uuid):
            if old_bundle_uuid in old_to_new:
                #print old_bundle_uuid, 'cached'
                return old_to_new[old_bundle_uuid]

            # Don't have any more information (because we probably hit the maximum depth)
            if old_bundle_uuid not in infos:
                #print old_bundle_uuid, 'no information'
                return old_bundle_uuid

            # Get information about the old bundle.
            info = infos[old_bundle_uuid]
            new_dependencies = [{
                'parent_uuid': recurse(dep['parent_uuid']),
                'parent_path': dep['parent_path'],
                'child_uuid': dep['child_uuid'],  # This is just a placeholder to do the equality test
                'child_path': dep['child_path']
            } for dep in info['dependencies']]

            # We're downstream, so need to make a new bundle
            if any(dep['parent_uuid'] in downstream for dep in info['dependencies']):
                # Now create a new bundle that mimics the old bundle.
                # Only change the name if the output name is supplied.
                old_bundle_name = info['metadata']['name']
                metadata = info['metadata']
                if new_output_name:
                    if old_bundle_uuid == old_output:
                        metadata['name'] = new_output_name
                    else:
                        # Just make up a name heuristically
                        metadata['name'] = new_output_name + '-' + info['metadata']['name']

                # Remove all the automatically generated keys
                cls = get_bundle_subclass(info['bundle_type'])
                for spec in cls.METADATA_SPECS:
                    if spec.generated and spec.key in metadata:
                        metadata.pop(spec.key)

                # Set the targets
                targets = {}
                for dep in new_dependencies:
                    targets[dep['child_path']] = (dep['parent_uuid'], dep['parent_path'])

                new_bundle_uuid = self.derive_bundle(info['bundle_type'], \
                    targets, info['command'], info['metadata'], worksheet_uuid if not shadow else None)
                if shadow:
                    self.model.add_shadow_worksheet_items(old_bundle_uuid, new_bundle_uuid)
                print '%s(%s) => %s(%s)' % (old_bundle_name, old_bundle_uuid, metadata['name'], new_bundle_uuid)
                downstream.add(old_bundle_uuid)
            else:
                #print '%s(%s) => same' % (info['metadata']['name'], old_bundle_uuid)
                new_bundle_uuid = old_bundle_uuid

            old_to_new[old_bundle_uuid] = new_bundle_uuid  # Cache it
            return new_bundle_uuid

        if old_output:
            return recurse(old_output)
        else:
            # Don't have a particular output we're targetting, so just create
            # new versions of all the uuids.
            for uuid in infos: recurse(uuid)

    #############################################################################
    # Implementations of worksheet-related client methods follow!
    #############################################################################

    @authentication_required
    def new_worksheet(self, name):
        worksheet = Worksheet({'name': name, 'items': [], 'owner_id': self._current_user_id()})
        self.model.save_worksheet(worksheet)
        return worksheet.uuid

    def list_worksheets(self):
        current_user = self.auth_handler.current_user()
        if current_user is None:
            return self.model.list_worksheets()
        else:
            return self.model.list_worksheets(current_user.unique_id)

    def get_worksheet_info(self, uuid):
        '''
        The returned info object contains items which are (bundle_info, value_object, type).
        '''
        worksheet = self.model.get_worksheet(uuid)
        current_user = self.auth_handler.current_user()
        current_user_id = None if current_user is None else current_user.unique_id
        check_has_read_permission(self.model, current_user_id, worksheet)

        # Create the info by starting out with the metadata.
        # The items here are (bundle_uuid, value, type).
        result = worksheet.get_info_dict()
        result['items'] = self._convert_items_from_db(result['items'])
        return result

    def _convert_items_from_db(self, items):
        '''
        (bundle_uuid, value, type) -> (bundle_info, value_obj, type)
        '''
        # We need to do some finicky stuff here to convert the bundle_uuids into
        # bundle_info dicts. However, we still make O(1) database calls because we
        # use the optimized batch_get_bundles multiget method.
        uuids = set(
            bundle_uuid for (bundle_uuid, value, type) in items
            if bundle_uuid is not None
        )
        bundles = self.model.batch_get_bundles(uuid=uuids)
        bundle_dict = {bundle.uuid: self._bundle_to_bundle_info(bundle) for bundle in bundles}

        # Go through the items and substitute the components
        new_items = []
        for (bundle_uuid, value, type) in items:
            bundle_info = bundle_dict.get(bundle_uuid, {'uuid': bundle_uuid}) if bundle_uuid else None
            value_obj = worksheet_util.string_to_tokens(value) if type == worksheet_util.TYPE_DIRECTIVE else value
            new_items.append((bundle_info, value_obj, type))
        return new_items


    @authentication_required
    def add_worksheet_item(self, worksheet_uuid, item):
        worksheet = self.model.get_worksheet(worksheet_uuid)
        check_has_full_permission(self.model, self._current_user_id(), worksheet)
        self.model.add_worksheet_item(worksheet_uuid, item)

    @authentication_required
    def update_worksheet(self, worksheet_info, new_items):
        # Convert (bundle_spec, value, type) pairs into canonical (bundle_uuid, value, type) pairs.
        # This step could take O(n) database calls! However, it will only hit the
        # database for each bundle the user has newly specified by name - bundles
        # that were already in the worksheet will be referred to by uuid, so
        # get_bundle_uuid will be an in-memory call for these. This hit is acceptable.
        worksheet_uuid = worksheet_info['uuid']
        last_item_id = worksheet_info['last_item_id']
        length = len(worksheet_info['items'])
        worksheet = self.model.get_worksheet(worksheet_uuid)
        check_has_full_permission(self.model, self._current_user_id(), worksheet)
        try:
            new_items = [worksheet_util.convert_item_to_db(item) for item in new_items]
            self.model.update_worksheet(worksheet_uuid, last_item_id, length, new_items)
        except UsageError:
            # Turn the model error into a more readable one using the object.
            raise UsageError('%s was updated concurrently!' % (worksheet,))

    @authentication_required
    def rename_worksheet(self, worksheet_spec, name):
        uuid = self.get_worksheet_uuid(worksheet_spec)
        worksheet = self.model.get_worksheet(uuid)
        check_has_full_permission(self.model, self._current_user_id(), worksheet)
        self.model.rename_worksheet(worksheet, name)

    @authentication_required
    def delete_worksheet(self, worksheet_spec):
        uuid = self.get_worksheet_uuid(worksheet_spec)
        worksheet = self.model.get_worksheet(uuid)
        check_has_full_permission(self.model, self._current_user_id(), worksheet)
        self.model.delete_worksheet(uuid)

    #############################################################################
    # Commands related to groups and permissions follow!
    #############################################################################

    @authentication_required
    def list_groups(self):
        group_dicts = self.model.batch_get_all_groups(
            None,
            {'owner_id': self._current_user_id(), 'user_defined': True},
            {'user_id': self._current_user_id()})
        for group_dict in group_dicts:
            role = 'member'
            if group_dict['is_admin'] == True:
                if group_dict['owner_id'] == group_dict['user_id']:
                    role = 'owner'
                else:
                    role = 'co-owner'
            group_dict['role'] = role
        return group_dicts

    @authentication_required
    def new_group(self, name):
        group = Group({'name': name, 'user_defined': True, 'owner_id': self._current_user_id()})
        group.validate()
        group_dict = self.model.create_group(group.to_dict())
        return group_dict

    @authentication_required
    def rm_group(self, group_spec):
        group_info = permission.unique_group_managed_by(self.model, group_spec, self._current_user_id())
        if group_info['owner_id'] != self._current_user_id():
            raise UsageError('A group cannot be deleted by its co-owners.')
        self.model.delete_group(group_info['uuid'])
        return group_info

    @authentication_required
    def group_info(self, group_spec):
        group_info = permission.unique_group_with_user(self.model, group_spec, self._current_user_id())
        users_in_group = self.model.batch_get_user_in_group(group_uuid=group_info['uuid'])
        user_ids = [group_info['owner_id']]
        user_ids.extend([u['user_id'] for u in users_in_group])
        users = self.auth_handler.get_users('ids', user_ids)
        members = []
        roles = {}
        for row in users_in_group:
            roles[row['user_id']] = 'co-owner' if row['is_admin'] == True else 'member'
        roles[group_info['owner_id']] = 'owner'
        for user_id in user_ids:
            if user_id in users:
                user = users[user_id]
                members.append({'name': user.name, 'role': roles[user_id]})
        group_info['members'] = members
        return group_info

    @authentication_required
    def add_user(self, username, group_spec, is_admin):
        group_info = permission.unique_group_managed_by(self.model, group_spec, self._current_user_id())
        users = self.auth_handler.get_users('names', [username])
        user = users[username]
        if user is None:
            raise UsageError("%s is not a valid user." % (username,))
        if user.unique_id == self._current_user_id():
            raise UsageError("You cannot add yourself to a group.")
        members = self.model.batch_get_user_in_group(user_id=user.unique_id, group_uuid=group_info['uuid'])
        if len(members) > 0:
            member = members[0]
            if user.unique_id == group_info['owner_id']:
                raise UsageError("You cannot modify the owner a group.")
            if member['is_admin'] != is_admin:
                self.model.update_user_in_group(user.unique_id, group_info['uuid'], is_admin)
                member['operation'] = 'Modified'
        else:
            member = self.model.add_user_in_group(user.unique_id, group_info['uuid'], is_admin)
            member['operation'] = 'Added'
        member['name'] = username
        return member

    @authentication_required
    def rm_user(self, username, group_spec):
        group_info = permission.unique_group_managed_by(self.model, group_spec, self._current_user_id())
        users = self.auth_handler.get_users('names', [username])
        user = users[username]
        if user is None:
            raise UsageError("%s is not a valid user." % (username,))
        if user.unique_id == group_info['owner_id']:
            raise UsageError("You cannot modify the owner a group.")
        members = self.model.batch_get_user_in_group(user_id=user.unique_id, group_uuid=group_info['uuid'])
        if len(members) > 0:
            member = members[0]
            self.model.delete_user_in_group(user.unique_id, group_info['uuid'])
            member['name'] = username
            return member
        return None

    @authentication_required
    def set_worksheet_perm(self, worksheet_spec, permission_name, group_spec):
        uuid = self.get_worksheet_uuid(worksheet_spec)
        worksheet = self.model.get_worksheet(uuid)
        check_has_full_permission(self.model, self._current_user_id(), worksheet)
        new_permission = parse_permission(permission_name)
        group_info = permission.unique_group(self.model, group_spec)
        old_permissions = self.model.get_permission(group_info['uuid'], worksheet.uuid)
        if new_permission == 0:
            if len(old_permissions) > 0:
                self.model.delete_permission(group_info['uuid'], worksheet.uuid)
        else:
            if len(old_permissions) == 1:
                self.model.update_permission(group_info['uuid'], worksheet.uuid, new_permission)
            else:
                if len(old_permissions) > 0:
                    self.model.delete_permission(group_info['uuid'], worksheet.uuid)
                self.model.add_permission(group_info['uuid'], worksheet.uuid, new_permission)
        return {'worksheet': worksheet,
                'group_info': group_info,
                'permission': new_permission}
