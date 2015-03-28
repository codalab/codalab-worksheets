'''
LocalBundleClient is BundleClient implementation that interacts directly with a
BundleStore and a BundleModel. All filesystem operations are handled locally.
'''
from time import sleep
import contextlib
import os, sys
import copy
import types

from codalab.bundles import (
    get_bundle_subclass,
    UPLOADED_TYPES,
)
from codalab.common import (
  precondition,
  State,
  Command,
  AuthorizationError,
  UsageError,
  PermissionError
)
from codalab.client.bundle_client import BundleClient
from codalab.lib import (
    canonicalize,
    path_util,
    file_util,
    worksheet_util,
    spec_util,
)
from codalab.objects.worksheet import Worksheet
from codalab.objects import permission
from codalab.objects.permission import (
    check_bundles_have_read_permission, # unused
    check_bundles_have_all_permission,
    check_worksheet_has_read_permission,
    check_worksheet_has_all_permission,
    parse_permission,
    permission_str,
    Group
)

from codalab.model.tables import (
    GROUP_OBJECT_PERMISSION_ALL,
    GROUP_OBJECT_PERMISSION_READ,
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

    def _current_user(self):
        return self.auth_handler.current_user()
    def _current_user_id(self):
        user = self._current_user()
        return user.unique_id if user else None
    def _current_user_name(self):
        user = self._current_user()
        return user.name if user else None

    def _bundle_to_bundle_info(self, bundle):
        '''
        Helper: Convert bundle to bundle_info.
        '''
        # See tables.py
        result = {
          'uuid': bundle.uuid,
          'bundle_type': bundle.bundle_type,
          'owner_id': bundle.owner_id,
          'command': bundle.command,
          'data_hash': bundle.data_hash,
          'state': bundle.state,
          'metadata': bundle.metadata.to_dict(),
          'dependencies': [dep.to_dict() for dep in bundle.dependencies],
        }
        for dep in result['dependencies']:
            uuid = dep['parent_uuid']
            dep['parent_name'] = self.model.get_bundle_names([uuid]).get(uuid)

        return result

    def get_bundle_uuid(self, worksheet_uuid, bundle_spec):
        return canonicalize.get_bundle_uuid(self.model, self._current_user_id(), worksheet_uuid, bundle_spec)

    def search_bundle_uuids(self, worksheet_uuid, keywords, max_results, count):
        return self.model.get_bundle_uuids({
            '*': keywords,
            'worksheet_uuid': worksheet_uuid,
            'user_id': self._current_user_id(),
        }, max_results=max_results, count=count)

    # Helper
    def get_target_path(self, target):
        return canonicalize.get_target_path(self.bundle_store, self.model, target)

    # Helper
    def get_bundle_target(self, target):
        (bundle_uuid, subpath) = target
        return (self.model.get_bundle(bundle_uuid), subpath)

    def get_worksheet_uuid(self, base_worksheet_uuid, worksheet_spec):
        if worksheet_spec == '':
            # Default worksheet name: take the username.
            worksheet_spec = self._current_user_name()
            try:
                return canonicalize.get_worksheet_uuid(self.model, base_worksheet_uuid, worksheet_spec)
            except UsageError:
                return self.new_worksheet(worksheet_spec)
        else:
            return canonicalize.get_worksheet_uuid(self.model, base_worksheet_uuid, worksheet_spec)

    def validate_user_metadata(self, bundle_subclass, metadata):
        '''
        Check that the user did not supply values for any auto-generated metadata.
        Raise a UsageError with the offending keys if they are.
        '''
        # Allow generated keys as well
        legal_keys = set(spec.key for spec in bundle_subclass.METADATA_SPECS)
        illegal_keys = [key for key in metadata if key not in legal_keys]
        if illegal_keys:
            raise UsageError('Illegal metadata keys: %s' % (', '.join(illegal_keys),))

    def bundle_info_to_construct_args(self, info):
        # Helper function.
        # Convert info (see bundle_model) to the actual information to construct
        # the bundle.  This is a bit ad-hoc.  Future: would be nice to have a more
        # uniform way of serializing bundle information.
        bundle_type = info['bundle_type']
        #print 'CONVERT', bundle_type, info
        if bundle_type == 'program' or bundle_type == 'dataset':
            construct_args = {'metadata': info['metadata'], 'uuid': info['uuid'],
                              'data_hash': info['data_hash']}
        elif bundle_type == 'make' or bundle_type == 'run':
            targets = [(item['child_path'], (item['parent_uuid'], item['parent_path']))
                        for item in info['dependencies']]
            construct_args = {'targets': targets, 'command': info['command'],
                              'metadata': info['metadata'], 'uuid': info['uuid'],
                              'data_hash': info['data_hash'], 'state': info['state']}
        else:
            raise UsageError('Invalid bundle_type: %s' % bundle_type)
        return construct_args

    @authentication_required
    # Called when |path| is a url.
    # Only used to expose uploading URLs directly to the RemoteBundleClient.
    def upload_bundle_url(self, path, info, worksheet_uuid, follow_symlinks):
        return self.upload_bundle(path, info, worksheet_uuid, follow_symlinks)

    @authentication_required
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
        if path:
            (data_hash, bundle_store_metadata) = self.bundle_store.upload(path, follow_symlinks=follow_symlinks)
            metadata.update(bundle_store_metadata)
            precondition(construct_args.get('data_hash', data_hash) == data_hash, \
                'Provided data_hash doesn\'t match: %s versus %s' % (construct_args.get('data_hash'), data_hash))
            construct_args['data_hash'] = data_hash
        # Set the owner
        construct_args['owner_id'] = self._current_user_id()
        bundle = bundle_subclass.construct(**construct_args)
        self.model.save_bundle(bundle)

        # Inherit properties of worksheet
        self._bundle_inherit_workheet_permissions(bundle.uuid, worksheet_uuid)

        if worksheet_uuid:
            self.add_worksheet_item(worksheet_uuid, worksheet_util.bundle_item(bundle.uuid))
            # TODO: don't fail if don't have permissions
        return bundle.uuid

    @authentication_required
    def derive_bundle(self, bundle_type, targets, command, metadata, worksheet_uuid):
        '''
        For both make and run bundles.
        Add the resulting bundle to the given worksheet_uuid (optional).
        '''
        bundle_subclass = get_bundle_subclass(bundle_type)
        self.validate_user_metadata(bundle_subclass, metadata)
        owner_id = self._current_user_id()
        bundle = bundle_subclass.construct(targets=targets, command=command, metadata=metadata, owner_id=owner_id)
        self.model.save_bundle(bundle)

        # Inherit properties of worksheet
        self._bundle_inherit_workheet_permissions(bundle.uuid, worksheet_uuid)

        if worksheet_uuid:
            self.add_worksheet_item(worksheet_uuid, worksheet_util.bundle_item(bundle.uuid))
            # TODO: don't fail if don't have permissions
        return bundle.uuid

    def _bundle_inherit_workheet_permissions(self, bundle_uuid, worksheet_uuid):
        group_permissions = self.model.get_group_worksheet_permissions(worksheet_uuid)
        for permissions in group_permissions:
            self.set_bundles_perm([bundle_uuid], permissions['group_uuid'], permission_str(permissions['permission']))

    @authentication_required
    def kill_bundles(self, bundle_uuids):
        '''
        Send a kill command to all the given bundles.
        '''
        check_bundles_have_all_permission(self.model, self._current_user(), bundle_uuids)
        for bundle_uuid in bundle_uuids:
            self.model.add_bundle_action(bundle_uuid, Command.KILL)

    @authentication_required
    def chown_bundles(self, bundle_uuids, user_spec):
        '''
        Set the owner of the bundles to the user.
        '''
        check_bundles_have_all_permission(self.model, self._current_user(), bundle_uuids)
        user_info = self.user_info(user_spec)
        # Update bundles
        for bundle_uuid in bundle_uuids:
            bundle = self.model.get_bundle(bundle_uuid)
            self.model.update_bundle(bundle, {'owner_id': user_info['id']})

    def open_target(self, target):
        check_bundles_have_read_permission(self.model, self._current_user(), [target[0]])
        path = self.get_target_path(target)
        path_util.check_isfile(path, 'open_target')
        return open(path)

    @authentication_required
    def update_bundle_metadata(self, uuid, metadata):
        check_bundles_have_all_permission(self.model, self._current_user(), [uuid])
        bundle = self.model.get_bundle(uuid)
        self.validate_user_metadata(bundle, metadata)
        self.model.update_bundle(bundle, {'metadata': metadata})

    @authentication_required
    def delete_bundles(self, uuids, force, recursive, data_only, dry_run):
        '''
        Delete the bundles specified by |uuids|.
        If |recursive|, add all bundles downstream too.
        If |data_only|, only remove from the bundle store, not the bundle metadata.
        '''
        relevant_uuids = self.model.get_self_and_descendants(uuids, depth=sys.maxint)
        uuids_set = set(uuids)
        relevant_uuids_set = set(relevant_uuids)
        if not recursive:
            # If any descendants exist, then we only delete uuids if force = True.
            if (not force) and uuids_set != relevant_uuids_set:
                relevant = self.model.batch_get_bundles(uuid=(set(relevant_uuids) - set(uuids)))
                raise UsageError('Can\'t delete because the following bundles depend on %s:\n  %s' % (
                  uuids,
                  '\n  '.join(bundle.simple_str() for bundle in relevant),
                ))
            relevant_uuids = uuids
        check_bundles_have_all_permission(self.model, self._current_user(), relevant_uuids)

        # Make sure that bundles are not referenced in multiple places (otherwise, it's very dangerous)
        if not force:
            result = self.model.get_host_worksheet_uuids(relevant_uuids)
            for uuid, host_worksheet_uuids in result.items():
                if len(set(host_worksheet_uuids)) > 1:
                    raise UsageError('Bundle %s appears in multiple worksheets: %s, not deleting' % (uuid, host_worksheet_uuids))

        # Get data hashes
        relevant_data_hashes = set(bundle.data_hash for bundle in self.model.batch_get_bundles(uuid=relevant_uuids) if bundle.data_hash)

        # Delete the actual bundle
        if not dry_run:
            if data_only:
                # Just remove references to the data hashes
                self.model.remove_data_hash_references(relevant_uuids)
            else:
                # Actually delete the bundle
                self.model.delete_bundles(relevant_uuids)

        # Delete the data_hash
        for data_hash in relevant_data_hashes:
            self.bundle_store.cleanup(self.model, data_hash, relevant_uuids, dry_run)

        return relevant_uuids

    def get_bundle_info(self, uuid, get_children=False, get_host_worksheets=False, get_permissions=False):
        return self.get_bundle_infos([uuid], get_children, get_host_worksheets, get_permissions).get(uuid)

    def get_bundle_infos(self, uuids, get_children=False, get_host_worksheets=False, get_permissions=False):
        '''
        get_children, get_host_worksheets, get_permissions: whether we want to return more detailed information.
        Return map from bundle uuid to info.
        '''
        if len(uuids) == 0:
            return {}
        bundles = self.model.batch_get_bundles(uuid=uuids)
        bundle_dict = {bundle.uuid: self._bundle_to_bundle_info(bundle) for bundle in bundles}

        # Filter out bundles that we don't have read permission on
        permissions = self.model.get_user_bundle_permissions(self._current_user_id(), uuids, self.model.get_bundle_owner_ids(uuids))
        for uuid, permission in permissions.items():
            if permission < GROUP_OBJECT_PERMISSION_READ:
                if uuid in bundle_dict:
                    del bundle_dict[uuid]

        # Too harsh
        #check_bundles_have_read_permission(self.model, self._current_user(), [bundle.uuid for bundle in bundles])

        # Lookup the user names of all the owners
        user_ids = [info['owner_id'] for info in bundle_dict.values()]
        users = self.auth_handler.get_users('ids', user_ids) if len(user_ids) > 0 else []
        if users:
            for info in bundle_dict.values():
                user = users[info['owner_id']]
                info['owner_name'] = user.name if user else None
                info['owner'] = '%s(%s)' % (info['owner_name'], info['owner_id'])

        if get_children:
            result = self.model.get_children_uuids(uuids)
            # Gather all children bundle uuids
            children_uuids = [uuid for l in result.values() for uuid in l]
            # Lookup bundle names
            names = self.model.get_bundle_names(children_uuids)
            # Fill in info
            for uuid, info in bundle_dict.items():
                info['children'] = [{'uuid': uuid, 'metadata': {'name': names[uuid]}} for uuid in result[uuid]]

        if get_host_worksheets:
            # bundle_uuids -> list of worksheet_uuids
            result = self.model.get_host_worksheet_uuids(uuids)
            # Gather all worksheet uuids
            worksheet_uuids = [uuid for l in result.values() for uuid in l]
            worksheets = dict((worksheet.uuid, worksheet) for worksheet in self.model.batch_get_worksheets(fetch_items=False, uuid=worksheet_uuids))
            # Fill the info
            for uuid, info in bundle_dict.items():
                info['host_worksheets'] = [{'uuid': worksheet_uuid, 'name': worksheets[worksheet_uuid].name} for worksheet_uuid in result[uuid]]

        if get_permissions:
            # Fill the info
            result = self.model.batch_get_group_bundle_permissions(uuids)
            for uuid, info in bundle_dict.items():
                info['group_permissions'] = result[uuid]

        return bundle_dict

    # Return information about an individual target inside the bundle.

    def get_target_info(self, target, depth):
        check_bundles_have_read_permission(self.model, self._current_user(), [target[0]])
        path = self.get_target_path(target)
        return path_util.get_info(path, depth)

    def cat_target(self, target, out):
        check_bundles_have_read_permission(self.model, self._current_user(), [target[0]])
        path = self.get_target_path(target)
        path_util.cat(path, out)

    def head_target(self, target, num_lines):
        check_bundles_have_read_permission(self.model, self._current_user(), [target[0]])
        path = self.get_target_path(target)
        return path_util.read_lines(path, num_lines)

    def open_target_handle(self, target):
        check_bundles_have_read_permission(self.model, self._current_user(), [target[0]])
        path = self.get_target_path(target)
        return open(path) if path and os.path.exists(path) else None
    def close_target_handle(self, handle):
        handle.close()

    def download_target(self, target, follow_symlinks):
        check_bundles_have_read_permission(self.model, self._current_user(), [target[0]])
        # Don't need to download anything because it's already local.
        # Note that we can't really enforce follow_symlinks, but this is okay,
        # because we will follow them when we copy it from the target path.
        return (self.get_target_path(target), None)

    @authentication_required
    def mimic(self, old_inputs, old_output, new_inputs, new_output_name, worksheet_uuid, depth, shadow, dry_run):
        '''
        old_inputs: list of bundle uuids
        old_output: bundle uuid that we produced
        new_inputs: list of bundle uuids that are analogous to old_inputs
        new_output_name: name of the bundle to create to be analogous to old_output (possibly None)
        worksheet_uuid: add newly created bundles to this worksheet
        depth: how far to do a BFS up from old_output.
        shadow: whether to add the new inputs right after all occurrences of the old inputs in worksheets.
        '''
        #print 'old_inputs: %s, new_inputs: %s, old_output: %s, new_output_name: %s' % (old_inputs, new_inputs, old_output, new_output_name)

        # Build the graph (get all the infos).
        # If old_output is given, look at ancestors of old_output until we
        # reached some depth.  If it's not given, we first get all the
        # descendants first, and then get their ancestors.
        infos = {}  # uuid -> bundle info
        if old_output:
            bundle_uuids = [old_output]
        else:
            bundle_uuids = self.model.get_self_and_descendants(old_inputs, depth=depth)
        all_bundle_uuids = list(bundle_uuids) # should be infos.keys() in order
        for _ in range(depth):
            new_bundle_uuids = []
            for bundle_uuid in bundle_uuids:
                if bundle_uuid in infos: continue  # Already visited
                info = infos[bundle_uuid] = self.get_bundle_info(bundle_uuid)
                for dep in info['dependencies']:
                    parent_uuid = dep['parent_uuid']
                    if parent_uuid not in infos:
                        new_bundle_uuids.append(parent_uuid)
            all_bundle_uuids = new_bundle_uuids + all_bundle_uuids
            bundle_uuids = new_bundle_uuids

        # Make sure we have read access to all the bundles involved here.
        check_bundles_have_read_permission(self.model, self._current_user(), list(infos.keys()))

        # Now go recursively create the bundles.
        old_to_new = {}  # old_uuid -> new_uuid
        downstream = set()  # old_uuid -> whether we're downstream of an input (and actually needs to be mapped onto a new uuid)
        plan = []  # sequence of (old, new) bundle infos to make
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

            # If we're downstream of any inputs, we need to make a new bundle.
            if any(dep['parent_uuid'] in downstream for dep in info['dependencies']):
                # Now create a new bundle that mimics the old bundle.
                # Only change the name if the output name is supplied.
                old_bundle_name = info['metadata']['name']
                new_info = copy.deepcopy(info)
                new_metadata = new_info['metadata']
                if new_output_name:
                    if old_bundle_uuid == old_output:
                        new_metadata['name'] = new_output_name
                    else:
                        # Just make up a name heuristically
                        new_metadata['name'] = new_output_name + '-' + info['metadata']['name']

                # Remove all the automatically generated keys
                cls = get_bundle_subclass(new_info['bundle_type'])
                for spec in cls.METADATA_SPECS:
                    if spec.generated and spec.key in new_metadata:
                        new_metadata.pop(spec.key)

                # Set the targets
                targets = [(dep['child_path'], (dep['parent_uuid'], dep['parent_path'])) for dep in new_dependencies]

                if dry_run:
                    new_bundle_uuid = None
                else:
                    new_bundle_uuid = self.derive_bundle(new_info['bundle_type'], \
                        targets, new_info['command'], new_metadata, None)

                new_info['uuid'] = new_bundle_uuid
                plan.append((info, new_info))
                downstream.add(old_bundle_uuid)
            else:
                new_bundle_uuid = old_bundle_uuid

            old_to_new[old_bundle_uuid] = new_bundle_uuid  # Cache it
            return new_bundle_uuid

        if old_output:
            recurse(old_output)
        else:
            # Don't have a particular output we're targetting, so just create
            # new versions of all the uuids.
            for uuid in all_bundle_uuids:
                recurse(uuid)

        # Add to worksheet
        if not dry_run:
            if shadow:
                # Add each new bundle in the "shadow" of the old_bundle (right after it).
                for old_bundle_uuid, new_bundle_uuid in old_to_new.items():
                    self.model.add_shadow_worksheet_items(old_bundle_uuid, new_bundle_uuid)
            else:
                # A prelude of a bundle on a worksheet is the set of items that occur right before it (markup, directives, etc.)
                # Let W be the first worksheet containing the old_inputs[0].
                # Add all items on that worksheet that appear in old_to_new along with their preludes.
                # For items not on this worksheet, add them at the end (instead of orphaning them).
                host_worksheet_uuids = self.model.get_host_worksheet_uuids([old_inputs[0]])[old_inputs[0]]
                new_bundle_uuids_added = set()
                skipped = True  # Whether there were items that we didn't include in the prelude (in which case we want to put '')
                if len(host_worksheet_uuids) > 0:
                    # Choose a single worksheet.
                    if worksheet_uuid in host_worksheet_uuids:
                        # If current worksheet is one of them, favor that one.
                        host_worksheet_uuid = worksheet_uuid
                    else:
                        # Choose an arbitrary one (in the future, have a better way of canonicalizing).
                        host_worksheet_uuid = host_worksheet_uuids[0]

                    # Fetch the worksheet
                    worksheet_info = self.get_worksheet_info(host_worksheet_uuid, fetch_items=True)

                    prelude_items = []  # The prelude that we're building up
                    for item in worksheet_info['items']:
                        (bundle_info, subworkheet_info, value_obj, item_type) = item
                        just_added = False

                        if item_type == worksheet_util.TYPE_BUNDLE:
                            old_bundle_uuid = bundle_info['uuid']
                            if old_bundle_uuid in old_to_new:
                                # Flush the prelude gathered so far.
                                new_bundle_uuid = old_to_new[old_bundle_uuid]
                                if old_bundle_uuid != new_bundle_uuid:  # Only add novel bundles
                                    # Stand in for things skipped (this is important so directives have proper extent).
                                    if skipped:
                                        self.model.add_worksheet_item(worksheet_uuid, worksheet_util.markup_item(''))

                                    # Add prelude and items
                                    for item2 in prelude_items:
                                        self.add_worksheet_item(worksheet_uuid, worksheet_util.convert_item_to_db(item2))
                                    self.add_worksheet_item(worksheet_uuid, worksheet_util.bundle_item(new_bundle_uuid))
                                    new_bundle_uuids_added.add(new_bundle_uuid)
                                    just_added = True

                        if (item_type == worksheet_util.TYPE_MARKUP and value_obj != '') or item_type == worksheet_util.TYPE_DIRECTIVE:
                            prelude_items.append(item)  # Include in prelude
                            skipped = False
                        else:
                            prelude_items = [] # Reset
                            skipped = not just_added

                # Add the bundles that haven't been added yet
                if skipped:
                    self.model.add_worksheet_item(worksheet_uuid, worksheet_util.markup_item(''))
                for info, new_info in plan:
                    new_bundle_uuid = new_info['uuid']
                    if new_bundle_uuid not in new_bundle_uuids_added:
                        self.add_worksheet_item(worksheet_uuid, worksheet_util.bundle_item(new_bundle_uuid))

        return plan

    #############################################################################
    # Implementations of worksheet-related client methods follow!
    #############################################################################

    def ensure_unused_worksheet_name(self, name):
        # Ensure worksheet names are unique.  Note: for simplicity, we are
        # ensuring uniqueness across the system, even on worksheet names that
        # the user may not have access to.
        try:
            self.get_worksheet_uuid(None, name)
            exists = True
        except UsageError, e:
            # Note: this exception could be thrown also when there's multiple
            # worksheets with the same name.  In the future, we want to rule
            # that out.
            exists = False
        if exists:
            raise UsageError('Worksheet with name %s already exists' % name)

    @authentication_required
    def new_worksheet(self, name):
        self.ensure_unused_worksheet_name(name)
        # Don't need any permissions to do this.
        worksheet = Worksheet({'name': name, 'items': [], 'owner_id': self._current_user_id()})
        self.model.save_worksheet(worksheet)

        # Make worksheet publicly readable by default
        self.set_worksheet_perm(worksheet.uuid, self.model.public_group_uuid, 'read')

        return worksheet.uuid

    def list_worksheets(self):
        # Permissions enforced by selecting only current user's.
        current_user = self._current_user()
        if current_user is None:
            results = self.model.list_worksheets()
        else:
            results = self.model.list_worksheets(current_user.unique_id)
        self._set_owner_names(results)
        return results

    def _set_owner_names(self, results):
        '''
        Helper function: Set owner_name given owner_id of each item in results.
        '''
        owner_names = self._user_id_to_names([r['owner_id'] for r in results])
        for r, owner_name in zip(results, owner_names):
            r['owner_name'] = owner_name

    def get_worksheet_info(self, uuid, fetch_items=False, fetch_permission=True):
        '''
        The returned info object contains items which are (bundle_info, subworksheet_info, value_obj, type).
        '''
        worksheet = self.model.get_worksheet(uuid, fetch_items=fetch_items)
        check_worksheet_has_read_permission(self.model, self._current_user(), worksheet)

        # Create the info by starting out with the metadata.
        result = worksheet.get_info_dict()

        result['owner_name'] = self._user_id_to_name(result['owner_id'])

        if fetch_items:
            result['items'] = self._convert_items_from_db(result['items'])

        # Note that these permissions are relative to the current user.
        # Need to make another database query.
        if fetch_permission:
            result['group_permissions'] = self.model.get_group_worksheet_permissions(worksheet.uuid)
            result['permission'] = self.model.get_user_worksheet_permissions(self._current_user_id(), [worksheet.uuid], {worksheet.uuid: worksheet.owner_id})[worksheet.uuid]

        return result

    def _user_id_to_name(self, user_id):
        return self._user_id_to_names([user_id])[0]

    def _user_id_to_names(self, user_ids):
        if len(user_ids) == 0: return []
        results = self.auth_handler.get_users('ids', user_ids)
        def get_name(r): return r.name if r else None
        return [get_name(results[user_id] if results else None) for user_id in user_ids]

    def _convert_items_from_db(self, items):
        '''
        Helper function.
        (bundle_uuid, subworksheet_uuid, value, type) -> (bundle_info, subworksheet_info, value_obj, type)
        '''
        # Database only contains the uuid; need to expand to info.
        # We need to do to convert the bundle_uuids into bundle_info dicts.
        # However, we still make O(1) database calls because we use the
        # optimized batch_get_bundles multiget method.
        bundle_uuids = set(
            bundle_uuid for (bundle_uuid, subworksheet_uuid, value, type) in items
            if bundle_uuid is not None
        )
        bundle_dict = self.get_bundle_infos(bundle_uuids)

        # Go through the items and substitute the components
        new_items = []
        for (bundle_uuid, subworksheet_uuid, value, type) in items:
            bundle_info = bundle_dict.get(bundle_uuid, {'uuid': bundle_uuid}) if bundle_uuid else None
            if subworksheet_uuid:
                try:
                    subworksheet_info = self.model.get_worksheet(subworksheet_uuid, fetch_items=False).to_dict()
                except UsageError, e:
                    # If can't get the subworksheet, it's probably invalid, so just replace it with an error
                    type = worksheet_util.TYPE_MARKUP
                    subworksheet_info = None
                    value = 'ERROR: non-existent worksheet %s' % subworksheet_uuid
            else:
                subworksheet_info = None
            value_obj = worksheet_util.string_to_tokens(value) if type == worksheet_util.TYPE_DIRECTIVE else value
            new_items.append((bundle_info, subworksheet_info, value_obj, type))
        return new_items

    @authentication_required
    def add_worksheet_item(self, worksheet_uuid, item):
        '''
        Add the given item to the worksheet.
        '''
        worksheet = self.model.get_worksheet(worksheet_uuid, fetch_items=False)
        check_worksheet_has_all_permission(self.model, self._current_user(), worksheet)
        self.model.add_worksheet_item(worksheet_uuid, item)

    @authentication_required
    def update_worksheet(self, worksheet_info, new_items):
        '''
        Set the worksheet to have items |new_items|.
        '''
        worksheet_uuid = worksheet_info['uuid']
        last_item_id = worksheet_info['last_item_id']
        length = len(worksheet_info['items'])
        worksheet = self.model.get_worksheet(worksheet_uuid, fetch_items=False)
        check_worksheet_has_all_permission(self.model, self._current_user(), worksheet)
        try:
            new_items = [worksheet_util.convert_item_to_db(item) for item in new_items]
            self.model.update_worksheet(worksheet_uuid, last_item_id, length, new_items)
        except UsageError:
            # Turn the model error into a more readable one using the object.
            raise UsageError('%s was updated concurrently!' % (worksheet,))

    @authentication_required
    def rename_worksheet(self, uuid, name):
        worksheet = self.model.get_worksheet(uuid, fetch_items=False)
        check_worksheet_has_all_permission(self.model, self._current_user(), worksheet)
        self.ensure_unused_worksheet_name(name)
        self.model.rename_worksheet(worksheet, name)

    @authentication_required
    def chown_worksheet(self, uuid, owner_spec):
        '''
        Change the owner of the given worksheet |uuid| to |owner|.
        '''
        worksheet = self.model.get_worksheet(uuid, fetch_items=False)
        check_worksheet_has_all_permission(self.model, self._current_user(), worksheet)
        owner_id = self.user_info(owner_spec)['id']
        self.model.chown_worksheet(worksheet, owner_id)

    @authentication_required
    def delete_worksheet(self, uuid):
        worksheet = self.model.get_worksheet(uuid, fetch_items=True)
        check_worksheet_has_all_permission(self.model, self._current_user(), worksheet)
        # Be safe!
        if len(worksheet.items) > 0:
            raise UsageError("Can\'t delete worksheet %s because it is not empty" % worksheet.uuid)
        self.model.delete_worksheet(uuid)

    def interpret_file_genpaths(self, requests):
        '''
        Helper function.
        requests: list of (bundle_uuid, genpath, post-processing-func)
        Return responses: corresponding list of strings
        '''
        target_cache = {}
        responses = []
        for (bundle_uuid, genpath, post) in requests:
            value = worksheet_util.interpret_file_genpath(self, target_cache, bundle_uuid, genpath, post)
            #print 'interpret_file_genpaths', bundle_uuid, genpath, value
            responses.append(value)
        return responses

    def resolve_interpreted_items(self, interpreted_items):
        """
        Called by the web interface.  Takes a list of interpreted worksheet
        items (returned by worksheet_util.interpret_items) and fetches the
        appropriate information, replacing the 'interpreted' field in each item.
        The result can be serialized via JSON.
        """
        is_last_newline = False
        for item in interpreted_items:
            mode = item['mode']
            data = item['interpreted']
            properties = item['properties']
            is_newline = (data == '')
            # if's in order of most frequent
            if mode == 'markup':
                # no need to do anything
                pass
            elif mode == 'record' or mode == 'table':
                # header_name_posts is a list of (name, post-processing) pairs.
                header, contents = data
                # Request information
                contents = worksheet_util.interpret_genpath_table_contents(self, contents)
                data = (header, contents)
            elif mode == 'inline':
                if not (is_newline and is_last_newline):
                    if isinstance(data, tuple) or isinstance(data, type):
                        data = self.interpret_file_genpaths([data])[0]
            elif mode == 'contents':
                info = self.get_target_info(data, 1)
                if 'type' not in info:
                    pass
                elif info['type'] == 'file':
                    data = self.head_target(data, int(properties.get('maxlines', 10)))
            elif mode == 'html':
                data = self.head_target(data, None)
            elif mode == 'image':
                path = self.get_target_path(data)
                data = path_util.base64_encode(path)
            elif mode == 'search':
                search_interpreted = worksheet_util.interpret_search(client, worksheet_info['uuid'], data)
                data = search_interpreted
            elif mode == 'worksheet':
                pass
            else:
                raise UsageError('Invalid display mode: %s' % mode)
            # Assign the interpreted from the processed data
            item['interpreted'] = data

            # We need to get check if this is a run and we can get the stdout and stderr
            if 'bundle_info' in item:  # making sure this is a bundle, not markdown or something else
                for info in item['bundle_info']:
                    if isinstance(info, dict) and info.get('bundle_type', None) == 'run':
                        target = (info['uuid'], '')
                        target_info = self.get_target_info(target, 2)
                        target_info['stdout'] = None
                        target_info['stderr'] = None
                        # if we have std out or err update it.
                        contents = target_info.get('contents')
                        if contents:
                            for item in contents:
                                if item['name'] in ['stdout', 'stderr']:
                                    lines = self.head_target((info['uuid'], item['name']), 100)
                                    if lines:
                                        lines = ' '.join(lines)
                                        info[item['name']] = lines

            is_last_newline = is_newline

        return interpreted_items

    #############################################################################
    # Commands related to groups and permissions follow!
    #############################################################################

    @authentication_required
    def list_groups(self):
        # Only list groups that we're part of.
        if self._current_user_id() == self.model.root_user_id:
            group_dicts = self.model.batch_get_all_groups(None, {'user_defined': True}, None)
        else:
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
                    role = 'admin'
            group_dict['role'] = role
        self._set_owner_names(group_dicts)
        return group_dicts

    @authentication_required
    def new_group(self, name):
        group = Group({'name': name, 'user_defined': True, 'owner_id': self._current_user_id()})
        group.validate()
        group_dict = self.model.create_group(group.to_dict())
        self.model.add_user_in_group(self._current_user_id(), group_dict['uuid'], True)
        return group_dict

    @authentication_required
    def rm_group(self, group_spec):
        group_info = self._get_group_info(group_spec, need_admin=True)
        self.model.delete_group(group_info['uuid'])
        return group_info

    @authentication_required
    def user_info(self, user_spec):
        '''
        Return {'name': ..., 'id': ...}
        '''
        if user_spec == None:
            user = self.auth_handler.current_user()
        elif spec_util.ID_REGEX.match(user_spec):
            user = self.auth_handler.get_users('ids', [user_spec])[user_spec]
        else:
            user = self.auth_handler.get_users('names', [user_spec])[user_spec]
        if user:
            return {'id': user.unique_id, 'name': user.name}
        raise UsageError('Invalid user specification: %s' % user_spec)

    @authentication_required
    def group_info(self, group_spec):
        '''
        Return information about the given group.
        In particular, we get all its members.
        '''
        group_info = self._get_group_info(group_spec, need_admin=False)

        # Get all the members
        users_in_group = self.model.batch_get_user_in_group(group_uuid=group_info['uuid'])
        user_ids = [u['user_id'] for u in users_in_group]
        users = self.auth_handler.get_users('ids', user_ids) if len(user_ids) > 0 else []
        members = []
        roles = {}
        for row in users_in_group:
            roles[row['user_id']] = 'admin' if row['is_admin'] == True else 'member'
        roles[group_info['owner_id']] = 'owner'
        for user_id in user_ids:
            if user_id in users:
                user = users[user_id]
                members.append({'user_id': user_id, 'user_name': user.name if user else None, 'role': roles[user_id]})
        group_info['members'] = members
        return group_info

    @authentication_required
    def add_user(self, user_spec, group_spec, is_admin):
        '''
        Add the given |user_spec| to the |group_spec| with |is_admin| privileges.
        Return information about the operation performed.
        '''
        # Lookup group and user
        group_info = self._get_group_info(group_spec, need_admin=True)
        user_info = self.user_info(user_spec)

        # Look to see what the user's current status is in the group.
        members = self.model.batch_get_user_in_group(user_id=user_info['id'], group_uuid=group_info['uuid'])
        if len(members) > 0:
            member = members[0]
            self.model.update_user_in_group(user_info['id'], group_info['uuid'], is_admin)
            member['operation'] = 'Modified'
        else:
            member = self.model.add_user_in_group(user_info['id'], group_info['uuid'], is_admin)
            member['operation'] = 'Added'
        member['name'] = user_info['name']
        return member

    @authentication_required
    def rm_user(self, user_spec, group_spec):
        '''
        Remove given |user_spec| from the given |group_spec|.
        '''
        # Lookup group and user
        group_info = self._get_group_info(group_spec, need_admin=True)
        user_info = self.user_info(user_spec)

        # Look to see what the user's current status is in the group.
        members = self.model.batch_get_user_in_group(user_id=user_info['id'], group_uuid=group_info['uuid'])
        if len(members) > 0:
            member = members[0]
            self.model.delete_user_in_group(user_info['id'], group_info['uuid'])
            member['name'] = user_info['name']
            return member
        return None

    @authentication_required
    def set_bundles_perm(self, bundle_uuids, group_spec, permission_spec):
        '''
        Give the given |group_spec| the desired |permission_spec| on |bundle_uuids|.
        '''
        check_bundles_have_all_permission(self.model, self._current_user(), bundle_uuids)
        group_info = self._get_group_info(group_spec, need_admin=False)

        for bundle_uuid in bundle_uuids:
            old_permission = self.model.get_group_bundle_permission(group_info['uuid'], bundle_uuid)
            new_permission = parse_permission(permission_spec)
            if new_permission > 0:
                if old_permission > 0:
                    self.model.update_bundle_permission(group_info['uuid'], bundle_uuid, new_permission)
                else:
                    self.model.add_bundle_permission(group_info['uuid'], bundle_uuid, new_permission)
            else:
                if old_permission > 0:
                    self.model.delete_bundle_permission(group_info['uuid'], bundle_uuid)
        return {'group_info': group_info, 'permission': new_permission}

    @authentication_required
    def set_worksheet_perm(self, worksheet_uuid, group_spec, permission_spec):
        '''
        Give the given |group_spec| the desired |permission_spec| on |worksheet_uuid|.
        '''
        worksheet = self.model.get_worksheet(worksheet_uuid, fetch_items=False)
        check_worksheet_has_all_permission(self.model, self._current_user(), worksheet)
        group_info = self._get_group_info(group_spec, need_admin=False)
        old_permission = self.model.get_group_worksheet_permission(group_info['uuid'], worksheet.uuid)
        new_permission = parse_permission(permission_spec)

        if new_permission > 0:
            if old_permission > 0:
                self.model.update_worksheet_permission(group_info['uuid'], worksheet.uuid, new_permission)
            else:
                self.model.add_worksheet_permission(group_info['uuid'], worksheet.uuid, new_permission)
        else:
            if old_permission > 0:
                self.model.delete_worksheet_permission(group_info['uuid'], worksheet.uuid)
        return {'worksheet': {'uuid': worksheet.uuid, 'name': worksheet.name},
                'group_info': group_info,
                'permission': new_permission}

    def _get_group_info(self, group_spec, need_admin):
        '''
        Resolve |group_spec| and return the associated group_info.
        '''
        user_id = self._current_user_id()

        # If we're root, then we can access any group.
        if user_id == self.model.root_user_id:
            user_id = None

        group_info = permission.unique_group(self.model, group_spec, user_id)

        # If not root and need admin access, but don't have it, raise error.
        if user_id and need_admin and not group_info['is_admin']:
            raise UsageError('You are not the admin of group %s.' % group_spec)

        # No one can admin the public group (not even root), because it's a special group.
        if need_admin and group_info['uuid'] == self.model.public_group_uuid:
            raise UsageError('Cannot modify the public group %s.' % group_spec)

        return group_info
