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
    check_has_read_permission,
    check_has_all_permission,
    check_has_read_permission_on_bundles,
    check_has_all_permission_on_bundles,
    Group,
    parse_permission
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

    def _bundle_to_bundle_info(self, bundle, children=None):
        '''
        Helper: Convert bundle to bundle_info.
        '''
        # See tables.py
        result = {
          'uuid': bundle.uuid,
          'bundle_type': bundle.bundle_type,
          'owner_id': bundle.owner_id,
          'owner_name': self.user_info(bundle.owner_id)['name'],
          'command': bundle.command,
          'data_hash': bundle.data_hash,
          'state': bundle.state,
          'metadata': bundle.metadata.to_dict(),
          'dependencies': [dep.to_dict() for dep in bundle.dependencies],
        }
        result['owner'] = '%s(%s)' % (result['owner_name'], result['owner_id'])

        for dep in result['dependencies']: dep['parent_name'] = self.model.get_name(dep['parent_uuid'])
        if children is not None:
            result['children'] = [child.simple_str() for child in children]
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
            targets = { item['child_path'] : (item['parent_uuid'], item['parent_path'])
                        for item in info['dependencies'] }
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
        (data_hash, bundle_store_metadata) = self.bundle_store.upload(path, follow_symlinks=follow_symlinks)
        metadata.update(bundle_store_metadata)
        # TODO: check that if the data hash already exists, it's the same as before.
        construct_args['data_hash'] = data_hash
        # Set the owner
        construct_args['owner_id'] = self._current_user_id()
        bundle = bundle_subclass.construct(**construct_args)
        self.model.save_bundle(bundle)
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
        if worksheet_uuid:
            self.add_worksheet_item(worksheet_uuid, worksheet_util.bundle_item(bundle.uuid))
            # TODO: don't fail if don't have permissions
        return bundle.uuid

    @authentication_required
    def kill_bundles(self, bundle_uuids):
        check_has_all_permission_on_bundles(self.model, self._current_user(), bundle_uuids)
        for bundle_uuid in bundle_uuids:
            self.model.add_bundle_action(bundle_uuid, Command.KILL)

    def open_target(self, target):
        check_has_read_permission_on_bundles(self.model, self._current_user(), [target[0]])
        path = self.get_target_path(target)
        path_util.check_isfile(path, 'open_target')
        return open(path)

    @authentication_required
    def update_bundle_metadata(self, uuid, metadata):
        check_has_all_permission_on_bundles(self.model, self._current_user(), [uuid])
        bundle = self.model.get_bundle(uuid)
        self.validate_user_metadata(bundle, metadata)
        self.model.update_bundle(bundle, {'metadata': metadata})

    @authentication_required
    def delete_bundles(self, uuids, force, recursive, dry_run):
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
        check_has_all_permission_on_bundles(self.model, self._current_user(), relevant_uuids)
        if not dry_run:
            self.model.delete_bundles(relevant_uuids)
        return list(relevant_uuids)

    def get_bundle_info(self, uuid, get_children=False):
        '''
        Return information about the bundle.
        get_children: whether we want to return information about the children too.
        '''
        check_has_read_permission_on_bundles(self.model, self._current_user(), [uuid])
        bundle = self.model.get_bundle(uuid)
        if get_children:
            # TODO: make sure we have access to children.
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
        check_has_read_permission_on_bundles(self.model, self._current_user(), [target[0]])
        path = self.get_target_path(target)
        return path_util.get_info(path, depth)

    def cat_target(self, target, out):
        check_has_read_permission_on_bundles(self.model, self._current_user(), [target[0]])
        path = self.get_target_path(target)
        path_util.cat(path, out)

    def head_target(self, target, num_lines):
        check_has_read_permission_on_bundles(self.model, self._current_user(), [target[0]])
        path = self.get_target_path(target)
        return path_util.read_lines(path, num_lines)

    def open_target_handle(self, target):
        check_has_read_permission_on_bundles(self.model, self._current_user(), [target[0]])
        path = self.get_target_path(target)
        return open(path) if path and os.path.exists(path) else None
    def close_target_handle(self, handle):
        handle.close()

    def download_target(self, target, follow_symlinks):
        check_has_read_permission_on_bundles(self.model, self._current_user(), [target[0]])
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

        # Make sure we have read access to all the bundles involved here.
        # TODO: need to double check that this is right.
        check_has_read_permission_on_bundles(self.model, self._current_user(), list(infos.keys()))

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

            # We're downstream, so need to make a new bundle
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
                targets = {}
                for dep in new_dependencies:
                    targets[dep['child_path']] = (dep['parent_uuid'], dep['parent_path'])

                if dry_run:
                    new_bundle_uuid = None
                else:
                    new_bundle_uuid = self.derive_bundle(new_info['bundle_type'], \
                        targets, new_info['command'], new_metadata, worksheet_uuid if not shadow else None)
                new_info['uuid'] = new_bundle_uuid
                if shadow:
                    self.model.add_shadow_worksheet_items(old_bundle_uuid, new_bundle_uuid)

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
            for uuid in infos: recurse(uuid)
        return plan

    #############################################################################
    # Implementations of worksheet-related client methods follow!
    #############################################################################

    @authentication_required
    def new_worksheet(self, name):
        # Don't need any permissions to do this.
        worksheet = Worksheet({'name': name, 'items': [], 'owner_id': self._current_user_id()})
        self.model.save_worksheet(worksheet)
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
        check_has_read_permission(self.model, self._current_user(), worksheet)

        # Create the info by starting out with the metadata.
        result = worksheet.get_info_dict()

        result['owner_name'] = self._user_id_to_name(result['owner_id'])

        if fetch_items:
            result['items'] = self._convert_items_from_db(result['items'])

        # Note that these permissions are relative to the current user.
        # Need to make another database query.
        if fetch_permission:
            result['group_permissions'] = self.model.get_group_permissions(worksheet.uuid)
            result['permission'] = self.model.get_user_permission(self._current_user_id(), worksheet.uuid, worksheet.owner_id)

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
        bundles = self.model.batch_get_bundles(uuid=bundle_uuids)
        bundle_dict = {bundle.uuid: self._bundle_to_bundle_info(bundle) for bundle in bundles}

        # Go through the items and substitute the components
        new_items = []
        for (bundle_uuid, subworksheet_uuid, value, type) in items:
            bundle_info = bundle_dict.get(bundle_uuid, {'uuid': bundle_uuid}) if bundle_uuid else None
            if subworksheet_uuid:
                subworksheet_info = self.model.get_worksheet(subworksheet_uuid, fetch_items=False).to_dict()
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
        check_has_all_permission(self.model, self._current_user(), worksheet)
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
        check_has_all_permission(self.model, self._current_user(), worksheet)
        try:
            new_items = [worksheet_util.convert_item_to_db(item) for item in new_items]
            self.model.update_worksheet(worksheet_uuid, last_item_id, length, new_items)
        except UsageError:
            # Turn the model error into a more readable one using the object.
            raise UsageError('%s was updated concurrently!' % (worksheet,))

    @authentication_required
    def rename_worksheet(self, uuid, name):
        worksheet = self.model.get_worksheet(uuid, fetch_items=False)
        check_has_all_permission(self.model, self._current_user(), worksheet)
        self.model.rename_worksheet(worksheet, name)

    @authentication_required
    def chown_worksheet(self, uuid, owner_spec):
        '''
        Change the owner of the given worksheet |uuid| to |owner|.
        '''
        worksheet = self.model.get_worksheet(uuid, fetch_items=False)
        check_has_all_permission(self.model, self._current_user(), worksheet)
        owner_id = self.user_info(owner_spec)['id']
        self.model.chown_worksheet(worksheet, owner_id)

    @authentication_required
    def delete_worksheet(self, uuid):
        worksheet = self.model.get_worksheet(uuid, fetch_items=False)
        check_has_all_permission(self.model, self._current_user(), worksheet)
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
            value = worksheet_util.interpret_file_genpath(self, target_cache, bundle_uuid, genpath)
            #print 'interpret_file_genpaths', bundle_uuid, genpath, value
            value = worksheet_util.apply_func(post, value)
            responses.append(value)
        return responses

    def resolve_interpreted_items(self, interpreted_items):
        """
        Helper function.
        Takes a list of interpreted worksheet items loops through them and depending
        on the type will find genpath for bundle info being requested.

        Returns as a full interpeted_items lists which can be easialy json or rpc
        """
        is_last_newline = False
        for item in interpreted_items:
            mode = item['mode']
            data = item['interpreted']
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
                    data = self.head_target(data, 10)
            elif mode == 'html':
                data = self.head_target(data, None)
            elif mode == 'image':
                path = self.get_target_path(data)
                encoded = path_util.base64_encode(path)
                data = encoded
            elif mode == 'search':
                search_interpreted = worksheet_util.interpret_search(client, worksheet_info['uuid'], data)
                data = search_interpreted
            elif mode == 'worksheet':
                #placeholder
                pass
            else:
                raise UsageError('Invalid display mode: %s' % mode)

            item['interpreted'] = data
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
    def set_worksheet_perm(self, worksheet_uuid, group_spec, permission_spec):
        '''
        Give the given |group_spec| the desired |permission_spec| on |worksheet_uuid|.
        '''
        worksheet = self.model.get_worksheet(worksheet_uuid, fetch_items=False)
        check_has_all_permission(self.model, self._current_user(), worksheet)
        group_info = self._get_group_info(group_spec, need_admin=False)
        old_permission = self.model.get_group_permission(group_info['uuid'], worksheet.uuid)
        new_permission = parse_permission(permission_spec)

        if new_permission > 0:
            if old_permission > 0:
                self.model.update_permission(group_info['uuid'], worksheet.uuid, new_permission)
            else:
                self.model.add_permission(group_info['uuid'], worksheet.uuid, new_permission)
        else:
            if old_permission > 0:
                self.model.delete_permission(group_info['uuid'], worksheet.uuid)
        return {'worksheet': {'uuid': worksheet.uuid, 'name': worksheet.name},
                'group_info': group_info,
                'permission': new_permission}

    @authentication_required
    def chown(self, bundle_uuids, user_spec):
        '''
        Set the owner of the bundles to the user.
        '''
        check_has_all_permission_on_bundles(self.model, self._current_user(), bundle_uuids)
        user_info = self.user_info(user_spec)
        # Update bundles
        for bundle_uuid in bundle_uuids:
            bundle = self.model.get_bundle(bundle_uuid)
            self.model.update_bundle(bundle, {'owner_id': user_info['id']})

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
