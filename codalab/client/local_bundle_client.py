"""
LocalBundleClient is BundleClient implementation that interacts directly with a
BundleStore and a BundleModel. All filesystem operations are handled locally.
"""
import base64
import copy
import datetime
import os
import re
import sys

from codalab.bundles import (
    get_bundle_subclass,
    PrivateBundle,
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
    worksheet_util,
    file_util,
    spec_util,
    formatting,
)
from codalab.objects.worksheet import Worksheet
from codalab.objects import permission
from codalab.objects.permission import (
    check_bundles_have_read_permission,
    check_bundles_have_all_permission,
    check_worksheet_has_read_permission,
    check_worksheet_has_all_permission,
    parse_permission,
    permission_str,
    Group
)

from codalab.model.tables import (
    GROUP_OBJECT_PERMISSION_READ,
)


def authentication_required(func):
    """
    Functions decorated by authentication_required will fail fast with an AuthorizationError if the request
    it not authenticated (i.e. the auth_handler doesn't have a current user set).
    """
    def decorated(self, *args, **kwargs):
        if self.auth_handler.current_user() is None:
            raise AuthorizationError("Not authenticated")
        return func(self, *args, **kwargs)

    return decorated


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
        """
        Helper: Convert bundle to bundle_info.
        """
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

        # Shim in args
        result['args'] = worksheet_util.interpret_genpath(result, 'args')

        return result

    @staticmethod
    def _mask_bundle_info(bundle_info):
        """
        Return a copy of the bundle_info dict that returns '<hidden>'
        for all fields except 'uuid'.
        """
        private_str = '<private>'
        return {
            'uuid': bundle_info['uuid'],
            'bundle_type': PrivateBundle.BUNDLE_TYPE,
            'owner_id': None,
            'command': None,
            'data_hash': None,
            'state': None,
            'metadata': {
                'name': private_str
            },
            'dependencies': [],
        }

    def get_bundle_uuids(self, worksheet_uuid, bundle_specs):
        return [self._get_bundle_uuid(worksheet_uuid, bundle_spec) for bundle_spec in bundle_specs]

    def _get_bundle_uuid(self, worksheet_uuid, bundle_spec):
        if '/' in bundle_spec:  # <worksheet_spec>/<bundle_spec>
            # Shift to new worksheet
            worksheet_spec, bundle_spec = bundle_spec.split('/', 1)
            worksheet_uuid = self.get_worksheet_uuid(worksheet_uuid, worksheet_spec)

        return canonicalize.get_bundle_uuid(self.model, self._current_user_id(), worksheet_uuid, bundle_spec)

    def resolve_owner_in_keywords(self, keywords):
        # Resolve references to owner ids
        def resolve(keyword):
            # Example: owner=codalab => owner_id=0
            m = re.match('owner=(.+)', keyword)
            if not m:
                return keyword
            return 'owner_id=%s' % self._user_name_to_id(m.group(1))
        return map(resolve, keywords)

    def search_bundle_uuids(self, worksheet_uuid, keywords):
        keywords = self.resolve_owner_in_keywords(keywords)
        return self.model.search_bundle_uuids(self._current_user_id(), worksheet_uuid, keywords)

    # Helper
    def get_target_path(self, target):
        check_bundles_have_read_permission(self.model, self._current_user(), [target[0]])
        path = canonicalize.get_target_path(self.bundle_store, self.model, target)
        if os.path.islink(path):
            raise UsageError('Following symlink disallowed: %s/%s' % (target[0], target[1]))
        if not os.path.exists(path):
            # Too stringent, maybe hasn't been created yet.
            #raise UsageError('Target does not exist: %s/%s' % (target[0], target[1]))
            return None
        return path

    # Helper
    def get_bundle_target(self, target):
        (bundle_uuid, subpath) = target
        return (self.model.get_bundle(bundle_uuid), subpath)

    def get_worksheet_uuid(self, base_worksheet_uuid, worksheet_spec):
        if worksheet_spec == '' or worksheet_spec == worksheet_util.HOME_WORKSHEET:
            # Default worksheet name: take the username.
            worksheet_spec = spec_util.home_worksheet(self._current_user_name())
            return self.new_worksheet(worksheet_spec, True)
        else:
            return canonicalize.get_worksheet_uuid(self.model, base_worksheet_uuid, worksheet_spec)

    @staticmethod
    def validate_user_metadata(bundle_subclass, metadata):
        """
        Check that the user did not supply values for any auto-generated metadata.
        Raise a UsageError with the offending keys if they are.
        """
        # Allow generated keys as well
        legal_keys = set(spec.key for spec in bundle_subclass.METADATA_SPECS)
        illegal_keys = [key for key in metadata if key not in legal_keys]
        if illegal_keys:
            raise UsageError('Illegal metadata keys: %s' % (', '.join(illegal_keys),))

    @staticmethod
    def bundle_info_to_construct_args(info):
        """
        Helper function.
        Convert info (see bundle_model) to the actual information to construct
        the bundle.  This is a bit ad-hoc.  Future: would be nice to have a more
        uniform way of serializing bundle information.
        """
        bundle_type = info['bundle_type']
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
    def upload_bundle_url(self, sources, follow_symlinks, exclude_patterns, git, unpack, remove_sources, info, worksheet_uuid, add_to_worksheet):
        """
        Called when |sources| is a URL.  Only used to expose uploading URLs
        directly to the RemoteBundleClient.
        """
        return self.upload_bundle(sources, follow_symlinks, exclude_patterns, git, unpack, remove_sources, info, worksheet_uuid, add_to_worksheet)

    @authentication_required
    def upload_bundle(self, sources, follow_symlinks, exclude_patterns, git, unpack, remove_sources, info, worksheet_uuid, add_to_worksheet):
        """
        |sources|, |follow_symlinks|, |exclude_patterns|, |git|, |unpack|, |remove_sources|: see BundleStore.upload()
        |info|: information about the bundle.
        |worksheet_uuid|: which worksheet to inherit permissions on
        |add_to_worksheet|: whether to add to this worksheet or not.
        Note: |sources| could be None (e.g., if we are copying a bundle where we've only kept the metadata).
        """
        worksheet = self.model.get_worksheet(worksheet_uuid, fetch_items=False)
        check_worksheet_has_all_permission(self.model, self._current_user(), worksheet)
        self._check_worksheet_not_frozen(worksheet)

        # Construct initial metadata
        bundle_type = info['bundle_type']
        if 'uuid' in info:  # Happens when we're copying bundles.
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

        # Upload the source and record additional metadata from the upload.
        if sources is not None:
            (data_hash, bundle_store_metadata) = self.bundle_store.upload(sources=sources,
                                                                          follow_symlinks=follow_symlinks,
                                                                          exclude_patterns=exclude_patterns,
                                                                          git=git,
                                                                          unpack=unpack,
                                                                          remove_sources=remove_sources)
            metadata.update(bundle_store_metadata)
        else:
            data_hash = None
        if construct_args.get('data_hash', data_hash) != data_hash:
            print >>sys.stderr, 'ERROR: provided data_hash doesn\'t match: %s versus %s' %\
                                (construct_args.get('data_hash'), data_hash)
        construct_args['data_hash'] = data_hash

        # Set the owner
        construct_args['owner_id'] = self._current_user_id()
        bundle = bundle_subclass.construct(**construct_args)
        self.model.save_bundle(bundle)

        # Inherit properties of worksheet
        self._bundle_inherit_workheet_permissions(bundle.uuid, worksheet_uuid)

        # Add to worksheet
        if add_to_worksheet:
            self.add_worksheet_item(worksheet_uuid, worksheet_util.bundle_item(bundle.uuid))

        return bundle.uuid

    @authentication_required
    def derive_bundle(self, bundle_type, targets, command, metadata, worksheet_uuid):
        """
        For both make and run bundles.
        Add the resulting bundle to the given worksheet_uuid.
        """
        worksheet = self.model.get_worksheet(worksheet_uuid, fetch_items=False)
        check_worksheet_has_all_permission(self.model, self._current_user(), worksheet)
        self._check_worksheet_not_frozen(worksheet)
        bundle_uuid = self._derive_bundle(bundle_type, targets, command, metadata, worksheet_uuid)

        # Add to worksheet
        self.add_worksheet_item(worksheet_uuid, worksheet_util.bundle_item(bundle_uuid))
        return bundle_uuid

    def _derive_bundle(self, bundle_type, targets, command, metadata, worksheet_uuid):
        """
        Helper function that creates the bundle but doesn't add it to the worksheet.
        Returns the uuid.
        """
        bundle_subclass = get_bundle_subclass(bundle_type)
        self.validate_user_metadata(bundle_subclass, metadata)
        owner_id = self._current_user_id()
        bundle = bundle_subclass.construct(targets=targets, command=command, metadata=metadata, owner_id=owner_id)
        self.model.save_bundle(bundle)
        # Inherit properties of worksheet
        self._bundle_inherit_workheet_permissions(bundle.uuid, worksheet_uuid)
        return bundle.uuid

    def _bundle_inherit_workheet_permissions(self, bundle_uuid, worksheet_uuid):
        group_permissions = self.model.get_group_worksheet_permissions(self._current_user_id(), worksheet_uuid)
        for permissions in group_permissions:
            self.set_bundles_perm([bundle_uuid], permissions['group_uuid'], permission_str(permissions['permission']))

    @authentication_required
    def kill_bundles(self, bundle_uuids):
        """
        Send a kill command to all the given bundles.
        """
        check_bundles_have_all_permission(self.model, self._current_user(), bundle_uuids)
        for bundle_uuid in bundle_uuids:
            self.model.add_bundle_action(bundle_uuid, Command.KILL)

    @authentication_required
    def chown_bundles(self, bundle_uuids, user_spec):
        """
        Set the owner of the bundles to the user.
        """
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
        """
        Delete the bundles specified by |uuids|.
        If |force|, allow deletion of bundles that have descendants or that appear across multiple worksheets.
        If |recursive|, add all bundles downstream too.
        If |data_only|, only remove from the bundle store, not the bundle metadata.
        """
        relevant_uuids = self.model.get_self_and_descendants(uuids, depth=sys.maxint)
        uuids_set = set(uuids)
        relevant_uuids_set = set(relevant_uuids)
        if not recursive:
            # If any descendants exist, then we only delete uuids if force = True.
            if (not force) and uuids_set != relevant_uuids_set:
                relevant = self.model.batch_get_bundles(uuid=(set(relevant_uuids) - set(uuids)))
                raise UsageError('Can\'t delete bundles %s because the following bundles depend on them:\n  %s' % (
                  ' '.join(uuids),
                  '\n  '.join(bundle.simple_str() for bundle in relevant),
                ))
            relevant_uuids = uuids
        check_bundles_have_all_permission(self.model, self._current_user(), relevant_uuids)

        # Make sure we don't delete bundles which are active.
        if not force:
            states = self.model.get_bundle_states(uuids)
            active_uuids = [uuid for (uuid, state) in states.items() if state in [State.QUEUED, State.RUNNING]]
            if len(active_uuids) > 0:
                raise UsageError('Can\'t delete queued or running bundles (--force to override): %s' %
                                 ' '.join(active_uuids))

        # Make sure that bundles are not referenced in multiple places (otherwise, it's very dangerous)
        result = self.model.get_host_worksheet_uuids(relevant_uuids)
        for uuid, host_worksheet_uuids in result.items():
            worksheets = self.model.batch_get_worksheets(fetch_items=False, uuid=host_worksheet_uuids)
            frozen_worksheets = [worksheet for worksheet in worksheets if worksheet.frozen]
            if len(frozen_worksheets) > 0:
                raise UsageError("Can't delete bundle %s because it appears in frozen worksheets "
                                 "(need to delete worksheet first):\n  %s" %
                                 (uuid, '\n  '.join(worksheet.simple_str() for worksheet in frozen_worksheets)))
            if not force and len(host_worksheet_uuids) > 1:
                raise UsageError("Can't delete bundle %s because it appears in multiple worksheets "
                                 "(--force to override):\n  %s" %
                                 (uuid, '\n  '.join(worksheet.simple_str() for worksheet in worksheets)))

        # Get data hashes
        relevant_data_hashes = set(bundle.data_hash
                                   for bundle in self.model.batch_get_bundles(uuid=relevant_uuids)
                                   if bundle.data_hash)

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
        """
        get_children, get_host_worksheets, get_permissions: whether we want to return more detailed information.
        Return map from bundle uuid to info.
        """
        if len(uuids) == 0:
            return {}
        bundles = self.model.batch_get_bundles(uuid=uuids)
        bundle_dict = {bundle.uuid: self._bundle_to_bundle_info(bundle) for bundle in bundles}

        # Filter out bundles that we don't have read permission on
        def select_unreadable_bundles(uuids):
            permissions = self.model.get_user_bundle_permissions(self._current_user_id(), uuids, self.model.get_bundle_owner_ids(uuids))
            return [uuid for uuid, permission in permissions.items() if permission < GROUP_OBJECT_PERMISSION_READ]

        def select_unreadable_worksheets(uuids):
            permissions = self.model.get_user_worksheet_permissions(self._current_user_id(), uuids, self.model.get_worksheet_owner_ids(uuids))
            return [uuid for uuid, permission in permissions.items() if permission < GROUP_OBJECT_PERMISSION_READ]

        # Mask bundles that we can't access
        for uuid in select_unreadable_bundles(uuids):
            if uuid in bundle_dict:
                bundle_dict[uuid] = self._mask_bundle_info(bundle_dict[uuid])

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
            unreadable = set(select_unreadable_bundles(children_uuids))
            children_uuids = [uuid for uuid in children_uuids if uuid not in unreadable]
            # Lookup bundle names
            names = self.model.get_bundle_names(children_uuids)
            # Fill in info
            for uuid, info in bundle_dict.items():
                info['children'] = [{'uuid': child_uuid, 'metadata': {'name': names[child_uuid]}} \
                    for child_uuid in result[uuid] if child_uuid not in unreadable]

        if get_host_worksheets:
            # bundle_uuids -> list of worksheet_uuids
            result = self.model.get_host_worksheet_uuids(uuids)
            # Gather all worksheet uuids
            worksheet_uuids = [uuid for l in result.values() for uuid in l]
            unreadable = set(select_unreadable_worksheets(worksheet_uuids))
            worksheet_uuids = [uuid for uuid in worksheet_uuids if uuid not in unreadable]
            # Lookup names
            worksheets = dict((worksheet.uuid, worksheet) for worksheet in self.model.batch_get_worksheets(fetch_items=False, uuid=worksheet_uuids))
            # Fill the info
            for uuid, info in bundle_dict.items():
                info['host_worksheets'] = [{'uuid': worksheet_uuid, 'name': worksheets[worksheet_uuid].name} \
                    for worksheet_uuid in result[uuid] if worksheet_uuid not in unreadable]

        if get_permissions:
            # Fill the info
            group_result = self.model.batch_get_group_bundle_permissions(self._current_user_id(), uuids)
            result = self.model.get_user_bundle_permissions(self._current_user_id(), uuids, self.model.get_bundle_owner_ids(uuids))
            for uuid, info in bundle_dict.items():
                info['group_permissions'] = group_result[uuid]
                info['permission'] = result[uuid]

        return bundle_dict

    # Return information about an individual target inside the bundle.

    def get_target_info(self, target, depth):
        check_bundles_have_read_permission(self.model, self._current_user(), [target[0]])
        path = self.get_target_path(target)
        if path is None:
            return None
        return path_util.get_info(path, depth)

    def cat_target(self, target, out):
        check_bundles_have_read_permission(self.model, self._current_user(), [target[0]])
        path = self.get_target_path(target)
        if path is None:
            return
        path_util.cat(path, out)

    # Maximum number of bytes to read per line requested
    MAX_BYTES_PER_LINE = 128

    def head_target(self, target, max_num_lines, replace_non_unicode=False):
        max_total_bytes = None if max_num_lines is None else max_num_lines * self.MAX_BYTES_PER_LINE
        check_bundles_have_read_permission(self.model, self._current_user(), [target[0]])
        path = self.get_target_path(target)
        lines = path_util.read_lines(path, max_num_lines, max_total_bytes)
        if lines is None:
            return None

        if replace_non_unicode:
            lines = map(formatting.verbose_contents_str, lines)

        return map(base64.b64encode, lines)

    def open_target_handle(self, target):
        check_bundles_have_read_permission(self.model, self._current_user(), [target[0]])
        path = self.get_target_path(target)
        return open(path) if path and os.path.exists(path) else None

    @staticmethod
    def close_target_handle(handle):
        handle.close()

    def download_target(self, target, final_path):
        check_bundles_have_read_permission(self.model, self._current_user(), [target[0]])
        # Don't need to download anything because it's already local.
        # Note that we can't really enforce follow_symlinks, but this is okay,
        # because we will follow them when we copy it from the target path.
        source_path = self.get_target_path(target)
        path_util.copy(source_path, final_path)

    @authentication_required
    def mimic(self, old_inputs, old_output, new_inputs, new_output_name, worksheet_uuid, depth, shadow, dry_run):
        """
        old_inputs: list of bundle uuids
        old_output: bundle uuid that we produced
        new_inputs: list of bundle uuids that are analogous to old_inputs
        new_output_name: name of the bundle to create to be analogous to old_output (possibly None)
        worksheet_uuid: add newly created bundles to this worksheet
        depth: how far to do a BFS up from old_output.
        shadow: whether to add the new inputs right after all occurrences of the old inputs in worksheets.
        """
        worksheet = self.model.get_worksheet(worksheet_uuid, fetch_items=False)
        check_worksheet_has_all_permission(self.model, self._current_user(), worksheet)
        self._check_worksheet_not_frozen(worksheet)

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
        created_uuids = set()  # set of uuids which were newly created
        plan = []  # sequence of (old, new) bundle infos to make
        for old, new in zip(old_inputs, new_inputs):
            old_to_new[old] = new
            downstream.add(old)

        # Return corresponding new_bundle_uuid
        def recurse(old_bundle_uuid):
            if old_bundle_uuid in old_to_new:
                return old_to_new[old_bundle_uuid]

            # Don't have any more information (because we probably hit the maximum depth)
            if old_bundle_uuid not in infos:
                return old_bundle_uuid

            # Get information about the old bundle.
            info = infos[old_bundle_uuid]
            new_dependencies = [{
                'parent_uuid': recurse(dep['parent_uuid']),
                'parent_path': dep['parent_path'],
                'child_uuid': dep['child_uuid'],  # This is just a placeholder to do the equality test
                'child_path': dep['child_path']
            } for dep in info['dependencies']]

            # If there are no inputs or if we're downstream of any inputs, we need to make a new bundle.
            lone_output = (len(old_inputs) == 0 and old_bundle_uuid == old_output)
            downstream_of_inputs = any(dep['parent_uuid'] in downstream for dep in info['dependencies'])
            if lone_output or downstream_of_inputs:
                # Now create a new bundle that mimics the old bundle.
                # Only change the name if the output name is supplied.
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
                    if new_info['bundle_type'] not in ('make', 'run'):
                        raise UsageError('Can\'t mimic %s since it is not make or run' % old_bundle_uuid)
                    new_bundle_uuid = self._derive_bundle(new_info['bundle_type'], \
                        targets, new_info['command'], new_metadata, worksheet_uuid)

                new_info['uuid'] = new_bundle_uuid
                plan.append((info, new_info))
                downstream.add(old_bundle_uuid)
                created_uuids.add(new_bundle_uuid)
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
                    if new_bundle_uuid in created_uuids:  # Only add novel bundles
                        self.model.add_shadow_worksheet_items(old_bundle_uuid, new_bundle_uuid)
            else:
                def newline():
                    self.model.add_worksheet_item(worksheet_uuid, worksheet_util.markup_item(''))
                # A prelude of a bundle on a worksheet is the set of items that occur right before it (markup,
                # directives, etc.)
                # Let W be the first worksheet containing the old_inputs[0].
                # Add all items on that worksheet that appear in old_to_new along with their preludes.
                # For items not on this worksheet, add them at the end (instead of making them floating).
                if old_output:
                    anchor_uuid = old_output
                elif len(old_inputs) > 0:
                    anchor_uuid = old_inputs[0]
                host_worksheet_uuids = self.model.get_host_worksheet_uuids([anchor_uuid])[anchor_uuid]
                new_bundle_uuids_added = set()

                # Whether there were items that we didn't include in the prelude (in which case we want to put '')
                skipped = True

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
                                if new_bundle_uuid in created_uuids:  # Only add novel bundles
                                    # Stand in for things skipped (this is important so directives have proper extent).
                                    if skipped:
                                        newline()

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
                for info, new_info in plan:
                    new_bundle_uuid = new_info['uuid']
                    if new_bundle_uuid not in new_bundle_uuids_added:
                        if skipped:
                            newline()
                            skipped = False
                        self.add_worksheet_item(worksheet_uuid, worksheet_util.bundle_item(new_bundle_uuid))

        return plan

    #############################################################################
    # Implementations of worksheet-related client methods follow!
    #############################################################################

    def ensure_unused_group_name(self, name):
        # Ensure group names are unique.  Note: for simplicity, we are
        # ensuring uniqueness across the system, even on group names that
        # the user may not have access to.
        groups = self.model.batch_get_groups(name=name)
        if len(groups) != 0:
            raise UsageError('Group with name %s already exists' % name)

    def ensure_unused_worksheet_name(self, name):
        # Ensure worksheet names are unique.  Note: for simplicity, we are
        # ensuring uniqueness across the system, even on worksheet names that
        # the user may not have access to.
        
        # If trying to set the name to a home worksheet, then it better be
        # user's home worksheet.
        username = self._current_user_name()
        if spec_util.is_home_worksheet(name) and spec_util.home_worksheet(username) != name:
            raise UsageError('Cannot create %s because this is potentially the home worksheet of another user' % name)

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
    def new_worksheet(self, name, ensure_exists):
        """
        Create a new worksheet with the given |name|.
        """
        # If |ensure_exists| = True, then quit if worksheet already exists.
        if ensure_exists:
            try:
                return self.get_worksheet_uuid(None, name)
            except UsageError:
                pass

        self.ensure_unused_worksheet_name(name)

        # Don't need any permissions to do this.
        worksheet = Worksheet({
            'name': name,
            'title': None,
            'frozen': None,
            'items': [],
            'owner_id': self._current_user_id()
        })
        self.model.new_worksheet(worksheet)

        # Make worksheet publicly readable by default
        self.set_worksheet_perm(worksheet.uuid, self.model.public_group_uuid, 'read')

        return worksheet.uuid

    def list_worksheets(self):
        return self.search_worksheets([])

    def search_worksheets(self, keywords):
        keywords = self.resolve_owner_in_keywords(keywords)
        results = self.model.search_worksheets(self._current_user_id(), keywords)
        self._set_owner_names(results)
        return results

    def _set_owner_names(self, results):
        """
        Helper function: Set owner_name given owner_id of each item in results.
        """
        owner_names = self._user_id_to_names([r['owner_id'] for r in results])
        for r, owner_name in zip(results, owner_names):
            r['owner_name'] = owner_name

    def get_worksheet_info(self, uuid, fetch_items=False, fetch_permission=True):
        """
        The returned info object contains items which are (bundle_info, subworksheet_info, value_obj, type).
        """
        worksheet = self.model.get_worksheet(uuid, fetch_items=fetch_items)
        check_worksheet_has_read_permission(self.model, self._current_user(), worksheet)

        # Create the info by starting out with the metadata.
        result = worksheet.to_dict()

        result['owner_name'] = self._user_id_to_name(result['owner_id'])

        if fetch_items:
            result['items'] = self._convert_items_from_db(result['items'])

        # Note that these group_permissions is universal and permissions are relative to the current user.
        # Need to make another database query.
        if fetch_permission:
            result['group_permissions'] = self.model.get_group_worksheet_permissions(
                self._current_user_id(), worksheet.uuid)
            result['permission'] = self.model.get_user_worksheet_permissions(
                self._current_user_id(), [worksheet.uuid], {worksheet.uuid: worksheet.owner_id}
            )[worksheet.uuid]

        return result

    def _user_id_to_name(self, user_id):
        return self._user_id_to_names([user_id])[0]

    def _user_name_to_id(self, user_name):
        results = self.auth_handler.get_users('names', [user_name])
        if not results[user_name]:
            raise UsageError('Unknown user: %s' % user_name)
        return results[user_name].unique_id

    def _user_id_to_names(self, user_ids):
        if len(user_ids) == 0:
            return []

        results = self.auth_handler.get_users('ids', user_ids)

        def get_name(r):
            return r.name if r else None

        return [get_name(results[user_id] if results else None) for user_id in user_ids]

    def _convert_items_from_db(self, items):
        """
        Helper function.
        (bundle_uuid, subworksheet_uuid, value, type) -> (bundle_info, subworksheet_info, value_obj, type)
        """
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
                    # type = worksheet_util.TYPE_MARKUP
                    subworksheet_info = {'uuid': subworksheet_uuid}
                    # value = 'ERROR: non-existent worksheet %s' % subworksheet_uuid
            else:
                subworksheet_info = None
            value_obj = formatting.string_to_tokens(value) if type == worksheet_util.TYPE_DIRECTIVE else value
            new_items.append((bundle_info, subworksheet_info, value_obj, type))
        return new_items

    @authentication_required
    def add_worksheet_item(self, worksheet_uuid, item):
        """
        Add the given item to the worksheet.
        """
        worksheet = self.model.get_worksheet(worksheet_uuid, fetch_items=False)
        check_worksheet_has_all_permission(self.model, self._current_user(), worksheet)
        self._check_worksheet_not_frozen(worksheet)
        self.model.add_worksheet_item(worksheet_uuid, item)

    @authentication_required
    def update_worksheet_items(self, worksheet_info, new_items):
        """
        Set the worksheet to have items |new_items|.
        """
        worksheet_uuid = worksheet_info['uuid']
        last_item_id = worksheet_info['last_item_id']
        length = len(worksheet_info['items'])
        worksheet = self.model.get_worksheet(worksheet_uuid, fetch_items=False)
        check_worksheet_has_all_permission(self.model, self._current_user(), worksheet)
        self._check_worksheet_not_frozen(worksheet)
        try:
            new_items = [worksheet_util.convert_item_to_db(item) for item in new_items]
            self.model.update_worksheet_items(worksheet_uuid, last_item_id, length, new_items)
        except UsageError:
            # Turn the model error into a more readable one using the object.
            raise UsageError('%s was updated concurrently!' % (worksheet,))

    @authentication_required
    def update_worksheet_metadata(self, uuid, info):
        """
        Change the metadata of the worksheet |uuid| to |info|,
        where |info| specifies name, title, owner, etc.
        """
        worksheet = self.model.get_worksheet(uuid, fetch_items=False)
        check_worksheet_has_all_permission(self.model, self._current_user(), worksheet)
        metadata = {}
        for key, value in info.items():
            if key == 'owner_spec':
                metadata['owner_id'] = self.user_info(value)['id']
            elif key == 'name':
                self.ensure_unused_worksheet_name(value)
                metadata[key] = value
            elif key == 'title':
                metadata[key] = value
            elif key == 'tags':
                metadata[key] = value
            elif key == 'freeze':
                metadata['frozen'] = datetime.datetime.now()
            else:
                raise UsageError('Unknown key: %s' % key)
        self.model.update_worksheet_metadata(worksheet, metadata)

    @authentication_required
    def delete_worksheet(self, uuid, force):
        worksheet = self.model.get_worksheet(uuid, fetch_items=True)
        check_worksheet_has_all_permission(self.model, self._current_user(), worksheet)
        if not force:
            if worksheet.frozen:
                raise UsageError("Can't delete worksheet %s because it is frozen (--force to override)." %
                                 worksheet.uuid)
            if len(worksheet.items) > 0:
                raise UsageError("Can't delete worksheet %s because it is not empty (--force to override)." %
                                 worksheet.uuid)
        self.model.delete_worksheet(uuid)

    def interpret_file_genpaths(self, requests):
        """
        Helper function.
        requests: list of (bundle_uuid, genpath, post-processing-func)
        Return responses: corresponding list of strings
        """
        target_cache = {}
        responses = []
        for (bundle_uuid, genpath, post) in requests:
            value = worksheet_util.interpret_file_genpath(self, target_cache, bundle_uuid, genpath, post)
            responses.append(value)
        return responses

    DEFAULT_MAX_CONTENT_LINES = 10

    def resolve_interpreted_items(self, interpreted_items):
        """
        Called by the web interface.  Takes a list of interpreted worksheet
        items (returned by worksheet_util.interpret_items) and fetches the
        appropriate information, replacing the 'interpreted' field in each item.
        The result can be serialized via JSON.
        """
        for item in interpreted_items:
            mode = item['mode']
            data = item['interpreted']
            properties = item['properties']
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
            elif mode == 'contents':
                info = self.get_target_info(data, 1)
                if 'type' not in info:
                    data = None
                elif info['type'] == 'file':
                    data = self.head_target(
                        data,
                        int(properties.get('maxlines', self.DEFAULT_MAX_CONTENT_LINES)),
                        replace_non_unicode=True)
            elif mode == 'html':
                data = self.head_target(data, None)
            elif mode == 'image':
                path = self.get_target_path(data)
                data = path_util.base64_encode(path)
            elif mode == 'search':
                data = worksheet_util.interpret_search(self, None, data)
            elif mode == 'wsearch':
                data = worksheet_util.interpret_wsearch(self, data)
            elif mode == 'worksheet':
                pass
            else:
                raise UsageError('Invalid display mode: %s' % mode)
            # Assign the interpreted from the processed data
            item['interpreted'] = data

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
            if group_dict['is_admin']:
                if group_dict['owner_id'] == group_dict['user_id']:
                    role = 'owner'
                else:
                    role = 'admin'
            group_dict['role'] = role
        self._set_owner_names(group_dicts)
        return group_dicts

    @authentication_required
    def new_group(self, name):
        self.ensure_unused_group_name(name)
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
        """
        Return {'name': ..., 'id': ...}
        """
        if user_spec is None:
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
        """
        Return information about the given group.
        In particular, we get all its members.
        """
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
        """
        Add the given |user_spec| to the |group_spec| with |is_admin| privileges.
        Return information about the operation performed.
        """
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
        """
        Remove given |user_spec| from the given |group_spec|.
        """
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
        """
        Give the given |group_spec| the desired |permission_spec| on |bundle_uuids|.
        """
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
        """
        Give the given |group_spec| the desired |permission_spec| on |worksheet_uuid|.
        """
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
        """
        Resolve |group_spec| and return the associated group_info.
        """
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

    def get_events_log_info(self, query_info, offset, limit):
        return self.model.get_events_log_info(query_info, offset, limit)

    @staticmethod
    def _check_worksheet_not_frozen(worksheet):
        if worksheet.frozen:
            raise PermissionError('Cannot mutate frozen worksheet %s(%s).' % (worksheet.uuid, worksheet.name))
