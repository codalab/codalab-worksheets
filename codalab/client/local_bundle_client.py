'''
LocalBundleClient is BundleClient implementation that interacts directly with a
BundleStore and a BundleModel. All filesystem operations are handled locally.
'''
from codalab.bundles import (
  get_bundle_subclass,
  UPLOADED_TYPES,
)
from codalab.common import (
  precondition,
  UsageError,
    AuthorizationError,
    PermissionError,
)
from codalab.client.bundle_client import BundleClient
from codalab.lib import (
  canonicalize,
  path_util,
  worksheet_util,
)
from codalab.objects.worksheet import Worksheet
from codalab.objects import permission
from codalab.objects.permission import Group

def authentication_required(func):
    def decorate(self, *args, **kwargs):
        if self.auth_handler.current_user() is None:
            raise AuthorizationError("Not authenticated")
        return func(self, *args, **kwargs)
    return decorate

class LocalBundleClient(BundleClient):
    def __init__(self, address, bundle_store, model, auth_handler):
        self.address = address
        self.bundle_store = bundle_store
        self.model = model
        self.auth_handler = auth_handler

    def get_bundle_info(self, bundle, parents=None, children=None):
        hard_dependencies = bundle.get_hard_dependencies()
        result = {
          'bundle_type': bundle.bundle_type,
          'data_hash': bundle.data_hash,
          'metadata': bundle.metadata.to_dict(),
          'state': bundle.state,
          'uuid': bundle.uuid,
          'hard_dependencies': [dep.to_dict() for dep in hard_dependencies]
        }
        if parents is not None:
            result['parents'] = [str(parent) for parent in parents]
        if children is not None:
            result['children'] = [str(child) for child in children]
        return result

    def get_spec_uuid(self, bundle_spec):
        return canonicalize.get_spec_uuid(self.model, bundle_spec)

    def get_target_path(self, target):
        return canonicalize.get_target_path(self.bundle_store, self.model, target)

    def get_bundle_target(self, target):
        (bundle_spec, subpath) = target
        return (self.model.get_bundle(self.get_spec_uuid(bundle_spec)), subpath)

    def get_worksheet_uuid(self, worksheet_spec):
        return canonicalize.get_worksheet_uuid(self.model, worksheet_spec)

    def expand_worksheet_item(self, item):
        (bundle_spec, value, type) = item
        if bundle_spec is None:
            return (None, value or '', type or '')
        try:
            bundle_uuid = self.get_spec_uuid(bundle_spec)
        except UsageError, e:
            return (bundle_spec, str(e) if value is None else value)
        if bundle_uuid != bundle_spec and value is None:
            # The user specified a bundle for the first time without help text.
            # Produce some auto-generated help text here.
            bundle = self.model.get_bundle(bundle_uuid)
            value = bundle_spec
            if getattr(bundle.metadata, 'description', None):
                value = '%s: %s' % (value, bundle.metadata.description)
        return (bundle_uuid, value or '', type or '')

    def validate_user_metadata(self, bundle_subclass, metadata):
        '''
        Check that the user did not supply values for any auto-generated metadata.
        Raise a UsageError with the offending keys if they are.
        '''
        legal_keys = set(spec.key for spec in
          bundle_subclass.get_user_defined_metadata())
        illegal_keys = [key for key in metadata if key not in legal_keys]
        if illegal_keys:
            raise UsageError('Illegal metadata keys: %s' % (', '.join(illegal_keys),))

    def upload(self, bundle_type, path, metadata, worksheet_uuid=None):
        message = 'Invalid upload bundle_type: %s' % (bundle_type,)
        precondition(bundle_type in UPLOADED_TYPES, message)
        bundle_subclass = get_bundle_subclass(bundle_type)
        self.validate_user_metadata(bundle_subclass, metadata)
        # Upload the given path and record additional metadata from the upload.
        (data_hash, bundle_store_metadata) = self.bundle_store.upload(path)
        metadata.update(bundle_store_metadata)
        bundle = bundle_subclass.construct(data_hash=data_hash, metadata=metadata)
        self.model.save_bundle(bundle)
        if worksheet_uuid:
            self.add_worksheet_item(worksheet_uuid, bundle.uuid)
        return bundle.uuid

    def make(self, targets, metadata, worksheet_uuid=None):
        bundle_subclass = get_bundle_subclass('make')
        self.validate_user_metadata(bundle_subclass, metadata)
        targets = {
          key: self.get_bundle_target(target)
          for (key, target) in targets.iteritems()
        }
        bundle = bundle_subclass.construct(targets, metadata)
        self.model.save_bundle(bundle)
        if worksheet_uuid:
            self.add_worksheet_item(worksheet_uuid, bundle.uuid)
        return bundle.uuid

    def run(self, targets, command, metadata, worksheet_uuid=None):
        bundle_subclass = get_bundle_subclass('run')
        self.validate_user_metadata(bundle_subclass, metadata)
        targets = {
          key: self.get_bundle_target(target)
          for (key, target) in targets.iteritems()
        }
        bundle = bundle_subclass.construct(targets, command, metadata)
        self.model.save_bundle(bundle)
        if worksheet_uuid:
            self.add_worksheet_item(worksheet_uuid, bundle.uuid)
        return bundle.uuid

    def edit(self, uuid, metadata):
        bundle = self.model.get_bundle(uuid)
        self.validate_user_metadata(bundle, metadata)
        self.model.update_bundle(bundle, {'metadata': metadata})

    def delete(self, bundle_spec, force=False):
        uuid = self.get_spec_uuid(bundle_spec)
        children = self.model.get_children(uuid)
        if children and not force:
            raise UsageError('Bundles depend on %s:\n  %s' % (
              bundle_spec,
              '\n  '.join(str(child) for child in children),
            ))
        child_worksheets = self.model.get_child_worksheets(uuid)
        if child_worksheets and not force:
            raise UsageError('Worksheets depend on %s:\n  %s' % (
              bundle_spec,
              '\n  '.join(str(child) for child in child_worksheets),
            ))
        self.model.delete_bundle_tree([uuid], force=force)

    def info(self, bundle_spec, parents=False, children=False):
        uuid = self.get_spec_uuid(bundle_spec)
        bundle = self.model.get_bundle(uuid)
        parents = self.model.get_parents(uuid) if parents else None
        children = self.model.get_children(uuid) if children else None
        return self.get_bundle_info(bundle, parents=parents, children=children)

    def ls(self, target):
        path = self.get_target_path(target)
        return path_util.ls(path)

    def cat(self, target):
        path = self.get_target_path(target)
        path_util.cat(path)

    def head(self, target, lines=10):
        path = self.get_target_path(target)
        return path_util.read_file(path, lines)

    def search(self, query=None):
        if query:
            bundles = self.model.search_bundles(**query)
        else:
            bundles = self.model.batch_get_bundles()
        return [self.get_bundle_info(bundle) for bundle in bundles]

    #############################################################################
    # Implementations of worksheet-related client methods follow!
    #############################################################################

    def new_worksheet(self, name):
        worksheet = Worksheet({'name': name, 'items': [], 'owner_id': None})
        self.model.save_worksheet(worksheet)
        return worksheet.uuid

    def worksheet_info(self, worksheet_spec):
        uuid = self.get_worksheet_uuid(worksheet_spec)
        worksheet = self.model.get_worksheet(uuid)
        result = worksheet.get_info_dict()
        # We need to do some finicky stuff here to convert the bundle_uuids into
        # bundle info dicts. However, we still make O(1) database calls because we
        # use the optimized batch_get_bundles multiget method.
        uuids = set(
            bundle_uuid for (bundle_uuid, _, _) in result['items']
          if bundle_uuid is not None
        )
        bundles = self.model.batch_get_bundles(uuid=uuids)
        bundle_dict = {bundle.uuid: self.get_bundle_info(bundle) for bundle in bundles}

        # If a bundle uuid is orphaned, we still have to return the uuid in a dict.
        items = []
        result['items'] = [
          (
               None if bundle_uuid is None else
               bundle_dict.get(bundle_uuid, {'uuid': bundle_uuid}),
                    worksheet_util.expand_worksheet_item_info(worksheet_spec, value, type),
                    type,
          )
            for (bundle_uuid, value, type) in result['items']
        ]
        return result

    def add_worksheet_item(self, worksheet_spec, bundle_spec):
        worksheet_uuid = self.get_worksheet_uuid(worksheet_spec)
        bundle_uuid = self.get_spec_uuid(bundle_spec)
        bundle = self.model.get_bundle(bundle_uuid)
        # Compute a nice value for this item, using the description if it exists.
        item_value = bundle_spec
        if getattr(bundle.metadata, 'description', None):
            item_value = '%s: %s' % (item_value, bundle.metadata.description)
        item = (bundle.uuid, item_value, 'bundle')
        self.model.add_worksheet_item(worksheet_uuid, item)

    def update_worksheet(self, worksheet_info, new_items):
        # Convert (bundle_spec, value) pairs into canonical (bundle_uuid, value, type) pairs.
        # This step could take O(n) database calls! However, it will only hit the
        # database for each bundle the user has newly specified by name - bundles
        # that were already in the worksheet will be referred to by uuid, so
        # get_spec_uuid will be an in-memory call for these. This hit is acceptable.
        canonical_items = [self.expand_worksheet_item(item) for item in new_items]
        worksheet_uuid = worksheet_info['uuid']
        last_item_id = worksheet_info['last_item_id']
        length = len(worksheet_info['items'])
        worksheet = self.model.get_worksheet(worksheet_uuid)
        try:
            self.model.update_worksheet(
              worksheet_uuid, last_item_id, length, canonical_items)
        except UsageError:
            # Turn the model error into a more readable one using the object.
            raise UsageError('%s was updated concurrently!' % (worksheet,))

    def rename_worksheet(self, worksheet_spec, name):
        uuid = self.get_worksheet_uuid(worksheet_spec)
        worksheet = self.model.get_worksheet(uuid)
        self.model.rename_worksheet(worksheet, name)

    def delete_worksheet(self, worksheet_spec):
        uuid = self.get_worksheet_uuid(worksheet_spec)
        self.model.delete_worksheet(uuid)

    #############################################################################
    # Commands related to groups and permissions follow!
    #############################################################################

    def _current_user_id(self):
        return self.auth_handler.current_user().unique_id

    @authentication_required
    def list_groups(self):
        return self.model.batch_get_all_groups(
            None, 
            {'owner_id': self._current_user_id(), 'user_defined': True},
            {'user_id': self._current_user_id() })

    @authentication_required
    def new_group(self, name):
        group = Group({'name': name, 'user_defined': True, 'owner_id': self._current_user_id()})
        self.model.create_group(group)
        return group.to_dict()

    @authentication_required
    def rm_group(self, group_spec):
        group_info = permission.unique_group_managed_by(self.model, group_spec, self._current_user_id())
        self.model.delete_group(group_info['uuid'])
        return group_info

    @authentication_required
    def group_info(self, group_spec):
        group_info = permission.unique_group_with_user(self.model, group_spec, self._current_user_id())
        users_in_group = self.model.batch_get_user_in_group(group_uuid=group_info['uuid'])
        user_ids = [int(group_info['owner_id'])]
        user_ids.extend([int(u['user_id']) for u in users_in_group])
        users = self.auth_handler.get_users('ids', user_ids)
        members = []
        roles = {}
        for row in users_in_group:
            roles[int(row['user_id'])] = 'co-owner' if row['is_admin'] == True else 'member'
        roles[group_info['owner_id']] = 'owner'
        for user_id in user_ids:
            if user_id in users:
                user = users[user_id]
                members.append({'name': user.name, 'role': roles[user_id]})
        group_info['members'] = members
        return group_info

    @authentication_required
    def add_user(self, username, group_spec, is_admin=False):
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
    def set_worksheet_perm(self, group_spec, worksheet_spec, permission):
        pass
        #TODO

    @authentication_required
    def set_bundle_perm(self, group_spec, bundle_spec, permission):
        pass

