'''
Worksheet is the ORM class for a worksheet in the bundle system. This class is
backed by two tables in the database: worksheet and worksheet_item.

A single worksheet will have many item rows, ordered by the id the database
assigned to each one. Each of this rows will have a string value and and
optionally a bundle_uuid which makes it a bundle rows.
'''
from codalab.common import precondition
from codalab.lib import spec_util
from codalab.model.orm_object import ORMObject


class Worksheet(ORMObject):
  COLUMNS = ('uuid', 'name')

  def validate(self):
    '''
    Check a number of basic conditions that would indicate serious errors if
    they do not hold. Right now, validation only checks this worksheet's uuid,
    its name, and all of its items' bundle uuids.
    '''
    spec_util.check_uuid(self.uuid)
    spec_util.check_name(self.name)
    for item in self.items:
      if item.bundle_uuid is not None:
        spec_util.check_uuid(item.bundle_uuid)

  def __repr__(self):
    return 'Worksheet(uuid=%r, name=%r)' % (self.uuid, self.name)

  def update_in_memory(self, row, strict=False):
    items = row.pop('items', None)
    if strict:
      precondition(items is not None, 'No metadata: %s' % (row,))
      item_ids = [item['id'] for item in items]
      message = 'Worksheet items were not sorted: %s' % (items,)
      precondition(item_ids == sorted(set(item_ids)), message)
      if 'uuid' not in row:
        row['uuid'] = self.generate_uuid()
    super(Worksheet, self).update_in_memory(row)
    if items is not None:
      self.items = [(item['bundle_uuid'], item['value']) for item in items]
      self.last_item_id = items[-1]['id'] if items else -1
  
  def to_info_dict(self):
    return {
      'uuid': self.uuid,
      'name': self.name,
      'items': self.items,
      'last_item_id': self.last_item_id,
    }
