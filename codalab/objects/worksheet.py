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


# We will keep worksheet items sorted in the database by maintining a sort_key
# for each item that was batch-added to a worksheet by a call to update_worksheet.
# These sort keys will be strictly upper-bounded by the maximum id at the time
# at which the edit was BEGUN. This ensures that any worksheet items appended to
# the sheet between the time the edit was begun and committed will have ids
# greater than the maximum sort key.
def item_sort_key(item):
    return item['id'] if item['sort_key'] is None else item['sort_key']


class Worksheet(ORMObject):
    COLUMNS = ('uuid', 'name', 'owner_id')

    def validate(self):
        '''
        Check a number of basic conditions that would indicate serious errors if
        they do not hold. Right now, validation only checks this worksheet's uuid
        and its name.
        '''
        spec_util.check_uuid(self.uuid)
        spec_util.check_name(self.name)
        spec_util.check_id(self.owner_id)

    def __repr__(self):
        return 'Worksheet(uuid=%r, name=%r)' % (self.uuid, self.name)

    def update_in_memory(self, row, strict=False):
        items = row.pop('items', None)
        if strict:
            precondition(items is not None, 'No items: %s' % (row,))
            item_sort_keys = [item_sort_key(item) for item in items]
            message = 'Worksheet items were not distinct and sorted: %s' % (items,)
            precondition(item_sort_keys == sorted(set(item_sort_keys)), message)
            if 'uuid' not in row:
                row['uuid'] = spec_util.generate_uuid()
        super(Worksheet, self).update_in_memory(row)
        if items is not None:
            self.items = [(item['bundle_uuid'], item['value'], item['type']) for item in items]
            self.last_item_id = max(item['id'] for item in items) if items else -1

    def get_info_dict(self):
        return {
          'uuid': self.uuid,
          'name': self.name,
          'items': self.items,
          'last_item_id': self.last_item_id,
        }
