'''
NameBundle is an abstract Bundle supertype that all other bundle types subclass.
It requires name, description, and tags metadata for all of its subclasses.
TODO: merge into bundle.py.
'''
import time
from typing import List

from codalab.common import UsageError
from codalab.lib import spec_util
from codalab.objects.bundle import Bundle
from codalab.objects.metadata_spec import MetadataSpec


class NamedBundle(Bundle):
    NAME_LENGTH = 32
    METADATA_SPECS = [
        MetadataSpec(
            'name',
            str,
            'Short name (not necessarily unique), which must start with a letter or underscore and can only contain letters, digits, underscores, periods, and dashes (name).',
            short_key='n',
        ),
        MetadataSpec(
            'description', str, 'Full description of the bundle (description).', short_key='d',
        ),
        MetadataSpec(
            'tags',
            list,
            'Space-separated list of tags used for search, e.g. machine-learning (tags).',
            metavar='TAG',
        ),
        MetadataSpec(
            'created',
            int,
            'Time when this bundle was created (created).',
            generated=True,
            formatting='date',
        ),
        MetadataSpec(
            'data_size',
            int,
            'Size of this bundle in bytes (data_size).',
            generated=True,
            formatting='size',
        ),
        MetadataSpec(
            'failure_message',
            str,
            'Error message if this run bundle failed (failure_message).',
            generated=True,
        ),
        MetadataSpec(
            'error_traceback',
            str,
            'Error traceback if this run bundle failed (error_traceback).',
            generated=True,
            hidden=True,
        ),
    ]  # type: List

    @classmethod
    def construct(cls, row):
        # The base NamedBundle construct method takes a bundle row and adds in
        # automatically generated metadata values.
        row['metadata'] = dict(row['metadata'], created=int(time.time()))
        return cls(row)

    def validate(self):
        super(NamedBundle, self).validate()
        bundle_type = self.bundle_type.title()
        if not self.metadata.name:
            raise UsageError('%ss must have non-empty names' % (bundle_type,))
        spec_util.check_name(self.metadata.name)

    def __repr__(self):
        return '%s(uuid=%r, name=%r)' % (
            self.__class__.__name__,
            str(self.uuid),
            str(self.metadata.name),
        )

    def simple_str(self):
        return self.metadata.name + '(' + self.uuid + ')'
