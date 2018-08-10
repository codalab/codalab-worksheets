'''
Metadata is a wrapper around all of the metadata rows for a single bundle.
Its constructor takes both the metadata and the bundle's metadata specs,
and validates the metadata before returning.
'''
from codalab.common import UsageError
from codalab.lib import formatting


class Metadata(object):
    def __init__(self, metadata_specs, metadata_dict):
        if isinstance(metadata_dict, (list, tuple)):
            metadata_dict = self.collapse_dicts(metadata_specs, metadata_dict)
        self._metadata_keys = set()
        for (key, value) in metadata_dict.iteritems():
            self.set_metadata_key(key, value)

    def validate(self, metadata_specs):
        '''
        Check that this metadata has the correct metadata keys and that it has
        metadata values of the correct types.
        '''
        expected_keys = set(spec.key for spec in metadata_specs)
        for key in self._metadata_keys:
            if key not in expected_keys:
                raise UsageError('Unexpected metadata key: %s' % (key,))
        for spec in metadata_specs:
            if spec.key in self._metadata_keys:
                value = getattr(self, spec.key)
                if spec.type is float and isinstance(value, int):
                    # cast int to float
                    value = float(value)
                # Validate formatted string fields
                if issubclass(spec.type, basestring) and spec.formatting is not None and value:
                    try:
                        if spec.formatting == 'duration':
                            formatting.parse_duration(value)
                        elif spec.formatting == 'size':
                            formatting.parse_size(value)
                        elif spec.formatting == 'date':
                            formatting.parse_datetime(value)
                    except ValueError as e:
                        raise UsageError(e.message)
                if value is not None and not isinstance(value, spec.type):
                    raise UsageError(
                      'Metadata value for %s should be of type %s, was %s (type %s)' %
                      (spec.key, spec.type.__name__, value, type(value).__name__)
                    )
            elif not spec.generated:
                raise UsageError('Missing metadata key: %s' % (spec.key,))

    def set_metadata_key(self, key, value):
        '''
        Set this Metadata object's key to be the given value. Record the key.
        '''
        self._metadata_keys.add(key)
        setattr(self, key, value)

    @classmethod
    def collapse_dicts(cls, metadata_specs, rows):
        '''
        Convert a list of Metadata dictionaries into a normalized metadata dict.
        '''
        metadata_dict = {}
        metadata_spec_dict = {}
        for spec in metadata_specs:
            if spec.type == list or not spec.generated:
                metadata_dict[spec.key] = spec.get_constructor()()
            metadata_spec_dict[spec.key] = spec
        for row in rows:
            (maybe_unicode_key, value) = (row['metadata_key'], row['metadata_value'])
            # If the key is Unicode text (which is the case if it was extracted from a
            # database), cast it to a string. This operation encodes it with UTF-8.
            key = str(maybe_unicode_key)
            if key not in metadata_spec_dict:
                #print 'Warning: %s not in %s, skipping value %s!' % (key, metadata_spec_dict.keys(), value)
                continue  # Somewhat dangerous since we might lose information

            spec = metadata_spec_dict[key]
            if spec.type == list:
                metadata_dict[key].append(value)
            else:
                if metadata_dict.get(key):
                    raise UsageError(
                      'Got duplicate values %s and %s for key %s' %
                      (metadata_dict[key], value, key)
                    )
                # Convert string to the right type (e.g., string to int)
                metadata_dict[key] = spec.get_constructor()(value)
        return metadata_dict

    def to_dicts(self, metadata_specs):
        '''
        Serialize this metadata object and return a list of dicts that can be saved
        to a MySQL table. These dicts should have the following keys:
          metadata_key
          metadata_value
        '''
        result = []
        for spec in metadata_specs:
            if spec.key in self._metadata_keys:
                value = getattr(self, spec.key)
                if value == None: continue
                values = value if spec.type == list else (value,)
                for value in values:
                    result.append({
                      'metadata_key': unicode(spec.key),
                      'metadata_value': unicode(value),
                    })
        return result

    def to_dict(self):
        '''
        Serialize this metadata to human-readable JSON format. This format is NOT
        an appropriate one to save to a database.
        '''
        items = [(key, getattr(self, key)) for key in self._metadata_keys]
        return {
          key: value
          for (key, value) in items
        }
