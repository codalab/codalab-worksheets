class Metadata(object):
  def __init__(self, **kwargs):
    self._metadata_keys = set()
    for (key, value) in kwargs.iteritems():
      self.set_metadata_key(key, value)

  def validate(self, metadata_specs):
    '''
    Check that this metadata has the correct metadata keys and that it has
    metadata values of the correct types.
    '''
    expected_keys = set(spec.key for spec in metadata_specs)
    for key in self._metadata_keys:
      if key not in expected_keys:
        raise ValueError('Unexpected metadata key: %s' % (key,))
    for spec in metadata_specs:
      if spec.key not in self._metadata_keys:
        raise ValueError('Missing metadata key: %s' % (key,))
      value = getattr(self, spec.key)
      if not isinstance(value, spec.type):
        raise ValueError(
          'Metadata value for %s should be of type %s, was %s' %
          (key, spec.type, type(value))
        )

  def set_metadata_key(self, key, value):
    '''
    Set this Metadata object's key to be the given value. Record the key.
    '''
    self._metadata_keys.add(key)
    if isinstance(value, (set, list, tuple)):
      value = set(value)
    setattr(self, key, value)

  @staticmethod
  def get_type_constructor(value_type):
    '''
    Return the type constructor for each type of metadata.
    Note that basestrings cannot be instantiated, so we return unicode instead.
    '''
    return unicode if value_type == basestring else value_type 

  @classmethod
  def from_dicts(cls, metadata_specs, rows):
    '''
    Construct a Metadata object given a denormalized list of metadata dicts.
    These dicts may either be those returned by from_dicts or sqlalchemy Row objects from the metadata table.
    '''
    metadata_dict = {}
    metadata_types = {}
    for spec in metadata_specs:
      metadata_types[spec.key] = spec.type
      metadata_dict[spec.key] = cls.get_type_constructor(spec.type)()
    for row in rows:
      (maybe_unicode_key, value) = (row['metadata_key'], row['metadata_value'])
      # If the key is Unicode text (which is the case if it was extracted from a
      # database), cast it to a string. This operation encodes it with UTF-8.
      key = str(maybe_unicode_key)
      value_type = metadata_types[key]
      if value_type == set:
        metadata_dict[key].add(value)
      else:
        if metadata_dict.get(key):
          raise ValueError(
            'Got duplicate values %s and %s for key %s' %
            (metadata_dict[key], value, key)
          )
        metadata_dict[key] = cls.get_type_constructor(value_type)(value)
    return Metadata(**metadata_dict)

  def to_dicts(self, metadata_specs):
    '''
    Serialize this metadata object and return a list of dicts that can be saved
    to a MySQL table. These dicts should have the following keys:
      metadata_key
      metadata_value
    '''
    result = []
    for spec in metadata_specs:
      value = getattr(self, spec.key, self.get_type_constructor(spec.type)())
      values = value if spec.type == set else (value,)
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
      key: list(value) if isinstance(value, (list, set, tuple)) else value
      for (key, value) in items
    }
