class MetadataSpec(object):
  def __init__(self, key, type, description, short_key=None, default=None):
    self.key = key
    self.type = type
    self.description = description
    self.short_key = short_key or self.key[:4]
    self.default = default
