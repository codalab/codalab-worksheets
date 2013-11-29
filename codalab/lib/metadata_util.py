def request_missing_metadata(args, metadata_types):
  return {key: getattr(args, key) for key in metadata_types}
