class BundleClient(object):
  def upload(self, path, metadata):
    '''
    Create a new bundle with a copy of the directory at the given path in the
    local filesystem. Return its id. If the path leads to a file, the new bundle
    will only contain only that file.
    '''
    # TODO(skishore): This function should handle symlinks in the directory
    # being uploaded, either by realizing them or erroring out if they exist.
    raise NotImplementedError

  def update(self, bundle_id, metadata):
    '''
    Update a bundle's metadata with the given data. Overwrite old metadata.
    '''
    # TODO(skishore): We need a way to update bundle contents.
    # Do we need to have a version for each bundles that is only null for
    # derived bundles but not for uploaded bundles?
    raise NotImplementedError

  def download(self, bundle_id, path):
    '''
    Download the contents of the given bundle to the given local path.
    '''
    # TODO(skishore): What are we going to do about dependencies here?
    # We should probably realize them. This isn't too bad, because run bundles
    # will not include their dependencies in their final value.
    raise NotImplementedError

  def make(self, targets):
    '''
    Create a new bundle with dependencies on the given targets. Return its id.
    targets should be a dict mapping target keys to (bundle_id, path) pairs.
    Each of the targets will by symlinked into the new bundle at its key.
    '''
    # TODO(skishore): How will metadata be inferred for this bundle type?
    # TODO(skishore): Figure out if this method will call run or vice-versa.
    raise NotImplementedError

  def run(self, program_bundle_id, targets, command):
    '''
    Run the given program bundle, create bundle of output, and return its id.
    The input bundles (the targets) are symlinked in as dependencies.
    '''
    # TODO(skishore): After evaluating a run, we should only save files in the
    # output directory. We should drop the program and input directories.
    raise NotImplementedError

  def info(self, bundle_id):
    '''
    Return a dict containing detailed information about a given bundle:
      location: its physical location on the filesystem
      metadata: its metadata object
      status: a description of the bundle's status
    '''
    raise NotImplementedError

  def list(self, target):
    '''
    Return a directory listing of the target, which is a (bundle_id, path) pair.
    This listing should include the same information as ls -la.
    '''
    # TODO(skishore): Need to decide on an output format for this method.
    raise NotImplementedError

  def search(self, query):
    '''
    Run a search on bundle metadata and return the ids of all bundles that
    are returned by the query.
    '''
    raise NotImplementedError

  def wait(self, bundle_id):
    '''
    Block on the execution of the given bundle. Return SUCCESS or FAILED
    based on whether it was computed successfully.
    '''
    # This method can just be implemented by repeatedly calling info().
    raise NotImplementedError
