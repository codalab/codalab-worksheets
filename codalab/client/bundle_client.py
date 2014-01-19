'''
Abstract base class that describes the client interface for interacting with
the CodaLab bundle system.

There are three categories of BundleClient commands:
  - Commands that create and edit bundles: upload, make, run and update.
  - Commands for browsing bundles: info, ls, cat, grep, and search.
  - Various utility commands for pulling bundles back out of the system.

There are a couple of implementations of this class:
  - LocalBundleClient - interacts directly with a BundleStore and BundleModel.
  - RemoteBundleClient - shells out to a BundleRPCServer to implement its API.
'''
import time

from codalab.common import State


class BundleClient(object):
  # Commands for creating/editing bundles: upload, make, run, edit, and delete.

  def upload(self, bundle_type, path, metadata):
    '''
    Create a new bundle with a copy of the directory at the given path in the
    local filesystem. Return its uuid. If the path leads to a file, the new
    bundle will only contain only that file.
    '''
    raise NotImplementedError

  def make(self, targets, metadata):
    '''
    Create a new bundle with dependencies on the given targets. Return its uuid.
    targets should be a dict mapping target keys to (bundle_spec, path) pairs.
    Each of the targets will by symlinked into the new bundle at its key.
    '''
    raise NotImplementedError

  def run(self, program_target, input_target, command, metadata):
    '''
    Run the given program bundle, create bundle of output, and return its uuid.
    The program and input targets are (bundle_spec, path) pairs that are
    symlinked in as dependencies during runtime.
    '''
    raise NotImplementedError

  def edit(self, uuid, metadata):
    '''
    Update the bundle with the given uuid with the new metadata.
    '''
    raise NotImplementedError

  def delete(self, bundle_spec, force=False):
    '''
    bundle_spec should be either a bundle uuid, a unique prefix of a uuid, or
    a unique bundle name.

    Delete this bundle. If force is True, delete all its (direct and indirect)
    descendents too. If force is False, and if this bundle has any downstream
    dependencies, this method will raise a UsageError.
    '''
    raise NotImplementedError

  # Commands for browsing bundles: info, ls, cat, grep, and search.

  def info(self, bundle_spec, parents=False, children=False):
    '''
    Return a dict containing detailed information about a given bundle:
      bundle_type: one of (program, dataset, macro, make, run)
      data_hash: hash of the bundle's data, if the bundle is READY
      metadata: its metadata dict
      state: its current state
      uuid: its uuid
      hard_dependencies: list of this bundle's realized dependencies

    If parents is True, this dict will also map 'parents' to a list of string
    representations of each bundle that this bundle depends on. If children is
    True, then it will map 'children' to bundles that depend on this bundle.

    Note that the list of parents and children include all logical dependencies,
    not just dependencies that are realized within the final bundle. For
    example, a run would depend on its program and input even though symlinks to
    those bundles are deleted before the program is uploaded.
    '''
    raise NotImplementedError

  def ls(self, target):
    '''
    Return (list of directories, list of files) located underneath the target.
    The target should be a (bundle_spec, path) pair.
    '''
    raise NotImplementedError

  def cat(self, target):
    '''
    Print the contents of the target file at to stdout.
    '''
    raise NotImplementedError

  def grep(self, target, pattern):
    '''
    Grep the contents of the target bundle, directory, or file for the pattern.
    '''
    raise NotImplementedError

  def search(self, query=None):
    '''
    Run a search on bundle metadata and return data for all bundles that match.
    The data for each bundle is a dict with the same keys as a dict from info.
    '''
    raise NotImplementedError

  # Various utility commands for pulling bundles back out of the system.

  def download(self, uuid, path):
    '''
    Download the contents of the given bundle to the given local path.
    '''
    # TODO(skishore): What are we going to do about dependencies here?
    # We should probably realize them. This isn't too bad, because derived
    # bundles will not include their dependencies in their final value.
    raise NotImplementedError

  def wait(self, bundle_spec):
    '''
    Block on the execution of the given bundle. Return READY or FAILED
    based on whether it was computed successfully.
    '''
    # Constants for a simple exponential backoff routine that will decrease the
    # frequency at which we check this bundle's state from 1s to 1m.
    period = 1.0
    backoff = 1.1
    max_period = 60.0
    info = self.info(bundle_spec)
    while info['state'] not in (State.READY, State.FAILED):
      time.sleep(period)
      period = min(backoff*period, max_period)
      info = self.info(bundle_spec)
    return info['state']
