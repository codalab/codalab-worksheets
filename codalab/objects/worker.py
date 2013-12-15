import contextlib
import datetime
import random
import time

from codalab.common import (
  precondition,
  State,
)


class Worker(object):
  def __init__(self, bundle_store, model):
    self.bundle_store = bundle_store
    self.model = model
    self.profiling_depth = 0

  def pretty_print(self, message):
    time_str = datetime.datetime.utcnow().isoformat()[:19].replace('T', ' ')
    print '%s: %s%s' % (time_str, '  '*self.profiling_depth, message)

  @contextlib.contextmanager
  def profile(self, message):
    self.pretty_print(message)
    self.profiling_depth += 1
    start_time = time.time()
    yield
    elapsed_time = time.time() - start_time
    self.profiling_depth -= 1
    self.pretty_print('Done! Took %0.2fs.' % (elapsed_time,))

  def update_bundle_states(self, bundles, new_state):
    '''
    Update a list of bundles all in one state to all be in the new_state.
    Return True if all updates succeed.
    '''
    if bundles:
      message = 'Setting %s bundles to %s...' % (
        len(bundles),
        new_state.upper(),
      )
      with self.profile(message):
        states = set(bundle.state for bundle in bundles)
        precondition(len(states) == 1, 'Got multiple states: %s' % (states,))
        success = self.model.batch_update_bundles(
          bundles=bundles,
          update={'state': new_state},
          condition={'state': bundles[0].state},
        )
        if not success:
          self.pretty_print('WARNING: update failed!')
        return success
    return True

  def update_created_bundles(self):
    '''
    Scan through CREATED bundles check their dependencies' statuses.
    If any parent is FAILED, move them to FAILED.
    If all parents are READY, move them to STAGED.
    '''
    with self.profile('Getting CREATED bundles...'):
      bundles = self.model.batch_get_bundles(state=State.CREATED)
      self.pretty_print('Got %s bundles.' % (len(bundles),))
    parent_uuids = set(
      dep.parent_uuid for bundle in bundles for dep in bundle.dependencies
    )
    with self.profile('Getting parents...'):
      parents = self.model.batch_get_bundles(uuid=parent_uuids)
      self.pretty_print('Got %s bundles.' % (len(parents),))
    parent_states = {parent.uuid: parent.state for parent in parents}
    bundles_to_fail = []
    bundles_to_stage = []
    for bundle in bundles:
      parent_states = set(
        parent_states.get(dep.parent_uuid, State.FAILED)
        for dep in bundle.dependencies
      )
      if State.FAILED in parent_states:
        bundles_to_fail.append(bundle)
      elif all(state == State.READY for state in parent_states):
        bundles_to_stage.append(bundle)
    self.update_bundle_states(bundles_to_fail, State.FAILED)
    self.update_bundle_states(bundles_to_stage, State.STAGED)
    num_blocking = len(bundles) - len(bundles_to_fail) - len(bundles_to_stage)
    self.pretty_print('%s bundles are still blocking.' % (num_blocking,))

  def update_staged_bundles(self):
    '''
    If there are any STAGED bundles, pick one and try to lock it.
    If we get a lock, move the locked bundle to RUNNING and then run it.
    '''
    with self.profile('Getting STAGED bundles...'):
      bundles = self.model.batch_get_bundles(state=State.STAGED)
      self.pretty_print('Got %s bundles.' % (len(bundles),))
    random.shuffle(bundles)
    for bundle in bundles:
      if self.update_bundle_states([bundle], State.RUNNING):
        self.run_bundle(bundle)
        break
    else:
      self.pretty_print('Failed to lock a bundle!')

  def run_bundle(self, bundle):
    '''
    Run the given bundle and then update its state to be either READY or FAILED.
    If the bundle is now READY, its data_hash should be set.
    '''
    # Check that we're running a bundle in the RUNNING state.
    state_message = 'Unexpected bundle state: %s' % (bundle.state,)
    precondition(bundle.state == State.RUNNING, state_message)
    data_hash_message = 'Unexpected bundle data_hash: %s' % (bundle.data_hash,)
    precondition(bundle.data_hash is None, data_hash_message)
    # Compute a dict mapping parent_uuid -> parent for each dep of this bundle.
    parent_uuids = set(dep.parent_uuid for dep in bundle.dependencies)
    parents = self.model.batch_get_bundles(uuid=parent_uuids)
    parent_dict = {parent.uuid: parent for parent in parents}
    # Run the bundle. Mark it READY if it is successful and FAILED otherwise.
    with self.profile('Running %s...' % (bundle,)):
      try:
        data_hash = bundle.run(self.bundle_store, parent_dict)
        self.pretty_print('Success! Got data_hash: %s' % (data_hash,))
        update = {'data_hash': data_hash, 'state': State.READY}
      except Exception, e:
        self.pretty_print('FAILED! Caught %s: %s' % (e.__class__.__name__, e))
        update = {'state': State.FAILED}
    with self.profile('Setting 1 bundle to %s...' % (update['state'].upper(),)):
      condition = {'state': bundle.state}
      if not self.model.batch_update_bundles([bundle], update, condition):
        self.pretty_print('WARNING: update failed!')
