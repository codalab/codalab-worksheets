import random

from codalab.common import State


class Worker(object):
  def __init__(self, bundle_store, model):
    self.bundle_store = bundle_store
    self.model = model

  def update_created_bundles(self):
    '''
    Scan through CREATED bundles check their dependencies' statuses.
    If any parent is FAILED, move them to FAILED.
    If all parents are READY, move them to STAGED.
    '''
    bundles = self.model.batch_get_bundles(state=State.CREATED)
    print 'Got %s CREATED bundles...' % (len(bundles),)
    parent_uuids = set(
      dep.parent_uuid for bundle in bundles for dep in bundle.dependencies
    )
    parents = self.model.batch_get_bundles(uuid=parent_uuids)
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
    print 'Failing %s bundles...' % (len(bundles_to_fail),)
    self.model.batch_update_bundle_states(bundles_to_fail, State.FAILED)
    print 'Staging %s bundles...' % (len(bundles_to_stage),)
    self.model.batch_update_bundle_states(bundles_to_stage, State.STAGED)
    num_blocking = len(bundles) - len(bundles_to_fail) - len(bundles_to_stage)
    print '%s bundles are still blocking.' % (num_blocking,)

  def update_staged_bundles(self):
    '''
    If there are any STAGED bundles, pick one and try to lock it.
    If we get a lock, move the locked bundle to RUNNING and then run it.
    '''
    bundles = self.model.batch_get_bundles(state=State.STAGED)
    print 'Got %s STAGED bundles...' % (len(bundles),)
    random.shuffle(bundles)
    for bundle in bundles:
      if self.model.batch_update_bundle_states([bundle], State.RUNNING):
        print 'Got a lock on %s' % (bundle,)
        parent_uuids = set(dep.parent_uuid for dep in bundle.dependencies)
        parents = self.model.batch_get_bundles(uuid=parent_uuids)
        parent_dict = {parent.uuid: parent for parent in parents}
        if set(parent_dict) != parent_uuids:
          missing_uuids = set(parent_dict) - parent_uuids
          print 'FAILED: missing parents: %s' % (', '.join(missing_uuids),)
          self.model.batch_update_bundle_states([bundle], State.FAILED)
          return
        bundle.run(self.bundle_store, parent_dict)
