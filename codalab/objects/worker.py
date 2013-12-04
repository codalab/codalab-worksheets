from codalab.common import State


class Worker(object):
  def __init__(self, bundle_store, model):
    self.bundle_store = bundle_store
    self.model = model

  def update_created_bundles(self):
    bundles = self.model.batch_get_bundles(state=State.CREATED)
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
    self.model.batch_update_bundles(bundles_to_fail, {'state': State.FAILED})
    print 'Staging %s bundles...' % (len(bundles_to_stage),)
    self.model.batch_update_bundles(bundles_to_stage, {'state': State.STAGED})
    num_blocking = len(bundles) - len(bundles_to_fail) - len(bundles_to_stage)
    print '%s bundles are still blocking.' % (num_blocking,)
