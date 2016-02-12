'''
Worker is a class that executes bundles that need to be run.

It requires direct access to the bundle database and data store, and it
provides a few methods once it is initialized:
  update_created_bundles: update bundles that are blocking on others.
  update_ready_bundles: run a single bundle in the ready state.
'''
import contextlib
import datetime
import random
import subprocess
import sys
import time
import tempfile
import traceback
import os

from codalab.common import (
  precondition,
  State,
  UsageError,
)
from codalab.lib import (
  canonicalize,
  path_util,
)
from codalab.bundles.run_bundle import RunBundle
from codalab.bundles.make_bundle import MakeBundle
from codalab.lib.bundle_action import BundleAction
from codalab.machines import remote_machine

class Worker(object):
    def __init__(self, bundle_store, model, machine, auth_handler):
        self.bundle_store = bundle_store
        self.model = model
        self.profiling_depth = 0
        self.verbose = 0
        self.machine = machine
        self.auth_handler = auth_handler  # In order to get names of owners

    def pretty_print(self, message):
        time_str = datetime.datetime.utcnow().isoformat()[:19].replace('T', ' ')
        print '%s: %s%s' % (time_str, '  '*self.profiling_depth, message)

    @contextlib.contextmanager
    def profile(self, message):
        #self.pretty_print(message)
        self.profiling_depth += 1
        start_time = time.time()
        result = yield
        elapsed_time = time.time() - start_time
        self.profiling_depth -= 1
        #if result: self.pretty_print('%s: %0.2fs.' % (message, elapsed_time,))
        #self.pretty_print('Done! Took %0.2fs.' % (elapsed_time,))

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
                if not success and self.verbose >= 1:
                    self.pretty_print('WARNING: update failed!')
                return success
        return True

    def get_parent_dict(self, bundle):
        # Compute a dict mapping parent_uuid -> parent for each dep of this bundle.
        parent_uuids = set(dep.parent_uuid for dep in bundle.dependencies)
        parents = self.model.batch_get_bundles(uuid=parent_uuids)
        parent_dict = {parent.uuid: parent for parent in parents}
        return parent_dict

    def start_bundle(self, bundle):
        '''
        Run the given bundle using an available Machine.
        Return whether something was started.
        '''
        # Check that we're running a bundle in the QUEUED state.
        state_message = 'Unexpected bundle state: %s' % (bundle.state,)
        precondition(bundle.state == State.QUEUED, state_message)
        data_hash_message = 'Unexpected bundle data_hash: %s' % (bundle.data_hash,)
        precondition(bundle.data_hash is None, data_hash_message)

        # Run the bundle.
        with self.profile('Running bundle...'):
            started = False
            if isinstance(bundle, RunBundle):
                try:
                    # Get the username of the bundle
                    results = self.auth_handler.get_users('ids', [bundle.owner_id])
                    if results.get(bundle.owner_id):
                        username = results[bundle.owner_id].name
                    else:
                        username = str(bundle.owner_id)

                    status = self.machine.start_bundle(bundle, self.bundle_store, self.get_parent_dict(bundle), username)
                    if status != None:
                        status['started'] = int(time.time())
                        started = True

                except Exception as e:
                    # If there's an exception, we just make the bundle fail
                    # (even if it's not the bundle's fault).
                    temp_dir = canonicalize.get_current_location(self.bundle_store, bundle.uuid)
                    path_util.make_directory(temp_dir)
                    status = {'bundle': bundle, 'success': False, 'failure_message': 'Internal error: ' + str(e), 'temp_dir': temp_dir}
                    print '=== INTERNAL ERROR: %s' % e
                    started = True  # Force failing
                    traceback.print_exc()
            else:  # MakeBundle
                started = True
            if started:
                print '-- START BUNDLE: %s' % (bundle,)
                self._update_events_log('start_bundle', bundle, (bundle.uuid,))

            # If we have a MakeBundle, then just process it immediately.
            if isinstance(bundle, MakeBundle):
                temp_dir = canonicalize.get_current_location(self.bundle_store, bundle.uuid)
                path_util.make_directory(temp_dir)
                status = {'bundle': bundle, 'success': True, 'temp_dir': temp_dir}

            # Update database
            if started:
                self.update_running_bundle(status)
            return started

    def _safe_get_bundle(self, uuid):
        try:
            return self.model.get_bundle(uuid)
        except:
            return None

    def check_bundle_actions(self):
        '''
        Process bundle actions (e.g., kill, write).  Get the bundle actions
        from the database and dispatch to the machine.
        Return if anything was done
        '''
        bundle_actions = self.model.pop_bundle_actions()
        if self.verbose >= 2: print 'bundle_actions:', bundle_actions
        did_something = False
        for x in bundle_actions:
            # Get the bundle
            bundle = self._safe_get_bundle(x.bundle_uuid)
            if not bundle:  # Might have been deleted
                continue
            if bundle.state in [State.READY, State.FAILED]:  # Already terminated
                continue

            # Perform it the action
            if self.machine.send_bundle_action(bundle, x.action):
                # Append this action to the bundle to record that this action was
                # performed.
                new_actions = getattr(bundle.metadata, 'actions', []) + [x.action]
                db_update = {'metadata': {'actions': new_actions}}
                self.model.update_bundle(bundle, db_update)
                did_something = True
            else:
                # Add the action back
                self.model.add_bundle_action(bundle.uuid, x.action)

        return did_something

    def check_timed_out_bundles(self, update_timeout=60*60*24):
        '''
        Filters the list of running bundles by their time, and marks those that
        have not been updated in > update_timeout seconds as FAILED.
        Default update_timeout: 1 day
        '''
        uuids = self.model.search_bundle_uuids(worksheet_uuid=None, user_id=self.model.root_user_id,
                                               keywords=['state='+','.join([State.RUNNING, State.QUEUED])])
        bundles = self.model.batch_get_bundles(uuid=uuids)
        def _failed(bundle):
            now = int(time.time())
            since_last_update = now - bundle.metadata.last_updated
            return since_last_update >= update_timeout
        failed_bundles = filter(_failed, bundles)
        for bundle in failed_bundles:
            failure_msg = 'No response from worker in %s' % time.strftime('%H:%M:%S', time.gmtime(update_timeout))
            metadata_update = {'failure_message': failure_msg}
            update = {'state': State.FAILED, 'metadata': metadata_update}
            self.model.update_bundle(bundle, update)

    # Poll processes to see if bundles have finished running
    # Either way, update the bundle metadata.
    def check_finished_bundles(self):
        statuses = self.machine.get_bundle_statuses()

        # Lookup the bundle given the uuid from the status
        new_statuses = []
        # Lookup all the uuids and bundles of the relevant job handles
        handles = [status['job_handle'] for status in statuses]
        uuids = self.model.search_bundle_uuids(worksheet_uuid=None, user_id=self.model.root_user_id, keywords=['job_handle='+','.join(handles)])
        bundles = self.model.batch_get_bundles(uuid=uuids)
        handle_to_bundles = {}
        for bundle in bundles:
            handle = bundle.metadata.job_handle
            handle_to_bundles[handle] = bundle
        for status in statuses:
            handle = status['job_handle']
            bundle = handle_to_bundles.get(handle)
            if not bundle:
                continue
            status['bundle'] = bundle
            new_statuses.append(status)
        statuses = new_statuses

        # Now that we have the bundle information and thus the temporary directory,
        # we can fetch the rest of the status.
        for status in statuses:
            if 'bundle_handler' in status:
                status.update(status['bundle_handler'](status['bundle']))
                del status['bundle_handler']

        # Make a note of runnning jobs (according to the database) which aren't
        # mentioned in statuses.  These are probably zombies, and we want to
        # get rid of them if they have been issued a kill action.
        status_bundle_uuids = set(status['bundle'].uuid for status in statuses)
        running_bundles = self.model.batch_get_bundles(state=State.RUNNING)
        for bundle in running_bundles:
            if bundle.uuid in status_bundle_uuids: continue  # Exists, skip
            if BundleAction.KILL not in getattr(bundle.metadata, 'actions', set()): continue  # Not killing
            status = {'state': State.FAILED, 'bundle': bundle}
            print 'work_manager: %s (%s): killing zombie %s' % (bundle.uuid, bundle.state, status)
            self.update_running_bundle(status)

        # Update the status of these bundles.
        for status in statuses:
            bundle = status['bundle']
            if bundle.state in [State.READY, State.FAILED]:  # Skip bundles that have already completed.
                continue
            print 'work_manager: %s (%s): %s' % (bundle.uuid, bundle.state, status)
            self.update_running_bundle(status)

    def update_running_bundle(self, status):
        '''
        Update the database with information about the bundle given by |status|.
        If the bundle is completed, then we need to install the bundle and clean up.
        '''
        status['last_updated'] = int(time.time())

        # Update the bundle's data with status (which is the new information).
        bundle = status['bundle']

        # Update to the database
        db_update = {}

        # Update state
        if 'state' in status and status['state']:
            db_update['state'] = status['state']

        # Add metadata from the machine
        db_update['metadata'] = metadata = {}
        bundle_subclass = type(bundle)
        for spec in bundle_subclass.METADATA_SPECS:
            value = status.get(spec.key)
            if value is not None:
                metadata[spec.key] = value

        #print 'update_running_bundle', status

        # See if the bundle is completed.
        success = status.get('success')
        if success is not None:
            # Re-install dependencies.
            # - For RunBundle, remove the dependencies.
            # - For MakeBundle, copy.  This way, we maintain the invariant that
            # we always only need to look back one-level at the dependencies,
            # not recurse.
            try:
                temp_dir = status.get('temp_dir')
                if not temp_dir:
                    temp_dir = bundle.metadata.temp_dir
                if isinstance(bundle, RunBundle):
                    print >>sys.stderr, 'Worker.finalize_bundle: removing dependencies from %s (RunBundle)' % temp_dir
                    bundle.remove_dependencies(self.bundle_store, self.get_parent_dict(bundle), temp_dir)
                else:
                    print >>sys.stderr, 'Worker.finalize_bundle: installing (copying) dependencies to %s (MakeBundle)' % temp_dir
                    bundle.install_dependencies(self.bundle_store, self.get_parent_dict(bundle), temp_dir, copy=True)

                db_update['data_hash'] = path_util.hash_directory(temp_dir)
                metadata.update(data_size=path_util.get_size(temp_dir))
            except Exception as e:
                print '=== INTERNAL ERROR: %s' % e
                traceback.print_exc()
                success = False
                metadata['failure_message'] = 'Internal error: ' + e.message

            # Clean up any state for RunBundles.
            if isinstance(bundle, RunBundle):
                try:
                    self.machine.finalize_bundle(bundle)
                except Exception as e:
                    success = False
                    if 'failure_message' not in metadata:
                        metadata['failure_message'] = e.message
                    else:
                        metadata['failure_message'] += '\n' + e.message

            state = State.READY if success else State.FAILED
            db_update['state'] = state
            print '-- END BUNDLE: %s [%s]' % (bundle, state)
            print ''

            self._update_events_log('finalize_bundle', bundle, (bundle.uuid, state, metadata))

            # Update user statistics
            self.model.increment_user_time_used(bundle.owner_id, getattr(bundle.metadata, 'time', 0))
            self.model.update_user_disk_used(bundle.owner_id)

        # Update database!
        self.model.update_bundle(bundle, db_update)


    def update_created_bundles(self):
        '''
        Scan through CREATED bundles check their dependencies' statuses.
        If any parent is FAILED, move them to FAILED.
        If all parents are READY, move them to STAGED.
        Return whether something happened
        '''
        #print '-- Updating CREATED bundles! --'
        with self.profile('Getting CREATED bundles...'):
            bundles = self.model.batch_get_bundles(state=State.CREATED)
            if self.verbose >= 1 and len(bundles) > 0:
                self.pretty_print('Updating %s created bundles.' % (len(bundles),))
        parent_uuids = set(
          dep.parent_uuid for bundle in bundles for dep in bundle.dependencies
        )

        with self.profile('Getting parents...'):
            parents = self.model.batch_get_bundles(uuid=parent_uuids)
        all_parent_states = {parent.uuid: parent.state for parent in parents}
        all_parent_uuids = set(all_parent_states)
        bundles_to_fail = []
        bundles_to_stage = []
        for bundle in bundles:
            parent_uuids = set(dep.parent_uuid for dep in bundle.dependencies)
            missing_uuids = parent_uuids - all_parent_uuids
            # If uuid doesn't exist, then don't process this bundle yet (the dependency might show up later)
            if missing_uuids: continue
            parent_states = {uuid: all_parent_states[uuid] for uuid in parent_uuids}
            failed_uuids = [
              uuid for (uuid, state) in parent_states.iteritems()
              if state == State.FAILED
            ]
            if failed_uuids:
                bundles_to_fail.append(
                  (bundle, 'Parent bundles failed: %s' % (', '.join(failed_uuids),)))
            elif all(state == State.READY for state in parent_states.itervalues()):
                bundles_to_stage.append(bundle)

        with self.profile('Failing %s bundles...' % (len(bundles_to_fail),)):
            for (bundle, failure_message) in bundles_to_fail:
                metadata_update = {'failure_message': failure_message}
                update = {'state': State.FAILED, 'metadata': metadata_update}
                self.model.update_bundle(bundle, update)
        self.update_bundle_states(bundles_to_stage, State.STAGED)
        num_processed = len(bundles_to_fail) + len(bundles_to_stage)
        num_blocking  = len(bundles) - num_processed
        if num_processed > 0:
            self.pretty_print('%s CREATED bundles => %s STAGED, %s FAILED; %s bundles still waiting on dependencies.' % \
                (num_processed, len(bundles_to_stage), len(bundles_to_fail), num_blocking,))
            return True
        return False

    def update_staged_bundles(self):
        '''
        If there are any STAGED bundles, pick one and try to lock it.
        If we get a lock, move the locked bundle to QUEUED.
        The status will be changed to RUNNING later.
        '''
        #print '-- Updating STAGED bundles! --'
        with self.profile('Getting STAGED bundles...'):
            bundles = self.model.batch_get_bundles(state=State.STAGED)
            if self.verbose >= 1 and len(bundles) > 0:
                self.pretty_print('Staging %s bundles.' % (len(bundles),))
        new_running_bundles = 0
        for bundle in bundles:
            if not self.update_bundle_states([bundle], State.QUEUED):
                self.pretty_print('WARNING: Bundle running, but state failed to update')
            else:
                if self.start_bundle(bundle):
                    new_running_bundles += 1
                else:
                    # Restage: undo state change to RUNNING
                    self.update_bundle_states([bundle], State.STAGED)
        else:
            if self.verbose >= 2: self.pretty_print('Failed to lock a bundle!')
        return new_running_bundles > 0

    def run_loop(self, num_iterations, sleep_time):
        '''
        Repeat forever (if iterations != None) or for a finite number of iterations.
        Moves created bundles to staged and actually executes the staged bundles.
        '''
        self.pretty_print('Running worker loop (num_iterations = %s, sleep_time = %s)' % (num_iterations, sleep_time))
        iteration = 0
        while not num_iterations or iteration < num_iterations:
            # Check to see if we need to take any actions on bundles
            bool_action = self.check_bundle_actions()
            # Try to stage bundles
            self.update_created_bundles()
            # Try to run bundles with READY parents
            bool_run = self.update_staged_bundles()
            # Check to see if any bundles are done running
            bool_done = self.check_finished_bundles()
            # Check to see if any bundles have timed out
            bool_timed_out = self.check_timed_out_bundles()

            # Sleep only if nothing happened.
            if not (bool_action or bool_run or bool_done or bool_timed_out):
                time.sleep(sleep_time)
            else:
                # Advance counter only if something interesting happened
                iteration += 1

    def _update_events_log(self, command, bundle, args):
      self.model.update_events_log(
        user_id=bundle.owner_id,
        user_name=None,  # Don't know
        command=command,
        args=args,
        uuid=bundle.uuid)
