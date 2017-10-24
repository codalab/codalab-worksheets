import json
import os
import tempfile
import threading
import time
import unittest

from codalabworker.dependency_manager import DependencyManager
from codalabworker.file_util import remove_path

class DependencyManagerTest(unittest.TestCase):
    def setUp(self):
        self.work_dir = tempfile.mkdtemp()
        self.manager = DependencyManager(self.work_dir, None)
        self.bundles_dir = os.path.join(self.work_dir, 'bundles')

    def tearDown(self):
        remove_path(self.work_dir)

    def test_load_state(self):
        self.manager.finish_run('uuid1')
        self.manager.finish_run('uuid2')
        with open(os.path.join(self.bundles_dir, 'random_file'), 'w'):
            pass
        self.assertIn('random_file', os.listdir(self.bundles_dir))
        new_manager = DependencyManager(self.work_dir, 1 * 1024 * 1024)
        self.check_state([('uuid1', ''), ('uuid2', '')], new_manager)
        self.assertIn(DependencyManager.STATE_FILENAME, os.listdir(self.work_dir))
        self.assertNotIn('random_file', os.listdir(self.bundles_dir))

    def test_downloading(self):
        self.manager.add_dependency('uuid1', '', 'uuid2')

        # Check cases that should not block.
        self.check_add_dependency_blocks('uuid1', 'a', 'uuid5', False, True)  # Different path
        self.check_add_dependency_blocks('uuid6', '', 'uuid7', False, True)  # Different UUID

        # This call will block until the failed download. Then, it will be asked
        # to retry the download.
        self.check_add_dependency_blocks('uuid1', '', 'uuid3', True, True)

        self.manager.finish_download('uuid1', '', False)
        self.check_state([])
        time.sleep(0.01)

        # This call will block until both attempts to download are done. Then,
        # it will not be asked to retry the download.
        self.check_add_dependency_blocks('uuid1', '', 'uuid4', True, False)

        self.manager.finish_download('uuid1', '', True)
        self.check_state([('uuid1', '')])

    def test_download_path_conflict(self):
        self.assertEqual(self.manager.add_dependency('uuid1', 'a/b_c', 'uuid2')[0],
                         os.path.join(self.bundles_dir, 'uuid1_a_b_c'))
        self.assertEqual(self.manager.add_dependency('uuid1', 'a_b/c', 'uuid2')[0],
                         os.path.join(self.bundles_dir, 'uuid1_a_b_c_'))

    def test_cleanup(self):
        self.manager = DependencyManager(self.work_dir, 2 * 1024 * 1024)

        self.manager.finish_run('uuid1') # Has dependency, so will not be removed.
        self.manager.add_dependency('uuid1', '', 'uuid100')

        self.manager.add_dependency('uuid2', '', 'uuid100') # Downloading, so will not be removed.

        self.manager.finish_run('uuid3')  # Used after uuid4, so will be left.
        self.manager.finish_run('uuid4')  # Will be removed.
        self.manager.add_dependency('uuid4', '', 'uuid100')
        self.manager.remove_dependency('uuid4', '', 'uuid100')
        self.manager.add_dependency('uuid3', '', 'uuid100')
        self.manager.remove_dependency('uuid3', '', 'uuid100')

        for uuid in ['uuid1', 'uuid2', 'uuid3', 'uuid4']:
            with open(self.manager.get_run_path(uuid), 'wb') as f:
                f.write(' ' * 1024 * 1024)

        self.manager._cleanup_sleep_secs = 0
        self.manager.start_cleanup_thread()
        time.sleep(0.1)
        self.manager.stop_cleanup_thread()

        self.check_state([('uuid1', ''), ('uuid3', '')])
        self.assertIn(('uuid2', ''), self.manager._dependencies)
        self.assertItemsEqual([DependencyManager.STATE_FILENAME, 'bundles'], os.listdir(self.work_dir))
        self.assertItemsEqual(['uuid1', 'uuid2', 'uuid3'], os.listdir(self.bundles_dir))

    def check_state(self, expected_targets, manager=None):
        if manager is None:
            manager = self.manager
        targets = []
        expected_paths = []
        with manager._lock:
            for target, dependency in manager._dependencies.iteritems():
                if not dependency.downloading:
                    targets.append(target)
                expected_paths.append(dependency.path)
            self.assertItemsEqual(expected_targets, targets)
            self.assertItemsEqual(expected_paths, manager._paths)

        state_file_targets = []
        with open(manager._state_file, 'r') as f:
            for dep in json.loads(f.read()):
                state_file_targets.append(tuple(dep['target']))
            self.assertItemsEqual(expected_targets, state_file_targets)

    def check_add_dependency_blocks(self, parent_uuid, parent_path, uuid,
                                  expected_blocks, expected_should_download):
        blocked = [True]
        def blocking_code():
            _, should_download = self.manager.add_dependency(parent_uuid, parent_path, uuid)
            self.assertEqual(should_download, expected_should_download, msg='%s/%s from %s' % (parent_uuid, parent_path, uuid))
            blocked[0] = False

        threading.Thread(target=blocking_code).start()
        time.sleep(0.01)
        self.assertEqual(blocked[0], expected_blocks)
