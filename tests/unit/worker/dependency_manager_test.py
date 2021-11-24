import os
import unittest
import shutil
import tempfile
import threading
from concurrent.futures import ProcessPoolExecutor
from unittest.mock import MagicMock

from codalab.worker.bundle_state import DependencyKey
from codalab.worker.dependency_manager import DependencyManager
from codalab.worker.nfs_dependency_manager import NFSDependencyManager, NFSLock


class DependencyManagerTest(unittest.TestCase):
    def setUp(self):
        self.work_dir = tempfile.mkdtemp()
        self.state_path = os.path.join(self.work_dir, "dependencies-state.json")
        self.dependency_manager = NFSDependencyManager(
            commit_file=self.state_path,
            bundle_service=None,
            worker_dir=self.work_dir,
            max_cache_size_bytes=1024,
            download_dependencies_max_retries=1,
        )

    def tearDown(self):
        shutil.rmtree(self.work_dir)

    def test_get_has(self):
        dependent_uuid = "0x2"
        dependency_key = DependencyKey(parent_uuid="0x1", parent_path="parent")
        state = self.dependency_manager.get(dependent_uuid, dependency_key)
        self.assertTrue(self.dependency_manager.has(dependency_key))
        self.assertEqual(state.stage, "DOWNLOADING")
        self.assertEqual(state.path, "0x1_parent")
        self.assertEqual(state.dependents, {dependent_uuid})

    def test_release(self):
        dependency_key = DependencyKey(parent_uuid="0x1", parent_path="parent")
        self.dependency_manager.get("0x2", dependency_key)
        state = self.dependency_manager.get("0x3", dependency_key)
        # Passing in the same dependency key with a different dependent, will just add the dependent
        self.assertEqual(state.dependents, {"0x2", "0x3"})

        # Release 0x2 as a dependent
        self.dependency_manager.release("0x2", dependency_key)
        dependencies = self.dependency_manager._fetch_dependencies()
        state = dependencies[dependency_key]
        self.assertEqual(state.dependents, {"0x3"})

        # Release 0x3 as a dependent - should be left with no dependents
        self.dependency_manager.release("0x3", dependency_key)
        dependencies = self.dependency_manager._fetch_dependencies()
        state = dependencies[dependency_key]
        self.assertEqual(len(state.dependents), 0)

    def test_all_dependencies(self):
        dependency_key = DependencyKey(parent_uuid="0x1", parent_path="parent")
        self.dependency_manager.get("0x2", dependency_key)
        dependency_key = DependencyKey(parent_uuid="0x3", parent_path="parent2")
        self.dependency_manager.get("0x4", dependency_key)
        dependency_keys = self.dependency_manager.all_dependencies
        self.assertEqual(len(dependency_keys), 2)

    def test_concurrency(self):
        num_of_dependency_managers = 2
        with ProcessPoolExecutor(max_workers=num_of_dependency_managers) as executor:
            futures = [
                executor.submit(task, self.work_dir, self.state_path)
                for _ in range(num_of_dependency_managers)
            ]
            for future in futures:
                print(future.result())
                self.assertIsNone(future.exception())


def task(work_dir, state_path):
    """
    Runs the end-to-end workflow of the Dependency Manager.
    Note: ProcessPoolExecutor must serialize everything before sending it to the worker,
          so this function needs to be defined at the top-level.
    """
    # Mock Bundle Service to return a random file object
    mock_bundle_service = MagicMock()
    mock_bundle_service.get_bundle_info = MagicMock(return_value={type: "file"})
    fileobj = open(os.path.join(work_dir, "random_file"), "wb")
    # fileobj.seek((1024 * 1024 * 1024) - 1)
    fileobj.seek(1024 - 1)
    fileobj.write(b"\0")
    mock_bundle_service.get_bundle_contents = MagicMock(return_value=fileobj)

    # Create and start a dependency manager
    process_id = os.getpid()
    print(f"Starting a DependencyManager on process {process_id}...")
    dependency_manager = NFSDependencyManager(
        commit_file=state_path,
        bundle_service=mock_bundle_service,
        worker_dir=work_dir,
        max_cache_size_bytes=1024,
        download_dependencies_max_retries=1,
    )
    dependency_manager.start()
    print("started")

    # Register a run's UUID as a dependent of a parent bundle with UUID 0x1
    dependency_key = DependencyKey(parent_uuid="0x1", parent_path="parent")
    run_uuid = f"0x{process_id}"
    state = dependency_manager.get(run_uuid, dependency_key)
    dependency_path = os.path.join(dependency_manager.dependencies_dir, state.path)
    print(dependency_path)
    assert os.path.exists(dependency_path), "Dependency was deleted even with dependents."

    # Release the run bundle as a dependent
    dependency_manager.release(run_uuid, dependency_key)
    dependencies = dependency_manager._fetch_dependencies()
    state = dependencies[dependency_key]
    print(f"Checking {run_uuid} in {state.dependents}")
    assert (
        run_uuid not in state.dependents
    ), "Dependent should not be in the list of dependents after unregistering."

    # Stop the Dependency Manager
    dependency_manager.stop()


class NFSLockTest(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.state_path = os.path.join(self.test_dir, "state.txt")
        self.lock_path = f"{self.state_path}.lock"

    def tearDown(self):
        try:
            os.remove(self.state_path)
        except OSError:
            pass
        os.rmdir(self.test_dir)

    def test_nfs_lock(self):
        def create_run(lock_id):
            lock = locks[lock_id]

            def t() -> None:
                for _ in range(1_000):
                    with lock:
                        self.assertTrue(lock.is_locked)
                        for i, other_locks in enumerate(locks):
                            if i == lock_id:
                                continue
                            self.assertFalse(other_locks.is_locked)

                        f = open(self.state_path, "w")
                        f.write(str(lock_id))
                        f.close()
                        self.assertTrue(lock.is_locked)

            return t

        # Runs multiple threads, which acquire the same lock file with a different flufl.lock object
        number_of_locks = 30
        locks = [NFSLock(self.lock_path) for _ in range(number_of_locks)]
        threads = [
            threading.Thread(target=create_run(lock_id), name=f"thread_lock{lock_id}")
            for lock_id in range(number_of_locks)
        ]

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        for lock in locks:
            self.assertFalse(lock.is_locked)
