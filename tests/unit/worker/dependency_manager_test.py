import os
import unittest
import tempfile
import threading

from codalab.worker.dependency_manager import NFSLock


class NFSLockTest(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.state_path = os.path.join(self.test_dir, 'state.txt')
        self.lock_path = f'{self.state_path}.lock'

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
