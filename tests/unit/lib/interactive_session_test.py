from codalab.lib.interactive_session import InteractiveSession
from codalab.worker.download_util import BundleTarget

import unittest
import re


class InteractiveSessionTest(unittest.TestCase):
    def test_get_docker_run_command(self):
        targets = [('key', BundleTarget('uuid1', '')), ('key2', BundleTarget('uuid2', ''))]
        bundle_locations = {'uuid1': 'local/path1', 'uuid2': 'local/path2'}
        session = InteractiveSession(
            'some-docker-image', dependencies=targets, bundle_locations=bundle_locations
        )
        expected_regex = (
            'docker run -it --name interactive-session-0x[a-z0-9]{32} -w \\/0x[a-z0-9]{32} -v '
            '[\\s\\S]{0,100}local\\/path1:\\/0x[a-z0-9]{32}\\/key:ro -v '
            '[\\s\\S]{0,100}local\\/path2:\\/0x[a-z0-9]{32}\\/key2:ro '
            'some-docker-image bash'
        )
        self.assertTrue(re.match(expected_regex, session.get_docker_run_command()))

    def test_get_docker_run_command_with_subpaths(self):
        targets = [
            ('key', BundleTarget('uuid1', 'sub/path1')),
            ('key2', BundleTarget('uuid2', 'sub/path2')),
        ]
        bundle_locations = {'uuid1': 'local/path1', 'uuid2': 'local/path2'}
        session = InteractiveSession(
            'some-docker-image', dependencies=targets, bundle_locations=bundle_locations
        )
        expected_regex = (
            'docker run -it --name interactive-session-0x[a-z0-9]{32} -w \\/0x[a-z0-9]{32} -v '
            '[\\s\\S]{0,100}local\\/path1/sub/path1:\\/0x[a-z0-9]{32}\\/key:ro -v '
            '[\\s\\S]{0,100}local\\/path2/sub/path2'
            ':\\/0x[a-z0-9]{32}\\/key2:ro some-docker-image bash'
        )
        self.assertTrue(re.match(expected_regex, session.get_docker_run_command()))

    def test_missing_bundle_location(self):
        try:
            targets = [
                ('key', BundleTarget('uuid1', 'sub/path1')),
                ('key2', BundleTarget('uuid2', 'sub/path2')),
            ]
            # Missing a location of uuid2 bundle
            bundle_locations = {'uuid1': 'local/path1'}
            session = InteractiveSession(
                'some-docker-image', dependencies=targets, bundle_locations=bundle_locations
            )
            session.start()
        except Exception as e:
            self.assertEqual(str(e), 'Missing bundle location for bundle uuid: uuid2')
            return
        self.fail('Should have thrown an error for the missing bundle location')
