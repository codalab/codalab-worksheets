import unittest
from codalab.lib.bundle_util import get_bundle_state_details


class GetBundleStateDetailsTest(unittest.TestCase):
    def test_running_bundle(self):
        """
        Returns `run_status` if state is `running`.
        """
        run_status = 'Running.'
        running_bundle = {
            'bundle_type': 'run',
            'state': 'running',
            'metadata': {'run_status': run_status},
        }
        state_details = get_bundle_state_details(running_bundle)
        self.assertEqual(state_details, run_status)

    def test_created_bundles(self):
        """
        Returns hardcoded details for bundles that are not running or staged.
        """
        # run bundle
        run_bundle = {
            'bundle_type': 'run',
            'state': 'created',
        }
        run_bundle_details = get_bundle_state_details(run_bundle)
        run_bundle_details_expected = (
            'Bundle has been created but its contents have not been populated yet.'
        )

        # uploaded bundle
        uploaded_bundle = {
            'bundle_type': 'dataset',
            'state': 'created',
        }
        uploaded_bundle_details = get_bundle_state_details(uploaded_bundle)
        uploaded_bundle_details_expected = (
            'Bundle has been created but its contents have not been uploaded yet.'
        )

        # make bundle
        make_bundle = {
            'bundle_type': 'make',
            'state': 'created',
        }
        make_bundle_details = get_bundle_state_details(make_bundle)
        make_bundle_details_expected = (
            'Bundle has been created but its contents have not yet been populated.'
        )

        # assertions
        self.assertEqual(run_bundle_details, run_bundle_details_expected)
        self.assertEqual(uploaded_bundle_details, uploaded_bundle_details_expected)
        self.assertEqual(make_bundle_details, make_bundle_details_expected)
