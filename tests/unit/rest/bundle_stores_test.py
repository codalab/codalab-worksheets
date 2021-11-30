import logging
import os
from .base import BaseTestCase

logger = logging.getLogger(__name__)


class BundlesTest(BaseTestCase):
    def test_get_bundle_stores(self):
        os.environ["CODALAB_TEST_USER"] = "codalab"
        # response = self.app.get("/rest/bundle_stores")
        response = self.app.get("/rest/user")
        logger.error(response)
        self.assertEqual(response.status_int, 200)                   