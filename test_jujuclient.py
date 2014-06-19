import unittest
import os
from jujuclient import Environment

ENDPOINT = os.environ.get("JUJU_ENDPOINT")
AUTH = os.environ.get("JUJU_AUTH")


class ClientFunctionalTest(unittest.TestCase):

    def setUp(self):
        self.client = Environment(ENDPOINT)
        self.client.login(AUTH)

    def tearDown(self):
        self.client.close()
        self.client = None

    def assert_service(self, svc_name):
        status = self.client.status()
        services = status.get('Services', {})
        self.assertTrue(
            svc_name in services,
            "Service {} does not exist".format(svc_name)
        )

    def assert_not_service(self, svc_name):
        status = self.client.status()
        services = status.get('Services', {})
        self.assertFalse(
            svc_name in services,
            "Service {} exists".format(svc_name)
        )

    def test_juju_info(self):
        self.assertEqual(
            sorted(self.client.info().keys()),
            ['DefaultSeries', 'Name', 'ProviderType', 'UUID'])

    def test_deploy_and_destroy(self):
        self.assert_not_service('db')
        self.client.deploy('db', 'precise/mysql-46')
        self.assert_service('db')
        self.client.destroy_service('db')
        # XXX: This library claims to be synchronous but destroying a service
        # takes over 3 seconds to be removed from the status listing.
        import time
        time.sleep(4)
        self.assert_not_service('db')

    def test_expose_unexpose(self):
        pass

    def test_add_remove_units(self):
        pass

    def test_get_set_config(self):
        pass

    def test_get_set_constraints(self):
        pass

    def test_get_set_annotations(self):
        pass

    def test_add_remove_relation(self):
        pass

    def test_status(self):
        pass

    def test_info(self):
        pass


if __name__ == '__main__':
    unittest.main()
