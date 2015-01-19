import errno
import mock
import os
import socket
import ssl
import unittest


from jujuclient import Environment, Connector

# Must set either both of these
ENDPOINT = os.environ.get("JUJU_ENDPOINT")
AUTH = os.environ.get("JUJU_AUTH")
# OR set this one
ENV_NAME = os.environ.get("JUJU_TEST_ENV")


if not (ENDPOINT and AUTH) and not ENV_NAME:
    print(ENDPOINT, AUTH)
    print(ENV_NAME)
    raise ValueError("No Testing Environment Defined.")


class ClientConnectorTest(unittest.TestCase):

    @mock.patch('jujuclient.websocket')
    def test_connect_socket(self, websocket):
        address = "wss://abc:17070"
        Connector.connect_socket(address)
        websocket.create_connection.assert_called_once_with(
            address, origin=address, sslopt={
                'ssl_version': ssl.PROTOCOL_TLSv1,
                'cert_reqs': ssl.CERT_NONE})

    @mock.patch('socket.create_connection')
    def test_is_server_available_unknown_error(self, connect_socket):
        connect_socket.side_effect = ValueError()
        self.assertRaises(
            ValueError, Connector().is_server_available,
            'foo.example.com:7070')

    @mock.patch('socket.create_connection')
    def test_is_server_available_known_error(self, connect_socket):
        e = socket.error()
        e.errno = errno.ETIMEDOUT
        connect_socket.side_effect = e
        self.assertFalse(
            Connector().is_server_available("foo.example.com:7070"))


class ClientFunctionalTest(unittest.TestCase):

    def setUp(self):
        if ENV_NAME:
            self.client = Environment.connect(ENV_NAME)
        else:
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
        if svc_name in services:
            self.assertTrue(
                services[svc_name]['Life'] in ('dying', 'dead'))

    def test_juju_info(self):
        self.assertEqual(
            sorted(self.client.info().keys()),
            ['DefaultSeries', 'Name', 'ProviderType', 'UUID'])

    def test_deploy_and_destroy(self):
        self.assert_not_service('db')
        self.client.deploy('db', 'cs:trusty/mysql-1')
        self.assert_service('db')
        self.client.destroy_service('db')
        self.assert_not_service('db')

    def xtest_expose_unexpose(self):
        pass

    def xtest_add_remove_units(self):
        pass

    def xtest_get_set_config(self):
        pass

    def xtest_get_set_constraints(self):
        pass

    def xtest_get_set_annotations(self):
        pass

    def xtest_add_remove_relation(self):
        pass

    def xtest_status(self):
        pass

    def xtest_info(self):
        pass


if __name__ == '__main__':
    unittest.main()
