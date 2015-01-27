import errno
import mock
import os
import shutil
import socket
import ssl
import tempfile
import unittest

import yaml

from jujuclient import (
    Environment, Connector,
    # Watch wrappers
    WaitForNoMachines,
    # Facades
    Actions, Annotations, Backups, Charms, HA, KeyManager, UserManager)


ENV_NAME = os.environ.get("JUJU_TEST_ENV")

if not ENV_NAME:
    raise ValueError("No Testing Environment Defined.")


def reset(env):
    status = env.status()
    env.destroy_machines(
        sorted(status['Machines'].keys())[1:],
        force=True)
    for s in status['Services'].keys():
        env.destroy_service(s)
    watch = env.get_watch()
    WaitForNoMachines(watch).run()


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


class KeyManagerTest(unittest.TestCase):
    def setUp(self):
        self.env = Environment.connect(ENV_NAME)
        self.keys = KeyManager(self.env)

    def tearDown(self):
        self.env.close()

    def verify_keys(self, expected, user='admin', present=True):
        keys = self.key.keys(user)['Results'][0]['Result']
        for e in expected:
            found = False
            for k in keys:
                if e in k:
                    found = True
                    break
            if not present:
                if found:
                    raise AssertionError("%s not found in %s" % (e, keys))
                return
            if not found:
                raise AssertionError("%s not found in %s" % (e, keys))

    def test_key_manager(self):
        self.verify_keys(['juju-client-key', 'juju-system-key'])
        self.assertEqual(
            self.key.import_keys('admin', ['hazmat']),
            {u'Results': [{u'Error': None}]})
        self.verify_keys(['ssh-import-id lp:hazmat'])
        self.key.delete(
            'admin',
            'kapil@objectrealms-laptop.local # ssh-import-id lp:hazmat')
        self.verify_keys(['ssh-import-id lp:hazmat'], present=False)


class BackupTest(unittest.TestCase):
    def setUp(self):
        self.env = Environment.connect(ENV_NAME)
        self.bm = Backups(self.env)

    def tearDown(self):
        self.env.close()

    def test_backups(self):
        self.assertEqual(self.bm.list()['List'], [])
        info = self.bm.create('abc')
        self.assertEqual(len(self.bm.list()['List']), 2)
        self.assertEqual(self.bm.info(info['ID'])['Notes'], 'abc')
        self.bm.remove(info['ID'])
        self.assertEqual(len(self.bm.list()['List']), [])


class UserManagerTest(unittest.TestCase):

    def setUp(self):
        self.env = Environment.connect(ENV_NAME)
        self.um = UserManager(self.env)

    def tearDown(self):
        self.env.close()

    def assert_user(self, user):
        result = self.um.info(user['username'])
        result = result['results'][0]['result']
        result.pop('date-created')
        self.assertEqual(result, user)

    def test_user_manager(self):
        result = self.um.add(
            {'username': 'magicmike', 'display-name': 'zerocool',
             'password': 'guess'})
        self.assertEqual(
            result,
            {'results': [{'tag': 'user-magicmike@local'}]})
        self.assert_user({
            'username': 'magicmike',
            'disabled': False,
            'display-name': 'zerocool',
            'created-by': 'admin@local'})
        self.um.disable('mike')
        self.assert_user({
            'username': 'magicmike',
            'disabled': True,
            'display-name': 'zerocool',
            'created-by': 'admin@local'})
        self.um.enable('mike')
        self.assert_user({
            'username': 'magicmike',
            'disabled': False,
            'display-name': 'zerocool',
            'created-by': 'admin@local'})
        self.assertEqual(
            self.um.set_password({'username': 'mike', 'password': 'iforgot'}),
            {u'Results': [{u'Error': None}]})
        self.um.disable('mike')


class CharmBase(object):

    _repo_dir = None

    @property
    def repo_dir(self):
        if not self._repo_dir:
            self._repo_dir = self.mkdir()
        return self._repo_dir

    def mkdir(self):
        d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, d)
        return d

    def write_local_charm(self, md, config=None, actions=None):
        charm_dir = os.path.join(self.repo_dir, md['series'], md['name'])
        if not os.path.exists(charm_dir):
            os.makedirs(charm_dir)
        md_path = os.path.join(charm_dir, 'metadata.yaml')
        with open(md_path, 'w') as fh:
            md.pop('series', None)
            fh.write(yaml.safe_dump(md))

        if config is not None:
            cfg_path = os.path.join(charm_dir, 'config.yaml')
            with open(cfg_path, 'w') as fh:
                fh.write(yaml.safe_dump(config))

        if actions is not None:
            act_path = os.path.join(charm_dir, 'actions.yaml')
            with open(act_path, 'w') as fh:
                fh.write(yaml.safe_dump(actions))

        with open(os.path.join(charm_dir, 'revision'), 'w') as fh:
            fh.write('1')


class ActionTest(unittest.TestCase, CharmBase):

    def setUp(self):
        self.env = Environment.connect(ENV_NAME)
        self.actions = Actions(self.env)
        self.setupCharm()

    def tearDown(self):
        self.env.close()

    def setupCharm(self):
        actions = {
            'deepsix': {
                'description': 'does something with six',
                'params': {
                    'optiona': {
                        'type': 'string',
                        'default': 'xyz'}}}}
        self.write_local_charm({
            'name': 'mysql',
            'summary': 'its a db',
            'description': 'for storing things',
            'series': 'trusty',
            'provides': {
                'db': {
                    'interface': 'mysql'}}}, actions=actions)

    def test_actions(self):
        result = self.env.add_local_charm_dir(
            os.path.join(self.repo_dir, 'trusty', 'mysql'),
            'trusty')
        charm_url = result['CharmURL']
        self.env.deploy('action-db', charm_url)
        actions = self.actions.service_actions('action-db')
        self.assertEqual(
            actions,
            {u'results': [
                {u'servicetag': u'service-action-db',
                 u'actions': {u'ActionSpecs':
                              {u'deepsix': {
                                  u'Params': {u'title': u'deepsix',
                                              u'type': u'object',
                                              u'description':
                                              u'does something with six',
                                              u'properties': {
                                                  u'optiona': {
                                                      u'default': u'xyz',
                                                      u'type': u'string'}}},
                                  u'Description': u'does something with six'
            }}}}]})
        result = self.actions.enqueue_units(
            'action-db/0', 'deepsix', {'optiona': 'bez'})
        self.assertEqual(result, [])


class CharmTest(unittest.TestCase):

    def setUp(self):
        self.env = Environment.connect(ENV_NAME)
        self.charms = Charms(self.env)

    def tearDown(self):
        self.env.close()

    def test_charm(self):
        self.charms.list()
        self.env.add_charm('cs:~hazmat/trusty/etcd-6')
        self.charms.list()
        self.charms.info('cs:~hazmat/trusty/etcd-6')


class HATest(unittest.TestCase):

    def setUp(self):
        self.env = Environment.connect(ENV_NAME)
        self.ha = HA(self.env)

    def tearDown(self):
        self.env.close()

    def test_ha(self):
        previous = self.env.status()
        self.ha.ensure_availability(3)
        current = self.env.status()
        self.assertNotEqual(previous, current)


class AnnotationTest(unittest.TestCase):

    def setUp(self):
        self.env = Environment.connect(ENV_NAME)
        self.charms = Annotations(self.env)

    def tearDown(self):
        self.env.close()


class ClientTest(unittest.TestCase):

    def setUp(self):
        self.env = Environment.connect(ENV_NAME)

    def tearDown(self):
        reset(self.env)
        self.env = None

    def assert_service(self, svc_name):
        status = self.env.status()
        services = status.get('Services', {})
        self.assertTrue(
            svc_name in services,
            "Service {} does not exist".format(svc_name)
        )

    def assert_not_service(self, svc_name):
        status = self.env.status()
        services = status.get('Services', {})
        if svc_name in services:
            self.assertTrue(
                services[svc_name]['Life'] in ('dying', 'dead'))

    def test_juju_info(self):
        self.assertEqual(
            sorted(self.env.info().keys()),
            ['DefaultSeries', 'Name', 'ProviderType', 'UUID'])

    def test_add_get_charm(self):
        self.env.add_charm('cs:~hazmat/trusty/etcd-6')
        charm = self.env.get_charm(
            'cs:~hazmat/trusty/etcd-6')
        self.assertEqual(charm, {})

    def test_deploy_and_destroy(self):
        self.assert_not_service('db')
        self.env.deploy('db', 'cs:trusty/mysql-1')
        self.assert_service('db')
        self.env.destroy_service('db')
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
