"""
Juju Client
-----------

Seriously Alpha. Works now, but API *will* change.

A simple synchronous python client for the juju-core/gojuju websocket api.

Example Usage::

   from jujuclient import Environment

   env = Environment("wss://instance-url:17070")
   env.login('secret')
   watcher = env.watch()

   env.deploy('loadbalancer', 'cs:precise/haproxy')
   env.deploy('db', 'cs:precise/mysql')
   env.deploy('blog', 'cs:precise/wordpress')

   env.add_relation('blog', 'db')
   env.add_relation('blog', 'loadbalancer')

   env.expose('loadbalancer')

   env.set_config('blog', {'engine': 'apache'})
   env.get_config('blog')
   env.set_constraints('blog', {'cpu-cores': 4})
   env.add_units('blog', 4)
   env.remove_units(['blog/0'])

   env.destroy_service('blog')

   for change_set in watcher:
       print change_set

Todo

- Provide a buffered in mem option with watches on a single conn.

Upstream/Server
  - need proper status output, or other introspection beyond AllWatcher
  - deploy local charm
  - bad constraints fail silently
  - need terminate machine api
  - clarify usage/working of env annotation
"""
# License: GPL
# Author: Kapil Thangavelu <kapil.foss@gmail.com>

from contextlib import contextmanager
import json
import pprint
import signal
import StringIO

import websocket


class AlreadyConnected(Exception):
    pass


class LoginRequired(Exception):
    pass


class TimeoutError(StopIteration):
    pass


class TimeoutWatchInProgress(Exception):
    pass


class EnvError(Exception):

    def __init__(self, error):
        self.error = error
        self.message = error['Error']

    def __str__(self):
        stream = StringIO.StringIO()
        pprint.pprint(self.error, stream, indent=4)
        return "<Env Error - Details:\n %s >" % (
            stream.getvalue())


class RPC(object):

    _auth = False
    _request_id = 0

    def _rpc(self, op):
        if not self._auth and not op.get("Request") == "Login":
            raise LoginRequired()
        if not 'Params' in op:
            op['Params'] = {}
        op['RequestId'] = self._request_id
        self._request_id += 1
        self.conn.send(json.dumps(op))
        raw = self.conn.recv()
        result = json.loads(raw)
        #print "raw", op['Request'], raw
        if 'Error' in result:
            raise EnvError(result)
        return result['Response']


class Watcher(RPC):

    _auth = True

    def __init__(self, conn):
        self.conn = conn
        self.watcher_id = None

    def start(self):
        result = self._rpc({
            'Type': 'Client',
            'Request': 'WatchAll',
            'Params': {}})
        self.watcher_id = result['AllWatcherId']
        return result

    def next(self):
        if self.watcher_id is None:
            self.start()
        result = self._rpc({
            'Type': 'AllWatcher',
            'Request': 'Next',
            'Id': self.watcher_id})
        return result['Deltas']

    def stop(self):
        result = self._rpc({
            'Type': 'AllWatcher',
            'Request': 'Stop',
            'Id': self.watcher_id})
        self.conn.close()
        return result

    def __iter__(self):
        return self


class TimeoutWatcher(Watcher):
    # A simple non concurrent watch using signals..

    _timeout = None

    def set_timeout(self, timeout):
        self._timeout = timeout

    def next(self):
        with self._set_alarm(self._timeout):
            return super(TimeoutWatcher, self).next()

    @classmethod
    @contextmanager
    def _set_alarm(cls, timeout):
        try:
            handler = signal.getsignal(signal.SIGALRM)
            if callable(handler):
                if handler.__name__ == '_set_alarm':
                    raise TimeoutWatchInProgress()
                raise RuntimeError("Existing signal handler found %r" % handler)
            signal.signal(signal.SIGALRM, cls._on_alarm)
            signal.alarm(timeout)
            yield None
        finally:
            signal.signal(signal.SIGALRM, signal.SIG_DFL)

    @classmethod
    def _on_alarm(cls, x, frame):
        raise TimeoutError()


class Environment(RPC):

    def __init__(self, endpoint, conn=None):
        self.endpoint = endpoint
        self._watches = []
        # For watches.
        self._creds = None

        if conn is not None:
            self.conn = conn
        else:
            self.conn = websocket.create_connection(endpoint)

    def close(self):
        for w in self._watches:
            w.stop()
        self.conn.close()

    def login(self, password, user="user-admin"):
        if self.conn and self.conn.connected and self._auth:
            raise AlreadyConnected()
        # Store for constructing separate authenticated watch connections.
        self._creds = {'password': password, 'user': user}
        self._rpc({"Type": "Admin", "Request": "Login",
                   "Params": {"AuthTag": user, "Password": password}})
        self._auth = True

    def info(self):
        return self._rpc({
            "Type": "Client",
            "Request": "EnvironmentInfo"})

    def status(self):
        # Status is currently broken, only reports machine ids.
        return self._rpc({"Type": "Client", "Request": "Status"})

    def get_watch(self, timeout=None):
        # Separate conn per watcher to keep sync usage simple, else we have to
        # buffer watch results with requestid dispatch. At the moment
        # with the all watcher, an app only needs one watch, which likely to
        # change.
        watch_env = Environment(self.endpoint)
        watch_env.login(**self._creds)
        if timeout is not None:
            watcher = TimeoutWatcher(watch_env.conn)
            watcher.set_timeout(timeout)
        else:
            watcher = Watcher(watch_env.conn)
        self._watches.append(watcher)
        watcher.start()
        return watcher

    watch = get_watch

    def get_charm(self, charm_url):
        return self._rpc(
            {"Type": "Client",
             "Request": "CharmInfo",
             "Params": {
                 "CharmURL": charm_url}})

    def _prepare_strparams(self, d):
        r = {}
        for k, v in d.items():
            r[k] = str(v)
        return r

    def _prepare_constraints(self, constraints):
        for k in ['cpu-cores', 'cpu-power', 'mem']:
            if constraints.get(k):
                constraints[k] = int(constraints[k])
        return constraints

    # Relations
    def add_relation(self, endpoint_a, endpoint_b):
        return self._rpc({
            'Type': 'Client',
            'Request': 'AddRelation',
            'Params': {
                'Endpoints': [endpoint_a, endpoint_b]
            }})

    def remove_relation(self, endpoint_a, endpoint_b):
        return self._rpc({
            'Type': 'Client',
            'Request': 'DestroyRelation',
            'Params': {
                'Endpoints': [endpoint_a, endpoint_b]
            }})

    # Service
    def deploy(self, service_name, charm_url, num_units=1, config=None, constraints=None):
        """
        """
        svc_config = {}
        if config:
            svc_config = self._prepare_strparams(config)

        svc_constraints = {}
        if constraints:
            svc_constraints = self._prepare_constraints(constraints)

        return self._rpc(
            {"Type": "Client",
             "Request": "ServiceDeploy",
             "Params": {
                 "ServiceName": service_name,
                 "CharmURL": charm_url,
                 "NumUnits": num_units,
                 "Config": svc_config,
                 "Constraints": svc_constraints}})

    def set_config(self, service_name, config):
        assert isinstance(config, dict)
        svc_config = self._prepare_strparams(config)
        return self._rpc({
            "Type": "Client",
            "Request": "ServiceSet",
            "Params": {
                "ServiceName": service_name,
                "Options": svc_config}})

    def get_config(self, service_name):
        return self._rpc(
            {"Type": "Client",
             "Request": "ServiceGet",
             "Params": {
                 "ServiceName": service_name}})

    def get_constraints(self, service_name):
        return self._rpc(
            {"Type": "Client",
             "Request": "GetServiceConstraints",
             "Params": {
                 "ServiceName": service_name}})

    def set_constraints(self, service_name, constraints):
        return self._rpc(
            {"Type": "Client",
             "Request": "SetServiceConstraints",
             "Params": {
                 "ServiceName": service_name,
                 "Constraints": self._prepare_constraints(constraints)}})

    def destroy_service(self, service_name):
        return self._rpc({
            "Type": "Client",
            "Request": "ServiceDestroy",
            "Params": {
                "ServiceName": service_name}})

    def expose(self, service_name):
        return self._rpc({
            "Type": "Client",
            "Request": "ServiceExpose",
            "Params": {
                "ServiceName": service_name}})

    def unexpose(self, service_name):
        return self._rpc({
            "Type": "Client",
            "Request": "ServiceUnexpose",
            "Params": {
                "ServiceName": service_name}})

    # Units
    def add_units(self, service_name, num_units=1):
        return self._rpc({
            "Type": "Client",
            "Request": "AddServiceUnits",
            "Params": {
                "ServiceName": service_name,
                "NumUnits": num_units}})

    def remove_units(self, unit_names):
        return self._rpc({
            "Type": "Client",
            "Request": "DestroyServiceUnits",
            "Params": {
                "UnitNames": unit_names}})

    def resolved(self, unit_name, retry=False):
        return self._rpc({
            "Type": "Client",
            "Request": "Resolved",
            "Params": {
                "UnitName": unit_name,
                "Retry": retry}})

    # Annotations
    def set_annotation(self, entity, entity_type, annotation):
        """
        Set annotations on an entity.

        Valid entity types are 'service', 'unit', 'machine', 'environment'.
        """
        # valid entity types
        a = self._prepare_strparams(annotation)
        return self._rpc({
            "Type": "Client",
            "Request": "SetAnnotations",
            "Params": {
                "Tag": entity_type + '-' + entity.replace("/", "-"),
                "Pairs": a}})

    def get_annotation(self, entity, entity_type):
        return self._rpc({
            "Type": "Client",
            "Request": "GetAnnotations",
            "Params": {
                "Tag": "%s-%s" % (entity_type, entity.replace("/", "-"))}})


def main():

    import os
    juju_url, juju_token = os.environ.get("JUJU_URL"), os.environ.get("JUJU_TOKEN")
    if not juju_url or not juju_token:
        raise ValueError("JUJU_URL and JUJU_TOKEN should be defined for tests.")
    env = Environment(juju_url)
    env.login(juju_token)
    watcher = env.get_watch(timeout=3)

    print "Env info", env.info()

    for change_set in watcher:
        for change in change_set:
            print "state change", change

    env.deploy("test-blog", "cs:wordpress")
    env.deploy("test-db", "cs:mysql")
    env.add_relation("test-db", "test-blog")
    config = env.get_config("test-blog")

    print "waiting for changes for 30s"
    watcher.set_timeout(30)
    for change_set in watcher:
        for change in change_set:
            print "state change", change

    env.destroy_service('test-blog')
    env.destroy_service('test-db')

if __name__ == '__main__':
    main()
