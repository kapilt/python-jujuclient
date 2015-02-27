"""
Juju Client
-----------

A simple synchronous python client for the juju-core websocket api.

Supports python 2.7 & 3.4+.

See README for example usage.
"""
# License: GPLv3
# Author: Kapil Thangavelu <kapil.foss@gmail.com>

from base64 import b64encode
from contextlib import contextmanager
import copy
import errno
import json
import logging
import os
import pprint
import shutil
import signal
import socket
import ssl
import stat
import tempfile
import time
import urllib
import warnings
import zipfile

import websocket

# py 2 and py 3 compat
try:
    from httplib import HTTPSConnection
    from StringIO import StringIO
except ImportError:
    from http.client import HTTPSConnection
    from io import StringIO


# There are two pypi modules with the name websocket (python-websocket
# and websocket) We utilize python-websocket, sniff and error if we
# find the wrong one.
try:
    websocket.create_connection
except AttributeError:
    raise RuntimeError(
        "Expected 'python-websocket' egg "
        "found incompatible gevent 'websocket' egg")


websocket.logger = logging.getLogger("websocket")

log = logging.getLogger("jujuclient")


TAG_PREFIXES = (
    "action", "charm", "disk",
    "environment", "machine", "network",
    "relation", "service", "unit", "user")


class EnvironmentNotBootstrapped(Exception):

    def __init__(self, environment):
        self.environment = environment

    def __str__(self):
        return "Environment %s is not bootstrapped" % self.environment


class AlreadyConnected(Exception):
    pass


class LoginRequired(Exception):
    pass


class TimeoutError(StopIteration):
    pass


class TimeoutWatchInProgress(Exception):
    pass


class UnitErrors(Exception):

    def __init__(self, errors):
        self.errors = errors


class EnvError(Exception):

    def __init__(self, error):
        self.error = error
        self.message = error['Error']
        # Call the base class initializer so that this exception can be pickled
        # (see http://bugs.python.org/issue1692335).
        super(EnvError, self).__init__(error)

    def __str__(self):
        stream = StringIO()
        pprint.pprint(self.error, stream, indent=4)
        return "<Env Error - Details:\n %s >" % (
            stream.getvalue())


class Jobs(object):
    HostUnits = "JobHostUnits"
    ManageEnviron = "JobManageEnviron"
    ManageState = "JobManageState"


class Connector(object):
    """Abstract out the details of connecting to state servers.

    Covers
    - finding state servers, credentials, certs for a named env.
    - verifying state servers are listening
    - connecting an environment or websocket to a state server.
    """

    retry_conn_errors = (errno.ETIMEDOUT, errno.ECONNREFUSED, errno.ECONNRESET)

    def run(self, cls, env_name):
        """Given an environment name, return an authenticated client to it."""
        jhome, data = self.parse_env(env_name)
        cert_dir = os.path.join(jhome, 'jclient')
        if not os.path.exists(cert_dir):
            os.mkdir(cert_dir)
        cert_path = self.write_ca(cert_dir, env_name, data)
        address = self.get_state_server(data)
        if not address:
            return
        return self.connect_env(
            cls, address, env_name, data['user'], data['password'],
            cert_path, data.get('environ-uuid'))

    def connect_env(self, cls, address, name, user, password,
                    cert_path=None, env_uuid=None):
        """Given environment info return an authenticated client to it."""
        endpoint = "wss://%s" % address
        if env_uuid:
            endpoint += "/environment/%s/api" % env_uuid
        env = cls(endpoint, name=name, ca_cert=cert_path, env_uuid=env_uuid)
        if not user.startswith('user-'):
            user = "user-%s" % user
        env.login(user=user, password=password)
        return env

    @classmethod
    def connect_socket(cls, endpoint, cert_path=None):
        """Return a websocket connection to an endpoint."""

        sslopt = cls.get_ssl_config(cert_path)
        return websocket.create_connection(
            endpoint, origin=endpoint, sslopt=sslopt)

    @staticmethod
    def get_ssl_config(cert_path=None):
        sslopt = {'ssl_version': ssl.PROTOCOL_TLSv1}
        if cert_path:
            sslopt['ca_certs'] = cert_path
            # ssl.match_hostname is broken for us, need to disable per
            # https://github.com/liris/websocket-client/issues/105
            # when that's available, we can just selectively disable
            # the host name match, for now we have to disable cert
            # checking :-(
            sslopt['check_hostname'] = False
        else:
            sslopt['cert_reqs'] = ssl.CERT_NONE
        return sslopt

    def connect_socket_loop(self, endpoint, cert_path=None, timeout=120):
        """Retry websocket connections to an endpoint till its connected."""
        t = time.time()
        while (time.time() > t + timeout):
            try:
                return Connector.connect_socket(endpoint, cert_path)
            except socket.error as err:
                if not err.errno in self.retry_conn_errors:
                    raise
                time.sleep(1)
                continue

    def write_ca(self, cert_dir, cert_name, data):
        """Write ssl ca to the given."""
        cert_path = os.path.join(cert_dir, '%s-cacert.pem' % cert_name)
        with open(cert_path, 'w') as ca_fh:
            ca_fh.write(data['ca-cert'])
        return cert_path

    def get_state_server(self, data):
        """Given a list of state servers, return one that's listening."""
        found = False
        for s in data['state-servers']:
            if self.is_server_available(s):
                found = True
                break
        if not found:
            return
        return s

    def parse_env(self, env_name):
        import yaml
        jhome = os.path.expanduser(
            os.environ.get('JUJU_HOME', '~/.juju'))
        jenv = os.path.join(jhome, 'environments', '%s.jenv' % env_name)
        if not os.path.exists(jenv):
            raise EnvironmentNotBootstrapped(env_name)

        with open(jenv) as fh:
            data = yaml.safe_load(fh.read())
            return jhome, data

    def is_server_available(self, server):
        """ Given address/port, return true/false if it's up """
        address, port = server.split(":")
        try:
            socket.create_connection((address, port), 3)
            return True
        except socket.error as err:
            if err.errno in self.retry_conn_errors:
                return False
            else:
                raise


class LogIterator(object):

    def __init__(self, conn):
        self.conn = conn

    def __iter__(self):
        return self

    def next(self):
        try:
            return self.conn.recv()
        except websocket.WebSocketConnectionClosedException:
            self.conn.close()
            raise StopIteration()
        except Exception:
            self.conn.close()
            raise

    __next__ = next


class RPC(object):

    _auth = False
    _request_id = 0
    _debug = False
    _reconnect_params = None
    conn = None

    def _rpc(self, op):
        if not self._auth and not op.get("Request") == "Login":
            raise LoginRequired()
        if not 'Params' in op:
            op['Params'] = {}
        op['RequestId'] = self._request_id
        self._request_id += 1
        if self._debug:
            log.debug("rpc request:\n%s" % (json.dumps(op, indent=2)))
        self.conn.send(json.dumps(op))
        raw = self.conn.recv()
        result = json.loads(raw)
        if self._debug:
            log.debug("rpc response:\n%s" % (json.dumps(result, indent=2)))

        if 'Error' in result:
            # The backend disconnects us on err, bug: http://pad.lv/1160971
            self.conn.connected = False
            raise EnvError(result)
        return result['Response']

    def login(self, password, user="user-admin"):
        """Login gets shared to watchers for reconnect."""
        if self.conn and self.conn.connected and self._auth:
            raise AlreadyConnected()
        # Store for constructing separate authenticated watch connections.
        self._creds = {'password': password, 'user': user}
        result = self._rpc(
            {"Type": "Admin", "Request": "Login",
             "Params": {"AuthTag": user, "Password": password}})
        self._auth = True
        self._info = copy.deepcopy(result)
        return result

    def set_reconnect_params(self, params):
        self._reconnect_params = params

    def reconnect(self):
        if self.conn:
            self._auth = False
            self.conn.close()
        if not self._reconnect_params:
            return False

        log.info("Reconnecting client")
        self.conn = Connector.connect_socket_loop(
            self._reconnect_params['url'],
            self._reconnect_params['ca_cert'])
        self.login(self._reconnect_params['password'],
                   self._reconnect_params['user'])
        return True


class Watcher(RPC):

    _auth = True

    def __init__(self, conn, auto_reconnect=True):
        self.conn = conn
        self.watcher_id = None
        self.running = False
        self.auto_reconnect = auto_reconnect
        # For debugging, attach the wrapper
        self.context = None

    def start(self):
        result = self._rpc({
            'Type': 'Client',
            'Request': 'WatchAll',
            'Params': {}})
        self.watcher_id = result['AllWatcherId']
        self.running = True
        return result

    def next(self):
        if self.watcher_id is None:
            self.start()
        if not self.running:
            raise StopIteration("Stopped")
        try:
            result = self._rpc({
                'Type': 'AllWatcher',
                'Request': 'Next',
                'Id': self.watcher_id})
        except EnvError as e:
            if "state watcher was stopped" in e.message:
                if not self.auto_reconnect:
                    raise
                if not self.reconnect():
                    raise
                return next(self)
            raise
        return result['Deltas']

    # py3 compat
    __next__ = next

    def reconnect(self):
        self.watcher_id = None
        self.running = False
        return super(Watcher, self).reconnect()

    def stop(self):
        if not self.conn.connected:
            return
        try:
            result = self._rpc({
                'Type': 'AllWatcher', 'Request': 'Stop',
                'Id': self.watcher_id})
        except (EnvError, socket.error):
            # We're about to close the connection.
            result = None
        self.conn.close()
        self.watcher_id = None
        self.running = False
        return result

    def set_context(self, context):
        self.context = context
        return self

    def __iter__(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc, v, t):
        self.stop()


class TimeoutWatcher(Watcher):
    # A simple non concurrent watch using signals..

    _timeout = None

    def set_timeout(self, timeout):
        self._timeout = timeout

    def next(self):
        with self._set_alarm(self._timeout):
            return super(TimeoutWatcher, self).next()

    # py3 compat
    __next__ = next

    @classmethod
    @contextmanager
    def _set_alarm(cls, timeout):
        try:
            handler = signal.getsignal(signal.SIGALRM)
            if callable(handler):
                if handler.__name__ == '_set_alarm':
                    raise TimeoutWatchInProgress()
                raise RuntimeError(
                    "Existing signal handler found %r" % handler)
            signal.signal(signal.SIGALRM, cls._on_alarm)
            signal.alarm(timeout)
            yield None
        finally:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, signal.SIG_DFL)

    @classmethod
    def _on_alarm(cls, x, frame):
        raise TimeoutError()


class Environment(RPC):
    """A client to a juju environment."""

    def __init__(self, endpoint, name=None, conn=None,
                 ca_cert=None, env_uuid=None):
        self.name = name
        self.endpoint = endpoint
        self._watches = []
        # For watches.
        self._creds = None
        self._ca_cert = ca_cert
        self._info = None
        self._env_uuid = env_uuid

        if conn is not None:
            self.conn = conn
        else:
            self.conn = Connector.connect_socket(endpoint, self._ca_cert)

    def close(self):
        """Close the connection and any extant associated watches."""
        for w in self._watches:
            w.stop()
        if self.conn.connected:
            self.conn.close()

    @classmethod
    def connect(cls, env_name):
        """Connect and login to the named environment."""
        return Connector().run(cls, env_name)

    @property
    def uuid(self):
        return self._env_uuid

    @property
    def tag(self):
        return "environment-%s" % self._env_uuid

    def login(self, password, user="user-admin"):
        """Login to the environment.
        """
        result = super(Environment, self).login(
            password, user)
        negotiate_facades(self, self._info)
        return result

    def _http_conn(self):
        endpoint = self.endpoint.replace('wss://', '')
        host, remainder = endpoint.split(':', 1)
        port, _ = remainder.split('/', 1)
        conn = HTTPSConnection(host, port)
        headers = {
            'Authorization': 'Basic %s' % b64encode(
                '%(user)s:%(password)s' % (self._creds))}
        path = ""
        if self._env_uuid:
            path = "/environment/%s" % self._env_uuid
        return conn, headers, path

    # Charm ops / see charm facade for listing charms in environ.
    def add_local_charm_dir(self, charm_dir, series):
        """Add a local charm to the environment.

        This will automatically generate an archive from
        the charm dir and then add_local_charm.
        """
        fh = tempfile.NamedTemporaryFile()
        CharmArchiveGenerator(charm_dir).make_archive(fh.name)
        with fh:
            return self.add_local_charm(
                fh, series, os.stat(fh.name).st_size)

    def add_local_charm(self, charm_file, series, size=None):
        """Add a local charm to an environment.

        Uses an https endpoint at the same host:port as the wss.
        Supports large file uploads.
        """
        conn, headers, path_prefix = self._http_conn()
        path = "%s/charms?series=%s" % (path_prefix, series)
        headers['Content-Type'] = 'application/zip'
        # Specify if its a psuedo-file object,
        # httplib will try to stat non strings.
        if size:
            headers['Content-Length'] = size
        conn.request("POST", path, charm_file, headers)
        response = conn.getresponse()
        result = json.loads(response.read())
        if not response.status == 200:
            raise EnvError(result)
        return result

    def add_charm(self, charm_url):
        """Add a charm store charm to the environment.

        Example::

          >>> env.add_charm('cs:trusty/mysql-6')
          {}

        charm_url must be a fully qualifed charm url, including
        series and revision.
        """
        return self._rpc(
            {"Type": "Client",
             "Request": "AddCharm",
             "Params": {"URL": charm_url}})

    def resolve_charms(self, names):
        """Resolve an ambigious charm name via the charm store.

        Note this does not resolve the charm revision, only series.

        >>> env.resolve_charms(['rabbitmq-server', 'mysql'])
        {u'URLs': [{u'URL': u'cs:trusty/rabbitmq-server'},
                   {u'URL': u'cs:trusty/mysql'}]}
        """
        if not isinstance(names, (list, tuple)):
            names = [names]
        return self._rpc(
            {"Type": "Client",
             "Request": "ResolveCharms",
             "Params": {"References": names}})

    def download_charm(self, charm_url, path=None, fh=None):
        """Download a charm from the env to the given path or file like object.

        Returns the integer size of the downloaded file::

          >>> env.add_charm('cs:~hazmat/trusty/etcd-6')
          >>> env.download_charm('cs:~hazmat/trusty/etcd-6', 'etcd.zip')
          3649263
        """
        if not path and not fh:
            raise ValueError("Must provide either path or fh")
        conn, headers, path_prefix = self._http_conn()
        url_path = "%s/charms?file=*&url=%s" % (path_prefix, charm_url)
        if path:
            fh = open(path, 'wb')
        with fh:
            conn.request("GET", url_path, "", headers)
            response = conn.getresponse()
            if response.status != 200:
                raise EnvError({"Error": response.read()})
            shutil.copyfileobj(response, fh, 2 ** 15)
            size = fh.tell()
        return size

    def get_charm(self, charm_url):
        """Get information about a charm in the environment.

        Example::

          >>> env.get_charm('cs:~hazmat/trusty/etcd-6')

         {u'URL': u'cs:~hazmat/trusty/etcd-6',
            u'Meta': {
                u'Peers': {
                    u'cluster': {u'Name': u'cluster', u'Limit': 1, u'Scope':
                    u'global', u'Interface': u'etcd-raft', u'Role': u'peer',
                    u'Optional': False}},
                u'OldRevision': 0,
                u'Description': u"...",
                u'Format': 1, u'Series': u'', u'Tags': None, u'Storage': None,
                u'Summary': u'A distributed key value store for configuration',
                u'Provides': {u'client': {
                    u'Name':u'client', u'Limit': 0, u'Scope': u'global',
                    u'Interface':u'etcd', u'Role': u'provider',
                    u'Optional': False}},
                u'Subordinate': False, u'Requires': None, u'Categories': None,
                u'Name': u'etcd'},
                u'Config': {u'Options': {
                    u'debug': {
                        u'Default': True, u'Type': u'boolean',
                        u'Description': u'Enable debug logging'},
              u'snapshot': {u'Default': True, u'Type': u'boolean',
              u'Description': u'Enable log snapshots'}}},
           u'Actions': {u'ActionSpecs': None},
           u'Revision': 6}
        """
        return self._rpc(
            {"Type": "Client",
             "Request": "CharmInfo",
             "Params": {
                 "CharmURL": charm_url}})

    # Environment operations
    def info(self):
        """Return information about the environment.

        >>> env.info()
        {u'ProviderType': u'manual',
         u'UUID': u'96b7d32a-3c54-4885-836c-98359028a604',
         u'DefaultSeries': u'trusty',
         u'Name': u'ocean'}
        """
        return self._rpc({
            "Type": "Client",
            "Request": "EnvironmentInfo"})

    def status(self, filters=()):
        """Return the state of the environment.

        Includes information on machines, services, relations, and units
        in the environment.

        filters can be specified as a sequence of names to focus on
        particular services or units.

        Note this only loosely corresponds to cli status output format.
        """
        if not isinstance(filters, (list, tuple)):
            filters = [filters]
        return self._rpc({
            'Type': 'Client',
            'Request': 'FullStatus',
            'Params': {'Patterns': filters}})

    def get_env_constraints(self):
        """Get the default constraints associated to the environment.

        >>> env.get_env_constraints()
        {u'Constraints': {}}
        """
        return self._rpc({
            "Type": "Client",
            "Request": "GetEnvironmentConstraints"})

    def set_env_constraints(self, constraints):
        """Set the default environment constriants.
        """
        return self._rpc({
            "Type": "Client",
            "Request": "SetEnvironmentConstraints",
            "Params": {"ServiceName": "",
                       "Constraints": self._prepare_constraints(constraints)}})

    def get_env_config(self):
        """Get the environment configuration.

        >>> env.get_env_config()['Config'].keys()
        [u'rsyslog-ca-cert',
        u'enable-os-refresh-update', u'firewall-mode',
        u'logging-config', u'enable-os-upgrade',
        u'bootstrap-retry-delay', u'default-series',
        u'bootstrap-user', u'uuid', u'lxc-clone-aufs',
        u'admin-secret', u'set-numa-control-policy', u'agent-version',
        u'disable-network-management', u'ca-private-key', u'type',
        u'bootstrap-timeout', u'development', u'block-remove-object',
        u'tools-metadata-url', u'api-port', u'storage-listen-ip',
        u'block-destroy-environment', u'image-stream',
        u'block-all-changes', u'authorized-keys',
        u'ssl-hostname-verification', u'state-port',
        u'storage-auth-key', u'syslog-port', u'use-sshstorage',
        u'image-metadata-url', u'bootstrap-addresses-delay', u'name',
        u'charm-store-auth', u'agent-metadata-url', u'ca-cert',
        u'test-mode', u'bootstrap-host', u'storage-port',
        u'prefer-ipv6', u'proxy-ssh']
        """
        return self._rpc({
            "Type": "Client",
            "Request": "EnvironmentGet"})

    def set_env_config(self, config):
        """Update the environment configuration with the given mapping.

        *Note* that several of these properties are read-only or
        configurable only at a boot time.
        """
        return self._rpc({
            "Type": "Client",
            "Request": "EnvironmentSet",
            "Params": {"Config": config}})

    def unset_env_config(self, keys):
        """Reset the given environment config to the default juju values.
        """
        return self._rpc({
            "Type": "Client",
            "Request": "EnvironmentUnset",
            "Params": {"Keys": keys}})

    def set_env_agent_version(self, version):
        """Upgrade an environment to the given agent version."""
        return self._rpc({
            "Type": "Client",
            "Request": "SetEnvironAgentVersion",
            "Params": {"Version": version}})

    def agent_version(self):
        """Return the agent version of the juju api server/env."""
        return self._rpc({
            "Type": "Client",
            "Request": "AgentVersion",
            "Params": {}})

    def find_tools(self, major=0, minor=0, series="", arch=""):
        return self._rpc({
            "Type": "Client",
            "Request": "FindTools",
            "Params": {
                "MajorVersion": int(major),
                "MinorVersion": int(minor),
                "Arch": arch,
                "Series": series}})

    def debug_log(self, include_entity=(), include_module=(),
                  exclude_entity=(), exclude_module=(), limit=0,
                  back_log=0, level=None, replay=False):
        """Return an iterator over juju logs in the environment.

        >>> logs = env.debug_log(back_log=10, limit=20)
        >>> for l in logs: print l
        """
        d = {}
        if include_entity:
            d['includeEntity'] = include_entity
        if include_module:
            d['includeModule'] = include_module
        if exclude_entity:
            d['excludeEntity'] = exclude_entity
        if exclude_module:
            d['excludeModule'] = exclude_module
        if limit:
            d['maxLines'] = limit
        if level:
            d['level'] = level
        if back_log:
            d['backlog'] = back_log
        if replay:
            d['replay'] = str(bool(replay)).lower()

        # ca cert for ssl cert validation if present.
        cert_pem = os.path.join(
            os.path.expanduser(
                os.environ.get('JUJU_HOME', '~/.juju')),
            'jclient', "%s.pem" % self.name)
        if not os.path.exists(cert_pem):
            cert_pem = None
        sslopt = Connector.get_ssl_config(cert_pem)

        p = urllib.urlencode(d)
        headers = [
            'Authorization: Basic %s' % b64encode(
                '%(user)s:%(password)s' % (self._creds))]

        if self._env_uuid:
            url = self.endpoint.rsplit('/', 1)[0]
            url += "/log"
        else:
            url = self.endpoint + "/log"

        if p:
            url = "%s?%s" % (url, p)

        conn = websocket.create_connection(
            url, origin=self.endpoint, sslopt=sslopt, header=headers)

        # Error message if any is pre-pended.
        result = json.loads(conn.recv())
        if result['Error']:
            conn.close()
            raise EnvError(result)

        return LogIterator(conn)

    def run_on_all_machines(self, command, timeout=None):
        """Run the given shell command on all machines in the environment."""
        return self._rpc({
            "Type": "Client",
            "Request": "RunOnAllMachines",
            "Params": {"Commands": command,
                       "Timeout": timeout}})

    def run(self, targets, command, timeout=None):
        """Run a shell command on the targets (services, units, or machines).
        """
        return self._rpc({
            "Type": "Client",
            "Request": "Run",
            "Params": {"Commands": command,
                       "Timeout": timeout}})

    # Machine ops
    def add_machine(self, series="", constraints=None,
                    machine_spec="", parent_id="", container_type=""):
        """Allocate a new machine from the iaas provider or create a container
        on an existing machine.
        """
        if machine_spec:
            err_msg = "Cant specify machine spec with container_type/parent_id"
            assert not (parent_id or container_type), err_msg
            parent_id, container_type = machine_spec.split(":", 1)

        params = dict(
            Series=series,
            Constraints=self._prepare_constraints(constraints),
            ContainerType=container_type,
            ParentId=parent_id,
            Jobs=[Jobs.HostUnits])
        return self.add_machines([params])['Machines'][0]

    def add_machines(self, machines):
        """Allocate multiple machines from the iaas provider.

        See add_machine for format of parameters.
        """
        return self._rpc({
            "Type": "Client",
            "Request": "AddMachines",
            "Params": {
                "MachineParams": machines}})

    def register_machine(self, instance_id, nonce, series, hardware, addrs):
        """Register/Enlist a machine into an environment state.

        The machine will need to have tools installed and subsequently
        connect to the state server with the given nonce
        credentials. The machine_config method can be used to
        construct a suitable set of commands.

        Parameters:

          - nonce: is the initial password for the new machine.
          - addrs: list of ip addresses for the machine.
          - hw: is the hardware characterstics of the machine, applicable keys.
            - Arch
            - Mem
            - RootDisk size
            - CpuCores
            - CpuPower
            - Tags
        """
        params = dict(
            Series=series,
            InstanceId=instance_id,
            Jobs=[Jobs.HostUnits],
            HardwareCharacteristics=hardware,
            Addrs=addrs,
            Nonce=nonce)
        return self.register_machines([params])['Machines'][0]

    def register_machines(self, machines):
        """Register a set of machines see :method:register_machine."""
        return self._rpc({
            "Type": "Client",
            "Request": "InjectMachines",
            "Params": {
                "MachineParams": machines}})

    def destroy_machines(self, machine_ids, force=False):
        """Remove the given machines from the environment.

        Will also deallocate from them from the iaas provider.

        If :param: force is provided then the machine and
        units on it will be forcibly destroyed without waiting
        for hook execution state machines.
        """
        params = {"MachineNames": machine_ids}
        if force:
            params["Force"] = True
        return self._rpc({
            "Type": "Client",
            "Request": "DestroyMachines",
            "Params": params})

    def provisioning_script(self, machine_id, nonce,
                            data_dir="/var/lib/juju", disable_apt=False):
        """Return a shell script to initialize a machine as part of the env.

        Used inconjunction with :method:register_machine for 'manual' provider
        style machines.

        Common use is to provide this as userdata.
        """
        return self._rpc({
            "Type": "Client",
            "Request": "ProvisioningScript",
            "Params": {
                "MachineId": machine_id,
                "Nonce": nonce,
                "DataDir": data_dir,
                "DisablePackageCommands": disable_apt}})

    def machine_config(self, machine_id, series, arch):
        """Return information needed to render cloudinit for a machine.
        """
        return self._rpc({
            "Type": "Client",
            "Request": "MachineConfig",
            "Params": {
                "MachineId": machine_id,
                "Series": series,
                "Arch": arch}})

    def retry_provisioning(self, machines):
        """Mark machines for provisioner to retry iaas provisioning.

        If provisioning failed for a transient reason, this method can
        be utilized to retry provisioning for the given machines.
        """
        return self._rpc({
            "Type": "Client",
            "Request": "RetryProvisioning",
            "Params": {
                "Entities": [{"Tag": "machine-%s"} for x in machines]}})

    # Watch Wrapper methods
    def get_stat(self):
        """DEPRECATED: A status emulator using the watch api, returns
        immediately.
        """
        warnings.warn(
            "get_stat is deprecated, use status()", DeprecationWarning)
        watch = self.get_watch()
        return StatusTranslator().run(watch)

    def wait_for_units(
            self, timeout=None, goal_state="started", callback=None):
        """Wait for all units to reach a given state.

        Any unit errors will cause an exception to be raised.
        """
        watch = self.get_watch(timeout)
        return WaitForUnits(watch, goal_state).run(callback)

    def wait_for_no_machines(self, timeout, callback=None):
        """For unit tests doing teardowns, or deployer during reset.
        """
        watch = self.get_watch(timeout)
        return WaitForNoMachines(watch).run(callback)

    def get_watch(self, timeout=None, connection=None, watch_class=None):
        """Get a watch connection to observe changes to the environment.
        """
        # Separate conn per watcher to keep sync usage simple, else we have to
        # buffer watch results with requestid dispatch. At the moment
        # with the all watcher, an app only needs one watch, which is likely to
        # change to discrete watches on individual bits.
        if connection is None:
            watch_env = Environment(self.endpoint)
            watch_env.login(**self._creds)
        else:
            watch_env = connection

        p = dict(self._creds)
        p.update({
            'url': self.endpoint,
            'origin': self.endpoint,
            'ca_cert': self._ca_cert})
        if timeout is not None:
            if watch_class is None:
                watch_class = TimeoutWatcher
            watcher = watch_class(watch_env.conn)
            watcher.set_timeout(timeout)
        else:
            if watch_class is None:
                watch_class = Watcher
            watcher = watch_class(watch_env.conn)
        watcher.set_reconnect_params(p)
        self._watches.append(watcher)
        watcher.start()
        return watcher

    watch = get_watch

    def _prepare_strparams(self, d):
        r = {}
        for k, v in list(d.items()):
            r[k] = str(v)
        return r

    def _prepare_constraints(self, constraints):
        for k in ['cpu-cores', 'cpu-power', 'mem']:
            if constraints.get(k):
                constraints[k] = int(constraints[k])
        return constraints

    # Relations
    def add_relation(self, endpoint_a, endpoint_b):
        """Add a relation between two endpoints."""
        return self._rpc({
            'Type': 'Client',
            'Request': 'AddRelation',
            'Params': {
                'Endpoints': [endpoint_a, endpoint_b]
            }})

    def remove_relation(self, endpoint_a, endpoint_b):
        """Remove a relation between two endpoints."""
        return self._rpc({
            'Type': 'Client',
            'Request': 'DestroyRelation',
            'Params': {
                'Endpoints': [endpoint_a, endpoint_b]
            }})

    # Service
    def deploy(self, service_name, charm_url, num_units=1,
               config=None, constraints=None, machine_spec=None):
        """Deploy a service.

        To use with local charms, the charm must have previously
        been added with a call to add_local_charm or add_local_charm_dir.
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
                 "Constraints": svc_constraints,
                 "ToMachineSpec": machine_spec}})

    def set_config(self, service_name, config):
        """Set a service's configuration."""
        assert isinstance(config, dict)
        svc_config = self._prepare_strparams(config)
        return self._rpc({
            "Type": "Client",
            "Request": "ServiceSet",
            "Params": {
                "ServiceName": service_name,
                "Options": svc_config}})

    def unset_config(self, service_name, config_keys):
        """Unset configuration values of a service to restore charm defaults.
        """
        return self._rpc({
            "Type": "Client",
            "Request": "ServiceUnset",
            "Params": {
                "ServiceName": service_name,
                "Options": config_keys}})

    def set_charm(self, service_name, charm_url, force=False):
        """Set the charm url for a service.
        """
        return self._rpc({
            "Type": "Client",
            "Request": "ServiceSetCharm",
            "Params": {
                "ServiceName": service_name,
                "CharmUrl": charm_url,
                "Force": force}})

    def get_service(self, service_name):
        """Returns dict of Charm, Config, Constraints, Service keys.

        Charm -> charm used by service
        Service -> service name
        Config -> Currently configured options and descriptions
        Constraints -> Constraints set on service (not environment inherited).
        """
        return self._rpc(
            {"Type": "Client",
             "Request": "ServiceGet",
             "Params": {
                 "ServiceName": service_name}})

    def get_config(self, service_name):
        """Returns service configuration.
        """
        return self.get_service(service_name)['Config']

    def get_constraints(self, service_name):
        return self._rpc(
            {"Type": "Client",
             "Request": "GetServiceConstraints",
             "Params": {
                 "ServiceName": service_name}})['Constraints']

    def set_constraints(self, service_name, constraints):
        return self._rpc(
            {"Type": "Client",
             "Request": "SetServiceConstraints",
             "Params": {
                 "ServiceName": service_name,
                 "Constraints": self._prepare_constraints(constraints)}})

    def update_service(self, service_name, charm_url="", force_charm_url=False,
                       min_units=None, settings=None, constraints=None):
        """Update a service.

        Can update a service's charm, modify configuration, constraints,
        and the minimum number of units.
        """
        svc_config = {}
        if settings:
            svc_config = self._prepare_strparams(settings)

        return self._rpc(
            {"Type": "Client",
             "Request": "ServiceUpdate",
             "Params": {
                 "ServiceName": service_name,
                 "CharmUrl": charm_url,
                 "MinUnits": min_units,
                 "SettingsStrings": svc_config,
                 "Constraints": self._prepare_constraints(constraints)}})

    def destroy_service(self, service_name):
        """Destroy a service and all of its units.

        On versions of juju 1.22+ this will also deallocate the iaas
        machine resources those units where assigned to if they
        where the only unit residing on the machine.
        """
        return self._rpc({
            "Type": "Client",
            "Request": "ServiceDestroy",
            "Params": {
                "ServiceName": service_name}})

    def expose(self, service_name):
        """Provide external access to a given service.

        Will manipulate the iaas layer's firewall machinery
        to enabmle public access from outside of the environment.
        """
        return self._rpc({
            "Type": "Client",
            "Request": "ServiceExpose",
            "Params": {
                "ServiceName": service_name}})

    def unexpose(self, service_name):
        """Remove external access to a given service.

        Will manipulate the iaas layer's firewall machinery
        to disable public access from outside of the environment.
        """
        return self._rpc({
            "Type": "Client",
            "Request": "ServiceUnexpose",
            "Params": {
                "ServiceName": service_name}})

    def valid_relation_names(self, service_name):
        """All possible relation names of a service.

        Per its charm metadata.
        """
        return self._rpc({
            "Type": "Client",
            "Request": "ServiceCharmRelations",
            "Params": {
                "ServiceName": service_name}})

    # Units
    def add_units(self, service_name, num_units=1):
        """Add n units of a given service.

        Machines will be allocated from the iaas provider
        or unused machines in the environment that
        match the service's constraints.
        """
        return self._rpc({
            "Type": "Client",
            "Request": "AddServiceUnits",
            "Params": {
                "ServiceName": service_name,
                "NumUnits": num_units}})

    def add_unit(self, service_name, machine_spec=None):
        """Add a unit of the given service

        Optionally with placement onto a given existing
        machine or a new container.
        """
        params = {
            "ServiceName": service_name,
            "NumUnits": 1}
        if machine_spec:
            params["ToMachineSpec"] = machine_spec
        return self._rpc({
            "Type": "Client",
            "Request": "AddServiceUnits",
            "Params": params})

    def remove_units(self, unit_names):
        """Remove the given service units.
        """
        return self._rpc({
            "Type": "Client",
            "Request": "DestroyServiceUnits",
            "Params": {
                "UnitNames": unit_names}})

    def resolved(self, unit_name, retry=False):
        """Mark a unit's error as resolved, optionally retrying hook execution.
        """
        return self._rpc({
            "Type": "Client",
            "Request": "Resolved",
            "Params": {
                "UnitName": unit_name,
                "Retry": retry}})

    # Multi-context

    def get_public_address(self, target):
        # Return the public address of the machine or unit.
        return self._rpc({
            "Type": "Client",
            "Request": "PublicAddress",
            "Params": {
                "Target": target}})

    def get_private_address(self, target):
        """Return the public address of the machine or unit.
        """
        return self._rpc({
            "Type": "Client",
            "Request": "PrivateAddress",
            "Params": {
                "Target": target}})

    # Annotations
    def set_annotation(self, entity, entity_type, annotation):
        """
        Set annotations on an entity.

        Valid entity types for this method are 'service', 'unit',
        'machine', 'environment'.

        Use the annotation facade if available as it supports more
        entities, and setting and getting values enmass.
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


def negotiate_facades(env, server_info):
    """Auto-negotiate api facades available based on server login information.

    This annotates facades instances directly onto env/client as well as
    a 'facades' mapping of facade name to version & attribute.
    """
    facade_map = {}
    for factory in APIFacade.__subclasses__():
        facade_map[factory.name] = factory

    facades = {}
    for fenv in server_info.get('Facades', []):
        factory = facade_map.get(fenv['Name'])
        if factory is None:
            continue
        matched_version = False
        for v in fenv['Versions']:
            if v in factory.versions:
                matched_version = v
        if matched_version is False:
            continue
        f = factory(env, matched_version)
        setattr(env, f.key, f)
        facades[f.name] = {'attr': f.key, 'version': matched_version}
    setattr(env, 'facades', facades)


class APIFacade(object):

    def __init__(self, env, version=None):
        self.env = env
        if version is None:
            version = self.versions[-1]
        self.version = version

    def _format_tag(self, name):
        return {'Tag': self._format_tag_name(name)}

    def _format_tag_name(self, name):
        for n in TAG_PREFIXES:
            if name.startswith("%s-" % n):
                return name
        if name.isdigit():
            return "machine-%s" % name
        if name.startswith('cs:') or name.startswith('local:'):
            return "charm-%s" % name
        if '/' in name:
            return "unit-%s" % name
        if '@' in name:
            return "user-%s" % name
        else:
            raise ValueError("Could not guess entity tag for %s" % name)

    def _format_user_names(self, names):
        """reformat a list of usernames as user tags."""
        if not isinstance(names, (list, tuple)):
            names = [names]
        r = []
        for n in names:
            n = self._format_user_tag(n)
            r.append({'Tag': n})
        return r

    def _format_user_tag(self, n):
        if not n.startswith('user-'):
            n = "user-%s" % n
        if not '@' in n:
            n = "%s@local" % n
        return n

    def rpc(self, op):
        if not 'Type' in op:
            op['Type'] = self.name
        if not 'Version' in op:
            op['Version'] = self.version
        return self.env._rpc(op)


class UserManager(APIFacade):
    key = "users"
    name = "UserManager"
    versions = [0]

    def add(self, users):
        """
        param users: a list structure with each element corresponding to
        a dict with keys for 'username', 'display-name', 'password'.
        alternatively a dict to add a single user.

        Example::

          >>> env.users.add({'username': 'mike',
                      'display-name': 'Michael Smith',
                      'password': 'zebramoon'})
          {u'results': [{u'tag': u'user-mike@local'}]}
        """
        if isinstance(users, dict):
            users = [users]
        return self.rpc({
            "Request": "AddUser",
            "Params": {"Users": users}})

    def enable(self, names):
        """
        params names: list of usernames to enable or disable.
        """
        return self.rpc({
            "Request": "EnableUser",
            "Params": {"Entities": self._format_user_names(names)}})

    def disable(self, names):
        """
        params names: list of usernames to enable or disable.
        """
        return self.rpc({
            "Request": "DisableUser",
            "Params": {"Entities": self._format_user_names(names)}})

    def list(self, names=(), disabled=True):
        """List information about the given users in the environment.

        If no names are passed then return information about all users.

        Example::

          >>> env.users.list()
          {u'results': [{u'result': {
                u'username': u'admin',
                u'last-connection':
                u'2015-01-20T14:45:47Z',
                u'disabled': False,
                u'date-created': u'2015-01-19T22:12:35Z',
                u'display-name': u'admin',
                u'created-by': u'admin'}}]}

        also re disabled see bug on includedisabled on core.
        """
        return self.rpc({
            "Request": "UserInfo",
            "Params": {"Entities": self._format_user_names(names),
                       "IncludeDisabled": disabled}})

    def set_password(self, entities):
        """params entities:  a list of dictionaries with
        'username' and 'password', alternatively a single
        dicitonary.
        """
        if isinstance(entities, dict):
            entities = [entities]

        users = []

        for e in entities:
            if not 'username' in e or not 'password' in e:
                raise ValueError(
                    "Invalid parameter for set password %s" % entities)
            users.append(
                {'Tag': self._format_user_tag(e['username']),
                 'Password': e['password']})

        return self.rpc({
            "Request": "SetPassword",
            "Params": {"Changes": users}})


class Charms(APIFacade):
    """Access information about charms extant in the environment.

    Note Currently broken per bug: http://pad.lv/1414086
    """
    key = "charms"
    name = "Charms"
    versions = [1]

    def info(self, charm_url):
        """Retrieve information about a charm in the environment.

        Charm url must be fully qualified.

        >>> env.charms.info('cs:~hazmat/trusty/etcd-6')
        """
        return self.rpc({
            "Request": "CharmInfo",
            "Params": {"CharmURL": charm_url}})

    def list(self, names=()):
        """Retrieve all charms with the given names or all charms.

        >>> env.charms.list('etcd')

        """
        if not isinstance(names, (list, tuple)):
            names = [names]
        return self.rpc({
            "Request": "List",
            "Params": {"Names": names}})


class Annotations(APIFacade):
    """Get and set annotations enmass on entities.

    Note Currently broken per bug: http://pad.lv/1414086
    """
    key = "annotations"
    name = "Annotations"
    versions = [1]

    def get(self, names):
        """Get annotations on a set of names.

        Names can be a singelton or list, ideally in tag format (type-$id) ala
        unit-mysql/0, machine-22, else this method will attempt to introspect
        $id and utilize the appropriate type prefix to construct a tag.

        Note the tag format for the environment itself uses the environment
        uuid.

        >>> env.annotations.get(['cs:~hazmat/trusty/etcd-6'])
        {u'Results': [{u'EntityTag': u'charm-cs:~hazmat/trusty/etcd-6',
                       u'Annotations': {u'vcs': u'bzr'},
                       u'Error': {u'Error': None}}]}
        """
        if not isinstance(names, (list, tuple)):
            names = [names]
        entities = map(self._format_tag, names)

        return self.rpc({
            'Request': 'Get',
            'Params': {
                'Entities': entities}})

    def set(self, annotations):
        """Set annotations on a set of entities.

        Format is a sequence of sequences (name, annotation_dict)

        Entity tag format is inferred if possible.

        >>> env.annotations.set([
            ('charm-cs:~hazmat/trusty/etcd-6', {'vcs': 'bzr'})])
        {u'Results': []}

        >>> env.annotations.set([('mysql', {'x': 1, 'y': 2})])
        {u'Results': []}
        """
        e_a = []
        for a in annotations:
            if not isinstance(a, (list, tuple)) and len(a) == 2:
                raise ValueError(
                    "Annotation values should be a list/tuple"
                    "of name, dict %s" % a)
            n, d = a
            if not isinstance(d, dict):
                raise ValueError(
                    "Annotation values should be a list/tuple"
                    "of name, dict %s" % a)
            e_a.append({'EntityTag': self._format_tag_name(n),
                        'Annotations': d})
        return self.rpc({
            'Request': 'Set',
            'Params': {'Annotations': e_a}})


class KeyManager(APIFacade):
    """
    Note: Key management implementation is work in progress atm, the
    api is scoped at a user level but the implementation allows for
    global access to all users. ie. any key added has root access to the
    environment for all users.
    """
    key = "keys"
    name = "KeyManager"
    versions = [0]

    def list(self, names, mode=True):
        """ Return a set of ssh keys or fingerprints for the given users.

        Mode: is a boolean, true is show the full key, false for fingerprints.

        >>> env.keys.list('user-admin', mode=False)
         {u'Results': [
             {u'Result': [u'42:d1:22:a4:f3:38:b2:e8:ce... (juju-system-key)']]
              u'Error': None}]}
        """
        return self.rpc({
            "Request": "ListKeys",
            "Params": {"Entities": self._format_user_names(names),
                       "Mode": mode}})

    def add(self, user, keys):
        return self.rpc({
            "Request": "ListKeys",
            "Params": {"User": user,
                       "Keys": keys}})

    def delete(self, user, keys):
        """Remove the given ssh keys for the given user.

        Key parameters pass in should correspond to fingerprints or comment.
        """
        return self.rpc({
            "Request": "DeleteKeys",
            "Params": {"User": user,
                       "Keys": keys}})

    def import_keys(self, user, keys):
        """Import env user's keys using ssh-import-id.

        >>> env.keys.import_keys('admin', 'gh:kapilt')
        """
        return self.rpc({
            "Request": "ImportKeys",
            "Params": {"User": user,
                       "Keys": keys}})


class Backups(APIFacade):
    key = "backups"
    name = "Backups"
    versions = [0]

    def create(self, notes):
        """Create in this client is synchronous. It returns after
        the backup is taken.

        >>> env.backups.create('abc')
        {u'Machine': u'0',
        u'Version': u'1.23-alpha1.1',
        u'Started': u'2015-01-22T18:05:30.014657514Z',
        u'Checksum': u'nDAiKQmhrpiB2W5n/OijqUJtGYE=',
        u'ChecksumFormat': u'SHA-1, base64 encoded',
        u'Hostname': u'ocean-0',
        u'Environment': u'28d91a3d-b50d-4549-80a1-165fe1cc62db',
        u'Finished': u'2015-01-22T18:05:38.11633437Z',
        u'Stored': u'2015-01-22T18:05:42Z',
        u'Notes': u'abc',
        u'ID': u'20150122-180530.28d91a3d-b50d-4549-80a1-165fe1cc62db',
        u'Size': 17839021}
        """
        return self.rpc({
            "Request": "Create",
            "Params": {"Notes": notes}})

    def info(self, backup_id):
        """Get info on a given backup. Given all backup info is returned
        on 'list' this method is exposed just for completeness.

        >>> env.backups.info(
            ...   "20150122-180530.28d91a3d-b50d-4549-80a1-165fe1cc62db")
        {u'Checksum': u'nDAiKQmhrpiB2W5n/OijqUJtGYE=',
        u'ChecksumFormat': u'SHA-1, base64 encoded',
        u'Environment': u'28d91a3d-b50d-4549-80a1-165fe1cc62db',
        u'Finished': u'2015-01-22T18:05:38Z',
        u'Hostname': u'ocean-0',
        u'ID': u'20150122-180530.28d91a3d-b50d-4549-80a1-165fe1cc62db',
        u'Machine': u'0',
        u'Notes': u'abc',
        u'Size': 17839021,
        u'Started': u'2015-01-22T18:05:30Z',
        u'Stored': u'2015-01-22T18:05:42Z',
        u'Version': u'1.23-alpha1.1'}
         """
        return self.rpc({
            "Request": "Info",
            "Params": {"Id": backup_id}})

    def list(self):
        """ List all the backups and their info.

        >>> env.backups.list()
        {u'List': [{u'Checksum': u'nDAiKQmhrpiB2W5n/OijqUJtGYE=',
            u'ChecksumFormat': u'SHA-1, base64 encoded',
            u'Environment': u'28d91a3d-b50d-4549-80a1-165fe1cc62db',
            u'Finished': u'2015-01-22T18:05:38Z',
            u'Hostname': u'ocean-0',
            u'ID': u'20150122-180530.28d91a3d-b50d-4549-80a1-165fe1cc62db',
            u'Machine': u'0',
            u'Notes': u'abc',
            u'Size': 17839021,
            u'Started': u'2015-01-22T18:05:30Z',
            u'Stored': u'2015-01-22T18:05:42Z',
            u'Version': u'1.23-alpha1.1'}]}
        """
        return self.rpc({
            "Request": "List",
            "Params": {}})

    def remove(self, backup_id):
        """ Remove the given backup.
        >>> env.backups.remove(
        ...    '20150122-181136.28d91a3d-b50d-4549-80a1-165fe1cc62db')
        {}
        """
        return self.rpc({
            "Request": "Remove",
            "Params": {"Id": backup_id}})

    def download(self, backup_id, path=None, fh=None):
        """ Download the given backup id to the given path or file handle.

        TODO:
         - Progress callback (its chunked encoding so don't know size)
           bug on core to send size: http://pad.lv/1414021
         - Checksum validation ('digest' header has sha checksum)
        """
        if fh is None and path is None:
            raise ValueError("Please specify path or file")
        conn, headers, path_prefix = self.rpc._http_conn()
        headers['Content-Type'] = 'application/json'
        if path:
            fh = open(path, 'wb')
        with fh:
            url_path = "%s/backups" % path_prefix
            conn.request(
                "GET", url_path, json.dumps({"ID": backup_id}), headers)
            response = conn.getresponse()
            if response.status != 200:
                raise EnvError({"Error": response.read()})
            shutil.copyfileobj(response, fh, 2 ** 15)
            size = fh.tell()
        return size

    # No restore in trunk yet, so waiting.
    def upload(self):
        raise NotImplementedError()


class ImageManager(APIFacade):
    """Find information about the images available to a given environment.

    This information ultimately derives from the simple streams image metadata
    for the environment.
    """
    key = "images"
    name = "ImageManager"
    versions = [1]

    def list(self, image_specs=()):
        """List information about the matching images.

        image_spec = {'Kind': 'kind', 'Series': 'trusty', 'Arch': 'amd64'}
        """
        if not isinstance(image_specs, (list, tuple)):
            image_specs = [image_specs]
        return self.rpc({
            'Request': 'ListImages',
            'Params': {'Images': image_specs}})

    def delete(self, image_specs):
        """Delete the specified image

        image_spec = {'Kind': 'kind', 'Series': 'trusty', 'Arch': 'amd64'}
        """
        if not isinstance(image_specs, (list, tuple)):
            image_specs = [image_specs]

        return self.rpc({
            'Request': 'DeleteImages',
            'Params': {'Images': image_specs}})


class HA(APIFacade):
    """Manipulate the ha properties of an environment.
    """
    key = "ha"
    name = "HighAvailability"
    versions = [1]

    def ensure_availability(self, num_state_servers, series=None,
                            constraints=None, placement=None):
        """Enable multiple state servers on machines.

        Note placement api is specifically around instance placement, ie
        it can specify zone or maas name. Existing environment machines
        can't be designated state servers :-(
        """
        return self.rpc({
            'Request': 'EnsureAvailability',
            'Params': {
                'EnvironTag': "environment-%s" % self._env_uuid,
                'NumStateServers': int(num_state_servers),
                'Series': series,
                'Constraints': self._format_constraints(constraints),
                'Placement': placement}})


class Actions(APIFacade):
    """Api to interact with charm defined operations.

    See https://juju.ubuntu.com/docs/actions.html for more details.
    """
    key = "actions"
    name = "Action"
    versions = [0]

    # Query services for available action definitions
    def service_actions(self, services):
        """Return available actions for the given services.
        """
        return self.rpc({
            "Request": "ServicesCharmActions",
            "Params": {
                "Entities": self._format_receivers(services)}})

    def enqueue_units(self, units, action_name, params):
        """Enqueue an action on a set of units."""
        if not isinstance(units, (list, tuple)):
            units = [units]
        actions = []
        for u in units:
            if not u.startswith('unit-'):
                u = "unit-%s" % u
            actions.append({
                'Tag': '',
                'Name': action_name,
                'Receiver': u,
                'Params': params})
        return self._enqueue(actions)

    def _enqueue(self, actions):
        return self.rpc({
            'Request': 'Enqueue',
            'Params': {'Actions': actions}})

    def cancel(self, action_ids):
        """Cancel a pending action by id."""
        return self.rpc({
            'Request': 'Cancel',
            "Params": {
                "Entities": action_ids}})

    # Query action info
    def info(self, action_ids):
        """Return information on a set of actions."""
        return self.rpc({
            'Request': 'Actions',
            'Params': {
                'Entities': action_ids}})

    def find(self, prefixes):
        """Find actions by prefixes on their ids...
        """
        if not isinstance(prefixes, (list, tuple)):
            prefixes = [prefixes]
        return self.rpc({
            "Request": "FindActionTagsByPrefix",
            "Params": {
                "Prefixes": prefixes}})

    # Query actions instances by receiver.
    def all(self, receivers):
        """Return all actions for the given receivers."""
        return self.rpc({
            'Request': 'ListAll',
            "Params": {
                "Entities": self._format_receivers(receivers)}})

    def pending(self, receivers):
        """Return all pending actions for the given receivers."""
        return self.rpc({
            'Request': 'ListPending',
            "Params": {
                "Entities": self._format_receivers(receivers)}})

    def completed(self, receivers):
        """Return all completed actions for the given receivers."""
        return self.rpc({
            'Request': 'ListCompleted',
            "Params": {
                "Entities": self._format_receivers(receivers)}})

    def _format_receivers(self, names):
        if not isinstance(names, (list, tuple)):
            names = [names]
        receivers = []
        for n in names:
            if n.startswith('unit-') or n.startswith('service-'):
                pass
            elif '/' in n:
                n = "unit-%s" % n
            else:
                n = "service-%s" % n
            receivers.append({"Tag": n})
        return receivers


class EnvironmentManager(APIFacade):
    """Create multiple environments within a state server.

    ***Jan 2015 - Note MESS is still under heavy development
    and under a feature flag, api is likely not stable.***
    """

    key = "mess"
    name = "EnvironmentManager"
    versions = [1]

    def create(self, owner, account, config):
        """Create a new logical environment within a state server.
        """
        return self.rpc({
            'Request': 'CreateEnvironment',
            'Params': {'OwnerTag': self._format_user_tag(owner),
                       'Account': account,
                       'Config': config}})

    def list(self, owner):
        """List environments available to the given user.

        >>> env.mess.list('user-admin')
        {u'Environments': [
            {u'OwnerTag': u'user-admin@local',
             u'Name': u'ocean',
             u'UUID': u'f8947ad0-c592-48d9-86d1-d948ec90f6cd'}]}
        """
        return self.rpc({
            'Request': 'ListEnvironments',
            'Params': {'Tag': self._format_user_tag(owner)}})


# Unit tests for the watch wrappers are in lp:juju-deployer...
class WatchWrapper(object):

    def __init__(self, watch):
        self.watch = watch

    def run(self, callback=None):
        seen_initial = False
        with self.watch.set_context(self):
            for change_set in self.watch:
                for change in change_set:
                    self.process(*change)
                    if seen_initial and callable(callback):
                        callback(*change)
                if self.complete() is True:
                    self.watch.stop()
                    break
                seen_initial = True

    def process(self):
        """process watch events."""

    def complete(self):
        """watch wrapper complete """


class WaitForUnits(WatchWrapper):
    """
    Wait for units of the environment to reach a particular goal state.
    """
    def __init__(self, watch, state='started', service=None):
        super(WaitForUnits, self).__init__(watch)
        self.units = {}
        self.goal_state = state
        self.service = service

    def process(self, entity_type, change, data):
        if entity_type != "unit":
            return
        if change == "remove" and data['Name'] in self.units:
            del self.units[data['Name']]
        else:
            self.units[data['Name']] = data

    def complete(self):
        state = {'pending': [], 'errors': []}
        for k, v in list(self.units.items()):
            if v['Status'] == "error":
                state['errors'] = [v]
            elif v['Status'] != self.goal_state:
                state['pending'] = [v]
        if not state['pending'] and not state['errors']:
            return True
        if state['errors'] and not self.goal_state == "removed":
            raise UnitErrors(state['errors'])
        return state['pending']


class WaitForNoMachines(WatchWrapper):
    """
    Wait for all non state servers to be terminated.
    """

    def __init__(self, watch):
        super(WaitForNoMachines, self).__init__(watch)
        self.machines = {}

    def process(self, entity_type, change, data):
        if entity_type != 'machine':
            return
        if change == 'remove' and data['Id'] in self.machines:
            del self.machines[data['Id']]
        else:
            self.machines[data['Id']] = data

    def complete(self):
        if list(self.machines.keys()) == ['0']:
            return True


class StatusTranslator(object):
    """
    Status emulation from watch api.
    """

    key_map = {
        'InstanceId': 'instance-id',
        'PublicAddress': 'public-address',
        'Status': 'agent-state',
        "MachineId": "Machine",
        'CharmURL': 'charm',
        'StatusInfo': 'agent-state-info',
        "Number": 'port',
        "Ports": "open-ports"
    }
    remove_keys = set(['Life', "PrivateAddress", "MinUnits"])
    skip_empty_keys = set(['StatusInfo', "Ports"])

    def run(self, watch):
        self.data = {'machines': {}, 'services': {}}
        with watch:
            change_set = next(watch)
            for change in change_set:
                entity_type, change_kind, d = change
                if entity_type == "machine":
                    self._machine(d)
                elif entity_type == "service":
                    self._service(d)
                elif entity_type == "unit":
                    self._unit(d)
                elif entity_type == "relation":
                    self._relation(d)
        result = dict(self.data)
        self.data.clear()
        return result

    def _translate(self, d):
        r = {}
        for k, v in list(d.items()):
            if k in self.remove_keys:
                continue
            if k in self.skip_empty_keys and not v:
                continue
            tk = self.key_map.get(k, k)

            r[tk.lower()] = v
        return r

    def _machine(self, d):
        mid = d.pop('Id')
        self.data.setdefault('machines', {})[mid] = self._translate(d)

    def _unit(self, d):
        svc_units = self.data.setdefault("services", {}).setdefault(
            d['Service'], {}).setdefault('units', {})
        d.pop("Service")
        d.pop("Series")
        d.pop("CharmURL")
        name = d.pop('Name')
        ports = d.pop('Ports')
        tports = d.setdefault('Ports', [])
        # Workaround for lp:1425435
        if ports:
            for p in ports:
                tports.append("%s/%s" % (p['Number'], p['Protocol']))
        svc_units[name] = self._translate(d)

    def _service(self, d):
        d.pop('Config')
        d.pop('Constraints')
        name = d.pop('Name')
        svc = self.data.setdefault('services', {}).setdefault(name, {})
        svc.update(self._translate(d))

    def _relation(self, d):
        d['Endpoints'][0]['RemoteService'] = d['Endpoints'][0]['ServiceName']
        if len(d['Endpoints']) != 1:
            d['Endpoints'][1]["RemoteService"] = d[
                'Endpoints'][0]['ServiceName']
            d['Endpoints'][0]["RemoteService"] = d[
                'Endpoints'][1]['ServiceName']
        for ep in d['Endpoints']:
            svc_rels = self.data.setdefault(
                'services', {}).setdefault(
                    ep['ServiceName'], {}).setdefault('relations', {})
            svc_rels.setdefault(
                ep['Relation']['Name'], []).append(ep['RemoteService'])


class CharmArchiveGenerator(object):

    def __init__(self, path):
        self.path = path

    def make_archive(self, path):
        """Create archive of directory and write to ``path``.
        :param path: Path to archive
        Ignored
        - build/* - This is used for packing the charm itself and any
                    similar tasks.
        - */.*    - Hidden files are all ignored for now.  This will most
                    likely be changed into a specific ignore list (.bzr, etc)
        """
        zf = zipfile.ZipFile(path, 'w', zipfile.ZIP_DEFLATED)
        for dirpath, dirnames, filenames in os.walk(self.path):
            relative_path = dirpath[len(self.path) + 1:]
            if relative_path and not self._ignore(relative_path):
                zf.write(dirpath, relative_path)
            for name in filenames:
                archive_name = os.path.join(relative_path, name)
                if not self._ignore(archive_name):
                    real_path = os.path.join(dirpath, name)
                    self._check_type(real_path)
                    if os.path.islink(real_path):
                        self._check_link(real_path)
                        self._write_symlink(
                            zf, os.readlink(real_path), archive_name)
                    else:
                        zf.write(real_path, archive_name)
        zf.close()
        return path

    def _check_type(self, path):
        """Check the path
        """
        s = os.stat(path)
        if stat.S_ISDIR(s.st_mode) or stat.S_ISREG(s.st_mode):
            return path
        raise ValueError("Invalid Charm at % %s" % (
            path, "Invalid file type for a charm"))

    def _check_link(self, path):
        link_path = os.readlink(path)
        if link_path[0] == "/":
            raise ValueError(
                "Invalid Charm at %s: %s" % (
                    path, "Absolute links are invalid"))
        path_dir = os.path.dirname(path)
        link_path = os.path.join(path_dir, link_path)
        if not link_path.startswith(os.path.abspath(self.path)):
            raise ValueError(
                "Invalid charm at %s %s" % (
                    path, "Only internal symlinks are allowed"))

    def _write_symlink(self, zf, link_target, link_path):
        """Package symlinks with appropriate zipfile metadata."""
        info = zipfile.ZipInfo()
        info.filename = link_path
        info.create_system = 3
        # Magic code for symlinks / py2/3 compat
        # 27166663808 = (stat.S_IFLNK | 0755) << 16
        info.external_attr = 2716663808
        zf.writestr(info, link_target)

    def _ignore(self, path):
        if path == "build" or path.startswith("build/"):
            return True
        if path.startswith('.'):
            return True
