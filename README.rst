Juju Client
-----------

A simple synchronous python client for the juju-core/gojuju websocket api.


Example Usage
=============

  ::

   from jujuclient import Environment

   # Simple connection method by env name, parses local jenv file from client
   env = Environment.connect('env-name')

   # By hand
   env = Environment('wss://address:port')
   env.login('password')

   # create a watch, watches create their own connection in support of simple
   # synchronous usage.
   watcher = env.watch()

   # deploy some services
   env.deploy('loadbalancer', 'cs:precise/haproxy')
   env.deploy('db', 'cs:precise/mysql')
   env.deploy('blog', 'cs:precise/wordpress')

   # add some relations
   env.add_relation('blog', 'db')
   env.add_relation('blog', 'loadbalancer')

   # expose a service
   env.expose('loadbalancer')

   # config & constraints settings
   env.set_config('blog', {'engine': 'apache'})
   env.get_config('blog')
   env.set_constraints('blog', {'cpu-cores': 4})

   # add/remove units
   env.add_units('blog', 4)
   env.remove_units(['blog/0'])

   # service dstruction
   env.destroy_service('blog')

   # watch the events
   for change_set in watcher:
       for change in change_set:
           print change


Facade Usage
============

Juju provides for multiple apis behind a single endpoint using with different apis
segmentations known as 'facades', each facade is versioned independently. This client
provides for facade auto negotiation to best available matching client version and login
capabilities which are directly annotated on to the env instance.

The extant facades implemented by this client for 1.23 are Charms, KeyManager, UserManager,
HighAvailability, Action, and Annotations.

Example usage::

   >>> pprint.pprint(env.facades)

   {'Action': {'attr': 'actions', 'version': 0},
    'Annotations': {'attr': 'annotations', 'version': 1},
    'Backups': {'attr': 'backups', 'version': 0},
    'Charms': {'attr': 'charms', 'version': 1},
    'EnvironmentManager': {'attr': 'mess', 'version': 1},
    'HighAvailability': {'attr': 'ha', 'version': 1},
    'ImageManager': {'attr': 'images', 'version': 1},
    'KeyManager': {'attr': 'keys', 'version': 0},
    'UserManager': {'attr': 'users', 'version': 0}}

   >>> env.charms.list()
   {'CharmURLs': ['cs:~hazmat/trusty/etcd-6']}

   >>> env.users.list()
   {'results': [{'result': {
        'username': 'admin',
        'date-created': '2015-01-27T14:55:22Z',
        'disabled': False,
        'created-by': 'admin',
        'last-connection': '2015-01-27T20:42:33Z',
        'display-name': 'admin'}}]}


