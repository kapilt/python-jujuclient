Juju Client
-----------

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

