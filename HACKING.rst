Juju Client Hacking Tips
========================

Running tests
-------------

In order to run the tests a Juju environment must be bootstrapped manually for
the tests to interact.  Do the following to run against an ec2 environment::

    $ juju bootstrap -e ec2

After it is booted, set the dns-name of machine 0 to be JUJU_ENDPOINT::

    $ export JUJU_ENDPOINT=`juju status | \
      grep dns-name | awk -F': ' '{print "wss://"$2":17070"}'`

You must also set JUJU_AUTH to be the admin-secret from the file in
$JUJU_HOME/environments/ec2

Once those two environment variables are set you can run the tests with::

    $ python test_jujuclient.py
