
Monitoring Interfaces
=====================

Scylla exposes two interfaces for online monitoring, as described below

Prometheus
----------
By default, Scylla listen on port 9180 for `Prometheus <https://prometheus.io/>`_ requests. To connect a Prometheus server to scylla in your prometheus.yaml configuration file, add scylla as a target with :code:`your-ip:9180`

For more information on monitoring Scylla with Prometheus see :doc:`Scylla Monitoring Stack <monitoring-stack/>`

You can change Prometheus listening address and port in scylla.yaml file

.. code-block:: yaml

   # prometheus port
   # By default, Scylla opens prometheus API port on port 9180
   # setting the port to 0 will disable the prometheus API.
   prometheus_port: 9180
   #
   # prometheus address
   # By default, Scylla binds all interfaces to the prometheus API
   # It is possible to restrict the listening address to a specific one
   prometheus_address: 0.0.0.0

Collectd
--------

Starting from Scylla version 2.3 `scyllatop <http://www.scylladb.com/2016/03/22/scyllatop/>`_ will not use Collectd any more.

By default, Scylla send metrics to a local `Collectd <https://collectd.org/>`_ process, allowing you to watch Scylla status with `scyllatop <http://www.scylladb.com/2016/03/22/scyllatop/>`_. Scylla can also sends metric over Collectd protocol to external Collectd server, `Graphite <http://graphite.wikidot.com/>`_ or similar tools. To forward metrics to external server, update :code:`/etc/collectd.d/scylla.conf` to work as a proxy:

.. code-block:: apacheconf

   LoadPlugin network
   <Plugin "network">
     Listen "127.0.0.1" "25826"
     Server "remote-ip" "25826"
     Forward true
   </Plugin>

Where :code:`remote-ip` is the IP of your external Collectd server. Make sure to keep other elements of the file as is. Restart the collectd server for the new configuration to apply :code:`sudo service collectd restart`

To change sample rate of Scylla metrics, update the :code:`SCYLLA_ARGS` line in :code:`/etc/sysconfig/scylla-server` file, parameter :code:`--collectd-poll-period 3000` (number in ms)


