
Monitoring Interfaces
=====================

Scylla exposes two interfaces for online monitoring, as described below

Prometheus
----------
By default, Scylla listens on port 9180 for `Prometheus <https://prometheus.io/>`_ requests. To connect a Prometheus server to Scylla in your prometheus.yaml configuration file, add Scylla as a target with :code:`your-ip:9180`

For more information on monitoring Scylla with Prometheus see :doc:`Scylla Monitoring Stack <monitoring-stack>`.

You can change the Prometheus listening address and port in scylla.yaml file

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
