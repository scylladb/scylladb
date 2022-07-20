==================
Configuration file
==================

.. include:: /operating-scylla/manager/_common/note-versions.rst


Scylla Manager has a single configuration file ``/etc/scylla-manager/scylla-manager.yaml``.
Note that the file will open as read-only unless you edit it as the root user or by using sudo.
Usually, there is no need to edit the configuration file.

HTTP/HTTPS server settings
==========================

With server settings, you may specify if Scylla Manager should be available over HTTP, HTTPS, or both.

.. code-block:: yaml

   # Bind REST API to the specified TCP address using HTTP protocol.
   # http: 127.0.0.1:56080

   # Bind REST API to the specified TCP address using HTTPS protocol.
   https: 127.0.0.1:56443

Prometheus settings
===================

.. code-block:: yaml

   # Bind prometheus API to the specified TCP address using HTTP protocol.
   # By default it binds to all network interfaces, but you can restrict it
   # by specifying it like this 127:0.0.1:56090 or any other combination
   # of ip and port.
   prometheus: ':56090'

If changing prometheus IP or port, please remember to adjust rules in :doc:`prometheus server </operating-scylla/monitoring/2.2/monitoring-stack>`.

.. code-block:: yaml

   - targets:
     - IP:56090

Debug endpoint settings
=======================

In this section, you can specify the pprof debug server address.
It allows you to run profiling on demand on a live application.
By default, the server is running on port ``56112``.

.. code-block:: none

   debug: 127.0.0.1:56112

.. _manager-2.1-logging-settings:

Logging settings
================

Logging settings specify log output and level.

.. code-block:: yaml

   # Logging configuration.
   logger:
     # Where to output logs, syslog or stderr.
     mode: syslog
     # Available log levels are error, info, and debug.
     level: info

Database settings
=================

Database settings allow for :doc:`using a remote cluster <use-a-remote-db>` to store Scylla Manager data.

.. code-block:: yaml

   # Scylla Manager database, used to store management data.
   database:
     hosts:
       - 127.0.0.1
   # Enable or disable client/server encryption.
   #  ssl: false
   #
   # Database credentials.
   #  user: user
   #  password: password
   #
   # Local datacenter name, specify if using a remote, multi-dc cluster.
   #  local_dc:
   #
   # Database connection timeout.
   #  timeout: 600ms
   #
   # Keyspace for management data, for create statement see /etc/scylla-manager/create_keyspace.cql.tpl.
   #  keyspace: scylla_manager
   #  replication_factor: 1

   # Optional custom client/server encryption options.
   #ssl:
   # CA certificate used to validate server cert. If not set, will use he host's root CA set.
   #  cert_file:
   #
   # Verify the hostname and server cert.
   #  validate: true
   #
   # Client certificate and key in PEM format. It has to be provided when
   # client_encryption_options.require_client_auth=true is set on server.
   #  user_cert_file:
   #  user_key_file

Health check settings
=====================

Health check settings let you specify the timeout threshold.
If there is no response from a node after this time period is reached, the :ref:`status <sctool_status>` report (``sctool status``) shows the node as ``DOWN``.

.. code-block:: yaml

   # Healthcheck service configuration.
   #healthcheck:
   # Timeout for CQL status checks.
   #  timeout: 250ms
   #  ssl_timeout: 750ms

Backup settings
===============

Backup settings let you specify backup parameters.

.. code-block:: yaml

   # Backup service configuration.
   #backup:
   # Minimal amount of free disk space required to take a snapshot.
   #  disk_space_free_min_percent: 10
   #
   # Maximal time for a backup run to be considered fresh and can be continued from
   # the same snapshot. If exceeded, a new run with a new snapshot will be created.
   # Zero means no limit.
   #  age_max: 12h

.. _repair-settings:

Repair settings
===============

Repair settings let you specify repair parameters.

.. code-block:: yaml

   # Repair service configuration.
   #repair:
   # Number of segments repaired by Scylla in a single repair command. Increase
   # this value to make repairs faster, note that this may result in increased load
   # on the cluster.
   #  segments_per_repair: 1
   #
   # Maximal number of shards on a host repaired at the same time. By default all
   # shards are repaired in parallel.
   #  shard_parallel_max: 0
   #
   # Maximal allowed number of failed segments per shard. In case of a failure
   # to repair a segment Scylla Manager will try to repair it multiple times
   # depending on the specified number of retries (default 3). If the
   # shard_failed_segments_max limit is exceeded repair task will immediately
   # fail, and the next repair run will start the repair procedure from the beginning.
   #  shard_failed_segments_max: 25
   #
   # In case of an error, hold back repair for the specified amount of time.
   #  error_backoff: 5m
   #
   # Frequency Scylla Manager poll Scylla node for repair command status.
   #  poll_interval: 200ms
   #
   # Maximal time a paused repair is considered fresh and can be continued,
   # if an exceeded repair will start from the beginning. Zero means no limit.
   #  age_max: 0
   #
   # Distribution of data among cores (shards) within a node.
   # Copy value from Scylla configuration file.
   #  murmur3_partitioner_ignore_msb_bits: 12
