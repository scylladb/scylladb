====================
Cluster Health Check
====================

.. include:: /operating-scylla/manager/_common/note-versions.rst

Scylla Manager automatically adds a health check task to all new nodes when the cluster is added to the Scylla Manager and to all existing nodes
during the upgrade procedure. You can see the tasks created by the healthcheck when you run
the :ref:`sctool task list <sctool-task-list>` command.

For example:

.. code-block:: none

   sctool task list -c manager-testcluster

returns:

.. code-block:: none

   Cluster: manager-testcluster
   ╭──────────────────────────────────────────────────────┬───────────────────────────────┬──────┬───────────┬────────╮
   │ task                                                 │ next run                      │ ret. │ arguments │ status │
   ├──────────────────────────────────────────────────────┼───────────────────────────────┼──────┼───────────┼────────┤
   │ healthcheck/018da854-b9ff-4e0a-bae7-ca65c677c559     │ 02 Apr 19 18:06:31 UTC (+15s) │ 0    │           │ NEW    │
   │ healthcheck_api/597f237f-103d-4994-8167-3ff591150b7e │ 02 Apr 19 18:07:01 UTC (+1h)  │ 0    │           │ NEW    │
   │ repair/21006f88-0c8c-4e11-9e84-83c319f80d0c          │ 03 Apr 19 00:00:00 UTC (+7d)  │ 3/3  │           │ NEW    │
   ╰──────────────────────────────────────────────────────┴───────────────────────────────┴──────┴───────────┴────────╯

The health check task ensures that CQL native port is accessible on all the nodes. For each node, in parallel,
Scylla Manager opens a connection to a CQL port and asks for server options. If there is no response or the response takes longer than 250 milliseconds, the node is considered to be DOWN otherwise, the node is considered to be UP.
The results are available using the :ref:`sctool status <sctool_status>` command.

For example:

.. code-block:: none

   sctool status -c prod-cluster2

returns:

.. code-block:: none

   Datacenter: dc1
   ╭──────────┬─────┬──────────┬────────────────╮
   │ CQL      │ SSL │ REST     │ Host           │
   ├──────────┼─────┼──────────┼────────────────┤
   │ UP (2ms) │ OFF │ UP (1ms) │ 192.168.100.11 │
   │ UP (1ms) │ OFF │ UP (0ms) │ 192.168.100.12 │
   │ UP (2ms) │ OFF │ UP (0ms) │ 192.168.100.13 │
   ╰──────────┴─────┴──────────┴────────────────╯
   Datacenter: dc2
   ╭──────────┬─────┬──────────┬────────────────╮
   │ CQL      │ SSL │ REST     │ Host           │
   ├──────────┼─────┼──────────┼────────────────┤
   │ UP (2ms) │ OFF │ UP (1ms) │ 192.168.100.21 │
   │ UP (1ms) │ OFF │ UP (1ms) │ 192.168.100.22 │
   │ UP (1ms) │ OFF │ UP (1ms) │ 192.168.100.23 │
   ╰──────────┴─────┴──────────┴────────────────╯

If you have enabled the Scylla Monitoring stack, the Scylla Manager dashboard includes the same cluster status report.
In addition, the Prometheus Alert Manager has an alert to report when a Scylla node health check fails.

Scylla Manager just works!
It reads CQL IP address and port from node configuration and can automatically detect TLS/SSL connection.
There are two types of CQL health check `Credentials agnostic health check`_ and `CQL query health check`_.

.. _manager-2.1-credentials-agnostic-health-check:

Credentials agnostic health check
---------------------------------

Scylla Manager does not require database credentials to work.
CQL health check is based on sending `CQL OPTIONS frame <https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L302>`_ and does not start a CQL session.
This is simple and effective but does not test CQL all the way down.
For that, you may consider upgrading to `CQL query health check`_.

.. _manager-2.1-cql-query-health-check:

CQL query health check
----------------------

You may specify CQL ``username`` and ``password`` flags when adding a cluster to Scylla Manager using :ref:`sctool cluster add command <sctool-cluster-add>`.
It's also possible to add or change that using :ref:`sctool cluster update command <sctool-cluster-update>`.
Once Scylla Manager has CQL credential to the cluster, when performing a health check, it would try to connect to each node and execute ``SELECT now() FROM system.local`` query.
