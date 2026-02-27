Administration Guide
********************

For training material, also check out the `Admin Procedures lesson <https://university.scylladb.com/courses/scylla-operations/lessons/admin-procedures-and-basic-monitoring/>`_ on Scylla University.

System requirements
===================
Make sure you have met the :doc:`System Requirements </getting-started/system-requirements>`  before you install and configure Scylla. 

Download and Install
====================

See the :doc:`getting started page </getting-started/index>` for info on installing Scylla on your platform.


System configuration
====================
See :ref:`System Configuration Guide <system-configuration-files-and-scripts>` for details on optimum OS settings for Scylla. (These settings are performed automatically in the Scylla packages, Docker containers, and Amazon AMIs.)

.. _admin-scylla-configuration:

Scylla Configuration
====================
Scylla configuration files are:

+-------------------------------------------------------+---------------------------------+
| Installed location                                    | Description                     |
+=======================================================+=================================+
| :code:`/etc/default/scylla-server` (Ubuntu/Debian)    | Server startup options          |
| :code:`/etc/sysconfig/scylla-server` (others)         |                                 |
+-------------------------------------------------------+---------------------------------+
| :code:`/etc/scylla/scylla.yaml`                       | Main Scylla configuration file  |
+-------------------------------------------------------+---------------------------------+
| :code:`/etc/scylla/cassandra-rackdc.properties`       | Rack & dc configuration file    |
+-------------------------------------------------------+---------------------------------+

.. _check-your-current-version-of-scylla:

Check your current version of Scylla
------------------------------------
This command allows you to check your current version of Scylla. Note that this command is not the :doc:`nodetool version </operating-scylla/nodetool-commands/version>` command which reports the CQL version.
If you are looking for the CQL or Cassandra version, refer to the CQLSH reference for :ref:`SHOW VERSION <cqlsh-show-version>`.

.. code-block:: shell

   scylla --version

Output displays the Scylla version. Your results may differ.

.. code-block:: shell

   4.4.0-0.20210331.05c6a40f0

.. _admin-address-configuration-in-scylla:

Address Configuration in Scylla
-------------------------------

The following addresses can be configured in scylla.yaml:

.. list-table::
   :widths: 30 70
   :header-rows: 1

   * - Address Type
     - Description
   * - listen_address
     - Address Scylla listens for connections from other nodes. See storage_port and ssl_storage_ports.
   * - rpc_address
     - Address on which Scylla is going to expect Thrift and CQL client connections. See rpc_port, native_transport_port and native_transport_port_ssl in the :ref:`Networking <cqlsh-networking>` parameters.
   * - broadcast_address
     - Address that is broadcasted to tell other Scylla nodes to connect to. Related to listen_address above.
   * - broadcast_rpc_address
     - Address that is broadcasted to tell the clients to connect to. Related to rpc_address.
   * - seeds
     - The broadcast_addresses of the existing nodes. You must specify the address of at least one existing node.
   * - endpoint_snitch
     - Node's address resolution helper.
   * - api_address
     - Address for REST API requests. See api_port in the :ref:`Networking <cqlsh-networking>` parameters.
   * - prometheus_address
     - Address for Prometheus queries. See prometheus_port in the :ref:`Networking <cqlsh-networking>` parameters and `ScyllaDB Monitoring Stack <https://monitoring.docs.scylladb.com/stable/>`_ for more details.
   * - replace_node_first_boot
     - Host ID of a dead node this Scylla node is replacing. Refer to :doc:`Replace a Dead Node in a Scylla Cluster </operating-scylla/procedures/cluster-management/replace-dead-node>` for more details.

.. note:: When the listen_address, rpc_address, broadcast_address, and broadcast_rpc_address parameters are not set correctly, Scylla does not work as expected.

scylla-server
-------------
The :code:`scylla-server` file contains configuration related to starting up the Scylla server.

.. _admin-scylla.yaml:

.. include:: /operating-scylla/scylla-yaml.inc

.. _admin-compression:

Compression
-----------

In Scylla, you can configure compression at rest and compression in transit.
For compression in transit, you can configure compression between nodes or between the client and the node.


.. _admin-client-node-compression:

Client - Node Compression
^^^^^^^^^^^^^^^^^^^^^^^^^^

Compression between the client and the node is set by the driver that the application is using to access Scylla.

For example:

* `Scylla Python Driver <https://python-driver.docs.scylladb.com/master/api/cassandra/cluster.html#cassandra.cluster.Cluster.compression>`_
* `Scylla Java Driver <https://github.com/scylladb/java-driver/tree/3.7.1-scylla/manual/compression>`_
* `Go Driver <https://godoc.org/github.com/gocql/gocql#Compressor>`_

Refer to the :doc:`Drivers Page </using-scylla/drivers/index>` for more drivers.

.. _internode-compression:

Internode Compression
^^^^^^^^^^^^^^^^^^^^^

Internode compression is configured in the scylla.yaml

internode_compression controls whether traffic between nodes is compressed.

* all  - all traffic is compressed.
* dc   - traffic between different datacenters is compressed.
* none - nothing is compressed (default).

Configuring TLS/SSL in scylla.yaml
----------------------------------

Scylla versions 1.1 and greater support encryption between nodes and between client and node. See the Scylla :doc:`Scylla TLS/SSL guide: </operating-scylla/security/index>` for configuration settings.

.. _cqlsh-networking:

Networking
----------

The ScyllaDB ports are detailed in the table below. For ScyllaDB Manager ports, see the `Scylla Manager Documentation <https://manager.docs.scylladb.com/>`_.

.. image:: /operating-scylla/security/Scylla-Ports2.png

.. include:: /operating-scylla/_common/networking-ports.rst

All ports above need to be open to external clients (CQL), external admin systems (JMX), and other nodes (RPC). REST API port can be kept closed for incoming external connections.

The JMX service, :code:`scylla-jmx`, runs on port 7199. It is required in order to manage Scylla using :code:`nodetool` and other Apache Cassandra-compatible utilities. The :code:`scylla-jmx` process must be able to connect to port 10000 on localhost. The JMX service listens for incoming JMX connections on all network interfaces on the system.

Advanced networking
-------------------
It is possible that a client, or another node, may need to use a different IP address to connect to a Scylla node from the address that the node is listening on. This is the case when a node is behind port forwarding. Scylla allows for setting alternate IP addresses.

Do not set any IP address to :code:`0.0.0.0`.

.. list-table::
   :widths: 30 40 30
   :header-rows: 1

   * - Address Type
     - Description
     - Default
   * - listen_address (required)
     - Address Scylla listens for connections from other nodes. See storage_port and ssl_storage_ports.
     - No default
   * - rpc_address (required)
     - Address on which Scylla is going to expect Thrift and CQL clients connections. See rpc_port, native_transport_port and native_transport_port_ssl in the :ref:`Networking <cqlsh-networking>` parameters.
     - No default
   * - broadcast_address
     - Address that is broadcasted to tell other Scylla nodes to connect to. Related to listen_address above.
     - listen_address
   * - broadcast_rpc_address
     - Address that is broadcasted to tell the clients to connect to. Related to rpc_address.
     - rpc_address


If other nodes can connect directly to :code:`listen_address`, then :code:`broadcast_address` does not need to be set.

If clients can connect directly to :code:`rpc_address`, then :code:`broadcast_rpc_address` does not need to be set.

.. note:: For tips and examples on how to configure these addresses, refer to :doc:`How to Properly Set Address Values in scylla.yaml </kb/yaml-address>`

.. _admin-core-dumps:

Core dumps
----------
On RHEL and CentOS, the `Automatic Bug Reporting Tool <https://abrt.readthedocs.io/en/latest/>`_ conflicts with Scylla coredump configuration. Remove it before installing Scylla: :code:`sudo yum remove -y abrt`

Scylla places any core dumps in :code:`var/lib/scylla/coredump`. They are not visible with the :code:`coredumpctl` command. See the :doc:`System Configuration Guide </getting-started/system-configuration/>` for details on core dump configuration scripts. Check with Scylla support before sharing any core dump, as they may contain sensitive data.

Schedule fstrim
===============

Scylla sets up daily fstrim on the filesystem(s),
containing your Scylla commitlog and data directory. This utility will
discard, or trim, any blocks no longer in use by the filesystem.

Experimental Features
=====================

Scylla Open Source uses experimental flags to expose non-production-ready features safely. These features are not stable enough to be used in production, and their API will likely change, breaking backward or forward compatibility.

In recent Scylla versions, these features are controlled by the ``experimental_features`` list in scylla.yaml, allowing one to choose which experimental to enable.
For example, some of the experimental features in Scylla Open Source 4.5 are: ``udf``, ``alternator-streams`` and ``raft``.
Use ``scylla --help`` to get the list of experimental features.

Scylla Enterprise and Scylla Cloud do not officially support experimental Features.

Monitoring
==========
Scylla exposes interfaces for online monitoring, as described below.

Monitoring Interfaces
---------------------

`Scylla Monitoring Interfaces <https://monitoring.docs.scylladb.com/stable/reference/monitoring_apis.html>`_

Monitoring Stack
----------------

|mon_root|

JMX
---
Scylla JMX is compatible with Apache Cassandra, exposing the relevant subset of MBeans.

.. REST

.. include:: /operating-scylla/rest.rst

Un-contents
-----------

Scylla is designed for high performance before tuning, for fewer layers that interact in unpredictable ways, and to use better algorithms that do not require manual tuning. The following items are found in the manuals for other data stores but do not need to appear here.

Configuration un-contents
^^^^^^^^^^^^^^^^^^^^^^^^^

* Generating tokens
* Configuring virtual nodes

Operations un-contents
^^^^^^^^^^^^^^^^^^^^^^

* Tuning Bloom filters
* Data caching
* Configuring memtable throughput
* Configuring compaction
* Compression

Testing compaction and compression
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* Tuning Java resources
* Purging gossip state on a node


Help with Scylla
================
Contact `Support <https://www.scylladb.com/product/support/>`_, or visit the Scylla `Community <https://www.scylladb.com/open-source-community/>`_ page for peer support.

.. include:: /rst_include/apache-copyrights-index.rst

.. include:: /rst_include/apache-copyrights-index-all-attributes.rst
