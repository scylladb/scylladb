Administration Guide
********************

For training material, also check out the `Admin Procedures lesson <https://university.scylladb.com/courses/scylla-operations/lessons/admin-procedures-and-basic-monitoring/>`_ on ScyllaDB University.

System requirements
===================
Make sure you have met the :doc:`System Requirements </getting-started/system-requirements>`  before you install and configure ScyllaDB. 

Download and Install
====================

See the :doc:`getting started page </getting-started/index>` for info on installing ScyllaDB on your platform.


System configuration
====================
See :ref:`System Configuration Guide <system-configuration-files-and-scripts>` for details on optimum OS settings for ScyllaDB. (These settings are performed automatically in the ScyllaDB packages, Docker containers, and Amazon AMIs.)

.. _admin-scylla-configuration:

ScyllaDB Configuration
======================
ScyllaDB configuration files are:

+-------------------------------------------------------+-----------------------------------+
| Installed location                                    | Description                       |
+=======================================================+===================================+
| :code:`/etc/default/scylla-server` (Ubuntu/Debian)    | Server startup options            |
| :code:`/etc/sysconfig/scylla-server` (others)         |                                   |
+-------------------------------------------------------+-----------------------------------+
| :code:`/etc/scylla/scylla.yaml`                       | Main ScyllaDB configuration file  |
+-------------------------------------------------------+-----------------------------------+
| :code:`/etc/scylla/cassandra-rackdc.properties`       | Rack & dc configuration file      |
+-------------------------------------------------------+-----------------------------------+
| :code:`/etc/scylla/object_storage.yaml`               | Object storage configuration file |
+-------------------------------------------------------+-----------------------------------+

.. _check-your-current-version-of-scylla:

Check your current version of ScyllaDB
--------------------------------------
This command allows you to check your current version of ScyllaDB. Note that this command is not the :doc:`nodetool version </operating-scylla/nodetool-commands/version>` command which reports the CQL version.
If you are looking for the CQL or Cassandra version, refer to the CQLSH reference for :ref:`SHOW VERSION <cqlsh-show-version>`.

.. code-block:: shell

   scylla --version

Output displays the ScyllaDB version. Your results may differ.

.. code-block:: shell

   4.4.0-0.20210331.05c6a40f0

.. _admin-address-configuration-in-scylla:

Address Configuration in ScyllaDB
---------------------------------

The following addresses can be configured in scylla.yaml:

.. list-table::
   :widths: 30 70
   :header-rows: 1

   * - Address Type
     - Description
   * - listen_address
     - Address ScyllaDB listens for connections from other nodes. See storage_port and ssl_storage_ports.
   * - rpc_address
     - Address on which ScyllaDB is going to expect CQL client connections. See rpc_port, native_transport_port and native_transport_port_ssl in the :ref:`Networking <cqlsh-networking>` parameters.
   * - broadcast_address
     - Address that is broadcasted to tell other ScyllaDB nodes to connect to. Related to listen_address above.
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
     - Host ID of a dead node this ScyllaDB node is replacing. Refer to :doc:`Replace a Dead Node in a ScyllaDB Cluster </operating-scylla/procedures/cluster-management/replace-dead-node>` for more details.

.. note:: When the listen_address, rpc_address, broadcast_address, and broadcast_rpc_address parameters are not set correctly, ScyllaDB does not work as expected.

scylla-server
-------------
The :code:`scylla-server` file contains configuration related to starting up the ScyllaDB server.

.. _admin-scylla.yaml:

.. include:: /operating-scylla/scylla-yaml.inc

.. _object-storage-configuration:

Configuring Object Storage :label-caution:`Experimental`
========================================================

Scylla has the ability to communicate directly with S3-compatible storage. This
feature enables various functionalities, but requires proper configuration of
storage endpoints.

To enable S3-compatible storage features, you need to describe the endpoints
where SSTable files can be stored. This is done using a YAML configuration file.

The ``object_storage.yaml`` file should follow this format:

.. code-block:: yaml

   endpoints:
     - name: <endpoint_address_or_domain_name>
       port: <port_number>
       https: <true_or_false> # optional
       aws_region: <region_name> # optional, e.g. us-east-1
       aws_access_key_id: <access_key> # optional
       aws_secret_access_key: <secret_access_key> # optional
       aws_session_token: <session_token> # optional


The AWS-related options (``aws_region``, ``aws_access_key_id``,
``aws_secret_access_key``, ``aws_session_token``) can be configured in two ways:

* Directly in the YAML file (as shown above).
* Using environment variables:

  - ``AWS_DEFAULT_REGION``
  - ``AWS_ACCESS_KEY_ID``
  - ``AWS_SECRET_ACCESS_KEY``
  - ``AWS_SESSION_TOKEN``

.. note::

   - All AWS-related parameters must be either present or absent as a group.
   - When set, these values are used by the S3 client to sign requests.
   - If not set, requests are sent unsigned, which may not be accepted by all servers.

By default, Scylla looks for the configuration file named ``object_storage.yaml``
in the same directory as ``scylla.yaml``. You can override this location using the
:confval:`object_storage_config_file` option in ``scylla.yaml``:

.. code-block:: yaml

   object-storage-config-file: object-storage-config-file.yaml

.. _admin-compression:

Compression
-----------

In ScyllaDB, you can configure compression at rest and compression in transit.
For compression in transit, you can configure compression between nodes or between the client and the node.


.. _admin-client-node-compression:

Client - Node Compression
^^^^^^^^^^^^^^^^^^^^^^^^^^

Compression between the client and the node is set by the driver that the application is using to access ScyllaDB.

For example:

* `ScyllaDB Python Driver <https://python-driver.docs.scylladb.com/master/api/cassandra/cluster.html#cassandra.cluster.Cluster.compression>`_
* `ScyllaDB Java Driver <https://github.com/scylladb/java-driver/tree/3.7.1-scylla/manual/compression>`_
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

ScyllaDB versions 1.1 and greater support encryption between nodes and between client and node. See the ScyllaDB :doc:`ScyllaDB TLS/SSL guide: </operating-scylla/security/index>` for configuration settings.

.. _cqlsh-networking:

Networking
----------

The ScyllaDB ports are detailed in the table below. For ScyllaDB Manager ports, see the `ScyllaDB Manager Documentation <https://manager.docs.scylladb.com/>`_.

.. image:: /operating-scylla/security/Scylla-Ports2.png

.. include:: /operating-scylla/_common/networking-ports.rst

All ports above need to be open to external clients (CQL) and other nodes (RPC). REST API port can be kept closed for incoming external connections.

Advanced networking
-------------------
It is possible that a client, or another node, may need to use a different IP address to connect to a ScyllaDB node from the address that the node is listening on. This is the case when a node is behind port forwarding. ScyllaDB allows for setting alternate IP addresses.

Do not set any IP address to :code:`0.0.0.0`.

.. list-table::
   :widths: 30 40 30
   :header-rows: 1

   * - Address Type
     - Description
     - Default
   * - listen_address (required)
     - Address ScyllaDB listens for connections from other nodes. See storage_port and ssl_storage_ports.
     - No default
   * - rpc_address (required)
     - Address on which ScyllaDB is going to expect CQL clients connections. See rpc_port, native_transport_port and native_transport_port_ssl in the :ref:`Networking <cqlsh-networking>` parameters.
     - No default
   * - broadcast_address
     - Address that is broadcasted to tell other ScyllaDB nodes to connect to. Related to listen_address above.
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
On RHEL and CentOS, the `Automatic Bug Reporting Tool <https://abrt.readthedocs.io/en/latest/>`_ conflicts with ScyllaDB coredump configuration. Remove it before installing ScyllaDB: :code:`sudo yum remove -y abrt`

ScyllaDB places any core dumps in :code:`var/lib/scylla/coredump`. They are not visible with the :code:`coredumpctl` command. See the :doc:`System Configuration Guide </getting-started/system-configuration/>` for details on core dump configuration scripts. Check with ScyllaDB support before sharing any core dump, as they may contain sensitive data.

Schedule fstrim
===============

ScyllaDB sets up daily fstrim on the filesystem(s),
containing your ScyllaDB commitlog and data directory. This utility will
discard, or trim, any blocks no longer in use by the filesystem.

Experimental Features
=====================

ScyllaDB Open Source uses experimental flags to expose non-production-ready features safely. These features are not stable enough to be used in production, and their API will likely change, breaking backward or forward compatibility.

In recent ScyllaDB versions, these features are controlled by the ``experimental_features`` list in scylla.yaml, allowing one to choose which experimental to enable.
For example, some of the experimental features in ScyllaDB Open Source 4.5 are: ``udf``, ``alternator-streams`` and ``raft``.
Use ``scylla --help`` to get the list of experimental features.

ScyllaDB Enterprise and ScyllaDB Cloud do not officially support experimental Features.

.. _admin-keyspace-storage-options:

Keyspace storage options
------------------------

..
   This section must be moved to Data Definition> CREATE KEYSPACE
   when support for object storage is GA.

By default, SStables of a keyspace are stored in a local directory.
As an alternative, you can configure your keyspace to be stored
on Amazon S3 or another S3-compatible object store.

Support for object storage is experimental and must be explicitly
enabled in the ``scylla.yaml`` configuration file by specifying
the ``keyspace-storage-options`` option:

.. code-block:: yaml

   experimental_features:
     - keyspace-storage-options


Before creating keyspaces with object storage, you also need to
:ref:`configure <object-storage-configuration>` the object storage
credentials and endpoint.

Monitoring
==========
ScyllaDB exposes interfaces for online monitoring, as described below.

Monitoring Interfaces
---------------------

`ScyllaDB Monitoring Interfaces <https://monitoring.docs.scylladb.com/stable/reference/monitoring_apis.html>`_

Monitoring Stack
----------------

|mon_root|

.. REST

.. include:: /operating-scylla/rest.rst

Un-contents
-----------

ScyllaDB is designed for high performance before tuning, for fewer layers that interact in unpredictable ways, and to use better algorithms that do not require manual tuning. The following items are found in the manuals for other data stores but do not need to appear here.

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


Help with ScyllaDB
==================
Contact `Support <https://www.scylladb.com/product/support/>`_, or visit the ScyllaDB `Community <https://www.scylladb.com/open-source-community/>`_ page for peer support.

.. include:: /rst_include/apache-copyrights-index.rst

.. include:: /rst_include/apache-copyrights-index-all-attributes.rst
