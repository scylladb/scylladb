=========================
Administration Guide
=========================

.. tip::

   For training material, see the `Admin Procedures lesson <https://university.scylladb.com/courses/scylla-operations/lessons/admin-procedures-and-basic-monitoring/>`_ on ScyllaDB University.

Before You Begin
---------------------------

System Requirements
===========================

Before you install and configure ScyllaDB, make sure your environment meets the :doc:`System Requirements </getting-started/system-requirements>`.

Download and Install
===========================

To install ScyllaDB on your platform, see :doc:`Getting Started </getting-started/index>`.

System Configuration
===========================

For the recommended OS settings for ScyllaDB, see the :ref:`System Configuration Guide <system-configuration-files-and-scripts>`.
ScyllaDB packages, Docker containers, and Amazon AMIs apply these settings automatically.

.. _check-your-current-version-of-scylla:

Check Your ScyllaDB Version
----------------------------------------------

To check which version of ScyllaDB you are running, run:

.. code-block:: shell

   scylla --version

The output shows the ScyllaDB version. Your results may differ. For example:

.. code-block:: shell

   4.4.0-0.20210331.05c6a40f0

.. note::

   This is not the same as the :doc:`nodetool version </operating-scylla/nodetool-commands/version>` command,
   which reports the CQL version. To check the CQL or Cassandra version, see
   :ref:`SHOW VERSION <cqlsh-show-version>` in the CQLSh reference.

.. _admin-scylla-configuration:

ScyllaDB Configuration Files
--------------------------------

ScyllaDB uses the following configuration files:

+-------------------------------------------------------+-----------------------------------------------+
| Installed location                                    | Description                                   |
+=======================================================+===============================================+
| :code:`/etc/default/scylla-server` (Ubuntu/Debian)    | Server startup options                        |
| :code:`/etc/sysconfig/scylla-server` (others)         |                                               |
+-------------------------------------------------------+-----------------------------------------------+
| :code:`/etc/scylla/scylla.yaml`                       | Main ScyllaDB configuration file              |
+-------------------------------------------------------+-----------------------------------------------+
| :code:`/etc/scylla/cassandra-rackdc.properties`       | Rack and datacenter configuration file        |
+-------------------------------------------------------+-----------------------------------------------+

scylla-server
===============

The :code:`scylla-server` file contains settings used when starting the ScyllaDB server.

.. _admin-scylla.yaml:

.. include:: /operating-scylla/scylla-yaml.inc

Experimental Features
-------------------------------

ScyllaDB uses experimental flags to safely expose features that are not production-ready.
These features are not stable enough for production use, and their API is likely to change
in ways that break backward or forward compatibility.

In recent ScyllaDB versions, you control these features with the ``experimental_features``
list in ``scylla.yaml``, which lets you choose which experimental features to enable.
To get the list of experimental features, run ``scylla --help``.

.. _admin-address-configuration-in-scylla:

Address Configuration in ScyllaDB
-------------------------------------

You can configure the following addresses in ``scylla.yaml``:

.. list-table::
   :widths: 30 70
   :header-rows: 1

   * - Address Type
     - Description
   * - ``listen_address``
     - The address on which ScyllaDB listens for connections from other nodes. See ``storage_port`` and ``ssl_storage_ports``.
   * - ``rpc_address``
     - The address on which ScyllaDB expects CQL client connections. See ``rpc_port``, ``native_transport_port``, and ``native_transport_port_ssl`` in the :ref:`Networking <cqlsh-networking>` parameters.
   * - ``broadcast_address``
     - The address broadcast to other ScyllaDB nodes to tell them where to connect. Related to ``listen_address``.
   * - ``broadcast_rpc_address``
     - The address broadcast to clients to tell them where to connect. Related to ``rpc_address``.
   * - ``seeds``
     - The ``broadcast_address`` values of the existing nodes. You must specify the address of at least one existing node.
   * - ``endpoint_snitch``
     - The node's address resolution helper.
   * - ``api_address``
     - The address for REST API requests. See ``api_port`` in the :ref:`Networking <cqlsh-networking>` parameters.
   * - ``prometheus_address``
     - The address for Prometheus queries. See ``prometheus_port`` in the :ref:`Networking <cqlsh-networking>` parameters and the `ScyllaDB Monitoring Stack <https://monitoring.docs.scylladb.com/stable/>`_ for details.
   * - ``replace_node_first_boot``
     - The Host ID of the dead node that this node is replacing. See :doc:`Replace a Dead Node in a ScyllaDB Cluster </operating-scylla/procedures/cluster-management/replace-dead-node>` for details.

.. note:: If ``listen_address``, ``rpc_address``, ``broadcast_address``, and ``broadcast_rpc_address`` are not set correctly, ScyllaDB does not work as expected.

.. _cqlsh-networking:

Networking
----------

The following table lists the ScyllaDB ports. For ScyllaDB Manager ports, see the `ScyllaDB Manager Documentation <https://manager.docs.scylladb.com/>`_.

.. image:: /operating-scylla/security/Scylla-Ports2.png

.. include:: /operating-scylla/_common/networking-ports.rst

All the ports above must be open to external clients (CQL) and to other nodes (RPC).
You can keep the REST API port closed to incoming external connections.

Advanced Networking
======================

A client or another node may need to connect to a ScyllaDB node using an IP address
that is different from the address the node listens on, for example, when the node is
behind port forwarding. In such cases, you can set alternate IP addresses.

.. warning:: Do not set any IP address to :code:`0.0.0.0`.

.. list-table::
   :widths: 30 40 30
   :header-rows: 1

   * - Address Type
     - Description
     - Default
   * - ``listen_address`` (required)
     - The address on which ScyllaDB listens for connections from other nodes. See ``storage_port`` and ``ssl_storage_ports``.
     - No default
   * - ``rpc_address`` (required)
     - The address on which ScyllaDB expects CQL client connections. See ``rpc_port``, ``native_transport_port``, and ``native_transport_port_ssl`` in the :ref:`Networking <cqlsh-networking>` parameters.
     - No default
   * - ``broadcast_address``
     - The address broadcast to other ScyllaDB nodes to tell them where to connect. Related to ``listen_address``.
     - ``listen_address``
   * - ``broadcast_rpc_address``
     - The address broadcast to clients to tell them where to connect. Related to ``rpc_address``.
     - ``rpc_address``

* If other nodes can connect directly to :code:`listen_address`, you do not need to set :code:`broadcast_address`.
* If clients can connect directly to :code:`rpc_address`, you do not need to set :code:`broadcast_rpc_address`.

.. note:: For tips and examples on configuring these addresses, see :doc:`How to Properly Set Address Values in scylla.yaml </kb/yaml-address>`.

Configuring TLS/SSL in scylla.yaml
----------------------------------

ScyllaDB supports encryption between nodes and between clients and nodes.
For configuration settings, see the :doc:`ScyllaDB TLS/SSL guide </operating-scylla/security/index>`.

.. _admin-compression:

Compression
----------------

In ScyllaDB, you can configure compression at rest and compression in transit.
Compression in transit can be configured between nodes or between the client and the node.

.. _admin-client-node-compression:

Client-Node Compression
===============================

Compression between the client and the node is set by the driver that the application uses to access ScyllaDB.
For example:

* `ScyllaDB Python Driver <https://python-driver.docs.scylladb.com/master/api/cassandra/cluster.html#cassandra.cluster.Cluster.compression>`_
* `ScyllaDB Java Driver <https://github.com/scylladb/java-driver/tree/3.7.1-scylla/manual/compression>`_
* `Go Driver <https://godoc.org/github.com/gocql/gocql#Compressor>`_

For more drivers, see `ScyllaDB Drivers <https://docs.scylladb.com/stable/drivers/index.html>`_.

.. _internode-compression:

Internode Compression
===========================

Internode compression is configured in ``scylla.yaml`` with the ``internode_compression``
option, which controls whether traffic between nodes is compressed:

* ``all`` - All traffic is compressed.
* ``dc`` - Traffic between different datacenters is compressed.
* ``rack`` - Traffic between different racks is compressed.
* ``none`` - Nothing is compressed (default).

.. _object-storage-configuration:

Configuring Object Storage
----------------------------------

ScyllaDB can communicate directly with S3-compatible object storage and with
Google Cloud Storage (GCS). Before you use features that rely on object storage,
you must enable and configure access to the storage endpoints.

Storage endpoints define where data can be stored. You specify them in
the ``scylla.yaml`` configuration file.

.. _object-storage-endpoint-name:

The Endpoint Name
=================

The ``name`` of an endpoint is also its identifier. Statements that select an
object-storage endpoint, such as
:ref:`CREATE KEYSPACE <cql-keyspace-storage-options>`, must repeat this string
**exactly**. The lookup is an exact string comparison, so
``https://s3.us-east-1.amazonaws.com`` and ``https://s3.us-east-1.amazonaws.com:443``
are two different endpoints, even though they address the same server.

When writing the ``name``:

* Write it as a URL, for example, ``https://s3.us-east-1.amazonaws.com``.
* The scheme selects whether the connection is encrypted. Use ``https`` in production.
* The port is taken from the URL. If you omit it, the scheme default is used
  (443 for ``https``, 80 for ``http``).
* Do not add a trailing slash or a path. It is ignored when connecting, but it is
  still part of the endpoint identifier, so ``https://s3.us-east-1.amazonaws.com/``
  and ``https://s3.us-east-1.amazonaws.com`` are two different endpoints.

Amazon S3 and S3-Compatible Stores
==================================

An endpoint with no ``type`` is treated as S3. However, we recommend stating
``type: s3`` explicitly so that the backend of each entry is clear.

.. code-block:: yaml

   object_storage_endpoints:
     - name: http[s]://<endpoint_address_or_domain_name>[:<port_number>]
       type: s3
       aws_region: <region_name> # required unless AWS_DEFAULT_REGION is set, e.g. us-east-1
       iam_role_arn: <iam_role> # optional

Example:

.. code:: yaml

   object_storage_endpoints:
     - name: https://s3.us-east-1.amazonaws.com
       type: s3
       aws_region: us-east-1
       iam_role_arn: arn:aws:iam::123456789012:role/my-scylla-role

An S3 endpoint requires a region. You can set it with the ``aws_region`` option
or with the ``AWS_DEFAULT_REGION`` environment variable. If neither provides a
non-empty value, ScyllaDB logs an error and ignores the endpoint.

S3 Credentials
==============

This section applies to S3 and S3-compatible endpoints. For GCS endpoints,
credentials are configured with ``credentials_file``, as described in
:ref:`Google Cloud Storage <object-storage-gcs-config>`.

You can configure the AWS-related credentials options (``aws_access_key_id``,
``aws_secret_access_key``, ``aws_session_token``) using
the following environment variables:

* ``AWS_ACCESS_KEY_ID``
* ``AWS_SECRET_ACCESS_KEY``
* ``AWS_SESSION_TOKEN``

The ScyllaDB S3 client resolves credentials in the following order:

#. Environment variables
#. The EC2 Instance Metadata Service
#. The AWS Security Token Service (STS). STS also requires ``iam_role_arn`` to be set.

``iam_role_arn`` must be the ARN of the IAM **role** to assume
(``arn:aws:iam::<account>:role/<name>``), not the ARN of the instance profile
that contains that role. See `AssumeRole
<https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRole.html>`_ in
the AWS documentation.

.. note::

   - All AWS-related parameters must be either all present or all absent.
   - When set, these values are used by the S3 client to sign requests.
   - If not set, requests are sent unsigned, which some servers may not accept.

.. note::

   On EC2, prefer an instance profile or STS so that no long-lived credentials
   are stored on the node.

   Object stores that do not provide an EC2-compatible instance metadata service,
   such as :ref:`OCI <admin-oci-object-storage>`, can only be accessed with
   credentials supplied through the environment variables listed above. Unlike
   the GCS client, which reads a service-account file, the S3 client has no
   file-based credentials provider.

.. _admin-oci-object-storage:

Oracle OCI Object Storage
=================================

Oracle Cloud Infrastructure (OCI) Object Storage is compatible with the Amazon
S3 API, so ScyllaDB can use it without any special configuration other than
specifying the OCI S3-compatible endpoint.

**Endpoint URL format**

OCI S3-compatible endpoint URLs follow this pattern:

.. code-block:: none

   https://<namespace>.compat.objectstorage.<region>.oci.customer-oci.com:443

Where:

* ``<namespace>`` is your OCI Object Storage namespace.
* ``<region>`` is the OCI region identifier, for example, ``us-ashburn-1``.

**Credentials**

OCI does not provide an instance metadata endpoint compatible with AWS STS/EC2,
so you **must** supply credentials explicitly through environment variables.
Create a Customer Secret Key in the OCI Console and set:

.. code-block:: bash

   export AWS_ACCESS_KEY_ID=<customer_secret_key_access_key>
   export AWS_SECRET_ACCESS_KEY=<customer_secret_key_secret>

.. note::

   The ``iam_role_arn`` field is specific to AWS IAM. Omit it
   when configuring an OCI endpoint.

**Configuration example**

.. code:: yaml

   object_storage_endpoints:
     - name: https://idedxcgnkfkt.compat.objectstorage.us-ashburn-1.oci.customer-oci.com:443
       aws_region: us-ashburn-1

.. _object-storage-gcs-config:

Google Cloud Storage
====================

To select the GCS backend, set ``type: gs``. The endpoint ``name`` is usually
``default``, which resolves to the standard GCS endpoint. You only need a URI
when using a private proxy or a mock server.

.. code-block:: yaml

   object_storage_endpoints:
     - name: default
       type: gs
       credentials_file: <path to a service-account JSON file> # optional

If you omit ``credentials_file``, the default credentials on the machine are
used: the current user's credentials, or the instance credentials when running
on a GCE instance. The ``GOOGLE_APPLICATION_CREDENTIALS`` environment variable is
also honored.

.. warning::

   ``type`` defaults to ``s3`` when it is not specified. A GCS endpoint that
   omits ``type: gs`` is configured as an S3 endpoint pointing at a Google URL,
   which fails in ways that are hard to diagnose.

Tuning Options
==============

.. list-table::
   :widths: 30 15 55
   :header-rows: 1

   * - Option
     - Default
     - Description
   * - ``object_storage_endpoints``
     - empty
     - The list of endpoints described above. Live-updateable: you can add
       endpoints without restarting the node.
   * - ``object_storage_connections``
     - empty
     - The maximum number of connections per shard for **S3** endpoints, per connection
       pool, specified as a map from the pool name to a positive count.

       A scheduling group uses the pool named after it, with the following exceptions:

       * Every service-level group uses ``service_levels``.
       * A group with no pool of its own uses ``general``. In this case, the node
         logs a warning naming that group and counts it in
         ``scylla_s3_unclassified_groups``.

       Entries override the built-in values:
       ``service_levels: 128``, ``memtable: 16``, ``memtable_to_cache: 8``,
       ``compaction: 16``, ``maintenance_compaction: 16``, ``streaming: 64``,
       ``backup: 32``, ``maintenance: 16``, ``general: 16``.

       Live-updateable. Removing an entry keeps the cap of a pool that already
       exists. The GCS backend ignores this option.
   * - ``object_storage_connections_per_shard``
     - 128
     - Deprecated, superseded by ``object_storage_connections``.

       This option set one budget for the whole shard, which was split across the
       scheduling groups by their shares. If you set it, it is still honored as a
       shard ceiling: the built-in per-pool values are scaled up or down to match
       that total exactly, keeping their proportions and at least one connection
       per pool. The built-in values total 312, so a node configured with 512 gets
       ``service_levels`` 207 and ``streaming`` 104, and a node configured with 128
       gets ``service_levels`` 50 and ``streaming`` 26.

       A budget lower than the number of pools cannot be honored: every pool keeps
       one connection, and the node logs that the total exceeds the budget.

       If you name any pool in ``object_storage_connections``, this option is
       ignored entirely: the caps are taken from ``object_storage_connections``
       and the old budget is not consulted.

       Live-updateable.

.. _admin-keyspace-storage-options:

Keyspace Storage Options
------------------------

By default, a keyspace's SSTables are stored in a local directory.
Alternatively, you can configure a keyspace to be stored on Amazon S3,
another S3-compatible object store, or Google Cloud Storage.

Before you create keyspaces with object storage, you need to
:ref:`configure <object-storage-configuration>` the object storage
credentials and endpoint. Then, create the keyspace with a ``STORAGE``
option naming that endpoint. See
:ref:`Keyspace storage options <cql-keyspace-storage-options>` for the CQL
syntax.

.. important::

   Object-storage keyspaces are a **Preview** feature: they are available for
   evaluation, but are not generally available and are not recommended for
   production data. Review :ref:`Object storage keyspaces: status and limitations
   <object-storage-limitations>` before using this feature. In particular,
   snapshots and backup are **not available** for object-storage keyspaces.

.. _admin-object-storage-config:

Required Configuration
======================

Object storage has much higher latency than local disks. Before using an
object-storage keyspace, you must adjust the following settings:

* ``query_page_size_in_bytes: 67108864`` (64 MiB). Without a larger page size,
  range scans and multi-partition reads issue many small object requests and are
  likely to time out.
* Increase request timeouts. The relevant ``scylla.yaml`` options are:

  * ``range_request_timeout_in_ms`` (default 10000)
  * ``read_request_timeout_in_ms`` (default 5000)
  * ``write_request_timeout_in_ms`` (default 2000)

  All of them are live-updateable. See :ref:`scylla.yaml <admin-scylla.yaml>`
  for the full list.

  Instead of raising the global defaults, you can limit the increase to the affected
  workload with the ``USING TIMEOUT`` CQL extension on individual statements or with a
  per-service-level timeout. Client-side timeouts are configured in the driver
  and must be raised to match.
* ``force_capacity_based_balancing: true``. By default, the tablet load balancer
  equalizes storage utilization using SSTable sizes fetched from load
  statistics. For object-storage keyspaces, those sizes do not correspond to local
  disk usage, so you must switch the balancer to capacity-based balancing.
  It treats tablets as equally sized and balances them against node and
  shard capacity instead. This option is live-updateable.

Verifying Keyspace Storage
==========================

To check which keyspaces are stored on object storage, run:

.. code-block:: cql

   SELECT keyspace_name, storage_type, storage_options
   FROM system_schema.scylla_keyspaces;

.. code-block:: none

    keyspace_name | storage_type | storage_options
   ---------------+--------------+---------------------------------------------------------------------------
               ks |           S3 | {'bucket': 'my-bucket', 'endpoint': 'https://s3.us-east-1.amazonaws.com'}

* ``storage_type`` is set only for object-storage keyspaces, to ``S3`` or ``GS``.
  In that case, ``storage_options`` holds the bucket and endpoint.
* There is no ``LOCAL`` value. A keyspace that does not appear in the output, or
  appears with a null ``storage_type``, is not stored on object storage.
* This table does not list all keyspaces.

.. _object-storage-limitations:

Object Storage Keyspaces: Status and Limitations
-------------------------------------------------

Object-storage keyspaces are a **Preview** feature in this release.

Object-storage keyspaces target archival workloads: data that is queried
infrequently and benefits from object-storage pricing. Read latency is
much higher than on local disks, and there is no local disk cache, so
the cost of a read scales with the number of SSTables it touches.

.. warning::

   Not all of the limitations listed below are enforced. Materialized views, secondary
   indexes, LWT, counters, and CDC are accepted on an object-storage keyspace
   without an error, but their behavior there is undefined and untested.

Supported
=========

* Tablets.
* Reads, writes, memtable flush, and compaction.
* Adding, removing, replacing, and decommissioning nodes. For object-storage
  keyspaces, tablet migration moves no data: ownership of the shared objects is
  transferred by reference instead of copying them.
* Repair, including incremental repair.
* Encryption at Rest.
* Amazon S3 and S3-compatible object stores. Google Cloud Storage works over
  the same code path, but is **experimental** (see below).

Not Supported
=============

.. list-table::
   :widths: 35 65
   :header-rows: 1

   * - Capability
     - Status
   * - Snapshots, backup, and restore
     - Not available. ``nodetool snapshot`` fails for object-storage keyspaces
       with ``Snapshotting non-local tables is not implemented``, and every
       backup path is built on snapshots. Restoring **into** an object-storage
       keyspace is also unavailable.
   * - Vnode keyspaces
     - Not supported. An object-storage keyspace must use tablets, which is the
       default for new keyspaces.
   * - Mixed-storage clusters
     - Not supported, and refused. A cluster must use either local storage or
       object storage for its user keyspaces, not both.

       ``CREATE KEYSPACE`` is rejected when the requested storage differs from
       the storage used by the cluster's existing user keyspaces. A cluster that
       already holds keyspaces of both kinds keeps working, and only a warning is
       issued.

       The ``restrict_mixed_storage_clusters`` option relaxes the refusal to a
       warning (``warn``) or removes it (``false``). It is live-updateable and
       defaults to ``true``.
   * - Materialized views and secondary indexes
     - Not supported.
   * - LWT, counters, CDC, Alternator
     - Not supported. An Alternator table is stored locally, so ``CreateTable``
       is rejected with a ``ValidationException`` on a cluster whose user
       keyspaces are in object storage. This refusal also follows the
       ``restrict_mixed_storage_clusters`` setting.
   * - Size-based tablet load balancing
     - The load balancer's size-based mode treats SSTable sizes as local disk
       usage, which does not hold for object storage. Set
       ``force_capacity_based_balancing: true`` as described in
       :ref:`Required configuration <admin-object-storage-config>`.
   * - Changing a keyspace's storage type
     - Not possible. ``ALTER KEYSPACE`` cannot move a keyspace between local and
       object storage. To move existing data, create a second keyspace
       with the desired storage and copy the data through CQL, reading from
       one keyspace and writing to the other.
   * - Google Cloud Storage backend
     - Experimental. ScyllaDB exposes no per-request metrics for the GCS
       backend, so you cannot observe the object-storage request rate, latency,
       retries, or errors. Use S3 for production deployments.

Backup and ScyllaDB Manager
===========================

ScyllaDB Manager cannot back up object-storage keyspaces, and a backup task that
includes them fails. In this release, there is no way to back up an
object-storage keyspace.

Storing SSTables in an object store does **not** make them a backup. The objects
are live SSTables, and ScyllaDB deletes them when the last reference to them
is removed.

.. _admin-views-with-tablets:

Views with Tablets
------------------

Materialized Views (MV) and Secondary Indexes (SI) are supported in keyspaces that use tablets
only when the keyspaces are :term:`RF-rack-valid <RF-rack-valid keyspace>`.

When a keyspace contains a Materialized View or Secondary Index, some operations are restricted to maintain
the RF-rack condition. While the view or index exists, you cannot:

* Alter the keyspace's replication factor to a value that would violate the RF-rack-valid property.
* Add a node in a new rack in an existing datacenter.
* Remove the last node in a rack.

.. _admin-core-dumps:

Core Dumps
----------

ScyllaDB places core dumps in :code:`/var/lib/scylla/coredump`. They are not visible
with the :code:`coredumpctl` command. For details on core dump configuration scripts, see the
:doc:`System Configuration Guide </getting-started/system-configuration/>`.

.. caution:: Core dumps may contain sensitive data. Check with ScyllaDB support before sharing any core dump.

.. note::

   On RHEL and CentOS, the `Automatic Bug Reporting Tool <https://abrt.readthedocs.io/en/latest/>`_
   conflicts with the ScyllaDB core dump configuration. Remove it before installing ScyllaDB:

   .. code-block:: shell

      sudo yum remove -y abrt

Schedule fstrim
----------------------

ScyllaDB sets up a daily fstrim on the filesystems that contain your ScyllaDB
commitlog and data directory. This utility discards (trims) any blocks that are
no longer used by the filesystem.

Monitoring
---------------------

ScyllaDB exposes the following interfaces for online monitoring.

Monitoring Interfaces
==========================

See `ScyllaDB Monitoring Interfaces <https://monitoring.docs.scylladb.com/stable/reference/monitoring_apis.html>`_.

Monitoring Stack
=======================

|mon_root|

.. REST

.. include:: /operating-scylla/rest.rst

What You Don't Need to Configure
-----------------------------------

ScyllaDB is designed to deliver high performance without tuning. It has fewer layers that
interact in unpredictable ways and uses better algorithms that do not require manual tuning.
The following topics appear in the manuals of other data stores, but are not needed in ScyllaDB.

Configuration
============================

* Generating tokens
* Configuring virtual nodes

Operations
============================

* Tuning Bloom filters
* Data caching
* Configuring memtable throughput
* Configuring compaction
* Compression

Testing Compaction and Compression
====================================

* Tuning Java resources
* Purging gossip state on a node

Getting Help
--------------------------

Contact `Support <https://www.scylladb.com/product/support/>`_, or visit the ScyllaDB `Community <https://www.scylladb.com/open-source-community/>`_ page for peer support.

.. include:: /rst_include/apache-copyrights-index.rst

.. include:: /rst_include/apache-copyrights-index-all-attributes.rst
