
ScyllaDB and Apache Cassandra Compatibility 
=============================================

Latest update: ScyllaDB 5.0

ScyllaDB is a drop-in replacement for Apache Cassandra 3.11, with additional features from Apache Cassandra 4.0.
This page contains information about ScyllaDB compatibility with Apache Cassandra. 

The tables on this page include information about ScyllaDB Open Source support for Apache Cassandra features. 
They do not include the ScyllaDB Enterprise-only features or ScyllaDB-specific features with no match in 
Apache Cassandra.  See :doc:`ScyllaDB Features </using-scylla/features>` for more information about ScyllaDB features.

How to Read the Tables on This Page
-------------------------------------

* |v| - Available in ScyllaDB and compatible with Apache Cassandra.
* |x| - Not available in ScyllaDB.
* **NC** - Available in ScyllaDB, but not compatible with Apache Cassandra.

Interfaces
----------

.. list-table::
   :widths: 23 42 35
   :header-rows: 1

   * - Apache Cassandra Interface
     - Version Supported by ScyllaDB
     - Comments
   * - CQL
     - | Fully compatible with version 3.3.1, with additional features from later CQL versions (for example, :ref:`Duration type <durations>`).
       | Fully compatible with protocol v4, with additional features from v5.
     - More below
   * - Thrift 
     - Compatible with Cassandra 2.1
     - 
   * - SSTable format (all versions)
     - 3.11(mc / md / me), 2.2(la), 2.1.8 (ka)
     - | ``me`` - supported in ScyllaDB Open Source 5.1 and ScyllaDB Enterprise 2022.2.0 (and later)
       | ``md`` - supported in ScyllaDB Open Source 4.3 and ScyllaDB Enterprise 2021.1.0 (and later)
       

   * - JMX   
     - 3.11
     - More below
   * - Configuration (cassandra.yaml)
     - 3.11
     - 
   * - Log
     - NC
     - 
   * - Gossip and internal streaming
     - NC
     - 
   * - SSL
     - NC
     - 


..  _3.3.1: https://github.com/apache/cassandra/blob/cassandra-2.2/doc/cql3/CQL.textile#changes

Supported Tools
---------------

The tools are based on Apache Cassandra 3.11.

.. include:: /operating-scylla/_common/tools_index.rst

Features
--------

.. _consistency-level-read-and-write:

Consistency Level (read and write)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

+-------------------------------------+--------------+
|   Options                           | Support      |
+=====================================+==============+
| Any (Write Only)                    | |v|          |
+-------------------------------------+--------------+
| One                                 | |v|          |
+-------------------------------------+--------------+
| Two                                 | |v|          |
+-------------------------------------+--------------+
| Three                               | |v|          |
+-------------------------------------+--------------+
| Quorum                              | |v|          |
+-------------------------------------+--------------+
| All                                 | |v|          |
+-------------------------------------+--------------+
| Local One                           | |v|          |
+-------------------------------------+--------------+
| Local Quorum                        | |v|          |
+-------------------------------------+--------------+
| Each Quorum (Write Only)            | |v|          |
+-------------------------------------+--------------+
| SERIAL                              | |v| :sup:`*` |
+-------------------------------------+--------------+
| LOCAL_SERIAL                        | |v|:sup:`*`  |
+-------------------------------------+--------------+

:sup:`*` From ScyllaDB 4.0. See `Scylla LWT`_


Snitches
^^^^^^^^
+-------------------------------------+--------+
|   Options                           | Support|
+=====================================+========+
| SimpleSnitch_                       |   |v|  |
+-------------------------------------+--------+
| RackInferringSnitch_                |   |v|  |
+-------------------------------------+--------+
| PropertyFileSnitch                  |   |x|  |
+-------------------------------------+--------+
| GossipingPropertyFileSnitch_        |   |v|  |
+-------------------------------------+--------+
| Dynamic snitching                   |   |x|  |
+-------------------------------------+--------+
| EC2Snitch_                          |   |v|  |
+-------------------------------------+--------+
| EC2MultiRegionSnitch_               |   |v|  |
+-------------------------------------+--------+
| GoogleCloudSnitch_                  |   |v|  |
+-------------------------------------+--------+
| CloudstackSnitch                    |   |x|  |
+-------------------------------------+--------+

Partitioners
^^^^^^^^^^^^
+-------------------------------------+--------------+
|   Options                           | Support      |
+=====================================+==============+
| Murmur3Partitioner (default)        |   |v|        |
+-------------------------------------+--------------+
| RandomPartitioner                   | |x| :sup:`*` |
+-------------------------------------+--------------+
| OrderPreservingPartitioner          | |x|          |
+-------------------------------------+--------------+
| ByteOrderedPartitioner              | |x| :sup:`*` |
+-------------------------------------+--------------+
| CollatingOrderPreservingPartitioner |    |x|       |
+-------------------------------------+--------------+

:sup:`*` Removed in ScyllaDB 4.0

Protocol Options
^^^^^^^^^^^^^^^^
+-------------------------------------+--------------+
|   Options                           | Support      |
+=====================================+==============+
| Encryption_                         |   |v|        |
+-------------------------------------+--------------+
| Authentication_                     |   |v|        |
+-------------------------------------+--------------+
| Compression_  (see below)           |   |v|        |
+-------------------------------------+--------------+


Compression
^^^^^^^^^^^
+-------------------------------------+--------------+
|   Options                           | Support      |
+=====================================+==============+
|CQL Compression                      |   |v|        |
+-------------------------------------+--------------+
| LZ4                                 |   |v|        |
+-------------------------------------+--------------+
| Snappy                              |   |v|        |
+-------------------------------------+--------------+
| `Node to Node Compression`_         |   |v|        |
+-------------------------------------+--------------+
| `Client to Node Compression`_       |   |v|        |
+-------------------------------------+--------------+

Backup and Restore
^^^^^^^^^^^^^^^^^^
+-------------------------------------+--------------+
|   Options                           | Support      |
+=====================================+==============+
| Snapshot_                           |   |v|        |
+-------------------------------------+--------------+
| `Incremental backup`_               |   |v|        |
+-------------------------------------+--------------+
| Restore_                            |   |v|        |
+-------------------------------------+--------------+

Repair and Consistency
^^^^^^^^^^^^^^^^^^^^^^
+-------------------------------------+--------------+
|   Options                           | Support      |
+=====================================+==============+
| `Nodetool Repair`_                  |   |v|        |
+-------------------------------------+--------------+
| Incremental Repair                  | |x|          |
+-------------------------------------+--------------+
|`Hinted Handoff`_                    | |v|          |
+-------------------------------------+--------------+
|`Lightweight transactions`_          |  |v|:sup:`*` |
+-------------------------------------+--------------+


:sup:`*` From ScyllaDB 4.0. See `Scylla LWT`_

Replica Replacement Strategy
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

+-------------------------------------+--------------+
|   Options                           | Support      |
+=====================================+==============+
| SimpleStrategy                      |   |v|        |
+-------------------------------------+--------------+
| NetworkTopologyStrategy             |   |v|        |
+-------------------------------------+--------------+



Security
^^^^^^^^

+-------------------------------------+--------------+
|   Options                           | Support      |
+=====================================+==============+
| Role Based Access Control (RBAC)    |   |v|        |
+-------------------------------------+--------------+


Indexing and Caching
^^^^^^^^^^^^^^^^^^^^^

+-------------------------------------+-----------------------------------------+
|   Options                           | Support                                 |
+=====================================+=========================================+
|row / key cache                      | |x| (More on `Scylla memory and cache`_)|
+-------------------------------------+-----------------------------------------+
|`Secondary Index`_                   | |v| :sup:`*`                            |
+-------------------------------------+-----------------------------------------+
|`Materialized Views`_                |  |v|:sup:`*`                            |
+-------------------------------------+-----------------------------------------+

:sup:`*` In ScyllaDB Open Source and ScyllaDB Enterprise from 2019.1

Additional Features
^^^^^^^^^^^^^^^^^^^

+-----------------------------------+-------------------------------------+
| Feature                           | Support                             |
+===================================+=====================================+
|Counters                           | |v|                                 |
+-----------------------------------+-------------------------------------+
|User Defined Types                 | |v|                                 |
+-----------------------------------+-------------------------------------+
|User Defined Functions             | |x| :sup:`*`                        |
+-----------------------------------+-------------------------------------+
|Time to live (TTL)                 | |v|                                 |
+-----------------------------------+-------------------------------------+
|Super Column                       | |x|                                 |
+-----------------------------------+-------------------------------------+
|vNode Enable                       | |v| Default                         |
+-----------------------------------+-------------------------------------+
|vNode Disable                      | |x|                                 |
+-----------------------------------+-------------------------------------+
|Triggers                           | |x|                                 |
+-----------------------------------+-------------------------------------+
|Batch Requests                     | |v| Includes conditional updates    |
+-----------------------------------+-------------------------------------+

:sup:`*`  Experimental 

.. _`Secondary Index`: /using-scylla/secondary-indexes/
.. _`Lightweight Transactions`: /using-scylla/lwt/
.. _`Materialized Views`: /using-scylla/materialized-views/
.. _`Node to Node Compression`: /operating-scylla/admin/#internode-compression
.. _`Client to Node Compression`: /operating-scylla/admin/#client-node-compression
.. _`Compression`: /operating-scylla/admin/#compression
.. _`Scylla LWT`: /using-scylla/lwt/
.. _401: https://github.com/scylladb/scylla/issues/401
.. _1141: https://github.com/scylladb/scylla/issues/1141
.. _1619: https://github.com/scylladb/scylla/issues/1619
.. _577: https://github.com/scylladb/scylla/issues/577
.. _`Scylla memory and cache`: http://www.scylladb.com/technology/memory/
.. _Encryption: /operating-scylla/security/client_node_encryption/
.. _Authentication: /operating-scylla/security/authentication/
.. _Authorization: /operating-scylla/security/authorization/
.. _`Nodetool Repair`: /operating-scylla/nodetool-commands/repair/
.. _Snapshot: /operating-scylla/procedures/backup-restore/backup/#full-backup-snapshots
.. _`Incremental backup`: /operating-scylla/procedures/backup-restore/backup/#incremental-backup
.. _Restore: /operating-scylla/procedures/backup-restore/restore/
.. _SimpleSnitch: /operating-scylla/system-configuration/snitch/#simplesnitch
.. _RackInferringSnitch: /operating-scylla/system-configuration/snitch/#rackinferringsnitch
.. _GossipingPropertyFileSnitch: /operating-scylla/system-configuration/snitch/#gossipingpropertyfilesnitch/
.. _EC2Snitch: /operating-scylla/system-configuration/snitch/#ec2snitch/
.. _EC2MultiRegionSnitch: /operating-scylla/system-configuration/snitch/#ec2multiregionsnitch
.. _GoogleCloudSnitch: /operating-scylla/system-configuration/snitch/#googlecloudsnitch
.. _`Hinted Handoff`: /architecture/anti-entropy/hinted-handoff/

CQL Command Compatibility
-------------------------

Create Keyspace
^^^^^^^^^^^^^^^
+-----------------------------------+-------------------------------------+
| Feature                           | Support                             |
+===================================+=====================================+
|DURABLE_WRITES                     | |v|                                 |
+-----------------------------------+-------------------------------------+
|IF NOT EXISTS                      | |v|                                 |
+-----------------------------------+-------------------------------------+
|WITH REPLICATION                   | |v|    (see below)                  |
+-----------------------------------+-------------------------------------+

Create Keyspace with Replication
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

+-----------------------------------+-------------------------------------+
| Feature                           | Support                             |
+===================================+=====================================+
|SimpleStrategy                     | |v|                                 |
+-----------------------------------+-------------------------------------+
|NetworkTopologyStrategy            | |v|                                 |
+-----------------------------------+-------------------------------------+
|OldNetworkTopologyStrategy         | |x|                                 |
+-----------------------------------+-------------------------------------+

Create Table
^^^^^^^^^^^^
+-----------------------------------+-------------------------------------+
| Feature                           | Support                             |
+===================================+=====================================+
| Primary key column                | |v|                                 |
+-----------------------------------+-------------------------------------+
| Compound primary key              | |v|                                 |
+-----------------------------------+-------------------------------------+
| Composite partition key           | |v|                                 |
+-----------------------------------+-------------------------------------+
| Clustering order                  | |v|                                 |
+-----------------------------------+-------------------------------------+
| Static column                     | |v|                                 |
+-----------------------------------+-------------------------------------+



Create Table Att
................

+-----------------------------------+-------------------------------------+
| Feature                           | Support                             |
+===================================+=====================================+
|bloom_filter_fp_chance             | |v|                                 |
+-----------------------------------+-------------------------------------+
|caching                            | |x| (ignored)                       |
+-----------------------------------+-------------------------------------+
|comment                            | |v|                                 |
+-----------------------------------+-------------------------------------+
|compaction                         | |v|                                 |
+-----------------------------------+-------------------------------------+
|compression                        | |v|                                 |
+-----------------------------------+-------------------------------------+
|dclocal_read_repair_chance         ||v|                                  |
+-----------------------------------+-------------------------------------+
|default_time_to_live               ||v|                                  |
+-----------------------------------+-------------------------------------+
| gc_grace_seconds                  ||v|                                  |
+-----------------------------------+-------------------------------------+
| index_interval                    | |x|                                 |
+-----------------------------------+-------------------------------------+
| max_index_interval                ||v|                                  |
+-----------------------------------+-------------------------------------+
| memtable_flush_period_in_ms       | |x| (ignored)                       |
+-----------------------------------+-------------------------------------+
| min_index_interval                ||v|                                  |
+-----------------------------------+-------------------------------------+
|populate_io_cache_on_flush         | |x|                                 |
+-----------------------------------+-------------------------------------+
|read_repair_chance                 ||v|                                  |
+-----------------------------------+-------------------------------------+
|replicate_on_write                 | |x|                                 |
+-----------------------------------+-------------------------------------+
|speculative_retry                  | ``ALWAYS``, ``NONE``                |
+-----------------------------------+-------------------------------------+

Create Table Compaction
.......................

+----------------------------------------+-------------------------------------+
| Feature                                | Support                             |
+========================================+=====================================+
| SizeTieredCompactionStrategy_ (STCS)   | |v|                                 |
+----------------------------------------+-------------------------------------+
|LeveledCompactionStrategy_ (LCS)        | |v|                                 |
+----------------------------------------+-------------------------------------+
|DateTieredCompactionStrategy (DTCS)     | |v|  :sup:`*`                       |
+----------------------------------------+-------------------------------------+
|TimeWindowCompactionStrategy_ (TWCS)    | |v|                                 |
+----------------------------------------+-------------------------------------+

:sup:`*`  Deprecated in ScyllaDB 4.0, use TWCS instead

Create Table Compression
........................

+----------------------------------------+-------------------------------------+
| Feature                                | Support                             |
+========================================+=====================================+
|sstable_compression LZ4Compressor       | |v|                                 |
+----------------------------------------+-------------------------------------+
|sstable_compression SnappyCompressor    | |v|                                 |
+----------------------------------------+-------------------------------------+
|sstable_compression DeflateCompressor   | |v|                                 |
+----------------------------------------+-------------------------------------+
|chunk_length_kb                         | |v|                                 |
+----------------------------------------+-------------------------------------+
|crc_check_chance                        | |x|                                 |
+----------------------------------------+-------------------------------------+

Alter Commands
..............

+----------------------------------------+-------------------------------------+
| Feature                                | Support                             |
+========================================+=====================================+
|ALTER KEYSPACE                          | |v|                                 |
+----------------------------------------+-------------------------------------+
|ALTER TABLE                             | |v|                                 |
+----------------------------------------+-------------------------------------+
|ALTER TYPE                              | |v|                                 |
+----------------------------------------+-------------------------------------+
|ALTER USER                              | |v|                                 |
+----------------------------------------+-------------------------------------+
|ALTER ROLE                              | |v|                                 |
+----------------------------------------+-------------------------------------+

Data Manipulation
.................

+----------------------------------------+-------------------------------------+
| Feature                                | Support                             |
+========================================+=====================================+
|BATCH                                   | |v|                                 |
+----------------------------------------+-------------------------------------+
|INSERT                                  | |v|                                 |
+----------------------------------------+-------------------------------------+
|Prepared Statements                     | |v|                                 |
+----------------------------------------+-------------------------------------+
|SELECT                                  | |v|                                 |
+----------------------------------------+-------------------------------------+
|TRUNCATE                                | |v|                                 |
+----------------------------------------+-------------------------------------+
|UPDATE                                  | |v|                                 |
+----------------------------------------+-------------------------------------+
|USE                                     | |v|                                 |
+----------------------------------------+-------------------------------------+

Create Commands
...............

+----------------------------------------+-------------------------------------+
| Feature                                | Support                             |
+========================================+=====================================+
|CREATE TRIGGER                          ||x|                                  |
+----------------------------------------+-------------------------------------+
|CREATE USER                             | |v|                                 |
+----------------------------------------+-------------------------------------+
|CREATE ROLE                             | |v|                                 |
+----------------------------------------+-------------------------------------+

Drop Commands
.............

+----------------------------------------+-------------------------------------+
| Feature                                | Support                             |
+========================================+=====================================+
|DROP KEYSPACE                           | |v|                                 |
+----------------------------------------+-------------------------------------+
|DROP TABLE                              | |v|                                 |
+----------------------------------------+-------------------------------------+
|DROP TRIGGER                            | |x|                                 |
+----------------------------------------+-------------------------------------+
|DROP TYPE                               | |v|                                 |
+----------------------------------------+-------------------------------------+
|DROP USER                               | |v|                                 |
+----------------------------------------+-------------------------------------+
|DROP ROLE                               | |v|                                 |
+----------------------------------------+-------------------------------------+

Roles and Permissions
.....................

+----------------------------------------+-------------------------------------+
| Feature                                | Support                             |
+========================================+=====================================+
|GRANT PERMISSIONS                       | |v|                                 |
+----------------------------------------+-------------------------------------+
|GRANT ROLE                              | |v|                                 |
+----------------------------------------+-------------------------------------+
|LIST PERMISSIONS                        | |v|                                 |
+----------------------------------------+-------------------------------------+
|LIST USERS                              | |v|                                 |
+----------------------------------------+-------------------------------------+
|LIST ROLES                              | |v|                                 |
+----------------------------------------+-------------------------------------+
|REVOKE PERMISSIONS                      | |v|                                 |
+----------------------------------------+-------------------------------------+
|REVOKE ROLE                             | |v|                                 |
+----------------------------------------+-------------------------------------+

Materialized Views
..................

+----------------------------------------+-------------------------------------+
| Feature                                | Support                             |
+========================================+=====================================+
| MATERIALIZED VIEW                      | |v|                                 |
+----------------------------------------+-------------------------------------+
| ALTER MATERIALIZED VIEW                | |v|                                 |
+----------------------------------------+-------------------------------------+
|CREATE MATERIALIZED VIEW                | |v|                                 |
+----------------------------------------+-------------------------------------+
|DROP MATERIALIZED VIEW                  | |v|                                 |
+----------------------------------------+-------------------------------------+

Index commands
..............

+----------------------------------------+-------------------------------------+
| Feature                                | Support                             |
+========================================+=====================================+
|INDEX                                   | |v|                                 |
+----------------------------------------+-------------------------------------+
|CREATE INDEX                            | |v|                                 |
+----------------------------------------+-------------------------------------+
|DROP INDEX                              | |v|                                 |
+----------------------------------------+-------------------------------------+


.. _SizeTieredCompactionStrategy: /getting-started/compaction/#size-tiered-compaction-strategy

.. _LeveledCompactionStrategy: /getting-started/compaction/#leveled-compaction-strategy

.. _TimeWindowCompactionStrategy: /getting-started/compaction/#time-window-compactionstrategy

.. _1432: https://github.com/scylladb/scylla/issues/1432

.. include:: /rst_include/apache-copyrights-index.rst

.. include:: /rst_include/apache-copyrights-index-all-attributes.rst
