=========================================
Raft Consensus Algorithm in ScyllaDB
=========================================

Introduction
--------------
ScyllaDB was originally designed, following Apache Cassandra, to use gossip for topology and schema updates and the Paxos consensus algorithm for
strong data consistency (:doc:`LWT </using-scylla/lwt>`). To achieve stronger consistency without performance penalty, ScyllaDB 5.x has turned to Raft - a consensus algorithm designed as an alternative to both gossip and Paxos.

Raft is a consensus algorithm that implements a distributed, consistent, replicated log across members (nodes). Raft implements consensus by first electing a distinguished leader, then giving the leader complete responsibility for managing the replicated log. The leader accepts log entries from clients, replicates them on other servers, and tells servers when it is safe to apply log entries to their state machines.

Raft uses a heartbeat mechanism to trigger a leader election. All servers start as followers and remain in the follower state as long as they receive valid RPCs (heartbeat) from a leader or candidate. A leader sends periodic heartbeats to all followers to maintain his authority (leadership). Suppose a follower receives no communication over a period called the election timeout. In that case, it assumes no viable leader and begins an election to choose a new leader.

Leader selection is described in detail in the `Raft paper <https://raft.github.io/raft.pdf>`_.

ScyllaDB 5.x may use Raft to maintain schema updates in every node (see below). Any schema update, like ALTER, CREATE or DROP TABLE, is first committed as an entry in the replicated Raft log, and, once stored on most replicas, applied to all nodes **in the same order**, even in the face of a node or network failures.

Following ScyllaDB 5.x releases will use Raft to guarantee consistent topology updates similarly.

.. _raft-quorum-requirement:

Quorum Requirement
-------------------

Raft requires at least a quorum of nodes in a cluster to be available. If multiple nodes fail
and the quorum is lost, the cluster is unavailable for schema updates. See :ref:`Handling Failures <raft-handling-failures>`
for information on how to handle failures.


Upgrade Considerations for SyllaDB 5.0 and Later
==================================================

Note that when you have a two-DC cluster with the same number of nodes in each DC, the cluster will lose the quorum if one
of the DCs is down.
**We recommend configuring three DCs per cluster to ensure that the cluster remains available and operational when one DC is down.**

Enabling Raft
---------------

Enabling Raft in ScyllaDB 5.0 and 5.1
=====================================

.. warning::
  In ScyllaDB 5.0 and 5.1, Raft is an experimental feature.

It is not possible to enable Raft in an existing cluster in ScyllaDB 5.0 and 5.1.
In order to have a Raft-enabled cluster in these versions, you must create a new cluster with Raft enabled from the start.

.. warning::

   **Do not** use Raft in production clusters in ScyllaDB 5.0 and 5.1. Such clusters won't be able to correctly upgrade to ScyllaDB 5.2.

   Use Raft only for testing and experimentation in clusters which can be thrown away.

.. warning::
    Once enabled, Raft cannot be disabled on your cluster. The cluster nodes will fail to restart if you remove the Raft feature.

When creating a new cluster, add ``raft`` to the list of experimental features in your ``scylla.yaml`` file:

.. code-block:: yaml

    experimental_features:
     - raft

Verifying that Raft is enabled
===============================
You can verify that Raft is enabled on your cluster in one of the following ways:

* Retrieve the list of supported features by running:

   .. code-block:: sql

       cqlsh> SELECT supported_features FROM system.local;
    
  With Raft enabled, the list of supported features in the output includes ``SUPPORTS_RAFT_CLUSTER_MANAGEMENT``. For example:

   .. code-block:: console
       :class: hide-copy-button
       
       supported_features
       -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
       CDC,CDC_GENERATIONS_V2,COMPUTED_COLUMNS,CORRECT_COUNTER_ORDER,CORRECT_IDX_TOKEN_IN_SECONDARY_INDEX,CORRECT_NON_COMPOUND_RANGE_TOMBSTONES,CORRECT_STATIC_COMPACT_IN_MC,COUNTERS,DIGEST_FOR_NULL_VALUES,DIGEST_INSENSITIVE_TO_EXPIRY,DIGEST_MULTIPARTITION_READ,HINTED_HANDOFF_SEPARATE_CONNECTION,INDEXES,LARGE_PARTITIONS,LA_SSTABLE_FORMAT,LWT,MATERIALIZED_VIEWS,MC_SSTABLE_FORMAT,MD_SSTABLE_FORMAT,ME_SSTABLE_FORMAT,NONFROZEN_UDTS,PARALLELIZED_AGGREGATION,PER_TABLE_CACHING,PER_TABLE_PARTITIONERS,RANGE_SCAN_DATA_VARIANT,RANGE_TOMBSTONES,ROLES,ROW_LEVEL_REPAIR,SCHEMA_TABLES_V3,SEPARATE_PAGE_SIZE_AND_SAFETY_LIMIT,STREAM_WITH_RPC_STREAM,SUPPORTS_RAFT_CLUSTER_MANAGEMENT,TOMBSTONE_GC_OPTIONS,TRUNCATION_TABLE,UDA,UNBOUNDED_RANGE_TOMBSTONES,VIEW_VIRTUAL_COLUMNS,WRITE_FAILURE_REPLY,XXHASH

* Retrieve the list of experimental features by running:

   .. code-block:: sql

       cqlsh> SELECT value FROM system.config WHERE name = 'experimental_features'
    
  With Raft enabled, the list of experimental features in the output includes ``raft``.

.. _raft-schema-changes:

Safe Schema Changes with Raft
-------------------------------
In ScyllaDB, schema is based on :doc:`Data Definition Language (DDL) </cql/ddl>`. In earlier ScyllaDB versions, schema changes were tracked via the gossip protocol, which might lead to schema conflicts if the updates are happening concurrently.

Implementing Raft eliminates schema conflicts and allows full automation of DDL changes under any conditions, as long as a quorum
of nodes in the cluster is available. The following examples illustrate how Raft provides the solution to problems with schema changes.

* A network partition may lead to a split-brain case, where each subset of nodes has a different version of the schema.

     With Raft, after a network split, the majority of the cluster can continue performing schema changes, while the minority needs to wait until it can rejoin the majority. Data manipulation statements on the minority can continue unaffected, provided the :ref:`quorum requirement <raft-quorum-requirement>` is satisfied.

* Two or more conflicting schema updates are happening at the same time. For example, two different columns with the same definition are simultaneously added to the cluster. There is no effective way to resolve the conflict - the cluster will employ the schema with the most recent timestamp, but changes related to the shadowed table will be lost.

     With Raft, concurrent schema changes are safe.



In summary, Raft makes schema changes safe, but it requires that a quorum of nodes in the cluster is available.


.. _raft-handling-failures:

Handling Failures
------------------
Raft requires a quorum of nodes in a cluster to be available. If one or more nodes are down, but the quorum is live, reads, writes,
and schema udpates proceed unaffected.
When the node that was down is up again, it first contacts the cluster to fetch the latest schema and then starts serving queries.

The following examples show the recovery actions depending on the number of nodes and DCs in your cluster.

Examples
=========

.. list-table:: Cluster A: 1 datacenter, 3 nodes
   :widths: 20 40 40
   :header-rows: 1

   * - Failure
     - Consequence
     - Action to take
   * - 1 node
     - Schema updates are possible and safe.
     - Try restarting the node. If the node is dead, :doc:`replace it with a new node </operating-scylla/procedures/cluster-management/replace-dead-node/>`.
   * - 2 nodes
     - Cluster is not fully opetarional. The data is available for reads and writes, but schema changes are impossible.
     - Restart at least 1 of the 2 nodes that are down to regain quorum. If you can’t recover at least 1 of the 2 nodes, contact `ScyllaDB support <https://www.scylladb.com/product/support/>`_ for assistance.
   * - 1 datacenter
     - Cluster is not fully opetarional. The data is available for reads and writes, but schema changes are impossible.
     - When the DC comes back online, restart the nodes. If the DC does not come back online and nodes are lost, :doc:`restore the latest cluster backup into a new cluster </operating-scylla/procedures/backup-restore/restore/>`. You can contact `ScyllaDB support <https://www.scylladb.com/product/support/>`_ for assistance.


.. list-table:: Cluster B: 2 datacenters, 6  nodes (3 nodes per DC)
   :widths: 20 40 40
   :header-rows: 1

   * - Failure
     - Consequence
     - Action to take
   * - 1-2 nodes
     - Schema updates are possible and safe.
     - Try restarting the node(s). If the node is dead, :doc:`replace it with a new node </operating-scylla/procedures/cluster-management/replace-dead-node/>`.
   * - 3 nodes
     - Cluster is not fully opetarional. The data is available for reads and writes, but schema changes are impossible.
     - Restart 1 of the 3 nodes that are down to regain quorum. If you can’t recover at least 1 of the 3 failed nodes, contact `ScyllaDB support <https://www.scylladb.com/product/support/>`_ for assistance.
   * - 1DC
     - Cluster is not fully opetarional. The data is available for reads and writes, but schema changes are impossible.
     - When the DCs come back online, restart the nodes. If the DC fails to come back online and the nodes are lost, :doc:`restore the latest cluster backup into a new cluster </operating-scylla/procedures/backup-restore/restore/>`. You can contact `ScyllaDB support <https://www.scylladb.com/product/support/>`_ for assistance.


.. list-table:: Cluster C: 3 datacenter, 9  nodes (3 nodes per DC)
   :widths: 20 40 40
   :header-rows: 1

   * - Failure
     - Consequence
     - Action to take
   * - 1-4 nodes
     - Schema updates are possible and safe.
     - Try restarting the nodes. If the nodes are dead, :doc:`replace them with new nodes </operating-scylla/procedures/cluster-management/replace-dead-node-or-more/>`.
   * - 1 DC
     - Schema updates are possible and safe.
     - When the DC comes back online, try restarting the nodes in the cluster. If the nodes are dead, :doc:`add 3 new nodes in a new region </operating-scylla/procedures/cluster-management/add-dc-to-existing-dc/>`.
   * - 2 DCs
     - Cluster is not fully opetarional. The data is available for reads and writes, but schema changes are impossible.
     - When the DCs come back online, restart the nodes. If at least one DC fails to come back online and the nodes are lost, :doc:`restore the latest cluster backup into a new cluster </operating-scylla/procedures/backup-restore/restore/>`. You can contact `ScyllaDB support <https://www.scylladb.com/product/support/>`_ for assistance.
     

.. _raft-learn-more:

Learn More About Raft
----------------------
* `The Raft Consensus Algorithm <https://raft.github.io/>`_
* `Achieving NoSQL Database Consistency with Raft in ScyllaDB <https://www.scylladb.com/tech-talk/achieving-nosql-database-consistency-with-raft-in-scylla/>`_ - A tech talk by Konstantin Osipov
* `Making Schema Changes Safe with Raft <https://www.scylladb.com/presentations/making-schema-changes-safe-with-raft/>`_ - A Scylla Summit talk by Konstantin Osipov (register for access)
* `The Future of Consensus in ScyllaDB 5.0 and Beyond <https://www.scylladb.com/presentations/the-future-of-consensus-in-scylladb-5-0-and-beyond/>`_ - A Scylla Summit talk by Tomasz Grabiec (register for access)


