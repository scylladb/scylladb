=========================================
Raft Consensus Algorithm in ScyllaDB
=========================================

Introduction
--------------
ScyllaDB was originally designed, following Apache Cassandra, to use gossip for topology and schema updates and the Paxos consensus algorithm for
strong data consistency (:doc:`LWT </features/lwt>`). To achieve stronger consistency without performance penalty, ScyllaDB has turned to Raft - a consensus algorithm designed as an alternative to both gossip and Paxos.

Raft is a consensus algorithm that implements a distributed, consistent, replicated log across members (nodes). Raft implements consensus by first electing a distinguished leader, then giving the leader complete responsibility for managing the replicated log. The leader accepts log entries from clients, replicates them on other servers, and tells servers when it is safe to apply log entries to their state machines.

Raft uses a heartbeat mechanism to trigger a leader election. All servers start as followers and remain in the follower state as long as they receive valid RPCs (heartbeat) from a leader or candidate. A leader sends periodic heartbeats to all followers to maintain his authority (leadership). Suppose a follower receives no communication over a period called the election timeout. In that case, it assumes no viable leader and begins an election to choose a new leader.

Leader selection is described in detail in the `Raft paper <https://raft.github.io/raft.pdf>`_.

ScyllaDB uses Raft to:

* Manage schema updates in every node. Any schema update, like ALTER, CREATE or DROP TABLE, 
  is first committed as an entry in the replicated Raft log, and, once stored on most replicas,
  applied to all nodes **in the same order**, even in the face of a node or network failures.
* Manage cluster topology. All topology operations are consistently sequenced, making topology
  updates fast and safe.

.. _raft-quorum-requirement:

Quorum Requirement
-------------------

Raft requires at least a quorum of nodes in a cluster to be available. If multiple nodes fail
and the quorum is lost, the cluster is unavailable for schema updates or topology changes. See :ref:`Handling Failures <raft-handling-failures>`
for information on how to handle failures.

Note that when you have a two-DC cluster with the same number of nodes in each DC, the cluster will lose the quorum if one
of the DCs is down.
**We recommend configuring three DCs per cluster to ensure that the cluster remains available and operational when one DC is down.**
In the case of two-DC cluster with the same number of nodes in each DC, it is sufficient for the third DC to contain only one node.
That node can be configured with ``join_ring=false`` and run on a weaker machine.
See :doc:`Configuration Parameters </reference/configuration-parameters/>` for details about the ``join_ring`` option.

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

.. _raft-topology-changes:

Consistent Topology with Raft
-----------------------------------------------------------------

ScyllaDB uses Raft to manage cluster topology. With Raft-managed topology 
enabled, all topology operations are internally sequenced in a consistent 
way. A centralized coordination process ensures that topology metadata is 
synchronized across the nodes on each step of a topology change procedure. 
This makes topology updates fast and safe, as the cluster administrator can 
trigger many topology operations concurrently, and the coordination process 
will safely drive all of them to completion. For example, multiple nodes can 
be bootstrapped concurrently, which couldn't be done with the old 
gossip-based topology.

.. note::

    Enabling consistent topology changes is mandatory in versions 2025.2 and later. If consistent topology changes are
    disabled in your cluster, you need to follow the instructions in
    `Enable Consistent Topology Updates <https://docs.scylladb.com/manual/branch-2025.1/upgrade/upgrade-guides/upgrade-guide-from-2024.x-to-2025.1/enable-consistent-topology.html>`_.

    If you are uncertain whether consistent topology changes are enabled, refer to the guide below.

.. _verifying-consistent-topology-changes-enabled:

Verifying that consistent topology changes are enabled
-----------------------------------------------------------------

You can verify that consistent topology management is enabled on your cluster in two ways:

#. By querying the ``system.topology`` table:

    .. code-block:: cql

        cqlsh> SELECT upgrade_state FROM system.topology;

   The query should return ``done`` after upgrade is complete:

    .. code-block:: console

        upgrade_state
        ---------------
                done

        (1 rows)

    An empty result or a value of ``not_upgraded`` means that upgrade has not started yet. Any other value means that upgrade is in progress.

#. By sending a GET HTTP request to the `/`storage_service/raft_topology/upgrade`` endpoint. For example, you can do it with ``curl`` like this:

    .. code-block:: bash

        curl -X GET "http://127.0.0.1:10000/storage_service/raft_topology/upgrade"

   It returns a JSON string, with the same meaning and value as the ``upgrade_state`` column in ``system.topology`` (see the previous point).


.. _raft-handling-failures:

Handling Failures
------------------

See :doc:`Handling Node Failures </troubleshooting/handling-node-failures>`.

.. _raft-learn-more:

Learn More About Raft
----------------------
* `The Raft Consensus Algorithm <https://raft.github.io/>`_
* `Achieving NoSQL Database Consistency with Raft in ScyllaDB <https://www.scylladb.com/tech-talk/achieving-nosql-database-consistency-with-raft-in-scylla/>`_ - A tech talk by Konstantin Osipov
* `Making Schema Changes Safe with Raft <https://www.scylladb.com/presentations/making-schema-changes-safe-with-raft/>`_ - A ScyllaDB Summit talk by Konstantin Osipov (register for access)
* `The Future of Consensus in ScyllaDB 5.0 and Beyond <https://www.scylladb.com/presentations/the-future-of-consensus-in-scylladb-5-0-and-beyond/>`_ - A ScyllaDB Summit talk by Tomasz Grabiec (register for access)

