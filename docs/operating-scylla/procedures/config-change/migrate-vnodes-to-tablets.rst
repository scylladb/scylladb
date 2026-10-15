Migrate a Keyspace from Vnodes to Tablets
==========================================

This procedure describes how to migrate an existing keyspace from vnodes
to tablets. Tablets are designed to be the long-term replacement for vnodes,
offering numerous benefits such as faster topology operations, automatic load
balancing, automatic cleanups, and improved streaming performance. Migrating to
tablets is strongly recommended. See :doc:`Data Distribution with Tablets </architecture/tablets/>`
for details.

ℹ️ The migration currently has some `Limitations`_, which will be lifted in
future releases.

.. note::

   The migration is an online operation. This means that the keyspace remains
   fully available to users throughout the migration, provided that its
   replication factor is greater than 1. Reads and writes continue to be served
   using vnodes until the migration is finished.

.. warning::

   During the migration, you should expect degraded performance on the migrating
   keyspace. The reasons are the following:

   * **Rolling restart**: Each node must upgrade its storage from vnodes to
     tablets. This is an offline operation happening on startup, so a restart is
     needed. Upon restart, each node performs a heavy and time-consuming
     resharding operation to reorganize its data based on tablets, and remains
     offline until this operation completes. Resharding may last from minutes to
     hours, depending on the amount of data that the node holds. At this time,
     the node cannot serve any requests.
   * **Unbalanced tablets**: The initial tablet layout mirrors the vnode layout.
     The tablet load balancer does not rebalance tablets until the migration is
     finished, so some shards may carry more data than others during the
     migration. The imbalance is expected to be more prominent in clusters with
     very large nodes (hundreds of vCPUs).
   * **Loss of shard awareness**: During the migration and until the rolling
     restart is complete, the cluster is in a mixed state with some nodes using
     vnodes and others using tablets. In this state, queries may cause
     cross-shard operations within nodes, reducing performance.

   The performance will return to normal after the migration finishes and the
   tablet load balancer rebalances the data.

Prerequisites
-------------

Cluster state
~~~~~~~~~~~~~

* All nodes in the cluster must be **up and running**. You can check the status
  of all nodes with
  :doc:`nodetool status </operating-scylla/nodetool-commands/status/>`.
* All nodes must be running ScyllaDB 2026.2 or later.
* Disk utilization on every node should be **below 50%**. This headroom helps
  prevent problems during the migration's resharding phase, which temporarily
  requires additional disk space.

Keyspace and table configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* The keyspace's replication strategy must be the **NetworkTopologyStrategy**,
  and its replication factor must equal the number of racks to ensure that the
  keyspace will be :term:`RF-rack-valid <RF-rack-valid keyspace>` after the
  migration. If your configuration requires changes, see
  :doc:`Update Topology Strategy From Simple to Network </operating-scylla/procedures/cluster-management/update-topology-strategy-from-simple-to-network>`
  and :doc:`How to Safely Increase the Replication Factor </kb/rf-increase>`.
* All tables in the keyspace should have ``tombstone_gc`` mode set to ``repair``
  to prevent tombstone garbage collection from running during migration.
  For guidance, see :ref:`Tombstone Garbage Collection <ddl-tombstones-gc>`.

Limitations
-----------

The current migration procedure has the following limitations:

* **Multi-DC clusters** are supported, but every datacenter in the migrating
  keyspace's replication strategy must have a replication factor of at least one.
  Datacenters with a replication factor of zero (RF=0) are not yet supported.
* Clusters with :doc:`zero-token nodes </architecture/zero-token-nodes>` are
  not yet supported.

* **No schema changes** during the migration. Do not create, alter, or drop
  tables in the migrating keyspace until the migration is finished.
* **No topology changes** during the migration. Do not add, remove, decommission,
  replace, or rebuild nodes while a migration is in progress.
* **No ScyllaDB version upgrades** during the migration. Every node must run the
  same version from the moment a migration is started until it is finalized.
* **No repair** operations during the migration. Do not run ``nodetool repair``
  on the migrating keyspace while a migration is in progress.
* **No TRUNCATE** on tables in the migrating keyspace during the migration.
* Only **CQL base tables** can be migrated. Materialized views, secondary
  indexes, CDC tables, and Alternator tables are not supported.
* Tables with **counters** or **LWTs** cannot be migrated.
* Only tables using the **Incremental Compaction Strategy (ICS)** can be migrated.
  Other compaction strategies are not supported.

Overview
--------

The migration consists of three phases:

1. **Prepare**: Create tablet maps for the tables being migrated. Each tablet
   inherits its token range and replica set from the corresponding vnode range.
   You can prepare all tables of the keyspace at once, or one table at a time
   (see :ref:`migrating-one-table-at-a-time`).
2. **Storage upgrade**: Restart each node one at a time, upgrading its storage
   from vnodes to tablets. Upon restart, the node begins resharding data into
   tablets. This is a storage-layer operation and is unrelated to ScyllaDB
   version upgrades.
3. **Finalize**: Once all nodes have been upgraded, commit the migration by
   clearing the migration state and switching the keyspace schema to tablets.

During the first two phases, the migration is reversible; you can roll back to
vnodes. However, once the migration is finalized, it cannot be reversed.

.. note::

   In the following sections, any reference to "upgrade" or "downgrade" of a
   node will refer to the migration of its storage from vnodes to tablets or
   vice versa. Do not confuse it with version upgrades/downgrades.

Procedure
---------

#. Prepare the keyspace for migration:

   #. Create tablet maps for all tables in the keyspace:

      .. code-block:: console

         scylla nodetool migrate-to-tablets start <keyspace>

   #. Verify that the keyspace is in ``migrating_to_tablets`` state and no node needs a restart yet:

      .. code-block:: console

         scylla nodetool migrate-to-tablets status <keyspace>

      **Example:**

      .. code-block:: console

         $ scylla nodetool migrate-to-tablets status ks
         Keyspace: ks
         Status: migrating_to_tablets
         Migrating Tables: All
         Direction: tablets

         Nodes:
         Host ID                                Address    Status
         99d8de76-3954-4727-911a-6a07251b180c   10.0.0.1   Restarted
         0b5fd6f6-9670-4faf-a480-ad58cf119007   10.0.0.2   Restarted
         017dd39a-3d06-4c8a-8ac4-379f9e595607   10.0.0.3   Restarted

   .. _upgrade-nodes:

#. Upgrade all nodes to tablets:

   #. Pick a node.

   #. Mark the node for upgrade to tablets:

      .. note::

         This is a node-local operation. Use the IP address of the node that
         you are upgrading.

      .. caution::

         Do not mark more than one node for upgrade at the same time. Even if
         you restart them serially, unexpected restarts can happen for various
         reasons (crashes, power failures, etc.) leading to parallel node
         upgrades which can reduce availability.

      .. code-block:: console

         scylla nodetool -h <node-ip> migrate-to-tablets upgrade

   #. Verify that the node status changed to ``Needs restart``:

      .. code-block:: console

         scylla nodetool migrate-to-tablets status <keyspace>

      **Example:**

      .. code-block:: console

         $ scylla nodetool migrate-to-tablets status ks
         Keyspace: ks
         Status: migrating_to_tablets
         Migrating Tables: All
         Direction: tablets

         Nodes:
         Host ID                                Address    Status
         99d8de76-3954-4727-911a-6a07251b180c   10.0.0.1   Needs restart  <---
         0b5fd6f6-9670-4faf-a480-ad58cf119007   10.0.0.2   Restarted
         017dd39a-3d06-4c8a-8ac4-379f9e595607   10.0.0.3   Restarted

   #. Drain and stop the node:

      .. code-block:: console

         scylla nodetool -h <node-ip> drain

      .. include:: /rst_include/scylla-commands-stop-index.rst

   #. Restart the node:

      .. include:: /rst_include/scylla-commands-start-index.rst

   #. Wait until the node is UP and has returned to the ScyllaDB cluster using :doc:`nodetool status </operating-scylla/nodetool-commands/status/>`.
      This operation may take a long time due to resharding. To monitor
      resharding progress, use the task manager API:

      .. code-block:: console

         scylla nodetool tasks list compaction -h <node-ip> --keyspace <keyspace> | grep -i reshard

   #. Verify that the node status changed to ``Restarted``:

      .. code-block:: console

         scylla nodetool migrate-to-tablets status <keyspace>

      **Example:**

      .. code-block:: console

         $ scylla nodetool migrate-to-tablets status ks
         Keyspace: ks
         Status: migrating_to_tablets
         Migrating Tables: All
         Direction: tablets

         Nodes:
         Host ID                                Address    Status
         99d8de76-3954-4727-911a-6a07251b180c   10.0.0.1   Restarted  <---
         0b5fd6f6-9670-4faf-a480-ad58cf119007   10.0.0.2   Restarted
         017dd39a-3d06-4c8a-8ac4-379f9e595607   10.0.0.3   Restarted

   #. Move to the next node and repeat from step a until all nodes are upgraded.

#. Finalize the migration:

   .. warning::

      Finalization **cannot be undone**. Once the migration is finalized, the
      keyspace cannot be switched back to vnodes.

   #. Issue the finalization request:

      .. code-block:: console

         scylla nodetool migrate-to-tablets finalize <keyspace>

   #. Verify that the keyspace status changed to ``tablets``:

      .. code-block:: console

         scylla nodetool migrate-to-tablets status <keyspace>

      **Example:**

      .. code-block:: console

         $ scylla nodetool migrate-to-tablets finalize ks
         Keyspace: ks
         Status: tablets

   #. Optionally, wait for tablet layout normalization to complete:

      After finalization, a tablet normalization process takes place in the
      background which transforms the tablets of all migrated tables into a
      power-of-two layout. Until power-of-two convergence is complete, the
      tablet layout of all these tables will be suboptimal, causing degraded
      performance. Run the following command repeatedly to monitor the per-table
      status of this background work:

      .. code-block:: console

         scylla nodetool migrate-to-tablets status <keyspace> --with-tablet-status

      **Example:**

      .. code-block:: console

         $ scylla nodetool migrate-to-tablets status ks --with-tablet-status
         Keyspace: ks
         Status: tablets

         Tablet info:
           Pow2 tablet layout convergence: in progress
           Tables converging: 2/3

         Table   Status       Tablets   Target
         t1      converging   2301      2048
         t2      converging   2176      2048
         t3      converged    2048      -

.. _migrating-one-table-at-a-time:

Migrating One Table at a Time
-----------------------------

Preparing a table makes every node reshard it on its next restart. For a
keyspace with many tables, or with very large ones, resharding them all in a
single restart can take a long time and put the node under heavy load.

A keyspace can instead be migrated in rounds, a few tables at a time. Each
round follows the same three phases as the main procedure, and ends by
finalizing the tables it migrated.

Procedure
~~~~~~~~~

#. Prepare the tables for this round, naming one table at a time:

   .. code-block:: console

      scylla nodetool migrate-to-tablets start <keyspace>.<table>

   Repeat for each table you want in this round. As with a whole keyspace,
   tables can only be prepared before any node has been marked for upgrade.

#. Upgrade and restart the nodes, as in step 2 of the main procedure. Each
   node reshards the tables prepared in this round.

#. Finalize each table:

   .. code-block:: console

      scylla nodetool migrate-to-tablets finalize <keyspace>.<table>

   A finalized table runs on tablets for good. It is handed to the load
   balancer, which begins converging it to its final tablet count, and it can
   no longer be rolled back.

#. Repeat for the remaining tables. Finalizing the last table of the keyspace
   also switches the keyspace itself to tablets, so no extra step is needed.

``status`` names the tables currently being migrated - those prepared but not
yet finalized - and says which nodes still owe a restart for them. It prints
``All`` instead of a list when every table of the keyspace is being migrated:

.. code-block:: console

   $ scylla nodetool migrate-to-tablets status ks
   Keyspace: ks
   Status: migrating_to_tablets
   Migrating Tables: t3
   Direction: tablets

   Nodes:
   Host ID                                Address    Status
   99d8de76-3954-4727-911a-6a07251b180c   10.0.0.1   Restarted
   0b5fd6f6-9670-4faf-a480-ad58cf119007   10.0.0.2   Needs restart

Rolling back
~~~~~~~~~~~~

Tables that have been prepared but not yet finalized can be rolled back with
the `Rollback Procedure`_: their tablet maps are dropped and they return to
vnodes. Tables finalized in an earlier round keep their tablets and are not
affected, and tables that were never prepared are untouched.

Finalizing is the point of no return, for a table just as for a keyspace.

.. note::

   The restrictions in `Limitations`_ apply until the keyspace itself has
   switched to tablets, which happens when its last table is finalized - not
   merely until the tables you care about have been migrated.

Rollback Procedure
------------------

.. note::

   Rollback is only possible **before finalization**. Once the migration is
   finalized, it cannot be reversed.

If you need to abort the migration **before finalization**, you can roll back
by downgrading each node back to vnodes. The rollback procedure is the
following:

#. List the nodes. Every node that was marked for upgrade has to be downgraded
   and restarted, whether or not it has already restarted, so the list is simply
   every node shown:

   .. code-block:: console

      scylla nodetool migrate-to-tablets status <keyspace>

   **Example:**

   .. code-block:: console

      $ scylla nodetool migrate-to-tablets status ks
      Keyspace: ks
      Status: migrating_to_tablets
      Migrating Tables: All
      Direction: tablets

      Nodes:
      Host ID                                Address    Status
      99d8de76-3954-4727-911a-6a07251b180c   10.0.0.1   Restarted  <---
      0b5fd6f6-9670-4faf-a480-ad58cf119007   10.0.0.2   Needs restart  <---
      017dd39a-3d06-4c8a-8ac4-379f9e595607   10.0.0.3   Restarted

#. For **each upgraded or upgrading node** in the cluster, perform a downgrade
   (one node at a time):

   #. Mark the node for downgrade:

      .. code-block:: console

         scylla nodetool -h <node-ip> migrate-to-tablets downgrade

   #. Check the node status. A node that has anything to revert shows
      ``Needs restart``; one that was never restarted after being marked shows
      ``Restarted``, having nothing to revert:

      .. code-block:: console

         scylla nodetool migrate-to-tablets status <keyspace>

      **Example:**

      .. code-block:: console

         $ scylla nodetool migrate-to-tablets status ks
         Keyspace: ks
         Status: migrating_to_tablets
         Migrating Tables: All
         Direction: vnodes

         Nodes:
         Host ID                                Address    Status
         99d8de76-3954-4727-911a-6a07251b180c   10.0.0.1   Needs restart  <---
         0b5fd6f6-9670-4faf-a480-ad58cf119007   10.0.0.2   Needs restart
         017dd39a-3d06-4c8a-8ac4-379f9e595607   10.0.0.3   Restarted

   #. If the node status is ``Restarted``, the downgrade is complete. Move to
      the next node and repeat from step a.

   #. If the node shows ``Needs restart``, restart it to complete the
      downgrade:

      #. Drain and stop the node:

         .. code-block:: console

            scylla nodetool -h <node-ip> drain

         .. include:: /rst_include/scylla-commands-stop-index.rst

      #. Restart the node:

         .. include:: /rst_include/scylla-commands-start-index.rst

      #. Wait until the node is UP and has returned to the ScyllaDB cluster using :doc:`nodetool status </operating-scylla/nodetool-commands/status/>`.
         This operation may take a long time due to resharding. To monitor
         resharding progress, use the task manager API:

         .. code-block:: console

            scylla nodetool tasks list compaction -h <node-ip> --keyspace <keyspace> | grep -i reshard

      #. Verify that the node status changed to ``Restarted``:

         .. code-block:: console

            scylla nodetool migrate-to-tablets status <keyspace>

         **Example:**

         .. code-block:: console

            $ scylla nodetool migrate-to-tablets status ks
            Keyspace: ks
            Status: migrating_to_tablets
            Migrating Tables: All
            Direction: vnodes

            Nodes:
            Host ID                                Address    Status
            99d8de76-3954-4727-911a-6a07251b180c   10.0.0.1   Restarted  <---
            0b5fd6f6-9670-4faf-a480-ad58cf119007   10.0.0.2   Needs restart
            017dd39a-3d06-4c8a-8ac4-379f9e595607   10.0.0.3   Restarted

      #. Move to the next node and repeat from step a until all nodes are
         downgraded.

#. Once all nodes have been downgraded, finalize the rollback:

   .. code-block:: console

      scylla nodetool migrate-to-tablets finalize <keyspace>

Migrating multiple keyspaces
----------------------------

Migrating multiple keyspaces simultaneously is supported. The procedure is the
same as with a single keyspace except that the preparation and finalization
steps need to be repeated for each keyspace. However, note that a new migration
cannot be started once another migration is in the upgrade phase. The migrations
need to be prepared and finalized together.

To migrate multiple keyspaces simultaneously, follow these steps:

#. For **each keyspace**, prepare it for migration:

   .. code-block:: console

      scylla nodetool migrate-to-tablets start <keyspace1>
      scylla nodetool migrate-to-tablets start <keyspace2>
      ...

   Verify that all keyspaces are in ``migrating_to_tablets`` state before
   proceeding:

   .. code-block:: console

      scylla nodetool migrate-to-tablets status <keyspace1>
      scylla nodetool migrate-to-tablets status <keyspace2>
      ...

#. Upgrade all nodes in the cluster following the same :ref:`procedure <upgrade-nodes>`
   as for a single keyspace. Each node restart reshards all keyspaces under
   migration in one pass.

#. For **each keyspace**, finalize the migration:

   .. code-block:: console

      scylla nodetool migrate-to-tablets finalize <keyspace1>
      scylla nodetool migrate-to-tablets finalize <keyspace2>
      ...