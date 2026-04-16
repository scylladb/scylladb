Migrate a Keyspace from Vnodes to Tablets
==========================================

This procedure describes how to migrate an existing keyspace from vnodes
to tablets. Tablets are designed to be the long-term replacement for vnodes,
offering numerous benefits such as faster topology operations, automatic load
balancing, automatic cleanups, and improved streaming performance. Migrating to
tablets is strongly recommended. See :doc:`Data Distribution with Tablets </architecture/tablets/>`
for details.

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

* All nodes in the cluster must be **up and running**. You can check the status
  of all nodes with
  :doc:`nodetool status </operating-scylla/nodetool-commands/status/>`.
* All nodes must be running ScyllaDB 2026.2 or later.

Limitations
-----------

The current migration procedure has the following limitations:

* The total number of **vnode tokens** in the cluster must be a **power of two**
  and the tokens must be **evenly spaced** across the token ring. This is
  verified automatically when starting the migration.
* **No schema changes** during the migration. Do not create, alter, or drop
  tables in the migrating keyspace until the migration is finished.
* **No topology changes** during the migration. Do not add, remove, decommission,
  or replace nodes while a migration is in progress.
* **No TRUNCATE** on tables in the migrating keyspace during the migration.
* Only **CQL base tables** can be migrated. Materialized views, secondary
  indexes, CDC tables, and Alternator tables are not supported.
* Tables with **counters** or **LWTs** cannot be migrated.

Overview
--------

The migration consists of three phases:

1. **Prepare**: Create tablet maps for all tables in the keyspace. Each tablet
   inherits its token range and replica set from the corresponding vnode range.
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

   #. Verify that the keyspace is in ``migrating_to_tablets`` state and all nodes are still using vnodes:

      .. code-block:: console

         scylla nodetool migrate-to-tablets status <keyspace>

      **Example:**

      .. code-block:: console

         $ scylla nodetool migrate-to-tablets status ks
         Keyspace: ks
         Status: migrating_to_tablets

         Nodes:
         Host ID                                Status
         99d8de76-3954-4727-911a-6a07251b180c   uses vnodes
         0b5fd6f6-9670-4faf-a480-ad58cf119007   uses vnodes
         017dd39a-3d06-4c8a-8ac4-379f9e595607   uses vnodes

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

   #. Verify that the node status changed from ``vnodes`` to ``migrating to tablets``:

      .. code-block:: console

         scylla nodetool migrate-to-tablets status <keyspace>

      **Example:**

      .. code-block:: console

         $ scylla nodetool migrate-to-tablets status ks
         Keyspace: ks
         Status: migrating_to_tablets

         Nodes:
         Host ID                                Status
         99d8de76-3954-4727-911a-6a07251b180c   migrating to tablets  <---
         0b5fd6f6-9670-4faf-a480-ad58cf119007   uses vnodes
         017dd39a-3d06-4c8a-8ac4-379f9e595607   uses vnodes

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

   #. Verify that the node status changed from ``migrating to tablets`` to ``uses tablets``:

      .. code-block:: console

         scylla nodetool migrate-to-tablets status <keyspace>

      **Example:**

      .. code-block:: console

         $ scylla nodetool migrate-to-tablets status ks
         Keyspace: ks
         Status: migrating_to_tablets

         Nodes:
         Host ID                                Status
         99d8de76-3954-4727-911a-6a07251b180c   uses tablets  <---
         0b5fd6f6-9670-4faf-a480-ad58cf119007   uses vnodes
         017dd39a-3d06-4c8a-8ac4-379f9e595607   uses vnodes

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

Rollback Procedure
------------------

.. note::

   Rollback is only possible **before finalization**. Once the migration is
   finalized, it cannot be reversed.

If you need to abort the migration **before finalization**, you can roll back
by downgrading each node back to vnodes. The rollback procedure is the
following:

#. Find all nodes that have been upgraded to tablets (status: ``uses tablets``)
   or they are in the process of upgrading to tablets (status: ``migrating to tablets``):

   .. code-block:: console

      scylla nodetool migrate-to-tablets status <keyspace>

   **Example:**

   .. code-block:: console

      $ scylla nodetool migrate-to-tablets status ks
      Keyspace: ks
      Status: migrating_to_tablets

      Nodes:
      Host ID                                Status
      99d8de76-3954-4727-911a-6a07251b180c   uses tablets  <---
      0b5fd6f6-9670-4faf-a480-ad58cf119007   migrating to tablets  <---
      017dd39a-3d06-4c8a-8ac4-379f9e595607   uses vnodes

#. For **each upgraded or upgrading node** in the cluster, perform a downgrade
   (one node at a time):

   #. Mark the node for downgrade:

      .. code-block:: console

         scylla nodetool -h <node-ip> migrate-to-tablets downgrade

   #. Check the node status. The status for a previously upgraded node should
      change from ``uses tablets`` to ``migrating to vnodes``. The status for a
      previously upgrading node should change from ``migrating to tablets`` to
      ``uses vnodes`` or ``migrating to vnodes``:

      .. code-block:: console

         scylla nodetool migrate-to-tablets status <keyspace>

      **Example:**

      .. code-block:: console

         $ scylla nodetool migrate-to-tablets status ks
         Keyspace: ks
         Status: migrating_to_tablets

         Nodes:
         Host ID                                Status
         99d8de76-3954-4727-911a-6a07251b180c   migrating to vnodes  <---
         0b5fd6f6-9670-4faf-a480-ad58cf119007   migrating to tablets
         017dd39a-3d06-4c8a-8ac4-379f9e595607   uses vnodes

   #. If the node status is ``uses vnodes``, the downgrade is complete. Move to
      the next node and repeat from step a.

   #. If the node is ``migrating to vnodes``, restart it to complete the
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

      #. Verify that the node status changed from ``migrating to vnodes`` to ``uses vnodes``:

         .. code-block:: console

            scylla nodetool migrate-to-tablets status <keyspace>

         **Example:**

         .. code-block:: console

            $ scylla nodetool migrate-to-tablets status ks
            Keyspace: ks
            Status: migrating_to_tablets

            Nodes:
            Host ID                                Status
            99d8de76-3954-4727-911a-6a07251b180c   uses vnodes  <---
            0b5fd6f6-9670-4faf-a480-ad58cf119007   migrating to tablets
            017dd39a-3d06-4c8a-8ac4-379f9e595607   uses vnodes

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