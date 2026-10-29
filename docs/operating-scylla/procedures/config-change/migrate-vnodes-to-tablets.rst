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

* The keyspace's replication strategy must be the **NetworkTopologyStrategy**.
  To change it, see
  :doc:`Update Topology Strategy From Simple to Network </operating-scylla/procedures/cluster-management/update-topology-strategy-from-simple-to-network>`.
* In every datacenter, the keyspace's replication factor must equal the number
  of racks in that datacenter, to ensure that the keyspace will be
  :term:`RF-rack-valid <RF-rack-valid keyspace>` after the migration.
  Racks that contain only :doc:`zero-token nodes </architecture/zero-token-nodes>`
  are not counted. A datacenter can also have a replication factor of zero
  (RF=0), in which case it holds no replicas of the keyspace. For example, a
  keyspace with replication ``{'dc1': 3, 'dc2': 2, 'dc3': 0}`` can be migrated
  if ``dc1`` has three racks and ``dc2`` has two racks. An
  :doc:`arbiter datacenter </operating-scylla/procedures/cluster-management/arbiter-dc>`,
  which contains only zero-token nodes, must have RF=0. To change the
  replication factor, see :doc:`How to Safely Increase the Replication Factor </kb/rf-increase>`.
  If the keyspace has materialized views or secondary indexes, or the
  ``rf_rack_valid_keyspaces`` option is enabled, the migration refuses to
  start unless the keyspace is RF-rack-valid. Migration with the
  ``enforce_rack_list`` option enabled is not currently supported. Check the
  Limitations section below for more details.
* All tables, materialized views and secondary indexes in the keyspace should
  have ``tombstone_gc`` mode set to ``repair`` to prevent tombstone garbage
  collection from running during migration. For guidance, see
  :ref:`Tombstone Garbage Collection <ddl-tombstones-gc>`.
* All materialized views and secondary indexes in the keyspace must be
  **fully built** before the migration is started. Builds in progress are not
  carried over to tablets. Check the build status with
  :doc:`nodetool viewbuildstatus </operating-scylla/nodetool-commands/viewbuildstatus>`;
  for a secondary index, pass the name of its backing view, ``<index name>_index``.

Limitations
-----------

The current migration procedure has the following limitations:

* Clusters with the ``enforce_rack_list`` option enabled are not yet
  supported: the migrated keyspace would keep its numeric replication factors,
  which the option rejects. Disable the option, run the migration, convert the
  replication factors to rack lists with ``ALTER KEYSPACE``, then enable it
  again.

* **No schema changes** during the migration. Creating or dropping tables, views,
  or indexes, and dropping the migrating keyspace are rejected until the
  migration is finished. Altering the properties of existing tables and views is
  allowed, except enabling CDC, which is not supported.
* **No planned topology changes** during the migration. Do not add, remove,
  decommission, or rebuild nodes while a migration is in progress. Replacing
  a dead node is supported as an emergency recovery operation.
* **No repair** operations during the migration. Do not run ``nodetool repair``
  on the migrating keyspace while a migration is in progress.
* **No backup and restore** for migrating tables during the migration.
  Do not take snapshots, run backups, or restore into the migrating keyspace
  while the migration is in progress.
* **No TRUNCATE** on tables in the migrating keyspace during the migration.
* Only **CQL tables, materialized views, and secondary indexes** can be
  migrated. CDC tables and Alternator tables are not supported. Views whose
  partition key consists of exactly the base table's partition key columns,
  in the same order, including the backing views of local secondary indexes,
  are migrated as :term:`co-located tables <Colocated Table>` with their base
  table (sharing its tablets). Other views, including the backing views of
  global secondary indexes, get tablets of their own.
* Tables with **LWTs** cannot be migrated.
* Only tables using the **Incremental Compaction Strategy (ICS)** can be migrated.
  Other compaction strategies are not supported.

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

#. If the cluster is managed by `ScyllaDB Manager <https://manager.docs.scylladb.com/>`_,
   `suspend <https://manager.docs.scylladb.com/stable/sctool/suspend-resume.html>`_
   its tasks. Backup, restore, and repair are not yet supported on migrating
   keyspaces:

   .. code-block:: console

      sctool suspend --cluster <cluster-id|name> --no-continue

   The ``--no-continue`` option deletes the local snapshots of backup tasks.
   Otherwise, these snapshots would keep a copy of the pre-migration SSTables
   on disk throughout the migration, as resharding rewrites all SSTables.

#. Prepare the keyspace for migration:

   #. Create tablet maps for all tables in the keyspace:

      .. code-block:: console

         nodetool migrate-to-tablets start <keyspace>

   #. Verify that the keyspace is in ``migrating_to_tablets`` state and all nodes are still using vnodes:

      .. code-block:: console

         nodetool migrate-to-tablets status <keyspace>

      **Example:**

      .. code-block:: console

         $ nodetool migrate-to-tablets status ks
         Keyspace: ks
         Status: migrating_to_tablets

         Nodes:
         Host ID                                Address    Status
         99d8de76-3954-4727-911a-6a07251b180c   10.0.0.1   uses vnodes
         0b5fd6f6-9670-4faf-a480-ad58cf119007   10.0.0.2   uses vnodes
         017dd39a-3d06-4c8a-8ac4-379f9e595607   10.0.0.3   uses vnodes

   .. _upgrade-nodes:

#. Upgrade all nodes to tablets:

   .. _upgrade-node-start:

   #. Pick a node and open a shell on it to run the following commands.

   #. Mark the node for upgrade to tablets:

      .. caution::

         Do not mark more than one node for upgrade at the same time. Even if
         you restart them serially, unexpected restarts can happen for various
         reasons (crashes, power failures, etc.) leading to parallel node
         upgrades which can reduce availability.

      .. code-block:: console

         nodetool migrate-to-tablets upgrade

   #. Verify that the node status changed from ``vnodes`` to ``migrating to tablets``:

      .. code-block:: console

         nodetool migrate-to-tablets status <keyspace>

      **Example:**

      .. code-block:: console

         $ nodetool migrate-to-tablets status ks
         Keyspace: ks
         Status: migrating_to_tablets

         Nodes:
         Host ID                                Address    Status
         99d8de76-3954-4727-911a-6a07251b180c   10.0.0.1   migrating to tablets  <---
         0b5fd6f6-9670-4faf-a480-ad58cf119007   10.0.0.2   uses vnodes
         017dd39a-3d06-4c8a-8ac4-379f9e595607   10.0.0.3   uses vnodes

   #. Drain and stop the node:

      .. code-block:: console

         nodetool drain

      .. include:: /rst_include/scylla-commands-stop-index.rst

   #. Restart the node:

      .. tabs::

         .. group-tab:: Supported OS

            .. code-block:: shell

               sudo systemctl start --no-block scylla-server

            The ``--no-block`` option makes the command return immediately
            instead of blocking until resharding completes, so that you can
            monitor its progress as described in the next step.

         .. group-tab:: Docker

            .. code-block:: shell

               docker exec -it some-scylla supervisorctl start scylla

            (with *some-scylla* container already running)

   #. Wait until the node is UP and has returned to the ScyllaDB cluster using :doc:`nodetool status </operating-scylla/nodetool-commands/status/>`.
      This operation may take a long time due to resharding. To monitor
      resharding progress, use the task manager API:

      .. code-block:: console

         nodetool tasks list compaction --keyspace <keyspace>

      **Example:**

      .. code-block:: console

         $ nodetool tasks list compaction --keyspace ks
         task_id                                type                   kind scope state   sequence_number keyspace table entity shard start_time           end_time
         ccf28999-d19c-4c94-8b61-592a5f10dba8   resharding compaction  node table running 730             ks       t1           0     2026-08-07T08:22:03Z

      The command should show one resharding task per migrating table.
      To inspect the progress of one particular task, pass its task ID to
      ``nodetool tasks status`` and compare the number of resharded bytes
      (``progress_completed``) with the total number of bytes to be resharded
      (``progress_total``):

      .. code-block:: console

         nodetool tasks status <task-id>

      **Example:**

      .. code-block:: console

         $ nodetool tasks status ccf28999-d19c-4c94-8b61-592a5f10dba8
         id: ccf28999-d19c-4c94-8b61-592a5f10dba8
         type: resharding compaction
         kind: node
         scope: table
         state: running
         is_abortable: false
         start_time: 2026-08-07T08:22:03Z
         end_time:
         error:
         parent_id: e85de42c-9bf0-3a0b-a3ce-6d481b093bc7
         sequence_number: 730
         shard: 0
         keyspace: ks
         table: t1
         entity:
         progress_units: bytes
         progress_total: 5347926524519
         progress_completed: 252509969840
         children_ids: [{task_id: 22461a60-ae95-4356-a5cb-dceac72c75b5, node: 172.31.0.228 }, {task_id: e2f86734-6213-42b3-85ca-b86864a97dfb, node: 172.31.0.228 }, ...]

      .. note::

         Resharding may be followed by a reshape compaction, which rewrites the
         resharded SSTables into the layout expected by the table's compaction
         strategy. Both resharding and reshaping are part of the node's boot
         process and both appear in ``nodetool tasks list compaction``. Wait for
         both to complete before proceeding to the next step.

   #. Verify that the node status changed from ``migrating to tablets`` to ``uses tablets``:

      .. code-block:: console

         nodetool migrate-to-tablets status <keyspace>

      **Example:**

      .. code-block:: console

         $ nodetool migrate-to-tablets status ks
         Keyspace: ks
         Status: migrating_to_tablets

         Nodes:
         Host ID                                Address    Status
         99d8de76-3954-4727-911a-6a07251b180c   10.0.0.1   uses tablets  <---
         0b5fd6f6-9670-4faf-a480-ad58cf119007   10.0.0.2   uses vnodes
         017dd39a-3d06-4c8a-8ac4-379f9e595607   10.0.0.3   uses vnodes

   #. Move to the next node and repeat from :ref:`step a <upgrade-node-start>`
      until all nodes are upgraded.

#. Finalize the migration:

   .. warning::

      Finalization **cannot be undone**. Once the migration is finalized, the
      keyspace cannot be switched back to vnodes.

   #. Issue the finalization request:

      .. code-block:: console

         nodetool migrate-to-tablets finalize <keyspace>

      Finalization switches co-located materialized views and local secondary
      indexes that use the ``repair`` ``tombstone_gc`` mode to ``timeout``.
      After the migration they become
      :term:`co-located tables <Colocated Table>`, which do not support
      ``repair``, and ``timeout`` is their default mode. Other views and the
      backing views of global secondary indexes keep their mode.

   #. Verify that the keyspace status changed to ``tablets``:

      .. code-block:: console

         nodetool migrate-to-tablets status <keyspace>

      **Example:**

      .. code-block:: console

         $ nodetool migrate-to-tablets status ks
         Keyspace: ks
         Status: tablets

   #. Wait for tablet layout normalization to complete:

      After finalization, a tablet normalization process takes place in the
      background which transforms the tablets of all migrated tables into a
      power-of-two layout. Until power-of-two convergence is complete, the
      tablet layout of all these tables will be suboptimal, causing degraded
      performance. Run the following command repeatedly to monitor the per-table
      status of this background work:

      .. code-block:: console

         nodetool migrate-to-tablets status <keyspace> --with-tablet-status

      **Example:**

      .. code-block:: console

         $ nodetool migrate-to-tablets status ks --with-tablet-status
         Keyspace: ks
         Status: tablets

         Tablet info:
           Pow2 tablet layout convergence: in progress
           Tables converging: 2/3

         Table   Status       Tablets   Target
         t1      converging   2301      2048
         t2      converging   2176      2048
         t3      converged    2048      -

#. If you suspended ScyllaDB Manager in the first step, resume it:

   .. code-block:: console

      sctool resume --cluster <cluster-id|name> --no-continue

   The ``--no-continue`` option makes interrupted tasks start from scratch,
   instead of continuing from progress recorded before the migration.

#. Run a full cluster repair to ensure that all nodes have the most up-to-date
   data. As the storage upgrade process kept each node offline for an extended
   period of time, its dataset might have become stale.

   If the keyspace has materialized views or secondary indexes, the repair
   also covers updates to them that were lost during finalization. Each node
   switches to the tablet-based way of pairing base and view replicas when it
   applies the finalization, so view updates written while the nodes
   disagreed can miss some view replicas.

   Either run the repair with ``nodetool``, as described below, or let
   `ScyllaDB Manager <https://manager.docs.scylladb.com/>`_ run it for you.

   For vnode-based keyspaces, run
   :doc:`nodetool repair </operating-scylla/nodetool-commands/repair>` on
   **every** node:

   .. code-block:: console

      nodetool repair -pr

   For tablet-based keyspaces, including the one you just migrated, run
   :doc:`nodetool cluster repair </operating-scylla/nodetool-commands/cluster/repair>`
   on **any** single node:

   .. code-block:: console

      nodetool cluster repair

   Run both if the cluster has keyspaces of both kinds.

.. _vnodes-migration-rollback:

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

      nodetool migrate-to-tablets status <keyspace>

   **Example:**

   .. code-block:: console

      $ nodetool migrate-to-tablets status ks
      Keyspace: ks
      Status: migrating_to_tablets

      Nodes:
      Host ID                                Address    Status
      99d8de76-3954-4727-911a-6a07251b180c   10.0.0.1   uses tablets  <---
      0b5fd6f6-9670-4faf-a480-ad58cf119007   10.0.0.2   migrating to tablets  <---
      017dd39a-3d06-4c8a-8ac4-379f9e595607   10.0.0.3   uses vnodes

#. For **each upgraded or upgrading node** in the cluster, perform a downgrade
   (one node at a time):

   .. _downgrade-node-start:

   #. Open a shell on the node to run the following commands.

   #. Mark the node for downgrade:

      .. code-block:: console

         nodetool migrate-to-tablets downgrade

   #. Check the node status. The status for a previously upgraded node should
      change from ``uses tablets`` to ``migrating to vnodes``. The status for a
      previously upgrading node should change from ``migrating to tablets`` to
      ``uses vnodes`` or ``migrating to vnodes``:

      .. code-block:: console

         nodetool migrate-to-tablets status <keyspace>

      **Example:**

      .. code-block:: console

         $ nodetool migrate-to-tablets status ks
         Keyspace: ks
         Status: migrating_to_tablets

         Nodes:
         Host ID                                Address    Status
         99d8de76-3954-4727-911a-6a07251b180c   10.0.0.1   migrating to vnodes  <---
         0b5fd6f6-9670-4faf-a480-ad58cf119007   10.0.0.2   migrating to tablets
         017dd39a-3d06-4c8a-8ac4-379f9e595607   10.0.0.3   uses vnodes

   #. If the node status is ``uses vnodes``, the downgrade is complete. Move to
      the next node and repeat from :ref:`step a <downgrade-node-start>`.

   #. If the node is ``migrating to vnodes``, restart it to complete the
      downgrade:

      #. Drain and stop the node:

         .. code-block:: console

            nodetool drain

         .. include:: /rst_include/scylla-commands-stop-index.rst

      #. Restart the node:

         .. tabs::

            .. group-tab:: Supported OS

               .. code-block:: shell

                  sudo systemctl start --no-block scylla-server

               The ``--no-block`` option makes the command return immediately
               instead of blocking until resharding completes, so that you can
               monitor its progress as described in the next step.

            .. group-tab:: Docker

               .. code-block:: shell

                  docker exec -it some-scylla supervisorctl start scylla

               (with *some-scylla* container already running)

      #. Wait until the node is UP and has returned to the ScyllaDB cluster using :doc:`nodetool status </operating-scylla/nodetool-commands/status/>`.
         This operation may take a long time due to resharding. To monitor
         resharding progress, use the task manager API:

         .. code-block:: console

            nodetool tasks list compaction --keyspace <keyspace>

         **Example:**

         .. code-block:: console

            $ nodetool tasks list compaction --keyspace ks
            task_id                                type                   kind scope state   sequence_number keyspace table entity shard start_time           end_time
            ccf28999-d19c-4c94-8b61-592a5f10dba8   resharding compaction  node table running 730             ks       t1           0     2026-08-07T08:22:03Z

         The command should show one resharding task per migrating table.
         To inspect the progress of one particular task, pass its task ID to
         ``nodetool tasks status`` and compare the number of resharded bytes
         (``progress_completed``) with the total number of bytes to be resharded
         (``progress_total``):

         .. code-block:: console

            nodetool tasks status <task-id>

         **Example:**

         .. code-block:: console

            $ nodetool tasks status ccf28999-d19c-4c94-8b61-592a5f10dba8
            id: ccf28999-d19c-4c94-8b61-592a5f10dba8
            type: resharding compaction
            kind: node
            scope: table
            state: running
            is_abortable: false
            start_time: 2026-08-07T08:22:03Z
            end_time:
            error:
            parent_id: e85de42c-9bf0-3a0b-a3ce-6d481b093bc7
            sequence_number: 730
            shard: 0
            keyspace: ks
            table: t1
            entity:
            progress_units: bytes
            progress_total: 5347926524519
            progress_completed: 252509969840
            children_ids: [{task_id: 22461a60-ae95-4356-a5cb-dceac72c75b5, node: 172.31.0.228 }, {task_id: e2f86734-6213-42b3-85ca-b86864a97dfb, node: 172.31.0.228 }, ...]

         .. note::

            Resharding may be followed by a reshape compaction, which rewrites the
            resharded SSTables into the layout expected by the table's compaction
            strategy. Both resharding and reshaping are part of the node's boot
            process and both appear in ``nodetool tasks list compaction``. Wait for
            both to complete before proceeding to the next node.

      #. Verify that the node status changed from ``migrating to vnodes`` to ``uses vnodes``:

         .. code-block:: console

            nodetool migrate-to-tablets status <keyspace>

         **Example:**

         .. code-block:: console

            $ nodetool migrate-to-tablets status ks
            Keyspace: ks
            Status: migrating_to_tablets

            Nodes:
            Host ID                                Address    Status
            99d8de76-3954-4727-911a-6a07251b180c   10.0.0.1   uses vnodes  <---
            0b5fd6f6-9670-4faf-a480-ad58cf119007   10.0.0.2   migrating to tablets
            017dd39a-3d06-4c8a-8ac4-379f9e595607   10.0.0.3   uses vnodes

      #. Move to the next node and repeat from
         :ref:`step a <downgrade-node-start>` until all nodes are downgraded.

#. Once all nodes have been downgraded, finalize the rollback:

   .. code-block:: console

      nodetool migrate-to-tablets finalize <keyspace>

#. If you suspended ScyllaDB Manager before starting the migration, resume it:

   .. code-block:: console

      sctool resume --cluster <cluster-id|name> --no-continue

   The ``--no-continue`` option makes interrupted tasks start from scratch,
   instead of continuing from progress recorded before the rollback.

#. Run a full cluster repair to ensure that all nodes have the most up-to-date
   data. As the storage downgrade process kept each node offline for an extended
   period of time, its dataset might have become stale.

   Either run the repair with ``nodetool``, as described below, or let
   `ScyllaDB Manager <https://manager.docs.scylladb.com/>`_ run it for you.

   For vnode-based keyspaces, including the one you just rolled back, run
   :doc:`nodetool repair </operating-scylla/nodetool-commands/repair>` on
   **every** node:

   .. code-block:: console

      nodetool repair -pr

   For tablet-based keyspaces, run
   :doc:`nodetool cluster repair </operating-scylla/nodetool-commands/cluster/repair>`
   on **any** single node:

   .. code-block:: console

      nodetool cluster repair

   Run both if the cluster has keyspaces of both kinds.

Recovering a node that fails to start
-------------------------------------

Resharding is an offline operation and it temporarily needs additional disk space.
If it cannot complete - most commonly because the node ran out of disk space - the
node fails to start, and it fails again on every subsequent start.

If you can free disk space or extend the volume, do that and restart the node: it
completes the upgrade, and you do not need the procedure below. Use the procedure
only to abort the migration on such a node. The node cannot be rolled back with the
:ref:`vnodes-migration-rollback` above, because that procedure requires the node to
be UP.

.. caution::

   The ``force_vnodes_for_migrating_tables`` option only affects the node it is set
   on, and it does not change the cluster state. Until you mark the node for
   downgrade (step 3 below), the migration cannot be finalized in either direction.
   Use it to recover a node that will not start, not as a way to control the
   migration.

#. Add the following to the node's ``scylla.yaml`` and start the node:

   .. code-block:: yaml

      force_vnodes_for_migrating_tables: true

   The node starts with vnodes, reshards any data it had already upgraded back to
   vnodes, and logs the following warning:

   .. code-block:: none

      force_vnodes_for_migrating_tables is set, overriding the intended storage mode recorded in system.topology ('tablets')

#. Wait until the node is UP using :doc:`nodetool status </operating-scylla/nodetool-commands/status/>`.
   To monitor resharding progress, open a shell on the node and run:

   .. code-block:: console

      nodetool tasks list compaction --keyspace <keyspace>

   .. note::

      Until you complete the next step, ``migrate-to-tablets status`` keeps reporting
      this node as ``migrating to tablets``. This does not mean that the node is
      still upgrading.

#. Mark the node for downgrade, from a shell on the node:

   .. code-block:: console

      nodetool migrate-to-tablets downgrade

   Verify that the node's status is ``uses vnodes``:

   .. code-block:: console

      nodetool migrate-to-tablets status <keyspace>

#. Remove ``force_vnodes_for_migrating_tables`` from the node's ``scylla.yaml``.

#. Continue with the :ref:`vnodes-migration-rollback` for the remaining upgraded nodes, then
   finalize the rollback.

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

      nodetool migrate-to-tablets start <keyspace1>
      nodetool migrate-to-tablets start <keyspace2>
      ...

   Verify that all keyspaces are in ``migrating_to_tablets`` state before
   proceeding:

   .. code-block:: console

      nodetool migrate-to-tablets status <keyspace1>
      nodetool migrate-to-tablets status <keyspace2>
      ...

#. Upgrade all nodes in the cluster following the same :ref:`procedure <upgrade-nodes>`
   as for a single keyspace. Each node restart reshards all keyspaces under
   migration in one pass.

#. For **each keyspace**, finalize the migration:

   .. code-block:: console

      nodetool migrate-to-tablets finalize <keyspace1>
      nodetool migrate-to-tablets finalize <keyspace2>
      ...

Replacing a Dead Node during Migration
--------------------------------------

If a node fails during the migration, replace it using the standard procedure
for :doc:`replacing a dead node </operating-scylla/procedures/cluster-management/replace-dead-node/>`.

The replacing node must have the same shard count as the replaced node. If the
shard count differs, the replacement will fail.

After replacement completes, you can resume the migration. The replacing node
always starts in vnode mode and must be upgraded to tablets, regardless of the
storage mode of node it replaced.
