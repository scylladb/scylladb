==================================================
Cluster Platform Migration  Using Node Cycling
==================================================

This procedure describes how to migrate a ScyllaDB cluster to new instance types
using the add-and-replace approach, which is commonly used for:

* Migrating from one CPU architecture to another (e.g., x86_64 to ARM/Graviton)
* Upgrading to newer instance types with better performance
* Changing instance families within the same cloud provider

The add-and-replace approach maintains data replication throughout the migration
and ensures zero downtime for client applications.

.. note::

   This procedure does **not** change the ScyllaDB software version. All nodes
   (both existing and new) must run the same ScyllaDB version. For software
   version upgrades, see :doc:`About Upgrade </upgrade/about-upgrade>`.

Overview
--------

The add-and-replace migration follows these steps:

#. Add new nodes (on target instance type) to the existing cluster
#. Wait for data to stream to the new nodes
#. Decommission old nodes (on source instance type)

This approach keeps the cluster operational throughout the migration while
maintaining the configured replication factor.

Key characteristics
===================

* **Zero downtime**: Client applications continue to operate during migration
* **Data safety**: Replication factor is maintained throughout the process
* **Flexible**: Works with both vnodes and tablets-enabled clusters
* **Multi-DC support**: Can migrate nodes across multiple datacenters

.. warning::

   Ensure your cluster has sufficient capacity during the migration. At the peak
   of the process, your cluster will temporarily have double the number of nodes.

Prerequisites
-------------

Check cluster health
====================

Before starting the migration, verify that your cluster is healthy:

#. Check that all nodes are in Up Normal (UN) status:

   .. code-block:: shell

      nodetool status

   All nodes should show ``UN`` status. Do not proceed if any nodes are down.

#. Verify schema agreement across all nodes:

   .. code-block:: shell

      nodetool describecluster

   All nodes should report the same schema version under "Schema versions:".

#. Ensure no streaming or repair operations are in progress:

   .. code-block:: shell

      nodetool netstats
      nodetool compactionstats

Plan the migration
==================

Before provisioning new instances, plan the following:

**Instance type mapping**: Identify the source and target instance types.
If your cluster uses vnodes (not tablets), ensure the target instance type
has the same shard count as the source.

.. note::

   With tablets enabled, shard count mismatch between instance types is
   tolerated. Without tablets, shard counts must match.

**Rack assignment planning**: Each new node must be assigned to the same rack
as the node it will replace. This maintains rack-aware topology for:

* Rack-aware replication (NetworkTopologyStrategy)
* Proper data distribution across failure domains
* Minimizing data movement during decommission

Example mapping for a 3-node cluster:

.. code-block:: none

   Source nodes (to be decommissioned):     Target nodes (to be added):
   192.168.1.10 - RACK0                 →   192.168.2.10 - RACK0
   192.168.1.11 - RACK1                 →   192.168.2.11 - RACK1
   192.168.1.12 - RACK2                 →   192.168.2.12 - RACK2

Create a backup
===============

Create a snapshot before starting the migration:

.. code-block:: shell

   nodetool snapshot -t pre_migration_backup
   nodetool listsnapshots


Procedure
---------

Adding new nodes
================

#. Provision new instances with the target instance type. Ensure:

   * The same ScyllaDB version as existing nodes
   * Same network configuration and security groups
   * Appropriate storage configuration

#. On each new node, configure ``/etc/scylla/scylla.yaml`` to join the existing
   cluster:

   * **cluster_name**: Must match the existing cluster name
   * **seeds**: IP addresses of existing nodes in the cluster
   * **endpoint_snitch**: Must match the existing cluster configuration
   * **listen_address**: IP address of the new node
   * **rpc_address**: IP address of the new node
   * **auto_bootstrap**: Set to ``true``

   All other cluster-wide settings (tablets configuration, encryption settings,
   experimental features, etc.) must match the existing nodes.

   .. caution::

      Make sure that the ScyllaDB version on the new node is identical to the
      version on the other nodes in the cluster. Running nodes with different
      versions is not supported.

#. If using ``GossipingPropertyFileSnitch``, configure
   ``/etc/scylla/cassandra-rackdc.properties`` with the correct datacenter
   and rack assignment for this node:

   .. code-block:: none

      dc = <datacenter-name>
      rack = <rack-name>
      prefer_local = true

   .. warning::

      Each node must have the correct rack assignment. Using the same rack for
      all new nodes breaks rack-aware replication topology.

#. Start ScyllaDB on the new node:

   .. code-block:: shell

      sudo systemctl start scylla-server

   For Docker deployments:

   .. code-block:: shell

      docker exec -it <container-name> supervisorctl start scylla

#. Monitor the bootstrap process from an existing node:

   .. code-block:: shell

      nodetool status

   The new node will appear with ``UJ`` (Up, Joining) status while streaming
   data from existing nodes. Wait until it transitions to ``UN`` (Up, Normal).

   **Example output during bootstrap:**

   .. code-block:: shell

      Datacenter: dc1
      Status=Up/Down
      State=Normal/Leaving/Joining/Moving
      --  Address        Load       Tokens  Owns   Host ID                               Rack
      UN  192.168.1.10   500 MB     256     33.3%  8d5ed9f4-7764-4dbd-bad8-43fddce94b7c  RACK0
      UN  192.168.1.11   500 MB     256     33.3%  125ed9f4-7777-1dbn-mac8-43fddce9123e  RACK1
      UN  192.168.1.12   500 MB     256     33.3%  675ed9f4-6564-6dbd-can8-43fddce952gy  RACK2
      UJ  192.168.2.10   250 MB     256     ?      a1b2c3d4-5678-90ab-cdef-112233445566  RACK0

   **Example output after bootstrap completes:**

   .. code-block:: shell

      Datacenter: dc1
      Status=Up/Down
      State=Normal/Leaving/Joining/Moving
      --  Address        Load       Tokens  Owns   Host ID                               Rack
      UN  192.168.1.10   400 MB     256     25.0%  8d5ed9f4-7764-4dbd-bad8-43fddce94b7c  RACK0
      UN  192.168.1.11   400 MB     256     25.0%  125ed9f4-7777-1dbn-mac8-43fddce9123e  RACK1
      UN  192.168.1.12   400 MB     256     25.0%  675ed9f4-6564-6dbd-can8-43fddce952gy  RACK2
      UN  192.168.2.10   400 MB     256     25.0%  a1b2c3d4-5678-90ab-cdef-112233445566  RACK0

#. For tablets-enabled clusters, wait for tablet load balancing to complete.
   After the node reaches ``UN`` status, verify no streaming is in progress:

   .. code-block:: shell

      nodetool netstats

   Wait until output shows "Not sending any streams" and no active receiving streams.

#. Repeat steps 1-7 for each new node to be added.

.. note::

   You can add multiple nodes in parallel if they are in different datacenters.
   Within a single datacenter, add nodes one at a time for best results.


Updating seed node configuration
================================

If any of your original nodes are configured as seed nodes, you must update
the seed configuration before decommissioning them.

#. Check the current seed configuration on any node:

   .. code-block:: shell

      grep -A 4 "seed_provider" /etc/scylla/scylla.yaml

#. If the seeds include nodes you plan to decommission, update ``scylla.yaml``
   on **all new nodes** to use the new node IPs as seeds:

   .. code-block:: yaml

      seed_provider:
        - class_name: org.apache.cassandra.locator.SimpleSeedProvider
          parameters:
            - seeds: "192.168.2.10,192.168.2.11,192.168.2.12"

   .. note::

      Updating seed configuration on the **old nodes** (that will be
      decommissioned) is optional. Seeds are only used during node startup
      to discover the cluster. If you don't plan to restart the old nodes
      before decommissioning them, their seed configuration doesn't matter.
      However, updating all nodes is recommended for safety in case an old
      node unexpectedly restarts during the migration.

#. Restart ScyllaDB on each new node (one at a time) to apply the new seed
   configuration:

   .. code-block:: shell

      sudo systemctl restart scylla-server

   Wait for the node to fully start before restarting the next node.

#. After restarting the new nodes, verify the cluster is healthy:

   .. code-block:: shell

      nodetool status
      nodetool describecluster

.. warning::

   Complete this seed list update on **all new nodes** before decommissioning
   any old nodes. This ensures the new nodes can reform the cluster after
   the old nodes are removed.


Decommissioning old nodes
=========================

After all new nodes are added and healthy, decommission the old nodes one
at a time.

#. Verify all nodes are healthy before starting decommission:

   .. code-block:: shell

      nodetool status

   All nodes should show ``UN`` status.

#. On the node to be decommissioned, run:

   .. code-block:: shell

      nodetool decommission

   This command blocks until the decommission is complete. The node will
   stream its data to the remaining nodes.

#. Monitor the decommission progress from another node:

   .. code-block:: shell

      nodetool status

   The decommissioning node will transition from ``UN`` → ``UL`` (Up, Leaving)
   → removed from the cluster.

   You can also monitor streaming progress:

   .. code-block:: shell

      nodetool netstats

#. After decommission completes, verify the node is no longer in the cluster:

   .. code-block:: shell

      nodetool status

   The decommissioned node should no longer appear in the output.

#. Verify schema agreement is maintained:

   .. code-block:: shell

      nodetool describecluster

#. Wait for the cluster to stabilize before decommissioning the next node.
   Ensure no streaming operations are in progress.

#. Repeat steps 1-6 for each old node to be decommissioned.


Post-migration verification
---------------------------

After all old nodes are decommissioned, verify the migration was successful.

Verify cluster topology
=======================

.. code-block:: shell

   nodetool status

Confirm:

* All nodes show ``UN`` (Up, Normal) status
* Only the new instance type nodes are present
* Nodes are balanced across racks

Verify schema agreement
=======================

.. code-block:: shell

   nodetool describecluster

All nodes should report the same schema version.

Verify data connectivity
========================

Connect to the cluster and run a test query:

.. code-block:: shell

   cqlsh <node-ip> -e "SELECT count(*) FROM system_schema.keyspaces;"

.. note::

   If ScyllaDB is configured with ``listen_interface``, you must use the
   node's interface IP address (not localhost) for cqlsh connections.

Verify ScyllaDB version
=======================

Confirm all nodes are running the same ScyllaDB version:

.. code-block:: shell

   nodetool version

Verify data integrity (optional)
================================

Run data verification on each keyspace:

.. code-block:: shell

   nodetool verify <keyspace_name>

Rollback
--------

If issues occur during the migration, you can roll back by reversing the
procedure.

During add phase
================

If a new node fails to bootstrap:

#. Stop ScyllaDB on the new node:

   .. code-block:: shell

      sudo systemctl stop scylla-server

#. From an existing node, remove the failed node:

   .. code-block:: shell

      nodetool removenode <host-id-of-failed-node>

During decommission phase
=========================

If a decommission operation gets stuck:

#. If the node is still reachable, try stopping and restarting ScyllaDB
#. If the node is unresponsive, from another node:

   .. code-block:: shell

      nodetool removenode <host-id>

   See :doc:`Remove a Node from a ScyllaDB Cluster </operating-scylla/procedures/cluster-management/remove-node>`
   for more details.

Full rollback
=============

To roll back after the migration is complete (all nodes on new instance type),
apply the same add-and-replace procedure in reverse:

#. Add new nodes on the original instance type
#. Wait for data streaming to complete
#. Decommission the nodes on the new instance type


Troubleshooting
---------------

Node stuck in Joining (UJ) state
================================

If a new node remains in ``UJ`` state for an extended period:

* Check ScyllaDB logs for streaming errors: ``journalctl -u scylla-server``
* Verify network connectivity between nodes
* Ensure sufficient disk space on all nodes
* Check for any ongoing operations that may be blocking

Decommission taking too long
============================

Decommission duration depends on data size. If it appears stuck:

* Check streaming progress: ``nodetool netstats``
* Look for errors in ScyllaDB logs
* Verify network bandwidth between nodes

Schema disagreement
===================

If nodes report different schema versions:

* Wait a few minutes for schema to propagate
* If disagreement persists, restart the nodes one by one
* Run ``nodetool describecluster`` to verify agreement


Additional resources
--------------------

* :doc:`Adding a New Node Into an Existing ScyllaDB Cluster </operating-scylla/procedures/cluster-management/add-node-to-cluster>`
* :doc:`Remove a Node from a ScyllaDB Cluster </operating-scylla/procedures/cluster-management/remove-node>`
* :doc:`Replace a Running Node in a ScyllaDB Cluster </operating-scylla/procedures/cluster-management/replace-running-node>`
* :doc:`About Upgrade </upgrade/about-upgrade>`
