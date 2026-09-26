# Handling Node Failures

ScyllaDB relies on the Raft consensus algorithm, which requires at least a quorum
of nodes in a cluster to be available. If one or more nodes are down, but the quorum
is live, reads, writes, schema updates, and topology changes proceed unaffected. When the node that
was down is up again, it first contacts the cluster to fetch the latest schema and
then starts serving queries.

The following examples show the recovery actions when one or more nodes or DCs
are down, depending on the number of nodes  and DCs in your cluster.

## Examples

#### Cluster A: 1 datacenter, 3 nodes

| Failure   | Consequence                                                                         | Action to take                                                                                                                                                                                   |
|-----------|-------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1 node    | Schema and topology updates are possible and safe.                                  | Try restarting the node. If the node is dead, [replace it with a new node](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/procedures/cluster-management/replace-dead-node.md). |
| 2 nodes   | Data is available for reads and writes; schema and topology changes are impossible. | Restart at least 1 of the 2 nodes that are down to regain quorum. If you can’t recover at least 1 of the 2 nodes, consult the [manual recovery section](#recovery-procedure).                    |

#### Cluster B: 2 datacenters, 6  nodes (3 nodes per DC)

| Failure   | Consequence                                                                         | Action to take                                                                                                                                                                                      |
|-----------|-------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1-2 nodes | Schema and topology updates are possible and safe.                                  | Try restarting the node(s). If the node is dead, [replace it with a new node](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/procedures/cluster-management/replace-dead-node.md). |
| 3 nodes   | Data is available for reads and writes; schema and topology changes are impossible. | Restart 1 of the 3 nodes that are down to regain quorum. If you can’t recover at least 1 of the 3 failed nodes, consult the [manual recovery](#recovery-procedure) section.                         |
| 1DC       | Data is available for reads and writes; schema and topology changes are impossible. | When the DCs come back online, restart the nodes. If the DC fails to come back online and the nodes are lost, consult the [manual recovery](#recovery-procedure) section.                           |

#### Cluster C: 3 datacenter, 9  nodes (3 nodes per DC)

| Failure   | Consequence                                                                         | Action to take                                                                                                                                                                                                                                             |
|-----------|-------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1-4 nodes | Schema and topology updates are possible and safe.                                  | Try restarting the nodes. If the nodes are dead, [replace them with new nodes](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/procedures/cluster-management/replace-dead-node-or-more.md).                                               |
| 1 DC      | Schema and topology updates are possible and safe.                                  | When the DC comes back online, try restarting the nodes in the cluster. If the nodes are dead, [add 3 new nodes in a new region](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/procedures/cluster-management/add-dc-to-existing-dc.md). |
| 2 DCs     | Data is available for reads and writes, schema and topology changes are impossible. | When the DCs come back online, restart the nodes. If at least one DC fails to come back online and the nodes are lost, consult the [manual recovery](#recovery-procedure) section.                                                                         |

<a id="recovery-procedure"></a>

## Manual Recovery Procedure

You can follow the manual recovery procedure when:

* The majority of nodes (for example, 2 out of 3) failed and are irrecoverable.
* [The Raft upgrade procedure](https://opensource.docs.scylladb.com/branch-6.0/architecture/raft.md#verify-raft-procedure)
  or [the procedure for enabling consistent topology changes](https://opensource.docs.scylladb.com/branch-6.0/upgrade/upgrade-opensource/upgrade-guide-from-5.4-to-6.0/enable-consistent-topology.md)
  got stuck because one of the nodes failed in the middle of the procedure and is irrecoverable.

#### WARNING
Perform the manual recovery procedure **only** if you’re dealing with
**irrecoverable** nodes. If possible, restart your nodes, and use the manual
recovery procedure as a last resort.

#### WARNING
The manual recovery procedure is not supported [if tablets are enabled on any of your keyspaces](https://opensource.docs.scylladb.com/branch-6.0/architecture/tablets.md).
In such a case, you need to [restore from backup](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/procedures/backup-restore/restore.md).

During the manual recovery procedure you’ll enter a special `RECOVERY` mode, remove
all faulty nodes (using the standard [node removal procedure](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/procedures/cluster-management/remove-node.md)),
delete the internal Raft data, and restart the cluster. This will cause the cluster to
perform the Raft upgrade procedure again, initializing the Raft algorithm from scratch.

The manual recovery procedure is applicable both to clusters that were not running Raft
in the past and then had Raft enabled, and to clusters that were bootstrapped using Raft.

**Prerequisites**

* Before proceeding, make sure that the irrecoverable nodes are truly dead, and not,
  for example, temporarily partitioned away due to a network failure. If it is
  possible for the ‘dead’ nodes to come back to life, they might communicate and
  interfere with the recovery procedure and cause unpredictable problems.

  If you have no means of ensuring that these irrecoverable nodes won’t come back
  to life and communicate with the rest of the cluster, setup firewall rules or otherwise
  isolate your alive nodes to reject any communication attempts from these dead nodes.
* Prepare your service for downtime before proceeding.
  Entering `RECOVERY` mode requires a node restart. Restarting an additional node while
  some nodes are already dead may lead to unavailability of data queries (assuming that
  you haven’t lost it already). For example, if you’re using the standard RF=3,
  CL=QUORUM setup, and you’re recovering from a stuck upgrade procedure because one
  of your nodes is dead, restarting another node will cause temporary data query
  unavailability (until the node finishes restarting).

**Procedure**

1. Perform the following query on **every alive node** in the cluster, using e.g. `cqlsh`:
   ```cql
   cqlsh> UPDATE system.scylla_local SET value = 'recovery' WHERE key = 'group0_upgrade_state';
   ```
2. Perform a [rolling restart](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/procedures/config-change/rolling-restart.md) of your alive nodes.
3. Verify that all the nodes have entered `RECOVERY` mode when restarting; look for one of the following messages in their logs:
   > ```console
   > group0_client - RECOVERY mode.
   > raft_group0 - setup_group0: Raft RECOVERY mode, skipping group 0 setup.
   > raft_group0_upgrade - RECOVERY mode. Not attempting upgrade.
   > ```
4. Remove all your dead nodes using the [node removal procedure](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/procedures/cluster-management/remove-node.md).
5. Remove existing Raft cluster data by performing the following queries on **every alive node** in the cluster, using e.g. `cqlsh`:
   ```cql
   cqlsh> TRUNCATE TABLE system.topology;
   cqlsh> TRUNCATE TABLE system.discovery;
   cqlsh> TRUNCATE TABLE system.group0_history;
   cqlsh> DELETE value FROM system.scylla_local WHERE key = 'raft_group0_id';
   ```
6. Make sure that schema is synchronized in the cluster by executing [nodetool describecluster](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/nodetool-commands/describecluster.md) on each node and verifying that the schema version is the same on all nodes.
7. We can now leave `RECOVERY` mode. On **every alive node**, perform the following query:
   ```cql
   cqlsh> DELETE FROM system.scylla_local WHERE key = 'group0_upgrade_state';
   ```
8. Perform a [rolling restart](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/procedures/config-change/rolling-restart.md) of your alive nodes.
9. The Raft upgrade procedure will start anew. [Verify](https://opensource.docs.scylladb.com/branch-6.0/architecture/raft.md#verify-raft-procedure) that it finishes successfully.
10. Perform [the procedure for enabling consistent topology changes](https://opensource.docs.scylladb.com/branch-6.0/upgrade/upgrade-opensource/upgrade-guide-from-5.4-to-6.0/enable-consistent-topology.md).
