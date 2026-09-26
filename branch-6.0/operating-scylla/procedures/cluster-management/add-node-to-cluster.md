# Adding a New Node Into an Existing ScyllaDB Cluster (Out Scale)

#### NOTE
If you upgraded your cluster from version 5.4, see [After Upgrading from 5.4](#add-new-node-upgrade-info).

When you add a new node, other nodes in the cluster stream data to the new node. This operation is called bootstrapping and may
be time-consuming, depending on the data size and network bandwidth. If using a [multi-availability-zone](https://opensource.docs.scylladb.com/branch-6.0/faq.md#faq-best-scenario-node-multi-availability-zone), make sure they are balanced.

## Prerequisites

### Check the Status of Nodes

You cannot add new nodes to the cluster if any existing node is down.
Before adding new nodes, check the status of the nodes in the cluster using
[nodetool status](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/nodetool-commands/status.md) command.

### Collect Cluster Information

Log into one of the nodes in the cluster to collect the following information:

> * cluster_name - `grep cluster_name /etc/scylla/scylla.yaml`
> * seeds - `grep seeds: /etc/scylla/scylla.yaml`
> * endpoint_snitch - `grep endpoint_snitch /etc/scylla/scylla.yaml`
> * Scylla version - `scylla --version`
> * Authenticator - `grep authenticator /etc/scylla/scylla.yaml`

<a id="add-node-to-cluster-procedure"></a>

## Procedure

1. Install ScyllaDB on the nodes you want to add to the cluster. See [Getting Started](https://opensource.docs.scylladb.com/branch-6.0/getting-started/index.md) for further instructions. Follow the Scylla installation procedure up to `scylla.yaml` configuration phase. Make sure that the Scylla version of the new node is identical to the other nodes in the cluster.

   If the node starts during the process, follow [What to do if a Node Starts Automatically](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/procedures/cluster-management/clear-data.md).

   #### NOTE
   Make sure to use the same Scylla **patch release** on the new/replaced node, to match the rest of the cluster. It is not recommended to add a new node with a different release to the cluster.
   For example, use the following for installing Scylla patch release (use your deployed version)
   * Scylla Enterprise - `sudo yum install scylla-enterprise-2018.1.9`
   * Scylla open source - `sudo yum install scylla-3.0.3`

   #### NOTE
   It’s important to keep I/O scheduler configuration in sync on nodes with the same hardware.
   That’s why we recommend skipping running scylla_io_setup when provisioning a new node with exactly the same hardware setup as existing nodes in the cluster.

   Instead, we recommend to copy the following files from an existing node to the new node after running scylla_setup and restart scylla-server service (if it is already running):
   : * /etc/scylla.d/io.conf
     * /etc/scylla.d/io_properties.yaml

   Using different I/O scheduler configuration may result in unnecessary bottlenecks.
2. On each node, edit the `scylla.yaml` file `/etc/scylla/` to configure the parameters listed below.
   > * **cluster_name** - Specifies the name of the cluster.
   > * **listen_address** - Specifies the IP address that Scylla used to connect to the other Scylla nodes in the cluster.
   > * **endpoint_snitch** - Specifies the selected snitch.
   > * **rpc_address** - Specifies the address for client connections (Thrift, CQL).
   > * **seeds** - Specifies the IP address of an existing node in the cluster. The new node will use this IP to connect to the cluster and learn the cluster topology and state.
3. Start the nodes with the following command:
   > Supported OS
   > ```shell
   > sudo systemctl start scylla-server
   > ```

   > Docker
   > ```shell
   > docker exec -it some-scylla supervisorctl start scylla
   > ```

   > (with *some-scylla* container already running)
4. Verify that the nodes were added to the cluster using [nodetool status](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/nodetool-commands/status.md) command. Other nodes in the cluster will be streaming data to the new nodes, so the new nodes will be in Up Joining (UJ) status. Wait until the nodes’ status changes to Up Normal (UN) - the time depends on the data size and network bandwidth.

   **Example:**

   Nodes in the cluster are streaming data to the new node:
   ```shell
   Datacenter: DC1
   Status=Up/Down
   State=Normal/Leaving/Joining/Moving
   --  Address        Load       Tokens  Owns (effective)                         Host ID         Rack
   UN  192.168.1.201  112.82 KB  256     32.7%             8d5ed9f4-7764-4dbd-bad8-43fddce94b7c   B1
   UN  192.168.1.202  91.11 KB   256     32.9%             125ed9f4-7777-1dbn-mac8-43fddce9123e   B1
   UJ  192.168.1.203  124.42 KB  256     32.6%             675ed9f4-6564-6dbd-can8-43fddce952gy   B1
   ```

   Nodes in the cluster finished streaming data to the new node:
   ```shell
   Datacenter: DC1
   Status=Up/Down
   State=Normal/Leaving/Joining/Moving
   --  Address        Load       Tokens  Owns (effective)                         Host ID         Rack
   UN  192.168.1.201  112.82 KB  256     32.7%             8d5ed9f4-7764-4dbd-bad8-43fddce94b7c   B1
   UN  192.168.1.202  91.11 KB   256     32.9%             125ed9f4-7777-1dbn-mac8-43fddce9123e   B1
   UN  192.168.1.203  124.42 KB  256     32.6%             675ed9f4-6564-6dbd-can8-43fddce952gy   B1
   ```
5. When the new node status is Up Normal (UN), run the [nodetool cleanup](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/nodetool-commands/cleanup.md) command on all nodes in the cluster except for the new node that has just been added. Cleanup removes keys that were streamed to the newly added node and are no longer owned by the node.

   #### NOTE
   To prevent data resurrection, it’s essential to complete cleanup after adding nodes and before any node is decommissioned or removed.
   However, cleanup may consume significant resources. Use the following guideline to reduce cleanup impact:

   Tip 1: When adding multiple nodes, run the cleanup operations after all nodes are added on all nodes but the last one to be added.

   Tip 2: Postpone cleanup to low demand hours while ensuring it completes successfully before any node is decommissioned or removed.

   Tip 3: Run cleanup one node at a time, reducing overall cluster impact.
6. Wait until the new node becomes UN (Up Normal) in the output of [nodetool status](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/nodetool-commands/status.md) on one of the old nodes.
7. If you are using Scylla Monitoring, update the [monitoring stack](https://monitoring.docs.scylladb.com/stable/install/monitoring_stack.html#configure-scylla-nodes-from-files) to monitor it. If you are using Scylla Manager, make sure you install the [Manager Agent](https://manager.docs.scylladb.com/stable/install-scylla-manager-agent.html), and Manager can access it.

<a id="add-new-node-upgrade-info"></a>

## After Upgrading from 5.4

The procedure described above applies to clusters where consistent topology updates
are enabled. The feature is automatically enabled in new clusters.

If you’ve upgraded an existing cluster from version 5.4, ensure that you
[manually enabled consistent topology updates](https://opensource.docs.scylladb.com/branch-6.0/upgrade/upgrade-opensource/upgrade-guide-from-5.4-to-6.0/enable-consistent-topology.md).
Without consistent topology updates enabled, you must consider the following
limitations while applying the procedure:

* You can only bootstrap one node at a time. You need to wait until the status
  of one new node becomes UN (Up Normal) before adding another new node.
* If the node starts bootstrapping but fails in the middle, for example, due to
  a power loss, you can retry bootstrap by restarting the node. If you don’t want to
  retry, or the node refuses to boot on subsequent attempts, consult the
  [Handling Membership Change Failures](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/procedures/cluster-management/handling-membership-change-failures.md)
  document.
* The `system_auth` keyspace has not been upgraded to `system`.
  As a result, if `authenticator` is set to `PasswordAuthenticator`, you must
  increase the replication factor of the `system_auth` keyspace. It is
  recommended to set `system_auth` replication factor to the number of nodes
  in each DC.
