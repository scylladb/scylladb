# Adding a New Node Into an Existing Scylla Cluster (Out Scale)

When you add a new node, another node in the cluster streams data to the new node. This operation is called bootstrapping and may
be time-consuming, depending on the data size and network bandwidth. If using a [multi-availability-zone](https://opensource.docs.scylladb.com/branch-5.1/faq.md#faq-best-scenario-node-multi-availability-zone), make sure they are balanced.

## Prerequisites

Before adding the new node, check the node’s status in the cluster using [nodetool status](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/nodetool-commands/status.md) command.
You cannot add new nodes to the cluster if any nodes are down.

For example:

```shell
Datacenter: DC1
Status=Up/Down
State=Normal/Leaving/Joining/Moving
--  Address        Load       Tokens  Owns (effective)                         Host ID         Rack
UN  192.168.1.201  112.82 KB  256     32.7%             8d5ed9f4-7764-4dbd-bad8-43fddce94b7c   B1
DN  192.168.1.202  91.11 KB   256     32.9%             125ed9f4-7777-1dbn-mac8-43fddce9123e   B1
```

In the example above,  the node with the IP address 192.168.1.202 has a status of Down (DN). To proceed, you need to start the node that is down or [remove it from the cluster](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/procedures/cluster-management/remove-node.md).

Login to one of the nodes in the cluster to collect the following information:

* cluster_name - `cat /etc/scylla/scylla.yaml | grep cluster_name`
* seeds - `cat /etc/scylla/scylla.yaml | grep seeds:`
* endpoint_snitch - `cat /etc/scylla/scylla.yaml | grep endpoint_snitch`
* Scylla version - `scylla --version`
* Authentication status - `cat /etc/scylla/scylla.yaml | grep authenticator`

#### NOTE
If `authenticator` is set to `PasswordAuthenticator` - increase the `system_auth` table replication factor.

For example

`ALTER KEYSPACE system_auth WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'dc1' : <new_replication_factor>};`

It is recommended to set `system_auth` replication factor to the number of nodes in each DC.

<a id="add-node-to-cluster-procedure"></a>

## Procedure

1. Install Scylla on a new node. See [Getting Started](https://opensource.docs.scylladb.com/branch-5.1/getting-started/index.md) for further instructions - follow the Scylla installation procedure up to `scylla.yaml` configuration phase. Make sure that the Scylla version of the new node is identical to the other nodes in the cluster.
   > If the node starts during the process, follow [What to do if a Node Starts Automatically](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/procedures/cluster-management/clear-data.md).

   > #### NOTE
   > Make sure to use the same Scylla **patch release** on the new/replaced node, to match the rest of the cluster. It is not recommended to add a new node with a different release to the cluster.
   > For example, use the following for installing Scylla patch release (use your deployed version)
   > * Scylla Enterprise - `sudo yum install scylla-enterprise-2018.1.9`
   > * Scylla open source - `sudo yum install scylla-3.0.3`

   > #### NOTE
   > It’s important to keep I/O scheduler configuration in sync on nodes with the same hardware.
   > That’s why we recommend skipping running scylla_io_setup when provisioning a new node with exactly the same hardware setup as existing nodes in the cluster.

   > Instead, we recommend to copy the following files from an existing node to the new node after running scylla_setup and restart scylla-server service (if it is already running):
   > : * /etc/scylla.d/io.conf
   >   * /etc/scylla.d/io_properties.yaml

   > Using different I/O scheduler configuration may result in unnecessary bottlenecks.
2. In the `scylla.yaml` file in `/etc/scylla/`, edit the following parameters:
   > * **cluster_name** - Specifies the name of the cluster.
   > * **listen_address** - Specifies the IP address that Scylla used to connect to the other Scylla nodes in the cluster.
   > * **endpoint_snitch** - Specifies the selected snitch.
   > * **rpc_address** - Specifies the address for client connections (Thrift, CQL).
   > * **seeds** - Specifies the IP address of an existing node in the cluster. The new node will use this IP to connect to the cluster and learn the cluster topology and state.

   #### NOTE
   In earlier versions of ScyllaDB, seed nodes assisted in gossip. Starting with Scylla Open Source 4.3 and Scylla Enterprise 2021.1, the seed concept in gossip has been removed. If you are using an earlier version of ScyllaDB, you need to configure the seeds parameter in the following way:
   * Specify the list of the current seed nodes in the cluster.
   * Do not list the node you’re adding as a seed node.

   See [Scylla Seed Nodes](https://opensource.docs.scylladb.com/branch-5.1/kb/seed-nodes.md) for more information.

   We recommend updating your ScyllaDB to version 4.3 or later (Open Source) or 2021.1 or later (Enterprise).
3. Start the ScyllaDB node with the following command:
   > Supported OS
   > ```shell
   > sudo systemctl start scylla-server
   > ```

   > Docker
   > ```shell
   > docker exec -it some-scylla supervisorctl start scylla
   > ```

   > (with *some-scylla* container already running)
4. Verify that the node was added to the cluster using [nodetool status](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/nodetool-commands/status.md) command. Another node in the cluster will be streaming data to the new node, so the new node will be in Up Joining (UJ) status. Wait until the node’s status changes to Up Normal (UN) - the time depends on the data size and network bandwidth.
   > **For example:**

   > The node in the cluster is streaming data to the new node:
   > ```shell
   > Datacenter: DC1
   > Status=Up/Down
   > State=Normal/Leaving/Joining/Moving
   > --  Address        Load       Tokens  Owns (effective)                         Host ID         Rack
   > UN  192.168.1.201  112.82 KB  256     32.7%             8d5ed9f4-7764-4dbd-bad8-43fddce94b7c   B1
   > UN  192.168.1.202  91.11 KB   256     32.9%             125ed9f4-7777-1dbn-mac8-43fddce9123e   B1
   > UJ  192.168.1.203  124.42 KB  256     32.6%             675ed9f4-6564-6dbd-can8-43fddce952gy   B1
   > ```

   The node in the cluster has finished streaming data to the new node:
   ```shell
   Datacenter: DC1
   Status=Up/Down
   State=Normal/Leaving/Joining/Moving
   --  Address        Load       Tokens  Owns (effective)                         Host ID         Rack
   UN  192.168.1.201  112.82 KB  256     32.7%             8d5ed9f4-7764-4dbd-bad8-43fddce94b7c   B1
   UN  192.168.1.202  91.11 KB   256     32.9%             125ed9f4-7777-1dbn-mac8-43fddce9123e   B1
   UN  192.168.1.203  124.42 KB  256     32.6%             675ed9f4-6564-6dbd-can8-43fddce952gy   B1
   ```
5. When the new node status is Up Normal (UN), use [nodetool cleanup](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/nodetool-commands/cleanup.md) cleanup command on all the nodes in the cluster except the new node that has just been added. It will remove keys that no longer belong to the node. Run this command one node at a time. It is possible to postpone this step to low demand hours.
   > #### NOTE
   > If you are using Scylla Enterprise 2018.1.5 or earlier or Scylla Open source 2.3 or earlier, **do not** run the `nodetool cleanup` command before upgrading to the latest release of your branch. See this [issue](https://github.com/scylladb/scylla/issues/3872) for more information.
6. Wait until the new node becomes UN (Up Normal) in the output of [nodetool status](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/nodetool-commands/status.md) on one of the old nodes.
   > #### NOTE
   > If you are using ScyllaDB Open Source 4.3 or later or ScyllaDB Enterprise 2021.1 or later and configure the list of seed nodes to participate in gossip, you can now edit the `scylla.yaml` files to add the new node as a seed node.
   > You don’t need to restart the Scylla service after modifying the seeds list in `scylla.yaml`.
7. If you are using Scylla Monitoring, update the [monitoring stack](https://monitoring.docs.scylladb.com/stable/install/monitoring_stack.html#configure-scylla-nodes-from-files) to monitor it. If you are using Scylla Manager, make sure you install the [Manager Agent](https://manager.docs.scylladb.com/stable/install-scylla-manager-agent.html), and Manager can access it.
