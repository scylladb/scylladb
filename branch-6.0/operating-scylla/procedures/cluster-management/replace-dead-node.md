# Replace a Dead Node in a ScyllaDB Cluster

#### NOTE
If you upgraded your cluster from version 5.4, see [After Upgrading from 5.4](#replace-node-upgrade-info).

Replace dead node operation will cause the other nodes in the cluster to stream data to the node that was replaced. This operation can take some time (depending on the data size and network bandwidth).

This procedure is for replacing one dead node. You can replace more than one dead node in parallel.

## Prerequisites

### Quorum of Nodes

Updating the cluster topology requires at least a quorum of nodes in a cluster to be available.
If the quorum is lost, it must be restored before you change the cluster topology.
See [Handling Node Failures](https://opensource.docs.scylladb.com/branch-6.0/troubleshooting/handling-node-failures.md) for details.

You can check the status of the nodes in the cluster using the [nodetool status](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/nodetool-commands/status.md) command.

### Verify the Node Status

Verify the status of the node you want to replace using the [nodetool status](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/nodetool-commands/status.md) command.

In the following example, the status of the node with the IP address 192.168.1.203 is
Down (DN), and the node can be replaced.

```shell
Datacenter: DC1
Status=Up/Down
State=Normal/Leaving/Joining/Moving
--  Address        Load       Tokens  Owns (effective)                         Host ID         Rack
UN  192.168.1.201  112.82 KB  256     32.7%             8d5ed9f4-7764-4dbd-bad8-43fddce94b7c   B1
UN  192.168.1.202  91.11 KB   256     32.9%             125ed9f4-7777-1dbn-mac8-43fddce9123e   B1
DN  192.168.1.203  124.42 KB  256     32.6%             675ed9f4-6564-6dbd-can8-43fddce952gy   B1
```

### Remove the Data

Log in to the dead node and manually remove the data if you can. Delete the data with the following commands:

```sh
sudo rm -rf /var/lib/scylla/data
sudo find /var/lib/scylla/commitlog -type f -delete
sudo find /var/lib/scylla/hints -type f -delete
sudo find /var/lib/scylla/view_hints -type f -delete
```

### Collect Cluster Information

Login to one of the nodes in the cluster with the UN status. Collect the following info from the node:

> * cluster_name - `cat /etc/scylla/scylla.yaml | grep cluster_name`
> * seeds - `cat /etc/scylla/scylla.yaml | grep seeds:`
> * endpoint_snitch - `cat /etc/scylla/scylla.yaml | grep endpoint_snitch`
> * Scylla version - `scylla --version`

## Procedure

1. Install Scylla on a new node, see [Getting Started](https://opensource.docs.scylladb.com/branch-6.0/getting-started/index.md) for further instructions. Follow the Scylla install procedure up to `scylla.yaml` configuration phase. Ensure that the Scylla version of the new node is identical to the other nodes in the cluster.

   #### NOTE
   Make sure to use the same Scylla **patch release** on the new/replaced node, to match the rest of the cluster. It is not recommended to add a new node with a different release to the cluster.
   For example, use the following for installing Scylla patch release (use your deployed version)
   * Scylla Enterprise - `sudo yum install scylla-enterprise-2018.1.9`
   * Scylla open source - `sudo yum install scylla-3.0.3`
2. In the `scylla.yaml` file edit the parameters listed below. The file can be found under `/etc/scylla/`.
   > - **cluster_name** - Set the selected cluster_name
   > - **listen_address** - IP address that Scylla uses to connect to other Scylla nodes in the cluster
   > - **seeds** - Set the seed nodes
   > - **endpoint_snitch** - Set the selected snitch
   > - **rpc_address** - Address for client connection (Thrift, CQL)
3. Add the `replace_node_first_boot` parameter to the `scylla.yaml` config file on the new node. This line can be added to any place in the config file. After a successful node replacement, there is no need to remove it from the `scylla.yaml` file. (Note: The obsolete parameters “replace_address” and “replace_address_first_boot” are not supported and should not be used). The value of the `replace_node_first_boot` parameter should be the Host ID of the node to be replaced.

   For example (using the Host ID of the failed node from above):

   `replace_node_first_boot: 675ed9f4-6564-6dbd-can8-43fddce952gy`
4. Start the new node.

   Supported OS
   ```shell
   sudo systemctl start scylla-server
   ```

   Docker
   ```shell
   docker exec -it some-scylla supervisorctl start scylla
   ```

   (with *some-scylla* container already running)
5. Verify that the node has been added to the cluster using `nodetool status` command.

   For example:
   > ```shell
   > Datacenter: DC1
   > Status=Up/Down
   > State=Normal/Leaving/Joining/Moving
   > --  Address        Load       Tokens  Owns (effective)                         Host ID         Rack
   > UN  192.168.1.201  112.82 KB  256     32.7%             8d5ed9f4-7764-4dbd-bad8-43fddce94b7c   B1
   > UN  192.168.1.202  91.11 KB   256     32.9%             125ed9f4-7777-1dbn-mac8-43fddce9123e   B1
   > DN  192.168.1.203  124.42 KB  256     32.6%             675ed9f4-6564-6dbd-can8-43fddce952gy   B1
   > ```

   > `192.168.1.203` is the dead node.

   > The replacing node `192.168.1.204` will be bootstrapping data.
   > We will not see `192.168.1.204` in `nodetool status` during the bootstrap.

   > Use `nodetool gossipinfo` to see `192.168.1.204` is in NORMAL status.
   > ```shell
   > /192.168.1.204
   >   generation:1553759984
   >   heartbeat:104
   >   HOST_ID:655ae64d-e3fb-45cc-9792-2b648b151b67
   >   STATUS:NORMAL
   >   RELEASE_VERSION:3.0.8
   >   X3:3
   >   X5:
   >   NET_VERSION:0
   >   DC:DC1
   >   X4:0
   >   SCHEMA:2790c24e-39ff-3c0a-bf1c-cd61895b6ea1
   >   RPC_ADDRESS:192.168.1.204
   >   X2:
   >   RACK:B1
   >   INTERNAL_IP:192.168.1.204

   > /192.168.1.203
   >   generation:1553759866
   >   heartbeat:2147483647
   >   HOST_ID:675ed9f4-6564-6dbd-can8-43fddce952gy
   >   STATUS:shutdown,true
   >   RELEASE_VERSION:3.0.8
   >   X3:3
   >   X5:0:18446744073709551615:1553759941343
   >   NET_VERSION:0
   >   DC:DC1
   >   X4:1
   >   SCHEMA:2790c24e-39ff-3c0a-bf1c-cd61895b6ea1
   >   RPC_ADDRESS:192.168.1.203
   >   RACK:B1
   >   LOAD:1.09776e+09
   >   INTERNAL_IP:192.168.1.203
   > ```

   > After the bootstrapping is over, `nodetool status` will show:
   > ```shell
   > Datacenter: DC1
   > Status=Up/Down
   > State=Normal/Leaving/Joining/Moving
   > --  Address        Load       Tokens  Owns (effective)                         Host ID         Rack
   > UN  192.168.1.201  112.82 KB  256     32.7%             8d5ed9f4-7764-4dbd-bad8-43fddce94b7c   B1
   > UN  192.168.1.202  91.11 KB   256     32.9%             125ed9f4-7777-1dbn-mac8-43fddce9123e   B1
   > UN  192.168.1.204  124.42 KB  256     32.6%             655ae64d-e3fb-45cc-9792-2b648b151b67   B1
   > ```
6. Run the `nodetool repair` command on the node that was replaced to make sure that the data is synced with the other nodes in the cluster. You can use [Scylla Manager](https://manager.docs.scylladb.com/) to run the repair.
   > #### NOTE
   > When [Repair Based Node Operations (RBNO)](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/procedures/cluster-management/repair-based-node-operation.md) for **replace** is enabled, there is no need to rerun repair.

## Setup RAID Following a Restart

In case you need to to restart (stop + start, not reboot) an instance with ephemeral storage, like EC2 i3, or i3en nodes,  you should be aware that:

> ephemeral volumes persist only for the life of the instance. When you stop, hibernate, or terminate an instance, the applications and data in its instance store volumes are erased. (see [https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/storage-optimized-instances.html](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/storage-optimized-instances.html))

In this case, the node’s data will be cleaned after restart. To remedy this, you need to recreate the RAID again.

1. Stop the Scylla server on the node you restarted. The rest of the commands will run on this node as well.

   Supported OS
   ```shell
   sudo systemctl stop scylla-server
   ```

   Docker
   ```shell
   docker exec -it some-scylla supervisorctl stop scylla
   ```

   (without stopping *some-scylla* container)
2. Run the following command, remembering not to mount an invalid RAID disk after reboot:
   ```none
   sudo sed -e '/.*scylla/s/^/#/g' -i /etc/fstab
   ```
3. Run the following command to replace the instance whose ephemeral volumes were erased (previously known by the Host ID of the node you are restarting) with the restarted instance. The restarted node will be assigned a new random Host ID.
   ```none
   echo 'replace_node_first_boot: 675ed9f4-6564-6dbd-can8-43fddce952gy' | sudo tee --append /etc/scylla/scylla.yaml
   ```
4. Run the following command to re-setup RAID
   ```none
   sudo /opt/scylladb/scylla-machine-image/scylla_create_devices
   ```
5. Start Scylla Server

   Supported OS
   ```shell
   sudo systemctl start scylla-server
   ```

   Docker
   ```shell
   docker exec -it some-scylla supervisorctl start scylla
   ```

   (with *some-scylla* container already running)

Sometimes the public/ private IP of instance is changed after restart. If so refer to the Replace [Procedure]() above.

<a id="replace-node-upgrade-info"></a>

## After Upgrading from 5.4

The procedure described above applies to clusters where consistent topology updates
are enabled. The feature is automatically enabled in new clusters.

If you’ve upgraded an existing cluster from version 5.4, ensure that you
[manually enabled consistent topology updates](https://opensource.docs.scylladb.com/branch-6.0/upgrade/upgrade-opensource/upgrade-guide-from-5.4-to-6.0/enable-consistent-topology.md).
Without consistent topology updates enabled, you must consider the following
limitations while applying the procedure:

* It’s essential to ensure the replaced (dead) node will never come back to the cluster,
  which might lead to a split-brain situation. Remove the replaced (dead) node from
  the cluster network or VPC.
* You can only replace one node at a time. You need to wait until the status
  of the new node becomes UN (Up Normal) before replacing another new node.
* If the new node starts and begins the replace operation but then fails in the middle,
  for example, due to a power loss, you can retry the replace by restarting the node.
  If you don’t want to retry, or the node refuses to boot on subsequent attempts, consult the
  [Handling Membership Change Failures](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/procedures/cluster-management/handling-membership-change-failures.md)
  document.
