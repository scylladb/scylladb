# Add a Decommissioned Node Back to a ScyllaDB Cluster

This procedure describes how to add a node to a ScyllaDB cluster after it was decommissioned.
In some cases, one would like to add a decommissioned node back to the cluster, for example, if the node was decommissioned by mistake. The following procedure describes the procedure of doing that, by clearing all data from it, and adding it as a new node in the cluster

## Prerequisites

Before adding the new node again to the cluster, we need to verify that the node was removed using the [nodetool status](https://opensource.docs.scylladb.com/branch-6.1/operating-scylla/nodetool-commands/status.md) command.

**For Example:**

The decommissioned node’s IP is `192.168.1.203`. Notice that this node is not in the cluster.

```shell
Datacenter: DC1
  Status=Up/Down
  State=Normal/Leaving/Joining/Moving
  --  Address        Load       Tokens  Owns (effective)                         Host ID         Rack
  UN  192.168.1.201  112.82 KB  256     32.7%             8d5ed9f4-7764-4dbd-bad8-43fddce94b7c   B1
  UN  192.168.1.202  91.11 KB   256     32.9%             125ed9f4-7777-1dbn-mac8-43fddce9123e   B1
```

## Procedure

1. Stop the decommissioned node using
<br/>

Supported OS

```shell
sudo systemctl stop scylla-server
```

Docker

```shell
docker exec -it some-scylla supervisorctl stop scylla
```

(without stopping *some-scylla* container)

2. Delete the data and commitlog folders
<br/>
```sh
sudo rm -rf /var/lib/scylla/data
sudo find /var/lib/scylla/commitlog -type f -delete
sudo find /var/lib/scylla/hints -type f -delete
sudo find /var/lib/scylla/view_hints -type f -delete
```

Since the node is added back to the cluster as a new node, you must delete the old node’s data folder. Otherwise, the old node’s state (like bootstrap status), will prevent the new node from starting its init procedure

3. Follow the [Adding a New Node Into an Existing ScyllaDB Cluster](https://opensource.docs.scylladb.com/branch-6.1/operating-scylla/procedures/cluster-management/add-node-to-cluster.md) procedure to add the decommissioned node back into the cluster
<br/>
