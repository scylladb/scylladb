# Replace a Running Node in a Scylla Cluster

There are two methods to replace a running node in a Scylla cluster.

1. [Add a new node to the cluster and then decommission the old node]()
2. [Replace a running node - by taking its place in the cluster]()

## Add a new node to the cluster and then decommission the old node

Adding a new node and only then decommissioning the old node allows the cluster to keep the same level of data replication throughout the process, but at the cost of more data being transferred during the procedure.
When adding a new node to a Scylla cluster, existing nodes will give the new node responsibility for a subset of their vNodes, making sure that data is once again equally distributed. In the process, these nodes will stream relevant data to the new node.
When decommissioning a node from a Scylla cluster, it will give its vNodes to other nodes, making sure data is once again equally distributed. In the process, this node will stream its data to the other nodes.
Hence, replacing a node by adding and decommissioning redistribute the vNodes twice, streaming a node worth of data each time.

### Procedure

1. Follow the procedure: [Adding a New Node Into an Existing Scylla Cluster](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/procedures/cluster-management/add-node-to-cluster.md).
2. Decommission the old node using the [Remove a Node from a Scylla Cluster](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/procedures/cluster-management/remove-node.md) procedure
3. Run the [nodetool cleanup](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/nodetool-commands/cleanup.md) command on all the remaining nodes in the cluster
4. Verify that the node removed using [nodetool status](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/nodetool-commands/status.md) command

## Replace a running node - by taking its place in the cluster

Stopping a node and taking its place in the cluster is not as safe as the data replication factor is temporarily reduced during the process. However, it is more efficient, as vNode distribution does not change, and only one node worth of data is streamed

### Procedure

1. Run [nodetool drain](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/nodetool-commands/drain.md) command (Scylla stops listening to its connections from the client and other nodes).
2. Stop the Scylla node you want to replace

Supported OS

```shell
sudo systemctl stop scylla-server
```

Docker

```shell
docker exec -it some-scylla supervisorctl stop scylla
```

(without stopping *some-scylla* container)

1. Follow the [Replace a Dead Node in a Scylla Cluster](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/procedures/cluster-management/replace-dead-node.md) procedure
2. Verify that the node is successfully replaced using [nodetool status](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/nodetool-commands/status.md) command
