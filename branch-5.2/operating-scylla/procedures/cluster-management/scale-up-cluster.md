# Upscale a Cluster

Upcaling your cluster involves moving the cluster to a larger instance. With this procedure, it can be done without downtime.

Scylla was designed with big servers and multi-cores in mind. In most cases, it is better to run a smaller cluster on a bigger machine instance than a larger cluster on a small machine instance.
However, there may be cases where you started with a small cluster, and you now you want to upscale.

There are a few alternatives to do this:

* [Add Bigger Nodes to a Cluster]() and removing the old smaller nodes. This is useful when you can not upscale (add more CPU) for each node, for example using I3 instances on EC2.
* [Upscale Nodes by Adding CPUs]()

<a id="add-bigger-nodes-to-a-cluster"></a>

## Add Bigger Nodes to a Cluster

This procedure can be used to either upscale an entire cluster or to upscale a single node.

1. Add [new bigger nodes](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/procedures/cluster-management/add-node-to-cluster.md)  to the cluster. Confirm Streaming has completed before continuing.
2. [Remove the old smaller nodes](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/procedures/cluster-management/remove-node.md). Confirm Streaming has completed before continuing.
3. Repeat steps 1 and 2 until the entire cluster is using bigger nodes.

#### NOTE
The cluster is only as strong as its weakest node. Do not overload the cluster before all nodes are as upscaled.

## Upscale Nodes by Adding CPUs

This procedure is only useful for entire clusters, not individual nodes.
Do the following on each node in the cluster:

1. Run [nodetool drain](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool-commands/drain.md) to stop traffic to the node.
2. Stop the service

   Supported OS
   ```shell
   sudo systemctl stop scylla-server
   ```

   Docker
   ```shell
   docker exec -it some-scylla supervisorctl stop scylla
   ```

   (without stopping *some-scylla* container)
3. Add cores
4. Run `scylla_setup` to set Scylla to the new HW configuration.
5. Start the service

   Supported OS
   ```shell
   sudo systemctl start scylla-server
   ```

   Docker
   ```shell
   docker exec -it some-scylla supervisorctl start scylla
   ```

   (with *some-scylla* container already running)

#### NOTE
Updating the number of cores will cause Scylla to reshard the SSTables to match the new core number. This is done by compacting all of the data on disk at startup.
