# Start Clusters Cleanly

In cases where you needed to shut down your cluster, use this procedure to bring it back up.

**Before you begin**

* Confirm that the cluster was shut down using the [shutdown procedure](https://opensource.docs.scylladb.com/branch-5.4/operating-scylla/procedures/cluster-management/safe-shutdown.md).
* (Only for versions prior to Scylla Open Source 4.3 and Scylla Enterprise 2021.1) Confirm that you know which nodes are the seed nodes. Seed nodes are specified in the `scylla.yaml` file.

**Procedure**

#### NOTE
If your Scylla version is earlier than Scylla Open Source 4.3 or Scylla Enterprise 2021.1, start the seed
nodes first. Validate that the seed nodes have all returned to normal by running [nodetool status](https://opensource.docs.scylladb.com/branch-5.4/operating-scylla/nodetool-commands/status.md).
If each seed node’s status is listed as `UN`, you can start the remaining nodes.

1. Start the nodes in parallel.

   Supported OS
   ```shell
   sudo systemctl start scylla-server
   ```

   Docker
   ```shell
   docker exec -it some-scylla supervisorctl start scylla
   ```

   (with *some-scylla* container already running)
2. Validate that the nodes have all returned to normal. Run [nodetool status](https://opensource.docs.scylladb.com/branch-5.4/operating-scylla/nodetool-commands/status.md).
   If each node’s status is listed as `UN`, then the start command has been executed successfully.
