# Start Clusters Cleanly

In cases where you needed to shut down your cluster, use this procedure to bring it back up.

**Before you begin**

Confirm that the cluster was shut down using the [shutdown procedure](https://opensource.docs.scylladb.com/stable/operating-scylla/procedures/cluster-management/safe-shutdown.md).

**Procedure**

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
2. Validate that the nodes have all returned to normal. Run [nodetool status](https://opensource.docs.scylladb.com/stable/operating-scylla/nodetool-commands/status.md).
   If each node’s status is listed as `UN`, then the start command has been executed successfully.
