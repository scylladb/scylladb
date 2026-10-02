# Shutdown Clusters Cleanly

In cases where you need to physically move hardware, or you have no other choice you will need to shut down your cluster in a safe manner.

**Before you begin**

Confirm no applications are running that are using the cluster as backend storage.

**Procedure**

On each node, in parallel:

1. Run the command [nodetool drain](https://opensource.docs.scylladb.com/stable/operating-scylla/nodetool-commands/drain.md).
2. Validate that the drain procedure has completed by running [nodetool status](https://opensource.docs.scylladb.com/stable/operating-scylla/nodetool-commands/status.md). If the node’s status is listed as `DN`, then the drain command has been executed successfully.
3. Stop the node after drain completed successfully.

   Supported OS
   ```shell
   sudo systemctl stop scylla-server
   ```

   Docker
   ```shell
   docker exec -it some-scylla supervisorctl stop scylla
   ```

   (without stopping *some-scylla* container)
4. To start the nodes again safely, proceed to the [Start Clusters Cleanly](https://opensource.docs.scylladb.com/stable/operating-scylla/procedures/cluster-management/safe-start.md) procedure.
