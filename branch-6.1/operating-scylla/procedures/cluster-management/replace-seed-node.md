# Replacing a Dead Seed Node

#### NOTE
The seed concept in gossip has been removed. Starting with ScyllaDB Open Source 4.3 and ScyllaDB Enterprise 2021.1,
a seed node is only used by a new node during startup to learn about the cluster topology. As a result, there’s no need
to replace the node configured with the `seeds` parameter in the `scylla.yaml` file.

In ScyllaDB, it is not possible to bootstrap a seed node. The following steps describe how to replace a dead seed node.

## Prerequisites

Verify that the node is listed as a seed node in the `scylla.yaml` file by running the following command:

`cat /etc/scylla/scylla.yaml | grep seeds:`

If the dead node’s IP address is in the seeds list, it needs to be replaced.

## Procedure

1. Perform steps 1-3 for all the nodes in the cluster:
   > 1. Promote an existing node from the cluster to be a seed node by adding the node IP to the seed list in the `scylla.yaml` file in `/etc/scylla/`.
   > 2. Remove the dead node IP from the seeds providers list. See [Remove a Seed Node from Seed List](https://opensource.docs.scylladb.com/branch-6.1/operating-scylla/procedures/cluster-management/remove-seed.md) for instructions.
   > 3. Restart the node in the cluster by running the following command:

   > Supported OS
   > ```shell
   > sudo systemctl restart scylla-server
   > ```

   > Docker
   > ```shell
   > docker exec -it some-scylla supervisorctl restart scylla
   > ```

   > (without restarting *some-scylla* container)

   > #### NOTE
   > This operation needs to be performed on all the nodes in the cluster. For this reason, you need to orchestrate the procedure so that only a small number of nodes are restarted simultaneously.
   > Use `nodetool status` to verify that restarted nodes are online before restarting more nodes. If too many nodes are offline, the cluster may suffer temporary service degradation or outage.
2. Replace the dead node using the [dead node replacement procedure](https://opensource.docs.scylladb.com/branch-6.1/operating-scylla/procedures/cluster-management/replace-dead-node.md).

Your cluster should have more than one seed node, but it’s not allowed to define all the nodes in the cluster to be seed nodes.
