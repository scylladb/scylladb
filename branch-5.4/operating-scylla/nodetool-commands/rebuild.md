# Nodetool rebuild

**rebuild** `[<src-dc-name>]` - This command rebuilds a node’s data by streaming data from other nodes in the cluster (similarly to bootstrap).
Rebuild operates on multiple nodes in a Scylla cluster. It streams data from a single source replica when rebuilding a token range. When executing the command, Scylla first figures out which ranges the local node (the one we want to rebuild) is responsible for. Then which node in the cluster contains the same ranges. Finally, Scylla streams the data to the local node.

When [adding a new data-center into an existing Scylla cluster](https://opensource.docs.scylladb.com/branch-5.4/operating-scylla/procedures/cluster-management/add-dc-to-existing-dc.md) use the rebuild command.

#### NOTE
The Scylla rebuild process continues to run in the background, even if the nodetool command is killed or interrupted.

For Example:

```shell
nodetool rebuild <src-dc-name>
```

[Nodetool Reference](https://opensource.docs.scylladb.com/branch-5.4/operating-scylla/nodetool.md)
