# nodetool decommission

**decommission** - Deactivate a selected node by streaming its data to the next node in the ring.

For example:

`nodetool decommission`

#### WARNING
Review current disk space utilization on existing nodes and make sure the amount of data streamed from the node being removed can fit into the disk space available on the remaining nodes. If there is not enough disk space on the remaining nodes, the removal of a node will fail. Add more storage to remaining nodes **before** starting the removal procedure.

Use the `nodetool netstats` command to monitor the progress of the token reallocation.

[Nodetool Reference](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool.md)
