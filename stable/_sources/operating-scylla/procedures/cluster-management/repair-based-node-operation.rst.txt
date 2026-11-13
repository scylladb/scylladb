====================================
Repair-Based Node Operations (RBNO)
====================================

In ScyllaDB, data is transferred between nodes during:

* Topology changes via node operations, such as adding or removing nodes.
* Repair - a row-level background process to compare and sync data between nodes.

By default, the row-level repair mechanism used for the repair process is also 
used during node operations (instead of streaming). We refer to it as 
Repair-Based Node Operations (RBNO).

RBNO is more robust, reliable, and safer for data consistency than streaming.
In particular, a failed node operation can resume from the point it stopped -
without sending data that has already been synced, which is a significant 
time-saver when adding or removing large nodes. In addition, with RBNO enabled,
you don't need to run repair before or after node operations, such as replace
or removenode.

RBNO is enabled for the following node operations:

* bootstrap
* decommission
* rebuild
* removenode
* replace

The following configuration options can be used to enable or disable RBNO:

* ``enable_repair_based_node_ops= true|false`` - Enables or disables RBNO.
* ``allowed_repair_based_node_ops= "replace,removenode,rebuild,bootstrap,decommission"`` - 
  Specifies the node operations for which the RBNO mechanism is enabled.

See :doc:`Configuration Parameters </reference/configuration-parameters/>` for details.

