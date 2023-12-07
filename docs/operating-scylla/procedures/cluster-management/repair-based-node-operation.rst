====================================
Repair-Based Node Operations (RBNO)
====================================

In ScyllaDB, data is transferred between nodes during:

* Topology changes via node operations, such as adding or removing nodes.
* Repair - a row-level background process to compare and sync data between nodes.

By default, the row-level repair mechanism used for the repair process is also 
used during node operations (instead of streaming). We refer to it as 
Repair-Based Node Operations (RBNO).

RBNO provides a more robust, reliable, and safer data streaming for node operations like node-replace and node-add/remove. 
In particular, a failed node operation can resume from the point it stopped - without sending data that has already been synced. 
In addition, with RBNO enabled, you don’t need to repair before or after node operations, such as replace or removenode.

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

