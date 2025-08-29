Repair Based Node Operations
****************************

Scylla has two use cases for transferring data between nodes: 

- Topology changes, like adding and removing nodes.
- Repair, a background process to compare and sync data between nodes.

Up to Scylla 4.6, the two used different underline logic. In later releases, the same data transferring logic used for repair is also used for topology changes, making it more robust, reliable, and safer for data consistency. In particular, node operations can restart from the same point it stopped without sending data that has been synced, a significant time-saver when adding or removing large nodes.
In 4.6, Repair Based Node Operations (RBNO) is enabled by default only for replace node operation. 
Example from scylla.yaml:

.. code:: yaml
   
   Enable_repair_based_node_ops: true
   allowed_repair_based_node_ops: replace

To enable other operations (experimental), add them as a comma-separated list to allowed_repair_based_node_ops. Available operations are:

* bootstrap
* replace
* removenode
* decommission
* rebuild

