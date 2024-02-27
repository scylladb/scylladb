
After Upgrading from 5.4
----------------------------

The procedure described above applies to clusters where consistent topology updates 
are enabled. The feature is automatically enabled in new clusters.

If you've upgraded an existing cluster from version 5.4, ensure that you 
:doc:`manually enabled consistent topology updates </upgrade/upgrade-opensource/upgrade-guide-from-5.4-to-6.0/enable-consistent-topology>`.
Without consistent topology updates enabled, you must consider the following
limitations while applying the procedure:
    
* It’s essential to ensure the removed node will **never** come back to the cluster, 
  which might adversely affect your data (data resurrection/loss). To prevent the removed 
  node from rejoining the cluster, remove that node from the cluster network or VPC.
* You can only remove one node at a time. You need to verify that the node has 
  been removed before removing another one.
* If ``nodetool decommission`` starts executing but fails in the middle, for example, 
  due to a power loss, consult the 
  :doc:`Handling Membership Change Failures </operating-scylla/procedures/cluster-management/handling-membership-change-failures>`
  document. 