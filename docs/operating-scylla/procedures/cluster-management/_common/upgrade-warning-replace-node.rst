
----------------------------
After Upgrading from 5.4
----------------------------

The procedure described above applies to clusters where consistent topology updates 
are enabled. The feature is automatically enabled in new clusters.

If you've upgraded an existing cluster from version 5.4, ensure that you 
:doc:`manually enabled consistent topology updates </upgrade/upgrade-opensource/upgrade-guide-from-5.4-to-6.0/enable-raft-topology>`.
Without consistent topology updates enabled, you must consider the following
limitations while applying the procedure:
    
* It’s essential to ensure the replaced (dead) node will never come back to the cluster, 
  which might lead to a split-brain situation. Remove the replaced (dead) node from 
  the cluster network or VPC.
* You can only replace one node at a time. You need to wait until the status 
  of the new node becomes UN (Up Normal) before replacing another new node.
* If the new node starts and begins the replace operation but then fails in the middle, 
  for example, due to a power loss, you can retry the replace by restarting the node. 
  If you don’t want to retry, or the node refuses to boot on subsequent attempts, consult the 
  :doc:`Handling Membership Change Failures </operating-scylla/procedures/cluster-management/handling-membership-change-failures>`
  document. 