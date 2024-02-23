
After Upgrading from 5.4
----------------------------

The procedure described above applies to clusters where consistent topology updates 
are enabled. The feature is automatically enabled in new clusters.

If you've upgraded an existing cluster from version 5.4, ensure that you 
:doc:`manually enabled consistent topology updates </upgrade/upgrade-opensource/upgrade-guide-from-5.4-to-6.0/enable-raft-topology>`.
Without consistent topology updates enabled, you must consider the following
limitations while applying the procedure:

* You can only bootstrap one node at a time. You need to wait until the status 
  of one new node becomes UN (Up Normal) before adding another new node.
* If the node starts bootstrapping but fails in the middle, for example, due to 
  a power loss, you can retry bootstrap by restarting the node. If you don't want to
  retry, or the node refuses to boot on subsequent attempts, consult the 
  :doc:`Handling Membership Change Failures </operating-scylla/procedures/cluster-management/handling-membership-change-failures>`
  document. 