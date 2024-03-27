
After Upgrading from 5.4
----------------------------

The procedure described above applies to clusters where consistent topology updates 
are enabled. The feature is automatically enabled in new clusters.

If you've upgraded an existing cluster from version 5.4, ensure that you 
:doc:`manually enabled consistent topology updates </upgrade/upgrade-opensource/upgrade-guide-from-5.4-to-6.0/enable-consistent-topology>`.
Without consistent topology updates enabled, you must take additional steps
to enable authentication: 
    
* Before you start the procedure, set the ``system_auth`` keyspace replication factor 
  to the number of nodes in the datacenter via cqlsh. It allows you to ensure that
  the user's information is kept highly available for the cluster. If ``system_auth`` 
  is not equal to the number of nodes and a node fails, the user whose information 
  is on that node will be denied access.
* After you start cqlsh with the default superuser username and password, run 
  a repair on the ``system_auth`` keyspace on all the nodes in the cluster, for example: 
  ``nodetool repair -pr system_auth``