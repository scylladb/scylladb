=========================================
What to do if a Node Starts Automatically 
=========================================

If, for any reason, the Scylla service started before you had a chance to update the configuration file, some of the system tables may already reflect an incorrect status, and unfortunately, a simple restart will not fix the issue.
In this case, the safest way is to stop the service, clean all of the data, and start the service again.

Procedure
---------

#. Stop the Scylla service. 

   .. include:: /rst_include/scylla-commands-stop-index.rst

#. Delete the Data and Commitlog folders.

   .. include:: /rst_include/clean-data-code.rst
   
#. Start the Scylla service.
   
   .. include:: /rst_include/scylla-commands-start-index.rst
   
#. Run 'nodetool status' to verify all nodes are up and joined.   

Additional Topics
-----------------

:doc:`Create a Scylla Cluster - Single Data Center (DC) </operating-scylla/procedures/cluster-management/create-cluster>`

:doc:`Scylla Procedures </operating-scylla/procedures/cluster-management/index/>`
