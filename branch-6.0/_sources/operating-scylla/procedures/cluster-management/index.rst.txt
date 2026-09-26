Cluster Management Procedures
=============================

.. toctree::
   :hidden:

   Single DC <create-cluster> 
   Multi-DC <create-cluster-multidc>
   New cluster on EC2  <ec2-dc>
   New node on a cluster <add-node-to-cluster>
   Add a new DC <add-dc-to-existing-dc>
   Upscale a Cluster <scale-up-cluster>
   Remove a node <remove-node>
   Replace one dead node <replace-dead-node>
   Replace multiple dead nodes <replace-dead-node-or-more>
   Replace a Seed Node <replace-seed-node>
   Replace a running node <replace-running-node>
   Safely Remove a Joining Node <safely-removing-joining-node> 
   rebuild-node
   Remove a DC <decommissioning-data-center>
   Clear Data <clear-data>
   Add a Decommissioned Node Back to a Scylla Cluster <revoke-decommission>
   Remove a Seed Node from Seed List <remove-seed>
   Update Topology Strategy From Simple to Network <update-topology-strategy-from-simple-to-network>
   Safely Shutdown Your Cluster <safe-shutdown>
   Safely Restart Your Cluster <safe-start>
   Handling Membership Change Failures <handling-membership-change-failures>
   repair-based-node-operation

.. panel-box::
  :title: Cluster and DC Creation
  :id: "getting-started"
  :class: my-panel

  Procedures for Cluster and Datacenter Management


  * :doc:`Create a ScyllaDB Cluster - Single Data Center (DC) </operating-scylla/procedures/cluster-management/create-cluster/>`

  * :doc:`Create a ScyllaDB Cluster - Multi Data Center (DC) </operating-scylla/procedures/cluster-management/create-cluster-multidc/>`

  * :doc:`Create a ScyllaDB Cluster on EC2 (Single or Multi Data Center)  </operating-scylla/procedures/cluster-management/ec2-dc/>`

  * :doc:`Remove a Data-Center from a ScyllaDB Cluster </operating-scylla/procedures/cluster-management/decommissioning-data-center/>`

  * :doc:`Remove a Seed Node from Seed List </operating-scylla/procedures/cluster-management/remove-seed/>`

  * :doc:`Upscale a Cluster </operating-scylla/procedures/cluster-management/scale-up-cluster/>`

  * :doc:`Safely Shutdown Your Cluster <safe-shutdown>`

  * :doc:`Safely Start or Restart Your Cluster <safe-start>`

.. panel-box::
  :title: Node Management
  :id: "getting-started"
  :class: my-panel

  Procedures for Node Management

  * :doc:`Adding a New Node Into an Existing ScyllaDB Cluster - Out Scale </operating-scylla/procedures/cluster-management/add-node-to-cluster/>`

  * :doc:`Adding a New Data-Center Into an Existing ScyllaDB Cluster - Out Scale </operating-scylla/procedures/cluster-management/add-dc-to-existing-dc/>`

  * :doc:`Remove a Node from a ScyllaDB Cluster - Down Scale </operating-scylla/procedures/cluster-management/remove-node/>`

  * :doc:`Replace a Dead Node in a ScyllaDB Cluster </operating-scylla/procedures/cluster-management/replace-dead-node/>`

  * :doc:`Replace More than One Dead Node in a ScyllaDB Cluster </operating-scylla/procedures/cluster-management/replace-dead-node-or-more/>`

  * :doc:`Replace a Seed Node in a ScyllaDB Cluster </operating-scylla/procedures/cluster-management/replace-seed-node/>`

  * :doc:`Replace a Running Node in a ScyllaDB Cluster </operating-scylla/procedures/cluster-management/replace-running-node/>`

  * :doc:`Safely Remove a Joining Node </operating-scylla/procedures/cluster-management/safely-removing-joining-node>`

  * :doc:`What to do if a Node Starts Automatically </operating-scylla/procedures/cluster-management/clear-data/>`

  * :doc:`Add a Decommissioned Node Back to a ScyllaDB Cluster </operating-scylla/procedures/cluster-management/revoke-decommission/>`

  * :doc:`Handling Membership Change Failures </operating-scylla/procedures/cluster-management/handling-membership-change-failures>`

  * :ref:`Add Bigger Nodes to a Cluster <add-bigger-nodes-to-a-cluster>`

  * :doc:`Repair Based Node Operations (RBNO) </operating-scylla/procedures/cluster-management/repair-based-node-operation>`

.. panel-box::
  :title: Topology Changes
  :id: "getting-started"
  :class: my-panel

  Procedures for Changing the Topology


  * :doc:`Update Topology Strategy From Simple to Network </operating-scylla/procedures/cluster-management/update-topology-strategy-from-simple-to-network>`
  * Some of these procedures are also covered in the `MMS course <https://university.scylladb.com/courses/the-mutant-monitoring-system-training-course/>`_ and in the `ScyllaDB Operations course <https://university.scylladb.com/courses/scylla-operations>`_ on ScyllaDB University

.. panel-box::
  :title: Handling Failures
  :id: "getting-started"
  :class: my-panel

  Procedures for handling failures and practical examples of different scenarios.

  * :ref:`Handling Failures<raft-handling-failures>`
