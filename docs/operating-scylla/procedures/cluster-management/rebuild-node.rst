============================================
Recover a Node After Losing the Data Volume
============================================

When running in Scylla on EC2, it is recommended to use instances with fast, ephemeral SSD drives.
Stopping and starting the instance for whatever reason means the data on the node is lost.

The good news is that ScyllaDB is an HA database, and replicas of the data are stored on multiple nodes. 
This is also why it's highly recommended to use a replication factor of at least three per datacenter, for example, ``replication = {'class': 'NetworkTopologyStrategy', 'DC1' : 3, 'DC2' : 3}``.

The following procedure is a particular use case of :doc:`Replace a Dead Node in a ScyllaDB Cluster Procedure </operating-scylla/procedures/cluster-management/replace-dead-node>`, when a node replaces itself using its previous identity (Host ID) and IP address.

#. Open the ``/etc/scylla/scylla.yaml`` file.

#. Add, if not present, else edit the ``replace_node_first_boot`` parameter and change it to the
   Host ID of the node before it restarted.

#. If you know of dead nodes in the cluster, for example, from :doc:`nodetool status </operating-scylla/nodetool-commands/status>`, use :ref:`ignore_dead_nodes_for_replace <confprop_ignore_dead_nodes_for_replace>` to list them.

#. Stop Scylla Server

   .. include:: /rst_include/scylla-commands-stop-index.rst

#. If there are multiple disks, execute a RAID setup for the disks by running the following script: ``/opt/scylladb/scylla-machine-image/scylla_create_devices``.
#. Start Scylla Server

   .. include:: /rst_include/scylla-commands-start-index.rst

#. Revert the ``replace_node_first_boot`` and ``ignore_dead_nodes_for_replace`` parameters to what they were before you ran this procedure.
   For ease of use, you can comment out the ``replace_node_first_boot`` parameter.
