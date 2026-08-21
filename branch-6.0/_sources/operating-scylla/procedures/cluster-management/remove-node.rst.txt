
Remove a Node from a ScyllaDB Cluster (Down Scale)
***************************************************

.. scylladb_include_flag:: upgrade-note-remove-node.rst

You can remove nodes from your cluster to reduce its size.

-----------------------
Removing a Running Node
-----------------------

#. Run the :doc:`nodetool status </operating-scylla/nodetool-commands/status>` command to check the status of the nodes in your cluster.

   .. code-block:: console

      Datacenter: DC1
         Status=Up/Down
         State=Normal/Leaving/Joining/Moving
         --  Address        Load       Tokens  Owns (effective)                         Host ID         Rack
         UN  192.168.1.201  112.82 KB  256     32.7%             8d5ed9f4-7764-4dbd-bad8-43fddce94b7c   B1
         UN  192.168.1.202  91.11 KB   256     32.9%             125ed9f4-7777-1dbn-mac8-43fddce9123e   B1
         UN  192.168.1.203  124.42 KB  256     32.6%             675ed9f4-6564-6dbd-can8-43fddce952gy   B1

#. If the node status is **Up Normal (UN)**, run the :doc:`nodetool decommission </operating-scylla/nodetool-commands/decommission>` command
   to remove the node you are connected to. Using ``nodetool decommission`` is the recommended method for cluster scale-down operations. It prevents data loss
   by ensuring that the node you're removing streams its data to the remaining nodes in the cluster.

   If the node is **Joining**, see :doc:`Safely Remove a Joining Node <safely-removing-joining-node>`.

   If the node status is **Down**, see `Removing an Unavailable Node`_.

   .. include:: /operating-scylla/_common/decommission_warning.rst

#. Run the ``nodetool netstats`` command to monitor the progress of the token reallocation.
#. Run the ``nodetool status`` command to verify that the node has been removed.

   .. code-block:: console

      Datacenter: DC1
      Status=Up/Down
      State=Normal/Leaving/Joining/Moving
      --  Address        Load       Tokens  Owns (effective)                         Host ID         Rack
      UN  192.168.1.201  112.82 KB  256     32.7%             8d5ed9f4-7764-4dbd-bad8-43fddce94b7c   B1
      UN  192.168.1.202  91.11 KB   256     32.9%             125ed9f4-7777-1dbn-mac8-43fddce9123e   B1

#. Manually remove the data and commit log stored on that node.
   
   When a node is removed from the cluster, its data is not automatically removed.  You need to manually
   remove the data to ensure it is no longer counted against the load on that node. Delete the data with the following commands:

   .. include:: /rst_include/clean-data-code.rst

----------------------------
Removing an Unavailable Node
----------------------------

If the node status is **Down Normal (DN)**, you should try to restore it. Once the node is up, use the ``nodetool decommission``
command (see `Removing a Running Node`_) to remove it.

If all attempts to restore the node have failed and the node is **permanently down**, you can remove the node by running the ``nodetool removenode``
command providing the Host ID of the node you are removing. See :doc:`nodetool removenode </operating-scylla/nodetool-commands/removenode>` for details.

.. warning::
    * Using ``nodetool removenode`` is a fallback procedure that should only be used when a node is permanently down and cannot
      be recovered.
    * You must never use ``nodetool removenode`` to remove a running node that can be reached by other nodes in the cluster.

**Example:**

.. code-block:: console
   
   nodetool removenode 675ed9f4-6564-6dbd-can8-43fddce952gy

The ``nodetool removenode`` command notifies other nodes that the token range it owns needs to be moved and
the nodes should redistribute the data using streaming. Using the command does not guarantee the consistency of the rebalanced data if
stream sources do not have the most recent data. In addition, if some nodes are unavailable or another error occurs,
the ``nodetool removenode`` operation will fail. To ensure successful operation and preserve consistency among replicas, you should:

* Make sure the status of all other nodes in the cluster is Up Normal (UN). If one or more nodes are unavailable, see :doc:`nodetool removenode </operating-scylla/nodetool-commands/removenode>` for instructions.
* Run a full cluster repair **before** ``nodetool removenode``, so all existing replicas have the most up-to-date data.
* In the case of node failures during the ``removenode`` operation, re-run repair before running
  ``nodetool removenode`` (not required when :doc:`Repair Based Node Operations (RBNO) <repair-based-node-operation>` for ``removenode`` 
  is enabled).

.. _remove-node-upgrade-info:

.. scylladb_include_flag:: upgrade-warning-remove-node.rst

Additional Information
----------------------
* :doc:`Nodetool Reference </operating-scylla/nodetool>`
