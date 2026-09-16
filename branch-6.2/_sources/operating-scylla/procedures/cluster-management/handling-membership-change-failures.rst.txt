Handling Cluster Membership Change Failures
*******************************************

.. scylladb_include_flag:: membership-change-failures-note.rst

A failure may happen in the middle of a cluster membership change (that is bootstrap, decommission, removenode, or replace), such as loss of power. If that happens, you should ensure that the cluster is brought back to a consistent state as soon as possible. Further membership changes might be impossible until you do so.

For example, a node that crashed in the middle of decommission might leave the cluster in a state where it considers the node to still be a member, but the node itself will refuse to restart and communicate with the cluster. This particular case is very unlikely - it requires a specifically timed crash to happen, after the data streaming phase of decommission finishes but before the node commits that it left. But if it happens, you won't be able to bootstrap other nodes (they will try to contact the partially-decommissioned node and fail) until you remove the remains of the node that crashed.

---------------------------
Handling a Failed Bootstrap
---------------------------

If a failure happens when trying to bootstrap a new node to the cluster, you can try bootstrapping the node again by restarting it.

If the failure persists or you decided that you don't want to bootstrap the node anymore, follow the instructions in the :ref:`cleaning up after a failed membership change <cleaning-up-after-change>` section to remove the remains of the bootstrapping node. You can then clear the node's data directories and attempt to bootstrap it again.

------------------------------
Handling a Failed Decommission
------------------------------

There are two cases.

Most likely the failure happened during the data repair/streaming phase - before the node tried to leave the token ring. Look for a log message containing "leaving token ring" in the logs of the node that you tried to decommission. For example:

.. code-block:: console

    INFO  2023-03-14 13:08:38,323 [shard 0] storage_service - decommission[5b2e752e-964d-4f36-871f-254491f4e8cc]: leaving token ring

If the message is **not** present, the failure happened before the node tried to leave the token ring. In that case you can simply restart the node and attempt to decommission it again.

If the message is present, the node attempted to leave the token ring, but it might have left the cluster only partially before the failure. **Do not try to restart the node**. Instead, you must make sure that the node is dead and remove any leftovers using the :doc:`removenode operation </operating-scylla/nodetool-commands/removenode/>`. See :ref:`cleaning up after a failed membership change <cleaning-up-after-change>`. Trying to restart the node after such failure results in unpredictable behavior - it may restart normally, it may refuse to restart, or it may even try to rebootstrap.

If you don't have access to the node's logs anymore, assume the second case (the node might have attempted to leave the token ring), **do not try to restart the node**, instead follow the :ref:`cleaning up after a failed membership change <cleaning-up-after-change>` section.

----------------------------
Handling a Failed Removenode
----------------------------

Simply retry the removenode operation.

If you somehow lost the host ID of the node that you tried to remove, follow the instructions in :ref:`cleaning up after a failed membership change <cleaning-up-after-change>`.

--------------------------
Handling a Failed Replace
--------------------------

Replace is a special case of bootstrap, but the bootstrapping node tries to take the place of another dead node. You can retry a failed replace operation by restarting the replacing node.

If the failure persists or you decided that you don't want to perform the replace anymore, follow the instructions in  :ref:`cleaning up after a failed membership change <cleaning-up-after-change>` section to remove the remains of the replacing node. You can then clear the node's data directories and attempt to replace again. Alternatively, you can remove the dead node which you initially tried to replace using :doc:`removenode </operating-scylla/nodetool-commands/removenode/>`, and perform a regular bootstrap.

.. _cleaning-up-after-change:

--------------------------------------------
Cleaning up after a Failed Membership Change
--------------------------------------------

After a failed membership change, the cluster may contain remains of a node that tried to leave or join - other nodes may consider the node a member, possibly in a transitioning state. It is important to remove any such "ghost" members. Their presence may reduce the cluster's availability, performance, or prevent further membership changes.

You need to determine the host IDs of any potential ghost members, then remove them using the :doc:`removenode operation </operating-scylla/nodetool-commands/removenode/>`. Note that after a failed replace, there may be two different host IDs that you'll want to find and run ``removenode`` on: the new replacing node and the old node that you tried to replace. (Or you can remove the new node only, then try to replace the old node again.)

Step One: Determining Host IDs of Ghost Members
===============================================

* After a failed bootstrap, you need to determine the host ID of the node that tried to bootstrap, if it managed to generate a host ID (it might not have chosen the host ID yet if it failed very early in the procedure, in which case there's nothing to remove). Look for a message containing ``system_keyspace - Setting local host id to`` in the node's logs, which will contain the node's host ID. For example: ``system_keyspace - Setting local host id to f180b78b-6094-434d-8432-7327f4d4b38d``. If you don't have access to the node's logs, read the generic method below.
* After a failed decommission, you need to determine the host ID of the node that tried to decommission. You can search the node's logs as in the failed bootstrap case (see above), or you can use the generic method below.
* After a failed removenode, you need to determine the host ID of the node that you tried to remove. You should already have it, since executing a removenode requires the host ID in the first place. But if you lost it somehow, read the generic method below.
* After a failed replace, you need to determine the host ID of the replacing node. Search the node's logs as in the failed bootstrap case (see above), or you can use the generic method below. You may also want to determine the host ID of the replaced node - either to attempt replacing it again after removing the remains of the previous replacing node, or to remove it using :doc:`nodetool removenode </operating-scylla/nodetool-commands/removenode/>`. You should already have the host ID of the replaced node if you used the ``replace_node_first_boot`` option to perform the replace.

If you cannot determine the ghost members' host ID using the suggestions above, use the method described below.

#. Make sure there are no ongoing membership changes.

#. Execute the following CQL query on one of your nodes to retrieve the Raft group 0 ID:

   .. code-block:: cql
    
    select value from system.scylla_local where key = 'raft_group0_id'

   For example:

   .. code-block:: cql
    
    cqlsh> select value from system.scylla_local where key = 'raft_group0_id';

     value
    --------------------------------------
     607fef80-c276-11ed-a6f6-3075f294cc65

#. Use the obtained Raft group 0 ID to query the set of all cluster members' host IDs (which includes the ghost members), by executing the following query:

   .. code-block:: cql
    
    select server_id from system.raft_state where group_id = <group0_id>

   replace ``<group0_id>`` with the group 0 ID that you obtained. For example:

   .. code-block:: cql
    
    cqlsh> select server_id from system.raft_state where group_id = 607fef80-c276-11ed-a6f6-3075f294cc65;

     server_id
    --------------------------------------
     26a9badc-6e96-4b86-a8df-5173e5ab47fe
     7991e7f5-692e-45a0-8ae5-438be5bc7c4f
     aff11c6d-fbe7-4395-b7ca-3912d7dba2c6

#. Execute the following CQL query to obtain the host IDs of all token ring members:

   .. code-block:: cql
    
    select host_id, up from system.cluster_status;

   For example:

   .. code-block:: cql
    
    cqlsh> select peer, host_id, up from system.cluster_status;

     peer      | host_id                              | up
    -----------+--------------------------------------+-------
     127.0.0.3 |                                 null | False
     127.0.0.1 | 26a9badc-6e96-4b86-a8df-5173e5ab47fe |  True
     127.0.0.2 | 7991e7f5-692e-45a0-8ae5-438be5bc7c4f |  True

   The output of this query is similar to the output of ``nodetool status``.

   We included the ``up`` column to see which nodes are down and the ``peer`` column to see their IP addresses.

   In this example, one of the nodes tried to decommission and crashed as soon as it left the token ring but before it left the Raft group. Its entry will show up in ``system.cluster_status`` queries with ``host_id = null``, like above, until the cluster is restarted.

#. A host ID belongs to a ghost member if:

   * It appears in the ``system.raft_state`` query but not in the ``system.cluster_status`` query,
   * Or it appears in the ``system.cluster_status`` query but does not correspond to any remaining node in your cluster.

   In our example, the ghost member's host ID was ``aff11c6d-fbe7-4395-b7ca-3912d7dba2c6`` because it appeared in the ``system.raft_state`` query but not in the ``system.cluster_status`` query.

   If you're unsure whether a given row in the ``system.cluster_status`` query corresponds to a node in your cluster, you can connect to each node in the cluster and execute ``select host_id from system.local`` (or search the node's logs) to obtain that node's host ID, collecting the host IDs of all nodes in your cluster. Then check if each host ID from the ``system.cluster_status`` query appears in your collected set; if not, it's a ghost member.

   A good rule of thumb is to look at the members marked as down (``up = False`` in ``system.cluster_status``) - ghost members are eventually marked as down by the remaining members of the cluster. But remember that a real member might also be marked as down if it was shutdown or partitioned away from the rest of the cluster. If in doubt, connect to each node and collect their host IDs, as described in the previous paragraph.

In some cases, even after a failed topology change, there may be no ghost members left - for example, if a bootstrapping node crashed very early in the procedure or a decommissioning node crashed after it committed the membership change but before it finalized its own shutdown steps.

If any ghost members are present, proceed to the next step.

Step Two: Removing the Ghost Members
====================================

Given the host IDs of ghost members, you can remove them using ``removenode``; follow the :doc:`documentation for removenode operation </operating-scylla/nodetool-commands/removenode/>`.

If you're executing ``removenode`` too quickly after a failed membership change, an error similar to the following might pop up:

.. code-block:: console

    nodetool: ScyllaDB API server HTTP POST to URL '/storage_service/remove_node' failed: seastar::rpc::remote_verb_error (node_ops_cmd_check: Node 127.0.0.2 rejected node_ops_cmd=removenode_abort from node=127.0.0.1 with ops_uuid=0ba0a5ab-efbd-4801-a31c-034b5f55487c, pending_node_ops={b47523f2-de6a-4c38-8490-39127dba6b6a}, pending node ops is in progress)

In that case simply wait for 2 minutes before trying ``removenode`` again.

If ``removenode`` returns an error like:

.. code-block:: console

    nodetool: ScyllaDB API server HTTP POST to URL '/storage_service/remove_node' failed: std::runtime_error (removenode[12e7e05b-d1ae-4978-b6a6-de0066aa80d8]: Host ID 42405b3b-487e-4759-8590-ddb9bdcebdc5 not found in the cluster)

and you're sure that you're providing the correct Host ID, it means that the member was already removed and you don't have to clean up after it.
