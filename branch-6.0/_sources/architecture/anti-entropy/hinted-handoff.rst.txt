ScyllaDB Hinted Handoff
========================

A typical write in Scylla works according to the scenarios described in our :doc:`Fault Tolerance documentation </architecture/architecture-fault-tolerance>`.

But what happens when a write request is sent to a Scylla node that is unresponsive due to reasons including heavy write load on a node, network issues, or even hardware failure? To ensure availability and consistency, Scylla implements :term:`hinted handoff<Hinted Handoff>`. 

 :term:`Hint<Hint>` = target replica ID + :term:`mutation<Mutation>` data


In other words, Scylla saves a copy of the writes intended for down nodes, and replays them to the nodes when they are up later.  Thus, the write operation flow, when a node is down, looks like this:

1. The co-ordinator determines all the replica nodes;

2. Based on the replication factor (RF) , the co-ordinator :doc:`attempts to write to RF nodes </architecture/architecture-fault-tolerance>`;

.. image:: ../1-write_op_RF_3.jpg

3. If one node is down, acknowledgments are only returned from two nodes:

.. image:: hinted-handoff-3.png

4. If the consistency level does not require responses from all replicas, the co-ordinator, *V* in this case,  will respond to the client that the write was successful.   The co-ordinator will write and store a :term:`hint<Hint>` for the missing node:

.. image:: hinted-handoff-4.png

5. Once the down node comes up, the co-ordinator will replay the hint for that node.  After the co-ordinator receives an acknowledgement of the write, the hint is deleted.

.. image:: hinted-handoff-5.png

A co-ordinator stores hints for a handoff under the following conditions:

1. For down nodes;
2. If the replica doesn't respond within :code:`write_request_timeout_in_ms`.

The co-ordinator will stop creating any hints for a dead node if the node's downtime is greater than :code:`max_hint_window_in_ms`.

Hinted handoff is enabled and managed by these settings in :code:`scylla.yaml`:

* :code:`hinted_handoff_enabled`: enables or disables the hinted handoff feature completely or enumerates data centers where hints are allowed. By default, “true” enables hints to all nodes.
* :code:`max_hint_window_in_ms`: do not generate hints if the destination node has been down for more than this value.  If  a node is down longer than this period, new hints are not created.  Hint generation resumes once the destination node is back up. By default, this is set to 3 hours.
* :code:`hints_directory`: the directory where Scylla will store hints. By default this is :code:`$SCYLLA_HOME/hints`.

Storing of the hint can also fail. Enabling hinted handoff therefore does not eliminate the need for repair; a user must recurrently :doc:`run a full repair </operating-scylla/procedures/maintenance/repair/>` to ensure data consistency across the cluster nodes.

.. include:: /rst_include/apache-copyrights.rst
