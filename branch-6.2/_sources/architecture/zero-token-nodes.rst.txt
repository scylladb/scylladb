=========================
Zero-token Nodes
=========================

By default, all nodes in a cluster own a set of token ranges and are used to
replicate data. In certain circumstances, you may choose to add a node that
doesn't own any token. Such nodes are referred to as zero-token nodes. They
do not have a copy of the data but only participate in Raft quorum voting.

To configure a zero-token node, set the ``join_ring`` parameter to ``false``.

You can use zero-token nodes in multi-DC deployments to reduce the risk of
losing a quorum of nodes.
See :doc:`Preventing Quorum Loss in Symmetrical Multi-DC Clusters </operating-scylla/procedures/cluster-management/arbiter-dc>` for details.

Note that:

* Zero-token nodes are ignored by drivers, so there is no need to change
  the load balancing policy on the clients after adding zero-token nodes
  to the cluster.
* Zero-token nodes never store replicated data, so running ``nodetool rebuild``,
  ``nodetool repair``, and ``nodetool cleanup`` can be skipped as it does not
  affect zero-token nodes.
