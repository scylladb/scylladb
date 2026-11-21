Disabling Tablet Background Operations
======================================

ScyllaDB migrates tablets in the background to balance the load, and splits
and merges tablets to change the number of tablets of a table, see
:doc:`Data Distribution with Tablets </architecture/tablets>`. You can disable
these operations with the REST API.

The settings described on this page apply to the whole cluster and are kept
across node restarts. Run each command on any one node. There is no need to
repeat it on other nodes.

Tablet load balancing
---------------------

The tablet load balancer migrates tablets between nodes, and between the
shards of a node, to keep the load balanced. To disable it:

.. code-block:: console

   curl -X POST "http://127.0.0.1:10000/storage_service/tablets/balancing?enabled=false"

Disabling stops the following actions:

* Migrating tablets between nodes to balance the load. A node added to the
  cluster does not take over its share of tablets until load balancing is
  enabled again.
* Migrating tablets between the shards of a node.
* Starting and completing tablet splits and merges, and scheduling the tablet
  migrations which prepare a merge. A split or a merge which is in progress
  is not cancelled. It completes after load balancing is enabled again.

Tablet migrations which move tablets away from a node which is being
decommissioned, removed or replaced are not affected, and neither is tablet
repair.

The command returns after the tablet migrations and tablet repairs which are
in progress have finished, which can take a long time. To limit the wait, pass
a grace period:

.. code-block:: console

   curl -X POST "http://127.0.0.1:10000/storage_service/tablets/balancing?enabled=false&grace_period_in_seconds=60"

After the grace period, ScyllaDB cancels the tablet migrations and tablet
repairs which are still in progress and can still be cancelled. The command
waits for the others, and for the migrations which move tablets away from a
node.

Without the ``grace_period_in_seconds`` parameter, the grace period is the
value of the ``tablet_transition_abort_grace_period_in_seconds`` configuration
option. The default is 0, which cancels nothing.

To enable load balancing again:

.. code-block:: console

   curl -X POST "http://127.0.0.1:10000/storage_service/tablets/balancing?enabled=true"

Tablet splits and merges
------------------------

Splits and merges involve the following work:

* A split runs SSTable compaction on every replica of the tablet, to
  segregate the data which belongs to each of the post-split tablets into
  separate SSTables.
* A merge migrates the replicas of adjacent tablets so that they are
  co-located on the same shard. Finalizing the merge after that is a quick
  metadata operation.

A split is a long and resource-heavy operation, which slows down other
maintenance operations. For example, scaling out often triggers splits,
because the added capacity allows more tablets per table. These splits slow
down the tablet migrations which balance the load onto the new nodes. You may
want to disable splits and merges before starting a scale-out, and enable them
again after the cluster is balanced. You may also want to disable them when
the cluster is short on CPU, I/O or disk space.

To prevent ScyllaDB from starting new splits and merges, and from completing
the ones in progress, for all tables:

.. code-block:: console

   curl -X POST "http://127.0.0.1:10000/storage_service/tablets/resize?enabled=false"

Disabling stops the following actions, for all tables:

* Starting a split or a merge. This includes a split or a merge requested by
  changing the tablet options of a table, such as ``min_tablet_count``. The
  new options take effect after splits and merges are enabled again.
* Completing a split or a merge which is in progress. ScyllaDB revokes the
  decision to split or merge, so the number of tablets of every table stays
  as it was when the command returned. Split compactions which are already
  running on nodes continue, see below.
* Scheduling tablet migrations which prepare a merge by bringing the tablets
  to be merged to the same nodes and shards. Migrations which are already
  running complete.
* The tablet layout normalization which follows a
  :doc:`vnodes-to-tablets migration </operating-scylla/procedures/config-change/migrate-vnodes-to-tablets>`.
  It continues after splits and merges are enabled again.

Split compactions which are already running on nodes are not cancelled. To
cancel them, run the following command on every node, after disabling splits
and merges:

.. code-block:: console

   nodetool stop SPLIT

See :doc:`nodetool stop </operating-scylla/nodetool-commands/stop>`.

Tablet migrations for load balancing, node operations such as adding or
removing a node, and repair are not affected.

The setting can be changed only after all nodes in the cluster are upgraded to
a version which supports it.

To check the current setting:

.. code-block:: console

   curl -X GET "http://127.0.0.1:10000/storage_service/tablets/resize"

The command prints ``true`` when splits and merges are enabled, which is the
default, and ``false`` when they are disabled.

Tablets of a growing table become larger than the target tablet size while
splits are disabled, so enable splits and merges again once the emergency is
over:

.. code-block:: console

   curl -X POST "http://127.0.0.1:10000/storage_service/tablets/resize?enabled=true"

ScyllaDB then resizes the tables according to their current sizes.
