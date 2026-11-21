Disabling Tablet Background Operations
======================================

ScyllaDB splits and merges tablets in the background to change the number of
tablets of a table, see
:ref:`Absolute number of tablets <absolute-number-of-tablets>`. In an
emergency, you can disable these operations with the REST API.

The settings described on this page apply to the whole cluster and are kept
across node restarts. Run each command on any one node. There is no need to
repeat it on other nodes.

Tablet splits and merges
------------------------

Splits and merges consume CPU, I/O and disk space: a split rewrites the
SSTables of the table on every replica, and a merge migrates tablets between
nodes. When the cluster is short on these resources, you can disable splits
and merges for all tables:

.. code-block:: console

   curl -X POST "http://127.0.0.1:10000/storage_service/tablets/resize?enabled=false"

Disabling stops the following actions, for all tables:

* Starting a split or a merge. This includes a split or a merge requested by
  changing the tablet options of a table, such as ``min_tablet_count``. The
  new options take effect after splits and merges are enabled again.
* Completing a split or a merge which is in progress. ScyllaDB cancels it, so
  the number of tablets of every table stays as it was when the command
  returned.
* Scheduling tablet migrations which prepare a merge by bringing the tablets
  to be merged to the same nodes and shards. Migrations which are already
  running complete.
* The tablet layout normalization which follows a
  :doc:`vnodes-to-tablets migration </operating-scylla/procedures/config-change/migrate-vnodes-to-tablets>`.
  It continues after splits and merges are enabled again.

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
